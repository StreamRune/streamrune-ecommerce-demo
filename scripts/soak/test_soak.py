"""Unit tests of the soak harness's resource-trend invariant, on recorded and synthetic metrics.

Run from the repository root: python3 -m unittest discover -s scripts/soak -p 'test_*.py'
(the root project's soakHarnessTest task runs the same, and `test` and `check` depend on it).
"""

import json
import unittest
from pathlib import Path

import soak

TESTDATA = Path(__file__).resolve().parent / "testdata"


def judge_recorded(name):
    """resource_trends over a recorded run, as invariant 11 calls it: no failure windows (the
    recorded runs have none) and the load's warm-up, min(600 s, a fifth of the load), left out."""
    run = json.loads((TESTDATA / name).read_text())
    starts = {(s["app"], s["generation"]): s["t"] for s in run["starts"]}
    warm_up = min(600, 0.2 * run["load_seconds"])
    return soak.resource_trends(run["metrics"], starts, [], warm_up, run["load_seconds"])


def synthetic_run(live_mb_at_age, restart_every, minutes=120):
    """One sample a minute of three backends; each restarts every restart_every minutes (a new
    generation), and its live data after GC is live_mb_at_age(age in minutes)."""
    starts, metrics = {}, []
    for app, offset in (("spring-1", 0), ("spring-2", 3), ("quarkus", 6)):
        gen, born = 1, -offset
        starts[(app, gen)] = born * 60.0
        for minute in range(minutes):
            if minute - born >= restart_every:
                gen, born = gen + 1, minute
                starts[(app, gen)] = born * 60.0
    for minute in range(minutes):
        apps = []
        for app in ("spring-1", "spring-2", "quarkus"):
            gen = max(g for (a, g), t in starts.items() if a == app and t <= minute * 60.0)
            age = minute - starts[(app, gen)] / 60.0
            live = live_mb_at_age(age) if app == "quarkus" else 35.0
            apps.append({"app": app, "generation": gen, "gc_live_mb": live, "rss_mb": 380.0, "threads": 42.0,
                         "outbox_pending": 3.0, "dlq_pending": 0.0, "saga_faulted": 0.0})
        metrics.append({"t": minute * 60.0 + 1, "phase": "load", "apps": apps,
                        "db": {"connections": 70, "idle_in_tx": 0},
                        "store": {"outbox_open": 5, "dlq": 0, "sagas_open": 10}})
    return soak.resource_trends(metrics, starts, [], 600, minutes * 60.0)


def smoke_shaped_run():
    """A five-minute run without failures as a smoke run samples it, every 15 seconds: the backends
    start a minute before the load, and everything rises, the open sagas from 10 to 250."""
    starts = {(app, 1): -60.0 for app in ("spring-1", "spring-2", "quarkus")}
    metrics = []
    for i in range(21):
        t = i * 15.0
        apps = [{"app": app, "generation": 1, "gc_live_mb": 30.0 + 2 * i, "rss_mb": 300.0 + 10 * i,
                 "threads": 40.0 + i, "outbox_pending": 5.0 * i, "dlq_pending": 0.0, "saga_faulted": 0.0}
                for app in ("spring-1", "spring-2", "quarkus")]
        metrics.append({"t": t, "phase": "load", "apps": apps,
                        "db": {"connections": 60 + 2 * i, "idle_in_tx": i},
                        "store": {"outbox_open": 20 * i, "dlq": i, "sagas_open": 10 + 12 * i}})
    # check() leaves out min(600 s, a fifth of the load): 60 s of a five-minute load.
    return soak.resource_trends(metrics, starts, [], 60, 300)


class ResourceTrendTest(unittest.TestCase):
    def test_flags_the_quarkus_live_data_that_grows_through_a_run_without_failures(self):
        judged, grow, lines = judge_recorded("leaking-quarkus.json")
        self.assertTrue(any(g.startswith("quarkus#1 gc_live_mb grows") for g in grow), "\n".join(lines))
        self.assertFalse(any(g.startswith("spring") for g in grow), "\n".join(grow))

    def test_passes_a_run_whose_resources_stay_flat(self):
        judged, grow, lines = judge_recorded("bounded.json")
        self.assertGreater(judged, 0)
        self.assertEqual([], grow, "\n".join(lines))

    def test_flags_a_leak_whose_processes_restart_before_any_collects_enough_samples(self):
        # Every process lives 12 minutes: 7 samples after its warm-up, fewer than one process
        # needs to be judged, yet each one's live data climbs 25 MB a minute.
        judged, grow, lines = synthetic_run(lambda age: 30.0 + 25.0 * max(0.0, age - 5), restart_every=12)
        self.assertFalse(any("#" in g for g in grow), "\n".join(grow))
        self.assertTrue(any(g.startswith("quarkus (its") and "gc_live_mb" in g for g in grow), "\n".join(lines))

    def test_passes_restarting_processes_whose_live_data_stays_flat(self):
        judged, grow, lines = synthetic_run(lambda age: 35.0 + (age % 3), restart_every=12)
        self.assertGreater(judged, 0)
        self.assertEqual([], grow, "\n".join(lines))

    def test_judges_no_series_of_a_smoke_shaped_run(self):
        judged, grow, lines = smoke_shaped_run()
        self.assertEqual(0, judged, "\n".join(lines))
        self.assertEqual([], grow)

    def test_a_smoke_run_reports_the_trend_as_not_judged_whatever_its_series(self):
        ok, summary, _ = soak.trend_verdict({"smoke": True}, 5, ["store sagas_open grows steadily"], [])
        self.assertIsNone(ok)
        self.assertIn("smoke run", summary)
        self.assertIs(False, soak.trend_verdict({"smoke": False}, 5, ["x grows"], [])[0])
        self.assertIsNone(soak.trend_verdict({"smoke": False}, 0, [], [])[0])

    def test_a_small_gauge_that_doubles_does_not_grow(self):
        # From 2 to 4 threads is twice as many, yet below the floor of ten.
        self.assertIs(False, soak.grows([2.0] * 6 + [4.0] * 6, soak.GROWTH_FLOOR["threads"]))
        self.assertIs(True, soak.grows([2.0] * 6 + [40.0] * 6, soak.GROWTH_FLOOR["threads"]))

    def test_a_last_quarter_with_one_low_reading_does_not_grow(self):
        # A G1 collection in the last quarter brings the live data back to its first level once:
        # the median rises, the floor does not.
        series = [100.0] * 12 + [300.0, 300.0, 100.0, 300.0]
        self.assertIs(False, soak.grows(series, soak.GROWTH_FLOOR["gc_live_mb"]))
        self.assertIs(True, soak.grows([100.0] * 12 + [300.0] * 4, soak.GROWTH_FLOOR["gc_live_mb"]))

    def test_does_not_judge_a_series_shorter_than_the_minimum(self):
        self.assertIsNone(soak.grows([1, 2, 3, 400, 500, 600, 700], 1))

    def test_a_live_data_gauge_at_zero_is_no_reading(self):
        # G1 reports 0 until its first collection of the old generation; zeros at the start of a
        # process are not a low baseline that a later reading grows from.
        judged, grow, lines = synthetic_run(lambda age: 0.0 if age < 30 else 100.0, restart_every=200)
        self.assertEqual([], grow, "\n".join(lines))


if __name__ == "__main__":
    unittest.main()
