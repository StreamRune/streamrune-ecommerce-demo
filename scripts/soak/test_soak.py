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
    recorded runs had none), the load's first ten minutes left out."""
    run = json.loads((TESTDATA / name).read_text())
    starts = {(s["app"], s["generation"]): s["t"] for s in run["starts"]}
    return soak.resource_trends(run["metrics"], starts, [], 600, run["load_seconds"])


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

    def test_does_not_judge_a_series_shorter_than_the_minimum(self):
        self.assertIsNone(soak.grows([1, 2, 3, 400, 500, 600, 700], 1))

    def test_a_live_data_gauge_at_zero_is_no_reading(self):
        # G1 reports 0 until its first collection of the old generation; zeros at the start of a
        # process are not a low baseline that a later reading grows from.
        judged, grow, lines = synthetic_run(lambda age: 0.0 if age < 30 else 100.0, restart_every=200)
        self.assertEqual([], grow, "\n".join(lines))


if __name__ == "__main__":
    unittest.main()
