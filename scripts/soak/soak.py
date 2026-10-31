#!/usr/bin/env python3
"""Soak test of the demo: mixed load, scheduled failures and invariant checks over hours.

Topology: one PostgreSQL and one RabbitMQ from docker-compose.yml (Compose project
streamrune-soak), and on that one database two Spring Boot backends (ports 8080 and 8091) and the
Quarkus backend (port 8082), each a JVM process this script starts and stops itself.

Usage (from the repository root, with the JARs built):
  ./gradlew :spring-app:bootJar :quarkus-app:quarkusBuild
  python3 scripts/soak/soak.py --duration 5m          # the smoke run
  python3 scripts/soak/soak.py --duration 2h          # a soak run
  python3 scripts/soak/soak.py check build/soak/<run> # the checks again, stack still up

docs/soak.md describes the load, the failure schedule, the invariants and the files of a run.
Standard library only; needs Python 3.10+, Docker and a JDK 25 (JAVA_HOME or java on PATH).
Exit status: 0 when every invariant holds, 1 when one fails, 2 when the run could not start.
"""

import argparse
import collections
import datetime
import heapq
import http.client
import json
import os
import random
import re
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PROJECT = "streamrune-soak"
DB = "streamrune_ecommerce"
SAGA_TYPE = "org.streamrune.ecommerce.commands.saga.OrderFulfillmentState"
ADMIN = "soak-admin"
RABBIT_API = ("localhost", 15672)
AUDIT_QUEUE = "soak.audit"
EXCHANGE = "streamrune.events"
INTEGRATION_QUEUE = "streamrune.integration"
TERMINAL_SAGA = ("COMPLETED", "COMPENSATED", "FAILED")


def now():
    return time.time()


def iso(t):
    return datetime.datetime.fromtimestamp(t).astimezone().isoformat(timespec="seconds")


def parse_duration(text):
    m = re.fullmatch(r"(\d+)([smh])", text.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"not a duration: {text} (use 90s, 5m or 2h)")
    return int(m.group(1)) * {"s": 1, "m": 60, "h": 3600}[m.group(2)]


class JsonLog:
    """An append-only JSON-lines file several threads write to."""

    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.f = open(path, "a", buffering=1)

    def write(self, record):
        line = json.dumps(record, separators=(",", ":"))
        with self.lock:
            if not self.f.closed:  # a reader thread that ends after the run closed its file
                self.f.write(line + "\n")

    def close(self):
        with self.lock:
            self.f.close()


def read_jsonl(path):
    out = []
    if not Path(path).exists():
        return out
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass  # a line cut by a killed run
    return out


# ---------------------------------------------------------------------------------------------
# Docker Compose, PostgreSQL and RabbitMQ
# ---------------------------------------------------------------------------------------------


def compose(*args, timeout=300, check=True):
    cmd = ["docker", "compose", "-p", COMPOSE_PROJECT, "-f", str(ROOT / "docker-compose.yml"), *args]
    r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed: {r.stderr.strip()}")
    return r.stdout.strip()


def container_id(service):
    return compose("ps", "-q", service).splitlines()[0]


def psql(sql, timeout=180):
    """Runs SQL in the soak database and returns psql's unaligned output."""
    r = subprocess.run(
        ["docker", "exec", "-i", container_id("postgres"), "psql", "-U", "postgres", "-d", DB,
         "-v", "ON_ERROR_STOP=1", "-At", "-F", "\t"],
        input=sql, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(f"psql failed: {r.stderr.strip()}\n{sql[:400]}")
    return r.stdout


def sql_rows(query, timeout=180):
    """The rows of a query as a list of dicts (through json_agg)."""
    out = psql(f"SELECT coalesce(json_agg(soak_row), '[]'::json) FROM ({query}) soak_row;", timeout)
    return json.loads(out.strip() or "[]")


def sql_one(query, timeout=60):
    rows = sql_rows(query, timeout)
    return rows[0] if rows else {}


def rabbit_api(method, path, body=None, timeout=10):
    conn = http.client.HTTPConnection(*RABBIT_API, timeout=timeout)
    auth = "Basic Z3Vlc3Q6Z3Vlc3Q="  # guest:guest, the broker's default user of docker-compose.yml
    data = json.dumps(body).encode() if body is not None else None
    conn.request(method, path, body=data,
                 headers={"Authorization": auth, "Content-Type": "application/json"})
    resp = conn.getresponse()
    raw = resp.read()
    conn.close()
    return resp.status, (json.loads(raw) if raw and raw[:1] in (b"{", b"[") else raw)


def wait_until(predicate, timeout, interval=1.0):
    deadline = now() + timeout
    while now() < deadline:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def start_infrastructure(fresh):
    if fresh:
        compose("down", "-v", "--remove-orphans", timeout=180, check=False)
    compose("up", "-d", "postgres", "rabbitmq", timeout=600)
    if not wait_until(lambda: "accepting" in subprocess.run(
            ["docker", "exec", container_id("postgres"), "pg_isready", "-U", "postgres"],
            capture_output=True, text=True).stdout, 120):
        raise RuntimeError("PostgreSQL did not become ready within 120 s")
    if not wait_until(lambda: rabbit_api("GET", "/api/overview")[0] == 200, 180, 2):
        raise RuntimeError("RabbitMQ's management API did not answer within 180 s")
    # The audit queue counts every integration event the outbox publishes: bound before the
    # backends start, so it sees the first one.
    rabbit_api("PUT", f"/api/exchanges/%2F/{EXCHANGE}",
               {"type": "topic", "durable": True, "auto_delete": False, "internal": False})
    rabbit_api("PUT", f"/api/queues/%2F/{AUDIT_QUEUE}", {"durable": True, "auto_delete": False})
    rabbit_api("POST", f"/api/bindings/%2F/e/{EXCHANGE}/q/{AUDIT_QUEUE}",
               {"routing_key": "integration.#"})


# ---------------------------------------------------------------------------------------------
# The backends
# ---------------------------------------------------------------------------------------------


def java_bin():
    home = os.environ.get("JAVA_HOME")
    return str(Path(home) / "bin" / "java") if home else "java"


class App:
    def __init__(self, name, kind, port, run_dir, events, args):
        self.name, self.kind, self.port = name, kind, port
        self.run_dir, self.events, self.args = run_dir, events, args
        self.proc = None
        self.generation = 0
        self.lock = threading.RLock()  # one lifecycle action at a time
        self.flag_lock = threading.Lock()

    @property
    def base(self):
        return f"http://localhost:{self.port}"

    def health_path(self):
        return "/actuator/health" if self.kind == "spring" else "/q/health"

    def command(self):
        heap = f"-Xmx{self.args.heap}"
        if self.kind == "spring":
            jar = ROOT / "spring-app/build/libs/spring-app-0.1.0-SNAPSHOT.jar"
            return [java_bin(), "--enable-preview", heap, "-jar", str(jar), f"--server.port={self.port}"]
        jar = ROOT / "quarkus-app/build/quarkus-app/quarkus-run.jar"
        return [java_bin(), "--enable-preview", heap, f"-Dquarkus.http.port={self.port}", "-jar", str(jar)]

    def env(self):
        env = dict(os.environ)
        env["MANAGEMENT_ENDPOINT_HEALTH_SHOW_DETAILS"] = "always"
        if self.kind == "quarkus" and self.args.quarkus_pool_size > 0:
            # Quarkus' own default (50) for the application pool, and so for the lock pool that
            # takes its size, opens more connections than PostgreSQL's 100 leave beside two
            # Spring backends; docs/soak.md explains the budget.
            env["QUARKUS_DATASOURCE_JDBC_MAX_SIZE"] = str(self.args.quarkus_pool_size)
        return env

    def start(self):
        with self.lock:
            self.generation += 1
            log = open(self.run_dir / "logs" / f"{self.name}.{self.generation}.log", "w")
            self.proc = subprocess.Popen(self.command(), cwd=ROOT, env=self.env(), stdout=log,
                                         stderr=subprocess.STDOUT, start_new_session=True)
            log.close()
            self.events.write({"t": now(), "app": self.name, "event": "start", "pid": self.proc.pid,
                               "generation": self.generation})

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def healthy(self):
        try:
            status, body = http_call(self.port, "GET", self.health_path(), timeout=5)
            return status == 200 and '"UP"' in body.replace(" ", "")
        except Exception:
            return False

    def wait_healthy(self, timeout=240):
        ok = wait_until(lambda: self.alive() and self.healthy(), timeout, 1.0)
        self.events.write({"t": now(), "app": self.name, "event": "healthy" if ok else "not-healthy"})
        return ok

    def kill9(self):
        with self.lock:
            if self.alive():
                os.kill(self.proc.pid, signal.SIGKILL)
                self.proc.wait(30)
                self.events.write({"t": now(), "app": self.name, "event": "kill-9"})

    def stop(self, timeout=90):
        with self.lock:
            if not self.alive():
                return
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout)
                self.events.write({"t": now(), "app": self.name, "event": "stopped",
                                   "exit": self.proc.returncode})
            except subprocess.TimeoutExpired:
                os.kill(self.proc.pid, signal.SIGKILL)
                self.proc.wait(30)
                self.events.write({"t": now(), "app": self.name, "event": "stop-timeout-kill-9"})

    def saga_role(self):
        """LEADING or STANDBY for the order-fulfillment saga, from the health details."""
        try:
            status, body = http_call(self.port, "GET", self.health_path(), timeout=5)
            doc = json.loads(body)
        except Exception:
            return None
        if self.kind == "spring":
            details = doc.get("components", {}).get("streamRune", {}).get("details", {})
            return (details.get("subscription.order-fulfillment-saga") or {}).get("role")
        for check in doc.get("checks", []):
            if check.get("name") == "streamrune":
                return (check.get("data") or {}).get("subscription.order-fulfillment-saga.role")
        return None

    def sync_payment_flags(self, flags):
        """Sets the simulated gateway's outage and decline switches to the wanted values."""
        with self.flag_lock:
            for flag, path, field in (("outage", "/api/admin/payment-outage", "outage"),
                                      ("decline", "/api/admin/payment-failure", "failureInjected")):
                want = flags[flag]
                for _ in range(3):
                    try:
                        status, body = http_call(self.port, "GET", path, headers=admin_headers(), timeout=5)
                        if status == 200 and json.loads(body).get(field) == want:
                            break
                        http_call(self.port, "POST", path + "/toggle", headers=admin_headers(), timeout=5)
                    except Exception:
                        time.sleep(1)


# ---------------------------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------------------------

_tls = threading.local()


def http_call(port, method, path, headers=None, body=None, timeout=30):
    """One request on this thread's keep-alive connection to the port; returns (status, text)."""
    conns = getattr(_tls, "conns", None)
    if conns is None:
        conns = _tls.conns = {}
    hdrs = {"Content-Type": "application/json", **(headers or {})}
    data = json.dumps(body).encode() if body is not None else None
    for attempt in (0, 1):
        conn = conns.get(port)
        if conn is None:
            conn = conns[port] = http.client.HTTPConnection("localhost", port, timeout=timeout)
        conn.timeout = timeout
        try:
            if conn.sock is not None:
                conn.sock.settimeout(timeout)
            conn.request(method, path, body=data, headers=hdrs)
            resp = conn.getresponse()
            text = resp.read().decode("utf-8", "replace")
            return resp.status, text
        except (http.client.RemoteDisconnected, BrokenPipeError, ConnectionResetError) as e:
            conn.close()
            conns.pop(port, None)
            # A request on a reused connection the server had already closed never reached it.
            if attempt == 0 and getattr(conn, "_soak_used", False) and isinstance(
                    e, (http.client.RemoteDisconnected, BrokenPipeError)):
                continue
            raise
        except Exception:
            conn.close()
            conns.pop(port, None)
            raise
        finally:
            conn._soak_used = True
    raise RuntimeError("unreachable")


def admin_headers():
    return {"X-User-Id": ADMIN, "X-User-Role": "ADMIN"}


def customer_headers(cid):
    return {"X-User-Id": cid, "X-User-Role": "CUSTOMER"}


# ---------------------------------------------------------------------------------------------
# Load driver
# ---------------------------------------------------------------------------------------------


class Driver:
    WEIGHTS = [("place_order", 45), ("register_customer", 12), ("receive_shipment", 10),
               ("adjust_stock", 6), ("update_profile", 4), ("update_price", 3), ("create_product", 2)]

    def __init__(self, run, apps, rate, workers):
        self.run, self.apps, self.rate = run, apps, rate
        self.rid = run.rid
        self.log = JsonLog(run.dir / "requests.jsonl")
        self.qlog = JsonLog(run.dir / "queries.jsonl")
        self.sselog = JsonLog(run.dir / "sse.jsonl")
        self.lock = threading.Lock()
        self.customers, self.products, self.orders = [], [], []
        self.counter = collections.Counter()
        self.tasks = []  # heap of (due, seq, fn)
        self.task_cv = threading.Condition()
        self.seq = 0
        self.stop_load = threading.Event()
        self.stop_all = threading.Event()
        self.n_workers = workers
        self.inflight = 0
        self.rr = 0
        self.saturated = 0

    # -- ids and routing --
    def next_id(self, prefix):
        with self.lock:
            self.counter[prefix] += 1
            return f"soak-{self.rid}-{prefix}{self.counter[prefix]}"

    def pick_apps(self):
        with self.lock:
            self.rr += 1
            start = self.rr
        return [self.apps[(start + i) % len(self.apps)] for i in range(len(self.apps))]

    def send(self, op, method, path, headers, body=None, key=None, entity=None, intent=None,
             resend=False, timeout=30):
        """Sends a command to one backend (the next one when a backend refuses the connection)
        and records it. Returns (status, text)."""
        hdrs = dict(headers)
        if key:
            hdrs["Idempotency-Key"] = key
        tried = []
        status, text, app_name = 0, "", None
        t0 = now()
        for app in self.pick_apps():
            app_name = app.name
            tried.append(app.name)
            try:
                status, text = http_call(app.port, method, path, hdrs, body, timeout)
                break
            except ConnectionRefusedError:
                status, text = 0, "connection refused"
                continue  # the request never reached that backend: the next one gets it
            except socket.timeout:
                status, text = 0, "timeout"
                break
            except Exception as e:
                status, text = 0, f"{type(e).__name__}: {e}"
                break
        rec = {"t": t0, "ms": round((now() - t0) * 1000, 1), "op": op, "method": method,
               "path": path, "app": app_name, "status": status, "key": key, "id": entity,
               "intent": intent, "resend": resend}
        if len(tried) > 1:
            rec["tried"] = tried
        if status >= 300 or status == 0:
            rec["body"] = text[:300]
            if text.startswith("{"):
                try:
                    doc = json.loads(text)
                    rec["reason"] = doc.get("reason")
                    rec["commandId"] = doc.get("commandId")
                except json.JSONDecodeError:
                    pass
        self.log.write(rec)
        return status, text

    # -- scheduling --
    def schedule(self, delay, fn):
        with self.task_cv:
            self.seq += 1
            heapq.heappush(self.tasks, (now() + delay, self.seq, fn))
            self.task_cv.notify()

    def worker(self):
        while not self.stop_all.is_set():
            with self.task_cv:
                while not self.stop_all.is_set():
                    if self.tasks and self.tasks[0][0] <= now():
                        _, _, fn = heapq.heappop(self.tasks)
                        self.inflight += 1
                        break
                    wait = (self.tasks[0][0] - now()) if self.tasks else 0.5
                    self.task_cv.wait(max(0.01, min(wait, 0.5)))
                else:
                    return
            try:
                fn()
            except Exception as e:
                self.log.write({"t": now(), "op": "driver-error", "error": f"{type(e).__name__}: {e}"})
            finally:
                with self.task_cv:
                    self.inflight -= 1

    def backlog(self):
        with self.task_cv:
            return sum(1 for d, _, _ in self.tasks if d <= now()) + self.inflight

    def generator(self):
        names = [w[0] for w in self.WEIGHTS]
        weights = [w[1] for w in self.WEIGHTS]
        interval = 1.0 / self.rate
        nxt = now()
        while not self.stop_load.is_set():
            nxt += interval
            delay = nxt - now()
            if delay > 0:
                time.sleep(delay)
            if self.backlog() > 4 * self.n_workers:
                self.saturated += 1
                continue
            op = random.choices(names, weights)[0]
            self.schedule(0, getattr(self, "op_" + op))

    # -- bootstrap --
    def bootstrap(self, products=20, customers=40):
        for _ in range(products):
            self.op_create_product(initial=100000)
        for _ in range(customers):
            self.op_register_customer()
        ok = wait_until(lambda: len(self.products) >= products and len(self.customers) >= customers, 120)
        if not ok:
            raise RuntimeError(f"bootstrap: {len(self.products)} products and {len(self.customers)}"
                               " customers visible after 120 s")

    def visible(self, path, headers):
        app = random.choice(self.apps)
        try:
            return http_call(app.port, "GET", path, headers, timeout=5)[0] == 200
        except Exception:
            return False

    def await_visible(self, path, headers, on_visible, waited=0):
        if self.visible(path, headers):
            on_visible()
        elif waited < 60:
            self.schedule(1, lambda: self.await_visible(path, headers, on_visible, waited + 1))

    # -- operations --
    def op_create_product(self, initial=None):
        pid = self.next_id("p")
        body = {"productId": pid, "name": f"Soak product {pid}", "description": "soak",
                "category": "Soak", "price": round(random.uniform(5, 50), 2), "initialStock": 0}
        key = str(uuid.uuid4())
        status, _ = self.send("create_product", "POST", "/api/products", admin_headers(), body, key, pid)
        if random.random() < 0.05:
            self.schedule(random.uniform(0.2, 5), lambda: self.send(
                "create_product", "POST", "/api/products", admin_headers(), body, key, pid, resend=True))
        if 200 <= status < 300:
            qty = initial or random.randint(2000, 5000)
            self.receive(pid, qty)
            self.schedule(0.5, lambda: self.await_visible(
                f"/api/products/{pid}", admin_headers(),
                lambda: self._add("products", pid)))

    def _add(self, attr, value):
        with self.lock:
            getattr(self, attr).append(value)

    def receive(self, pid, qty, op="receive_shipment"):
        key = str(uuid.uuid4())
        body = {"quantity": qty}
        self.send(op, "POST", f"/api/inventory/{pid}/receive", admin_headers(), body, key, pid)
        if random.random() < 0.15:
            self.schedule(random.uniform(0.05, 5), lambda: self.send(
                op, "POST", f"/api/inventory/{pid}/receive", admin_headers(), body, key, pid,
                resend=True))

    def op_receive_shipment(self):
        pid = self.random_of("products")
        if pid:
            self.receive(pid, random.randint(50, 200))

    def op_adjust_stock(self):
        pid = self.random_of("products")
        if not pid:
            return
        key = str(uuid.uuid4())
        body = {"quantity": random.randint(1, 5), "reason": "soak count"}
        path = f"/api/products/{pid}/stock"
        self.send("adjust_stock", "PUT", path, admin_headers(), body, key, pid)
        if random.random() < 0.15:
            self.schedule(random.uniform(0.05, 5), lambda: self.send(
                "adjust_stock", "PUT", path, admin_headers(), body, key, pid, resend=True))

    def op_update_price(self):
        pid = self.random_of("products")
        if pid:
            self.send("update_price", "PUT", f"/api/products/{pid}/price", admin_headers(),
                      {"price": round(random.uniform(5, 50), 2)}, str(uuid.uuid4()), pid)

    def op_register_customer(self):
        cid = self.next_id("c")
        body = {"customerId": cid, "name": f"Soak {cid}", "email": f"{cid}@soak.example",
                "address": "1 Soak Street", "phone": "+420000000000"}
        key = str(uuid.uuid4())
        status, _ = self.send("register_customer", "POST", "/api/customers", customer_headers(cid),
                              body, key, cid)
        if random.random() < 0.05:
            self.schedule(random.uniform(0.2, 5), lambda: self.send(
                "register_customer", "POST", "/api/customers", customer_headers(cid), body, key, cid,
                resend=True))
        if 200 <= status < 300:
            self.schedule(0.5, lambda: self.await_visible(
                f"/api/customers/{cid}", customer_headers(cid), lambda: self._add("customers", cid)))

    def op_update_profile(self):
        cid = self.random_of("customers")
        if cid:
            self.send("update_profile", "PUT", f"/api/customers/{cid}", customer_headers(cid),
                      {"name": f"Soak {cid} {random.randint(1, 999)}", "email": f"{cid}@soak.example",
                       "address": f"{random.randint(1, 99)} Soak Street", "phone": "+420000000000"},
                      str(uuid.uuid4()), cid)

    def random_of(self, attr):
        with self.lock:
            items = getattr(self, attr)
            return random.choice(items) if items else None

    def op_place_order(self):
        cid = self.random_of("customers")
        with self.lock:
            prods = random.sample(self.products, min(len(self.products), random.choice((1, 1, 2))))
        if not cid or not prods:
            return
        intent = random.choices(["happy", "cancel_soon", "cancel_confirmed", "ship", "oos"],
                                [70, 8, 8, 12, 2])[0]
        oid = self.next_id("o")
        lines = [{"productId": p, "quantity": random.randint(1, 3)} for p in prods]
        if intent == "oos":
            lines[0]["quantity"] = 10_000_000  # more than any product ever holds
        body = {"orderId": oid, "customerId": cid, "lines": lines}
        key = str(uuid.uuid4())
        status, _ = self.send("place_order", "POST", "/api/orders", customer_headers(cid), body, key,
                              oid, intent)
        if random.random() < 0.05:
            self.schedule(random.uniform(0.2, 5), lambda: self.send(
                "place_order", "POST", "/api/orders", customer_headers(cid), body, key, oid, intent,
                resend=True))
        if not 200 <= status < 300:
            return
        with self.lock:
            self.orders.append((oid, cid))
            if len(self.orders) > 2000:
                del self.orders[:1000]
        if intent == "cancel_soon":
            self.schedule(random.uniform(1, 3), lambda: self.cancel(oid, cid, intent, 0))
        elif intent in ("cancel_confirmed", "ship"):
            self.schedule(2, lambda: self.when_confirmed(oid, cid, intent, 0))

    def cancel(self, oid, cid, intent, waited):
        status, _ = self.send("cancel_order", "POST", f"/api/orders/{oid}/cancel",
                              customer_headers(cid), {"reason": "soak: changed my mind"},
                              str(uuid.uuid4()), oid, intent)
        if status == 403 and waited < 15:  # the read model does not know the order yet
            self.schedule(1, lambda: self.cancel(oid, cid, intent, waited + 1))

    def order_status(self, oid, cid):
        app = random.choice(self.apps)
        try:
            status, text = http_call(app.port, "GET", f"/api/orders/{oid}", customer_headers(cid), timeout=5)
            return json.loads(text).get("status") if status == 200 else None
        except Exception:
            return None

    def when_confirmed(self, oid, cid, intent, waited):
        st = self.order_status(oid, cid)
        if st == "CONFIRMED":
            if intent == "cancel_confirmed":
                self.cancel(oid, cid, intent, 0)
            else:
                status, _ = self.send("ship_order", "POST", f"/api/orders/{oid}/ship", admin_headers(),
                                      None, str(uuid.uuid4()), oid, intent)
                if 200 <= status < 300:
                    self.schedule(random.uniform(1, 5), lambda: self.send(
                        "deliver_order", "POST", f"/api/orders/{oid}/deliver", admin_headers(), None,
                        str(uuid.uuid4()), oid, intent))
        elif st in ("CANCELLED", "SHIPPED", "DELIVERED"):
            return
        elif waited < 300:  # ten minutes of polling covers every outage of the schedule
            self.schedule(2, lambda: self.when_confirmed(oid, cid, intent, waited + 1))
        else:
            self.log.write({"t": now(), "op": "follow-up-gave-up", "id": oid, "intent": intent,
                            "last_status": st})

    # -- readers --
    def query_reader(self, per_second):
        while not self.stop_load.is_set():
            time.sleep(random.expovariate(per_second))
            app = random.choice(self.apps)
            kind = random.choice(("order", "product", "products", "customer"))
            with self.lock:
                order = random.choice(self.orders) if self.orders else None
            pid = self.random_of("products")
            cid = self.random_of("customers")
            if kind == "order" and order:
                path, hdrs = f"/api/orders/{order[0]}", customer_headers(order[1])
            elif kind == "product" and pid:
                path, hdrs = f"/api/products/{pid}", admin_headers()
            elif kind == "customer" and cid:
                path, hdrs = f"/api/customers/{cid}", customer_headers(cid)
            else:
                path, hdrs = "/api/products", admin_headers()
            t0 = now()
            try:
                status, _ = http_call(app.port, "GET", path, hdrs, timeout=10)
            except Exception as e:
                status = 0
            self.qlog.write({"t": t0, "ms": round((now() - t0) * 1000, 1), "app": app.name,
                             "kind": kind, "status": status})

    def sse_reader(self):
        """Holds one live stream of an order (as its customer) for up to a minute, then the next."""
        while not self.stop_load.is_set():
            with self.lock:
                # A recent order, so that its saga's events are still to come; a stream of an order
                # the read model does not know yet is refused (its authorizer looks the customer up
                # there), and the reader then takes another.
                order = random.choice(self.orders[-60:-10]) if len(self.orders) > 60 else None
            if not order:
                time.sleep(1)
                continue
            app = random.choice(self.apps)
            t0, frames, status, end = now(), 0, 0, "rotated"
            conn = http.client.HTTPConnection("localhost", app.port, timeout=75)
            try:
                conn.request("GET", f"/api/sse/order/{order[0]}",
                             headers={**customer_headers(order[1]), "Accept": "text/event-stream"})
                resp = conn.getresponse()
                status = resp.status
                if status == 200:
                    while now() - t0 < 60 and not self.stop_load.is_set():
                        line = resp.fp.readline()
                        if not line:
                            end = "closed-by-server"
                            break
                        if line.startswith(b"data:"):
                            frames += 1
                else:
                    end = f"refused {status}"
            except Exception as e:
                end = f"error: {type(e).__name__}"
            finally:
                conn.close()
            self.sselog.write({"t": t0, "app": app.name, "order": order[0], "status": status,
                               "frames": frames, "seconds": round(now() - t0, 1), "end": end})
            if status != 200:
                time.sleep(0.5)

    def start(self, query_rate, sse_readers):
        self.threads = [threading.Thread(target=self.worker, daemon=True) for _ in range(self.n_workers)]
        for t in self.threads:
            t.start()
        self.bootstrap()
        self.load_threads = [threading.Thread(target=self.generator, daemon=True)]
        self.load_threads += [threading.Thread(target=self.query_reader, args=(query_rate / 2,), daemon=True)
                              for _ in range(2)]
        self.load_threads += [threading.Thread(target=self.sse_reader, daemon=True) for _ in range(sse_readers)]
        for t in self.load_threads:
            t.start()

    def finish(self, follow_up_grace):
        """Stops new load, lets the scheduled follow-ups run out, then stops the workers."""
        self.stop_load.set()
        deadline = now() + follow_up_grace
        while now() < deadline:
            with self.task_cv:
                pending = len(self.tasks) + self.inflight
            if pending == 0:
                break
            time.sleep(1)
        self.stop_all.set()
        with self.task_cv:
            left = len(self.tasks)
            self.task_cv.notify_all()
        for t in self.threads + self.load_threads:
            t.join(80)
        self.log.close()
        self.qlog.close()
        self.sselog.close()
        return left


# ---------------------------------------------------------------------------------------------
# Failure schedule
# ---------------------------------------------------------------------------------------------


def build_schedule(duration, smoke):
    """(offset seconds, kind, length seconds) of every failure, ending before the load stops."""
    events = []
    if smoke:
        d = duration
        events = [(0.08 * d, "payment-decline", 15), (0.12 * d, "payment-outage", min(180, 0.1 * d)),
                  (0.25 * d, "leader-kill", 30), (0.40 * d, "rabbitmq-stop", min(120, 0.08 * d)),
                  (0.55 * d, "postgres-pause", 10), (0.65 * d, "rolling-restart", 0)]
        return sorted(events)

    def every(first, period, kind, length):
        t = first
        while t + length < duration - 180:
            events.append((t, kind, length))
            t += period

    every(120, 240, "payment-decline", 20)
    every(300, 1500, "payment-outage", 180)
    every(450, 900, "leader-kill", 45)
    every(720, 1800, "rabbitmq-stop", 120)
    every(1200, 2400, "postgres-pause", 20)
    events.append((duration / 2 + 60, "rolling-restart", 0))
    return sorted(events)


class Chaos:
    def __init__(self, run, apps):
        self.run, self.apps = run, apps
        self.log = JsonLog(run.dir / "chaos.jsonl")
        self.flags = {"outage": False, "decline": False}
        self.lifecycle = threading.Lock()  # one kill or restart sequence at a time
        self.threads = []

    def record(self, kind, start, end, **detail):
        self.log.write({"kind": kind, "start": start, "end": end, **detail})

    def set_flag(self, flag, value):
        self.flags[flag] = value
        threads = [threading.Thread(target=a.sync_payment_flags, args=(self.flags,)) for a in self.apps if a.alive()]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)

    def restart_app(self, app, delay, hard):
        if hard:
            app.kill9()
        else:
            app.stop()
        time.sleep(delay)
        app.start()
        ok = app.wait_healthy()
        app.sync_payment_flags(self.flags)
        return ok

    def run_event(self, kind, length):
        t0 = now()
        try:
            if kind in ("payment-decline", "payment-outage"):
                flag = "decline" if kind == "payment-decline" else "outage"
                self.set_flag(flag, True)
                time.sleep(length)
                self.set_flag(flag, False)
                self.record(kind, t0, now())
            elif kind == "leader-kill":
                with self.lifecycle:
                    leader = None
                    for _ in range(30):
                        leading = [a for a in self.apps if a.alive() and a.saga_role() == "LEADING"]
                        if len(leading) == 1:
                            leader = leading[0]
                            break
                        time.sleep(1)
                    found = leader is not None
                    leader = leader or random.choice([a for a in self.apps if a.alive()] or self.apps)
                    t0 = now()
                    delay = random.uniform(30, 60) if length >= 30 else length
                    ok = self.restart_app(leader, delay, hard=True)
                    takeover = [a.name for a in self.apps if a.alive() and a.saga_role() == "LEADING"]
                    self.record(kind, t0, now(), target=leader.name, leader_found=found,
                                down_seconds=round(delay, 1), healthy_again=ok, leaders_after=takeover)
            elif kind == "rabbitmq-stop":
                compose("stop", "rabbitmq", timeout=120)
                time.sleep(length)
                compose("start", "rabbitmq", timeout=120)
                ok = wait_until(lambda: rabbit_api("GET", "/api/overview")[0] == 200, 180, 2)
                self.record(kind, t0, now(), back=ok)
            elif kind == "postgres-pause":
                cid = container_id("postgres")
                subprocess.run(["docker", "pause", cid], check=True, capture_output=True)
                time.sleep(length)
                subprocess.run(["docker", "unpause", cid], check=True, capture_output=True)
                self.record(kind, t0, now())
            elif kind == "rolling-restart":
                with self.lifecycle:
                    results = {}
                    for app in self.apps:
                        results[app.name] = self.restart_app(app, 2, hard=False)
                        time.sleep(10)
                    self.record(kind, t0, now(), healthy_again=results)
        except Exception as e:
            self.record(kind, t0, now(), error=f"{type(e).__name__}: {e}")

    def run_schedule(self, schedule, load_start, stop_event):
        for offset, kind, length in schedule:
            while now() < load_start + offset:
                if stop_event.wait(min(1.0, load_start + offset - now())):
                    return
            t = threading.Thread(target=self.run_event, args=(kind, length), daemon=True)
            t.start()
            self.threads.append(t)

    def join(self, timeout=600):
        for t in self.threads:
            t.join(timeout)


# ---------------------------------------------------------------------------------------------
# Samplers: metrics, the audit queue, the cheap checks
# ---------------------------------------------------------------------------------------------


def spring_metric(app, name, tag=None):
    path = f"/actuator/metrics/{name}" + (f"?tag={urllib.parse.quote(tag)}" if tag else "")
    status, text = http_call(app.port, "GET", path, timeout=5)
    if status != 200:
        return None
    for m in json.loads(text).get("measurements", []):
        if m.get("statistic") in ("VALUE", "COUNT"):
            return m["value"]
    return None


def prometheus(app):
    status, text = http_call(app.port, "GET", "/q/metrics", timeout=10)
    values = collections.defaultdict(float)
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        m = re.match(r"([a-zA-Z_:][\w:]*)(\{[^}]*\})?\s+(\S+)", line)
        if m:
            try:
                values[(m.group(1), m.group(2) or "")] += float(m.group(3))
            except ValueError:
                pass
    return values


def prom_sum(values, name, label=None):
    return sum(v for (n, labels), v in values.items() if n == name and (label is None or label in labels))


def sample_app(app):
    s = {"app": app.name, "generation": app.generation}
    try:
        rss = subprocess.run(["ps", "-o", "rss=", "-p", str(app.proc.pid)], capture_output=True, text=True).stdout
        s["rss_mb"] = round(int(rss.strip()) / 1024, 1) if rss.strip() else None
    except Exception:
        pass
    if app.kind == "spring":
        s["heap_used_mb"] = (spring_metric(app, "jvm.memory.used", "area:heap") or 0) / 2**20
        s["gc_live_mb"] = (spring_metric(app, "jvm.gc.live.data.size") or 0) / 2**20
        s["threads"] = spring_metric(app, "jvm.threads.live")
        for pool in ("HikariPool-1", "streamrune-advisory-lock"):
            short = "app" if pool == "HikariPool-1" else "lock"
            for stat in ("active", "idle", "pending"):
                s[f"{short}_pool_{stat}"] = spring_metric(app, f"hikaricp.connections.{stat}", f"pool:{pool}")
        s["outbox_pending"] = spring_metric(app, "streamrune.outbox.pending")
        s["dlq_pending"] = spring_metric(app, "streamrune.dlq.pending")
        s["dlq_awaiting_operator"] = spring_metric(app, "streamrune.dlq.awaiting_operator")
        s["saga_faulted"] = spring_metric(app, "streamrune.saga.faulted_backlog")
    else:
        v = prometheus(app)
        s["heap_used_mb"] = prom_sum(v, "jvm_memory_used_bytes", 'area="heap"') / 2**20
        s["gc_live_mb"] = prom_sum(v, "jvm_gc_live_data_size_bytes") / 2**20
        s["threads"] = prom_sum(v, "jvm_threads_live_threads")
        for stat in ("active", "idle", "pending"):
            s[f"lock_pool_{stat}"] = prom_sum(v, f"hikaricp_connections_{stat}", "streamrune-advisory-lock")
        s["app_pool_active"] = prom_sum(v, "agroal_active_count") or None
        s["outbox_pending"] = prom_sum(v, "streamrune_outbox_pending")
        s["dlq_pending"] = prom_sum(v, "streamrune_dlq_pending")
        s["dlq_awaiting_operator"] = prom_sum(v, "streamrune_dlq_awaiting_operator")
        s["saga_faulted"] = prom_sum(v, "streamrune_saga_faulted_backlog")
    for k, val in list(s.items()):
        if isinstance(val, float):
            s[k] = round(val, 1)
    return s


class Samplers:
    def __init__(self, run, apps, driver, metrics_interval, check_interval):
        self.run, self.apps, self.driver = run, apps, driver
        self.metrics = JsonLog(run.dir / "metrics.jsonl")
        self.received = JsonLog(run.dir / "received.jsonl")
        self.checks = JsonLog(run.dir / "checks.jsonl")
        self.metrics_interval, self.check_interval = metrics_interval, check_interval
        self.stop = threading.Event()

    def sample_once(self, phase="load"):
        rec = {"t": now(), "phase": phase, "apps": []}
        for app in self.apps:
            if app.alive():
                try:
                    rec["apps"].append(sample_app(app))
                except Exception as e:
                    rec["apps"].append({"app": app.name, "error": f"{type(e).__name__}"})
        try:
            rec["db"] = sql_one("SELECT count(*) AS connections, count(*) FILTER (WHERE state = 'active') AS active,"
                                " count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_tx"
                                " FROM pg_stat_activity WHERE backend_type = 'client backend'", 20)
            rec["store"] = sql_one("SELECT (SELECT coalesce(max(global_offset), 0) FROM event_stream) AS head,"
                                   " (SELECT count(*) FROM outbox_events WHERE status IN ('PENDING','IN_PROGRESS')) AS outbox_open,"
                                   " (SELECT count(*) FROM outbox_events WHERE status = 'FAILED') AS outbox_failed,"
                                   " (SELECT count(*) FROM dead_letter_queue) AS dlq,"
                                   " (SELECT count(*) FROM saga_state WHERE status NOT IN ('COMPLETED','COMPENSATED','FAILED')) AS sagas_open,"
                                   " (SELECT count(*) FROM saga_state WHERE status = 'FAULTED') AS sagas_faulted", 20)
        except Exception as e:
            rec["db_error"] = f"{type(e).__name__}"
        if self.driver:
            rec["driver_backlog"] = self.driver.backlog()
            rec["driver_saturated"] = self.driver.saturated
        self.metrics.write(rec)

    def drain_audit_queue(self):
        try:
            while True:
                status, msgs = rabbit_api("POST", f"/api/queues/%2F/{AUDIT_QUEUE}/get",
                                          {"count": 2000, "ackmode": "ack_requeue_false",
                                           "encoding": "auto", "truncate": 10}, timeout=60)
                if status != 200 or not isinstance(msgs, list):
                    return
                t = now()
                for m in msgs:
                    props = m.get("properties") or {}
                    self.received.write({"t": t, "id": props.get("message_id"),
                                         "type": props.get("type"), "redelivered": m.get("redelivered")})
                if len(msgs) < 2000:
                    break
            # Nobody consumes the demo's own queue in a soak run (the notifications service is not
            # started); it is emptied so that it does not grow for hours.
            rabbit_api("DELETE", f"/api/queues/%2F/{INTEGRATION_QUEUE}/contents", timeout=30)
        except Exception:
            pass  # the broker is stopped: the next round reads what is queued

    def cheap_checks(self):
        r = self.run
        try:
            res = sql_one(f"""
              SELECT (SELECT count(*) FROM (SELECT aggregate_id FROM event_stream
                        WHERE event_type IN ('CustomerRegistered','ProductCreated','OrderPlaced')
                        GROUP BY aggregate_type, aggregate_id HAVING count(*) > 1) d) AS created_twice,
                     (SELECT count(*) FROM (SELECT aggregate_id FROM event_stream WHERE aggregate_type = 'payment'
                        GROUP BY aggregate_id HAVING count(*) FILTER (WHERE event_type = 'PaymentCaptured') > 1) d) AS captured_twice,
                     (SELECT count(*) FROM outbox_events WHERE status = 'FAILED') AS outbox_failed,
                     (SELECT count(*) FROM saga_state WHERE status = 'FAULTED') AS sagas_faulted,
                     (SELECT count(*) FROM saga_dead_letters) AS saga_dead_letters,
                     (SELECT count(*) FROM projection_dead_letters) AS projection_dead_letters,
                     (SELECT count(*) FROM dead_letter_queue) AS dlq""", 120)
            failing = {k: v for k, v in res.items() if k in ("created_twice", "captured_twice") and v}
            self.checks.write({"t": now(), "result": res, "ok": not failing})
        except Exception as e:
            self.checks.write({"t": now(), "error": f"{type(e).__name__}: {e}", "ok": None})

    def loop(self):
        next_metrics = next_check = now()
        next_drain = now() + 20
        while not self.stop.is_set():
            t = now()
            if t >= next_metrics:
                self.sample_once()
                next_metrics = t + self.metrics_interval
            if t >= next_drain:
                self.drain_audit_queue()
                next_drain = t + 20
            if t >= next_check:
                self.cheap_checks()
                next_check = t + self.check_interval
            self.stop.wait(1)

    def start(self):
        self.thread = threading.Thread(target=self.loop, daemon=True)
        self.thread.start()

    def halt(self):
        self.stop.set()
        self.thread.join(120)


# ---------------------------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------------------------


class Run:
    def __init__(self, directory, rid):
        self.dir, self.rid = Path(directory), rid


def drain(run, timeout, log):
    """Waits until the sagas of the run have ended, every subscription is at the head of the event
    store, the outbox is empty and every queued replay is resolved, or the timeout passes."""
    deadline = now() + timeout
    status = {}
    queued = queued_keys(run)
    while now() < deadline:
        try:
            status = sql_one(f"""
              SELECT (SELECT count(*) FROM saga_state WHERE saga_id LIKE 'fulfillment-soak-{run.rid}-%'
                        AND status NOT IN ('COMPLETED','COMPENSATED','FAILED')) AS sagas_open,
                     (SELECT count(*) FROM projection_offset WHERE last_offset
                        < (SELECT coalesce(max(global_offset), 0) FROM event_stream)) AS subscriptions_behind,
                     (SELECT count(*) FROM outbox_events WHERE status IN ('PENDING','IN_PROGRESS')) AS outbox_open""")
            open_keys = 0
            if queued:
                inbox = {r["k"] for r in sql_rows("SELECT idempotency_key AS k FROM command_inbox WHERE idempotency_key IN ("
                                                  + ",".join("'" + ADMIN + "|" + k + "'" for k in queued) + ")")}
                open_keys = sum(1 for k in queued if f"{ADMIN}|{k}" not in inbox)
            status["queued_replays_open"] = open_keys
            log.write({"t": now(), "drain": status})
            if not any(status.values()):
                return True, status
        except Exception as e:
            log.write({"t": now(), "drain_error": f"{type(e).__name__}: {e}"})
        time.sleep(5)
    return False, status


def queued_keys(run):
    return sorted({r["key"] for r in read_jsonl(run.dir / "requests.jsonl")
                   if r.get("reason") == "QUEUED_FOR_REPLAY" and r.get("key")})


def run_soak(args):
    rid = datetime.datetime.now().strftime("%m%d%H%M")
    run = Run(Path(args.out) / f"run-{rid}" if args.out else ROOT / "build" / "soak" / f"run-{rid}", rid)
    (run.dir / "logs").mkdir(parents=True, exist_ok=True)
    smoke = args.duration < 1800
    meta = {"rid": rid, "duration": args.duration, "rate": args.rate, "smoke": smoke,
            "started": now(), "quarkus_pool_size": args.quarkus_pool_size, "heap": args.heap}
    (run.dir / "run.json").write_text(json.dumps(meta, indent=2))
    print(f"Soak run {rid}: {args.duration} s of load at {args.rate} commands/s, files in {run.dir}")

    for jar in ("spring-app/build/libs/spring-app-0.1.0-SNAPSHOT.jar", "quarkus-app/build/quarkus-app/quarkus-run.jar"):
        if not (ROOT / jar).exists():
            print(f"✗ {jar} is missing: ./gradlew :spring-app:bootJar :quarkus-app:quarkusBuild", file=sys.stderr)
            return 2
    for port in (8080, 8091, 8082):
        with socket.socket() as s:
            if s.connect_ex(("localhost", port)) == 0:
                print(f"✗ port {port} is taken; stop what listens there first", file=sys.stderr)
                return 2

    events = JsonLog(run.dir / "lifecycle.jsonl")
    print("Starting PostgreSQL and RabbitMQ …")
    start_infrastructure(fresh=not args.reuse_db)
    apps = [App("spring-1", "spring", 8080, run.dir, events, args),
            App("spring-2", "spring", 8091, run.dir, events, args),
            App("quarkus", "quarkus", 8082, run.dir, events, args)]
    try:
        # One at a time: the first applies the schema migrations on the empty database.
        for app in apps:
            app.start()
            if not app.wait_healthy():
                print(f"✗ {app.name} did not become healthy; see {run.dir}/logs", file=sys.stderr)
                return 2
        print("Backends up: " + ", ".join(f"{a.name}:{a.port} ({a.saga_role()})" for a in apps))

        driver = Driver(run, apps, args.rate, args.workers)
        samplers = Samplers(run, apps, driver, 15 if smoke else 60, 120 if smoke else 600)
        samplers.sample_once("start")
        samplers.start()
        driver.start(args.query_rate, args.sse_readers)
        load_start = now()
        meta["load_start"] = load_start
        (run.dir / "run.json").write_text(json.dumps(meta, indent=2))
        chaos = Chaos(run, apps)
        schedule = [] if args.no_chaos else build_schedule(args.duration, smoke)
        (run.dir / "schedule.json").write_text(json.dumps(schedule))
        chaos_stop = threading.Event()
        chaos_thread = threading.Thread(target=chaos.run_schedule, args=(schedule, load_start, chaos_stop), daemon=True)
        chaos_thread.start()
        print(f"Load running; {len(schedule)} failures scheduled.")
        while now() < load_start + args.duration:
            time.sleep(min(30, load_start + args.duration - now()))
            sent = driver.counter
            print(f"  {iso(now())}  {int(now() - load_start)} s, ids {dict(sent)}, backlog {driver.backlog()}", flush=True)
        chaos_stop.set()
        chaos_thread.join(10)
        chaos.join()
        meta["load_end"] = now()
        left = driver.finish(follow_up_grace=120)
        meta["follow_ups_abandoned"] = left
        chaos.set_flag("outage", False)
        chaos.set_flag("decline", False)
        for app in apps:
            if not app.alive():
                app.start()
                app.wait_healthy()
        print(f"Load stopped; draining (at most {args.drain_timeout} s) …")
        drained, drain_status = drain(run, args.drain_timeout, events)
        meta["drained"], meta["drain_status"], meta["drain_end"] = drained, drain_status, now()
        samplers.sample_once("end")
        samplers.drain_audit_queue()
        time.sleep(25)  # the last publishes of the relays
        samplers.drain_audit_queue()
        samplers.halt()
        (run.dir / "run.json").write_text(json.dumps(meta, indent=2))
        result = check(run, stop_apps=apps)
    finally:
        for app in apps:
            app.stop()
        if not args.keep_stack:
            compose("down", "-v", timeout=180, check=False)
    return result


# ---------------------------------------------------------------------------------------------
# Invariants
# ---------------------------------------------------------------------------------------------


class Report:
    def __init__(self):
        self.results = []  # (name, ok, summary, details)

    def add(self, name, ok, summary, details=None):
        self.results.append({"name": name, "ok": ok, "summary": summary, "details": details or []})
        mark = "✓" if ok else ("✗" if ok is False else "·")
        print(f"{mark} {name}: {summary}")
        for d in (details or [])[:10]:
            print(f"    {d}")


def windows(chaos, before, after):
    return [(c["start"] - before, c["end"] + after, c["kind"]) for c in chaos]


def in_window(t, wins):
    return next((k for s, e, k in wins if s <= t <= e), None)


def lifecycle_windows(run):
    """Every start of a backend after the first as a window: its startup lines belong to the
    failure or restart that caused it."""
    wins, starts = [], collections.Counter()
    for e in read_jsonl(run.dir / "lifecycle.jsonl"):
        if e.get("event") == "start":
            starts[e["app"]] += 1
            if starts[e["app"]] > 1:
                wins.append((e["t"] - 5, e["t"] + 120, "restart"))
    return wins


LOG_TS = [
    (re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+[+-]\d\d:\d\d)\s+ERROR\b"), "%Y-%m-%dT%H:%M:%S.%f%z"),
    (re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d+)\s+ERROR\b"), "%Y-%m-%d %H:%M:%S,%f"),
]


def scan_logs(run, wins):
    expected, unexpected = [], []
    for path in sorted((run.dir / "logs").glob("*.log")):
        with open(path, errors="replace") as f:
            for line in f:
                for rx, fmt in LOG_TS:
                    m = rx.match(line)
                    if m:
                        ts = datetime.datetime.strptime(m.group(1), fmt)
                        if ts.tzinfo is None:
                            ts = ts.astimezone()
                        kind = in_window(ts.timestamp(), wins)
                        entry = {"file": path.name, "t": ts.timestamp(), "window": kind, "line": line.strip()[:400]}
                        (expected if kind else unexpected).append(entry)
                        break
    return expected, unexpected


def error_signature(line):
    sig = re.sub(r"^\S+( \S+)?\s+ERROR\s+(\d+ --- )?", "", line)
    sig = re.sub(r"soak-\d+-[a-z]\d+|pay-soak-\S+|[0-9a-f]{8}-[0-9a-f-]{27}|\b\d+\b", "#", sig)
    return sig[:200]


def trend(samples, key):
    """(first, middle, last) and whether the series grows steadily: every third above the one
    before by more than 20 % and the last third's minimum above the first third's maximum."""
    vals = [s for s in samples if s is not None]
    if len(vals) < 9:
        return vals[:1] + vals[len(vals) // 2:len(vals) // 2 + 1] + vals[-1:], False
    n = len(vals) // 3
    a, b, c = vals[:n], vals[n:2 * n], vals[2 * n:]
    ma, mb, mc = statistics.median(a), statistics.median(b), statistics.median(c)
    growing = mb > ma * 1.2 and mc > mb * 1.2 and min(c) > max(a)
    return (vals[0], vals[len(vals) // 2], vals[-1]), growing


def check(run, stop_apps=None):
    """Every invariant over the files of the run and the database; returns the exit status."""
    rep = Report()
    meta = json.loads((run.dir / "run.json").read_text())
    rid = meta["rid"]
    reqs = read_jsonl(run.dir / "requests.jsonl")
    chaos = read_jsonl(run.dir / "chaos.jsonl")
    cmds = [r for r in reqs if r.get("method")]
    load_s = max(1, meta.get("load_end", now()) - meta.get("load_start", meta["started"]))
    ok2 = lambda r: 200 <= r["status"] < 300
    by_status = collections.Counter(str(r["status"]) + (f" {r['reason']}" if r.get("reason") else "") for r in cmds)
    resends = sum(1 for r in cmds if r.get("resend"))
    rep.add("load", None, f"{len(cmds)} commands in {int(load_s)} s = {len(cmds) / load_s:.1f}/s, "
            f"{resends} re-sends under the same key; statuses {dict(by_status.most_common())}")
    lat = collections.defaultdict(list)
    for r in cmds:
        if ok2(r):
            lat[r["op"]].append(r["ms"])
    lat_lines = []
    for op, xs in sorted(lat.items()):
        xs.sort()
        q = lambda p: xs[min(len(xs) - 1, int(p * len(xs)))]
        lat_lines.append(f"{op}: n={len(xs)} p50={q(.5):.0f} ms p95={q(.95):.0f} ms p99={q(.99):.0f} ms max={xs[-1]:.0f} ms")
    rep.add("latency (2xx)", None, f"{len(lat)} operations", lat_lines)
    queries = read_jsonl(run.dir / "queries.jsonl")
    sse = read_jsonl(run.dir / "sse.jsonl")
    rep.add("readers", None, f"{len(queries)} queries ({dict(collections.Counter(q['status'] for q in queries))}), "
            f"{len(sse)} SSE streams, {sum(s['frames'] for s in sse)} frames, ends "
            f"{dict(collections.Counter(s['end'] for s in sse))}")
    rep.add("chaos", None, f"{len(chaos)} failures: {dict(collections.Counter(c['kind'] for c in chaos))}",
            [f"{iso(c['start'])} +{c['end'] - c['start']:.0f}s {c['kind']} "
             + json.dumps({k: v for k, v in c.items() if k not in ('kind', 'start', 'end')}) for c in chaos])
    chaos_wins = windows([c for c in chaos if c["kind"] != "payment-decline"], 5, 120) + lifecycle_windows(run)

    # 1. Creations applied exactly once.
    created = {(r["aggregate_type"], r["aggregate_id"]): r["n"] for r in sql_rows(f"""
        SELECT aggregate_type, aggregate_id, count(*) AS n FROM event_stream
         WHERE event_type IN ('CustomerRegistered','ProductCreated','OrderPlaced')
           AND aggregate_id LIKE 'soak-{rid}-%' GROUP BY 1, 2""")}
    agg_of = {"register_customer": "customer", "create_product": "product", "place_order": "order"}
    per_id = collections.defaultdict(list)
    for r in cmds:
        if r["op"] in agg_of:
            per_id[(agg_of[r["op"]], r["id"])].append(r)
    bad = []
    for (agg, eid), rs in per_id.items():
        n = created.get((agg, eid), 0)
        if any(ok2(r) for r in rs) and n != 1:
            bad.append(f"{agg} {eid}: answered 2xx, created {n} times")
        elif n > 1:
            bad.append(f"{agg} {eid}: created {n} times")
        elif n == 1 and not any(ok2(r) for r in rs):
            pass  # applied although the answer was lost (timeout, reset): allowed, at most once
    unknown = sum(1 for (agg, eid), rs in per_id.items() if not any(ok2(r) for r in rs) and created.get((agg, eid)))
    rep.add("creations applied exactly once", not bad,
            f"{len(per_id)} ids, {sum(1 for rs in per_id.values() if any(ok2(r) for r in rs))} answered 2xx; "
            f"{unknown} applied without a 2xx answer (lost answer); {len(bad)} violations", bad)

    # 2. Keyed commands: once per key, nothing applied outside the inbox.
    keyed_ops = {"receive_shipment": ("inventory", "ShipmentReceived", "ReceiveShipment"),
                 "adjust_stock": ("product", "StockAdjusted", "AdjustStock")}
    inbox = {r["k"][len(ADMIN) + 1:]: r for r in sql_rows(f"""
        SELECT idempotency_key AS k, command_type, aggregate_id, global_offsets FROM command_inbox
         WHERE idempotency_key LIKE '{ADMIN}|%'""")}
    events_per = {(r["aggregate_type"], r["aggregate_id"], r["event_type"]): r["n"] for r in sql_rows(f"""
        SELECT aggregate_type, aggregate_id, event_type, count(*) AS n FROM event_stream
         WHERE event_type IN ('ShipmentReceived','StockAdjusted') AND aggregate_id LIKE 'soak-{rid}-%'
         GROUP BY 1, 2, 3""")}
    bad = []
    keys = collections.defaultdict(list)
    for r in cmds:
        if r["op"] in keyed_ops and r.get("key"):
            keys[r["key"]].append(r)
    for k, rs in keys.items():
        if any(ok2(r) for r in rs) and k not in inbox:
            bad.append(f"key {k} ({rs[0]['op']} {rs[0]['id']}): answered 2xx, not in the inbox")
        statuses = {r["status"] for r in rs if r["status"]}
        if len(rs) > 1 and any(ok2(r) for r in rs) and any(s >= 400 and s not in (500, 503) for s in statuses):
            bad.append(f"key {k}: the same key answered {sorted(statuses)}")
    inbox_count = collections.Counter()
    for k, row in inbox.items():
        if k in keys:
            op = keys[k][0]["op"]
            agg, ev, _ = keyed_ops[op]
            inbox_count[(agg, row["aggregate_id"], ev)] += 1
    for (agg, aid, ev), n in events_per.items():
        if inbox_count[(agg, aid, ev)] != n:
            bad.append(f"{agg} {aid}: {n} {ev} events, {inbox_count[(agg, aid, ev)]} inbox entries")
    rep.add("keyed commands applied once per key", not bad,
            f"{len(keys)} keys ({sum(1 for rs in keys.values() if len(rs) > 1)} sent more than once), "
            f"{sum(1 for k in keys if k in inbox)} in the inbox; {len(bad)} violations", bad)

    # 3. 503 QUEUED_FOR_REPLAY resolved; 4. no 500 without a dead-letter entry.
    dlq = {r["command_id"]: r for r in sql_rows("""
        SELECT command_id, command_type, aggregate_id, error_type, left(error_message, 200) AS error_message,
               dlq_attempts, idempotency_key, extract(epoch FROM published_at) AS published,
               extract(epoch FROM first_attempt_at) AS first_attempt FROM dead_letter_queue""")}
    queued = [r for r in cmds if r.get("reason") == "QUEUED_FOR_REPLAY"]
    bad, awaiting = [], []
    for r in queued:
        if r.get("key") in inbox:
            continue
        if r.get("commandId") in dlq:
            awaiting.append(f"{r['op']} {r['id']} key {r['key']}: still in the dead-letter queue")
        else:
            bad.append(f"{r['op']} {r['id']} key {r['key']}: neither applied nor in the dead-letter queue")
    rep.add("503 QUEUED_FOR_REPLAY applied or awaiting an operator", not bad,
            f"{len(queued)} answers, {len(queued) - len(bad) - len(awaiting)} applied, {len(awaiting)} still queued; "
            f"{len(bad)} lost", bad + awaiting)
    fives = [r for r in cmds if r["status"] == 500]
    bad = [f"{iso(r['t'])} {r['op']} {r['id']} on {r['app']}: {r.get('body', '')[:160]}"
           for r in fives if not (r.get("reason") == "AWAITING_OPERATOR" and r.get("commandId") in dlq)]
    rep.add("no 500 without a dead-letter entry", not bad,
            f"{len(fives)} answers 500, {len(fives) - len(bad)} with an entry awaiting an operator", bad)

    # 5. Sagas terminal, none stuck; 6. no healthy flow compensated.
    sagas = {r["saga_id"][len("fulfillment-"):]: r for r in sql_rows(f"""
        SELECT saga_id, status, extract(epoch FROM created_at) AS created, extract(epoch FROM updated_at) AS updated,
               episode_cause_event_id IS NULL AND status = 'COMPENSATED' AS by_deadline
          FROM saga_state WHERE saga_id LIKE 'fulfillment-soak-{rid}-%'""")}
    open_sagas = [f"{k}: {v['status']}" for k, v in sagas.items() if v["status"] not in TERMINAL_SAGA]
    rep.add("every saga terminal", not open_sagas,
            f"{len(sagas)} sagas: {dict(collections.Counter(v['status'] for v in sagas.values()))}", open_sagas)
    orders_ok = {}
    for r in cmds:
        if r["op"] == "place_order" and ok2(r):
            orders_ok.setdefault(r["id"], r)
    declined = {r["order_id"]: r["t"] for r in sql_rows(f"""
        SELECT payload->>'orderId' AS order_id, extract(epoch FROM created_at) AS t FROM event_stream
         WHERE aggregate_type = 'payment' AND event_type = 'PaymentFailed' AND aggregate_id LIKE 'pay-soak-{rid}-%'""")}
    decline_wins = windows([c for c in chaos if c["kind"] == "payment-decline"], 2, 15)
    bad, explained = [], collections.Counter()
    for oid, r in orders_ok.items():
        s = sagas.get(oid)
        if s is None:
            bad.append(f"{oid}: placed (2xx) but no saga")
            continue
        if r["intent"] == "happy" and s["status"] != "COMPLETED":
            if oid in declined and in_window(declined[oid], decline_wins):
                explained["declined in a decline window"] += 1
            else:
                cause = "deadline" if s["by_deadline"] else ("payment failed outside a decline window" if oid in declined else "unknown")
                bad.append(f"{oid}: healthy flow ended {s['status']} ({cause}); placed {iso(r['t'])} "
                           f"in window {in_window(r['t'], chaos_wins + decline_wins)}")
        if (r["intent"] == "oos" and s["status"] != "COMPENSATED"
                and not (oid in declined and in_window(declined[oid], decline_wins))):
            bad.append(f"{oid}: out-of-stock order ended {s['status']}")
    rep.add("no healthy flow compensated", not bad,
            f"{len(orders_ok)} orders; {sum(1 for o, r in orders_ok.items() if r['intent'] == 'happy')} healthy; "
            f"explained {dict(explained)}; {len(bad)} unexplained", bad)

    # 7. Money and stock.
    out = psql((ROOT / "scripts" / "money-and-stock-invariants.sql").read_text(), 600)
    viol = [l for l in out.splitlines() if f"soak-{rid}-" in l]
    rep.add("money and stock consistent (scripts/money-and-stock-invariants.sql)", not viol,
            f"{len(viol)} violations", viol)

    # 8. Outbox drained, every entry published, duplicates explained.
    ob = sql_one("""SELECT count(*) FILTER (WHERE status = 'DELIVERED') AS delivered,
                           count(*) FILTER (WHERE status IN ('PENDING','IN_PROGRESS')) AS open,
                           count(*) FILTER (WHERE status = 'FAILED') AS failed,
                           count(*) FILTER (WHERE status = 'SKIPPED') AS skipped FROM outbox_events""")
    delivered = {r["entry_id"] for r in sql_rows("SELECT entry_id FROM outbox_events WHERE status = 'DELIVERED'")}
    received = read_jsonl(run.dir / "received.jsonl")
    seen = collections.defaultdict(list)
    for m in received:
        seen[m["id"]].append(m["t"])
    missing = sorted(delivered - set(seen))
    dupes = {k: v for k, v in seen.items() if len(v) > 1}
    dup_wins = windows([c for c in chaos if c["kind"] != "payment-decline"], 30, 300) + lifecycle_windows(run)
    unexplained = [f"{k}: received {len(v)} times, at {', '.join(iso(t) for t in v)}" for k, v in dupes.items()
                   if not any(in_window(t, dup_wins) for t in v)]
    bad = []
    if ob["open"] or ob["failed"]:
        bad.append(f"outbox not drained: {ob}")
    bad += [f"{k}: DELIVERED, never received" for k in missing[:50]]
    bad += unexplained
    rep.add("outbox drained and every entry published once", not bad,
            f"outbox {ob}; {len(received)} messages received, {len(seen)} distinct, {len(missing)} delivered "
            f"entries never received, {len(dupes)} received more than once ({len(unexplained)} outside a failure window)", bad)

    # 9. Projections equal to a rebuild from the event store.
    behind = sql_rows("SELECT projection_name, last_offset FROM projection_offset WHERE last_offset "
                      "< (SELECT coalesce(max(global_offset), 0) FROM event_stream)")
    diffs = sql_rows("""
      WITH st AS (SELECT DISTINCT ON (aggregate_id) aggregate_id AS id,
                    CASE event_type WHEN 'OrderPlaced' THEN 'CREATED' WHEN 'OrderConfirmed' THEN 'CONFIRMED'
                      WHEN 'OrderShipped' THEN 'SHIPPED' WHEN 'OrderDelivered' THEN 'DELIVERED'
                      WHEN 'OrderCancelled' THEN 'CANCELLED' END AS status
                    FROM event_stream WHERE aggregate_type = 'order' ORDER BY aggregate_id, global_offset DESC),
           pay AS (SELECT DISTINCT ON (payload->>'orderId') payload->>'orderId' AS id,
                    CASE event_type WHEN 'PaymentInitiated' THEN 'PENDING' WHEN 'PaymentCaptured' THEN 'CAPTURED'
                      WHEN 'PaymentFailed' THEN 'FAILED' WHEN 'PaymentVoided' THEN 'VOIDED'
                      WHEN 'PaymentRefunded' THEN 'REFUNDED' END AS ps
                    FROM event_stream WHERE aggregate_type = 'payment' ORDER BY payload->>'orderId', global_offset DESC)
      SELECT 'orders' AS view, st.id, st.status || '/' || coalesce(pay.ps, '-') AS expected,
             coalesce(v.data->>'status', 'missing') || '/' || coalesce(v.data->>'paymentStatus', '-') AS actual
        FROM st LEFT JOIN pay ON pay.id = st.id LEFT JOIN orders_view v ON v.id = st.id
       WHERE v.id IS NULL OR v.data->>'status' IS DISTINCT FROM st.status
          OR v.data->>'paymentStatus' IS DISTINCT FROM pay.ps
      UNION ALL
      SELECT 'inventory', e.id, e.available || '/' || e.reserved || '/' || e.committed,
             coalesce(v.data->>'available', 'missing') || '/' || coalesce(v.data->>'reserved', '-') || '/' || coalesce(v.data->>'committed', '-')
        FROM (SELECT aggregate_id AS id,
                     (array_agg((payload->>'availableAfter')::int ORDER BY global_offset DESC)
                        FILTER (WHERE event_type <> 'ReservationConfirmed'))[1] AS available,
                     coalesce(sum((payload->>'quantity')::int) FILTER (WHERE event_type = 'StockReserved'), 0)
                       - coalesce(sum((payload->>'quantity')::int) FILTER (WHERE event_type IN ('StockReleased','ReservationConfirmed')), 0) AS reserved,
                     coalesce(sum((payload->>'quantity')::int) FILTER (WHERE event_type = 'ReservationConfirmed'), 0) AS committed
                FROM event_stream WHERE aggregate_type = 'inventory' GROUP BY aggregate_id) e
        LEFT JOIN inventory_view v ON v.id = e.id
       WHERE v.id IS NULL OR (v.data->>'available')::int <> e.available OR (v.data->>'reserved')::int <> e.reserved
          OR (v.data->>'committed')::int <> e.committed
      UNION ALL
      SELECT 'products', e.id, e.stock || '/' || e.price, coalesce(v.data->>'stock', 'missing') || '/' || coalesce(v.data->'price'->>'amount', '-')
        FROM (SELECT aggregate_id AS id,
                     (array_agg(coalesce(payload->>'newStock', payload->>'stock')::int ORDER BY global_offset DESC)
                        FILTER (WHERE event_type IN ('ProductCreated','StockAdjusted')))[1] AS stock,
                     (array_agg(coalesce(payload->'newPrice'->>'amount', payload->'price'->>'amount')::numeric ORDER BY global_offset DESC)
                        FILTER (WHERE event_type IN ('ProductCreated','PriceUpdated')))[1] AS price
                FROM event_stream WHERE aggregate_type = 'product' GROUP BY aggregate_id) e
        LEFT JOIN products_view v ON v.id = e.id
       WHERE v.id IS NULL OR (v.data->>'stock')::int <> e.stock OR (v.data->'price'->>'amount')::numeric <> e.price
      UNION ALL
      SELECT 'customers', c.aggregate_id, 'present', 'missing'
        FROM (SELECT aggregate_id FROM event_stream WHERE aggregate_type = 'customer' GROUP BY aggregate_id
               HAVING NOT bool_or(event_type = 'CustomerForgotten')) c
        LEFT JOIN customers_view v ON v.id = c.aggregate_id WHERE v.id IS NULL""")
    counts = sql_one("SELECT (SELECT count(*) FROM orders_view) AS orders, (SELECT count(*) FROM inventory_view) AS inventory,"
                     " (SELECT count(*) FROM products_view) AS products, (SELECT count(*) FROM customers_view) AS customers")
    bad = [f"subscription {b['projection_name']} at {b['last_offset']}, behind the head" for b in behind]
    bad += [f"{d['view']} {d['id']}: event store {d['expected']}, read model {d['actual']}" for d in diffs]
    rep.add("projections equal to a rebuild from the event store", not bad,
            f"rows {counts}; {len(diffs)} differences, {len(behind)} subscriptions behind", bad)

    # 10. Dead letters explained by a failure window.
    dl_wins = windows([c for c in chaos if c["kind"] != "payment-decline"], 10, 60) + lifecycle_windows(run)
    bad, expl = [], collections.Counter()
    for cid, e in dlq.items():
        kind = in_window(e["first_attempt"], dl_wins) or in_window(e["published"], dl_wins)
        if kind:
            expl[kind] += 1
        else:
            bad.append(f"{cid} {e['command_type'].rsplit('$', 1)[-1]} {e['aggregate_id']} at {iso(e['published'])}: "
                       f"{e['error_type']} {e['error_message']}")
    other = sql_rows("""SELECT 'saga' AS kind, saga_id AS id, event_type, error_type, left(error_message, 160) AS msg,
                               extract(epoch FROM first_faulted_at) AS t FROM saga_dead_letters
                        UNION ALL SELECT 'projection', projection_name, from_offset::text, error_type,
                               left(error_message, 160), extract(epoch FROM failed_at) FROM projection_dead_letters""")
    for o in other:
        kind = in_window(o["t"], dl_wins)
        if kind:
            expl[f"{o['kind']} dead letter in {kind}"] += 1
        else:
            bad.append(f"{o['kind']} dead letter {o['id']} {o['event_type']} at {iso(o['t'])}: {o['error_type']} {o['msg']}")
    rep.add("every dead letter explained by a failure window", not bad,
            f"{len(dlq)} command dead letters, {len(other)} saga/projection dead letters; explained {dict(expl)}", bad)

    # 11. Resource trend: the samples of the load after its warm-up, outside every failure window
    # (an outage fills the outbox and the pools on purpose).
    load_start = meta.get("load_start", meta["started"])
    warm_up = load_start + min(600, 0.2 * load_s)
    quiet = windows(chaos, 30, 180) + lifecycle_windows(run)
    metrics = [m for m in read_jsonl(run.dir / "metrics.jsonl")
               if m.get("phase") == "load" and warm_up <= m["t"] <= meta.get("load_end", now())
               and not in_window(m["t"], quiet)]
    lines, grow = [], []
    per_app = collections.defaultdict(list)
    for m in metrics:
        for a in m.get("apps", []):
            per_app[(a["app"], a.get("generation"))].append(a)
    for (app, gen), samples in sorted(per_app.items(), key=lambda x: (x[0][0], x[0][1] or 0)):
        if len(samples) < 3:
            continue
        for key in ("gc_live_mb", "heap_used_mb", "rss_mb", "threads", "app_pool_active", "lock_pool_active",
                    "lock_pool_pending", "outbox_pending", "dlq_pending", "saga_faulted"):
            series = [s.get(key) for s in samples]
            if all(v is None for v in series):
                continue
            (vals, growing) = trend(series, key)
            lines.append(f"{app}#{gen} {key}: start/mid/end {vals} over {len(samples)} samples" + (" GROWING" if growing else ""))
            if growing and key in ("gc_live_mb", "threads", "rss_mb", "outbox_pending", "dlq_pending", "saga_faulted"):
                grow.append(f"{app}#{gen} {key} grows steadily: {vals}")
    for key in ("connections", "idle_in_tx"):
        (vals, growing) = trend([m.get("db", {}).get(key) for m in metrics], key)
        lines.append(f"postgres {key}: start/mid/end {vals}" + (" GROWING" if growing else ""))
        if growing:
            grow.append(f"postgres {key} grows steadily: {vals}")
    for key in ("outbox_open", "dlq", "sagas_open"):
        (vals, growing) = trend([m.get("store", {}).get(key) for m in metrics], key)
        lines.append(f"store {key}: start/mid/end {vals}" + (" GROWING" if growing else ""))
        if growing:
            grow.append(f"store {key} grows steadily: {vals}")
    rep.add("no steady growth of a resource", not grow, f"{len(metrics)} samples; {len(grow)} growing", grow + lines)

    # 12. Log scan.
    if stop_apps:
        for app in stop_apps:
            app.stop()
    log_wins = windows(chaos, 5, 120) + lifecycle_windows(run)
    expected, unexpected = scan_logs(run, log_wins)
    sigs = collections.Counter(error_signature(e["line"]) for e in unexpected)
    rep.add("every ERROR line inside a failure window", not unexpected,
            f"{len(expected) + len(unexpected)} ERROR lines, {len(expected)} inside a failure window, "
            f"{len(unexpected)} outside ({len(sigs)} kinds)",
            [f"{n}× {s}" for s, n in sigs.most_common(20)] +
            [f"{e['file']} {iso(e['t'])}: {e['line'][:300]}" for e in unexpected[:20]])
    exp_sigs = collections.Counter(error_signature(e["line"]) for e in expected)
    rep.add("ERROR lines inside failure windows (expected)", None, f"{len(exp_sigs)} kinds",
            [f"{n}× {s}" for s, n in exp_sigs.most_common(25)])

    # 13. The periodic checks.
    periodic = read_jsonl(run.dir / "checks.jsonl")
    failed = [f"{iso(c['t'])}: {c.get('result') or c.get('error')}" for c in periodic if c.get("ok") is False]
    rep.add("periodic checks during the load", not failed, f"{len(periodic)} rounds, {len(failed)} failing",
            failed + [f"last: {periodic[-1].get('result')}"] if periodic else failed)

    verdicts = [r for r in rep.results if r["ok"] is not None]
    passed = sum(1 for r in verdicts if r["ok"])
    summary = {"rid": rid, "passed": passed, "failed": len(verdicts) - passed, "results": rep.results,
               "meta": meta}
    (run.dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print(f"\nInvariants: {passed} passed, {len(verdicts) - passed} failed. Files: {run.dir}")
    return 0 if passed == len(verdicts) else 1


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--duration", type=parse_duration, default=parse_duration("5m"),
                   help="how long the load runs (90s, 5m, 2h); below 30m the failures come on a compressed schedule")
    p.add_argument("--rate", type=float, default=30, help="commands per second (default 30)")
    p.add_argument("--workers", type=int, default=32, help="driver threads sending commands")
    p.add_argument("--query-rate", type=float, default=10, help="queries per second beside the commands")
    p.add_argument("--sse-readers", type=int, default=3, help="live streams held open at a time")
    p.add_argument("--drain-timeout", type=parse_duration, default=parse_duration("10m"))
    p.add_argument("--heap", default="768m", help="-Xmx of each backend")
    p.add_argument("--quarkus-pool-size", type=int, default=20,
                   help="quarkus.datasource.jdbc.max-size; 0 keeps Quarkus' default (docs/soak.md)")
    p.add_argument("--no-chaos", action="store_true", help="load only, no failures")
    p.add_argument("--reuse-db", action="store_true", help="keep the database of an earlier run")
    p.add_argument("--keep-stack", action="store_true", help="leave PostgreSQL and RabbitMQ running")
    p.add_argument("--out", help="directory for the run's files (default build/soak)")
    p.add_argument("command", nargs="?", default="run", choices=("run", "check"))
    p.add_argument("run_dir", nargs="?", help="with check: the directory of a run")
    args = p.parse_args()
    if args.command == "check":
        if not args.run_dir:
            p.error("check needs the directory of a run")
        meta = json.loads((Path(args.run_dir) / "run.json").read_text())
        return check(Run(args.run_dir, meta["rid"]))
    return run_soak(args)


if __name__ == "__main__":
    sys.exit(main())
