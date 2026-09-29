#!/usr/bin/env python3
"""pool_server.py — THE POOL head node.

The brain of the fleet. Holds the job queue and the worker registry in
SQLite, matches jobs to workers by capability labels, and requeues work
when a worker dies. Workers talk to it over plain HTTP (reach it over
Tailscale; set POOL_TOKEN so strangers can't submit jobs).

Zero dependencies: stdlib only (http.server, sqlite3, threading, json).

Job states: queued -> claimed -> running -> done | failed | cancelled
A worker claims a job (atomic lease), then reports start/complete/fail.
If the lease expires or the worker's heartbeat stops, the job goes back
to queued. A job is never executed by two workers at once: the claim is
a single atomic UPDATE ... WHERE state='queued'.

Environment:
  POOL_PORT        listen port (default 8765)
  POOL_DB          sqlite path (default ./pool.db next to this file)
  POOL_TOKEN       shared secret; required on every call when set.
                   Unset = open server (local testing only; it warns loudly)
  POOL_LEASE_S     claim lease seconds (default 300)
  POOL_DEAD_S      worker considered dead after this many seconds
                   without heartbeat (default 90)
  POOL_SWEEP_S     sweeper interval seconds (default 15)
  POOL_ARTIFACT_DIR where returned artifacts land (default ./artifacts)
  POOL_MAX_ARTIFACT_BYTES max artifact payload per job (default 10MB)

Usage:
  python3 pool_server.py            # foreground
  python3 pool_server.py --check    # verify config + schema, change nothing
"""

import hashlib
import json
import os
import re
import secrets
import sqlite3
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.environ.get("POOL_PORT", "8765"))
DB_PATH = os.environ.get("POOL_DB", os.path.join(HERE, "pool.db"))
TOKEN = os.environ.get("POOL_TOKEN", "")
LEASE_S = int(os.environ.get("POOL_LEASE_S", "300"))
DEAD_S = int(os.environ.get("POOL_DEAD_S", "90"))
SWEEP_S = int(os.environ.get("POOL_SWEEP_S", "15"))
ARTIFACT_DIR = os.environ.get("POOL_ARTIFACT_DIR", os.path.join(HERE, "artifacts"))
MAX_ARTIFACT_BYTES = int(os.environ.get("POOL_MAX_ARTIFACT_BYTES", str(10 * 1024 * 1024)))
MAX_BODY_BYTES = 32 * 1024 * 1024

TERMINAL = ("done", "failed", "cancelled")

# Job ids are server-minted ("job_" + hex). Anything else in a URL slot is
# rejected before it can touch the filesystem (artifact path traversal).
_SAFE_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id            TEXT PRIMARY KEY,
  name          TEXT NOT NULL,
  command       TEXT NOT NULL,
  env_json      TEXT NOT NULL DEFAULT '{}',
  needs_json    TEXT NOT NULL DEFAULT '{}',
  idem_key      TEXT,
  state         TEXT NOT NULL DEFAULT 'queued',
  worker_id     TEXT,
  attempts      INTEGER NOT NULL DEFAULT 0,
  max_attempts  INTEGER NOT NULL DEFAULT 3,
  not_before    REAL NOT NULL DEFAULT 0,
  lease_expires REAL,
  result_json   TEXT,
  created_at    REAL NOT NULL,
  updated_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state, not_before);
-- Idempotency keys must be unique so concurrent submitters can't double-book
-- the same job. SQLite unique indexes allow multiple NULLs, so keyless jobs
-- are unaffected.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idem ON jobs(idem_key);
CREATE TABLE IF NOT EXISTS workers (
  id             TEXT PRIMARY KEY,
  hostname       TEXT NOT NULL,
  caps_json      TEXT NOT NULL DEFAULT '{}',
  labels_json    TEXT NOT NULL DEFAULT '[]',
  state          TEXT NOT NULL DEFAULT 'alive',
  registered_at  REAL NOT NULL,
  last_heartbeat REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workers_state ON workers(state);
"""


def now():
    return time.time()


class Store:
    """Single SQLite connection guarded by a lock. All state changes go here."""

    def __init__(self, path):
        # RLock: sweep() reuses requeue() while holding the lock.
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        with self._lock:
            self.db.execute("PRAGMA journal_mode=WAL;")
            self.db.executescript(SCHEMA)
            self.db.commit()

    def _row(self, r):
        return dict(r) if r is not None else None

    # ---- jobs ----
    def submit(self, name, command, env, needs, max_attempts, idem_key):
        t = now()
        with self._lock:
            if idem_key:
                r = self.db.execute(
                    "SELECT id, state FROM jobs WHERE idem_key=? AND state NOT IN ('done','failed','cancelled')",
                    (idem_key,),
                ).fetchone()
                if r:
                    return r["id"], False  # already have it; don't double-submit
            # The pre-check above races under concurrency: two submitters can
            # both miss and both INSERT. idx_jobs_idem makes the loser's
            # INSERT fail; we then hand back the winner's job instead of a
            # duplicate.
            for _ in range(2):
                jid = "job_" + secrets.token_hex(8)
                try:
                    self.db.execute(
                        "INSERT INTO jobs (id,name,command,env_json,needs_json,idem_key,"
                        "state,attempts,max_attempts,created_at,updated_at)"
                        " VALUES (?,?,?,?,?,?,'queued',0,?,?,?)",
                        (jid, name, command, json.dumps(env or {}), json.dumps(needs or {}),
                         idem_key, max_attempts, t, t),
                    )
                    self.db.commit()
                    return jid, True
                except sqlite3.IntegrityError:
                    self.db.rollback()
                    if not idem_key:
                        raise  # can't happen (NULL keys don't conflict); don't swallow
                    r = self.db.execute(
                        "SELECT id FROM jobs WHERE idem_key=? AND state NOT IN ('done','failed','cancelled')",
                        (idem_key,),
                    ).fetchone()
                    if r:
                        return r["id"], False
                    # Winner's job already went terminal; loop once and mint fresh.
            raise sqlite3.IntegrityError("job submit failed after retry")

    def get_job(self, jid):
        with self._lock:
            return self._row(self.db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone())

    def list_jobs(self, state=None, limit=100):
        with self._lock:
            if state:
                rows = self.db.execute(
                    "SELECT * FROM jobs WHERE state=? ORDER BY created_at DESC LIMIT ?",
                    (state, limit),
                ).fetchall()
            else:
                rows = self.db.execute(
                    "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)
                ).fetchall()
            return [self._row(r) for r in rows]

    def cancel(self, jid):
        # A running job can be cancelled too: the worker's next heartbeat
        # carries the kill signal and the job is never resurrected by a late
        # complete/fail (those only touch claimed/running rows).
        with self._lock:
            cur = self.db.execute(
                "UPDATE jobs SET state='cancelled', updated_at=? "
                "WHERE id=? AND state IN ('queued','claimed','running')",
                (now(), jid),
            )
            self.db.commit()
            return cur.rowcount > 0

    def is_cancelled(self, jid):
        with self._lock:
            r = self.db.execute("SELECT state FROM jobs WHERE id=?", (jid,)).fetchone()
            return bool(r and r["state"] == "cancelled")

    def claim_next(self, worker_id, caps):
        """Atomically claim one queued job this worker can run. None if empty."""
        t = now()
        with self._lock:
            rows = self.db.execute(
                "SELECT * FROM jobs WHERE state='queued' AND not_before<=? "
                "ORDER BY created_at ASC LIMIT 50",
                (t,),
            ).fetchall()
            for r in rows:
                if _fits(json.loads(r["needs_json"]), caps):
                    cur = self.db.execute(
                        "UPDATE jobs SET state='claimed', worker_id=?, "
                        "lease_expires=?, updated_at=? "
                        "WHERE id=? AND state='queued'",
                        (worker_id, t + LEASE_S, t, r["id"]),
                    )
                    self.db.commit()
                    if cur.rowcount:
                        return self._row(self.db.execute(
                            "SELECT * FROM jobs WHERE id=?", (r["id"],)).fetchone())
            return None

    def mark_running(self, jid, worker_id):
        with self._lock:
            cur = self.db.execute(
                "UPDATE jobs SET state='running', lease_expires=?, updated_at=? "
                "WHERE id=? AND worker_id=? AND state='claimed'",
                (now() + LEASE_S, now(), jid, worker_id),
            )
            self.db.commit()
            return cur.rowcount > 0

    def heartbeat_job(self, jid, worker_id):
        """Renew the lease while a long job runs."""
        with self._lock:
            cur = self.db.execute(
                "UPDATE jobs SET lease_expires=?, updated_at=? "
                "WHERE id=? AND worker_id=? AND state='running'",
                (now() + LEASE_S, now(), jid, worker_id),
            )
            self.db.commit()
            return cur.rowcount > 0

    def complete(self, jid, worker_id, result):
        with self._lock:
            cur = self.db.execute(
                "UPDATE jobs SET state='done', result_json=?, lease_expires=NULL,"
                " updated_at=? WHERE id=? AND worker_id=? AND state IN ('claimed','running')",
                (json.dumps(result), now(), jid, worker_id),
            )
            self.db.commit()
            return cur.rowcount > 0

    def fail(self, jid, worker_id, error, retryable):
        t = now()
        with self._lock:
            # Only a live lease can fail. A job the owner cancelled (or that
            # already finished elsewhere) must not be resurrected to
            # queued/failed by a late worker report.
            r = self.db.execute(
                "SELECT attempts, max_attempts FROM jobs WHERE id=? AND worker_id=?"
                " AND state IN ('claimed','running')",
                (jid, worker_id),
            ).fetchone()
            if not r:
                return False
            attempts = r["attempts"] + 1
            if retryable and attempts < r["max_attempts"]:
                backoff = 30 * (2 ** (attempts - 1))
                self.db.execute(
                    "UPDATE jobs SET state='queued', worker_id=NULL, attempts=?,"
                    " not_before=?, lease_expires=NULL, result_json=?, updated_at=?"
                    " WHERE id=? AND state IN ('claimed','running')",
                    (attempts, t + backoff, json.dumps({"error": error, "retry": attempts}), t, jid),
                )
            else:
                self.db.execute(
                    "UPDATE jobs SET state='failed', attempts=?, lease_expires=NULL,"
                    " result_json=?, updated_at=? WHERE id=? AND state IN ('claimed','running')",
                    (attempts, json.dumps({"error": error}), t, jid),
                )
            self.db.commit()
            return True

    def requeue(self, jid, reason):
        """Put a claimed/running job back to queued (dead worker / expired lease).

        A requeue means the job was (probably) being executed, so it burns an
        attempt like a failure does — otherwise a job that kills every worker
        it lands on would spin the fleet forever and never reach 'failed'.
        The backoff keeps a flapping job from hot-looping the queue.
        """
        t = now()
        with self._lock:
            r = self.db.execute(
                "SELECT attempts, max_attempts FROM jobs WHERE id=? AND state IN ('claimed','running')",
                (jid,),
            ).fetchone()
            if not r:
                return False
            attempts = r["attempts"] + 1
            if attempts >= r["max_attempts"]:
                self.db.execute(
                    "UPDATE jobs SET state='failed', attempts=?, lease_expires=NULL,"
                    " result_json=?, updated_at=? WHERE id=?",
                    (attempts, json.dumps({"error": "requeued too many times: " + reason}), t, jid),
                )
            else:
                backoff = 30 * (2 ** (attempts - 1))
                self.db.execute(
                    "UPDATE jobs SET state='queued', worker_id=NULL, attempts=?,"
                    " not_before=?, lease_expires=NULL, result_json=?, updated_at=?"
                    " WHERE id=?",
                    (attempts, t + backoff,
                     json.dumps({"requeued": reason, "attempt": attempts}), t, jid),
                )
            self.db.commit()
            return True

    def sweep(self):
        """Mark dead workers; requeue their jobs and any expired leases."""
        t = now()
        requeued, dead = 0, 0
        with self._lock:
            for w in self.db.execute(
                "SELECT id FROM workers WHERE state='alive' AND last_heartbeat<?",
                (t - DEAD_S,),
            ).fetchall():
                self.db.execute("UPDATE workers SET state='dead' WHERE id=?", (w["id"],))
                dead += 1
            for j in self.db.execute(
                "SELECT id FROM jobs WHERE state IN ('claimed','running') AND lease_expires<?",
                (t,),
            ).fetchall():
                # self._lock is an RLock, so reusing requeue() here is safe
                # and keeps the attempt/backoff logic in exactly one place.
                if self.requeue(j["id"], "lease expired"):
                    requeued += 1
            self.db.commit()
        return {"dead_workers": dead, "requeued_jobs": requeued}

    # ---- workers ----
    def register(self, worker_id, hostname, caps, labels):
        t = now()
        wid = worker_id or ("wrk_" + secrets.token_hex(6))
        with self._lock:
            self.db.execute(
                "INSERT OR REPLACE INTO workers "
                "(id,hostname,caps_json,labels_json,state,registered_at,last_heartbeat)"
                " VALUES (?,?,?,?, 'alive',?,?)",
                (wid, hostname, json.dumps(caps or {}), json.dumps(labels or []), t, t),
            )
            self.db.commit()
        return wid

    def heartbeat(self, worker_id):
        with self._lock:
            cur = self.db.execute(
                "UPDATE workers SET last_heartbeat=?, state='alive' WHERE id=?",
                (now(), worker_id),
            )
            self.db.commit()
            return cur.rowcount > 0

    def list_workers(self):
        with self._lock:
            return [self._row(r) for r in self.db.execute(
                "SELECT * FROM workers ORDER BY last_heartbeat DESC").fetchall()]

    def stats(self):
        with self._lock:
            q = self.db.execute(
                "SELECT state, COUNT(*) c FROM jobs GROUP BY state").fetchall()
            w = self.db.execute(
                "SELECT state, COUNT(*) c FROM workers GROUP BY state").fetchall()
        return {"jobs": {r["state"]: r["c"] for r in q},
                "workers": {r["state"]: r["c"] for r in w}}


def _fits(needs, caps):
    """Capability match: can this worker run a job with these needs?"""
    try:
        if float(needs.get("min_cpu", 1)) > float(caps.get("cpu_count", 0)):
            return False
        if float(needs.get("min_ram_gb", 0.5)) > float(caps.get("ram_gb", 0)):
            return False
        if float(needs.get("min_disk_gb", 0)) > float(caps.get("disk_gb", 0)):
            return False
        want_gpu = needs.get("gpu")
        if want_gpu:
            have = caps.get("gpu")
            if not have:
                return False
            if isinstance(want_gpu, str) and want_gpu.lower() not in str(have).lower():
                return False
        for label in needs.get("labels", []) or []:
            if label not in (caps.get("labels") or []):
                return False
    except (TypeError, ValueError):
        return False
    return True


def _public_job(row):
    if not row:
        return None
    row = dict(row)
    for k in ("env_json", "needs_json", "result_json"):
        try:
            row[k.replace("_json", "")] = json.loads(row.pop(k) or "{}")
        except (ValueError, KeyError):
            pass
    return row


class Handler(BaseHTTPRequestHandler):
    server_version = "PoolHead/1.0"

    def log_message(self, *a):
        sys.stderr.write("[%s] %s\n" % (time.strftime("%H:%M:%S"), " ".join(map(str, a))))

    # -- helpers --
    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n > MAX_BODY_BYTES:
            raise ValueError("body too large")
        raw = self.rfile.read(n) if n else b"{}"
        return json.loads(raw.decode() or "{}")

    def _authed(self, qs):
        if not TOKEN:
            return True
        got = self.headers.get("Authorization", "")
        if got == "Bearer " + TOKEN:
            return True
        return qs.get("token") == [TOKEN]

    def _path(self):
        u = urllib.parse.urlsplit(self.path)
        return u.path, urllib.parse.parse_qs(u.query)

    # -- routes --
    def do_GET(self):
        path, qs = self._path()
        store = self.server.store
        if path == "/health":
            s = store.stats()
            return self._send(200, {"ok": True, "time": now(), **s})
        if path == "/agent/pool_worker.py":
            # Serve our own worker script so ephemeral boxes can curl it down.
            try:
                with open(os.path.join(HERE, "..", "worker", "pool_worker.py"), "rb") as f:
                    body = f.read()
            except OSError:
                return self._send(404, {"error": "worker script not found next to head-node"})
            self.send_response(200)
            self.send_header("Content-Type", "text/x-python")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return self.wfile.write(body)
        if not self._authed(qs):
            return self._send(401, {"error": "bad or missing token"})
        if path == "/jobs":
            state = (qs.get("state") or [None])[0]
            return self._send(200, {"jobs": [_public_job(j) for j in store.list_jobs(state)]})
        if path == "/workers":
            return self._send(200, {"workers": [self._pub_w(w) for w in store.list_workers()]})
        if path == "/next":
            wid = (qs.get("worker_id") or [None])[0]
            if not wid:
                return self._send(400, {"error": "worker_id required"})
            try:
                wait = min(float((qs.get("wait") or ["25"])[0]), 60)
            except (TypeError, ValueError):
                wait = 25
            deadline = now() + wait
            while now() < deadline:
                w = next((x for x in store.list_workers() if x["id"] == wid), None)
                if not w:
                    return self._send(404, {"error": "unknown worker; re-register"})
                caps = json.loads(w["caps_json"])
                caps["labels"] = json.loads(w["labels_json"])
                job = store.claim_next(wid, caps)
                if job:
                    return self._send(200, {"job": _public_job(job)})
                time.sleep(0.5)
            return self._send(200, {"job": None})
        m = path.split("/")
        if len(m) == 3 and m[1] == "jobs":
            job = store.get_job(m[2])
            if not job:
                return self._send(404, {"error": "no such job"})
            return self._send(200, {"job": _public_job(job)})
        return self._send(404, {"error": "not found"})

    def do_POST(self):
        path, qs = self._path()
        store = self.server.store
        if not self._authed(qs):
            return self._send(401, {"error": "bad or missing token"})
        try:
            body = self._read_json()
        except Exception as e:
            return self._send(400, {"error": "bad json: %s" % e})
        if path == "/jobs":
            if not body.get("command"):
                return self._send(400, {"error": "command is required"})
            try:
                max_attempts = int(body.get("max_attempts", 3))
            except (TypeError, ValueError):
                max_attempts = 3
            max_attempts = max(1, min(max_attempts, 100))
            jid, created = store.submit(
                body.get("name") or "job",
                body["command"],
                body.get("env"), body.get("needs"),
                max_attempts,
                body.get("idempotency_key"),
            )
            return self._send(200, {"id": jid, "created": created})
        if path == "/workers/register":
            wid = store.register(body.get("worker_id"), body.get("hostname") or "?",
                                 body.get("capabilities"), body.get("labels"))
            return self._send(200, {"worker_id": wid})
        if path == "/workers/heartbeat":
            if not body.get("worker_id"):
                return self._send(400, {"error": "worker_id required"})
            ok = store.heartbeat(body["worker_id"])
            # Piggyback: also renew the lease on the worker's running job,
            # and tell the worker if the owner cancelled that job so it can
            # kill the process tree instead of burning CPU on dead work.
            cancel_job = None
            if body.get("job_id"):
                store.heartbeat_job(body["job_id"], body["worker_id"])
                if store.is_cancelled(body["job_id"]):
                    cancel_job = body["job_id"]
            return self._send(200, {"ok": ok, "cancel_job": cancel_job})
        m = path.split("/")
        if len(m) == 4 and m[1] == "jobs":
            jid, action = m[2], m[3]
            wid = body.get("worker_id")
            if action == "cancel":
                return self._send(200, {"ok": store.cancel(jid)})
            if action == "start":
                if not wid:
                    return self._send(400, {"error": "worker_id required"})
                return self._send(200, {"ok": store.mark_running(jid, wid)})
            if action == "complete":
                if not wid:
                    return self._send(400, {"error": "worker_id required"})
                # Reject before touching the filesystem: a forged job id must
                # never reach _store_artifacts (path traversal -> arbitrary
                # file write next to the artifact dir).
                if not _SAFE_ID.match(jid) or not store.get_job(jid):
                    return self._send(404, {"error": "no such job"})
                result = body.get("result") or {}
                self._store_artifacts(jid, result)
                ok = store.complete(jid, wid, self._trim_result(result))
                return self._send(200, {"ok": ok})
            if action == "fail":
                if not wid:
                    return self._send(400, {"error": "worker_id required"})
                ok = store.fail(jid, wid, body.get("error", "unknown"),
                                bool(body.get("retryable", True)))
                return self._send(200, {"ok": ok})
        return self._send(404, {"error": "not found"})

    def _pub_w(self, w):
        w = dict(w)
        w["capabilities"] = json.loads(w.pop("caps_json") or "{}")
        w["labels"] = json.loads(w.pop("labels_json") or "[]")
        return w

    def _trim_result(self, result):
        """Keep the stored result small; artifacts live on disk."""
        r = dict(result)
        arts = r.get("artifacts") or []
        r["artifacts"] = [{"name": a.get("name"), "size": a.get("size"),
                           "sha256": a.get("sha256")} for a in arts]
        for k in ("stdout", "stderr"):
            if isinstance(r.get(k), str) and len(r[k]) > 65536:
                r[k] = r[k][-65536:]
        return r

    def _store_artifacts(self, jid, result):
        import base64
        # Defense in depth: even if a caller skips the route-level check,
        # a job id that isn't server-minted never becomes a filesystem path.
        if not _SAFE_ID.match(jid):
            return
        arts = result.get("artifacts") or []
        total = 0
        for a in arts:
            data_b64 = a.get("data_b64")
            if not data_b64:
                continue
            try:
                raw = base64.b64decode(data_b64)
            except Exception:
                continue
            total += len(raw)
            if total > MAX_ARTIFACT_BYTES:
                break
            name = os.path.basename(a.get("name") or "artifact")
            dest_dir = os.path.join(ARTIFACT_DIR, jid)
            os.makedirs(dest_dir, exist_ok=True)
            dest = os.path.join(dest_dir, name)
            # fail closed: never let a worker write outside the artifact dir
            if os.path.commonpath([dest_dir, os.path.abspath(dest)]) != os.path.abspath(dest_dir):
                continue
            with open(dest, "wb") as f:
                f.write(raw)
            a.pop("data_b64", None)  # don't keep bulk bytes in memory copy


def sweeper(store):
    while True:
        time.sleep(SWEEP_S)
        try:
            r = store.sweep()
            if r["dead_workers"] or r["requeued_jobs"]:
                sys.stderr.write("[sweep] dead=%d requeued=%d\n" % (r["dead_workers"], r["requeued_jobs"]))
        except Exception as e:
            sys.stderr.write("[sweep] error: %s\n" % e)


def check_only():
    problems = []
    if not TOKEN:
        print("WARN: POOL_TOKEN is not set — server will accept anyone. "
              "Set it before exposing this on a network.")
    try:
        s = Store(DB_PATH)
        st = s.stats()
        print("db ok: %s (jobs=%s workers=%s)" % (DB_PATH, st["jobs"], st["workers"]))
    except Exception as e:
        problems.append("db: %s" % e)
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    print("artifacts dir ok: %s" % ARTIFACT_DIR)
    if problems:
        print("PROBLEMS:")
        for p in problems:
            print(" -", p)
        return 1
    print("pool head-node check: ok")
    return 0


def main():
    if "--check" in sys.argv:
        sys.exit(check_only())
    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        return
    if not TOKEN:
        sys.stderr.write("WARN: POOL_TOKEN unset — open server. Set POOL_TOKEN in production.\n")
    store = Store(DB_PATH)
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    server.store = store
    threading.Thread(target=sweeper, args=(store,), daemon=True).start()
    sys.stderr.write("pool head node on :%d db=%s\n" % (PORT, DB_PATH))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
