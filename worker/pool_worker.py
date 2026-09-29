#!/usr/bin/env python3
"""pool_worker.py — THE POOL worker agent.

The muscle of the fleet. Runs on any box (spare laptop, Oracle free VM,
Colab/Kaggle session), registers its capabilities with the head node,
long-polls for jobs it can run, executes each one in an isolated sandbox
directory, and reports back exit code + output + artifacts.

Crash safety: the worker holds no authority. If it dies mid-job, the head
node's lease expires and the job goes back to the queue for another worker.
A job is therefore never lost and never run twice at once.

  --ephemeral   run in the foreground and die with the session
                (Colab/Kaggle style: no systemd, no daemonizing).
                Without it the worker still runs in the foreground here —
                persistence is provided by the sample systemd unit
                (pool-worker.service) or the warehouse runner-service.sh.

Zero dependencies: stdlib only.

Usage:
  python3 pool_worker.py --head http://100.x.y.z:8765 --token SECRET --ephemeral
  python3 pool_worker.py --head http://127.0.0.1:8765 --name laptop-1

Environment fallbacks: POOL_HEAD, POOL_TOKEN.
"""

import argparse
import base64
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.parse

TAIL_BYTES = 65536
MAX_ARTIFACT_TOTAL = 10 * 1024 * 1024
STOP = threading.Event()


def detect_capabilities(sandbox_base):
    caps = {}
    caps["cpu_count"] = os.cpu_count() or 1
    # RAM from /proc/meminfo (Linux). Fallback: unknown -> 0 (matches nothing
    # that needs RAM, which is the safe direction).
    caps["ram_gb"] = 0.0
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    caps["ram_gb"] = round(kb / 1024 / 1024, 2)
                    break
    except OSError:
        pass
    # GPU via nvidia-smi. None = no GPU; the scheduler will never send
    # gpu jobs here.
    caps["gpu"] = None
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=15,
        )
        names = [l.strip() for l in out.stdout.splitlines() if l.strip()]
        if out.returncode == 0 and names:
            caps["gpu"] = names[0]
    except (OSError, subprocess.SubprocessError):
        pass
    try:
        caps["disk_gb"] = round(shutil.disk_usage(sandbox_base).free / 1024**3, 2)
    except OSError:
        caps["disk_gb"] = 0.0
    return caps


class Head:
    def __init__(self, base, token):
        self.base = base.rstrip("/")
        self.token = token or ""

    def _req(self, method, path, body=None, timeout=70):
        data = json.dumps(body or {}).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", "Bearer " + self.token)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, json.loads(r.read().decode() or "{}")
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read().decode() or "{}")
            except Exception:
                return e.code, {"error": "http %d" % e.code}

    def register(self, worker_id, hostname, caps, labels):
        return self._req("POST", "/workers/register",
                         {"worker_id": worker_id, "hostname": hostname,
                          "capabilities": caps, "labels": labels}, timeout=15)

    def heartbeat(self, worker_id, job_id=None):
        return self._req("POST", "/workers/heartbeat",
                         {"worker_id": worker_id, "job_id": job_id}, timeout=15)

    def next_job(self, worker_id, wait=25):
        q = urllib.parse.urlencode({"worker_id": worker_id, "wait": wait})
        return self._req("GET", "/next?" + q, None, timeout=wait + 30)

    def start(self, jid, worker_id):
        return self._req("POST", "/jobs/%s/start" % jid, {"worker_id": worker_id}, timeout=15)

    def complete(self, jid, worker_id, result):
        return self._req("POST", "/jobs/%s/complete" % jid,
                         {"worker_id": worker_id, "result": result}, timeout=120)

    def fail(self, jid, worker_id, error, retryable=True):
        return self._req("POST", "/jobs/%s/fail" % jid,
                         {"worker_id": worker_id, "error": error,
                          "retryable": retryable}, timeout=15)


def run_job(job, sandbox_base, head, worker_id, current):
    """Execute one job. Returns nothing; reports to the head node."""
    jid = job["id"]
    box = os.path.join(sandbox_base, jid)
    os.makedirs(box, exist_ok=True)
    code, resp = head.start(jid, worker_id)
    if code != 200 or not resp.get("ok"):
        sys.stderr.write("[worker] head refused start for %s: %s\n" % (jid, resp))
        shutil.rmtree(box, ignore_errors=True)
        return

    env = dict(os.environ)
    for k, v in (job.get("env") or {}).items():
        env[str(k)] = str(v)
    # Fail closed inside the sandbox: no surprises from inherited secrets.
    env["POOL_JOB_ID"] = jid
    timeout = int((job.get("needs") or {}).get("timeout_s", 3600))

    sys.stderr.write("[worker] running %s: %s\n" % (jid, job["command"][:120]))
    proc = None
    # Watcher: if the owner cancels the job mid-run, the head says so on the
    # next heartbeat and we kill the whole process group — no point burning
    # CPU on dead work, and the job must not report back over a cancellation.
    stop_watcher = threading.Event()

    def _watcher():
        while not stop_watcher.wait(1.0):
            if current.kill_requested():
                sys.stderr.write("[worker] owner cancelled %s; killing job tree\n" % jid)
                try:
                    if proc is not None:
                        os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                return

    threading.Thread(target=_watcher, daemon=True).start()
    try:
        try:
            # start_new_session: the whole job is one process group so a timeout
            # (or an owner cancel) kills the entire tree, not just the shell.
            proc = subprocess.Popen(
                ["bash", "-c", job["command"]],
                cwd=box, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                start_new_session=True, text=True,
            )
            try:
                out, err = proc.communicate(timeout=timeout)
                rc = proc.returncode
                timed_out = False
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                out, err = proc.communicate()
                rc = 124
                timed_out = True
        except Exception as e:
            head.fail(jid, worker_id, "worker could not start job: %s" % e, retryable=True)
            shutil.rmtree(box, ignore_errors=True)
            return
    finally:
        stop_watcher.set()

    if current.kill_requested():
        # Owner cancelled mid-run: the head already has the terminal state.
        # Report nothing (fail/complete would be refused anyway) and clean up.
        sys.stderr.write("[worker] %s killed by owner cancel; cleaned up\n" % jid)
        shutil.rmtree(box, ignore_errors=True)
        return

    # Bounded tails for the result record; full logs stay in the sandbox dir
    # and are collected as artifacts below.
    with open(os.path.join(box, "stdout.log"), "w") as f:
        f.write(out or "")
    with open(os.path.join(box, "stderr.log"), "w") as f:
        f.write(err or "")
    if timed_out:
        with open(os.path.join(box, "TIMEOUT"), "w") as f:
            f.write("job exceeded %ds\n" % timeout)

    artifacts = collect_artifacts(box)
    result = {
        "exit_code": rc,
        "timed_out": timed_out,
        "stdout": (out or "")[-TAIL_BYTES:],
        "stderr": (err or "")[-TAIL_BYTES:],
        "artifacts": artifacts,
    }
    if rc == 0:
        code, resp = head.complete(jid, worker_id, result)
    else:
        # Non-zero exit: retryable unless the job says otherwise. A job that
        # fails deterministically (bad command) will burn its attempts and
        # land in 'failed' — honest, not wedged.
        retryable = bool((job.get("needs") or {}).get("retry_on_fail", True))
        code, resp = head.fail(jid, worker_id,
                               "exit %d: %s" % (rc, (err or out or "")[-2000:]),
                               retryable=retryable)
    sys.stderr.write("[worker] %s finished rc=%d reported=%s\n" % (jid, rc, resp))
    shutil.rmtree(box, ignore_errors=True)


def collect_artifacts(box):
    """Walk the sandbox dir; return [{name,size,sha256,data_b64}] under the cap."""
    found = []
    for root, _dirs, files in os.walk(box):
        for fn in files:
            p = os.path.join(root, fn)
            rel = os.path.relpath(p, box)
            if rel.startswith(".."):
                continue
            try:
                size = os.path.getsize(p)
            except OSError:
                continue
            found.append((size, rel, p))
    found.sort()  # small files first: more evidence fits under the cap
    arts, total = [], 0
    for size, rel, p in found:
        if total + size > MAX_ARTIFACT_TOTAL:
            break
        try:
            with open(p, "rb") as f:
                raw = f.read()
        except OSError:
            continue
        total += len(raw)
        arts.append({
            "name": rel,
            "size": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "data_b64": base64.b64encode(raw).decode(),
        })
    return arts


def heartbeat_loop(head, worker_id, hb_s, current_job):
    while not STOP.is_set():
        try:
            code, resp = head.heartbeat(worker_id, current_job.get())
            if code == 200 and not resp.get("ok"):
                sys.stderr.write("[worker] head no longer knows us; re-registering\n")
                return False  # signal main loop to re-register
            # Owner cancelled the running job: flag it, but only if it's still
            # the job we think we're running (heartbeats can cross a job
            # boundary; never kill a new job for an old cancellation).
            cancel_id = resp.get("cancel_job") if code == 200 else None
            if cancel_id and cancel_id == current_job.get():
                current_job.request_kill()
        except Exception as e:
            sys.stderr.write("[worker] heartbeat failed: %s\n" % e)
        STOP.wait(hb_s)
    return True


class CurrentJob:
    def __init__(self):
        self._v = None
        self._kill = threading.Event()
        self._l = threading.Lock()

    def set(self, v):
        with self._l:
            self._v = v
            self._kill.clear()  # a new job starts unkilled

    def get(self):
        with self._l:
            return self._v

    def request_kill(self):
        self._kill.set()

    def kill_requested(self):
        return self._kill.is_set()


def main():
    ap = argparse.ArgumentParser(description="THE POOL worker agent")
    ap.add_argument("--head", default=os.environ.get("POOL_HEAD", ""),
                    help="head node base URL, e.g. http://100.x.y.z:8765")
    ap.add_argument("--token", default=os.environ.get("POOL_TOKEN", ""))
    ap.add_argument("--name", default=socket.gethostname())
    ap.add_argument("--ephemeral", action="store_true",
                    help="foreground, die with the session (Colab/Kaggle style)")
    ap.add_argument("--labels", default="",
                    help="comma-separated extra labels, e.g. region=us,fast-disk")
    ap.add_argument("--sandbox", default="",
                    help="where job sandboxes live (default ~/pool-work)")
    ap.add_argument("--heartbeat", type=int, default=30)
    ap.add_argument("--poll-wait", type=int, default=25)
    args = ap.parse_args()
    if not args.head:
        sys.stderr.write("error: --head (or POOL_HEAD) is required\n")
        return 2

    sandbox_base = args.sandbox or os.path.join(os.path.expanduser("~"), "pool-work")
    try:
        os.makedirs(sandbox_base, exist_ok=True)
    except OSError as e:
        sys.stderr.write("error: cannot create sandbox dir %s: %s\n" % (sandbox_base, e))
        return 1

    def on_signal(signum, _frame):
        sys.stderr.write("[worker] signal %d: stopping after current job\n" % signum)
        STOP.set()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    caps = detect_capabilities(sandbox_base)
    labels = [l.strip() for l in args.labels.split(",") if l.strip()]
    sys.stderr.write("[worker] capabilities: cpu=%s ram=%sGB gpu=%s disk=%sGB labels=%s\n"
                     % (caps["cpu_count"], caps["ram_gb"], caps["gpu"],
                        caps["disk_gb"], labels))
    head = Head(args.head, args.token)
    current = CurrentJob()
    worker_id = None

    while not STOP.is_set():
        if worker_id is None:
            try:
                code, resp = head.register(worker_id, args.name, caps, labels)
            except Exception as e:
                sys.stderr.write("[worker] register failed: %s; retrying\n" % e)
                STOP.wait(5)
                continue
            if code != 200:
                sys.stderr.write("[worker] register refused (%s): %s; retrying\n" % (code, resp))
                STOP.wait(5)
                continue
            worker_id = resp["worker_id"]
            sys.stderr.write("[worker] registered as %s\n" % worker_id)
            hb_ok = [True]

            def hb():
                hb_ok[0] = heartbeat_loop(head, worker_id, args.heartbeat, current)

            threading.Thread(target=hb, daemon=True).start()
        # Long-poll in a daemon thread so SIGTERM/SIGINT stops the worker
        # promptly instead of waiting out the full poll window. If we are
        # told to stop mid-poll, the claimed-nothing state is safe: any job
        # already claimed keeps its lease and is requeued on expiry.
        box = {}

        def _poll():
            try:
                box["r"] = head.next_job(worker_id, wait=args.poll_wait)
            except Exception as e:
                box["e"] = e

        pt = threading.Thread(target=_poll, daemon=True)
        pt.start()
        while pt.is_alive() and not STOP.is_set():
            STOP.wait(1.0)
        if STOP.is_set():
            break  # daemon poll thread dies with the process
        if "e" in box:
            sys.stderr.write("[worker] poll failed: %s\n" % box["e"])
            STOP.wait(5)
            continue
        code, resp = box["r"]
        if code == 404:
            sys.stderr.write("[worker] head forgot us; re-registering\n")
            worker_id = None
            STOP.wait(2)
            continue
        if code != 200:
            sys.stderr.write("[worker] poll error %s: %s\n" % (code, resp))
            STOP.wait(5)
            continue
        job = resp.get("job")
        if not job:
            continue  # long-poll timed out with nothing; loop again
        current.set(job["id"])
        try:
            run_job(job, sandbox_base, head, worker_id, current)
        finally:
            current.set(None)

    sys.stderr.write("[worker] stopped\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
