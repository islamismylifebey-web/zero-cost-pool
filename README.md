# THE POOL — one brain, many muscles

Maurice's mission: stop paying per-machine. Scavenge every free computer —
Oracle free VMs, the Google e2-micro, spare laptops, even a Colab session —
and federate them behind **one head node** that deals work out to whatever
box fits.

## The mental model (read this first)

```
                    ┌─────────────────────────┐
                    │       HEAD NODE          │
                    │  (Oracle ARM free VM)    │
                    │                          │
                    │  job queue  +  worker     │
                    │  registry  +  scheduler   │
                    └────────┬────────┬─────────┘
                             │        │  (Tailscale — one private network)
              ┌──────────────┘        └──────────────┐
     ┌────────▼────────┐                   ┌────────▼────────┐
     │ WORKER: laptop  │                   │ WORKER: Colab   │
     │ cpu=8 ram=16GB  │                   │ cpu=2 + T4 GPU  │
     │ labels: none    │                   │ --ephemeral     │
     └─────────────────┘                   └─────────────────┘
```

- **Head node = the brain.** Always on, holds the queue, knows every
  worker's capabilities, never executes jobs itself.
- **Workers = the muscle.** Each box runs `pool_worker.py`, which reports
  what it has (`cpu_count`, `ram_gb`, `gpu` or none, `disk_gb`) and
  long-polls for jobs it can actually run.
- **Capabilities = labels.** A job says "I need ≥4 CPU, ≥8GB RAM, a GPU".
  The scheduler only deals it to a worker wearing all three labels.
  A GPU job never lands on a laptop with no GPU — it waits.

**What this is:** the render-farm / supercomputer pattern. One queue,
many machines, work flows to wherever it fits.

**What this is NOT:** it does not fuse machines into one big computer.
Two boxes cannot share RAM for a single program — no free tool does that
for normal software. If a job needs 32GB on one box, it needs a 32GB box.
The pool wins by running *many* jobs across *many* boxes, not by making
one giant one.

## Files

| Path | What it is |
|---|---|
| `head-node/pool_server.py` | the scheduler (stdlib Python 3 only) |
| `head-node/poolctl` | CLI: submit / list / status / cancel / workers / --check |
| `worker/pool_worker.py` | the worker agent (stdlib Python 3 only) |
| `worker/pool-worker.service` | sample systemd user unit for persistent workers |

Why Python 3, zero dependencies: it's already on every Ubuntu box, every
Oracle/Google free VM, and every Colab session. No pip, no venv, nothing
to install — you can `curl` one file onto a strange machine and join the
pool in 30 seconds.

Ideas borrowed from the Octopus runner (`~/workspace/octopus/src/runner.js`):
atomic claim leases (never double-execute), persisted state after every
transition (crash-safe), idempotency keys on submit, honest terminal states
(`done` only on exit 0 with the result stored; `failed` after attempts run
out — never silently dropped).

## Bring up the head node (Oracle free ARM VM)

On the Oracle VM (Ubuntu 24.04), after `warehouse-init.sh --install` and
`tailscale up`:

```bash
# as the runner user
mkdir -p ~/pool && cd ~/pool
cp -r <this repo>/pool/head-node ~/pool/
export POOL_TOKEN="$(openssl rand -hex 16)"   # write this down; workers need it
echo "POOL_TOKEN=$POOL_TOKEN" > ~/.config/pool.env && chmod 600 ~/.config/pool.env

# verify, then run (use the sample systemd unit pattern or tmux for now)
python3 ~/pool/head-node/pool_server.py --check
POOL_TOKEN="$POOL_TOKEN" python3 ~/pool/head-node/pool_server.py
```

Note the VM's Tailscale address (`tailscale ip -4`) — workers use
`http://<that-ip>:8765` as `--head`. Nothing is exposed to the public
internet; the pool lives entirely on the tailnet.

## Enroll a spare laptop

```bash
# on the laptop, after warehouse-init.sh --install and tailscale up
python3 pool_worker.py --head http://100.x.y.z:8765 --token "$POOL_TOKEN" \
    --name laptop-1 --labels region=home,fast-disk
```

Persistent (survives reboot): install `worker/pool-worker.service` as a
systemd user unit — see the comments at the top of that file.

## Enroll an ephemeral GPU box (Colab / Kaggle)

These boxes die without warning, so the worker runs in the foreground
and the head node treats it as expendable:

```python
# Colab cell 1: join the tailnet (needs a Tailscale auth key from the admin console)
!curl -fsSL https://tailscale.com/install.sh | sh
!tailscale up --authkey=tskey-... --hostname=colab-gpu-1

# Colab cell 2: fetch the worker FROM THE HEAD NODE and run it
!curl -s http://100.x.y.z:8765/agent/pool_worker.py -o pool_worker.py
!python3 pool_worker.py --head http://100.x.y.z:8765 --token "$POOL_TOKEN" \
    --name colab-gpu-1 --ephemeral --labels gpu,preemptible
```

(`GET /agent/pool_worker.py` serves the exact script the head node runs
beside — no GitHub, no copying files around.)

**GPU jobs must be checkpointable batches.** An ephemeral box can vanish
mid-render. The pool will requeue the job, but it restarts from zero —
so write jobs that save progress (frames, checkpoints) as they go, or
split work into small jobs (one job per scene, not one job per film).

## How a job flows, end to end

```bash
# 1. submit: "render scene 4, needs a GPU, 4GB RAM, 2h max"
poolctl submit --name scene-4 --gpu --min-ram 4 --timeout 7200 \
    --label preemptible -- 'python3 render.py --scene 4 --out ./frames/'

# 2. scheduler deals it to a live worker whose capabilities fit
# 3. worker runs it in ~/pool-work/<jobid>/, streams nothing until done
# 4. on exit 0: result + output tails + artifacts return to the head node
poolctl status job_abc123        # exit code, tails, artifact manifest
# artifacts land in <head>/artifacts/<jobid>/
```

Job states: `queued → claimed → running → done | failed | cancelled`.
A claim is an atomic lease (`POOL_LEASE_S`, default 300s). If the worker's
heartbeat stops (`POOL_DEAD_S`, default 90s) or the lease expires, the
sweeper puts the job back to `queued` — but the requeue **burns an attempt**
and backs off (30s, doubling), so a job that kills every worker it lands on
reaches `failed` instead of hot-looping the fleet forever.
`poolctl cancel` works on `running` jobs too: the worker's next heartbeat
carries the kill signal and the whole process tree is SIGKILLed within ~1s.
A late `complete`/`fail` from the killed worker is refused — a cancelled job
is never resurrected. Jobs are never lost and never run twice at once.

## The honest limits

1. **No cross-machine shared memory.** A job runs on exactly one box.
   Size jobs to fit the biggest box you have, not the sum of all boxes.
2. **Ephemeral GPU workers are preemptible.** Colab kills idle sessions
   (90 min) and caps sessions (~12h); Kaggle gives ~30 GPU-hours/week.
   Design GPU jobs as small checkpointed batches or they will waste work.
3. **The head node is load-bearing.** If it dies, workers idle (jobs and
   registry live in its SQLite file). Back up `pool.db`; better, run the
   head on the most reliable box you have. Workers are disposable by
   design — the head is not.
4. **Artifacts cap at 10MB per job** (`POOL_MAX_ARTIFACT_BYTES`). Big
   renders should upload to R2/object storage from the job itself and
   return only the URL + manifest — don't push gigabytes through the queue.
5. **Trust boundary:** any box with `POOL_TOKEN` can submit jobs that run
   shell commands on workers. Guard the token like a password, and only
   enroll boxes you control. (Workers run jobs as the `runner` user in a
   sandbox dir — not a security sandbox. Don't run untrusted code.)
6. **A crashed worker can leave its sandbox dir** (`~/pool-work/<jobid>`)
   behind; the job itself is safe (requeued), the dirt is local. Clean
   `~/pool-work` occasionally on persistent workers.

## What was tested (local, 2026-09-28)

- `py_compile` on both Python files; `bash -n` on `poolctl` — all clean.
- `pool_server.py --check` — schema creates, stats report.
- **End-to-end on 127.0.0.1:** submit `echo` job → worker claimed →
  `done`, exit 0, stdout tail + 4 artifacts returned and written to the
  head's artifact dir with matching sha256.
- **Capability mismatch:** GPU job stayed `queued` on a GPU-less worker;
  a job needing 1GB disk was correctly refused by a 0.5GB-/tmp worker.
- **Dead worker:** `kill -9` mid-job → lease expired → job requeued to
  `queued`; a fresh worker picked it up and completed it. Registry showed
  the old worker `dead`, the new one `alive`.
- **Idempotency:** same `--idem` key submitted twice → one job id returned.
  Also holds under a 20-thread concurrent submit race (unique index +
  loser-gets-winner's-id).
- **Cancel:** queued job cancelled cleanly. **Running** job cancelled →
  worker SIGKILLed the process tree within ~2s (via heartbeat signal);
  job stayed `cancelled`; a late `fail` was refused (no resurrection).
- **Requeue honesty:** claimed-then-abandoned job with `max_attempts=1`
  reached `failed` (attempt burned) instead of looping; with
  `max_attempts=3` it requeued with `not_before` backoff in the future.
- **Artifact traversal:** forged job id `/jobs/../complete` → 404, nothing
  written outside the artifact dir (previously wrote attacker files next
  to the head's own code).
- **Worker shutdown:** SIGTERM during a long-poll now exits in ~1s
  (previously waited out the full poll window).

## What needs a real multi-host test

- Head on Oracle ARM + worker on a second machine over Tailscale
  (auth, long-poll across the tailnet, artifact transfer off-localhost).
- An actual ephemeral GPU box (Colab): Tailscale install in the notebook,
  worker fetch from `/agent/pool_worker.py`, GPU capability detection via
  `nvidia-smi`, and a real preemption (does the job requeue cleanly?).
- `pool-worker.service` systemd lifecycle (enable, reboot, restart).
- Sustained load: dozens of jobs, worker churn, lease/expiry timing under
  real network latency (tune `POOL_LEASE_S` / `POOL_DEAD_S` if flappy).
