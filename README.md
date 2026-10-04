# flowd

A minimal but real **distributed task scheduling / workflow orchestration backend**:
DAG definition and validation, run state machine, worker leases with expiry and
takeover, retries with exponential backoff, idempotent completion, human
**approval gates** (`"kind": "approval"`), append-only history with replay,
multi-tenant isolation, and a small JSON HTTP API. Standard library only: no pip
installs, no network access, no third-party imports.

Data lives in a single directory: `<root>/<tenant>/workflows.json` plus
`<root>/<tenant>/runs/<run_id>.json`, written atomically, so restarting the
process sees exactly the same state.

## Run

```bash
# HTTP API (default data dir ./flowd_data)
python3 -m flowd --data-dir ./flowd_data serve --host 127.0.0.1 --port 8080

# tests
python3 -m unittest discover -s tests -v
```

## CLI

Global `--data-dir` goes **before** the subcommand. Every command prints one line
of JSON on stdout; errors print one line of JSON on stderr and exit non-zero.

```bash
python3 -m flowd --data-dir ./flowd_data submit --tenant acme --workflow etl --file steps.json
python3 -m flowd --data-dir ./flowd_data status --run-id run-abc --tenant acme
python3 -m flowd --data-dir ./flowd_data claim --run-id run-abc --tenant acme --worker w1
python3 -m flowd --data-dir ./flowd_data complete --run-id run-abc --step step1 --tenant acme --worker w1
python3 -m flowd --data-dir ./flowd_data fail --run-id run-abc --step step1 --tenant acme --worker w1 --error boom
python3 -m flowd --data-dir ./flowd_data list --tenant acme
```

`steps.json` is a JSON list of steps, e.g.
`[{"id": "step1", "depends_on": []}, {"id": "step2", "depends_on": ["step1"], "max_attempts": 3},
{"id": "signoff", "depends_on": ["step1"], "kind": "approval"}]`.
`kind` is optional (`task` by default) and must be `task` or `approval`.

## Approval gates

An `approval` step never enters `ready` and can never be claimed. Once all its
dependencies have `succeeded` (or immediately, when it has none) it moves to
`waiting`; the run then counts as `running` even though no lease is held.
Approval steps hold no lease, never retry, and always report `attempt: 0`;
`worker_id`, `lease_deadline`, `next_attempt_at`, `result` and `error` stay
`null`. Other independent task steps keep being claimed in the usual order.

A gate is resolved with
`POST /v1/runs/{run_id}/decision`, body
`{"tenant", "step_id", "actor", "decision"}`, where `decision` is `approve` or
`reject` and the three string fields must be non-empty after trimming
whitespace (400 otherwise, bad JSON included):

- `approve` -> the step `succeeded` and dependent task steps unlock; dependent
  approval gates start waiting.
- `reject` -> the step and the whole run `failed`; dependent steps stay
  `pending`.
- Repeating the same actor + decision returns 200 and keeps the first record
  and timestamp (no new history). A different actor, the opposite decision, a
  decision before the gate is waiting, or targeting a task step returns 409.
- Unknown run/step -> 404, another tenant -> 403. Conflicts are serialized by
  the store lock, so concurrent identical requests follow the same rules.

Run views add `kind: "approval"` and `approval` to waiting/decided steps only
(`null` before the first decision, then `{"actor", "decision", "at"}` in the
existing UTC format); task steps look exactly as before. Entering `waiting` and
the first decision each append one history event; the decision event carries
the usual `at/run_id/step_id/attempt/worker_id` fields plus `actor` and
`decision`. The state survives restarts and `replay`; old run documents without
`kind` are treated as tasks.

## HTTP API

| Method | Path | Success | Error codes |
| --- | --- | --- | --- |
| GET | `/healthz` | 200 `{"ok":true}` | - |
| POST | `/v1/workflows` | 201 validated plan | 400 validation error, 400 bad JSON |
| POST | `/v1/runs` | 201 run | 400 bad request, 404 unknown workflow |
| GET | `/v1/runs/{run_id}?tenant=` | 200 run with step states | 400 missing tenant, 403 cross-tenant, 404 unknown run |
| POST | `/v1/runs/{run_id}/claim` | 200 `{"step":...}`, 204 nothing ready | 400 bad request, 403 cross-tenant, 404 unknown run |
| POST | `/v1/runs/{run_id}/complete` | 200 updated run | 400 bad request, 403, 404 unknown step, 409 lease not held/expired |
| POST | `/v1/runs/{run_id}/fail` | 200 updated run | 400 bad request, 403, 404 unknown step, 409 lease not held/expired |
| POST | `/v1/runs/{run_id}/decision` | 200 updated run | 400 bad request/JSON, 403 cross-tenant, 404 unknown run/step, 409 conflict/not waiting/not an approval |
| GET | `/v1/runs?tenant=&status=&limit=&after=` | 200 `{"items":[...],"next_after":...}` | 400 missing tenant |

Errors are always `{"error": "<message>"}`.

## Step / run state machine

Task step states: `pending` -> `ready` -> `running` -> `succeeded` | `failed`.

- A step starts `pending` and becomes `ready` only when **all** dependencies are `succeeded`.
- `claim` (worker lease) moves a `ready` step to `running` and sets `lease_deadline`.
- `complete` moves `running` -> `succeeded` and is idempotent for the same worker.
- `fail` increments the attempt; while attempts remain the step returns to `ready`
  with `next_attempt_at = now + base * 2**(attempt-1)` (default base 1s).
- A lease past its deadline is reclaimed on the next `claim`, which records a
  `takeover` history entry naming the previous holder.
- Claim order is deterministic: `(topological index, step id)`, at most one active
  lease per step.

Approval step states: `pending` -> `waiting` -> `succeeded` | `failed`.

- Becomes `waiting` once all dependencies succeed (immediately with none), then
  waits for an HTTP `approve`/`reject` decision; it is never `ready`/`running`,
  is skipped by `claim`, and `complete`/`fail` on it return 409.
- `waiting` only blocks its own successors; independent task steps are unaffected.

Run states: `pending` -> `running` -> `succeeded` | `failed`.

- `pending` until a step is first claimed (or an approval gate starts waiting);
  `running` while work is outstanding.
- `succeeded` when every step is `succeeded`.
- `failed` as soon as any step exhausts `max_attempts` (default 3).

Every transition appends `{"at","run_id","step_id","type","attempt","worker_id"}`
to the run's append-only history; `Scheduler.replay(run_id)` rebuilds the current
states from that history and agrees with the stored document.

## Layout

```
flowd/model.py      DAG validation + topological order
flowd/store.py      atomic JSON persistence, tenants, runs, history
flowd/scheduler.py  claim/complete/fail, leases, retries, replay
flowd/http_app.py   ThreadingHTTPServer API
flowd/cli.py        argv parsing and JSON output
```
