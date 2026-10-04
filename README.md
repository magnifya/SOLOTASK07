# flowd

A minimal but real **distributed task scheduling / workflow orchestration backend**:
DAG definition and validation, run state machine, worker leases with expiry and
takeover, retries with exponential backoff, idempotent completion, human
**approval nodes**, append-only history with replay, multi-tenant isolation, and
a small JSON HTTP API. Standard library only: no pip installs, no network
access, no third-party imports.

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
`[{"id": "step1", "depends_on": []}, {"id": "step2", "depends_on": ["step1"], "max_attempts": 3}]`.
A step may set `"kind": "approval"` (default `"task"`) to insert a human gate;
the only accepted values are `task` and `approval`.

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
| POST | `/v1/runs/{run_id}/decision` | 200 updated run | 400 bad request/JSON, 403 cross-tenant, 404 unknown run/step, 409 not an approval/not waiting/already decided |
| GET | `/v1/runs?tenant=&status=&limit=&after=` | 200 `{"items":[...],"next_after":...}` | 400 missing tenant |

Errors are always `{"error": "<message>"}`.

### Approval decisions

`POST /v1/runs/{run_id}/decision` takes `{"tenant","step_id","actor","decision"}`.
`tenant`, `step_id` and `actor` must be non-empty strings after trimming;
`decision` is `"approve"` or `"reject"`. Missing fields, wrong types, blank
strings, illegal values and malformed JSON all return 400.

- First valid decision on a `waiting` approval returns 200 with the updated
  run. `approve` marks the node `succeeded` (attempt stays 0) and unlocks its
  successors; `reject` marks the node and the run `failed`, with successors
  left `pending`.
- Repeating the same `actor` + `decision` returns 200 and preserves the first
  record and timestamp (no extra history). A different actor, the opposite
  decision, deciding before the node is waiting, or targeting an ordinary task
  returns 409.
- Unknown run/step 404; deciding another tenant's run returns 403.
- Approval nodes are never leased, never retried and always report `attempt` 0;
  `claim` skips them and `complete`/`fail` return 409. They hold no lease fields
  and no `result`/`error`; the `approval` field is `null` until decided and then
  `{"actor","decision","at"}` using the existing UTC format.

## Step / run state machine

Step states: `pending` -> `ready` -> `running` -> `succeeded` | `failed`;
approval steps instead go `pending` -> `waiting` -> `succeeded` | `failed`.

- A step starts `pending` and becomes `ready` only when **all** dependencies are `succeeded`.
- An `approval` step becomes `waiting` under the same condition (immediately at
  run creation when it has no dependencies); waiting blocks only its successors.
- `claim` (worker lease) moves a `ready` task to `running` and sets `lease_deadline`;
  waiting approvals are never claimed.
- `complete` moves `running` -> `succeeded` and is idempotent for the same worker.
- `fail` increments the attempt; while attempts remain the step returns to `ready`
  with `next_attempt_at = now + base * 2**(attempt-1)` (default base 1s).
- Approval nodes take no lease and no retries (`attempt` stays 0); `complete`
  and `fail` against them return 409. A `decision` of `approve` succeeds the
  node and promotes successors; `reject` fails the node and the whole run.
- A lease past its deadline is reclaimed on the next `claim`, which records a
  `takeover` history entry naming the previous holder.
- Claim order is deterministic: `(topological index, step id)`, at most one active
  lease per step.

Run states: `pending` -> `running` -> `succeeded` | `failed`.

- `pending` until a step is first claimed; `running` while work is outstanding.
- `succeeded` when every step is `succeeded`.
- `failed` as soon as any step exhausts `max_attempts` (default 3) or an
  approval is rejected.

Every transition appends `{"at","run_id","step_id","type","attempt","worker_id"}`
to the run's append-only history (a `decision` event additionally carries
`actor` and `decision`); `Scheduler.replay(run_id)` rebuilds the current
states from that history and agrees with the stored document.

## Layout

```
flowd/model.py      DAG validation + topological order
flowd/store.py      atomic JSON persistence, tenants, runs, history
flowd/scheduler.py  claim/complete/fail, leases, retries, replay
flowd/http_app.py   ThreadingHTTPServer API
flowd/cli.py        argv parsing and JSON output
```
