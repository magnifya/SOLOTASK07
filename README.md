# flowd

A minimal but real **distributed task scheduling / workflow orchestration backend**:
DAG definition and validation, run state machine, worker leases with expiry and
takeover, retries with exponential backoff, idempotent completion, human
**approval nodes**, append-only history with replay, multi-tenant isolation, and
a small JSON HTTP API. Standard library only: no pip installs, no network
access, no third-party imports.

Data lives in a single directory: `<root>/<tenant>/workflows.json`,
`<root>/<tenant>/idempotency.json` (idempotency-key → run bindings) plus
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
| POST | `/v1/runs` | 201 run (new or replayed) | 400 bad request/max_parallelism/idempotency_key/params/not_before, 404 unknown workflow, 409 idempotency conflict |
| GET | `/v1/runs/{run_id}?tenant=` | 200 run with step states | 400 missing tenant, 403 cross-tenant, 404 unknown run |
| POST | `/v1/runs/{run_id}/claim` | 200 `{"step":...}`, 204 nothing ready | 400 bad request, 403 cross-tenant, 404 unknown run |
| POST | `/v1/runs/{run_id}/complete` | 200 updated run | 400 bad request, 403, 404 unknown step, 409 lease not held/expired |
| POST | `/v1/runs/{run_id}/fail` | 200 updated run | 400 bad request, 403, 404 unknown step, 409 lease not held/expired |
| POST | `/v1/runs/{run_id}/decision` | 200 updated run | 400 bad request/JSON, 403 cross-tenant, 404 unknown run/step, 409 not an approval/not waiting/already decided |
| POST | `/v1/runs/{run_id}/heartbeat` | 200 updated run | 400 bad request/JSON, 403 cross-tenant, 404 unknown run/step, 409 finished run/approval node/lease not held/expired |
| GET | `/v1/runs?tenant=&status=&limit=&after=` | 200 `{"items":[...],"next_after":...}` | 400 missing tenant |

Errors are always `{"error": "<message>"}`.

### Per-run concurrency quota

`POST /v1/runs` (and `Scheduler.start_run`) accept an optional
`max_parallelism`: the maximum number of simultaneously valid task leases the
run may hold. Omit it or pass `null` for unlimited; otherwise it must be a
positive integer. Booleans, floats, strings and non-positive integers are
rejected (Python raises `WorkflowError`, HTTP returns 400) and no run is
created. The quota is fixed at creation time and reported as `max_parallelism`
on the create response, run details, the run listing and `flowd status`; runs
written by older versions read back as `null`.

Only ordinary tasks that are `running` with `lease_deadline` strictly greater
than the current time count toward the quota; a deadline equal to the current
time is expired. Approval nodes and ready, waiting, pending-retry, succeeded
or failed tasks hold no slot, and one worker holding several tasks is counted
once per task. Expired leases are reclaimed via the usual takeover rules
before slots are counted; `heartbeat` only extends an existing lease and never
takes or releases a slot. When the quota is full, `Scheduler.claim` returns
`None`, HTTP claim returns 204 and the CLI prints its existing `step: null`
JSON: ready tasks stay ready and no claim history is appended (takeover
entries from leases reclaimed by the same call are still recorded). Quotas
are counted per `run_id`, so runs and tenants never share slots.

### Delayed start (`not_before`)

`POST /v1/runs` (and `Scheduler.start_run`) accept an optional `not_before`:
a UTC Unix timestamp in seconds. Omit it or pass `null` to open the root
nodes immediately (the legacy behavior). Any other value must be a finite,
non-negative integer or float; booleans, strings, negative numbers and
non-finite numbers (`NaN`, `Infinity`) are rejected with 400
(`bad_not_before`) and create no run.

A value at or before the current time behaves exactly like an immediate
start. A strictly future value creates a **sleeping** run: the run and every
node stay `pending` with `attempt` 0, no root task becomes `ready`, no root
approval becomes `waiting`, no history beyond `run_created` is appended, and
no quota slot is occupied. The run still appears in detail and list
responses, carrying its `not_before`.

`claim` is the only activation entry point. A claim while
`now < not_before` returns `None` (HTTP 204, the CLI keeps its
`step: null` output) and changes nothing: state, history and `updated_at`
are untouched. The first claim with `now >= not_before` opens the root
nodes — root tasks become `ready`, root approvals `waiting`, one history
event each — and then leases at most one task in the usual topological
order and under the quota. A run whose roots are all approvals activates on
that claim and still returns no task (204). Repeated claims never re-open a
node. Run detail/listing, idempotent re-creation and replay do not
activate, even once the time is due; an approval decision attempted before
activation returns 409 (`not_waiting`). After activation, completion,
retries, takeover, heartbeat and approval behavior are unchanged.

The scheduled time is part of the run document and survives a restart:
`Scheduler.replay` rebuilds the same run status, node states and
`not_before` whether the run was still sleeping or already activated. The
value is reported as `not_before` on the create response, run details, the
run listing and `flowd status`; runs written by older versions read it
back as `null`.

### Tenant-scoped idempotency keys

`POST /v1/runs` (and `Scheduler.start_run`) accept an optional
`idempotency_key`. Omit it or pass `null` to keep the legacy behavior: every
call creates a fresh run. Any other value must be a string that is non-empty
after trimming; whitespace is stripped from both ends and the trimmed key is
what is stored and matched. Non-strings and blank strings return 400
(`bad_idempotency_key`) and create nothing.

The first valid request for a `(tenant, key)` pair creates the run and stores
its binding in `<tenant>/idempotency.json`; every later request with the same
trimmed key returns that run's current state — including `succeeded` and
`failed` runs — as **201** with the normal run body and an `idempotency_key`
field. No new `run_id` is generated, steps are not reset, and a repeat appends
no history, does not touch `updated_at`, leases, approvals or quotas. Keys are
scoped per tenant, so the same key in two tenants is independent; bindings
survive a process restart over the same data directory.

When a key is present, `params` must be a JSON object (omit or `null` means
`{}`; arrays, strings, numbers and booleans return 400 `bad_params`). A repeat
matches the first creation semantically: object key order is ignored, array
order is significant, booleans are distinct from numbers (`true != 1`), and
ints/floats compare by numeric value (`1 == 1.0`). `workflow_id`, `params`,
`max_parallelism` and `not_before` must all equal the first request —
omitting `run_id` reuses the bound run, while an explicit `run_id` must
equal it; omitted or `null` quotas mean unlimited and differ from any
numeric quota; omitting `not_before` is equivalent to `null` and distinct
from an explicit timestamp, while int and float timestamps compare by
numeric value.

Validation order is fixed: an illegal key, `params` or quota returns 400 first
(`bad_idempotency_key`, then `bad_max_parallelism`, then `bad_params`), then
an illegal `not_before` returns 400 (`bad_not_before`); a legal request that
differs from the first creation returns 409
(`idempotency_conflict`); only a brand-new key pointing at an unknown workflow
returns 404 (`unknown_workflow`). Rejections create no run and never alter the
bound one. Matching always uses the first creation's stored content, so
re-submitting a workflow with the same id cannot change what a key replays.

Concurrent requests sharing one `WorkflowStore` are serialized by the store
lock: identical requests for one `(tenant, key)` all replay the single run, and
among differing requests only the content that wins creation is accepted while
the rest get 409.

The key is reported as `idempotency_key` on the create response, run details,
the listing, `flowd status` and `Scheduler.replay`. Runs written by older
versions read back as `null` and never participate in dedup; unkeyed creates
keep their existing semantics.

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
  Root steps open immediately at run creation unless the run has a future `not_before`, in
  which case they open on the first due `claim` (see [Delayed start](#delayed-start-not_before)).
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
- `heartbeat` (`{"tenant","step_id","worker_id","lease_seconds"?}`, default 30s)
  renews the lease on a `running` task held by the caller: the deadline becomes
  the later of its current value and `now + lease_seconds`. Only the deadline
  and `updated_at` change; a renewal that would not extend the deadline is a
  no-op that appends no history. Heartbeats against finished runs or approval
  nodes, foreign leases and expired deadlines are rejected with 409.
- Claim order is deterministic: `(topological index, step id)`, at most one active
  lease per step.

Run states: `pending` -> `running` -> `succeeded` | `failed`.

- `pending` until a step is first claimed; `running` while work is outstanding.
- `succeeded` when every step is `succeeded`.
- `failed` as soon as any step exhausts `max_attempts` (default 3) or an
  approval is rejected.

Every transition appends `{"at","run_id","step_id","type","attempt","worker_id"}`
to the run's append-only history (a `decision` event additionally carries
`actor` and `decision`; `claim` and `heartbeat` events carry the
`lease_deadline`); `Scheduler.replay(run_id)` rebuilds the current
states from that history and agrees with the stored document.

## Layout

```
flowd/model.py      DAG validation + topological order
flowd/store.py      atomic JSON persistence, tenants, runs, history
flowd/scheduler.py  claim/complete/fail, leases, retries, replay
flowd/http_app.py   ThreadingHTTPServer API
flowd/cli.py        argv parsing and JSON output
```
