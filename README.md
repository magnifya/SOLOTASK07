# flowd

A minimal but real **distributed task scheduling / workflow orchestration backend**:
DAG definition and validation, run state machine, worker leases with expiry and
takeover, retries with exponential backoff, idempotent completion, human
**approval nodes**, append-only history with replay, multi-tenant isolation, and
a small JSON HTTP API. Standard library only: no pip installs, no network
access, no third-party imports.

Data lives in a single directory: `<root>/<tenant>/workflows.json`,
`<root>/<tenant>/idempotency.json` (idempotency-key → run bindings),
`<root>/<tenant>/workers.json` (worker registrations),
`<root>/<tenant>/schedules.json` (periodic schedules),
`<root>/<tenant>/audit.json` (the tenant's audit stream) plus
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
| POST | `/v1/runs/{run_id}/claim` | 200 `{"step":...}`, 204 nothing ready | 400 bad request, 403 cross-tenant, 404 unknown run, 409 worker not registered/expired |
| POST | `/v1/tasks/claim` | 200 `{"run_id":...,"step":...}`, 204 nothing claimable | 400 bad tenant/worker_id/lease_seconds/JSON, 409 worker not registered/expired |
| POST | `/v1/runs/{run_id}/complete` | 200 updated run | 400 bad request, 403, 404 unknown step, 409 lease not held/expired |
| POST | `/v1/runs/{run_id}/fail` | 200 updated run | 400 bad request, 403, 404 unknown step, 409 lease not held/expired |
| POST | `/v1/runs/{run_id}/decision` | 200 updated run | 400 bad request/JSON, 403 cross-tenant, 404 unknown run/step, 409 not an approval/not waiting/already decided |
| POST | `/v1/runs/{run_id}/heartbeat` | 200 updated run | 400 bad request/JSON, 403 cross-tenant, 404 unknown run/step, 409 finished run/approval node/lease not held/expired |
| POST | `/v1/runs/{run_id}/cancel` | 200 cancelled run (repeat is a no-op) | 400 bad tenant/actor/JSON, 403 cross-tenant, 404 unknown run, 409 run already finished |
| POST | `/v1/workers/register` | 201 new registration, 200 refreshed registration | 400 bad tenant/worker_id/lease_seconds/JSON |
| POST | `/v1/workers/{worker_id}/heartbeat` | 200 registration record | 400 bad tenant/lease_seconds/JSON, 404 unknown worker, 409 worker_expired |
| GET | `/v1/workers?tenant=` | 200 `{"tenant":...,"items":[...]}` sorted by worker_id | 400 missing tenant |
| POST | `/v1/schedules` | 201 schedule record with `next_at` | 400 bad_tenant/bad_schedule_id/bad_interval/bad_first_at/bad_params/bad_max_parallelism/bad JSON, 404 unknown workflow, 409 schedule_exists |
| GET | `/v1/schedules?tenant=` | 200 `{"tenant":...,"items":[...]}` sorted by schedule_id | 400 missing tenant |
| POST | `/v1/schedules/{schedule_id}/dispatch` | 201 scheduled run, 204 not due yet | 400 bad_tenant/bad_schedule_id/bad JSON, 403 cross-tenant, 404 unknown schedule |
| GET | `/v1/runs?tenant=&status=&limit=&after=` | 200 `{"items":[...],"next_after":...}` | 400 missing tenant |
| GET | `/v1/audit?tenant=&action=&run_id=&limit=&after=` | 200 `{"tenant":...,"items":[...],"next_after":...}` | 400 bad_tenant/bad_action/bad_run_id/bad_limit/bad_after |

Errors are always `{"error": "<message>"}`.

### Tenant-scoped audit stream

Every state change also appends one record to the tenant's audit stream,
stored in `<tenant>/audit.json` and readable via
`GET /v1/audit?tenant=` (or `Scheduler.list_audit`). Each record is

```json
{"sequence": 7, "at": "2026-01-01T00:00:00.000000Z", "tenant": "acme",
 "action": "claim", "run_id": "r1", "step_id": "step1",
 "workflow_id": "etl", "schedule_id": null, "worker_id": "w1", "actor": null}
```

Run actions reuse the existing history types (`run_created`, `ready`,
`waiting`, `claim`, `heartbeat`, `takeover`, `complete`, `fail`, `retry`,
`attempts_exhausted`, `skipped`, `decision`, `cancel`, `run_started`,
`run_succeeded`, `run_failed`, `run_cancelled`); control-plane operations use fixed `resource.action`
identifiers: `workflow.submit`, `worker.register`, `worker.heartbeat`,
`schedule.create` and `schedule.dispatch`. Records carry only identifiers
and the actor (the decision maker on `decision` records, the canceller on
`cancel`/`run_cancelled` records, `null` elsewhere)
— never params, results or error text; identifiers that do not apply are
`null`. When one request causes several changes (e.g. two lease takeovers
plus a claim), the records enter the stream in the order the changes
actually happened.

`sequence` starts at 1 per tenant and is assigned under the store lock in
commit order, so it is gap-free, unique under concurrency and keeps
increasing across restarts. Query parameters: `tenant` is required and
must be a non-empty string (`bad_tenant`); `limit` defaults to 100 and
accepts only integers from 1 to 1000 (`bad_limit`); `after` defaults to 0
and returns only records with a greater sequence (`bad_after`); `action`
and `run_id` are optional exact-match filters that must be non-empty when
given (`bad_action` / `bad_run_id`). The response is
`{"tenant", "items", "next_after"}` with items in ascending sequence
order; `next_after` is the sequence of the page's last record, or `null`
when no further records follow. A tenant with no records returns 200 with
empty `items`.

Requests that change nothing append nothing: validation failures,
cross-tenant rejections, read-only requests, idempotency-key replays,
not-yet-due dispatches, lease renewals that do not extend the deadline and
204 responses with no state change are all absent from the stream. Reading
the audit stream never touches runs, leases, history or `updated_at`, and
a query only ever sees the requested tenant's own records — filters cannot
probe other tenants. Run history, `Scheduler.history`/`Scheduler.replay`,
the other HTTP routes, error statuses and the CLI output are unchanged.

### Tenant-wide fair claim

`POST /v1/tasks/claim` (`Scheduler.claim_fair`) leases one task from the
tenant's least-claimed eligible run, without naming a run:

```json
{"tenant": "acme", "worker_id": "w1", "lease_seconds": 30}
```

`tenant` and `worker_id` are trimmed and must be non-empty (`bad_tenant` /
`bad_worker`); `lease_seconds` may be omitted (default 30) and otherwise
must be a finite number strictly greater than zero (`bad_lease`); malformed
JSON or a non-object body is `bad_json`. All are 400. Once the tenant has
any worker registration the same gate as the single-run claim applies:
unregistered workers get 409 `worker_not_registered`, expired ones 409
`worker_expired`.

The call scans the tenant's runs. First, sleeping runs whose `not_before`
has come due are activated (root nodes opened, one `ready`/`waiting` event
each) in `run_id` order. Then, among the candidates — runs that are not
terminal, not still sleeping, hold at least one `ready` ordinary task whose
retry backoff has elapsed (`next_attempt_at` reached; approval-only runs
never qualify) and have not reached their
`max_parallelism` — the winner is chosen by fairness: fewest `claim`
events in the run's history, then `created_at`, then `run_id`. Expired
leases are reclaimed with the usual `takeover` events before slots are
counted, exactly like the single-run claim, and those events are recorded
even when the run is not selected. The winner leases its next task in
`(topological index, step id)` order, recording the usual `claim` and, when
the run leaves `pending`, `run_started` events.

Success returns 200 with `{"run_id": ..., "step": ...}` where `step` is the
same object the single-run claim returns. When no run is eligible the
response is 204 and no `claim` event is appended. At most one task is
leased per call. The fairness count is rebuilt from the append-only
history on every call, so restarts, `Scheduler.replay` and concurrent
requests (serialized by the store lock) all agree on the same choice. Only
the tenant's own runs are scanned; every other endpoint is unchanged.


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
are counted per `run_id`; tenants can additionally share one budget across
all of their runs via the tenant quota below.

### Tenant concurrency quota

`POST /v1/quotas` creates or updates the calling tenant's concurrency quota
and `GET /v1/quotas?tenant=` reads it back. The body must be a JSON object
(`bad_json` otherwise); `tenant` must be a string that is non-empty after
trimming (`bad_tenant`); `max_parallelism` may be omitted or `null` for
unlimited, otherwise it must be a positive integer — booleans, floats,
strings and non-positive integers are rejected (`bad_max_parallelism`). The
first write returns 201 and later updates 200; both responses carry the
normalized `tenant`, the current `max_parallelism` and `updated_at`.
Rewriting the same value changes nothing (same `updated_at`, no audit
record); a real change appends one `quota.set` record to the tenant's audit
stream. Validation failures and read-only queries append nothing. Reading
an unconfigured tenant returns 200 with `max_parallelism: null`; each
tenant only ever sees the record stored under its own name, and the record
survives a restart.

Once a quota is configured, every single-run claim and every fair claim
(`POST /v1/tasks/claim`) shares one active-lease budget across all of the
tenant's runs. Under the same store lock the claim first reclaims every
expired ordinary-task lease of the tenant (recording the usual `takeover`
history and audit entries), then counts ordinary tasks that are `running`
with `lease_deadline` strictly later than now — approval nodes and ready,
pending-retry, succeeded or failed steps hold no slot. A full budget
returns the usual `None` / HTTP 204: no claim event is appended, but
takeovers recorded by this call's reclaim are kept. With capacity free, the
existing run selection, fairness counts, topological order and per-run
`max_parallelism` all apply unchanged, and a successful lease occupies
exactly one slot. Lowering the quota never revokes existing leases; slots
freed by completion, failure, cancellation or takeover are immediately
reusable, and concurrent claims are serialized by the store lock. Tenants
without a configured quota keep the previous behavior exactly.

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

### Periodic schedules

`POST /v1/schedules` creates a persistent periodic schedule for a tenant:

```json
{"tenant": "acme", "schedule_id": "nightly", "workflow_id": "etl",
 "interval_seconds": 3600, "first_at": 1700003600,
 "params": {"mode": "full"}, "max_parallelism": 4}
```

`first_at`, `params` and `max_parallelism` are optional. Times are UTC
Unix seconds (ints or floats): omitting `first_at` (or passing `null`)
uses the creation moment, so the schedule is immediately due. The trimmed
`schedule_id` must be non-empty; `interval_seconds` must be a finite
number strictly greater than zero (booleans, strings, zero, negatives,
`NaN` and infinities are rejected); `first_at` must be a finite
non-negative number; `params` must be a JSON object (omit/`null` → `{}`);
`max_parallelism` uses the usual positive-integer validation. The error
codes are `bad_tenant`, `bad_schedule_id`, `bad_interval`, `bad_first_at`,
`bad_params`, `bad_max_parallelism`; an unknown workflow is
`unknown_workflow` (404) and a duplicate id within the same tenant is
`schedule_exists` (409). The response is the stored record and includes
`first_at` and `next_at`. `GET /v1/schedules?tenant=` returns the
tenant's schedules sorted by `schedule_id`.

Schedules do not run on a timer: they produce runs only when an
external trigger calls
`POST /v1/schedules/{schedule_id}/dispatch` with `{"tenant": ...}`.

- Before the schedule is due (`now < next_at`) the response is **204**
  with no body: no run is created and neither `next_at`, `updated_at`,
  any run, lease nor history is touched. An unknown schedule returns 404
  (`unknown_schedule`); another tenant's schedule returns 403
  (`cross_tenant`), checked before any firing.
- When due, the call atomically creates **one** run for exactly the
  current trigger point `scheduled_at = next_at` and advances
  `next_at += interval_seconds`. The run is created by the ordinary
  `POST /v1/runs` path (`first_at <= now`, so roots open normally),
  inheriting the schedule's `params` and `max_parallelism`, and carries
  `schedule_id` and `scheduled_at`. The response is **201** with the
  normal run body. Repeating the dispatch before the next trigger is due
  returns 204 and never creates a second run, because the create and the
  `next_at` advance happen together under the store lock.
- If several trigger points were missed, each due dispatch processes the
  oldest one (`scheduled_at` values come out in chronological order);
  callers simply repeat dispatch until they get a 204 to catch up.

The schedule record and `next_at` live in `<tenant>/schedules.json` and
survive a process restart over the same data directory. Runs created by
a schedule are ordinary runs in every respect — execution, approvals,
retries, lease takeover, heartbeat, idempotency and replay are unchanged
— and manual `POST /v1/runs` still creates exactly one run, reported
with `schedule_id` and `scheduled_at` set to `null`. Both fields also
appear on run detail/listing responses and `Scheduler.replay`; runs
written by older versions read them back as `null`.

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

### Trigger rules (`trigger_rule`)

Every step may set an optional `trigger_rule` controlling when its
dependencies unlock it. Omitting the field (or passing `null`) means
`all_success` — the legacy rule, so existing definitions are unchanged.
The accepted values are:

- `all_success` (default): unlock when **every** dependency is
  `succeeded`.
- `all_done`: unlock when every dependency reached a terminal state
  (`succeeded`, `failed`, `cancelled` or `skipped`), regardless of
  outcome.
- `any_success`: unlock as soon as **any** one dependency is
  `succeeded`.

A non-string value, an empty string or any other name is rejected with
400 (`bad_trigger_rule`): the workflow is neither saved nor overwritten
and no audit record is written. Root steps (no dependencies) open
immediately under all three rules. The rule is part of the normalized
plan, is frozen into the run's `plan` snapshot at creation and is
reported as `trigger_rule` on every step of the run details; runs
written by older versions read it back as `all_success`.

Scheduling treats `succeeded`, `failed`, `cancelled` and `skipped` as
terminal step states. When a dependency fails terminally (retries
exhausted or approval rejected) or is skipped, pending dependents are
re-evaluated in topological order: an `all_success` node whose condition
can never hold again becomes `skipped`, an `any_success` node whose
dependencies are all terminal without a success becomes `skipped`, and
each skipped node records exactly one `skipped` history event (which
also enters the tenant's audit stream). A `skipped` node holds no lease,
keeps `attempt` 0, is never claimed (`complete`/`fail` against it
conflict) and cascades to its own dependents in the same pass. When no
node is executable, `claim` keeps returning `None` / 204.

A run containing at least one non-`all_success` rule stays
`pending`/`running` while any node can still unlock, execute or wait for
an approval, and finishes only once **every** node is terminal: it is
`failed` when any node failed, otherwise `succeeded` (`skipped` nodes
alone do not fail a run); an explicit cancel still ends it as
`cancelled`. Runs made entirely of `all_success` steps keep the legacy
semantics exactly: the run fails the moment a step exhausts its retries
or an approval is rejected, and surviving successors stay `pending`.
Run details, `Scheduler.history` and `Scheduler.replay` all present the
new states and rules consistently, and replay rebuilds them after a
restart.

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

### Worker registration and health

Workers may register with a tenant so claim traffic can be limited to live,
known workers. Registration is opt-in per tenant: until a tenant has any
registration record, claims keep the legacy rule (any non-empty
`worker_id`). The moment at least one record exists, claims are gated.

- `POST /v1/workers/register` takes `{"tenant","worker_id","lease_seconds"?}`
  (default 30). `tenant` and `worker_id` are trimmed and must be non-empty;
  `lease_seconds` must be a finite number strictly greater than zero
  (booleans, strings, `null`, zero, negatives, `NaN` and infinities return
  400 and write nothing). The trimmed `worker_id` is what is stored. The
  first registration for a `(tenant, worker_id)` returns **201**; a repeat
  registration returns **200** and refreshes the expiry while preserving the
  original `registered_at`.
- The response is
  `{"worker_id","status","registered_at","last_heartbeat","expires_at"}`.
  `status` is `active` while the current time is strictly earlier than
  `expires_at`, otherwise `expired`; it is recomputed on every read.
- `POST /v1/workers/{worker_id}/heartbeat` takes
  `{"tenant","lease_seconds"?}` and sets the expiry to the later of the
  current expiry and `now + lease_seconds` (it never shortens). An unknown
  worker returns 404 (`unknown_worker`); an expired worker returns 409
  (`worker_expired`) and must re-register. Success returns the record.
- `GET /v1/workers?tenant=` returns the tenant's records sorted by
  `worker_id` with live status; a missing/blank tenant returns 400.
- Once the tenant has any records, `claim` accepts only an `active`
  registration belonging to that same tenant: an unregistered id returns 409
  (`worker_not_registered`), an expired registration 409
  (`worker_expired`), and another tenant's worker still returns 403 (checked
  first). Empty/blank `worker_id` remains 400. Empty tenants and tenants with
  no records are unaffected, as are `complete`/`fail` lease checks, the
  run-level lease `heartbeat`, expiry/takeover, task ordering and the 204
  "nothing ready" response.

Registrations live in `<tenant>/workers.json` (atomic writes) and survive a
process restart over the same directory; status is always derived from
`expires_at`, so no background reaper is needed. Registration, worker
heartbeats and listing append nothing to any run's history and never touch
`updated_at`, leases, approvals, quotas or replay.

## Step / run state machine

Step states: `pending` -> `ready` -> `running` -> `succeeded` | `failed`;
approval steps instead go `pending` -> `waiting` -> `succeeded` | `failed`;
a `pending` step whose trigger rule (see above) becomes unsatisfiable goes
straight to `skipped`.

- A step starts `pending` and becomes `ready` only when **all** dependencies are `succeeded`.
  Root steps open immediately at run creation unless the run has a future `not_before`, in
  which case they open on the first due `claim` (see [Delayed start](#delayed-start-not_before)).
- An `approval` step becomes `waiting` under the same condition (immediately at
  run creation when it has no dependencies); waiting blocks only its successors.
- `claim` (worker lease) moves a `ready` task to `running` and sets `lease_deadline`;
  waiting approvals are never claimed. A `ready` task whose `next_attempt_at`
  still lies in the future is not leased: `claim` skips it (other due `ready`
  tasks are still handed out in the usual order) and, when nothing is
  claimable, returns `None` / 204 without touching the run's state, history
  or `updated_at`. At exactly `next_attempt_at` the task becomes claimable
  again and a successful claim clears the field.
- `complete` moves `running` -> `succeeded` and is idempotent for the same worker.
- `fail` increments the attempt; while attempts remain the step returns to `ready`
  with `next_attempt_at = now + base * 2**(attempt-1)` (default base 1s), and
  the `retry` history event carries that `next_attempt_at` so `replay` and
  restarted schedulers restore the same waiting period.
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
`lease_deadline`; a `retry` event carries the scheduled `next_attempt_at`);
`Scheduler.replay(tenant, run_id)` rebuilds the current states from that
history and agrees with the stored document on every observable field.

### Workflow versions and same-name submissions

The normalized DAG definition used when a run is created — step order, node
`kind`, dependencies and `max_attempts` — is snapshotted into the run
document (`plan`) at creation time and never changes afterwards. Submitting
another workflow under the same `workflow_id` in the same tenant only
governs runs created after that submission; it cannot rewrite an existing
run's nodes, order, kinds, dependencies, retry limits or state machine.

`Scheduler.replay` rebuilds a run exclusively from its frozen definition
plus the append-only history, never from the workflow currently registered
under the run's `workflow_id`: a later same-name submission can neither
drop old nodes, introduce new ones nor change which dependencies unlock a
node, and replay even works after the registered workflow is deleted.
Documents written before snapshots existed reconstruct the frozen plan from
the node definitions and `step_order` stored in the run document itself.
Replay is idempotent and deterministic — replaying twice or after a restart
returns the identical document — and unknown run ids / cross-tenant reads
still return `unknown_run` / `cross_tenant`.

## Layout

```
flowd/model.py      DAG validation + topological order
flowd/store.py      atomic JSON persistence, tenants, runs, history, schedules, audit
flowd/scheduler.py  claim/complete/fail, leases, retries, replay, audit stream
flowd/http_app.py   ThreadingHTTPServer API
flowd/cli.py        argv parsing and JSON output
```
