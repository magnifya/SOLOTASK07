"""Scheduling engine: run state machine, leases, retries, history, replay.

All state transitions funnel through :class:`Scheduler` so the append-only
history stays the single source of truth.  ``replay`` rebuilds current state
from that history and must agree with the stored document.
"""

import math
import time as _time

from .model import KIND_APPROVAL, KIND_TASK, WorkflowError, format_time
from .store import (
    RUN_FAILED,
    RUN_PENDING,
    RUN_RUNNING,
    RUN_SUCCEEDED,
    STEP_FAILED,
    STEP_PENDING,
    STEP_READY,
    STEP_RUNNING,
    STEP_SUCCEEDED,
    STEP_WAITING,
    TERMINAL_RUN_STATES,
    WorkflowStore,
    new_run,
)


class LeaseError(WorkflowError):
    """Raised when a worker does not hold the lease it is acting on."""

    def __init__(self, message, code="lease_not_held"):
        super().__init__(message, code)


class ConflictError(WorkflowError):
    """Raised for an action that is incompatible with the step's state."""

    def __init__(self, message, code="conflict"):
        super().__init__(message, code)


WORKER_ACTIVE = "active"
WORKER_EXPIRED = "expired"
DEFAULT_WORKER_LEASE = 30


def backoff_seconds(attempt, base=1.0):
    """Exponential backoff: ``base * 2 ** (attempt - 1)``."""
    return float(base) * (2 ** (max(1, int(attempt)) - 1))


def normalize_max_parallelism(value):
    """Validate an optional per-run concurrency quota.

    ``None`` means unlimited; only positive integers are accepted (booleans,
    floats, strings and non-positive integers are rejected).
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise WorkflowError(
            "max_parallelism must be a positive integer or null", "bad_max_parallelism"
        )
    return int(value)


def normalize_idempotency_key(value):
    """Validate an optional idempotency key.

    ``None`` means idempotency is disabled and the call keeps its legacy
    behavior.  Otherwise the value must be a string that is non-empty after
    trimming; the trimmed key is what gets stored and matched.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise WorkflowError("idempotency_key must be a string", "bad_idempotency_key")
    key = value.strip()
    if not key:
        raise WorkflowError("idempotency_key must be a non-empty string", "bad_idempotency_key")
    return key


def normalize_tenant(value):
    """Validate a tenant: a string that is non-empty after trimming."""
    if not isinstance(value, str):
        raise WorkflowError("tenant must be a string", "bad_tenant")
    tenant = value.strip()
    if not tenant:
        raise WorkflowError("tenant must be a non-empty string", "bad_tenant")
    return tenant


def normalize_worker_id(value):
    """Validate a worker id: a string that is non-empty after trimming."""
    if not isinstance(value, str):
        raise WorkflowError("worker_id must be a string", "bad_worker")
    worker_id = value.strip()
    if not worker_id:
        raise WorkflowError("worker_id must be a non-empty string", "bad_worker")
    return worker_id


def normalize_worker_lease(value):
    """Validate an optional worker-registration lease in seconds.

    ``None`` (omitted) defaults to 30 seconds.  Otherwise the value must be
    a finite number strictly greater than zero; booleans, strings, NaN and
    infinities are rejected.
    """
    if value is None:
        return float(DEFAULT_WORKER_LEASE)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WorkflowError("lease_seconds must be a positive number", "bad_lease")
    try:
        seconds = float(value)
    except OverflowError:
        seconds = float("inf")
    if not math.isfinite(seconds) or seconds <= 0:
        raise WorkflowError("lease_seconds must be a positive number", "bad_lease")
    return seconds


def normalize_params(value):
    """Validate ``params`` on a keyed create: object only, null/omit = ``{}``."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise WorkflowError("params must be a JSON object", "bad_params")
    return value


def normalize_schedule_id(value):
    """Validate a schedule id: a string that is non-empty after trimming."""
    if not isinstance(value, str):
        raise WorkflowError("schedule_id must be a string", "bad_schedule_id")
    schedule_id = value.strip()
    if not schedule_id:
        raise WorkflowError("schedule_id must be a non-empty string", "bad_schedule_id")
    return schedule_id


def normalize_interval_seconds(value):
    """Validate a schedule interval.

    The value must be a finite number strictly greater than zero; booleans,
    strings, ``NaN`` and infinities are rejected.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WorkflowError("interval_seconds must be a finite positive number",
                            "bad_interval")
    try:
        seconds = float(value)
    except OverflowError:
        seconds = float("inf")
    if not math.isfinite(seconds) or seconds <= 0:
        raise WorkflowError("interval_seconds must be a finite positive number",
                            "bad_interval")
    return seconds


def normalize_first_at(value):
    """Validate an optional schedule first-fire time in UTC Unix seconds.

    ``None`` (omitted or explicit ``null``) means the creation time and is
    filled in by the caller.  Otherwise the value must be a finite,
    non-negative int or float (booleans, strings, NaN and infinities are
    rejected).
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WorkflowError(
            "first_at must be a finite non-negative number of Unix seconds or null",
            "bad_first_at",
        )
    try:
        seconds = float(value)
    except OverflowError:
        seconds = float("inf")
    if not math.isfinite(seconds) or seconds < 0:
        raise WorkflowError(
            "first_at must be a finite non-negative number of Unix seconds or null",
            "bad_first_at",
        )
    return seconds


def normalize_schedule_params(value):
    """Validate ``params`` on a schedule: object only, null/omit = ``{}``."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise WorkflowError("params must be a JSON object", "bad_params")
    return value


def normalize_not_before(value):
    """Validate an optional delayed-start time in UTC Unix seconds.

    ``None`` (omitted or explicit ``null``) means start immediately.
    Otherwise the value must be a finite, non-negative int or float
    (booleans, strings, NaN and infinities are rejected).
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WorkflowError(
            "not_before must be a finite non-negative number of Unix seconds or null",
            "bad_not_before",
        )
    try:
        seconds = float(value)
    except OverflowError:
        seconds = float("inf")
    if not math.isfinite(seconds) or seconds < 0:
        raise WorkflowError(
            "not_before must be a finite non-negative number of Unix seconds or null",
            "bad_not_before",
        )
    return seconds


def params_equal(left, right):
    """Semantic JSON equality for run params.

    Object key order is ignored, array order is preserved, booleans are not
    numbers (``True != 1``), and numeric ints/floats compare by numeric value
    (``1 == 1.0``).
    """
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    if isinstance(left, dict) or isinstance(right, dict):
        if not (isinstance(left, dict) and isinstance(right, dict)):
            return False
        if left.keys() != right.keys():
            return False
        return all(params_equal(left[name], right[name]) for name in left)
    if isinstance(left, list) or isinstance(right, list):
        if not (isinstance(left, list) and isinstance(right, list)):
            return False
        return len(left) == len(right) and all(
            params_equal(a, b) for a, b in zip(left, right)
        )
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    return left == right


def _retry_due(step, now_iso):
    """Whether a ready step's backoff has elapsed (or it never failed).

    ``next_attempt_at`` is set when a failure re-queues the step with
    remaining attempts; the step becomes claimable again only once the
    current time reaches that moment.  Both timestamps come from
    :func:`format_time`, so lexicographic comparison is chronological.
    """
    next_attempt_at = step.get("next_attempt_at")
    return next_attempt_at is None or next_attempt_at <= now_iso


def _active_lease_count(run, now):
    """Count ordinary tasks in this run holding a strictly unexpired lease.

    Approval nodes and tasks that are ready, waiting, succeeded, failed or
    pending retry do not count; a deadline equal to ``now`` is expired.
    Each held task counts once, even when one worker holds several.
    """
    return sum(
        1
        for step in run["steps"].values()
        if step.get("kind") != KIND_APPROVAL
        and step["status"] == STEP_RUNNING
        and step.get("lease_deadline") is not None
        and float(step["lease_deadline"]) > now
    )


def _promote_pending(run, now_iso):
    """Promote pending steps whose dependencies all succeeded.

    Ordinary tasks become ``ready``; approval steps become ``waiting`` for a
    human decision.  Returns ``(ready_ids, waiting_ids)``.
    """
    index = run["order_index"]
    ready, waiting = [], []
    for sid in sorted(run["steps"], key=lambda s: index.get(s, 0)):
        step = run["steps"][sid]
        if step["status"] != STEP_PENDING:
            continue
        if all(run["steps"][d]["status"] == STEP_SUCCEEDED for d in step["depends_on"]):
            if step.get("kind") == KIND_APPROVAL:
                step["status"] = STEP_WAITING
                waiting.append(sid)
            else:
                step["status"], step["ready_at"] = STEP_READY, now_iso
                ready.append(sid)
    return ready, waiting


def _derive_run_status(run):
    """Derive the run status from its steps; stays ``pending`` until claimed."""
    steps = list(run["steps"].values())
    if any(s["status"] == STEP_FAILED for s in steps):
        return RUN_FAILED
    if steps and all(s["status"] == STEP_SUCCEEDED for s in steps):
        return RUN_SUCCEEDED
    if any(s["status"] in (STEP_RUNNING, STEP_SUCCEEDED) for s in steps):
        return RUN_RUNNING
    return RUN_PENDING


def _roots_opened(run):
    """Whether the run's root nodes were already opened (activation done).

    A sleeping (delayed) run has exactly one history event, ``run_created``;
    activation appends one ``ready``/``waiting`` event per root node, and a
    validated workflow always has at least one root.
    """
    return any(e.get("type") in ("ready", "waiting") for e in run.get("history") or [])


class Scheduler:
    """Coordinates workers against a :class:`WorkflowStore`."""

    def __init__(self, store, clock=None, backoff_base=1.0, sleep=None):
        self.store = store
        self._clock = clock if clock is not None else store.now
        self.backoff_base = float(backoff_base)
        self._sleep = sleep if sleep is not None else _time.sleep

    def now(self):
        return float(self._clock())

    def _iso(self, when=None):
        return format_time(self.now() if when is None else when)

    def _event(self, run, run_id, step_id, etype, attempt=None, worker_id=None, **extra):
        event = {"at": self._iso(), "run_id": run_id, "step_id": step_id, "type": etype,
                 "attempt": attempt, "worker_id": worker_id}
        event.update(extra)
        run["history"].append(event)
        # Mirror the event into the tenant's audit stream under the history
        # type as its action; payloads (result/error/decision) stay out.
        self.store.append_audit(run.get("tenant"), etype, at=event["at"],
                                run_id=run_id, step_id=step_id, worker_id=worker_id,
                                actor=extra.get("actor", worker_id))
        return event

    # -- workflow / run lifecycle --------------------------------------
    def submit(self, tenant, workflow_id, steps):
        with self.store.locked():
            plan = self.store.save_workflow(tenant, workflow_id, steps)
            self.store.append_audit(tenant, "workflow.submit", at=self._iso(),
                                    workflow_id=plan["workflow_id"])
            return plan

    def start_run(self, tenant, workflow_id, run_id=None, params=None, max_parallelism=None,
                  idempotency_key=None, not_before=None, schedule_id=None, scheduled_at=None):
        key = normalize_idempotency_key(idempotency_key)
        max_parallelism = normalize_max_parallelism(max_parallelism)
        if key is not None:
            params = normalize_params(params)
        not_before = normalize_not_before(not_before)
        with self.store.locked():
            if key is not None:
                return self._start_idempotent_run(
                    tenant, workflow_id, run_id, params, max_parallelism, key, not_before,
                    schedule_id, scheduled_at
                )
            plan = self.store.get_workflow(tenant, workflow_id)
            run_id = run_id or self.store.generate_run_id()
            if self.store.run_exists(run_id) == tenant:
                raise WorkflowError("run already exists: %s" % run_id, "duplicate_run")
            run = new_run(tenant, workflow_id, run_id, params, plan, self._iso(),
                          max_parallelism=max_parallelism, not_before=not_before,
                          schedule_id=schedule_id, scheduled_at=scheduled_at)
            return self._create_run(run, plan)

    def _start_idempotent_run(self, tenant, workflow_id, run_id, params, max_parallelism, key,
                              not_before, schedule_id=None, scheduled_at=None):
        """Create once per (tenant, trimmed key); replay identical requests.

        Runs inside the store lock so concurrent identical requests share one
        run while differing requests conflict against the first creation.
        """
        index = self.store.load_idempotency(tenant)
        record = index.get(key)
        if record is not None:
            existing = self.get_run(tenant, record["run_id"])
            if run_id is not None and run_id != existing["run_id"]:
                raise ConflictError(
                    "idempotency_key %s is already bound to run %s"
                    % (key, existing["run_id"]),
                    "idempotency_conflict",
                )
            if existing["workflow_id"] != workflow_id \
                    or not params_equal(existing.get("params") or {}, params) \
                    or existing.get("max_parallelism") != max_parallelism \
                    or existing.get("not_before") != not_before:
                raise ConflictError(
                    "request differs from the run first created with idempotency_key %s" % key,
                    "idempotency_conflict",
                )
            # A repeat changes nothing: no history, no updated_at, no lease.
            return existing
        plan = self.store.get_workflow(tenant, workflow_id)
        run_id = run_id or self.store.generate_run_id()
        if self.store.run_exists(run_id) == tenant:
            raise WorkflowError("run already exists: %s" % run_id, "duplicate_run")
        run = new_run(tenant, workflow_id, run_id, params, plan, self._iso(),
                      max_parallelism=max_parallelism, idempotency_key=key,
                      not_before=not_before, schedule_id=schedule_id,
                      scheduled_at=scheduled_at)
        self._create_run(run, plan)
        index[key] = {"run_id": run_id}
        self.store.save_idempotency(tenant, index)
        return run

    def _create_run(self, run, plan):
        """Append creation events and persist a freshly built run.

        A run with a future ``not_before`` stays fully ``pending``: its root
        nodes are opened (one ``ready``/``waiting`` history event each) only
        when the first claim arrives at or after that time.  A past, present
        or missing start time opens the roots immediately, exactly like the
        legacy create behavior.
        """
        run_id = run["run_id"]
        self._event(run, run_id, None, "run_created")
        not_before = run.get("not_before")
        if not_before is None or float(not_before) <= self.now():
            self._open_root_nodes(run)
        run["status"], run["updated_at"] = _derive_run_status(run), self._iso()
        return self.store.save_run(run)

    def _open_root_nodes(self, run, when=None):
        """Promote every pending node whose dependencies all succeeded.

        Each promoted node records exactly one ``ready`` or ``waiting``
        history event, so calling this on repeated claims never re-opens a
        node.  Returns ``(ready_ids, waiting_ids)``.
        """
        now_iso = self._iso() if when is None else when
        ready, waiting = _promote_pending(run, now_iso)
        for sid in ready:
            self._event(run, run["run_id"], sid, "ready",
                        attempt=run["steps"][sid]["attempt"])
        for sid in waiting:
            self._event(run, run["run_id"], sid, "waiting", attempt=0)
        return ready, waiting

    def get_run(self, tenant, run_id):
        run = self.store.load_run(tenant, run_id)
        if run["tenant"] != tenant:
            raise WorkflowError("run %s belongs to another tenant" % run_id, "cross_tenant")
        return run

    def list_runs(self, tenant, status=None, limit=50, after=None):
        return self.store.list_runs(tenant, status=status, limit=limit, after=after)

    def history(self, run_id, tenant=None):
        if tenant is None:
            tenant = self.store.run_exists(run_id)
            if tenant is None:
                raise WorkflowError("unknown run: %s" % run_id, "unknown_run")
        return list(self.get_run(tenant, run_id)["history"])

    # -- audit stream ----------------------------------------------------
    def audit(self, tenant, action=None, run_id=None, limit=100, after=0):
        """Read the tenant's audit stream: ``{"tenant", "items", "next_after"}``.

        ``limit`` defaults to 100 and must be an integer in [1, 1000];
        ``after`` defaults to 0 and only records with a greater sequence are
        returned.  ``action`` and ``run_id`` are optional non-empty-string
        filters.  Reading is side-effect free: runs, leases, history and
        ``updated_at`` are never touched, and only the requested tenant's
        records are visible.
        """
        tenant = normalize_tenant(tenant)
        if limit is None:
            limit = 100
        if isinstance(limit, bool) or not isinstance(limit, int) \
                or limit < 1 or limit > 1000:
            raise WorkflowError("limit must be an integer between 1 and 1000", "bad_limit")
        if after is None:
            after = 0
        if isinstance(after, bool) or not isinstance(after, int) or after < 0:
            raise WorkflowError("after must be a non-negative integer", "bad_after")
        if action is not None:
            if not isinstance(action, str) or not action.strip():
                raise WorkflowError("action must be a non-empty string", "bad_action")
            action = action.strip()
        if run_id is not None:
            if not isinstance(run_id, str) or not run_id.strip():
                raise WorkflowError("run_id must be a non-empty string", "bad_run_id")
            run_id = run_id.strip()
        items, next_after = self.store.query_audit(tenant, action=action, run_id=run_id,
                                                   limit=limit, after=after)
        return {"tenant": tenant, "items": items, "next_after": next_after}

    # -- periodic schedules -------------------------------------------
    def create_schedule(self, tenant, schedule_id, workflow_id, interval_seconds,
                        first_at=None, params=None, max_parallelism=None):
        """Create a persistent periodic schedule for one tenant.

        ``first_at`` omitted (or ``null``) defaults to the current time.
        Returns the stored record; a duplicate id within the same tenant
        conflicts (``schedule_exists``).
        """
        tenant = normalize_tenant(tenant)
        schedule_id = normalize_schedule_id(schedule_id)
        interval = normalize_interval_seconds(interval_seconds)
        first_at = normalize_first_at(first_at)
        params = normalize_schedule_params(params)
        max_parallelism = normalize_max_parallelism(max_parallelism)
        with self.store.locked():
            schedules = self.store.load_schedules(tenant)
            if schedule_id in schedules:
                raise ConflictError(
                    "schedule already exists: %s" % schedule_id, "schedule_exists"
                )
            plan = self.store.get_workflow(tenant, workflow_id)
            now = self.now()
            next_at = now if first_at is None else first_at
            record = {
                "tenant": tenant,
                "schedule_id": schedule_id,
                "workflow_id": plan["workflow_id"],
                "interval_seconds": interval,
                "first_at": next_at,
                "next_at": next_at,
                "params": params,
                "max_parallelism": max_parallelism,
                "created_at": self._iso(now),
                "updated_at": self._iso(now),
            }
            schedules[schedule_id] = record
            self.store.save_schedules(tenant, schedules)
            self.store.append_audit(tenant, "schedule.create", at=self._iso(now),
                                    schedule_id=schedule_id)
            return dict(record)

    def list_schedules(self, tenant):
        """Return the tenant's schedules sorted by schedule_id."""
        tenant = normalize_tenant(tenant)
        with self.store.locked():
            schedules = self.store.load_schedules(tenant)
            return [dict(schedules[sid]) for sid in sorted(schedules)]

    def dispatch_schedule(self, tenant, schedule_id):
        """Fire a schedule when due.

        Returns ``(run, scheduled_at)`` when a run was created, or
        ``(None, None)`` when the schedule is not yet due (HTTP 204).  Only
        the single due trigger point is processed per call: ``next_at``
        advances by exactly one interval, so a repeat before the next
        trigger is due changes nothing and missed trigger points are caught
        up one per call in chronological order.
        """
        tenant = normalize_tenant(tenant)
        schedule_id = normalize_schedule_id(schedule_id)
        now = self.now()
        with self.store.locked():
            owner = self.store.schedule_exists(schedule_id)
            if owner is None:
                raise WorkflowError(
                    "unknown schedule: %s" % schedule_id, "unknown_schedule"
                )
            if owner != tenant:
                raise WorkflowError(
                    "schedule %s belongs to another tenant" % schedule_id, "cross_tenant"
                )
            schedules = self.store.load_schedules(tenant)
            record = schedules[schedule_id]
            scheduled_at = float(record["next_at"])
            if now < scheduled_at:
                return None, None
            run = self.start_run(
                tenant,
                record["workflow_id"],
                params=record.get("params") or {},
                max_parallelism=record.get("max_parallelism"),
                schedule_id=schedule_id,
                scheduled_at=scheduled_at,
            )
            record["next_at"] = scheduled_at + float(record["interval_seconds"])
            record["updated_at"] = self._iso(now)
            schedules[schedule_id] = record
            self.store.save_schedules(tenant, schedules)
            self.store.append_audit(tenant, "schedule.dispatch", at=self._iso(now),
                                    schedule_id=schedule_id, run_id=run["run_id"])
            return run, scheduled_at

    # -- worker registry -----------------------------------------------
    def _worker_view(self, record, now=None):
        """Project a stored registration record with live status."""
        now = self.now() if now is None else now
        deadline = float(record["expires_at_epoch"])
        return {
            "worker_id": record["worker_id"],
            "status": WORKER_ACTIVE if now < deadline else WORKER_EXPIRED,
            "registered_at": record["registered_at"],
            "last_heartbeat": record["last_heartbeat"],
            "expires_at": record["expires_at"],
        }

    def register_worker(self, tenant, worker_id, lease_seconds=None):
        """Register ``worker_id`` for ``tenant`` (or refresh its expiry).

        Returns ``(record, created)`` where ``created`` is ``True`` for the
        first registration (HTTP 201) and ``False`` for a repeat that
        refreshed an existing record (HTTP 200).  Registration never writes
        run history.
        """
        tenant = normalize_tenant(tenant)
        worker_id = normalize_worker_id(worker_id)
        lease = normalize_worker_lease(lease_seconds)
        now = self.now()
        with self.store.locked():
            workers = self.store.load_workers(tenant)
            existing = workers.get(worker_id)
            deadline = now + lease
            at = self._iso(now)
            if existing is None:
                record = {
                    "worker_id": worker_id,
                    "registered_at": at,
                    "last_heartbeat": at,
                    "expires_at": self._iso(deadline),
                    "expires_at_epoch": deadline,
                }
                created = True
            else:
                # A repeat registration simply refreshes the expiry; the
                # original registered_at is preserved.
                record = dict(existing)
                record.update(last_heartbeat=at, expires_at=self._iso(deadline),
                              expires_at_epoch=deadline)
                created = False
            workers[worker_id] = record
            self.store.save_workers(tenant, workers)
            self.store.append_audit(tenant, "worker.register", at=at, worker_id=worker_id)
            return self._worker_view(record, now), created

    def heartbeat_worker(self, tenant, worker_id, lease_seconds=None):
        """Heartbeat a registered worker.

        The new expiry is the later of the old one and ``now +
        lease_seconds``.  Unknown workers raise ``unknown_worker`` (404) and
        workers whose registration has expired raise ``worker_expired``
        (409); an expired worker must re-register.
        """
        tenant = normalize_tenant(tenant)
        worker_id = normalize_worker_id(worker_id)
        lease = normalize_worker_lease(lease_seconds)
        now = self.now()
        with self.store.locked():
            workers = self.store.load_workers(tenant)
            record = workers.get(worker_id)
            if record is None:
                raise WorkflowError("unknown worker: %s" % worker_id, "unknown_worker")
            if now >= float(record["expires_at_epoch"]):
                raise ConflictError(
                    "worker_expired: registration of worker %s has expired; re-register"
                    % worker_id,
                    "worker_expired",
                )
            deadline = max(float(record["expires_at_epoch"]), now + lease)
            extended = deadline > float(record["expires_at_epoch"])
            record = dict(record)
            record.update(last_heartbeat=self._iso(now), expires_at=self._iso(deadline),
                          expires_at_epoch=deadline)
            workers[worker_id] = record
            self.store.save_workers(tenant, workers)
            if extended:
                # A heartbeat that does not extend the lease is not audited.
                self.store.append_audit(tenant, "worker.heartbeat", at=self._iso(now),
                                        worker_id=worker_id)
            return self._worker_view(record, now)

    def list_workers(self, tenant):
        """Return the tenant's worker records sorted by worker_id.

        Status is computed against the current time on every call, so
        expired workers are reported without any background reaper.
        """
        tenant = normalize_tenant(tenant)
        now = self.now()
        with self.store.locked():
            workers = self.store.load_workers(tenant)
            return [self._worker_view(workers[worker_id], now)
                    for worker_id in sorted(workers)]

    def _check_worker_registration(self, tenant, worker_id, now):
        """Gate claims against the tenant's worker registry.

        A tenant with no registration records keeps the legacy rule (any
        non-empty worker_id).  Once any record exists the caller must be a
        registered worker whose registration is strictly unexpired.
        """
        workers = self.store.load_workers(tenant)
        if not workers:
            return
        record = workers.get(worker_id)
        if record is None:
            raise ConflictError(
                "worker_not_registered: worker %s is not registered for tenant %s"
                % (worker_id, tenant),
                "worker_not_registered",
            )
        if now >= float(record["expires_at_epoch"]):
            raise ConflictError(
                "worker_expired: registration of worker %s has expired; re-register"
                % worker_id,
                "worker_expired",
            )

    # -- lease expiry --------------------------------------------------
    def _expire_leases(self, run, now):
        """Release leases past their deadline; each one records a takeover."""
        reclaimed = []
        for sid in sorted(run["steps"], key=lambda s: run["order_index"].get(s, 0)):
            step = run["steps"][sid]
            if step["status"] != STEP_RUNNING:
                continue
            if step.get("lease_deadline") is None or float(step["lease_deadline"]) > now:
                continue
            holder = step["worker_id"]
            step.update(status=STEP_READY, worker_id=None, lease_deadline=None, ready_at=self._iso())
            self._event(run, run["run_id"], sid, "takeover", attempt=step["attempt"], worker_id=holder)
            reclaimed.append(sid)
        return reclaimed

    # -- claim / complete / fail ---------------------------------------
    def claim(self, tenant, run_id, worker_id, lease_seconds=30):
        """Lease the next ready step, or return ``None`` when none is ready.

        A delayed run (a future ``not_before``) cannot be activated early:
        claims before the start time return ``None`` without touching its
        state, history or ``updated_at``.  The first claim at or after the
        start time opens the root nodes (one ``ready``/``waiting`` event each)
        and then leases at most one task under the usual order and quota; a
        run whose roots are all approvals activates and still returns ``None``.

        Once the tenant has any registered worker the claim must come from an
        active registration (``worker_not_registered`` / ``worker_expired``);
        a tenant with no registrations keeps the legacy non-empty-id rule.

        A ready step whose retry backoff is still pending
        (``next_attempt_at`` in the future) is not leased; other ready tasks
        whose backoff has elapsed are handed out in the usual order instead.
        When nothing is leased and no lease takeover or root activation
        happened, the run's state, history and ``updated_at`` are left
        exactly as stored.
        """
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise WorkflowError("worker_id must be a non-empty string", "bad_worker")
        if not isinstance(lease_seconds, (int, float)) or isinstance(lease_seconds, bool) \
                or lease_seconds <= 0:
            raise WorkflowError("lease_seconds must be a positive number", "bad_lease")
        now = self.now()
        with self.store.locked():
            owner = self.store.run_exists(run_id)
            if owner is not None and owner != tenant:
                raise WorkflowError("run %s belongs to another tenant" % run_id, "cross_tenant")
            # Once a tenant has any registered worker, claims require an
            # active registration; a tenant without records keeps the legacy
            # "any non-empty worker_id" rule.
            self._check_worker_registration(tenant, worker_id.strip(), now)
            run = self.get_run(tenant, run_id)
            not_before = run.get("not_before")
            if not_before is not None and float(not_before) > now:
                # Still sleeping: activation happens only via a due claim.
                return None
            reclaimed = self._expire_leases(run, now)
            opened_ready, opened_waiting = self._open_root_nodes(run)
            now_iso = self._iso()
            candidates = [s for s in run["steps"].values()
                          if s["status"] == STEP_READY and _retry_due(s, now_iso)]
            index = run["order_index"]
            quota = run.get("max_parallelism")
            at_quota = quota is not None and _active_lease_count(run, now) >= int(quota)
            step = (min(candidates, key=lambda s: (index.get(s["id"], 0), s["id"]))
                    if candidates and not at_quota else None)
            previous = run["status"]
            if step is not None:
                step.update(status=STEP_RUNNING, worker_id=worker_id, next_attempt_at=None,
                            lease_deadline=now + float(lease_seconds), started_at=self._iso())
                self._event(run, run_id, step["id"], "claim", attempt=step["attempt"],
                            worker_id=worker_id, lease_deadline=step["lease_deadline"])
            elif not reclaimed and not opened_ready and not opened_waiting:
                # Nothing to lease and nothing else changed: keep the stored
                # state, history and updated_at untouched.
                return None
            run["status"] = _derive_run_status(run)
            if step is not None and run["status"] == RUN_RUNNING and previous == RUN_PENDING:
                self._event(run, run_id, None, "run_started")
            run["updated_at"] = self._iso()
            self.store.save_run(run)
            return dict(step) if step is not None else None

    def claim_fair(self, tenant, worker_id, lease_seconds=30):
        """Lease one task from the tenant's least-claimed eligible run.

        Scans every run of the tenant.  First, sleeping runs whose
        ``not_before`` has come due are activated (root nodes opened, one
        ``ready``/``waiting`` event each) in ``run_id`` order.  Then the
        candidates — runs that are not terminal, not still sleeping, hold at
        least one ``ready`` ordinary task whose retry backoff has elapsed
        (``next_attempt_at`` reached) and have not reached their
        ``max_parallelism`` — are ordered by fairness: the number of
        ``claim`` events in the run's history (fewest first), then
        ``created_at``, then ``run_id``.  A run holding only not-yet-due
        retries is skipped and its fairness count is left alone.  Expired
        leases are reclaimed with the usual ``takeover`` events before slots
        are counted, exactly like the single-run claim.  The winner leases
        its next due task in ``(topological index, step id)`` order,
        recording the usual ``claim`` and, when the run leaves ``pending``,
        ``run_started`` events.

        Returns ``(run_id, step)`` or ``None`` when no run is eligible; at
        most one task is leased per call.  The fairness count is rebuilt
        from the append-only history on every call, so restarts, replay and
        concurrent callers (serialized by the store lock) all agree.
        Worker-registration gating matches the single-run claim.
        """
        tenant = normalize_tenant(tenant)
        worker_id = normalize_worker_id(worker_id)
        lease = normalize_worker_lease(lease_seconds)
        now = self.now()
        with self.store.locked():
            self._check_worker_registration(tenant, worker_id, now)
            runs = self.store.load_all_runs(tenant)
            # Activate due sleeping runs in run_id order; already-activated
            # runs are left untouched.
            for run in runs:
                not_before = run.get("not_before")
                if not_before is None or float(not_before) > now:
                    continue
                if _roots_opened(run):
                    continue
                self._open_root_nodes(run)
                run["status"] = _derive_run_status(run)
                run["updated_at"] = self._iso()
                self.store.save_run(run)
            # Pick the fairest eligible run.
            now_iso = self._iso(now)
            best_key, best_run, best_previous = None, None, None
            for run in runs:
                if run["status"] in TERMINAL_RUN_STATES:
                    continue
                not_before = run.get("not_before")
                if not_before is not None and float(not_before) > now:
                    continue
                previous = run["status"]
                reclaimed = self._expire_leases(run, now)
                if reclaimed:
                    run["status"] = _derive_run_status(run)
                    run["updated_at"] = self._iso()
                    self.store.save_run(run)
                ready = [s for s in run["steps"].values()
                         if s["status"] == STEP_READY and _retry_due(s, now_iso)]
                if not ready:
                    continue
                quota = run.get("max_parallelism")
                if quota is not None and _active_lease_count(run, now) >= int(quota):
                    continue
                claims = sum(1 for e in run["history"] if e.get("type") == "claim")
                key = (claims, run.get("created_at") or "", run["run_id"])
                if best_key is None or key < best_key:
                    best_key, best_run, best_previous = key, run, previous
            if best_run is None:
                return None
            run = best_run
            index = run["order_index"]
            step = min((s for s in run["steps"].values()
                        if s["status"] == STEP_READY and _retry_due(s, now_iso)),
                       key=lambda s: (index.get(s["id"], 0), s["id"]))
            step.update(status=STEP_RUNNING, worker_id=worker_id, next_attempt_at=None,
                        lease_deadline=now + lease, started_at=self._iso())
            self._event(run, run["run_id"], step["id"], "claim", attempt=step["attempt"],
                        worker_id=worker_id, lease_deadline=step["lease_deadline"])
            run["status"] = _derive_run_status(run)
            if run["status"] == RUN_RUNNING and best_previous == RUN_PENDING:
                self._event(run, run["run_id"], None, "run_started")
            run["updated_at"] = self._iso()
            self.store.save_run(run)
            return run["run_id"], dict(step)

    def heartbeat(self, tenant, run_id, step_id, worker_id, lease_seconds=30):
        """Renew the lease on a running step held by ``worker_id``.

        The new deadline is the later of the current one and
        ``now + lease_seconds``; only the deadline and ``updated_at`` change.
        A renewal that would not extend the deadline returns the run
        unchanged and appends no history.
        """
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise WorkflowError("worker_id must be a non-empty string", "bad_worker")
        if not isinstance(step_id, str) or not step_id.strip():
            raise WorkflowError("step_id must be a non-empty string", "bad_step")
        if not isinstance(lease_seconds, (int, float)) or isinstance(lease_seconds, bool) \
                or not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise WorkflowError("lease_seconds must be a positive number", "bad_lease")
        now = self.now()
        with self.store.locked():
            owner = self.store.run_exists(run_id)
            if owner is not None and owner != tenant:
                raise WorkflowError("run %s belongs to another tenant" % run_id, "cross_tenant")
            run = self.get_run(tenant, run_id)
            if run["status"] in (RUN_SUCCEEDED, RUN_FAILED):
                raise ConflictError("run %s is already %s" % (run_id, run["status"]),
                                    "run_finished")
            step = run["steps"].get(step_id)
            if step is None:
                raise WorkflowError("unknown step: %s" % step_id, "unknown_step")
            if step.get("kind") == KIND_APPROVAL:
                raise ConflictError(
                    "step %s is an approval node; use /decision" % step_id, "not_a_task"
                )
            if step["status"] != STEP_RUNNING:
                raise LeaseError("step %s is not running" % step_id, "lease_not_held")
            if step["worker_id"] != worker_id:
                raise LeaseError("step %s lease is not held by worker %s" % (step_id, worker_id))
            deadline = float(step["lease_deadline"])
            if now >= deadline:
                raise LeaseError("lease for step %s expired" % step_id, "lease_expired")
            new_deadline = max(deadline, now + float(lease_seconds))
            if new_deadline <= deadline:
                return run
            step["lease_deadline"] = new_deadline
            self._event(run, run_id, step_id, "heartbeat", attempt=step["attempt"],
                        worker_id=worker_id, lease_deadline=new_deadline)
            run["updated_at"] = self._iso()
            self.store.save_run(run)
            return run

    def _require_lease(self, run, step_id, worker_id):
        step = run["steps"].get(step_id)
        if step is None:
            raise WorkflowError("unknown step: %s" % step_id, "unknown_step")
        if step["status"] == STEP_RUNNING and step["worker_id"] == worker_id:
            return step
        raise LeaseError("step %s lease is not held by worker %s" % (step_id, worker_id))

    def complete(self, tenant, run_id, step_id, worker_id, result=None):
        """Idempotent: repeating the same complete is a no-op."""
        now = self.now()
        with self.store.locked():
            run = self.get_run(tenant, run_id)
            step = run["steps"].get(step_id)
            if step is None:
                raise WorkflowError("unknown step: %s" % step_id, "unknown_step")
            if step.get("kind") == KIND_APPROVAL:
                raise ConflictError(
                    "step %s is an approval node; use /decision" % step_id, "not_a_task"
                )
            if step["status"] == STEP_SUCCEEDED and step["worker_id"] == worker_id:
                return run
            step = self._require_lease(run, step_id, worker_id)
            if float(step["lease_deadline"]) <= now:
                raise LeaseError("lease for step %s expired" % step_id, "lease_expired")
            step.update(attempt=step["attempt"] + 1, status=STEP_SUCCEEDED, result=result,
                        error=None, finished_at=self._iso(), lease_deadline=None)
            self._event(run, run_id, step_id, "complete", attempt=step["attempt"],
                        worker_id=worker_id, result=result)
            self._open_root_nodes(run)
            run["status"] = _derive_run_status(run)
            if run["status"] == RUN_SUCCEEDED:
                self._event(run, run_id, None, "run_succeeded")
            run["updated_at"] = self._iso()
            self.store.save_run(run)
            return run

    def fail(self, tenant, run_id, step_id, worker_id, error=None):
        """Count the attempt, retry with backoff, or fail the step and run."""
        now = self.now()
        with self.store.locked():
            run = self.get_run(tenant, run_id)
            step = run["steps"].get(step_id)
            if step is None:
                raise WorkflowError("unknown step: %s" % step_id, "unknown_step")
            if step.get("kind") == KIND_APPROVAL:
                raise ConflictError(
                    "step %s is an approval node; use /decision" % step_id, "not_a_task"
                )
            step = self._require_lease(run, step_id, worker_id)
            step.update(attempt=step["attempt"] + 1, worker_id=None, lease_deadline=None, error=error)
            self._event(run, run_id, step_id, "fail", attempt=step["attempt"], worker_id=worker_id,
                        error=error)
            if step["attempt"] >= int(step["max_attempts"]):
                step.update(status=STEP_FAILED, finished_at=self._iso(), next_attempt_at=None)
                self._event(run, run_id, step_id, "attempts_exhausted", attempt=step["attempt"],
                            worker_id=worker_id)
            else:
                step.update(status=STEP_READY, ready_at=self._iso(),
                            next_attempt_at=self._iso(now + backoff_seconds(
                                step["attempt"], self.backoff_base)))
                self._event(run, run_id, step_id, "retry", attempt=step["attempt"],
                            worker_id=worker_id, next_attempt_at=step["next_attempt_at"])
            run["status"] = _derive_run_status(run)
            if run["status"] == RUN_FAILED:
                self._event(run, run_id, None, "run_failed")
            run["updated_at"] = self._iso()
            self.store.save_run(run)
            return run

    # -- human decisions -----------------------------------------------
    def decide(self, tenant, run_id, step_id, actor, decision):
        """Record a human approve/reject decision for an approval step.

        Repeating the *same* decision from the *same* actor is idempotent and
        returns the unchanged run; a different actor or the opposite decision
        conflicts with the recorded decision.
        """
        if not isinstance(actor, str) or not actor.strip():
            raise WorkflowError("actor must be a non-empty string", "bad_actor")
        actor = actor.strip()
        if decision not in ("approve", "reject"):
            raise WorkflowError("decision must be 'approve' or 'reject'", "bad_decision")
        with self.store.locked():
            run = self.get_run(tenant, run_id)
            step = run["steps"].get(step_id)
            if step is None:
                raise WorkflowError("unknown step: %s" % step_id, "unknown_step")
            if step.get("kind") != KIND_APPROVAL:
                raise ConflictError("step %s is not an approval node" % step_id, "not_an_approval")
            recorded = step.get("approval")
            if recorded is not None:
                if recorded["actor"] == actor and recorded["decision"] == decision:
                    return run
                raise ConflictError(
                    "step %s already decided by %s: %s"
                    % (step_id, recorded["actor"], recorded["decision"]),
                    "already_decided",
                )
            if step["status"] != STEP_WAITING:
                raise ConflictError(
                    "step %s is not waiting for a decision" % step_id, "not_waiting"
                )
            event = self._event(run, run_id, step_id, "decision", attempt=0, worker_id=None,
                                actor=actor, decision=decision)
            at = event["at"]
            step["approval"] = {"actor": actor, "decision": decision, "at": at}
            previous = run["status"]
            if decision == "approve":
                step.update(status=STEP_SUCCEEDED, finished_at=at)
                self._open_root_nodes(run, at)
                run["status"] = _derive_run_status(run)
            else:
                step.update(status=STEP_FAILED, finished_at=at)
                run["status"] = RUN_FAILED
            if run["status"] == RUN_RUNNING and previous == RUN_PENDING:
                self._event(run, run_id, None, "run_started")
            if run["status"] == RUN_SUCCEEDED:
                self._event(run, run_id, None, "run_succeeded")
            elif run["status"] == RUN_FAILED:
                self._event(run, run_id, None, "run_failed")
            run["updated_at"] = at
            self.store.save_run(run)
            return run

    # -- history / replay ----------------------------------------------
    def replay(self, tenant, run_id):
        """Rebuild run state from the append-only history."""
        stored = self.get_run(tenant, run_id)
        plan = self.store.get_workflow(tenant, stored["workflow_id"])
        rebuilt = new_run(tenant, stored["workflow_id"], run_id, stored.get("params"), plan,
                          stored["created_at"],
                          max_parallelism=stored.get("max_parallelism"),
                          idempotency_key=stored.get("idempotency_key"),
                          not_before=stored.get("not_before"),
                          schedule_id=stored.get("schedule_id"),
                          scheduled_at=stored.get("scheduled_at"))
        steps = rebuilt["steps"]
        for event in stored["history"]:
            sid, etype, at = event.get("step_id"), event.get("type"), event.get("at")
            attempt = event.get("attempt")
            if sid is None:
                if etype == "run_succeeded":
                    rebuilt["status"] = RUN_SUCCEEDED
                elif etype == "run_failed":
                    rebuilt["status"] = RUN_FAILED
                elif etype == "run_started" and rebuilt["status"] == RUN_PENDING:
                    rebuilt["status"] = RUN_RUNNING
                continue
            step = steps.get(sid)
            if step is None:
                continue
            if etype == "ready":
                step.update(status=STEP_READY, ready_at=at)
            elif etype == "waiting":
                step.update(status=STEP_WAITING)
            elif etype == "claim":
                step.update(status=STEP_RUNNING, worker_id=event.get("worker_id"),
                            started_at=at, lease_deadline=event.get("lease_deadline"),
                            next_attempt_at=None)
            elif etype == "heartbeat":
                step["lease_deadline"] = event.get("lease_deadline")
            elif etype == "takeover":
                step.update(status=STEP_READY, worker_id=None, lease_deadline=None)
            elif etype == "complete":
                step.update(status=STEP_SUCCEEDED, result=event.get("result"), finished_at=at,
                            lease_deadline=None, worker_id=None,
                            attempt=step["attempt"] if attempt is None else attempt)
            elif etype == "decision":
                step.update(status=STEP_SUCCEEDED if event.get("decision") == "approve"
                            else STEP_FAILED, finished_at=at,
                            approval={"actor": event.get("actor"),
                                      "decision": event.get("decision"), "at": at})
            elif etype == "fail":
                step.update(error=event.get("error"), worker_id=None, lease_deadline=None,
                            attempt=step["attempt"] if attempt is None else attempt)
            elif etype == "retry":
                step.update(status=STEP_READY, ready_at=at,
                            next_attempt_at=event.get("next_attempt_at"))
            elif etype == "attempts_exhausted":
                step.update(status=STEP_FAILED, finished_at=at, next_attempt_at=None)
        if all(s["status"] == STEP_SUCCEEDED for s in steps.values()):
            rebuilt["status"] = RUN_SUCCEEDED
        elif any(s["status"] == STEP_FAILED for s in steps.values()):
            rebuilt["status"] = RUN_FAILED
        return rebuilt
