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


def normalize_params(value):
    """Validate ``params`` on a keyed create: object only, null/omit = ``{}``."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise WorkflowError("params must be a JSON object", "bad_params")
    return value


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
        return event

    # -- workflow / run lifecycle --------------------------------------
    def submit(self, tenant, workflow_id, steps):
        return self.store.save_workflow(tenant, workflow_id, steps)

    def start_run(self, tenant, workflow_id, run_id=None, params=None, max_parallelism=None,
                  idempotency_key=None):
        key = normalize_idempotency_key(idempotency_key)
        max_parallelism = normalize_max_parallelism(max_parallelism)
        if key is not None:
            params = normalize_params(params)
        with self.store.locked():
            if key is not None:
                return self._start_idempotent_run(
                    tenant, workflow_id, run_id, params, max_parallelism, key
                )
            plan = self.store.get_workflow(tenant, workflow_id)
            run_id = run_id or self.store.generate_run_id()
            if self.store.run_exists(run_id) == tenant:
                raise WorkflowError("run already exists: %s" % run_id, "duplicate_run")
            run = new_run(tenant, workflow_id, run_id, params, plan, self._iso(),
                          max_parallelism=max_parallelism)
            return self._create_run(run, plan)

    def _start_idempotent_run(self, tenant, workflow_id, run_id, params, max_parallelism, key):
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
                    or existing.get("max_parallelism") != max_parallelism:
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
                      max_parallelism=max_parallelism, idempotency_key=key)
        self._create_run(run, plan)
        index[key] = {"run_id": run_id}
        self.store.save_idempotency(tenant, index)
        return run

    def _create_run(self, run, plan):
        """Append creation events and persist a freshly built run."""
        run_id = run["run_id"]
        self._event(run, run_id, None, "run_created")
        ready, waiting = _promote_pending(run, self._iso())
        for sid in ready:
            self._event(run, run_id, sid, "ready", attempt=0)
        for sid in waiting:
            self._event(run, run_id, sid, "waiting", attempt=0)
        run["status"], run["updated_at"] = _derive_run_status(run), self._iso()
        return self.store.save_run(run)

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
        """Lease the next ready step, or return ``None`` when none is ready."""
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise WorkflowError("worker_id must be a non-empty string", "bad_worker")
        if not isinstance(lease_seconds, (int, float)) or isinstance(lease_seconds, bool) \
                or lease_seconds <= 0:
            raise WorkflowError("lease_seconds must be a positive number", "bad_lease")
        now = self.now()
        with self.store.locked():
            run = self.get_run(tenant, run_id)
            self._expire_leases(run, now)
            ready, waiting = _promote_pending(run, self._iso())
            for sid in ready:
                self._event(run, run_id, sid, "ready", attempt=run["steps"][sid]["attempt"])
            for sid in waiting:
                self._event(run, run_id, sid, "waiting", attempt=0)
            candidates = [s for s in run["steps"].values() if s["status"] == STEP_READY]
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
            run["status"] = _derive_run_status(run)
            if step is not None and run["status"] == RUN_RUNNING and previous == RUN_PENDING:
                self._event(run, run_id, None, "run_started")
            run["updated_at"] = self._iso()
            self.store.save_run(run)
            return dict(step) if step is not None else None

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
            ready, waiting = _promote_pending(run, self._iso())
            for sid in ready:
                self._event(run, run_id, sid, "ready", attempt=run["steps"][sid]["attempt"])
            for sid in waiting:
                self._event(run, run_id, sid, "waiting", attempt=0)
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
                            worker_id=worker_id)
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
                ready, waiting = _promote_pending(run, at)
                for sid in ready:
                    self._event(run, run_id, sid, "ready", attempt=run["steps"][sid]["attempt"])
                for sid in waiting:
                    self._event(run, run_id, sid, "waiting", attempt=0)
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
                          idempotency_key=stored.get("idempotency_key"))
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
                            started_at=at, lease_deadline=event.get("lease_deadline"))
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
                step["status"] = STEP_READY
            elif etype == "attempts_exhausted":
                step.update(status=STEP_FAILED, finished_at=at)
        if all(s["status"] == STEP_SUCCEEDED for s in steps.values()):
            rebuilt["status"] = RUN_SUCCEEDED
        elif any(s["status"] == STEP_FAILED for s in steps.values()):
            rebuilt["status"] = RUN_FAILED
        return rebuilt
