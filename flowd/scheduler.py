"""Scheduling engine: run state machine, leases, retries, history, replay.

All state transitions funnel through :class:`Scheduler` so the append-only
history stays the single source of truth.  ``replay`` rebuilds current state
from that history and must agree with the stored document.
"""

import time as _time

from .model import WorkflowError, format_time
from .store import (
    KIND_APPROVAL,
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

DECISION_APPROVE = "approve"
DECISION_REJECT = "reject"
DECISIONS = (DECISION_APPROVE, DECISION_REJECT)


class LeaseError(WorkflowError):
    """Raised when a worker does not hold the lease it is acting on."""

    def __init__(self, message, code="lease_not_held"):
        super().__init__(message, code)


class ConflictError(WorkflowError):
    """Raised when a request conflicts with the current step state."""

    def __init__(self, message, code="conflict"):
        super().__init__(message, code)


def backoff_seconds(attempt, base=1.0):
    """Exponential backoff: ``base * 2 ** (attempt - 1)``."""
    return float(base) * (2 ** (max(1, int(attempt)) - 1))


def _deps_succeeded(run, step):
    return all(run["steps"][d]["status"] == STEP_SUCCEEDED for d in step["depends_on"])


def _refresh_ready(run, now_iso):
    """Promote pending task steps whose dependencies all succeeded."""
    index = run["order_index"]
    moves = []
    for sid in sorted(run["steps"], key=lambda s: index.get(s, 0)):
        step = run["steps"][sid]
        if step["status"] != STEP_PENDING or step.get("kind") == KIND_APPROVAL:
            continue
        if _deps_succeeded(run, step):
            step["status"], step["ready_at"] = STEP_READY, now_iso
            moves.append(sid)
    return moves


def _refresh_waiting(run, now_iso):
    """Park pending approval steps whose dependencies all succeeded.

    An approval never becomes ``ready`` and therefore never gets claimed; it
    waits for an HTTP decision instead.
    """
    index = run["order_index"]
    moves = []
    for sid in sorted(run["steps"], key=lambda s: index.get(s, 0)):
        step = run["steps"][sid]
        if step["status"] != STEP_PENDING or step.get("kind") != KIND_APPROVAL:
            continue
        if _deps_succeeded(run, step):
            step["status"], step["ready_at"] = STEP_WAITING, now_iso
            moves.append(sid)
    return moves


def _derive_run_status(run):
    """Derive the run status from its steps; stays ``pending`` until claimed."""
    steps = list(run["steps"].values())
    if any(s["status"] == STEP_FAILED for s in steps):
        return RUN_FAILED
    if steps and all(s["status"] == STEP_SUCCEEDED for s in steps):
        return RUN_SUCCEEDED
    if any(s["status"] in (STEP_RUNNING, STEP_SUCCEEDED) for s in steps):
        return RUN_RUNNING
    # A waiting approval has work outstanding but no lease yet, so the run is
    # already in motion even though nothing has been claimed.
    if any(s["status"] == STEP_WAITING for s in steps):
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

    def start_run(self, tenant, workflow_id, run_id=None, params=None):
        plan = self.store.get_workflow(tenant, workflow_id)
        with self.store.locked():
            run_id = run_id or self.store.generate_run_id()
            if self.store.run_exists(run_id) == tenant:
                raise WorkflowError("run already exists: %s" % run_id, "duplicate_run")
            run = new_run(tenant, workflow_id, run_id, params, plan, self._iso())
            self._event(run, run_id, None, "run_created")
            for sid in _refresh_ready(run, self._iso()):
                self._event(run, run_id, sid, "ready", attempt=0)
            for sid in _refresh_waiting(run, self._iso()):
                self._event(run, run_id, sid, "waiting", attempt=0)
            previous = run["status"]
            run["status"] = _derive_run_status(run)
            if run["status"] == RUN_RUNNING and previous == RUN_PENDING:
                self._event(run, run_id, None, "run_started")
            run["updated_at"] = self._iso()
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
            for sid in _refresh_ready(run, self._iso()):
                self._event(run, run_id, sid, "ready", attempt=run["steps"][sid]["attempt"])
            for sid in _refresh_waiting(run, self._iso()):
                self._event(run, run_id, sid, "waiting", attempt=run["steps"][sid]["attempt"])
            candidates = [s for s in run["steps"].values() if s["status"] == STEP_READY]
            index = run["order_index"]
            step = min(candidates, key=lambda s: (index.get(s["id"], 0), s["id"])) if candidates else None
            previous = run["status"]
            if step is not None:
                step.update(status=STEP_RUNNING, worker_id=worker_id, next_attempt_at=None,
                            lease_deadline=now + float(lease_seconds), started_at=self._iso())
                self._event(run, run_id, step["id"], "claim", attempt=step["attempt"],
                            worker_id=worker_id)
            run["status"] = _derive_run_status(run)
            if step is not None and run["status"] == RUN_RUNNING and previous == RUN_PENDING:
                self._event(run, run_id, None, "run_started")
            run["updated_at"] = self._iso()
            self.store.save_run(run)
            return dict(step) if step is not None else None

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
                    "step %s is an approval step; decide it over the decision API" % step_id,
                    "not_a_task")
            if step["status"] == STEP_SUCCEEDED and step["worker_id"] == worker_id:
                return run
            step = self._require_lease(run, step_id, worker_id)
            if float(step["lease_deadline"]) <= now:
                raise LeaseError("lease for step %s expired" % step_id, "lease_expired")
            step.update(attempt=step["attempt"] + 1, status=STEP_SUCCEEDED, result=result,
                        error=None, finished_at=self._iso(), lease_deadline=None)
            self._event(run, run_id, step_id, "complete", attempt=step["attempt"],
                        worker_id=worker_id, result=result)
            for sid in _refresh_ready(run, self._iso()):
                self._event(run, run_id, sid, "ready", attempt=run["steps"][sid]["attempt"])
            for sid in _refresh_waiting(run, self._iso()):
                self._event(run, run_id, sid, "waiting", attempt=run["steps"][sid]["attempt"])
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
                    "step %s is an approval step; decide it over the decision API" % step_id,
                    "not_a_task")
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

    # -- approval decisions --------------------------------------------
    def decide(self, tenant, run_id, step_id, actor, decision):
        """Apply the first ``approve``/``reject`` decision to a waiting step.

        Repeating the very same decision (same actor, same outcome) is a
        no-op and returns the unchanged run; any other repeat is a conflict.
        """
        if not isinstance(actor, str) or not actor.strip():
            raise WorkflowError("actor must be a non-empty string", "bad_actor")
        if decision not in DECISIONS:
            raise WorkflowError("decision must be one of: %s" % ", ".join(DECISIONS),
                                "bad_decision")
        with self.store.locked():
            actor = actor.strip()
            run = self.get_run(tenant, run_id)
            step = run["steps"].get(step_id)
            if step is None:
                raise WorkflowError("unknown step: %s" % step_id, "unknown_step")
            if step.get("kind") != KIND_APPROVAL:
                raise ConflictError(
                    "step %s is not an approval step" % step_id, "not_an_approval")
            recorded = step.get("approval")
            if recorded is not None:
                if recorded["actor"] == actor and recorded["decision"] == decision:
                    return run
                raise ConflictError(
                    "step %s already has a decision by %s" % (step_id, recorded["actor"]),
                    "decision_conflict")
            if step["status"] != STEP_WAITING:
                raise ConflictError(
                    "step %s is not waiting for a decision" % step_id, "not_waiting")
            at = self._iso()
            step.update(status=(STEP_SUCCEEDED if decision == DECISION_APPROVE else STEP_FAILED),
                        approval={"actor": actor, "decision": decision, "at": at},
                        finished_at=at)
            self._event(run, run_id, step_id, "decision", attempt=0, worker_id=None,
                        actor=actor, decision=decision, at=at)
            if decision == DECISION_APPROVE:
                for sid in _refresh_ready(run, self._iso()):
                    self._event(run, run_id, sid, "ready",
                                attempt=run["steps"][sid]["attempt"])
                for sid in _refresh_waiting(run, self._iso()):
                    self._event(run, run_id, sid, "waiting",
                                attempt=run["steps"][sid]["attempt"])
            run["status"] = _derive_run_status(run)
            if run["status"] == RUN_SUCCEEDED:
                self._event(run, run_id, None, "run_succeeded")
            elif run["status"] == RUN_FAILED:
                self._event(run, run_id, None, "run_failed")
            run["updated_at"] = self._iso()
            self.store.save_run(run)
            return run

    # -- history / replay ----------------------------------------------
    def replay(self, tenant, run_id):
        """Rebuild run state from the append-only history."""
        stored = self.get_run(tenant, run_id)
        plan = self.store.get_workflow(tenant, stored["workflow_id"])
        rebuilt = new_run(tenant, stored["workflow_id"], run_id, stored.get("params"), plan,
                          stored["created_at"])
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
                step.update(status=STEP_WAITING, ready_at=at)
            elif etype == "claim":
                step.update(status=STEP_RUNNING, worker_id=event.get("worker_id"), started_at=at)
            elif etype == "takeover":
                step.update(status=STEP_READY, worker_id=None, lease_deadline=None)
            elif etype == "complete":
                step.update(status=STEP_SUCCEEDED, result=event.get("result"), finished_at=at,
                            lease_deadline=None, worker_id=None,
                            attempt=step["attempt"] if attempt is None else attempt)
            elif etype == "decision":
                decision = event.get("decision")
                step.update(status=(STEP_SUCCEEDED if decision == DECISION_APPROVE else STEP_FAILED),
                            approval={"actor": event.get("actor"), "decision": decision, "at": at},
                            finished_at=at, lease_deadline=None, worker_id=None, attempt=0)
            elif etype == "fail":
                step.update(error=event.get("error"),
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
