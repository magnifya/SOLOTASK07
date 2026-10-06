"""Persistence for flowd: workflows, runs, tenants and append-only history.

Layout::

    <root>/<tenant>/workflows.json
    <root>/<tenant>/idempotency.json
    <root>/<tenant>/workers.json
    <root>/<tenant>/schedules.json
    <root>/<tenant>/audit.json
    <root>/<tenant>/runs/<run_id>.json

Every write is atomic (temp file + ``os.replace``) so a crash cannot leave a
half written run behind, and a fresh store over the same root sees exactly
the same workflows and runs.
"""

import json
import os
import threading
import time as _time
import uuid
from contextlib import contextmanager

from .model import KIND_TASK, WorkflowError, format_time, plan_workflow

RUN_PENDING = "pending"
RUN_RUNNING = "running"
RUN_SUCCEEDED = "succeeded"
RUN_FAILED = "failed"

STEP_PENDING = "pending"
STEP_READY = "ready"
STEP_RUNNING = "running"
STEP_WAITING = "waiting"
STEP_SUCCEEDED = "succeeded"
STEP_FAILED = "failed"

TERMINAL_RUN_STATES = (RUN_SUCCEEDED, RUN_FAILED)


def atomic_write_json(path, payload):
    """Write ``payload`` as JSON to ``path`` atomically."""
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp = "%s.tmp.%d" % (path, os.getpid())
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, sort_keys=True, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def read_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return default


class WorkflowStore:
    """Filesystem backed store for one data directory (all tenants)."""

    def __init__(self, root, clock=None):
        self.root = os.path.abspath(root)
        self._clock = clock if clock is not None else _time.time
        self._lock = threading.RLock()
        os.makedirs(self.root, exist_ok=True)

    def now(self):
        return float(self._clock())

    def now_iso(self):
        return format_time(self.now())

    @contextmanager
    def locked(self):
        with self._lock:
            yield

    # -- paths ---------------------------------------------------------
    def tenant_dir(self, tenant):
        if not isinstance(tenant, str) or not tenant.strip():
            raise WorkflowError("tenant must be a non-empty string", "bad_tenant")
        return os.path.join(self.root, tenant.strip())

    def workflows_path(self, tenant):
        return os.path.join(self.tenant_dir(tenant), "workflows.json")

    def idempotency_path(self, tenant):
        return os.path.join(self.tenant_dir(tenant), "idempotency.json")

    def workers_path(self, tenant):
        return os.path.join(self.tenant_dir(tenant), "workers.json")

    def schedules_path(self, tenant):
        return os.path.join(self.tenant_dir(tenant), "schedules.json")

    def audit_path(self, tenant):
        return os.path.join(self.tenant_dir(tenant), "audit.json")

    def run_path(self, tenant, run_id):
        return os.path.join(self.tenant_dir(tenant), "runs", "%s.json" % run_id)

    def generate_run_id(self):
        return "run-" + uuid.uuid4().hex[:12]

    # -- workflows -----------------------------------------------------
    def save_workflow(self, tenant, workflow_id, steps):
        plan = plan_workflow(workflow_id, steps)
        with self._lock:
            data = self.load_workflows(tenant)
            data[plan["workflow_id"]] = plan
            atomic_write_json(self.workflows_path(tenant), data)
        return plan

    def load_workflows(self, tenant):
        return read_json(self.workflows_path(tenant), {}) or {}

    def get_workflow(self, tenant, workflow_id):
        plan = self.load_workflows(tenant).get(workflow_id)
        if plan is None:
            raise WorkflowError("unknown workflow: %s" % workflow_id, "unknown_workflow")
        return plan

    # -- idempotency index --------------------------------------------
    def load_idempotency(self, tenant):
        """Return the tenant's ``{trimmed key: {"run_id": ...}}`` map."""
        return read_json(self.idempotency_path(tenant), {}) or {}

    def save_idempotency(self, tenant, index):
        atomic_write_json(self.idempotency_path(tenant), index)
        return index

    # -- worker registry ----------------------------------------------
    def load_workers(self, tenant):
        """Return the tenant's ``{worker_id: registration record}`` map."""
        return read_json(self.workers_path(tenant), {}) or {}

    def save_workers(self, tenant, index):
        atomic_write_json(self.workers_path(tenant), index)
        return index

    # -- schedule registry --------------------------------------------
    def load_schedules(self, tenant):
        """Return the tenant's ``{schedule_id: schedule record}`` map."""
        return read_json(self.schedules_path(tenant), {}) or {}

    def save_schedules(self, tenant, index):
        atomic_write_json(self.schedules_path(tenant), index)
        return index

    def schedule_exists(self, schedule_id):
        """Return the tenant owning ``schedule_id``, or ``None``."""
        for tenant in sorted(os.listdir(self.root)):
            if schedule_id in (read_json(self.schedules_path(tenant), {}) or {}):
                return tenant
        return None

    # -- audit stream ---------------------------------------------------
    def append_audit(self, tenant, action, at=None, run_id=None, step_id=None,
                     workflow_id=None, schedule_id=None, worker_id=None, actor=None):
        """Append one record to the tenant's audit stream.

        The ``sequence`` is assigned under the store lock, so concurrent
        appends stay unique and follow commit order, and it continues from
        the persisted stream, so a restart keeps increasing.  Only
        identifiers and the actor are stored — never params, results or
        error text; identifiers that do not apply are ``None``.
        """
        with self._lock:
            path = self.audit_path(tenant)
            items = (read_json(path, None) or {}).get("items") or []
            record = {
                "sequence": (items[-1]["sequence"] + 1) if items else 1,
                "at": at if at is not None else self.now_iso(),
                "tenant": tenant.strip(),
                "action": action,
                "run_id": run_id,
                "step_id": step_id,
                "workflow_id": workflow_id,
                "schedule_id": schedule_id,
                "worker_id": worker_id,
                "actor": actor,
            }
            items.append(record)
            atomic_write_json(path, {"items": items})
            return dict(record)

    def load_audit(self, tenant):
        """Return the tenant's audit records in sequence order (read-only)."""
        return list((read_json(self.audit_path(tenant), None) or {}).get("items") or [])

    # -- runs ----------------------------------------------------------
    def save_run(self, run):
        atomic_write_json(self.run_path(run["tenant"], run["run_id"]), run)
        return run

    def load_run(self, tenant, run_id):
        run = read_json(self.run_path(tenant, run_id), None)
        if run is None:
            raise WorkflowError("unknown run: %s" % run_id, "unknown_run")
        return normalize_run(run)

    def run_exists(self, run_id):
        """Return the tenant owning ``run_id``, or ``None``."""
        for tenant in sorted(os.listdir(self.root)):
            if os.path.isfile(os.path.join(self.root, tenant, "runs", "%s.json" % run_id)):
                return tenant
        return None

    def load_all_runs(self, tenant):
        """Return every run of the tenant as a list sorted by ``run_id``."""
        runs_dir = os.path.join(self.tenant_dir(tenant), "runs")
        items = []
        for name in sorted(os.listdir(runs_dir)) if os.path.isdir(runs_dir) else []:
            run = read_json(os.path.join(runs_dir, name), None) if name.endswith(".json") else None
            if run is not None:
                items.append(normalize_run(run))
        items.sort(key=lambda r: r.get("run_id", ""))
        return items

    def list_runs(self, tenant, status=None, limit=50, after=None):
        runs_dir = os.path.join(self.tenant_dir(tenant), "runs")
        items = []
        for name in sorted(os.listdir(runs_dir)) if os.path.isdir(runs_dir) else []:
            run = read_json(os.path.join(runs_dir, name), None) if name.endswith(".json") else None
            if run is not None:
                items.append(normalize_run(run))
        items.sort(key=lambda r: (r.get("created_at", ""), r.get("run_id", "")))
        if status:
            items = [r for r in items if r.get("status") == status]
        if after:
            position = [i for i, r in enumerate(items) if r.get("run_id") == after]
            items = items[position[0] + 1:] if position else []
        limit = max(1, int(limit))
        page = items[:limit]
        return page, (page[-1]["run_id"] if len(items) > limit else None)


def normalize_run(run):
    """Backfill fields missing from documents written by older versions.

    Steps without ``kind`` predate approval nodes and are ordinary tasks.
    Runs without ``max_parallelism`` predate per-run concurrency quotas and
    have no limit (``None``).  Runs without ``idempotency_key`` predate
    tenant-scoped idempotency keys and never participate in dedup (``None``).
    Runs without ``not_before`` predate delayed start and begin immediately
    (``None``).  Runs without ``schedule_id``/``scheduled_at`` predate
    periodic scheduling and were created manually (``None``).  Steps without
    ``next_attempt_at`` predate retry backoff and are immediately claimable
    (``None``).  Runs without a frozen ``plan`` predate per-run DAG snapshots
    and reconstruct theirs from the node definitions and ``step_order`` saved
    in the run document itself, so replay never has to read the workflow
    registry (which a later same-name submission may have overwritten).
    Mutates and returns ``run``.
    """
    run.setdefault("max_parallelism", None)
    run.setdefault("idempotency_key", None)
    run.setdefault("not_before", None)
    run.setdefault("schedule_id", None)
    run.setdefault("scheduled_at", None)
    for step in run.get("steps", {}).values():
        step.setdefault("kind", KIND_TASK)
        step.setdefault("approval", None)
        step.setdefault("next_attempt_at", None)
    if not run.get("plan"):
        run["plan"] = plan_from_run(run)
    return run


def plan_from_run(run):
    """Reconstruct a normalized plan from the definitions frozen in a run.

    The run document keeps each node's ``kind``, ``depends_on`` and
    ``max_attempts`` together with ``step_order``; those are exactly the
    fields a plan needs, so documents written before plans were snapshotted
    remain replayable even after the workflow of the same id is overwritten
    or deleted.
    """
    steps = run.get("steps") or {}
    order = [sid for sid in (run.get("step_order") or sorted(steps)) if sid in steps]
    ordered = [
        {
            "id": steps[sid]["id"],
            "depends_on": list(steps[sid].get("depends_on") or []),
            "max_attempts": steps[sid]["max_attempts"],
            "kind": steps[sid].get("kind", KIND_TASK),
        }
        for sid in order
    ]
    return {
        "workflow_id": run.get("workflow_id"),
        "steps": ordered,
        "order": [step["id"] for step in ordered],
    }


def run_plan(run):
    """Return the normalized DAG plan frozen into ``run`` at creation.

    Newer documents carry an explicit snapshot under ``plan``; older ones
    reconstruct it via :func:`plan_from_run` from the run's own nodes and
    ordering.  The result is a fresh copy, so callers can never mutate the
    snapshot stored in the run document.
    """
    snapshot = run.get("plan")
    if not snapshot or not snapshot.get("steps"):
        snapshot = plan_from_run(run)
    return {
        "workflow_id": snapshot.get("workflow_id", run.get("workflow_id")),
        "steps": [dict(step) for step in snapshot.get("steps", [])],
        "order": list(snapshot.get("order", [])),
    }


def new_run(tenant, workflow_id, run_id, params, plan, now_iso, max_parallelism=None,
            idempotency_key=None, not_before=None, schedule_id=None, scheduled_at=None):
    """Build a fresh run document from a validated plan."""
    steps = {
        step["id"]: {
            "id": step["id"],
            "kind": step.get("kind", KIND_TASK),
            "depends_on": list(step["depends_on"]),
            "max_attempts": step["max_attempts"],
            "status": STEP_PENDING,
            "attempt": 0,
            "worker_id": None,
            "lease_deadline": None,
            "next_attempt_at": None,
            "ready_at": None,
            "started_at": None,
            "finished_at": None,
            "result": None,
            "error": None,
            "approval": None,
        }
        for step in plan["steps"]
    }
    return {
        "tenant": tenant,
        "workflow_id": workflow_id,
        "run_id": run_id,
        "status": RUN_PENDING,
        "params": params or {},
        "max_parallelism": max_parallelism,
        "idempotency_key": idempotency_key,
        "not_before": not_before,
        "schedule_id": schedule_id,
        "scheduled_at": scheduled_at,
        "created_at": now_iso,
        "updated_at": now_iso,
        # The normalized DAG as it was when this run was created.  A later
        # submission of another workflow with the same id must not rewrite
        # this run's nodes, order, kinds, dependencies or max_attempts;
        # replay rebuilds exclusively from this snapshot.
        "plan": {
            "workflow_id": plan["workflow_id"],
            "steps": [dict(step) for step in plan["steps"]],
            "order": list(plan["order"]),
        },
        "step_order": list(plan["order"]),
        "order_index": {sid: i for i, sid in enumerate(plan["order"])},
        "steps": steps,
        "history": [],
    }
