"""DAG definition, validation and shared helpers for flowd.

A workflow is a named DAG of steps; every step is a mapping::

    {"id": "build", "depends_on": ["fetch"], "max_attempts": 3}

Validation rejects duplicate ids, empty ids, unknown dependencies,
self/cyclic dependencies and bad ``max_attempts`` values.  A validated plan
is returned topologically sorted (ties broken by step id) so scheduling
order is deterministic.
"""

import heapq
import time as _time

STEP_STATES = ("pending", "ready", "running", "waiting", "succeeded", "failed")
RUN_STATES = ("pending", "running", "succeeded", "failed")
DEFAULT_MAX_ATTEMPTS = 3

KIND_TASK = "task"
KIND_APPROVAL = "approval"
STEP_KINDS = (KIND_TASK, KIND_APPROVAL)


class WorkflowError(Exception):
    """Raised for any rejected workflow definition or illegal call."""

    def __init__(self, message, code="invalid_workflow"):
        super().__init__(message)
        self.code = code
        self.message = message


def format_time(seconds):
    """Render epoch ``seconds`` as a sortable UTC ISO-8601 timestamp."""
    whole = int(seconds)
    micros = int(round((seconds - whole) * 1000000))
    if micros >= 1000000:
        whole, micros = whole + 1, micros - 1000000
    return "%s.%06dZ" % (_time.strftime("%Y-%m-%dT%H:%M:%S", _time.gmtime(whole)), micros)


def validate_step(raw, index, seen_ids):
    """Validate one step definition and return its normalized form."""
    if not isinstance(raw, dict):
        raise WorkflowError("step %d must be an object" % index)
    step_id = raw.get("id")
    if not isinstance(step_id, str):
        raise WorkflowError("step %d is missing a string id" % index)
    step_id = step_id.strip()
    if not step_id:
        raise WorkflowError("step %d has an empty id" % index)
    if step_id in seen_ids:
        raise WorkflowError("duplicate step id: %s" % step_id, "duplicate_step")
    seen_ids.add(step_id)
    depends_on = raw.get("depends_on") or []
    if not isinstance(depends_on, list):
        raise WorkflowError("step %s: depends_on must be a list" % step_id)
    for dep in depends_on:
        if not isinstance(dep, str) or not dep.strip():
            raise WorkflowError("step %s: depends_on entries must be non-empty strings" % step_id)
    depends_on = [d.strip() for d in depends_on]
    if len(set(depends_on)) != len(depends_on):
        raise WorkflowError("step %s: duplicate dependencies" % step_id)
    if step_id in depends_on:
        raise WorkflowError("step %s depends on itself" % step_id, "self_dependency")
    max_attempts = raw.get("max_attempts")
    if max_attempts is None:
        max_attempts = DEFAULT_MAX_ATTEMPTS
    if isinstance(max_attempts, bool) or not isinstance(max_attempts, int):
        raise WorkflowError("step %s: max_attempts must be an integer" % step_id, "bad_max_attempts")
    if max_attempts < 1:
        raise WorkflowError("step %s: max_attempts must be >= 1" % step_id, "bad_max_attempts")
    kind = raw.get("kind")
    if kind is None:
        kind = KIND_TASK
    if not isinstance(kind, str) or kind not in STEP_KINDS:
        raise WorkflowError(
            "step %s: kind must be one of %s" % (step_id, ", ".join(STEP_KINDS)), "bad_kind"
        )
    return {"id": step_id, "depends_on": depends_on, "max_attempts": max_attempts, "kind": kind}


def topo_order(steps):
    """Return step ids in dependency order using Kahn's algorithm."""
    ids = [s["id"] for s in steps]
    children = {sid: [] for sid in ids}
    indegree = {}
    for step in steps:
        for dep in step["depends_on"]:
            children[dep].append(step["id"])
        indegree[step["id"]] = len(step["depends_on"])
    heap = [(sid, sid) for sid in ids if indegree[sid] == 0]
    heapq.heapify(heap)
    order = []
    while heap:
        _, sid = heapq.heappop(heap)
        order.append(sid)
        for child in sorted(children[sid]):
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(heap, (child, child))
    if len(order) != len(ids):
        raise WorkflowError(
            "cyclic dependency involving: %s" % ", ".join(sorted(set(ids) - set(order))), "cycle"
        )
    return order


def plan_workflow(workflow_id, steps):
    """Validate ``steps`` and return a normalized, topologically sorted plan."""
    if not isinstance(workflow_id, str) or not workflow_id.strip():
        raise WorkflowError("workflow_id must be a non-empty string")
    if not isinstance(steps, list) or not steps:
        raise WorkflowError("steps must be a non-empty list")
    seen = set()
    normalized = [validate_step(raw, i, seen) for i, raw in enumerate(steps)]
    for step in normalized:
        for dep in step["depends_on"]:
            if dep not in seen:
                raise WorkflowError(
                    "step %s depends on unknown step: %s" % (step["id"], dep), "unknown_dependency"
                )
    order = topo_order(normalized)
    index = {sid: i for i, sid in enumerate(order)}
    return {
        "workflow_id": workflow_id.strip(),
        "steps": sorted(normalized, key=lambda s: index[s["id"]]),
        "order": order,
    }
