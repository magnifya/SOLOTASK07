"""Standard library HTTP API for flowd."""

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from .model import WorkflowError
from .scheduler import ConflictError, LeaseError

MAX_BODY = 1 << 20
STATUS_FOR_CODE = {"unknown_run": 404, "unknown_workflow": 404, "unknown_step": 404,
                   "unknown_worker": 404, "unknown_schedule": 404,
                   "schedule_exists": 409, "cross_tenant": 403}
DECISIONS = ("approve", "reject")


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "flowd/0.1"

    def log_message(self, fmt, *args):  # keep the console quiet
        pass

    # -- plumbing ------------------------------------------------------
    def _send(self, code, raw=b""):
        self.send_response(code)
        if code != 204:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        if raw:
            self.wfile.write(raw)

    def _json(self, code, payload):
        self._send(code, json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8"))

    def _error(self, code, message):
        self._json(code, {"error": message})

    def _body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        if length > MAX_BODY:
            raise WorkflowError("request body too large", "body_too_large")
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise WorkflowError("request body must be valid JSON", "bad_json")
        if not isinstance(payload, dict):
            raise WorkflowError("request body must be a JSON object", "bad_json")
        return payload

    def _tenant(self, query, body=None):
        tenant = (body or {}).get("tenant") or (query.get("tenant") or [None])[0]
        if not tenant:
            raise WorkflowError("tenant is required", "bad_tenant")
        return tenant
    # -- verbs ---------------------------------------------------------
    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def _dispatch(self, method):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        try:
            # The audit endpoint treats blank query values as present (and
            # therefore invalid) instead of dropping them like parse_qs does.
            query = parse_qs(parsed.query, keep_blank_values=(path == "/v1/audit"))
            self._route(method, path, query)
        except LeaseError as exc:
            self._error(409, exc.message)
        except ConflictError as exc:
            self._error(409, exc.message)
        except WorkflowError as exc:
            self._error(STATUS_FOR_CODE.get(exc.code, 400), exc.message)
        except Exception as exc:  # pragma: no cover - defensive
            self._error(500, "internal error: %s" % exc)

    def _route(self, method, path, query):
        body = {}
        if method == "GET":
            if path == "/healthz":
                return self._json(200, {"ok": True})
            if path == "/v1/runs":
                return self._list_runs(query)
            if path == "/v1/schedules":
                return self._list_schedules(query)
            if path == "/v1/workers":
                return self._list_workers(query)
            if path == "/v1/audit":
                return self._get_audit(query)
        elif method == "POST":
            if path == "/v1/tasks/claim":
                return self._claim_any_task(self._body())
            if path == "/v1/workers/register":
                return self._register_worker(self._body())
            worker_beat = re.fullmatch(r"/v1/workers/([^/]+)/heartbeat", path)
            if worker_beat:
                return self._worker_heartbeat(unquote(worker_beat.group(1)), self._body())
            if path == "/v1/workflows":
                body = self._body()
                tenant = self._tenant({}, body)
                plan = self.scheduler.submit(tenant, body.get("workflow_id"), body.get("steps"))
                return self._json(201, dict(plan, tenant=tenant))
            if path == "/v1/runs":
                body = self._body()
                run = self.scheduler.start_run(self._tenant({}, body), body.get("workflow_id"),
                                               body.get("run_id"), body.get("params"),
                                               body.get("max_parallelism"),
                                               body.get("idempotency_key"),
                                               body.get("not_before"))
                return self._json(201, _run_view(run))
            if path == "/v1/schedules":
                return self._create_schedule(self._body())
            schedule_dispatch = re.fullmatch(r"/v1/schedules/([^/]+)/dispatch", path)
            if schedule_dispatch:
                return self._dispatch_schedule(unquote(schedule_dispatch.group(1)), self._body())
            action = re.fullmatch(r"/v1/runs/([^/]+)/(claim|complete|fail|decision|heartbeat)", path)
            if action:
                if action.group(2) == "decision":
                    return self._decision(action.group(1), self._body())
                if action.group(2) == "heartbeat":
                    return self._heartbeat(action.group(1), self._body())
                return self._step_action(action.group(1), action.group(2), self._body())
        solo = re.fullmatch(r"/v1/runs/([^/]+)", path)
        if method == "GET" and solo:
            return self._get_run(query, solo.group(1))
        return self._error(404, "no route for %s %s" % (method, path))

    # -- handlers ------------------------------------------------------
    def _get_run(self, query, run_id):
        tenant = self._tenant(query)
        owner = self.scheduler.store.run_exists(run_id)
        if owner is not None and owner != tenant:
            raise WorkflowError("run %s belongs to another tenant" % run_id, "cross_tenant")
        return self._json(200, _run_view(self.scheduler.get_run(tenant, run_id)))

    def _list_runs(self, query):
        tenant = self._tenant(query)
        items, next_after = self.scheduler.list_runs(
            tenant,
            status=(query.get("status") or [None])[0],
            limit=int((query.get("limit") or ["50"])[0]),
            after=(query.get("after") or [None])[0],
        )
        return self._json(200, {"tenant": tenant, "items": [_run_view(r) for r in items],
                                "next_after": next_after})

    # -- audit stream ---------------------------------------------------
    def _get_audit(self, query):
        """Tenant-scoped audit tail: 200 {"tenant","items","next_after"}."""
        tenant = (query.get("tenant") or [None])[0]
        if not isinstance(tenant, str) or not tenant.strip():
            raise WorkflowError("tenant is required", "bad_tenant")
        limit = _query_int(query, "limit", "bad_limit")
        after = _query_int(query, "after", "bad_after")
        result = self.scheduler.audit(
            tenant,
            action=(query.get("action") or [None])[0],
            run_id=(query.get("run_id") or [None])[0],
            limit=100 if limit is None else limit,
            after=0 if after is None else after,
        )
        return self._json(200, result)

    # -- periodic schedules -------------------------------------------
    def _create_schedule(self, body):
        record = self.scheduler.create_schedule(
            self._tenant({}, body), body.get("schedule_id"), body.get("workflow_id"),
            body.get("interval_seconds"), body.get("first_at"), body.get("params"),
            body.get("max_parallelism"),
        )
        return self._json(201, _schedule_view(record))

    def _list_schedules(self, query):
        tenant = self._tenant(query)
        return self._json(200, {"tenant": tenant,
                                "items": [_schedule_view(r)
                                          for r in self.scheduler.list_schedules(tenant)]})

    def _dispatch_schedule(self, schedule_id, body):
        run, scheduled_at = self.scheduler.dispatch_schedule(self._tenant({}, body),
                                                              schedule_id)
        if run is None:
            return self._send(204)
        return self._json(201, _run_view(run, scheduled_at=scheduled_at))

    # -- worker registry -----------------------------------------------
    def _worker_lease(self, body):
        """Extract lease_seconds: omitted defaults to 30, explicit null is 400."""
        if "lease_seconds" not in body:
            return 30
        lease = body["lease_seconds"]
        if lease is None:
            raise WorkflowError("lease_seconds must be a positive number", "bad_lease")
        return lease

    def _register_worker(self, body):
        record, created = self.scheduler.register_worker(
            body.get("tenant"), body.get("worker_id"), self._worker_lease(body)
        )
        return self._json(201 if created else 200, record)

    def _worker_heartbeat(self, worker_id, body):
        record = self.scheduler.heartbeat_worker(
            body.get("tenant"), worker_id, self._worker_lease(body)
        )
        return self._json(200, record)

    def _list_workers(self, query):
        tenant = self._tenant(query)
        return self._json(200, {"tenant": tenant,
                                "items": self.scheduler.list_workers(tenant)})

    def _claim_any_task(self, body):
        """Tenant-wide fair claim: 200 {"run_id","step"} or 204 when idle."""
        lease = body.get("lease_seconds", 30)
        if lease is None:
            raise WorkflowError("lease_seconds must be a positive number", "bad_lease")
        result = self.scheduler.claim_fair(body.get("tenant"), body.get("worker_id"), lease)
        if result is None:
            return self._send(204)
        run_id, step = result
        return self._json(200, {"run_id": run_id, "step": _step_view(step)})

    def _step_action(self, run_id, action, body):
        tenant, worker_id = self._tenant({}, body), body.get("worker_id")
        if action == "claim":
            step = self.scheduler.claim(tenant, run_id, worker_id, body.get("lease_seconds", 30))
            return self._send(204) if step is None else self._json(200, {"step": _step_view(step)})
        if action == "complete":
            run = self.scheduler.complete(tenant, run_id, body.get("step_id"), worker_id,
                                          body.get("result"))
        else:
            run = self.scheduler.fail(tenant, run_id, body.get("step_id"), worker_id,
                                      body.get("error"))
        return self._json(200, _run_view(run))

    def _heartbeat(self, run_id, body):
        """Renew a lease; strict field validation, anything bad is a 400."""
        for name in ("tenant", "step_id", "worker_id"):
            value = body.get(name)
            if not isinstance(value, str) or not value.strip():
                raise WorkflowError("%s must be a non-empty string" % name, "bad_%s" % name)
        run = self.scheduler.heartbeat(body["tenant"], run_id, body["step_id"],
                                       body["worker_id"], body.get("lease_seconds", 30))
        return self._json(200, _run_view(run))

    def _decision(self, run_id, body):
        """Validate strictly; any missing/blank/wrong-type field is a 400."""
        fields = {}
        for name in ("tenant", "step_id", "actor"):
            value = body.get(name)
            if not isinstance(value, str) or not value.strip():
                raise WorkflowError("%s must be a non-empty string" % name, "bad_%s" % name)
            fields[name] = value.strip()
        decision = body.get("decision")
        if decision not in DECISIONS:
            raise WorkflowError("decision must be one of %s" % ", ".join(DECISIONS),
                                "bad_decision")
        tenant = fields["tenant"]
        owner = self.scheduler.store.run_exists(run_id)
        if owner is not None and owner != tenant:
            raise WorkflowError("run %s belongs to another tenant" % run_id, "cross_tenant")
        run = self.scheduler.decide(tenant, run_id, fields["step_id"], fields["actor"], decision)
        return self._json(200, _run_view(run))


def _query_int(query, name, code):
    """Parse an optional integer query parameter; ``None`` when absent."""
    raw = (query.get(name) or [None])[0]
    if raw is None:
        return None
    text = raw.strip()
    if not re.fullmatch(r"[+-]?\d+", text):
        raise WorkflowError("%s must be an integer" % name, code)
    return int(text)


def _step_view(step):
    return {"id": step["id"], "kind": step.get("kind", "task"), "status": step["status"],
            "attempt": step["attempt"],
            "max_attempts": step["max_attempts"], "depends_on": list(step["depends_on"]),
            "worker_id": step["worker_id"], "lease_deadline": step["lease_deadline"],
            "next_attempt_at": step["next_attempt_at"], "result": step.get("result"),
            "error": step.get("error"), "approval": step.get("approval")}


def _schedule_view(record):
    return {"tenant": record["tenant"], "schedule_id": record["schedule_id"],
            "workflow_id": record["workflow_id"],
            "interval_seconds": record["interval_seconds"],
            "first_at": record.get("first_at"), "next_at": record.get("next_at"),
            "params": record.get("params") or {},
            "max_parallelism": record.get("max_parallelism"),
            "created_at": record.get("created_at"), "updated_at": record.get("updated_at")}


def _run_view(run, scheduled_at=None):
    if scheduled_at is None:
        scheduled_at = run.get("scheduled_at")
    return {"tenant": run["tenant"], "run_id": run["run_id"], "workflow_id": run["workflow_id"],
            "status": run["status"], "params": run.get("params") or {},
            "max_parallelism": run.get("max_parallelism"),
            "idempotency_key": run.get("idempotency_key"),
            "not_before": run.get("not_before"),
            "schedule_id": run.get("schedule_id"),
            "scheduled_at": scheduled_at,
            "created_at": run.get("created_at"), "updated_at": run.get("updated_at"),
            "steps": [_step_view(run["steps"][sid]) for sid in run["step_order"]],
            "history_length": len(run.get("history") or [])}


def create_server(store, host="127.0.0.1", port=8080, scheduler=None):
    """Build (but do not start) a ThreadingHTTPServer bound to ``host:port``."""
    if scheduler is None:
        from .scheduler import Scheduler

        scheduler = Scheduler(store)
    return ThreadingHTTPServer((host, int(port)),
                               type("FlowdHandler", (_Handler,), {"scheduler": scheduler}))
