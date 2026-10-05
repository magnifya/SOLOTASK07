"""Standard library HTTP API for flowd."""

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .model import WorkflowError
from .scheduler import ConflictError, LeaseError

MAX_BODY = 1 << 20
STATUS_FOR_CODE = {"unknown_run": 404, "unknown_workflow": 404, "unknown_step": 404,
                   "cross_tenant": 403}
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
            self._route(method, path, parse_qs(parsed.query))
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
        elif method == "POST":
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


def _step_view(step):
    return {"id": step["id"], "kind": step.get("kind", "task"), "status": step["status"],
            "attempt": step["attempt"],
            "max_attempts": step["max_attempts"], "depends_on": list(step["depends_on"]),
            "worker_id": step["worker_id"], "lease_deadline": step["lease_deadline"],
            "next_attempt_at": step["next_attempt_at"], "result": step.get("result"),
            "error": step.get("error"), "approval": step.get("approval")}


def _run_view(run):
    return {"tenant": run["tenant"], "run_id": run["run_id"], "workflow_id": run["workflow_id"],
            "status": run["status"], "params": run.get("params") or {},
            "max_parallelism": run.get("max_parallelism"),
            "idempotency_key": run.get("idempotency_key"),
            "not_before": run.get("not_before"),
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
