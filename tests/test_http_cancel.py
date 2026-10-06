"""End-to-end HTTP tests for run cancellation (POST /v1/runs/{id}/cancel)."""

import json
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from flowd.http_app import create_server
from flowd.store import WorkflowStore

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
]
GATE = [
    {"id": "work", "depends_on": []},
    {"id": "ok", "depends_on": ["work"], "kind": "approval"},
]


class HttpCancelTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-cancel-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, body="OMIT"):
        data = None
        if body != "OMIT":
            data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
        request = urllib.request.Request("http://127.0.0.1:%d%s" % (self.port, path),
                                         data=data, method=method)
        if data is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                payload = response.read()
                return response.status, (json.loads(payload) if payload else None)
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            return exc.code, (json.loads(payload) if payload else None)

    def submit(self, steps=LINEAR, workflow_id="wf", tenant="acme"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": workflow_id, "steps": steps})
        self.assertEqual(status, 201, body)

    def start(self, run_id="r1", tenant="acme", **extra):
        body = {"tenant": tenant, "workflow_id": "wf", "run_id": run_id}
        body.update(extra)
        status, payload = self.call("POST", "/v1/runs", body)
        self.assertEqual(status, 201, payload)
        return payload

    def cancel(self, run_id="r1", tenant="acme", actor="ops"):
        return self.call("POST", "/v1/runs/%s/cancel" % run_id,
                         {"tenant": tenant, "actor": actor})


class HttpCancelFlowTest(HttpCancelTest):
    def test_cancel_pending_run(self):
        self.submit()
        self.start()
        status, run = self.cancel()
        self.assertEqual(status, 200, run)
        self.assertEqual(run["status"], "cancelled")
        self.assertEqual([s["status"] for s in run["steps"]],
                         ["cancelled", "cancelled"])
        # The same state is visible through status and list.
        status, fetched = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(fetched, run)
        status, listing = self.call("GET", "/v1/runs?tenant=acme&status=cancelled")
        self.assertEqual(status, 200)
        self.assertEqual([item["run_id"] for item in listing["items"]], ["r1"])
        # The audit stream records the cancellation in order.
        status, audit = self.call("GET", "/v1/audit?tenant=acme&run_id=r1")
        self.assertEqual(status, 200)
        self.assertEqual([item["action"] for item in audit["items"]],
                         ["run_created", "ready", "cancel", "cancel", "run_cancelled"])
        self.assertEqual(audit["items"][-1]["actor"], "ops")

    def test_cancel_running_run_releases_lease(self):
        self.submit()
        self.start()
        status, claim = self.call("POST", "/v1/runs/r1/claim",
                                  {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 200, claim)
        status, run = self.cancel()
        self.assertEqual(status, 200, run)
        step1 = run["steps"][0]
        self.assertEqual(step1["status"], "cancelled")
        self.assertIsNone(step1["worker_id"])
        self.assertIsNone(step1["lease_deadline"])

    def test_repeat_cancel_returns_same_run(self):
        self.submit()
        self.start()
        status, first = self.cancel()
        self.assertEqual(status, 200)
        status, second = self.cancel(actor="someone-else")
        self.assertEqual(status, 200)
        self.assertEqual(second, first)

    def test_cancel_afterwards_blocks_work(self):
        self.submit(GATE)
        self.start()
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        status, _ = self.cancel()
        self.assertEqual(status, 200)
        # claim keeps returning 204
        status, _ = self.call("POST", "/v1/runs/r1/claim",
                              {"tenant": "acme", "worker_id": "w2"})
        self.assertEqual(status, 204)
        status, _ = self.call("POST", "/v1/tasks/claim",
                              {"tenant": "acme", "worker_id": "w2"})
        self.assertEqual(status, 204)
        # complete / fail / heartbeat / decision all conflict
        for path, body in (
            ("/v1/runs/r1/complete",
             {"tenant": "acme", "step_id": "work", "worker_id": "w1"}),
            ("/v1/runs/r1/fail",
             {"tenant": "acme", "step_id": "work", "worker_id": "w1"}),
            ("/v1/runs/r1/heartbeat",
             {"tenant": "acme", "step_id": "work", "worker_id": "w1"}),
            ("/v1/runs/r1/decision",
             {"tenant": "acme", "step_id": "ok", "actor": "a", "decision": "approve"}),
        ):
            status, payload = self.call("POST", path, body)
            self.assertEqual(status, 409, (path, payload))
            self.assertIn("cancelled", payload["error"])

    def test_finished_run_conflicts(self):
        self.submit()
        self.start()
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        self.call("POST", "/v1/runs/r1/complete",
                  {"tenant": "acme", "step_id": "step1", "worker_id": "w1"})
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        self.call("POST", "/v1/runs/r1/complete",
                  {"tenant": "acme", "step_id": "step2", "worker_id": "w1"})
        status, payload = self.cancel()
        self.assertEqual(status, 409, payload)
        self.assertIn("already succeeded", payload["error"])


class HttpCancelValidationTest(HttpCancelTest):
    def test_bad_json(self):
        self.submit()
        self.start()
        status, _ = self.call("POST", "/v1/runs/r1/cancel", b"{not json")
        self.assertEqual(status, 400)
        status, _ = self.call("POST", "/v1/runs/r1/cancel", b"[1, 2]")
        self.assertEqual(status, 400)

    def test_bad_tenant(self):
        self.submit()
        self.start()
        for body in ({"actor": "ops"}, {"tenant": "", "actor": "ops"},
                     {"tenant": "   ", "actor": "ops"}, {"tenant": 5, "actor": "ops"}):
            status, _ = self.call("POST", "/v1/runs/r1/cancel", body)
            self.assertEqual(status, 400, body)

    def test_bad_actor(self):
        self.submit()
        self.start()
        for body in ({"tenant": "acme"}, {"tenant": "acme", "actor": ""},
                     {"tenant": "acme", "actor": "  "}, {"tenant": "acme", "actor": 9}):
            status, _ = self.call("POST", "/v1/runs/r1/cancel", body)
            self.assertEqual(status, 400, body)

    def test_unknown_run(self):
        status, _ = self.cancel(run_id="nope")
        self.assertEqual(status, 404)

    def test_cross_tenant(self):
        self.submit()
        self.start()
        status, _ = self.cancel(tenant="other")
        self.assertEqual(status, 403)


if __name__ == "__main__":
    unittest.main()
