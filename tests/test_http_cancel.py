"""End-to-end tests for POST /v1/runs/{run_id}/cancel."""

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


class HttpCancelTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-cancel-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, body=None, raw_body=None):
        """Return (status, parsed json or None); never raises on HTTP errors."""
        data = raw_body
        if body is not None:
            data = json.dumps(body).encode("utf-8")
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

    def start_run(self, tenant="acme", run_id="r1"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": "etl", "steps": LINEAR})
        self.assertEqual(status, 201, body)
        status, body = self.call("POST", "/v1/runs",
                                 {"tenant": tenant, "workflow_id": "etl", "run_id": run_id})
        self.assertEqual(status, 201, body)
        return body

    def cancel(self, run_id="r1", tenant="acme", actor="ops"):
        return self.call("POST", "/v1/runs/%s/cancel" % run_id,
                         {"tenant": tenant, "actor": actor})

    def test_cancel_returns_200_run_view(self):
        self.start_run()
        status, body = self.cancel()
        self.assertEqual(status, 200, body)
        self.assertEqual(body["status"], "cancelled")
        self.assertEqual([s["status"] for s in body["steps"]],
                         ["cancelled", "cancelled"])
        # The same state is visible through GET and list.
        status, fetched = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(fetched["status"], "cancelled")
        self.assertEqual(fetched["updated_at"], body["updated_at"])
        status, listing = self.call("GET", "/v1/runs?tenant=acme&status=cancelled")
        self.assertEqual([r["run_id"] for r in listing["items"]], ["r1"])

    def test_repeat_cancel_returns_same_run_without_new_history(self):
        self.start_run()
        _, first = self.cancel()
        status, second = self.cancel(actor="other")
        self.assertEqual(status, 200)
        self.assertEqual(second["updated_at"], first["updated_at"])
        self.assertEqual(second["history_length"], first["history_length"])

    def test_bad_json(self):
        self.start_run()
        status, body = self.call("POST", "/v1/runs/r1/cancel", raw_body=b"{nope")
        self.assertEqual(status, 400)
        status, body = self.call("POST", "/v1/runs/r1/cancel", raw_body=b"[1, 2]")
        self.assertEqual(status, 400)

    def test_bad_tenant_and_actor(self):
        self.start_run()
        for payload in ({}, {"tenant": "", "actor": "ops"},
                        {"tenant": "  ", "actor": "ops"},
                        {"tenant": 3, "actor": "ops"},
                        {"tenant": "acme"}, {"tenant": "acme", "actor": ""},
                        {"tenant": "acme", "actor": "  "},
                        {"tenant": "acme", "actor": 3}):
            status, body = self.call("POST", "/v1/runs/r1/cancel", payload)
            self.assertEqual(status, 400, payload)

    def test_unknown_run_404(self):
        status, _ = self.cancel(run_id="ghost")
        self.assertEqual(status, 404)

    def test_cross_tenant_403(self):
        self.start_run()
        status, _ = self.cancel(tenant="other")
        self.assertEqual(status, 403)

    def test_finished_run_409(self):
        self.start_run()
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        self.call("POST", "/v1/runs/r1/complete",
                  {"tenant": "acme", "step_id": "step1", "worker_id": "w1"})
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        self.call("POST", "/v1/runs/r1/complete",
                  {"tenant": "acme", "step_id": "step2", "worker_id": "w1"})
        status, _ = self.cancel()
        self.assertEqual(status, 409)

    def test_actions_after_cancel(self):
        self.start_run()
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        status, _ = self.cancel()
        self.assertEqual(status, 200)
        # claim keeps returning 204
        status, _ = self.call("POST", "/v1/runs/r1/claim",
                              {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 204)
        status, _ = self.call("POST", "/v1/tasks/claim",
                              {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 204)
        # complete / fail / heartbeat conflict with run_cancelled
        for action, payload in (
            ("complete", {"tenant": "acme", "step_id": "step1", "worker_id": "w1"}),
            ("fail", {"tenant": "acme", "step_id": "step1", "worker_id": "w1"}),
            ("heartbeat", {"tenant": "acme", "step_id": "step1", "worker_id": "w1"}),
        ):
            status, _ = self.call("POST", "/v1/runs/r1/%s" % action, payload)
            self.assertEqual(status, 409, action)

    def test_audit_stream_shows_cancel(self):
        self.start_run()
        self.cancel(actor="admin")
        status, body = self.call("GET", "/v1/audit?tenant=acme&run_id=r1")
        self.assertEqual(status, 200)
        actions = [r["action"] for r in body["items"]]
        self.assertEqual(actions[-3:], ["cancel", "cancel", "run_cancelled"])
        self.assertTrue(all(r["actor"] == "admin" for r in body["items"][-3:]))


if __name__ == "__main__":
    unittest.main()
