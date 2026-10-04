"""End-to-end tests for the stdlib HTTP API."""

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
    {"id": "step3", "depends_on": ["step2"]},
]


class HttpTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-")
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

    def submit_workflow(self, tenant="acme", workflow_id="etl", steps=None):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": workflow_id,
                                  "steps": LINEAR if steps is None else steps})
        self.assertEqual(status, 201, body)
        return body

    def start_run(self, tenant="acme", workflow_id="etl", run_id=None):
        payload = {"tenant": tenant, "workflow_id": workflow_id}
        if run_id:
            payload["run_id"] = run_id
        status, body = self.call("POST", "/v1/runs", payload)
        self.assertEqual(status, 201, body)
        return body

    def claim(self, run_id="r1", tenant="acme", worker="w1", lease=60):
        return self.call("POST", "/v1/runs/%s/claim" % run_id,
                         {"tenant": tenant, "worker_id": worker, "lease_seconds": lease})

    def complete(self, step_id, run_id="r1", tenant="acme", worker="w1"):
        return self.call("POST", "/v1/runs/%s/complete" % run_id,
                         {"tenant": tenant, "step_id": step_id, "worker_id": worker})

    def drive(self, run_id="r1"):
        """Claim/complete the whole linear DAG over HTTP."""
        body = None
        for expected in ("step1", "step2", "step3"):
            status, body = self.claim(run_id)
            self.assertEqual(status, 200, body)
            self.assertEqual(body["step"]["id"], expected)
            status, body = self.complete(expected, run_id)
            self.assertEqual(status, 200, body)
        return body

    def test_healthz(self):
        self.assertEqual(self.call("GET", "/healthz"), (200, {"ok": True}))

    def test_workflow_submit_and_validation_error(self):
        self.assertEqual(self.submit_workflow()["order"], ["step1", "step2", "step3"])
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": "acme", "workflow_id": "bad",
                                  "steps": [{"id": "a", "depends_on": ["ghost"]}]})
        self.assertEqual(status, 400)
        self.assertIn("ghost", body["error"])

    def test_malformed_json_returns_400(self):
        status, body = self.call("POST", "/v1/workflows", raw_body=b"{not json")
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_run_lifecycle_over_http(self):
        self.submit_workflow()
        run = self.start_run(run_id="r1")
        self.assertEqual(run["status"], "pending")
        self.assertEqual([s["status"] for s in run["steps"]], ["ready", "pending", "pending"])
        self.assertEqual(self.drive()["status"], "succeeded")
        status, body = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "succeeded")
        self.assertEqual([s["status"] for s in body["steps"]], ["succeeded"] * 3)

    def test_claim_returns_204_when_nothing_ready(self):
        self.submit_workflow()
        self.start_run(run_id="r1")
        self.drive()
        self.assertEqual(self.claim(), (204, None))

    def test_fail_retries_then_exhausts(self):
        self.submit_workflow(workflow_id="flaky",
                             steps=[{"id": "s", "depends_on": [], "max_attempts": 2}])
        self.start_run(workflow_id="flaky", run_id="r2")
        self.claim("r2")
        status, body = self.call("POST", "/v1/runs/r2/fail",
                                 {"tenant": "acme", "step_id": "s", "worker_id": "w1",
                                  "error": "boom"})
        self.assertEqual(status, 200)
        step = body["steps"][0]
        self.assertEqual((step["status"], step["attempt"]), ("ready", 1))
        self.assertIsNotNone(step["next_attempt_at"])
        self.claim("r2")
        status, body = self.call("POST", "/v1/runs/r2/fail",
                                 {"tenant": "acme", "step_id": "s", "worker_id": "w1",
                                  "error": "boom again"})
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "failed")
        self.assertEqual(body["steps"][0]["status"], "failed")

    def test_complete_without_lease_is_409(self):
        self.submit_workflow()
        self.start_run(run_id="r1")
        status, body = self.complete("step1", worker="w9")
        self.assertEqual(status, 409)
        self.assertIn("error", body)

    def test_unknown_run_is_404(self):
        status, body = self.call("GET", "/v1/runs/nope?tenant=acme")
        self.assertEqual(status, 404)
        self.assertIn("error", body)

    def test_cross_tenant_read_is_403(self):
        self.submit_workflow()
        self.start_run(run_id="r1")
        status, body = self.call("GET", "/v1/runs/r1?tenant=globex")
        self.assertEqual(status, 403)
        self.assertIn("error", body)
        status, body = self.call("GET", "/v1/runs?tenant=globex")
        self.assertEqual((status, body["items"]), (200, []))

    def test_listing_with_status_filter_and_limit(self):
        self.submit_workflow()
        for index in range(3):
            self.start_run(run_id="run%d" % index)
        status, body = self.call("GET", "/v1/runs?tenant=acme&limit=2")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["items"]), 2)
        self.assertEqual(body["next_after"], body["items"][-1]["run_id"])
        self.assertEqual(self.call("GET", "/v1/runs?tenant=acme&status=succeeded")[1]["items"], [])
        self.assertEqual(
            [r["run_id"] for r in self.call("GET", "/v1/runs?tenant=acme&limit=1&after=run0")[1]
             ["items"]], ["run1"])

    def test_missing_tenant_is_400(self):
        status, body = self.call("GET", "/v1/runs?tenant=")
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_unknown_route_is_404(self):
        status, body = self.call("GET", "/v1/nothing")
        self.assertEqual(status, 404)
        self.assertIn("error", body)


if __name__ == "__main__":
    unittest.main()
