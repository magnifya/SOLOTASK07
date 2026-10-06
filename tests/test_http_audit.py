"""End-to-end tests for GET /v1/audit."""

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


class HttpAuditTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-audit-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
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

    def audit(self, query="", tenant="acme"):
        return self.call("GET", "/v1/audit?tenant=%s%s" % (tenant, query))

    def seed(self):
        self.call("POST", "/v1/workflows",
                  {"tenant": "acme", "workflow_id": "etl", "steps": LINEAR})
        self.call("POST", "/v1/runs",
                  {"tenant": "acme", "workflow_id": "etl", "run_id": "r1"})
        self.call("POST", "/v1/runs/r1/claim",
                  {"tenant": "acme", "worker_id": "w1", "lease_seconds": 60})
        self.call("POST", "/v1/runs/r1/complete",
                  {"tenant": "acme", "step_id": "step1", "worker_id": "w1"})

    def test_empty_stream_is_200_with_empty_items(self):
        status, body = self.audit(tenant="ghost")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"tenant": "ghost", "items": [], "next_after": None})

    def test_audit_flow_records_control_plane_and_run_events(self):
        self.seed()
        status, body = self.audit()
        self.assertEqual(status, 200)
        self.assertEqual(body["tenant"], "acme")
        self.assertIsNone(body["next_after"])
        actions = [r["action"] for r in body["items"]]
        self.assertEqual(actions, ["workflow.submit", "run_created", "ready",
                                   "claim", "run_started", "complete", "ready"])
        sequences = [r["sequence"] for r in body["items"]]
        self.assertEqual(sequences, list(range(1, len(sequences) + 1)))
        for record in body["items"]:
            self.assertEqual(record["tenant"], "acme")
            self.assertTrue(record["at"].endswith("Z"))
        claim = [r for r in body["items"] if r["action"] == "claim"][0]
        self.assertEqual(claim["run_id"], "r1")
        self.assertEqual(claim["step_id"], "step1")
        self.assertEqual(claim["actor"], "w1")
        submit = body["items"][0]
        self.assertEqual(submit["workflow_id"], "etl")
        self.assertIsNone(submit["run_id"])

    def test_filters_and_pagination(self):
        self.seed()
        status, body = self.audit("&action=claim")
        self.assertEqual(status, 200)
        self.assertEqual([r["action"] for r in body["items"]], ["claim"])
        status, body = self.audit("&run_id=r1")
        self.assertTrue(all(r["run_id"] == "r1" for r in body["items"]))
        status, body = self.audit("&run_id=nope")
        self.assertEqual(body["items"], [])
        self.assertIsNone(body["next_after"])
        # Walk the stream one record at a time.
        seen, after = [], 0
        while True:
            status, page = self.audit("&limit=1&after=%d" % after)
            self.assertEqual(status, 200)
            seen.extend(page["items"])
            if page["next_after"] is None:
                break
            after = page["next_after"]
        everything = self.audit("&limit=1000")[1]["items"]
        self.assertEqual([r["sequence"] for r in seen],
                         [r["sequence"] for r in everything])

    def test_tenant_isolation(self):
        self.seed()
        self.call("POST", "/v1/workflows",
                  {"tenant": "globex", "workflow_id": "etl", "steps": LINEAR})
        acme = self.audit()[1]["items"]
        globex = self.audit(tenant="globex")[1]["items"]
        self.assertTrue(all(r["tenant"] == "acme" for r in acme))
        self.assertEqual([r["action"] for r in globex], ["workflow.submit"])
        # Another tenant's run_id is invisible behind the filter.
        self.assertEqual(self.audit("&run_id=r1", tenant="globex")[1]["items"], [])

    def test_noop_requests_append_nothing(self):
        self.seed()
        # Finish the run so a later claim is a no-state-change 204.
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        self.call("POST", "/v1/runs/r1/complete",
                  {"tenant": "acme", "step_id": "step2", "worker_id": "w1"})
        before = self.audit()[1]["items"]
        # 204: nothing ready to claim on the finished run, not-due dispatch.
        status, _ = self.call("POST", "/v1/runs/r1/claim",
                              {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 204)
        self.call("POST", "/v1/schedules",
                  {"tenant": "acme", "schedule_id": "s1", "workflow_id": "etl",
                   "interval_seconds": 3600, "first_at": 4_000_000_000})
        status, _ = self.call("POST", "/v1/schedules/s1/dispatch", {"tenant": "acme"})
        self.assertEqual(status, 204)
        # Idempotent replay of a keyed create appends nothing either.
        self.call("POST", "/v1/runs", {"tenant": "acme", "workflow_id": "etl",
                                       "run_id": "r2", "idempotency_key": "k"})
        middle = self.audit()[1]["items"]
        self.call("POST", "/v1/runs", {"tenant": "acme", "workflow_id": "etl",
                                       "run_id": "r2", "idempotency_key": "k"})
        self.assertEqual(self.audit()[1]["items"], middle)
        # The 204 claim and dispatch added nothing; schedule.create did.
        self.assertEqual([r["action"] for r in middle],
                         [r["action"] for r in before]
                         + ["schedule.create", "run_created", "ready"])

    def test_validation_errors(self):
        self.seed()
        for path in ("/v1/audit", "/v1/audit?tenant=", "/v1/audit?tenant=%20%20"):
            status, body = self.call("GET", path)
            self.assertEqual(status, 400, path)
            self.assertIn("error", body)
        for query in ("limit=0", "limit=1001", "limit=-1", "limit=abc",
                      "limit=1.5", "limit="):
            status, body = self.audit("&" + query)
            self.assertEqual(status, 400, query)
            self.assertIn("error", body)
        for query in ("after=-1", "after=x", "after=1.5", "after="):
            status, body = self.audit("&" + query)
            self.assertEqual(status, 400, query)
            self.assertIn("error", body)
        for query in ("action=", "action=%20%20", "run_id=", "run_id=%20"):
            status, body = self.audit("&" + query)
            self.assertEqual(status, 400, query)
            self.assertIn("error", body)
        # Boundary values are fine.
        self.assertEqual(self.audit("&limit=1")[0], 200)
        self.assertEqual(self.audit("&limit=1000")[0], 200)
        self.assertEqual(self.audit("&after=0")[0], 200)
        # Failed queries append nothing.
        self.assertEqual(len(self.audit()[1]["items"]), 7)

    def test_reading_audit_does_not_touch_runs(self):
        self.seed()
        before = self.call("GET", "/v1/runs/r1?tenant=acme")[1]
        self.audit("&limit=1")
        self.audit("&action=claim")
        after = self.call("GET", "/v1/runs/r1?tenant=acme")[1]
        self.assertEqual(before["updated_at"], after["updated_at"])
        self.assertEqual(before["history_length"], after["history_length"])


if __name__ == "__main__":
    unittest.main()
