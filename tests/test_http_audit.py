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
        """Return (status, parsed json or None); never raises on HTTP errors."""
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
        separator = "&" if query else ""
        return self.call("GET", "/v1/audit?tenant=%s%s%s" % (tenant, separator, query))

    def seed(self, tenant="acme", run_id="r1"):
        self.call("POST", "/v1/workflows",
                  {"tenant": tenant, "workflow_id": "etl", "steps": LINEAR})
        self.call("POST", "/v1/runs",
                  {"tenant": tenant, "workflow_id": "etl", "run_id": run_id})
        self.call("POST", "/v1/runs/%s/claim" % run_id,
                  {"tenant": tenant, "worker_id": "w1", "lease_seconds": 60})

    def test_audit_stream_over_http(self):
        self.seed()
        status, body = self.audit()
        self.assertEqual(status, 200)
        self.assertEqual(body["tenant"], "acme")
        self.assertIsNone(body["next_after"])
        self.assertEqual([r["action"] for r in body["items"]],
                         ["workflow.submit", "run_created", "ready", "claim",
                          "run_started"])
        sequences = [r["sequence"] for r in body["items"]]
        self.assertEqual(sequences, [1, 2, 3, 4, 5])
        first = body["items"][0]
        self.assertEqual(first["tenant"], "acme")
        self.assertIsNone(first["run_id"])
        self.assertIsNone(first["actor"])
        self.assertTrue(first["at"].endswith("Z"))

    def test_empty_stream_is_200_with_empty_items(self):
        status, body = self.audit(tenant="ghost")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"tenant": "ghost", "items": [], "next_after": None})

    def test_tenant_isolation_over_http(self):
        self.seed(tenant="acme", run_id="r1")
        self.seed(tenant="globex", run_id="g1")
        status, body = self.audit(tenant="globex")
        self.assertEqual(status, 200)
        self.assertTrue(all(r["tenant"] == "globex" for r in body["items"]))
        self.assertEqual([r["sequence"] for r in body["items"]], [1, 2, 3, 4, 5])
        # Filters cannot probe the other tenant's stream.
        status, body = self.audit("run_id=g1")
        self.assertEqual((status, body["items"]), (200, []))
        status, body = self.audit("run_id=r1", tenant="globex")
        self.assertEqual((status, body["items"]), (200, []))

    def test_action_and_run_id_filters(self):
        self.seed()
        self.call("POST", "/v1/workers/register", {"tenant": "acme", "worker_id": "w1"})
        status, body = self.audit("action=claim")
        self.assertEqual(status, 200)
        self.assertEqual([r["action"] for r in body["items"]], ["claim"])
        self.assertEqual(body["items"][0]["worker_id"], "w1")
        status, body = self.audit("action=worker.register")
        self.assertEqual([r["action"] for r in body["items"]], ["worker.register"])
        status, body = self.audit("run_id=r1")
        self.assertEqual([r["action"] for r in body["items"]],
                         ["run_created", "ready", "claim", "run_started"])
        status, body = self.audit("action=claim&run_id=r1")
        self.assertEqual(len(body["items"]), 1)

    def test_pagination_with_limit_and_after(self):
        self.seed()
        status, body = self.audit("limit=2")
        self.assertEqual(status, 200)
        self.assertEqual([r["sequence"] for r in body["items"]], [1, 2])
        self.assertEqual(body["next_after"], 2)
        status, body = self.audit("limit=2&after=2")
        self.assertEqual([r["sequence"] for r in body["items"]], [3, 4])
        self.assertEqual(body["next_after"], 4)
        status, body = self.audit("limit=2&after=4")
        self.assertEqual([r["sequence"] for r in body["items"]], [5])
        self.assertIsNone(body["next_after"])
        status, body = self.audit("after=5")
        self.assertEqual((body["items"], body["next_after"]), ([], None))

    def test_bad_tenant(self):
        for path in ("/v1/audit", "/v1/audit?tenant=", "/v1/audit?tenant=%20%20"):
            status, body = self.call("GET", path)
            self.assertEqual(status, 400, path)
            self.assertIn("error", body)

    def test_bad_limit(self):
        for value in ("0", "-1", "1001", "abc", "1.5", ""):
            status, body = self.audit("limit=%s" % value)
            self.assertEqual(status, 400, value)
            self.assertIn("error", body)
        for value in ("1", "100", "1000"):
            status, _ = self.audit("limit=%s" % value)
            self.assertEqual(status, 200, value)

    def test_bad_after(self):
        for value in ("-1", "abc", "1.5", ""):
            status, body = self.audit("after=%s" % value)
            self.assertEqual(status, 400, value)
            self.assertIn("error", body)
        status, _ = self.audit("after=0")
        self.assertEqual(status, 200)

    def test_bad_action_and_run_id(self):
        for query in ("action=", "action=%20%20"):
            status, body = self.audit(query)
            self.assertEqual(status, 400, query)
            self.assertIn("error", body)
        for query in ("run_id=", "run_id=%20%20"):
            status, body = self.audit(query)
            self.assertEqual(status, 400, query)
            self.assertIn("error", body)

    def test_rejected_and_noop_requests_leave_no_audit_trail(self):
        self.seed()
        status, _ = self.audit()
        count = len(self.call("GET", "/v1/audit?tenant=acme")[1]["items"])
        # Read-only requests.
        self.call("GET", "/v1/runs/r1?tenant=acme")
        self.call("GET", "/v1/runs?tenant=acme")
        self.call("GET", "/v1/workers?tenant=acme")
        self.call("GET", "/v1/schedules?tenant=acme")
        # Validation failures and conflicts.
        self.call("POST", "/v1/runs", {"tenant": "acme", "workflow_id": "ghost"})
        self.call("POST", "/v1/runs", {"tenant": "acme", "workflow_id": "etl",
                                       "max_parallelism": 0})
        self.call("POST", "/v1/runs/r1/complete",
                  {"tenant": "acme", "step_id": "step1", "worker_id": "stranger"})
        # Cross-tenant rejection.
        self.call("POST", "/v1/runs/r1/claim",
                  {"tenant": "globex", "worker_id": "w1", "lease_seconds": 10})
        # A heartbeat that does not extend the 60s lease.
        self.call("POST", "/v1/runs/r1/heartbeat",
                  {"tenant": "acme", "step_id": "step1", "worker_id": "w1",
                   "lease_seconds": 1})
        # Idempotency-key hit creates no new run.
        self.call("POST", "/v1/runs", {"tenant": "acme", "workflow_id": "etl",
                                       "run_id": "r2", "idempotency_key": "k"})
        self.call("POST", "/v1/runs", {"tenant": "acme", "workflow_id": "etl",
                                       "run_id": "r2", "idempotency_key": "k"})
        status, body = self.audit()
        self.assertEqual(status, 200)
        # Exactly two new records: the run_created + ready of the keyed r2.
        self.assertEqual(len(body["items"]), count + 2)
        self.assertEqual([r["action"] for r in body["items"]][-2:],
                         ["run_created", "ready"])
        runs = [r for r in body["items"] if r["action"] == "run_created"]
        self.assertEqual([r["run_id"] for r in runs], ["r1", "r2"])

    def test_not_due_dispatch_and_idle_claim_leave_no_audit_trail(self):
        self.seed()
        self.call("POST", "/v1/schedules",
                  {"tenant": "acme", "schedule_id": "s1", "workflow_id": "etl",
                   "interval_seconds": 3600, "first_at": 9999999999})
        status, before = self.audit()
        # Not-yet-due dispatch: 204, no record.
        status, _ = self.call("POST", "/v1/schedules/s1/dispatch", {"tenant": "acme"})
        self.assertEqual(status, 204)
        # Idle claim on the sleeping scheduled run is not possible; claim the
        # finished-nothing case: r1 has step1 leased, step2 pending -> 204.
        status, _ = self.call("POST", "/v1/tasks/claim",
                              {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 204)
        status, after = self.audit()
        self.assertEqual(after["items"], before["items"])

    def test_schedule_dispatch_is_audited_when_due(self):
        self.seed()
        self.call("POST", "/v1/schedules",
                  {"tenant": "acme", "schedule_id": "s1", "workflow_id": "etl",
                   "interval_seconds": 3600, "first_at": 0})
        status, run = self.call("POST", "/v1/schedules/s1/dispatch", {"tenant": "acme"})
        self.assertEqual(status, 201)
        status, body = self.audit("action=schedule.dispatch")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["items"]), 1)
        record = body["items"][0]
        self.assertEqual(record["schedule_id"], "s1")
        self.assertEqual(record["run_id"], run["run_id"])
        self.assertIsNone(record["actor"])


if __name__ == "__main__":
    unittest.main()
