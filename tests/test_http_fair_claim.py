"""End-to-end HTTP tests for POST /v1/tasks/claim (tenant-wide fair claim)."""

import json
import shutil
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from flowd.http_app import create_server
from flowd.store import WorkflowStore

FAN = [{"id": "a", "depends_on": []}, {"id": "b", "depends_on": []}]


class FairClaimHttpTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-fair-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, body=None, raw_body=None):
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

    def prepare(self, tenant="acme", run_id="r1", workflow_id="wf", steps=None, **kwargs):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": workflow_id,
                                  "steps": steps or FAN})
        self.assertEqual(status, 201, body)
        status, body = self.call("POST", "/v1/runs",
                                 dict({"tenant": tenant, "workflow_id": workflow_id,
                                       "run_id": run_id}, **kwargs))
        self.assertEqual(status, 201, body)

    def claim(self, tenant="acme", worker="w1", lease="omit"):
        body = {"tenant": tenant, "worker_id": worker}
        if lease != "omit":
            body["lease_seconds"] = lease
        return self.call("POST", "/v1/tasks/claim", body)

    # -- validation ----------------------------------------------------
    def test_bad_json_and_non_object_are_400(self):
        self.assertEqual(self.call("POST", "/v1/tasks/claim", raw_body=b"nope")[0], 400)
        self.assertEqual(self.call("POST", "/v1/tasks/claim", raw_body=b"[1]")[0], 400)

    def test_bad_tenant_worker_lease_are_400(self):
        self.prepare()
        bad = [
            {"worker_id": "w1"},
            {"tenant": "", "worker_id": "w1"},
            {"tenant": "   ", "worker_id": "w1"},
            {"tenant": 7, "worker_id": "w1"},
            {"tenant": "acme"},
            {"tenant": "acme", "worker_id": ""},
            {"tenant": "acme", "worker_id": "  "},
            {"tenant": "acme", "worker_id": 5},
            {"tenant": "acme", "worker_id": "w1", "lease_seconds": 0},
            {"tenant": "acme", "worker_id": "w1", "lease_seconds": -2},
            {"tenant": "acme", "worker_id": "w1", "lease_seconds": "30"},
            {"tenant": "acme", "worker_id": "w1", "lease_seconds": True},
            {"tenant": "acme", "worker_id": "w1", "lease_seconds": None},
        ]
        for payload in bad:
            status, body = self.call("POST", "/v1/tasks/claim", payload)
            self.assertEqual(status, 400, payload)
            self.assertIn("error", body)
        # Nothing was claimed by the rejected calls.
        self.assertEqual(self.claim()[0], 200)

    # -- happy path ------------------------------------------------------
    def test_no_runs_returns_204(self):
        self.assertEqual(self.claim(), (204, None))

    def test_claim_returns_run_id_and_step_view(self):
        self.prepare()
        status, body = self.claim()
        self.assertEqual(status, 200, body)
        self.assertEqual(set(body), {"run_id", "step"})
        self.assertEqual(body["run_id"], "r1")
        step = body["step"]
        self.assertEqual(step["id"], "a")
        self.assertEqual(step["status"], "running")
        self.assertEqual(step["worker_id"], "w1")
        self.assertIsNotNone(step["lease_deadline"])
        self.assertEqual(set(step),
                         {"id", "kind", "status", "attempt", "max_attempts",
                          "depends_on", "trigger_rule", "worker_id", "lease_deadline",
                          "next_attempt_at", "result", "error", "approval"})

    def test_lease_seconds_default_and_explicit(self):
        self.prepare()
        status, body = self.claim()  # omitted -> default 30
        self.assertEqual(status, 200)
        status, body = self.claim(lease=0.5)
        self.assertEqual(status, 200)
        self.assertEqual(body["step"]["id"], "b")
        self.assertEqual(self.claim(), (204, None))

    def test_fair_rotation_across_runs(self):
        self.prepare(run_id="r-a")
        self.prepare(run_id="r-b")
        picked = [self.claim()[1]["run_id"] for _ in range(4)]
        self.assertEqual(picked, ["r-a", "r-b", "r-a", "r-b"])
        self.assertEqual(self.claim(), (204, None))

    def test_quota_full_run_is_skipped(self):
        self.prepare(run_id="r1", max_parallelism=1)
        self.assertEqual(self.claim()[0], 200)
        self.assertEqual(self.claim(), (204, None))

    def test_future_not_before_run_is_skipped(self):
        self.prepare(run_id="r1", not_before=time.time() + 3600)
        self.assertEqual(self.claim(), (204, None))
        status, run = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(run["history_length"], 1)  # still just run_created

    def test_run_with_only_pending_retry_is_skipped(self):
        self.prepare(run_id="r1", workflow_id="flaky",
                     steps=[{"id": "s", "depends_on": [], "max_attempts": 2}])
        self.prepare(run_id="r2", workflow_id="wf2")
        status, body = self.claim()
        self.assertEqual((status, body["run_id"]), (200, "r1"))
        status, body = self.call("POST", "/v1/runs/r1/fail",
                                 {"tenant": "acme", "step_id": "s", "worker_id": "w1",
                                  "error": "boom"})
        self.assertEqual(status, 200)
        self.assertIsNotNone(body["steps"][0]["next_attempt_at"])
        # r1's retry is still backing off: the fair claim lands on r2 and
        # r1's history and updated_at stay untouched.
        before = self.call("GET", "/v1/runs/r1?tenant=acme")[1]
        status, body = self.claim()
        self.assertEqual((status, body["run_id"]), (200, "r2"))
        after = self.call("GET", "/v1/runs/r1?tenant=acme")[1]
        self.assertEqual(after["updated_at"], before["updated_at"])
        self.assertEqual(after["history_length"], before["history_length"])
        time.sleep(1.1)  # wait out the 1s default backoff
        status, body = self.claim()
        self.assertEqual((status, body["run_id"]), (200, "r1"))
        self.assertEqual((body["step"]["id"], body["step"]["attempt"]), ("s", 1))
        self.assertIsNone(body["step"]["next_attempt_at"])

    def test_all_runs_backing_off_returns_204(self):
        self.prepare(run_id="r1", workflow_id="flaky",
                     steps=[{"id": "s", "depends_on": [], "max_attempts": 2}])
        self.assertEqual(self.claim()[0], 200)
        self.call("POST", "/v1/runs/r1/fail",
                  {"tenant": "acme", "step_id": "s", "worker_id": "w1", "error": "boom"})
        self.assertEqual(self.claim(), (204, None))

    # -- worker registry gate ---------------------------------------------
    def test_worker_registration_gate(self):
        self.prepare()
        status, _ = self.call("POST", "/v1/workers/register",
                              {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 201)
        status, body = self.claim(worker="stranger")
        self.assertEqual(status, 409)
        self.assertIn("worker_not_registered", json.dumps(body))
        self.assertEqual(self.claim(worker="w1")[0], 200)

    def test_expired_worker_is_409(self):
        self.prepare()
        self.call("POST", "/v1/workers/register",
                  {"tenant": "acme", "worker_id": "w1", "lease_seconds": 0.05})
        for _ in range(50):
            workers = self.call("GET", "/v1/workers?tenant=acme")[1]["items"]
            if workers[0]["status"] == "expired":
                break
            time.sleep(0.02)
        status, body = self.claim(worker="w1")
        self.assertEqual(status, 409)
        self.assertIn("worker_expired", json.dumps(body))

    # -- isolation ---------------------------------------------------------
    def test_tenant_isolation(self):
        self.prepare(tenant="acme", run_id="r1")
        self.prepare(tenant="globex", run_id="g1")
        status, body = self.claim(tenant="acme")
        self.assertEqual((status, body["run_id"]), (200, "r1"))
        # globex still has both of its tasks; acme's second claim stays in acme.
        status, body = self.claim(tenant="acme")
        self.assertEqual((status, body["run_id"]), (200, "r1"))
        self.assertEqual(self.claim(tenant="acme"), (204, None))
        status, run = self.call("GET", "/v1/runs/g1?tenant=globex")
        self.assertEqual(run["history_length"], 3)  # created + 2 ready, no claim

    def test_single_run_claim_still_works(self):
        self.prepare()
        status, body = self.call("POST", "/v1/runs/r1/claim",
                                 {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 200)
        self.assertEqual(set(body), {"step"})
        self.assertEqual(body["step"]["id"], "a")


if __name__ == "__main__":
    unittest.main()
