"""HTTP API tests for tenant-level concurrency quotas (/v1/quotas)."""

import json
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from flowd.http_app import create_server
from flowd.store import WorkflowStore

FAN = [{"id": "a", "depends_on": []}, {"id": "b", "depends_on": []},
       {"id": "c", "depends_on": []}]


class HttpTenantQuotaTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-tenant-quota-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, body=None, raw=None):
        data = raw if raw is not None else (
            json.dumps(body).encode("utf-8") if body is not None else None)
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

    def set_quota(self, tenant="acme", quota=1):
        return self.call("POST", "/v1/quotas",
                         {"tenant": tenant, "max_parallelism": quota})

    def submit(self, tenant="acme", workflow_id="fan"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": workflow_id,
                                  "steps": FAN})
        self.assertEqual(status, 201, body)

    def start_run(self, run_id, tenant="acme"):
        status, body = self.call("POST", "/v1/runs",
                                 {"tenant": tenant, "workflow_id": "fan", "run_id": run_id})
        self.assertEqual(status, 201, body)

    def claim(self, run_id, worker="w1", lease=60, tenant="acme"):
        return self.call("POST", "/v1/runs/%s/claim" % run_id,
                         {"tenant": tenant, "worker_id": worker, "lease_seconds": lease})

    def claim_any(self, worker="w1", lease=60, tenant="acme"):
        return self.call("POST", "/v1/tasks/claim",
                         {"tenant": tenant, "worker_id": worker, "lease_seconds": lease})

    def audit_actions(self, tenant="acme"):
        status, body = self.call("GET", "/v1/audit?tenant=%s" % tenant)
        self.assertEqual(status, 200)
        return [item["action"] for item in body["items"]]

    # -- configuration endpoint ----------------------------------------
    def test_create_then_update_returns_201_then_200(self):
        status, body = self.set_quota(quota=2)
        self.assertEqual(status, 201, body)
        self.assertEqual(body["tenant"], "acme")
        self.assertEqual(body["max_parallelism"], 2)
        self.assertIsNotNone(body["updated_at"])
        status, body = self.set_quota(quota=3)
        self.assertEqual(status, 200, body)
        self.assertEqual(body["max_parallelism"], 3)

    def test_tenant_is_normalized_in_response_and_storage(self):
        status, body = self.set_quota(tenant="  acme  ")
        self.assertEqual(status, 201)
        self.assertEqual(body["tenant"], "acme")
        status, body = self.call("GET", "/v1/quotas?tenant=acme")
        self.assertEqual(body["max_parallelism"], 1)

    def test_omitted_or_null_quota_means_unlimited(self):
        status, body = self.call("POST", "/v1/quotas", {"tenant": "acme"})
        self.assertEqual(status, 201, body)
        self.assertIsNone(body["max_parallelism"])
        status, body = self.call("POST", "/v1/quotas",
                                 {"tenant": "acme", "max_parallelism": None})
        self.assertEqual(status, 200, body)
        self.assertIsNone(body["max_parallelism"])

    def test_same_value_update_keeps_updated_at_and_skips_audit(self):
        _, first = self.set_quota(quota=2)
        status, again = self.set_quota(quota=2)
        self.assertEqual(status, 200)
        self.assertEqual(again["updated_at"], first["updated_at"])
        self.assertEqual(self.audit_actions().count("quota.set"), 1)

    def test_every_change_appends_quota_set_audit(self):
        self.set_quota(quota=1)
        self.set_quota(quota=2)
        self.set_quota(quota=None)
        self.assertEqual(self.audit_actions().count("quota.set"), 3)

    def test_bad_json_and_non_object_bodies_are_rejected(self):
        for raw in (b"{not json", b"[1, 2]", b'"text"', b"42"):
            status, body = self.call("POST", "/v1/quotas", raw=raw)
            self.assertEqual(status, 400, raw)
            self.assertIn("error", body)
        self.assertEqual(self.audit_actions(), [])

    def test_bad_tenant_is_rejected(self):
        for payload in ({}, {"tenant": ""}, {"tenant": "   "}, {"tenant": 7},
                        {"tenant": None}, {"tenant": ["acme"]}):
            status, body = self.call("POST", "/v1/quotas",
                                     dict(payload, max_parallelism=1))
            self.assertEqual(status, 400, payload)
            self.assertIn("error", body)
        for path in ("/v1/quotas", "/v1/quotas?tenant=", "/v1/quotas?tenant=%20%20"):
            status, body = self.call("GET", path)
            self.assertEqual(status, 400, path)
            self.assertIn("error", body)
        self.assertEqual(self.audit_actions(), [])

    def test_bad_max_parallelism_is_rejected(self):
        for bad in (True, False, 2.5, 0.0, "2", 0, -1, [], {}):
            status, body = self.set_quota(quota=bad)
            self.assertEqual(status, 400, bad)
            self.assertIn("error", body)
        status, body = self.call("GET", "/v1/quotas?tenant=acme")
        self.assertEqual(status, 200)
        self.assertIsNone(body["max_parallelism"])
        self.assertEqual(self.audit_actions(), [])

    def test_unconfigured_tenant_reads_as_null(self):
        status, body = self.call("GET", "/v1/quotas?tenant=acme")
        self.assertEqual(status, 200, body)
        self.assertEqual(body["tenant"], "acme")
        self.assertIsNone(body["max_parallelism"])

    def test_read_queries_append_no_audit(self):
        self.set_quota(quota=1)
        before = len(self.audit_actions())
        self.call("GET", "/v1/quotas?tenant=acme")
        self.call("GET", "/v1/quotas?tenant=globex")
        self.assertEqual(len(self.audit_actions()), before)

    def test_quota_is_scoped_per_tenant(self):
        self.set_quota(tenant="acme", quota=1)
        status, body = self.call("GET", "/v1/quotas?tenant=globex")
        self.assertEqual(status, 200)
        self.assertIsNone(body["max_parallelism"])
        self.assertEqual(self.audit_actions("globex"), [])

    def test_quota_survives_restart(self):
        from flowd.scheduler import Scheduler
        self.set_quota(quota=2)
        # a "restarted" scheduler over the same data dir sees the quota
        self.server.RequestHandlerClass.scheduler = Scheduler(WorkflowStore(self.root))
        status, body = self.call("GET", "/v1/quotas?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(body["max_parallelism"], 2)

    # -- enforcement ----------------------------------------------------
    def test_claim_returns_204_when_tenant_budget_is_full(self):
        self.submit()
        self.set_quota(quota=1)
        self.start_run("r1")
        self.start_run("r2")
        status, body = self.claim("r1", worker="w1")
        self.assertEqual(status, 200, body)
        self.assertEqual(body["step"]["id"], "a")
        self.assertEqual(self.claim("r2", worker="w2"), (204, None))
        self.assertEqual(self.claim_any(worker="w3"), (204, None))

    def test_fair_claim_shares_the_budget(self):
        self.submit()
        self.set_quota(quota=1)
        self.start_run("r1")
        self.start_run("r2")
        status, body = self.claim_any(worker="w1")
        self.assertEqual(status, 200, body)
        self.assertEqual((body["run_id"], body["step"]["id"]), ("r1", "a"))
        self.assertEqual(self.claim_any(worker="w2"), (204, None))
        status, body = self.call("POST", "/v1/runs/r1/complete",
                                 {"tenant": "acme", "step_id": "a", "worker_id": "w1"})
        self.assertEqual(status, 200, body)
        status, body = self.claim_any(worker="w2")
        self.assertEqual(status, 200, body)
        self.assertEqual((body["run_id"], body["step"]["id"]), ("r2", "a"))

    def test_released_slot_can_be_claimed_again(self):
        self.submit()
        self.set_quota(quota=1)
        self.start_run("r1")
        self.start_run("r2")
        self.claim("r1", worker="w1")
        self.assertEqual(self.claim("r2", worker="w2"), (204, None))
        status, _ = self.call("POST", "/v1/runs/r1/complete",
                              {"tenant": "acme", "step_id": "a", "worker_id": "w1"})
        self.assertEqual(status, 200)
        status, body = self.claim("r2", worker="w2")
        self.assertEqual(status, 200, body)
        self.assertEqual(body["step"]["id"], "a")

    def test_unconfigured_tenant_keeps_legacy_behavior(self):
        self.submit()
        self.start_run("r1")
        self.start_run("r2")
        for run_id in ("r1", "r2"):
            for worker in ("w1", "w2", "w3"):
                status, _ = self.claim(run_id, worker=worker)
                self.assertEqual(status, 200, (run_id, worker))

    def test_quota_does_not_leak_across_tenants(self):
        self.submit()
        self.submit(tenant="globex")
        self.set_quota(tenant="acme", quota=1)
        self.start_run("r1")
        self.start_run("r2", tenant="globex")
        self.claim("r1", worker="w1")
        self.assertEqual(self.claim("r1", worker="w2"), (204, None))
        status, _ = self.claim("r2", worker="w1", tenant="globex")
        self.assertEqual(status, 200)


if __name__ == "__main__":
    unittest.main()
