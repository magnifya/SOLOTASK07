"""HTTP API tests for tenant-level concurrency quotas."""

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

    def get_quota(self, tenant="acme"):
        return self.call("GET", "/v1/quotas?tenant=%s" % tenant)

    def audit(self, tenant="acme"):
        status, body = self.call("GET", "/v1/audit?tenant=%s&limit=1000" % tenant)
        self.assertEqual(status, 200)
        return body["items"]

    def submit(self, tenant="acme", workflow_id="fan"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": workflow_id,
                                  "steps": FAN})
        self.assertEqual(status, 201, body)

    def start_run(self, run_id, tenant="acme"):
        status, body = self.call("POST", "/v1/runs",
                                 {"tenant": tenant, "workflow_id": "fan",
                                  "run_id": run_id})
        self.assertEqual(status, 201, body)

    def claim_any(self, worker, tenant="acme", lease=60):
        return self.call("POST", "/v1/tasks/claim",
                         {"tenant": tenant, "worker_id": worker,
                          "lease_seconds": lease})

    def claim_run(self, run_id, worker, tenant="acme", lease=60):
        return self.call("POST", "/v1/runs/%s/claim" % run_id,
                         {"tenant": tenant, "worker_id": worker,
                          "lease_seconds": lease})

    # -- configuration endpoint -----------------------------------------
    def test_create_then_update_returns_201_then_200(self):
        status, body = self.set_quota(quota=3)
        self.assertEqual(status, 201, body)
        self.assertEqual(body["tenant"], "acme")
        self.assertEqual(body["max_parallelism"], 3)
        self.assertIsNotNone(body["updated_at"])
        status, body = self.set_quota(quota=5)
        self.assertEqual(status, 200, body)
        self.assertEqual(body["max_parallelism"], 5)
        status, body = self.get_quota()
        self.assertEqual(status, 200)
        self.assertEqual(body["max_parallelism"], 5)

    def test_null_and_omitted_quota_mean_unlimited(self):
        status, body = self.call("POST", "/v1/quotas", {"tenant": "acme"})
        self.assertEqual(status, 201, body)
        self.assertIsNone(body["max_parallelism"])
        status, body = self.call("POST", "/v1/quotas",
                                 {"tenant": "acme", "max_parallelism": None})
        self.assertEqual(status, 200, body)
        self.assertIsNone(body["max_parallelism"])

    def test_same_value_update_keeps_updated_at_and_audits_once(self):
        _, first = self.set_quota(quota=2)
        _, again = self.set_quota(quota=2)
        self.assertEqual(again["updated_at"], first["updated_at"])
        self.assertEqual([r["action"] for r in self.audit()], ["quota.set"])
        _, changed = self.set_quota(quota=3)
        self.assertNotEqual(changed["updated_at"], first["updated_at"])
        self.assertEqual([r["action"] for r in self.audit()],
                         ["quota.set", "quota.set"])

    def test_unconfigured_tenant_reads_as_null(self):
        status, body = self.get_quota()
        self.assertEqual(status, 200)
        self.assertEqual(body["tenant"], "acme")
        self.assertIsNone(body["max_parallelism"])

    def test_tenant_is_trimmed_and_scoped(self):
        status, body = self.call("POST", "/v1/quotas",
                                 {"tenant": "  acme  ", "max_parallelism": 2})
        self.assertEqual(status, 201, body)
        self.assertEqual(body["tenant"], "acme")
        self.assertEqual(self.get_quota("acme")[1]["max_parallelism"], 2)
        # another tenant sees only its own (empty) record
        status, body = self.get_quota("globex")
        self.assertEqual(status, 200)
        self.assertIsNone(body["max_parallelism"])

    def test_bad_bodies_are_rejected(self):
        status, _ = self.call("POST", "/v1/quotas", raw=b"not json")
        self.assertEqual(status, 400)
        status, _ = self.call("POST", "/v1/quotas", raw=b"[1, 2]")
        self.assertEqual(status, 400)
        status, _ = self.call("POST", "/v1/quotas", raw=b'"acme"')
        self.assertEqual(status, 400)

    def test_bad_tenant_is_rejected(self):
        for payload in ({}, {"tenant": ""}, {"tenant": "   "},
                        {"tenant": 7}, {"tenant": True}, {"tenant": ["acme"]}):
            status, body = self.call("POST", "/v1/quotas",
                                     dict(payload, max_parallelism=1))
            self.assertEqual(status, 400, payload)
            self.assertIn("error", body)
        status, _ = self.call("GET", "/v1/quotas")
        self.assertEqual(status, 400)
        status, _ = self.call("GET", "/v1/quotas?tenant=")
        self.assertEqual(status, 400)
        status, _ = self.call("GET", "/v1/quotas?tenant=%20%20")
        self.assertEqual(status, 400)

    def test_bad_max_parallelism_is_rejected(self):
        for bad in (True, False, 2.5, 0, -1, "2", [], {}):
            status, body = self.call("POST", "/v1/quotas",
                                     {"tenant": "acme", "max_parallelism": bad})
            self.assertEqual(status, 400, bad)
            self.assertIn("error", body)
        self.assertIsNone(self.get_quota()[1]["max_parallelism"])

    def test_validation_failures_and_reads_add_no_audit(self):
        self.call("POST", "/v1/quotas", {"tenant": "acme", "max_parallelism": 0})
        self.call("POST", "/v1/quotas", {"tenant": "", "max_parallelism": 1})
        self.call("POST", "/v1/quotas", raw=b"nope")
        self.get_quota()
        self.assertEqual(self.audit(), [])

    def test_quota_survives_restart(self):
        self.assertEqual(self.set_quota(quota=2)[0], 201)
        self.server.server_close()
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        status, body = self.get_quota()
        self.assertEqual(status, 200)
        self.assertEqual(body["max_parallelism"], 2)

    # -- enforcement ------------------------------------------------------
    def test_budget_is_shared_between_runs_and_fair_claims(self):
        self.submit()
        self.start_run("r1")
        self.start_run("r2")
        self.assertEqual(self.set_quota(quota=1)[0], 201)
        status, body = self.claim_any("w1")
        self.assertEqual(status, 200)
        first_run = body["run_id"]
        # budget full: both claim styles report no work
        self.assertEqual(self.claim_any("w2"), (204, None))
        other = "r2" if first_run == "r1" else "r1"
        self.assertEqual(self.claim_run(other, "w2"), (204, None))
        # completing frees the shared slot
        status, _ = self.call("POST", "/v1/runs/%s/complete" % first_run,
                              {"tenant": "acme", "step_id": "a", "worker_id": "w1"})
        self.assertEqual(status, 200)
        status, body = self.claim_run(other, "w2")
        self.assertEqual(status, 200)
        self.assertEqual(body["step"]["id"], "a")

    def test_unconfigured_tenant_is_unlimited(self):
        self.submit()
        self.start_run("r1")
        self.start_run("r2")
        self.assertEqual(self.claim_run("r1", "w1")[0], 200)
        self.assertEqual(self.claim_run("r2", "w2")[0], 200)
        self.assertEqual(self.claim_any("w3")[0], 200)

    def test_quota_of_one_tenant_does_not_limit_another(self):
        self.submit()
        self.submit(tenant="globex")
        self.start_run("r1")
        self.start_run("g1", tenant="globex")
        self.set_quota(quota=1)
        self.assertEqual(self.claim_run("r1", "w1")[0], 200)
        self.assertEqual(self.claim_any("w2", tenant="globex")[0], 200)


if __name__ == "__main__":
    unittest.main()
