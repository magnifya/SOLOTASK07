"""End-to-end HTTP tests for worker registration, heartbeats and claim gating."""

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


class WorkerHttpTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-workers-")
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

    def register(self, tenant="acme", worker="w1", lease=30):
        body = {"tenant": tenant, "worker_id": worker}
        if lease != "omit":
            body["lease_seconds"] = lease
        return self.call("POST", "/v1/workers/register", body)

    def worker_heartbeat(self, worker="w1", tenant="acme", lease="omit"):
        body = {"tenant": tenant}
        if lease != "omit":
            body["lease_seconds"] = lease
        return self.call("POST", "/v1/workers/%s/heartbeat" % worker, body)

    def prepare_run(self, tenant="acme", workflow_id="etl", run_id="r1"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": workflow_id,
                                  "steps": LINEAR})
        self.assertEqual(status, 201, body)
        status, body = self.call("POST", "/v1/runs",
                                 {"tenant": tenant, "workflow_id": workflow_id,
                                  "run_id": run_id})
        self.assertEqual(status, 201, body)

    def claim(self, run_id="r1", tenant="acme", worker="w1", lease=60):
        return self.call("POST", "/v1/runs/%s/claim" % run_id,
                         {"tenant": tenant, "worker_id": worker, "lease_seconds": lease})

    # -- registration --------------------------------------------------
    def test_register_first_then_repeat(self):
        status, body = self.register()
        self.assertEqual(status, 201, body)
        self.assertEqual(set(body),
                         {"worker_id", "status", "registered_at",
                          "last_heartbeat", "expires_at"})
        self.assertEqual(body["worker_id"], "w1")
        self.assertEqual(body["status"], "active")
        registered_at = body["registered_at"]
        status, again = self.register(lease=90)
        self.assertEqual(status, 200, again)
        self.assertEqual(again["registered_at"], registered_at)
        self.assertGreaterEqual(again["expires_at"], body["expires_at"])

    def test_register_trims_tenant_and_worker_id(self):
        status, body = self.register(tenant="  acme  ", worker=" w1 \n")
        self.assertEqual(status, 201, body)
        self.assertEqual(body["worker_id"], "w1")
        status, listing = self.call("GET", "/v1/workers?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual([w["worker_id"] for w in listing["items"]], ["w1"])

    def test_register_default_lease_is_thirty_seconds(self):
        status, body = self.register(lease="omit")
        self.assertEqual(status, 201)
        self.assertEqual(body["status"], "active")

    def test_register_rejects_bad_inputs_with_400_and_writes_nothing(self):
        for payload in (
            {"worker_id": "w1"},
            {"tenant": "  ", "worker_id": "w1"},
            {"tenant": 7, "worker_id": "w1"},
            {"tenant": "acme"},
            {"tenant": "acme", "worker_id": ""},
            {"tenant": "acme", "worker_id": 5},
            {"tenant": "acme", "worker_id": "w1", "lease_seconds": 0},
            {"tenant": "acme", "worker_id": "w1", "lease_seconds": -3},
            {"tenant": "acme", "worker_id": "w1", "lease_seconds": "30"},
            {"tenant": "acme", "worker_id": "w1", "lease_seconds": True},
            {"tenant": "acme", "worker_id": "w1", "lease_seconds": None},
        ):
            status, body = self.call("POST", "/v1/workers/register", payload)
            self.assertEqual(status, 400, payload)
            self.assertIn("error", body)
        self.assertEqual(self.call("GET", "/v1/workers?tenant=acme")[1]["items"], [])

    def test_register_bad_json_is_400(self):
        self.assertEqual(self.call("POST", "/v1/workers/register", raw_body=b"nope")[0], 400)

    # -- listing -------------------------------------------------------
    def test_list_sorted_with_live_status_and_tenant_scope(self):
        self.register(worker="w3", lease=10)
        self.register(worker="w1", lease=1000)
        self.register(worker="w2", lease=1000)
        self.register(tenant="globex", worker="g1", lease=1000)
        status, body = self.call("GET", "/v1/workers?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual([w["worker_id"] for w in body["items"]], ["w1", "w2", "w3"])
        statuses = {w["worker_id"]: w["status"] for w in body["items"]}
        self.assertEqual(statuses["w1"], "active")
        self.assertEqual(statuses["w3"], "active")
        self.assertEqual(
            self.call("GET", "/v1/workers?tenant=globex")[1]["items"][0]["worker_id"], "g1")

    def test_list_requires_tenant(self):
        for path in ("/v1/workers", "/v1/workers?tenant="):
            status, body = self.call("GET", path)
            self.assertEqual(status, 400, path)
            self.assertIn("error", body)

    # -- worker heartbeat ----------------------------------------------
    def test_worker_heartbeat_extends_later_deadline(self):
        status, first = self.register(lease=100)
        self.assertEqual(status, 201)
        status, renewed = self.worker_heartbeat(lease=1)
        self.assertEqual(status, 200)
        # now+1 is earlier than the old deadline -> expiry unchanged
        self.assertEqual(renewed["expires_at"], first["expires_at"])
        status, longer = self.worker_heartbeat(lease=500)
        self.assertEqual(status, 200)
        self.assertGreater(longer["expires_at"], first["expires_at"])
        self.assertEqual(set(longer),
                         {"worker_id", "status", "registered_at",
                          "last_heartbeat", "expires_at"})

    def test_worker_heartbeat_errors(self):
        status, body = self.worker_heartbeat("ghost")
        self.assertEqual(status, 404)
        self.assertIn("error", body)
        self.register(lease=0.05)
        # Wait long enough for the registration to expire.
        self.assertTrue(self._wait_expired("w1"))
        status, body = self.worker_heartbeat("w1")
        self.assertEqual(status, 409)
        self.assertIn("worker_expired", json.dumps(body))
        status, body = self.worker_heartbeat("w1", lease=0)
        self.assertEqual(status, 400)
        status, body = self.worker_heartbeat("w1", tenant="  ")
        self.assertEqual(status, 400)

    def _wait_expired(self, worker, attempts=50):
        import time
        for _ in range(attempts):
            _, body = self.call("GET", "/v1/workers?tenant=acme")
            record = next(w for w in body["items"] if w["worker_id"] == worker)
            if record["status"] == "expired":
                return True
            time.sleep(0.02)
        return False

    # -- claim gate ----------------------------------------------------
    def test_legacy_claim_works_until_a_worker_registers(self):
        self.prepare_run()
        status, _ = self.claim(worker="anyone")
        self.assertEqual(status, 200)

    def test_claim_after_registration(self):
        self.prepare_run()
        self.assertEqual(self.register()[0], 201)
        status, body = self.claim(worker="stranger")
        self.assertEqual(status, 409)
        self.assertIn("worker_not_registered", json.dumps(body))
        status, body = self.claim(worker="w1")
        self.assertEqual(status, 200)
        self.assertEqual(body["step"]["id"], "step1")
        # Nothing ready afterwards still returns 204, registry notwithstanding.
        self.assertEqual(self.claim(worker="w1"), (204, None))

    def test_claim_with_expired_registration(self):
        self.prepare_run()
        self.register(lease=0.05)
        self.assertTrue(self._wait_expired("w1"))
        status, body = self.claim(worker="w1")
        self.assertEqual(status, 409)
        self.assertIn("worker_expired", json.dumps(body))
        # Re-registration restores the ability to claim.
        self.assertEqual(self.register(lease=30)[0], 200)
        status, body = self.claim(worker="w1")
        self.assertEqual(status, 200)
        self.assertEqual(body["step"]["id"], "step1")

    def test_registry_is_per_tenant(self):
        self.prepare_run("acme", run_id="r1")
        self.prepare_run("globex", workflow_id="etl2", run_id="g1")
        self.register(tenant="acme", worker="w1")
        # globex has no registry yet: legacy rule.
        self.assertEqual(self.claim(run_id="g1", tenant="globex", worker="anyone")[0], 200)
        self.register(tenant="globex", worker="g1")
        status, _ = self.claim(run_id="g1", tenant="globex", worker="w1")
        self.assertEqual(status, 409)
        self.assertEqual(self.claim(run_id="g1", tenant="globex", worker="g1")[0], 204)

    def test_cross_tenant_claim_is_403_even_when_registered_elsewhere(self):
        self.prepare_run("acme", run_id="r1")
        self.register(tenant="globex", worker="w1")
        status, body = self.claim(run_id="r1", tenant="globex", worker="w1")
        self.assertEqual(status, 403)
        self.assertIn("error", body)

    def test_registry_survives_server_restart(self):
        self.assertEqual(self.register(lease=1000)[0], 201)
        state = {w["worker_id"]: w for w in
                 self.call("GET", "/v1/workers?tenant=acme")[1]["items"]}
        self.server.shutdown()
        self.server.server_close()
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        status, body = self.call("GET", "/v1/workers?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(body["items"][0]["registered_at"], state["w1"]["registered_at"])
        self.assertEqual(body["items"][0]["status"], "active")
        # A repeat registration after restart is a refresh (200), not a create.
        self.assertEqual(self.register(lease=1000)[0], 200)

    def test_registry_does_not_touch_run_history_or_204(self):
        self.prepare_run()
        self.register()
        self.worker_heartbeat(lease=120)
        status, run = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(run["history_length"], 2)  # created + ready only
        self.assertEqual(self.claim(worker="w1")[0], 200)
        # existing lease heartbeat still works with the registry present
        status, body = self.call("POST", "/v1/runs/r1/heartbeat",
                                 {"tenant": "acme", "step_id": "step1",
                                  "worker_id": "w1", "lease_seconds": 500})
        self.assertEqual(status, 200)


if __name__ == "__main__":
    unittest.main()
