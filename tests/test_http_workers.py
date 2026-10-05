"""End-to-end HTTP tests for worker registration, health and claim gating."""

import json
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from flowd.http_app import create_server
from flowd.store import WorkflowStore

LINEAR = [{"id": "step1", "depends_on": []}, {"id": "step2", "depends_on": ["step1"]}]


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

    def register(self, worker="w1", tenant="acme", lease="omit"):
        body = {"tenant": tenant, "worker_id": worker}
        if lease != "omit":
            body["lease_seconds"] = lease
        return self.call("POST", "/v1/workers/register", body)

    def heartbeat(self, worker="w1", tenant="acme", lease="omit"):
        body = {"tenant": tenant}
        if lease != "omit":
            body["lease_seconds"] = lease
        return self.call("POST", "/v1/workers/%s/heartbeat" % worker, body)

    def test_register_201_then_200_and_fields(self):
        status, body = self.register(lease=30)
        self.assertEqual(status, 201, body)
        self.assertEqual(set(body), {"worker_id", "status", "registered_at",
                                     "last_heartbeat", "expires_at"})
        self.assertEqual(body["worker_id"], "w1")
        self.assertEqual(body["status"], "active")
        registered_at = body["registered_at"]

        status, again = self.register()  # default lease 30, repeat -> 200
        self.assertEqual(status, 200, again)
        self.assertEqual(again["status"], "active")
        self.assertEqual(again["registered_at"], registered_at)
        self.assertGreaterEqual(again["last_heartbeat"], registered_at)
        self.assertGreaterEqual(again["expires_at"], body["expires_at"])

    def test_register_trims_worker_id(self):
        status, body = self.register(worker="  w1 \t")
        self.assertEqual(status, 201, body)
        self.assertEqual(body["worker_id"], "w1")
        status, listing = self.call("GET", "/v1/workers?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual([w["worker_id"] for w in listing["items"]], ["w1"])

    def test_register_bad_requests_write_nothing(self):
        for body in ({}, {"tenant": "acme"}, {"worker_id": "w1"},
                     {"tenant": "  ", "worker_id": "w1"},
                     {"tenant": "acme", "worker_id": ""},
                     {"tenant": "acme", "worker_id": "w1", "lease_seconds": 0},
                     {"tenant": "acme", "worker_id": "w1", "lease_seconds": -1},
                     {"tenant": "acme", "worker_id": "w1", "lease_seconds": True},
                     {"tenant": "acme", "worker_id": "w1", "lease_seconds": "30"},
                     {"tenant": "acme", "worker_id": "w1", "lease_seconds": None}):
            status, reply = self.call("POST", "/v1/workers/register", body)
            self.assertEqual(status, 400, body)
            self.assertIn("error", reply)
        status, body = self.call("POST", "/v1/workers/register", raw_body=b"{bad")
        self.assertEqual(status, 400)
        self.assertEqual(self.call("GET", "/v1/workers?tenant=acme")[1]["items"], [])

    def test_worker_heartbeat_endpoint(self):
        self.register(lease=30)
        status, body = self.heartbeat(lease=1)  # would shorten -> later deadline kept
        self.assertEqual(status, 200, body)
        self.assertEqual(body["status"], "active")
        short = body["expires_at"]
        status, body = self.heartbeat(lease=3600)
        self.assertEqual(status, 200)
        self.assertGreater(body["expires_at"], short)

        self.assertEqual(self.heartbeat(worker="ghost")[0], 404)
        bad_tenant, reply = self.heartbeat(tenant="  ")
        self.assertEqual(bad_tenant, 400)
        self.assertIn("error", reply)

    def test_heartbeat_expired_worker_is_409(self):
        self.register(lease=1)
        import time
        time.sleep(1.1)
        status, body = self.call("GET", "/v1/workers?tenant=acme")
        self.assertEqual(body["items"][0]["status"], "expired")
        status, reply = self.heartbeat()
        self.assertEqual(status, 409)
        self.assertIn("error", reply)

    def test_list_requires_tenant_and_sorts(self):
        self.assertEqual(self.call("GET", "/v1/workers")[0], 400)
        self.assertEqual(self.call("GET", "/v1/workers?tenant=")[0], 400)
        self.register(worker="b")
        self.register(worker="a")
        self.register(worker="x", tenant="globex")
        status, body = self.call("GET", "/v1/workers?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(body["tenant"], "acme")
        self.assertEqual([w["worker_id"] for w in body["items"]], ["a", "b"])

    def test_claim_gating_over_http(self):
        self.call("POST", "/v1/workflows",
                  {"tenant": "acme", "workflow_id": "etl", "steps": LINEAR})
        self.assertEqual(self.call("POST", "/v1/runs",
                                   {"tenant": "acme", "workflow_id": "etl",
                                    "run_id": "r1"})[0], 201)

        def claim(worker):
            return self.call("POST", "/v1/runs/r1/claim",
                             {"tenant": "acme", "worker_id": worker, "lease_seconds": 1})

        # Before any registration the legacy rule works.
        self.assertEqual(claim("anyone")[0], 200)
        # Completion also keeps its legacy behavior (no registration check).
        self.assertEqual(self.call("POST", "/v1/runs/r1/complete",
                                   {"tenant": "acme", "step_id": "step1",
                                    "worker_id": "anyone"})[0], 200)

        self.register(worker="known", lease=1)
        status, reply = claim("stranger")
        self.assertEqual(status, 409)
        self.assertIn("stranger", reply["error"])
        status, step = claim("known")
        self.assertEqual(status, 200)
        self.assertEqual(step["step"]["id"], "step2")

        import time
        time.sleep(1.1)
        status, reply = claim("known")
        self.assertEqual(status, 409)
        self.assertIn("error", reply)

        # Refresh via re-register, then claim succeeds.
        self.assertEqual(self.register(worker="known", lease=30)[0], 200)
        self.assertEqual(claim("known")[0], 200)

    def test_claim_cross_tenant_is_403_even_when_registered_elsewhere(self):
        self.call("POST", "/v1/workflows",
                  {"tenant": "acme", "workflow_id": "etl", "steps": LINEAR})
        self.call("POST", "/v1/runs",
                  {"tenant": "acme", "workflow_id": "etl", "run_id": "r1"})
        self.register(worker="g1", tenant="globex")
        status, reply = self.call("POST", "/v1/runs/r1/claim",
                                  {"tenant": "globex", "worker_id": "g1"})
        self.assertEqual(status, 403)
        self.assertIn("error", reply)


if __name__ == "__main__":
    unittest.main()
