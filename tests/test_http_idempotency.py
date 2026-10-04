"""HTTP API tests for tenant scoped idempotent run creation."""

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


class HttpIdempotencyTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-idem-")
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

    def submit(self, tenant="acme", workflow_id="etl", steps=None):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": workflow_id,
                                  "steps": LINEAR if steps is None else steps})
        self.assertEqual(status, 201, body)

    def start(self, tenant="acme", workflow_id="etl", **extra):
        payload = {"tenant": tenant, "workflow_id": workflow_id}
        payload.update(extra)
        return self.call("POST", "/v1/runs", payload)

    def test_first_and_repeat_both_201_with_same_run_id(self):
        self.submit()
        status1, first = self.start(idempotency_key="  order-42  ", params={"a": 1})
        self.assertEqual(status1, 201)
        self.assertEqual(first["idempotency_key"], "order-42")
        status2, second = self.start(idempotency_key="order-42", params={"a": 1})
        self.assertEqual(status2, 201)
        self.assertEqual(second["run_id"], first["run_id"])
        self.assertEqual(second["idempotency_key"], "order-42")
        status, listing = self.call("GET", "/v1/runs?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(len(listing["items"]), 1)
        self.assertEqual(listing["items"][0]["idempotency_key"], "order-42")
        status, detail = self.call("GET", "/v1/runs/%s?tenant=acme" % first["run_id"])
        self.assertEqual(detail["idempotency_key"], "order-42")

    def test_repeat_after_run_finished_returns_latest_state_201(self):
        self.submit()
        status, first = self.start(idempotency_key="k1")
        self.assertEqual(status, 201)
        for sid in ("step1", "step2"):
            self.assertEqual(self.call("POST", "/v1/runs/%s/claim" % first["run_id"],
                                       {"tenant": "acme", "worker_id": "w1"})[0], 200)
            self.assertEqual(self.call("POST", "/v1/runs/%s/complete" % first["run_id"],
                                       {"tenant": "acme", "step_id": sid,
                                        "worker_id": "w1"})[0], 200)
        status, repeat = self.start(idempotency_key="k1")
        self.assertEqual(status, 201)
        self.assertEqual(repeat["status"], "succeeded")
        self.assertEqual(repeat["run_id"], first["run_id"])

    def test_bad_key_params_quota_are_400(self):
        self.submit()
        for payload in (
            {"idempotency_key": ""},
            {"idempotency_key": "   "},
            {"idempotency_key": 9},
            {"idempotency_key": True},
            {"idempotency_key": "k1", "params": [1, 2]},
            {"idempotency_key": "k1", "params": "nope"},
            {"idempotency_key": "k1", "max_parallelism": -2},
            {"idempotency_key": "k1", "max_parallelism": 1.5},
        ):
            status, body = self.start(**payload)
            self.assertEqual(status, 400, payload)
            self.assertEqual(set(body), {"error"})
        status, listing = self.call("GET", "/v1/runs?tenant=acme")
        self.assertEqual(listing["items"], [])

    def test_content_conflict_is_409(self):
        self.submit()
        status, _ = self.start(idempotency_key="k1", params={"a": 1})
        self.assertEqual(status, 201)
        status, body = self.start(idempotency_key="k1", params={"a": 2})
        self.assertEqual(status, 409)
        self.assertEqual(set(body), {"error"})
        status, body = self.start(idempotency_key="k1", max_parallelism=4)
        self.assertEqual(status, 409)
        status, body = self.start(idempotency_key="k1", run_id="different")
        self.assertEqual(status, 409)

    def test_new_key_unknown_workflow_is_404(self):
        status, body = self.start(workflow_id="ghost", idempotency_key="k1", params={})
        self.assertEqual(status, 404)
        self.assertIn("error", body)

    def test_param_semantics_over_http(self):
        self.submit()
        status, first = self.start(idempotency_key="k1",
                                   params={"b": [1, 2, 3], "a": {"y": True, "x": 1}})
        self.assertEqual(status, 201)
        status, repeat = self.start(idempotency_key="k1",
                                    params={"a": {"x": 1, "y": True}, "b": [1, 2, 3]})
        self.assertEqual(status, 201)
        self.assertEqual(repeat["run_id"], first["run_id"])
        status, body = self.start(idempotency_key="k1",
                                  params={"a": {"x": 1, "y": True}, "b": [3, 2, 1]})
        self.assertEqual(status, 409, body)
        status, body = self.start(idempotency_key="k1",
                                  params={"a": {"x": 1, "y": 1}, "b": [1, 2, 3]})
        self.assertEqual(status, 409)

    def test_tenant_isolation_over_http(self):
        self.submit("acme")
        self.submit("globex")
        _, acme = self.start(tenant="acme", idempotency_key="dup")
        _, globex = self.start(tenant="globex", idempotency_key="dup")
        self.assertNotEqual(acme["run_id"], globex["run_id"])
        _, acme_again = self.start(tenant="acme", idempotency_key="dup")
        self.assertEqual(acme_again["run_id"], acme["run_id"])

    def test_unkeyed_create_reports_null_and_is_not_deduped(self):
        self.submit()
        status, first = self.start()
        self.assertEqual(status, 201)
        self.assertIsNone(first["idempotency_key"])
        status, second = self.start()
        self.assertEqual(status, 201)
        self.assertNotEqual(second["run_id"], first["run_id"])
        self.assertIsNone(second["idempotency_key"])

    def test_repeated_hit_does_not_change_updated_at_or_history(self):
        self.submit()
        _, first = self.start(idempotency_key="k1", params={"a": 1})
        history_len, updated = first["history_length"], first["updated_at"]
        _, second = self.start(idempotency_key="k1", params={"a": 1})
        self.assertEqual(second["history_length"], history_len)
        self.assertEqual(second["updated_at"], updated)


if __name__ == "__main__":
    unittest.main()
