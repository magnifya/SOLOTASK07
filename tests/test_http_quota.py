"""HTTP API tests for per-run max_parallelism quotas."""

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


class HttpQuotaTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-quota-")
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

    def submit(self, workflow_id="fan", steps=None):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": "acme", "workflow_id": workflow_id,
                                  "steps": FAN if steps is None else steps})
        self.assertEqual(status, 201, body)

    def start_run(self, run_id="r1", extra=None):
        payload = {"tenant": "acme", "workflow_id": "fan", "run_id": run_id}
        if extra is not None:
            payload.update(extra)
        return self.call("POST", "/v1/runs", payload)

    def claim(self, run_id="r1", worker="w1", lease=60):
        return self.call("POST", "/v1/runs/%s/claim" % run_id,
                         {"tenant": "acme", "worker_id": worker, "lease_seconds": lease})

    def test_quota_echoed_on_create_detail_and_list(self):
        self.submit()
        status, body = self.start_run(extra={"max_parallelism": 2})
        self.assertEqual(status, 201, body)
        self.assertEqual(body["max_parallelism"], 2)
        status, detail = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(detail["max_parallelism"], 2)
        status, listing = self.call("GET", "/v1/runs?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(listing["items"][0]["max_parallelism"], 2)

    def test_omitted_or_null_quota_reported_as_null(self):
        self.submit()
        self.assertEqual(self.start_run(run_id="r1")[1]["max_parallelism"], None)
        self.assertEqual(self.start_run(run_id="r2", extra={"max_parallelism": None})[1]
                         ["max_parallelism"], None)

    def test_invalid_quota_returns_400_and_creates_no_run(self):
        self.submit()
        for bad in (True, 2.5, 0, -1, "2", [], {}):
            status, body = self.start_run(run_id="r-bad", extra={"max_parallelism": bad})
            self.assertEqual(status, 400, bad)
            self.assertIn("error", body)
        status, listing = self.call("GET", "/v1/runs?tenant=acme")
        self.assertEqual(listing["items"], [])

    def test_claim_returns_204_when_quota_is_full(self):
        self.submit()
        self.assertEqual(self.start_run(extra={"max_parallelism": 1})[0], 201)
        status, body = self.claim(worker="w1")
        self.assertEqual(status, 200)
        self.assertEqual(body["step"]["id"], "a")
        self.assertEqual(self.claim(worker="w2"), (204, None))

    def test_released_slot_can_be_claimed_again(self):
        self.submit()
        self.start_run(extra={"max_parallelism": 1})
        self.claim(worker="w1")
        self.assertEqual(self.claim(worker="w2"), (204, None))
        status, body = self.call("POST", "/v1/runs/r1/complete",
                                 {"tenant": "acme", "step_id": "a", "worker_id": "w1"})
        self.assertEqual(status, 200, body)
        status, body = self.claim(worker="w2")
        self.assertEqual(status, 200)
        self.assertEqual(body["step"]["id"], "b")


if __name__ == "__main__":
    unittest.main()
