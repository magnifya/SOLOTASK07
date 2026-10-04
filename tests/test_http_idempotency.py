"""End-to-end HTTP tests for tenant-scoped idempotent run creation."""

import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from flowd.http_app import create_server
from flowd.store import WorkflowStore, atomic_write_json

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
    {"id": "step3", "depends_on": ["step2"]},
]


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

    def start(self, tenant="acme", workflow_id="etl", run_id=None, params="OMIT",
              max_parallelism="OMIT", idempotency_key="OMIT"):
        body = {"tenant": tenant, "workflow_id": workflow_id}
        if run_id is not None:
            body["run_id"] = run_id
        if params != "OMIT":
            body["params"] = params
        if max_parallelism != "OMIT":
            body["max_parallelism"] = max_parallelism
        if idempotency_key != "OMIT":
            body["idempotency_key"] = idempotency_key
        return self.call("POST", "/v1/runs", body)

    def claim(self, run_id, worker="w1"):
        return self.call("POST", "/v1/runs/%s/claim" % run_id,
                         {"tenant": "acme", "worker_id": worker, "lease_seconds": 60})

    def complete(self, run_id, step_id, worker="w1"):
        return self.call("POST", "/v1/runs/%s/complete" % run_id,
                         {"tenant": "acme", "step_id": step_id, "worker_id": worker})

    # -- happy path ----------------------------------------------------
    def test_first_and_repeat_both_return_201_with_same_run(self):
        self.submit()
        status, first = self.start(params={"a": 1}, idempotency_key="k1")
        self.assertEqual(status, 201)
        self.assertEqual(first["idempotency_key"], "k1")
        status, again = self.start(params={"a": 1}, idempotency_key="k1")
        self.assertEqual(status, 201)
        self.assertEqual(again["run_id"], first["run_id"])
        self.assertEqual(again["idempotency_key"], "k1")
        self.assertEqual(len(self.call("GET", "/v1/runs?tenant=acme")[1]["items"]), 1)

    def test_repeat_reports_latest_state_in_terminal_runs(self):
        self.submit()
        status, run = self.start(idempotency_key="k")
        self.assertEqual(status, 201)
        for sid in ("step1", "step2", "step3"):
            self.assertEqual(self.claim(run["run_id"])[0], 200)
            self.assertEqual(self.complete(run["run_id"], sid)[0], 200)
        status, hit = self.start(idempotency_key="k")
        self.assertEqual(status, 201)
        self.assertEqual(hit["status"], "succeeded")
        self.assertEqual(hit["run_id"], run["run_id"])

    def test_key_is_trimmed(self):
        self.submit()
        status, first = self.start(idempotency_key="  k1 \t")
        self.assertEqual(status, 201)
        self.assertEqual(first["idempotency_key"], "k1")
        status, again = self.start(idempotency_key="k1")
        self.assertEqual(status, 201)
        self.assertEqual(again["run_id"], first["run_id"])

    def test_repeat_omitted_run_id_reuses_explicit_one(self):
        self.submit()
        status, first = self.start(run_id="r1", params={"a": 1}, idempotency_key="k1")
        self.assertEqual((status, first["run_id"]), (201, "r1"))
        status, again = self.start(params={"a": 1}, idempotency_key="k1")
        self.assertEqual(status, 201)
        self.assertEqual(again["run_id"], "r1")

    def test_repeat_does_not_change_history_or_updated_at(self):
        self.submit()
        status, first = self.start(params={"a": 1}, idempotency_key="k1")
        self.claim(first["run_id"])
        status, before = self.call("GET", "/v1/runs/%s?tenant=acme" % first["run_id"])
        self.assertEqual(status, 200)
        status, hit = self.start(params={"a": 1}, idempotency_key="k1")
        self.assertEqual(status, 201)
        self.assertEqual(hit["updated_at"], before["updated_at"])
        self.assertEqual(hit["history_length"], before["history_length"])

    # -- validation: 400 ----------------------------------------------
    def test_bad_key_returns_400_and_creates_nothing(self):
        self.submit()
        for bad in ("", "   ", 123, True, ["k"], {"k": 1}):
            status, body = self.start(params={}, idempotency_key=bad)
            self.assertEqual(status, 400, repr(bad))
            self.assertIn("error", body)
        self.assertEqual(self.call("GET", "/v1/runs?tenant=acme")[1]["items"], [])

    def test_bad_params_returns_400(self):
        self.submit()
        for bad in ([1], "x", 7, True, False):
            status, body = self.start(params=bad, idempotency_key="k-%r" % (bad,))
            self.assertEqual(status, 400, repr(bad))
            self.assertIn("error", body)
        self.assertEqual(self.call("GET", "/v1/runs?tenant=acme")[1]["items"], [])

    def test_bad_quota_returns_400_even_with_key(self):
        self.submit()
        status, body = self.start(params={}, max_parallelism=0, idempotency_key="k")
        self.assertEqual(status, 400)
        self.assertIn("error", body)
        self.assertEqual(self.call("GET", "/v1/runs?tenant=acme")[1]["items"], [])

    def test_params_semantic_equality_rules(self):
        self.submit()
        status, _ = self.start(params={"o": {"a": 1}, "l": [1, 2]}, idempotency_key="k")
        self.assertEqual(status, 201)
        # reordered object keys are the same; numeric int/float compare equal
        status, hit = self.start(params={"l": [1, 2.0], "o": {"a": 1}},
                                 idempotency_key="k")
        self.assertEqual(status, 201, hit)
        # array order differs
        self.assertEqual(self.start(params={"o": {"a": 1}, "l": [2, 1]},
                                   idempotency_key="k")[0], 409)
        # booleans are not numbers
        self.assertEqual(self.start(params={"o": {"a": True}, "l": [1, 2]},
                                   idempotency_key="k")[0], 409)

    # -- conflicts: 409 -----------------------------------------------
    def test_different_content_returns_409_and_leaves_run_untouched(self):
        self.submit()
        status, first = self.start(params={"a": 1}, max_parallelism=2, idempotency_key="k")
        self.assertEqual(status, 201)
        status, body = self.start(params={"a": 2}, max_parallelism=2, idempotency_key="k")
        self.assertEqual(status, 409)
        self.assertIn("error", body)
        status, body = self.start(params={"a": 1}, max_parallelism=3, idempotency_key="k")
        self.assertEqual(status, 409)
        # null/omitted quota (unlimited) differs from quota 2
        self.assertEqual(self.start(params={"a": 1}, idempotency_key="k")[0], 409)
        # different explicit run_id also conflicts
        self.assertEqual(self.start(run_id="r9", params={"a": 1}, max_parallelism=2,
                                   idempotency_key="k")[0], 409)
        # identical content still returns the original run
        status, hit = self.start(params={"a": 1}, max_parallelism=2, idempotency_key="k")
        self.assertEqual(status, 201)
        self.assertEqual(hit["run_id"], first["run_id"])
        items = self.call("GET", "/v1/runs?tenant=acme")[1]["items"]
        self.assertEqual(len(items), 1)

    # -- unknown workflow: 404 ----------------------------------------
    def test_new_key_unknown_workflow_returns_404(self):
        status, body = self.start(workflow_id="ghost", params={}, idempotency_key="k")
        self.assertEqual(status, 404)
        self.assertIn("error", body)
        # the rejected key must not become bound: submitting the workflow
        # later with the same key genuinely creates the run
        self.submit()
        status, first = self.start(params={}, idempotency_key="k")
        self.assertEqual(status, 201)
        self.assertEqual(first["workflow_id"], "etl")

    # -- tenant isolation ---------------------------------------------
    def test_same_key_is_isolated_per_tenant(self):
        self.submit("acme")
        self.submit("globex")
        status, acme = self.start(tenant="acme", params={"t": 1}, idempotency_key="shared")
        self.assertEqual(status, 201)
        status, globex = self.start(tenant="globex", params={"t": 2}, idempotency_key="shared")
        self.assertEqual(status, 201)
        self.assertNotEqual(acme["run_id"], globex["run_id"])
        status, hit = self.start(tenant="acme", params={"t": 1}, idempotency_key="shared")
        self.assertEqual((status, hit["run_id"]), (201, acme["run_id"]))
        # same content as acme but globex already bound "shared" differently
        self.assertEqual(self.start(tenant="globex", params={"t": 1},
                                   idempotency_key="shared")[0], 409)

    # -- reporting -----------------------------------------------------
    def test_detail_and_list_report_key_and_null_for_old_runs(self):
        self.submit()
        status, run = self.start(run_id="r1", idempotency_key="k1")
        self.assertEqual(status, 201)
        status, detail = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(detail["idempotency_key"], "k1")
        status, listing = self.call("GET", "/v1/runs?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(listing["items"][0]["idempotency_key"], "k1")

        # simulate an old run document written before keys existed
        path = os.path.join(self.root, "acme", "runs", "r1.json")
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        del doc["idempotency_key"]
        atomic_write_json(path, doc)
        status, detail = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(status, 200)
        self.assertIsNone(detail["idempotency_key"])
        self.assertIsNone(self.call("GET", "/v1/runs?tenant=acme")[1]["items"][0]
                          ["idempotency_key"])

    def test_unkeyed_create_keeps_legacy_shape(self):
        self.submit()
        status, body = self.start(params={"a": 1})
        self.assertEqual(status, 201)
        self.assertIsNone(body["idempotency_key"])

    # -- concurrency ---------------------------------------------------
    def test_concurrent_identical_requests_create_one_run(self):
        self.submit()
        outcomes = []
        barrier = threading.Barrier(8)

        def fire():
            barrier.wait()
            outcomes.append(self.start(params={"a": 1}, idempotency_key="hot"))

        threads = [threading.Thread(target=fire) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        statuses = [code for code, _ in outcomes]
        run_ids = {body["run_id"] for _, body in outcomes}
        self.assertEqual(statuses, [201] * 8)
        self.assertEqual(len(run_ids), 1)


if __name__ == "__main__":
    unittest.main()
