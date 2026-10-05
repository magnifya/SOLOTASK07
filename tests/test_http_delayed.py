"""End-to-end HTTP tests for delayed run start (``not_before``)."""

import json
import os
import shutil
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from flowd.http_app import create_server
from flowd.store import WorkflowStore, atomic_write_json

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
]
APPROVAL_ONLY = [{"id": "g", "depends_on": [], "kind": "approval"}]


class HttpDelayedStartTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-delay-")
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

    def submit(self, steps=LINEAR, workflow_id="wf"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": "acme", "workflow_id": workflow_id, "steps": steps})
        self.assertEqual(status, 201, body)

    def start(self, not_before="OMIT", run_id="r1", idempotency_key="OMIT"):
        body = {"tenant": "acme", "workflow_id": "wf", "run_id": run_id}
        if not_before != "OMIT":
            body["not_before"] = not_before
        if idempotency_key != "OMIT":
            body["idempotency_key"] = idempotency_key
        return self.call("POST", "/v1/runs", body)

    def step(self, body, step_id):
        return next(s for s in body["steps"] if s["id"] == step_id)

    # -- validation ----------------------------------------------------
    def test_bad_not_before_returns_400_and_creates_nothing(self):
        self.submit()
        for bad in (True, False, "1700000060", -1, -0.01, float("inf"), float("nan"),
                    [], {}, 10 ** 400):
            status, body = self.start(run_id="bad-%r" % (bad,), not_before=bad)
            self.assertEqual(status, 400, repr(bad))
            self.assertIn("error", body)
        self.assertEqual(self.call("GET", "/v1/runs?tenant=acme")[1]["items"], [])

    def test_omitted_and_null_mean_immediate_and_report_null(self):
        self.submit()
        status, body = self.start(run_id="r1")
        self.assertEqual(status, 201)
        self.assertIsNone(body["not_before"])
        self.assertEqual(self.step(body, "step1")["status"], "ready")
        status, body = self.start(run_id="r2", not_before=None)
        self.assertEqual(status, 201)
        self.assertIsNone(body["not_before"])

    def test_int_and_float_times_accepted(self):
        self.submit()
        when = time.time() + 60
        self.assertEqual(self.start(not_before=int(when))[1]["not_before"], int(when))
        self.assertEqual(self.start(run_id="r2", not_before=when + 0.5)[1]["not_before"],
                         when + 0.5)

    # -- delayed lifecycle ---------------------------------------------
    def test_future_run_is_pending_and_reports_not_before_everywhere(self):
        self.submit()
        when = time.time() + 3600
        status, body = self.start(not_before=when)
        self.assertEqual(status, 201)
        self.assertEqual(body["not_before"], when)
        self.assertEqual(body["status"], "pending")
        self.assertEqual([s["status"] for s in body["steps"]], ["pending", "pending"])
        status, detail = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(detail["not_before"], when)
        status, listing = self.call("GET", "/v1/runs?tenant=acme")
        self.assertEqual(listing["items"][0]["not_before"], when)

    def test_claim_before_due_is_204_and_changes_nothing(self):
        self.submit()
        status, _ = self.start(not_before=time.time() + 3600)
        self.assertEqual(status, 201)
        status, before = self.call("GET", "/v1/runs/r1?tenant=acme")
        for _ in range(2):
            self.assertEqual(self.call("POST", "/v1/runs/r1/claim",
                                       {"tenant": "acme", "worker_id": "w1"})[0], 204)
        status, after = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(after["steps"], before["steps"])
        self.assertEqual(after["updated_at"], before["updated_at"])
        self.assertEqual(after["history_length"], before["history_length"])
        # an early decision is rejected even with the run already created
        status, body = self.call("POST", "/v1/runs/r1/decision",
                                 {"tenant": "acme", "step_id": "step1", "actor": "a",
                                  "decision": "approve"})
        self.assertEqual(status, 409)

    def test_due_claim_activates_then_leases(self):
        self.submit()
        when = time.time() + 1
        self.assertEqual(self.start(not_before=when)[0], 201)
        # still sleeping
        self.assertEqual(self.call("POST", "/v1/runs/r1/claim",
                                   {"tenant": "acme", "worker_id": "w1"})[0], 204)
        time.sleep(1.2)  # now >= not_before
        status, body = self.call("POST", "/v1/runs/r1/claim",
                                 {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 200)
        self.assertEqual(body["step"]["id"], "step1")

    def test_approval_only_run_activates_with_no_task(self):
        self.submit(APPROVAL_ONLY)
        when = time.time() + 1
        self.assertEqual(self.start(not_before=when)[0], 201)
        time.sleep(1.2)
        # activation opens the approval, but no task is claimable -> 204
        self.assertEqual(self.call("POST", "/v1/runs/r1/claim",
                                   {"tenant": "acme", "worker_id": "w1"})[0], 204)
        status, detail = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(self.step(detail, "g")["status"], "waiting")
        # the decision is now accepted
        status, body = self.call("POST", "/v1/runs/r1/decision",
                                 {"tenant": "acme", "step_id": "g", "actor": "boss",
                                  "decision": "approve"})
        self.assertEqual(status, 200, body)

    # -- idempotency ---------------------------------------------------
    def test_idempotency_match_includes_not_before(self):
        self.submit()
        when = time.time() + 60
        self.assertEqual(self.start(not_before=when, idempotency_key="k")[0], 201)
        # same numeric time replays
        self.assertEqual(self.start(not_before=float(when), idempotency_key="k")[0], 201)
        # omitted/null is not an explicit time
        self.assertEqual(self.call("POST", "/v1/runs",
                                   {"tenant": "acme", "workflow_id": "wf", "run_id": "r1",
                                    "idempotency_key": "k"})[0], 409)
        # a different time conflicts and leaves the run alone
        self.assertEqual(self.start(not_before=when + 10, idempotency_key="k")[0], 409)
        status, detail = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(detail["not_before"], when)

    def test_implicit_time_first_then_explicit_conflicts(self):
        self.submit()
        self.assertEqual(self.start(idempotency_key="k")[0], 201)
        self.assertEqual(self.start(not_before=time.time() + 60, idempotency_key="k")[0], 409)

    # -- old documents -------------------------------------------------
    def test_old_run_without_field_reports_null(self):
        self.submit()
        self.assertEqual(self.start(run_id="r1")[0], 201)
        path = os.path.join(self.root, "acme", "runs", "r1.json")
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        del doc["not_before"]
        atomic_write_json(path, doc)
        status, detail = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(status, 200)
        self.assertIsNone(detail["not_before"])
        self.assertIsNone(self.call("GET", "/v1/runs?tenant=acme")[1]["items"][0]
                          ["not_before"])


if __name__ == "__main__":
    unittest.main()
