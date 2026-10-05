"""End-to-end HTTP tests for delayed run starts (``not_before``)."""

import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from flowd.http_app import create_server
from flowd.scheduler import Scheduler
from flowd.store import WorkflowStore, atomic_write_json

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
]


class FakeClock:
    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value


class HttpDelayedTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-delayed-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        store = WorkflowStore(self.root, clock=self.clock)
        scheduler = Scheduler(store, clock=self.clock)
        self.server = create_server(store, "127.0.0.1", 0, scheduler=scheduler)
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

    def submit(self, steps=None, workflow_id="wf"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": "acme", "workflow_id": workflow_id,
                                  "steps": LINEAR if steps is None else steps})
        self.assertEqual(status, 201, body)

    def start(self, not_before="OMIT", run_id="r1", workflow_id="wf", extra=None):
        body = {"tenant": "acme", "workflow_id": workflow_id, "run_id": run_id}
        if not_before != "OMIT":
            body["not_before"] = not_before
        if extra:
            body.update(extra)
        return self.call("POST", "/v1/runs", body)

    def claim(self, run_id="r1", worker="w1", lease=60):
        return self.call("POST", "/v1/runs/%s/claim" % run_id,
                         {"tenant": "acme", "worker_id": worker, "lease_seconds": lease})

    def decision(self, run_id, step_id, decision="approve", actor="boss"):
        return self.call("POST", "/v1/runs/%s/decision" % run_id,
                         {"tenant": "acme", "step_id": step_id, "actor": actor,
                          "decision": decision})

    def get(self, run_id="r1"):
        return self.call("GET", "/v1/runs/%s?tenant=acme" % run_id)

    # -- create / reporting --------------------------------------------
    def test_future_create_returns_pending_run_with_schedule(self):
        self.submit()
        status, body = self.start(not_before=self.clock.value + 60)
        self.assertEqual(status, 201, body)
        self.assertEqual(body["not_before"], self.clock.value + 60.0)
        self.assertEqual(body["status"], "pending")
        self.assertEqual([(s["status"], s["attempt"]) for s in body["steps"]],
                         [("pending", 0), ("pending", 0)])
        self.assertEqual(body["history_length"], 1)  # run_created only

    def test_omitted_null_and_past_time(self):
        self.submit()
        status, body = self.start(run_id="r1")
        self.assertEqual(status, 201)
        self.assertIsNone(body["not_before"])
        self.assertEqual(body["steps"][0]["status"], "ready")  # immediate
        status, body = self.start(not_before=None, run_id="r2")
        self.assertEqual(status, 201)
        self.assertIsNone(body["not_before"])
        status, body = self.start(not_before=self.clock.value - 5, run_id="r3")
        self.assertEqual(status, 201)
        self.assertEqual(body["not_before"], self.clock.value - 5.0)
        self.assertEqual(body["steps"][0]["status"], "ready")

    def test_bad_not_before_is_400_with_error_body_and_no_run(self):
        self.submit()
        for bad in (True, "1700000060", -1, float("inf"), float("nan"), [60], {"t": 1}):
            status, body = self.start(not_before=bad, run_id="r-bad")
            self.assertEqual(status, 400, repr(bad))
            self.assertIn("error", body)
        self.assertEqual(self.call("GET", "/v1/runs?tenant=acme")[1]["items"], [])

    def test_bad_time_rejected_before_unknown_workflow_lookup(self):
        status, body = self.start(not_before="soon", run_id="r1", workflow_id="ghost")
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_detail_list_and_old_document_report_field(self):
        self.submit()
        self.start(not_before=self.clock.value + 60)
        status, detail = self.get()
        self.assertEqual(status, 200)
        self.assertEqual(detail["not_before"], self.clock.value + 60.0)
        status, listing = self.call("GET", "/v1/runs?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(listing["items"][0]["not_before"], self.clock.value + 60.0)

        path = os.path.join(self.root, "acme", "runs", "r1.json")
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        del doc["not_before"]
        atomic_write_json(path, doc)
        self.assertIsNone(self.get()[1]["not_before"])
        self.assertIsNone(self.call("GET", "/v1/runs?tenant=acme")[1]["items"][0]
                          ["not_before"])

    # -- claim activation ----------------------------------------------
    def test_claim_204_before_due_changes_nothing(self):
        self.submit()
        status, created = self.start(not_before=self.clock.value + 60)
        self.assertEqual(status, 201)
        self.assertEqual(self.claim(), (204, None))
        status, detail = self.get()
        self.assertEqual(status, 200)
        self.assertEqual(detail["updated_at"], created["updated_at"])
        self.assertEqual(detail["history_length"], 1)
        self.assertEqual([s["status"] for s in detail["steps"]], ["pending", "pending"])

    def test_claim_at_due_activates_and_leases_root(self):
        self.submit()
        self.start(not_before=self.clock.value + 60)
        self.assertEqual(self.claim(), (204, None))
        self.clock.advance(60)  # now == not_before
        status, body = self.claim()
        self.assertEqual(status, 200)
        self.assertEqual(body["step"]["id"], "step1")
        detail = self.get()[1]
        self.assertEqual([s["status"] for s in detail["steps"]], ["running", "pending"])
        self.assertEqual(detail["history_length"], 4)  # created, ready, claim, started

    def test_approval_only_run_activates_with_no_task(self):
        self.submit([{"id": "g", "depends_on": [], "kind": "approval"}],
                    workflow_id="gate")
        self.start(workflow_id="gate", not_before=self.clock.value + 30)
        self.assertEqual(self.claim(), (204, None))
        self.clock.advance(30)
        self.assertEqual(self.claim(), (204, None))  # activated, nothing to lease
        detail = self.get()[1]
        self.assertEqual(detail["steps"][0]["status"], "waiting")
        self.assertEqual(detail["status"], "pending")
        self.assertEqual(detail["history_length"], 2)  # created, waiting
        # repeated claim adds nothing
        self.assertEqual(self.claim(), (204, None))
        self.assertEqual(self.get()[1]["history_length"], 2)
        # the opened approval is now decidable
        self.assertEqual(self.decision("r1", "g")[0], 200)

    def test_decision_before_activation_is_409(self):
        self.submit([{"id": "g", "depends_on": [], "kind": "approval"}],
                    workflow_id="gate")
        self.start(workflow_id="gate", not_before=self.clock.value + 60)
        status, body = self.decision("r1", "g")
        self.assertEqual(status, 409)
        self.assertIn("error", body)
        # due but never claimed: still not waiting
        self.clock.advance(60)
        self.assertEqual(self.decision("r1", "g")[0], 409)
        self.assertEqual(self.get()[1]["steps"][0]["status"], "pending")

    def test_activation_opens_roots_once_across_repeated_claims(self):
        self.submit([{"id": "a", "depends_on": []}, {"id": "b", "depends_on": []}],
                    workflow_id="fan")
        self.start(workflow_id="fan", not_before=self.clock.value + 10)
        self.clock.advance(10)
        self.assertEqual(self.claim(worker="w1")[1]["step"]["id"], "a")
        self.assertEqual(self.claim(worker="w2")[1]["step"]["id"], "b")
        self.assertEqual(self.claim(worker="w3"), (204, None))
        detail = self.get()[1]
        self.assertEqual([s["status"] for s in detail["steps"]], ["running", "running"])
        with open(os.path.join(self.root, "acme", "runs", "r1.json"),
                  "r", encoding="utf-8") as handle:
            history = json.load(handle)["history"]
        self.assertEqual([e["type"] for e in history].count("ready"), 2)

    # -- idempotency ----------------------------------------------------
    def test_idempotent_schedule_matching(self):
        self.submit()
        status, first = self.start(not_before=1_700_000_060, extra={"idempotency_key": "k"})
        self.assertEqual(status, 201)
        # int and float compare numerically
        status, hit = self.start(not_before=1_700_000_060.0, extra={"idempotency_key": "k"})
        self.assertEqual(status, 201)
        self.assertEqual(hit["run_id"], first["run_id"])
        # a different time conflicts
        self.assertEqual(self.start(not_before=1_700_000_120,
                                   extra={"idempotency_key": "k"})[0], 409)
        # explicit time never matches null
        self.assertEqual(self.start(not_before=None,
                                   extra={"idempotency_key": "k"})[0], 409)
        self.assertEqual(len(self.call("GET", "/v1/runs?tenant=acme")[1]["items"]), 1)

    def test_null_first_then_explicit_conflicts(self):
        self.submit()
        self.start(extra={"idempotency_key": "k"})
        self.assertEqual(self.start(not_before=0, extra={"idempotency_key": "k"})[0], 409)
        self.assertEqual(self.start(not_before=self.clock.value - 100,
                                   extra={"idempotency_key": "k"})[0], 409)

    def test_repeat_after_due_does_not_activate(self):
        self.submit()
        self.start(not_before=self.clock.value + 60, extra={"idempotency_key": "k"})
        self.clock.advance(120)
        status, hit = self.start(not_before=self.clock.value - 60,
                                 extra={"idempotency_key": "k"})
        self.assertEqual(status, 201)
        self.assertEqual([s["status"] for s in hit["steps"]], ["pending", "pending"])
        self.assertEqual(hit["history_length"], 1)

    def test_bad_time_on_repeat_is_400(self):
        self.submit()
        self.start(not_before=1_700_000_060, extra={"idempotency_key": "k"})
        status, body = self.start(not_before=False, extra={"idempotency_key": "k"})
        self.assertEqual(status, 400)
        self.assertIn("error", body)


if __name__ == "__main__":
    unittest.main()
