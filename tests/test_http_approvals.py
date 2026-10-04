"""End-to-end HTTP tests for approval nodes and POST /v1/runs/{id}/decision."""

import json
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from flowd.http_app import create_server
from flowd.store import WorkflowStore


def approval(step_id, depends_on=None):
    return {"id": step_id, "depends_on": depends_on or [], "kind": "approval"}


class HttpApprovalTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-approval-")
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

    def submit(self, steps, workflow_id="wf"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": "acme", "workflow_id": workflow_id, "steps": steps})
        self.assertEqual(status, 201, body)
        return body

    def start(self, steps=None, workflow_id="wf", run_id="r1"):
        if steps is not None:
            self.submit(steps, workflow_id)
        status, body = self.call("POST", "/v1/runs",
                                 {"tenant": "acme", "workflow_id": workflow_id, "run_id": run_id})
        self.assertEqual(status, 201, body)
        return body

    def decision(self, run_id, step_id, actor="alice", decision="approve", tenant="acme",
                 raw=None):
        body = raw if raw is not None else {"tenant": tenant, "step_id": step_id,
                                            "actor": actor, "decision": decision}
        return self.call("POST", "/v1/runs/%s/decision" % run_id, body)

    def step(self, body, step_id):
        return next(s for s in body["steps"] if s["id"] == step_id)

    # -- definition ----------------------------------------------------
    def test_bad_kind_on_submit_is_400(self):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": "acme", "workflow_id": "bad",
                                  "steps": [{"id": "a", "kind": "gate"}]})
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_approval_view_shape_while_waiting(self):
        run = self.start([approval("a"), {"id": "b", "depends_on": ["a"]}])
        node = self.step(run, "a")
        self.assertEqual(node["kind"], "approval")
        self.assertEqual(node["status"], "waiting")
        self.assertIsNone(node["approval"])
        self.assertEqual(node["attempt"], 0)
        self.assertIsNone(node["result"])
        self.assertIsNone(node["error"])
        self.assertIsNone(node["worker_id"])
        self.assertIsNone(node["lease_deadline"])
        self.assertEqual(self.step(run, "b")["kind"], "task")
        self.assertEqual(run["status"], "pending")

    def test_approval_waits_until_dependencies_succeed(self):
        self.start([{"id": "t", "depends_on": []}, approval("a", ["t"]),
                    {"id": "b", "depends_on": ["a"]}])
        status, claimed = self.call("POST", "/v1/runs/r1/claim",
                                    {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 200)
        self.assertEqual(claimed["step"]["id"], "t")
        status, run = self.call("POST", "/v1/runs/r1/complete",
                                {"tenant": "acme", "step_id": "t", "worker_id": "w1"})
        self.assertEqual(status, 200)
        self.assertEqual(self.step(run, "a")["status"], "waiting")
        self.assertEqual(self.step(run, "b")["status"], "pending")
        # claim skips the approval; nothing else is ready
        self.assertEqual(self.call("POST", "/v1/runs/r1/claim",
                                   {"tenant": "acme", "worker_id": "w2"})[0], 204)

    def test_independent_tasks_still_claimed_while_approval_waits(self):
        self.start([approval("a"), {"id": "b", "depends_on": []}])
        status, claimed = self.call("POST", "/v1/runs/r1/claim",
                                    {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual((status, claimed["step"]["id"]), (200, "b"))

    # -- decisions -----------------------------------------------------
    def test_approve_returns_updated_run_and_unlocks_successors(self):
        self.start([approval("a"), {"id": "b", "depends_on": ["a"]}])
        status, run = self.decision("r1", "a")
        self.assertEqual(status, 200)
        node = self.step(run, "a")
        self.assertEqual(node["status"], "succeeded")
        self.assertEqual(set(node["approval"]), {"actor", "decision", "at"})
        self.assertEqual(node["approval"]["actor"], "alice")
        self.assertEqual(node["approval"]["decision"], "approve")
        self.assertTrue(node["approval"]["at"].endswith("Z"))
        self.assertEqual(self.step(run, "b")["status"], "ready")

    def test_reject_fails_node_and_run_successors_stay_pending(self):
        self.start([approval("a"), {"id": "b", "depends_on": ["a"]}])
        status, run = self.decision("r1", "a", decision="reject", actor="  bob  ")
        self.assertEqual(status, 200)
        self.assertEqual(run["status"], "failed")
        node = self.step(run, "a")
        self.assertEqual((node["status"], node["attempt"]), ("failed", 0))
        self.assertEqual(node["approval"]["actor"], "bob")
        self.assertEqual(node["approval"]["decision"], "reject")
        self.assertIsNone(node["error"])
        self.assertEqual(self.step(run, "b")["status"], "pending")

    def test_decision_idempotent_for_same_actor_and_decision(self):
        self.start([approval("a")])
        status, first = self.decision("r1", "a")
        self.assertEqual(status, 200)
        status, second = self.decision("r1", "a", actor=" alice ")
        self.assertEqual(status, 200)
        self.assertEqual(self.step(first, "a")["approval"], self.step(second, "a")["approval"])
        self.assertEqual(first["history_length"], second["history_length"])

    def test_changed_actor_or_reversed_decision_is_409(self):
        self.start([approval("a")])
        self.decision("r1", "a")
        status, body = self.decision("r1", "a", actor="bob")
        self.assertEqual(status, 409)
        self.assertIn("error", body)
        status, body = self.decision("r1", "a", decision="reject")
        self.assertEqual(status, 409)
        status, body = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(self.step(body, "a")["approval"]["actor"], "alice")

    def test_early_decision_and_decision_on_task_are_409(self):
        self.start([{"id": "t", "depends_on": []}, approval("a", ["t"])])
        status, body = self.decision("r1", "a")  # still pending
        self.assertEqual(status, 409)
        self.assertIn("error", body)
        status, body = self.decision("r1", "t")  # ordinary task
        self.assertEqual(status, 409)

    def test_complete_and_fail_against_approval_are_409(self):
        self.start([approval("a")])
        status, body = self.call("POST", "/v1/runs/r1/complete",
                                 {"tenant": "acme", "step_id": "a", "worker_id": "w1"})
        self.assertEqual(status, 409)
        status, body = self.call("POST", "/v1/runs/r1/fail",
                                 {"tenant": "acme", "step_id": "a", "worker_id": "w1",
                                  "error": "boom"})
        self.assertEqual(status, 409)

    # -- request validation -------------------------------------------
    def test_decision_request_validation_is_400(self):
        self.start([approval("a")])
        good = {"tenant": "acme", "step_id": "a", "actor": "alice", "decision": "approve"}
        for field in ("tenant", "step_id", "actor", "decision"):
            bad = dict(good)
            del bad[field]
            self.assertEqual(self.call("POST", "/v1/runs/r1/decision", bad)[0], 400, field)
        for field in ("tenant", "step_id", "actor"):
            bad = dict(good)
            bad[field] = "   "
            self.assertEqual(self.call("POST", "/v1/runs/r1/decision", bad)[0], 400, field)
            bad = dict(good)
            bad[field] = 7
            self.assertEqual(self.call("POST", "/v1/runs/r1/decision", bad)[0], 400, field)
        self.assertEqual(self.call("POST", "/v1/runs/r1/decision",
                                   raw_body=b"{not json")[0], 400)
        self.assertEqual(self.call("POST", "/v1/runs/r1/decision",
                                   raw_body=b"[1, 2]")[0], 400)
        status, body = self.decision("r1", "a", raw=dict(good, decision="maybe"))
        self.assertEqual(status, 400)
        # rejected requests must not have recorded anything
        status, body = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertIsNone(self.step(body, "a")["approval"])

    # -- tenancy / existence ------------------------------------------
    def test_unknown_run_and_step_are_404(self):
        self.start([approval("a")])
        status, body = self.decision("missing", "a")
        self.assertEqual(status, 404)
        self.assertIn("error", body)
        status, body = self.decision("r1", "ghost")
        self.assertEqual(status, 404)

    def test_cross_tenant_decision_is_403(self):
        self.start([approval("a")])
        status, body = self.decision("r1", "a", tenant="globex")
        self.assertEqual(status, 403)
        self.assertIn("error", body)
        status, body = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertIsNone(self.step(body, "a")["approval"])

    def test_approval_fields_appear_in_listing(self):
        self.start([approval("a")])
        self.decision("r1", "a", decision="reject")
        status, body = self.call("GET", "/v1/runs?tenant=acme")
        self.assertEqual(status, 200)
        node = self.step(body["items"][0], "a")
        self.assertEqual(node["kind"], "approval")
        self.assertEqual(node["approval"]["decision"], "reject")


if __name__ == "__main__":
    unittest.main()
