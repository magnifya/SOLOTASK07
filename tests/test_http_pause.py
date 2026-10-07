"""End-to-end HTTP tests for run pause/resume.

Covered: POST /v1/runs/{id}/pause and POST /v1/runs/{id}/resume.
"""

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
GATE = [
    {"id": "work", "depends_on": []},
    {"id": "ok", "depends_on": ["work"], "kind": "approval"},
]


class HttpPauseTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-pause-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, body="OMIT"):
        data = None
        if body != "OMIT":
            data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
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

    def submit(self, steps=LINEAR, workflow_id="wf", tenant="acme"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": workflow_id, "steps": steps})
        self.assertEqual(status, 201, body)

    def start(self, run_id="r1", tenant="acme", **extra):
        body = {"tenant": tenant, "workflow_id": "wf", "run_id": run_id}
        body.update(extra)
        status, payload = self.call("POST", "/v1/runs", body)
        self.assertEqual(status, 201, payload)
        return payload

    def pause(self, run_id="r1", tenant="acme", actor="ops"):
        return self.call("POST", "/v1/runs/%s/pause" % run_id,
                         {"tenant": tenant, "actor": actor})

    def resume(self, run_id="r1", tenant="acme", actor="ops"):
        return self.call("POST", "/v1/runs/%s/resume" % run_id,
                         {"tenant": tenant, "actor": actor})


class HttpPauseFlowTest(HttpPauseTest):
    def test_pause_and_resume_cycle(self):
        self.submit()
        self.start()
        status, claim = self.call("POST", "/v1/runs/r1/claim",
                                  {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 200, claim)
        status, run = self.pause()
        self.assertEqual(status, 200, run)
        self.assertEqual(run["status"], "paused")
        step1 = run["steps"][0]
        self.assertEqual(step1["status"], "ready")
        self.assertIsNone(step1["worker_id"])
        self.assertIsNone(step1["lease_deadline"])
        self.assertIsNone(step1["next_attempt_at"])
        # The same state is visible through status and list.
        status, fetched = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual((status, fetched), (200, run))
        status, listing = self.call("GET", "/v1/runs?tenant=acme&status=paused")
        self.assertEqual([item["run_id"] for item in listing["items"]], ["r1"])
        # Claims hand out nothing while paused.
        status, _ = self.call("POST", "/v1/runs/r1/claim",
                              {"tenant": "acme", "worker_id": "w2"})
        self.assertEqual(status, 204)
        status, _ = self.call("POST", "/v1/tasks/claim",
                              {"tenant": "acme", "worker_id": "w2"})
        self.assertEqual(status, 204)
        # Resume restores a schedulable state and claims work again.
        status, run = self.resume()
        self.assertEqual(status, 200, run)
        self.assertIn(run["status"], ("pending", "running"))
        status, claim = self.call("POST", "/v1/runs/r1/claim",
                                  {"tenant": "acme", "worker_id": "w2"})
        self.assertEqual(status, 200, claim)
        self.assertEqual(claim["step"]["id"], "step1")
        # The audit stream records the cycle in order.
        status, audit = self.call("GET", "/v1/audit?tenant=acme&run_id=r1")
        self.assertEqual(
            [item["action"] for item in audit["items"]],
            ["run_created", "ready", "claim", "run_started",
             "pause", "run_paused", "run_resumed", "claim", "run_started"])
        actors = [item["actor"] for item in audit["items"]
                  if item["action"] in ("pause", "run_paused", "run_resumed")]
        self.assertEqual(actors, ["ops", "ops", "ops"])

    def test_repeat_pause_and_resume_return_same_run(self):
        self.submit()
        self.start()
        status, first = self.pause()
        self.assertEqual(status, 200)
        status, second = self.pause(actor="someone-else")
        self.assertEqual((status, second), (200, first))
        status, first = self.resume()
        self.assertEqual(status, 200)
        status, second = self.resume(actor="someone-else")
        self.assertEqual((status, second), (200, first))

    def test_decision_conflicts_while_paused(self):
        self.submit(GATE)
        self.start()
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        self.call("POST", "/v1/runs/r1/complete",
                  {"tenant": "acme", "worker_id": "w1", "step_id": "work"})
        status, _ = self.pause()
        self.assertEqual(status, 200)
        status, body = self.call("POST", "/v1/runs/r1/decision",
                                 {"tenant": "acme", "step_id": "ok",
                                  "actor": "alice", "decision": "approve"})
        self.assertEqual(status, 409)
        self.assertIn("paused", body["error"])

    def test_cancel_still_works_while_paused(self):
        self.submit()
        self.start()
        self.pause()
        status, run = self.call("POST", "/v1/runs/r1/cancel",
                                {"tenant": "acme", "actor": "ops"})
        self.assertEqual(status, 200, run)
        self.assertEqual(run["status"], "cancelled")


class HttpPauseErrorTest(HttpPauseTest):
    def test_bad_json(self):
        self.submit()
        self.start()
        for action in ("pause", "resume"):
            status, _ = self.call("POST", "/v1/runs/r1/%s" % action, b"{not json")
            self.assertEqual(status, 400, action)
            status, _ = self.call("POST", "/v1/runs/r1/%s" % action, [1, 2])
            self.assertEqual(status, 400, action)

    def test_bad_tenant_and_actor(self):
        self.submit()
        self.start()
        for action in ("pause", "resume"):
            for body in ({}, {"tenant": "  ", "actor": "ops"},
                         {"tenant": 7, "actor": "ops"},
                         {"tenant": "acme"}, {"tenant": "acme", "actor": " "},
                         {"tenant": "acme", "actor": 3}):
                status, _ = self.call("POST", "/v1/runs/r1/%s" % action, body)
                self.assertEqual(status, 400, (action, body))

    def test_unknown_run(self):
        self.assertEqual(self.pause(run_id="nope")[0], 404)
        self.assertEqual(self.resume(run_id="nope")[0], 404)

    def test_cross_tenant(self):
        self.submit()
        self.start()
        self.assertEqual(self.pause(tenant="other")[0], 403)
        self.assertEqual(self.resume(tenant="other")[0], 403)

    def test_finished_run_conflicts(self):
        self.submit()
        self.start()
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        self.call("POST", "/v1/runs/r1/complete",
                  {"tenant": "acme", "worker_id": "w1", "step_id": "step1"})
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        self.call("POST", "/v1/runs/r1/complete",
                  {"tenant": "acme", "worker_id": "w1", "step_id": "step2"})
        self.assertEqual(self.pause()[0], 409)
        self.assertEqual(self.resume()[0], 409)

    def test_resume_not_paused_conflicts(self):
        self.submit()
        self.start()
        status, body = self.resume()
        self.assertEqual(status, 409)
        self.assertIn("not paused", body["error"])


if __name__ == "__main__":
    unittest.main()
