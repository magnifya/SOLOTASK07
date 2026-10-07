"""End-to-end HTTP tests for run pause/resume.

Covers ``POST /v1/runs/{id}/pause`` and ``POST /v1/runs/{id}/resume``:
validation (400 ``bad_json``/``bad_tenant``/``bad_actor``), state transitions,
idempotent repeats, error statuses (403/404/409) and the paused run's effect
on claims and approval decisions.
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
    def test_pause_and_resume_run(self):
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
        # The same state is visible through status and list.
        status, fetched = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(fetched, run)
        status, listing = self.call("GET", "/v1/runs?tenant=acme&status=paused")
        self.assertEqual(status, 200)
        self.assertEqual([item["run_id"] for item in listing["items"]], ["r1"])
        # The audit stream records the pause in order.
        status, audit = self.call("GET", "/v1/audit?tenant=acme&run_id=r1")
        self.assertEqual(status, 200)
        self.assertEqual([item["action"] for item in audit["items"]],
                         ["run_created", "ready", "claim", "run_started",
                          "pause", "run_paused"])
        self.assertEqual(audit["items"][-1]["actor"], "ops")
        # Resume flips the run back and the task is claimable again.
        status, run = self.resume()
        self.assertEqual(status, 200, run)
        self.assertEqual(run["status"], "pending")
        status, audit = self.call("GET", "/v1/audit?tenant=acme&run_id=r1")
        self.assertEqual(audit["items"][-1]["action"], "run_resumed")
        status, claim = self.call("POST", "/v1/runs/r1/claim",
                                  {"tenant": "acme", "worker_id": "w2"})
        self.assertEqual(status, 200, claim)
        self.assertEqual(claim["step"]["id"], "step1")

    def test_repeat_pause_returns_same_run(self):
        self.submit()
        self.start()
        status, first = self.pause()
        self.assertEqual(status, 200)
        status, second = self.pause(actor="someone-else")
        self.assertEqual(status, 200)
        self.assertEqual(second, first)

    def test_repeat_resume_returns_same_run(self):
        self.submit()
        self.start()
        self.pause()
        status, first = self.resume()
        self.assertEqual(status, 200)
        self.assertEqual(first["status"], "pending")
        status, second = self.resume(actor="someone-else")
        self.assertEqual(status, 200)
        self.assertEqual(second, first)

    def test_paused_run_blocks_claims_and_decisions(self):
        self.submit(GATE)
        self.start()
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        self.call("POST", "/v1/runs/r1/complete",
                  {"tenant": "acme", "worker_id": "w1", "step_id": "work"})
        status, run = self.pause()
        self.assertEqual(status, 200)
        self.assertEqual(run["steps"][1]["status"], "waiting")
        # Single-run claim and fair claim both hand out nothing.
        status, _ = self.call("POST", "/v1/runs/r1/claim",
                              {"tenant": "acme", "worker_id": "w2"})
        self.assertEqual(status, 204)
        status, _ = self.call("POST", "/v1/tasks/claim",
                              {"tenant": "acme", "worker_id": "w2"})
        self.assertEqual(status, 204)
        # The waiting approval cannot be decided while paused.
        status, body = self.call("POST", "/v1/runs/r1/decision",
                                 {"tenant": "acme", "step_id": "ok", "actor": "alice",
                                  "decision": "approve"})
        self.assertEqual(status, 409)
        # Cancel keeps its usual semantics on a paused run.
        status, run = self.call("POST", "/v1/runs/r1/cancel",
                                {"tenant": "acme", "actor": "ops"})
        self.assertEqual(status, 200)
        self.assertEqual(run["status"], "cancelled")

    def test_paused_run_rejects_step_actions(self):
        self.submit()
        self.start()
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        self.pause()
        for action, extra in (("complete", {}), ("fail", {}), ("heartbeat", {})):
            body = {"tenant": "acme", "worker_id": "w1", "step_id": "step1"}
            body.update(extra)
            status, _ = self.call("POST", "/v1/runs/r1/%s" % action, body)
            self.assertEqual(status, 409, action)


class HttpPauseValidationTest(HttpPauseTest):
    def test_bad_json(self):
        self.submit()
        self.start()
        for path in ("pause", "resume"):
            status, _ = self.call("POST", "/v1/runs/r1/%s" % path, b"{not json")
            self.assertEqual(status, 400, path)
            status, _ = self.call("POST", "/v1/runs/r1/%s" % path, ["not", "an", "object"])
            self.assertEqual(status, 400, path)

    def test_bad_tenant_and_actor(self):
        self.submit()
        self.start()
        for path in ("pause", "resume"):
            for body in ({}, {"tenant": "acme"}, {"actor": "ops"},
                         {"tenant": "  ", "actor": "ops"},
                         {"tenant": "acme", "actor": "  "},
                         {"tenant": 7, "actor": "ops"},
                         {"tenant": "acme", "actor": 7}):
                status, _ = self.call("POST", "/v1/runs/r1/%s" % path, body)
                self.assertEqual(status, 400, (path, body))

    def test_unknown_run(self):
        for path in ("pause", "resume"):
            status, _ = self.call("POST", "/v1/runs/nope/%s" % path,
                                  {"tenant": "acme", "actor": "ops"})
            self.assertEqual(status, 404, path)

    def test_cross_tenant(self):
        self.submit()
        self.start()
        for path in ("pause", "resume"):
            status, _ = self.call("POST", "/v1/runs/r1/%s" % path,
                                  {"tenant": "other", "actor": "ops"})
            self.assertEqual(status, 403, path)
        status, run = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(run["status"], "pending")

    def test_finished_run_conflicts(self):
        self.submit()
        self.start()
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        self.call("POST", "/v1/runs/r1/complete",
                  {"tenant": "acme", "worker_id": "w1", "step_id": "step1"})
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        self.call("POST", "/v1/runs/r1/complete",
                  {"tenant": "acme", "worker_id": "w1", "step_id": "step2"})
        status, _ = self.pause()
        self.assertEqual(status, 409)
        # Cancelled runs conflict too, and a never-paused run cannot resume.
        self.submit(workflow_id="wf2")
        self.start(run_id="r2", workflow_id="wf2")
        self.call("POST", "/v1/runs/r2/cancel", {"tenant": "acme", "actor": "ops"})
        status, _ = self.pause(run_id="r2")
        self.assertEqual(status, 409)
        status, _ = self.resume(run_id="r2")
        self.assertEqual(status, 409)

    def test_not_paused_conflicts(self):
        self.submit()
        self.start()
        status, _ = self.resume()
        self.assertEqual(status, 409)


if __name__ == "__main__":
    unittest.main()
