"""End-to-end tests for GET /v1/runs/{id}/history and /replay."""

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
from flowd.store import WorkflowStore

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
]
RETRYING = [
    {"id": "step1", "depends_on": [], "max_attempts": 2, "retry_backoff_seconds": 600},
]
APPROVAL = [{"id": "gate", "depends_on": [], "kind": "approval"}]


class HttpHistoryReplayTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-hist-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, body=None, port=None):
        """Return (status, parsed json or None); never raises on HTTP errors."""
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (port or self.port, path), data=data, method=method)
        if data is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                payload = response.read()
                return response.status, (json.loads(payload) if payload else None)
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            return exc.code, (json.loads(payload) if payload else None)

    # -- helpers ---------------------------------------------------------
    def submit(self, steps=LINEAR, workflow_id="etl", tenant="acme"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": workflow_id,
                                  "steps": steps})
        self.assertEqual(status, 201, body)
        return body

    def start(self, run_id="r1", tenant="acme", workflow_id="etl", **extra):
        body = {"tenant": tenant, "workflow_id": workflow_id, "run_id": run_id}
        body.update(extra)
        status, body = self.call("POST", "/v1/runs", body)
        self.assertEqual(status, 201, body)
        return body

    def seed(self, tenant="acme", run_id="r1"):
        """A running run: run_created, ready, claim, run_started (4 events)."""
        self.submit(tenant=tenant)
        self.start(run_id=run_id, tenant=tenant)
        status, body = self.call("POST", "/v1/runs/%s/claim" % run_id,
                                 {"tenant": tenant, "worker_id": "w1",
                                  "lease_seconds": 60})
        self.assertEqual(status, 200, body)
        return body

    def history(self, run_id="r1", query="", tenant="acme"):
        separator = "&" if query else ""
        return self.call("GET", "/v1/runs/%s/history?tenant=%s%s%s"
                         % (run_id, tenant, separator, query))

    def replay(self, run_id="r1", tenant="acme", port=None):
        return self.call("GET", "/v1/runs/%s/replay?tenant=%s" % (run_id, tenant),
                         port=port)

    def detail(self, run_id="r1", tenant="acme"):
        status, body = self.call("GET", "/v1/runs/%s?tenant=%s" % (run_id, tenant))
        self.assertEqual(status, 200, body)
        return body

    def audit(self, tenant="acme"):
        status, body = self.call("GET", "/v1/audit?tenant=%s" % tenant)
        self.assertEqual(status, 200, body)
        return body

    def run_file_bytes(self, tenant="acme", run_id="r1"):
        with open(os.path.join(self.root, tenant, "runs", "%s.json" % run_id), "rb") as fh:
            return fh.read()

    def assert_replay_matches_detail(self, run_id="r1", tenant="acme"):
        expected = self.detail(run_id, tenant)
        status, body = self.replay(run_id, tenant)
        self.assertEqual(status, 200, body)
        self.assertEqual(body, expected)
        return body

    # -- history ---------------------------------------------------------
    def test_history_over_http(self):
        self.seed()
        status, body = self.history()
        self.assertEqual(status, 200, body)
        self.assertEqual(body["tenant"], "acme")
        self.assertEqual(body["run_id"], "r1")
        self.assertIsNone(body["next_after"])
        self.assertEqual([e["type"] for e in body["items"]],
                         ["run_created", "ready", "claim", "run_started"])
        first = body["items"][0]
        self.assertEqual(first["run_id"], "r1")
        self.assertIsNone(first["step_id"])
        self.assertTrue(first["at"].endswith("Z"))
        claim = body["items"][2]
        self.assertEqual(claim["step_id"], "step1")
        self.assertEqual(claim["worker_id"], "w1")
        self.assertEqual(claim["attempt"], 0)
        self.assertIn("lease_deadline", claim)

    def test_history_default_limit_is_100(self):
        self.seed()
        status, body = self.history()
        self.assertEqual(status, 200)
        self.assertEqual(len(body["items"]), 4)

    def test_history_pagination(self):
        self.seed()
        status, body = self.history(query="limit=2")
        self.assertEqual(status, 200)
        self.assertEqual([e["type"] for e in body["items"]], ["run_created", "ready"])
        self.assertEqual(body["next_after"], 2)
        status, body = self.history(query="limit=2&after=2")
        self.assertEqual([e["type"] for e in body["items"]], ["claim", "run_started"])
        self.assertIsNone(body["next_after"])
        status, body = self.history(query="after=4")
        self.assertEqual((body["items"], body["next_after"]), ([], None))
        # A page that ends exactly at the last event has no successor.
        status, body = self.history(query="limit=3&after=1")
        self.assertEqual(len(body["items"]), 3)
        self.assertIsNone(body["next_after"])

    def test_history_bad_tenant(self):
        self.seed()
        for path in ("/v1/runs/r1/history", "/v1/runs/r1/history?tenant=",
                     "/v1/runs/r1/history?tenant=%20%20"):
            status, body = self.call("GET", path)
            self.assertEqual(status, 400, path)
            self.assertIn("error", body)

    def test_history_bad_limit(self):
        self.seed()
        for value in ("0", "-1", "1001", "abc", "1.5", ""):
            status, body = self.history(query="limit=%s" % value)
            self.assertEqual(status, 400, value)
            self.assertIn("error", body)
        for value in ("1", "100", "1000"):
            status, _ = self.history(query="limit=%s" % value)
            self.assertEqual(status, 200, value)

    def test_history_bad_after(self):
        self.seed()
        for value in ("-1", "abc", "1.5", ""):
            status, body = self.history(query="after=%s" % value)
            self.assertEqual(status, 400, value)
            self.assertIn("error", body)
        status, _ = self.history(query="after=0")
        self.assertEqual(status, 200)

    def test_history_unknown_run(self):
        status, body = self.history(run_id="ghost")
        self.assertEqual(status, 404)
        self.assertIn("error", body)

    def test_history_cross_tenant(self):
        self.seed()
        status, body = self.history(tenant="globex")
        self.assertEqual(status, 403)
        self.assertIn("error", body)
        # The other tenant's own runs are unaffected.
        self.seed(tenant="globex", run_id="g1")
        status, body = self.history(run_id="g1", tenant="globex")
        self.assertEqual(status, 200)
        self.assertEqual(body["tenant"], "globex")
        self.assertEqual(len(body["items"]), 4)

    def test_history_is_read_only(self):
        self.seed()
        detail_before = self.detail()
        audit_before = self.audit()
        file_before = self.run_file_bytes()
        self.history()
        self.history(query="limit=1&after=1")
        self.history(query="limit=0")          # rejected
        self.history(tenant="globex")          # rejected
        self.history(run_id="ghost")           # rejected
        self.assertEqual(self.detail(), detail_before)
        self.assertEqual(self.audit(), audit_before)
        self.assertEqual(self.run_file_bytes(), file_before)

    # -- replay ----------------------------------------------------------
    def test_replay_matches_run_detail(self):
        self.seed()
        self.assert_replay_matches_detail()

    def test_replay_covers_pause_resume_cancel(self):
        self.seed()
        for action in ("pause", "resume"):
            status, _ = self.call("POST", "/v1/runs/r1/%s" % action,
                                  {"tenant": "acme", "actor": "bob"})
            self.assertEqual(status, 200)
            self.assert_replay_matches_detail()
        status, _ = self.call("POST", "/v1/runs/r1/pause",
                              {"tenant": "acme", "actor": "bob"})
        self.assertEqual(status, 200)
        body = self.assert_replay_matches_detail()
        self.assertEqual(body["status"], "paused")
        status, _ = self.call("POST", "/v1/runs/r1/cancel",
                              {"tenant": "acme", "actor": "bob"})
        self.assertEqual(status, 200)
        body = self.assert_replay_matches_detail()
        self.assertEqual(body["status"], "cancelled")

    def test_replay_covers_retry_wait(self):
        self.submit(steps=RETRYING)
        self.start()
        self.call("POST", "/v1/runs/r1/claim",
                  {"tenant": "acme", "worker_id": "w1", "lease_seconds": 60})
        status, run = self.call("POST", "/v1/runs/r1/fail",
                                {"tenant": "acme", "step_id": "step1",
                                 "worker_id": "w1", "error": "boom"})
        self.assertEqual(status, 200, run)
        body = self.assert_replay_matches_detail()
        step = body["steps"][0]
        self.assertEqual(step["status"], "ready")
        self.assertEqual(step["attempt"], 1)
        self.assertIsNotNone(step["next_attempt_at"])
        self.assertEqual(step["error"], "boom")

    def test_replay_covers_lease_takeover(self):
        self.submit()
        self.start()
        status, _ = self.call("POST", "/v1/runs/r1/claim",
                              {"tenant": "acme", "worker_id": "w1", "lease_seconds": 1})
        self.assertEqual(status, 200)
        status, _ = self.call("POST", "/v1/runs/r1/claim",
                              {"tenant": "acme", "worker_id": "w2", "lease_seconds": 1})
        self.assertEqual(status, 204)  # step1 is leased to w1 already
        time.sleep(1.2)
        status, body = self.call("POST", "/v1/runs/r1/claim",
                                 {"tenant": "acme", "worker_id": "w2", "lease_seconds": 60})
        self.assertEqual(status, 200, body)
        replayed = self.assert_replay_matches_detail()
        step = replayed["steps"][0]
        self.assertEqual(step["worker_id"], "w2")
        self.assertEqual(step["status"], "running")

    def test_replay_covers_approval(self):
        self.submit(steps=APPROVAL)
        self.start()
        status, _ = self.call("POST", "/v1/runs/r1/decision",
                              {"tenant": "acme", "step_id": "gate", "actor": "amy",
                               "decision": "approve"})
        self.assertEqual(status, 200)
        body = self.assert_replay_matches_detail()
        self.assertEqual(body["status"], "succeeded")
        self.assertEqual(body["steps"][0]["approval"],
                         {"actor": "amy", "decision": "approve",
                          "at": body["steps"][0]["approval"]["at"]})

    def test_replay_delayed_run_is_not_activated(self):
        self.submit()
        self.start(not_before=9999999999)
        detail_before = self.detail()
        self.assertEqual(detail_before["status"], "pending")
        self.assertEqual(detail_before["history_length"], 1)  # run_created only
        body = self.assert_replay_matches_detail()
        self.assertEqual(body["status"], "pending")
        self.assertTrue(all(s["status"] == "pending" for s in body["steps"]))
        # Replaying did not activate the run or touch its timestamps.
        self.assertEqual(self.detail(), detail_before)
        status, hist = self.history()
        self.assertEqual([e["type"] for e in hist["items"]], ["run_created"])

    def test_replay_uses_frozen_definition(self):
        self.seed()
        before = self.assert_replay_matches_detail()
        # Replacing the same-name workflow changes nothing for the old run.
        self.submit(steps=[{"id": "brand", "depends_on": []},
                           {"id": "new", "depends_on": ["brand"]},
                           {"id": "nodes", "depends_on": ["new"]}])
        self.assertEqual(self.assert_replay_matches_detail(), before)
        self.assertEqual([s["id"] for s in before["steps"]], ["step1", "step2"])
        # Deleting the workflow definition changes nothing either.
        os.remove(os.path.join(self.root, "acme", "workflows.json"))
        self.assertEqual(self.assert_replay_matches_detail(), before)

    def test_replay_is_repeatable_and_survives_restart(self):
        self.seed()
        status, first = self.replay()
        self.assertEqual(status, 200)
        status, second = self.replay()
        self.assertEqual(first, second)
        # A fresh server over the same store (a "restart") replays identically.
        restarted = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.addCleanup(restarted.server_close)
        self.addCleanup(restarted.shutdown)
        threading.Thread(target=restarted.serve_forever, daemon=True).start()
        port = restarted.server_address[1]
        status, third = self.replay(port=port)
        self.assertEqual(status, 200)
        self.assertEqual(third, first)

    def test_replay_unknown_run(self):
        status, body = self.replay(run_id="ghost")
        self.assertEqual(status, 404)
        self.assertIn("error", body)

    def test_replay_cross_tenant(self):
        self.seed()
        status, body = self.replay(tenant="globex")
        self.assertEqual(status, 403)
        self.assertIn("error", body)

    def test_replay_bad_tenant(self):
        self.seed()
        for path in ("/v1/runs/r1/replay", "/v1/runs/r1/replay?tenant=",
                     "/v1/runs/r1/replay?tenant=%20%20"):
            status, body = self.call("GET", path)
            self.assertEqual(status, 400, path)
            self.assertIn("error", body)

    def test_replay_is_read_only(self):
        self.seed()
        detail_before = self.detail()
        audit_before = self.audit()
        file_before = self.run_file_bytes()
        self.replay()
        self.replay()
        self.replay(tenant="globex")     # rejected
        self.replay(run_id="ghost")      # rejected
        self.assertEqual(self.detail(), detail_before)
        self.assertEqual(self.audit(), audit_before)
        self.assertEqual(self.run_file_bytes(), file_before)

    def test_replay_completed_run(self):
        self.seed()
        status, _ = self.call("POST", "/v1/runs/r1/complete",
                              {"tenant": "acme", "step_id": "step1", "worker_id": "w1",
                               "result": {"rows": 7}})
        self.assertEqual(status, 200)
        self.call("POST", "/v1/runs/r1/claim",
                  {"tenant": "acme", "worker_id": "w1", "lease_seconds": 60})
        status, run = self.call("POST", "/v1/runs/r1/complete",
                                {"tenant": "acme", "step_id": "step2", "worker_id": "w1",
                                 "result": "done"})
        self.assertEqual(status, 200, run)
        body = self.assert_replay_matches_detail()
        self.assertEqual(body["status"], "succeeded")
        self.assertEqual(body["steps"][0]["result"], {"rows": 7})
        self.assertEqual(body["steps"][1]["result"], "done")


if __name__ == "__main__":
    unittest.main()
