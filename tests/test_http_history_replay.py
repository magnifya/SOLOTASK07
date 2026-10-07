"""End-to-end tests for GET /v1/runs/{run_id}/history and .../replay."""

import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from flowd.http_app import create_server
from flowd.scheduler import Scheduler, WorkflowError
from flowd.store import WorkflowStore

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
]

APPROVAL = [
    {"id": "gate", "depends_on": [], "kind": "approval"},
    {"id": "after", "depends_on": ["gate"]},
]


class HttpRunHistoryReplayTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-history-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, body=None):
        """Return (status, parsed json or None); never raises on HTTP errors."""
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

    def get(self, path):
        return self.call("GET", path)

    def history(self, run_id, query="", tenant="acme"):
        separator = "&" if query else ""
        return self.get("/v1/runs/%s/history?tenant=%s%s%s"
                        % (run_id, tenant, separator, query))

    def replay(self, run_id, tenant="acme"):
        return self.get("/v1/runs/%s/replay?tenant=%s" % (run_id, tenant))

    def detail(self, run_id, tenant="acme"):
        return self.get("/v1/runs/%s?tenant=%s" % (run_id, tenant))

    def seed_linear(self, tenant="acme", run_id="r1"):
        self.call("POST", "/v1/workflows",
                  {"tenant": tenant, "workflow_id": "etl", "steps": LINEAR})
        self.call("POST", "/v1/runs",
                  {"tenant": tenant, "workflow_id": "etl", "run_id": run_id})
        self.call("POST", "/v1/runs/%s/claim" % run_id,
                  {"tenant": tenant, "worker_id": "w1", "lease_seconds": 60})
        self.call("POST", "/v1/runs/%s/complete" % run_id,
                  {"tenant": tenant, "step_id": "step1", "worker_id": "w1",
                   "result": {"ok": True}})
        self.call("POST", "/v1/runs/%s/claim" % run_id,
                  {"tenant": tenant, "worker_id": "w2", "lease_seconds": 60})

    def audit_actions(self, tenant="acme"):
        return [r["action"] for r in self.get("/v1/audit?tenant=%s" % tenant)[1]["items"]]

    # -- history --------------------------------------------------------
    def test_history_returns_events_in_append_order_with_fields(self):
        self.seed_linear()
        status, body = self.history("r1")
        self.assertEqual(status, 200)
        self.assertEqual(body["tenant"], "acme")
        self.assertEqual(body["run_id"], "r1")
        self.assertIsNone(body["next_after"])
        types = [e["type"] for e in body["items"]]
        self.assertEqual(types,
                         ["run_created", "ready", "claim", "run_started",
                          "complete", "ready", "claim"])
        claim = body["items"][2]
        self.assertEqual(claim["step_id"], "step1")
        self.assertEqual(claim["worker_id"], "w1")
        self.assertEqual(claim["attempt"], 0)
        self.assertIn("lease_deadline", claim)
        self.assertTrue(claim["at"].endswith("Z"))
        complete = body["items"][4]
        self.assertEqual(complete["result"], {"ok": True})
        self.assertEqual(complete["worker_id"], "w1")
        self.assertEqual(complete["attempt"], 1)
        # The run-level event carries no step id, exactly as stored.
        self.assertIsNone(body["items"][0]["step_id"])

    def test_history_default_limit_is_100(self):
        self.seed_linear()
        status, body = self.history("r1")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["items"]), 7)

    def test_history_pagination_limit_and_after(self):
        self.seed_linear()
        status, page1 = self.history("r1", "limit=2")
        self.assertEqual(status, 200)
        self.assertEqual([e["type"] for e in page1["items"]], ["run_created", "ready"])
        self.assertEqual(page1["next_after"], 2)
        status, page2 = self.history("r1", "limit=2&after=2")
        self.assertEqual([e["type"] for e in page2["items"]], ["claim", "run_started"])
        self.assertEqual(page2["next_after"], 4)
        status, page3 = self.history("r1", "limit=2&after=4")
        self.assertEqual([e["type"] for e in page3["items"]], ["complete", "ready"])
        self.assertEqual(page3["next_after"], 6)
        status, page4 = self.history("r1", "limit=2&after=6")
        self.assertEqual([e["type"] for e in page4["items"]], ["claim"])
        self.assertIsNone(page4["next_after"])
        status, drained = self.history("r1", "after=7")
        self.assertEqual((drained["items"], drained["next_after"]), ([], None))

    def test_history_bad_tenant(self):
        self.seed_linear()
        for path in ("/v1/runs/r1/history",
                     "/v1/runs/r1/history?tenant=",
                     "/v1/runs/r1/history?tenant=%20%20"):
            status, body = self.get(path)
            self.assertEqual(status, 400, path)
            self.assertIn("error", body)

    def test_history_bad_limit(self):
        self.seed_linear()
        for value in ("0", "-1", "1001", "abc", "1.5", ""):
            status, body = self.history("r1", "limit=%s" % value)
            self.assertEqual(status, 400, value)
            self.assertIn("error", body)
        for value in ("1", "100", "1000"):
            status, _ = self.history("r1", "limit=%s" % value)
            self.assertEqual(status, 200, value)

    def test_history_bad_after(self):
        self.seed_linear()
        for value in ("-1", "abc", "1.5", ""):
            status, body = self.history("r1", "after=%s" % value)
            self.assertEqual(status, 400, value)
            self.assertIn("error", body)
        status, _ = self.history("r1", "after=0")
        self.assertEqual(status, 200)

    def test_history_unknown_run_is_404(self):
        self.seed_linear()
        status, body = self.history("ghost")
        self.assertEqual(status, 404)
        self.assertIn("error", body)

    def test_history_cross_tenant_is_403(self):
        self.seed_linear(tenant="acme", run_id="r1")
        self.seed_linear(tenant="globex", run_id="g1")
        status, body = self.history("r1", tenant="globex")
        self.assertEqual(status, 403)
        self.assertIn("error", body)
        status, body = self.history("g1", tenant="acme")
        self.assertEqual(status, 403)

    def test_history_is_read_only(self):
        self.seed_linear()
        _, before = self.detail("r1")
        audit_before = self.audit_actions()
        for query in ("", "limit=2", "limit=2&after=2", "after=99", "limit=1000"):
            status, _ = self.history("r1", query)
            self.assertEqual(status, 200)
        _, after = self.detail("r1")
        self.assertEqual(after, before)
        self.assertEqual(self.audit_actions(), audit_before)
        # Cross-tenant and unknown-run reads are read-only too.
        self.history("r1", tenant="globex")
        self.history("ghost")
        _, after2 = self.detail("r1")
        self.assertEqual(after2, before)
        self.assertEqual(self.audit_actions(), audit_before)

    # -- replay ---------------------------------------------------------
    def test_replay_projection_matches_run_detail(self):
        self.seed_linear()
        status, replayed = self.replay("r1")
        self.assertEqual(status, 200)
        _, detail = self.detail("r1")
        self.assertEqual(replayed, detail)
        step1 = replayed["steps"][0]
        self.assertEqual(step1["status"], "succeeded")
        self.assertEqual(step1["attempt"], 1)
        self.assertEqual(step1["result"], {"ok": True})
        self.assertEqual(step1["worker_id"], "w1")
        self.assertEqual(replayed["steps"][1]["status"], "running")
        self.assertEqual(replayed["steps"][1]["worker_id"], "w2")

    def test_replay_is_repeatable_and_restart_stable(self):
        self.seed_linear()
        _, first = self.replay("r1")
        _, second = self.replay("r1")
        self.assertEqual(first, second)
        restarted = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        port = restarted.server_address[1]
        thread = threading.Thread(target=restarted.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(restarted.server_close)
        self.addCleanup(restarted.shutdown)
        with urllib.request.urlopen(
                "http://127.0.0.1:%d/v1/runs/r1/replay?tenant=acme" % port,
                timeout=10) as response:
            third = json.loads(response.read())
        self.assertEqual(third, first)

    def test_replay_unknown_run_is_404_and_cross_tenant_is_403(self):
        self.seed_linear()
        status, body = self.replay("ghost")
        self.assertEqual(status, 404)
        self.assertIn("error", body)
        status, body = self.replay("r1", tenant="globex")
        self.assertEqual(status, 403)
        self.assertIn("error", body)

    def test_replay_bad_tenant(self):
        self.seed_linear()
        for path in ("/v1/runs/r1/replay",
                     "/v1/runs/r1/replay?tenant=",
                     "/v1/runs/r1/replay?tenant=%20"):
            status, body = self.get(path)
            self.assertEqual(status, 400, path)
            self.assertIn("error", body)

    def test_replay_ignores_limit_and_after_query_params(self):
        self.seed_linear()
        status, body = self.get("/v1/runs/r1/replay?tenant=acme&limit=1&after=2")
        self.assertEqual(status, 200)
        _, detail = self.detail("r1")
        self.assertEqual(body, detail)

    def test_replay_uses_frozen_definition_when_workflow_replaced(self):
        self.seed_linear()
        # Replace the same-name workflow with a different DAG.
        self.call("POST", "/v1/workflows",
                  {"tenant": "acme", "workflow_id": "etl",
                   "steps": [{"id": "brand_new", "depends_on": []}]})
        status, replayed = self.replay("r1")
        self.assertEqual(status, 200)
        self.assertEqual([s["id"] for s in replayed["steps"]], ["step1", "step2"])
        self.assertEqual([s["status"] for s in replayed["steps"]],
                         ["succeeded", "running"])
        # Even deleting the registered workflow entirely keeps replay working.
        os.remove(os.path.join(self.root, "acme", "workflows.json"))
        status, replayed = self.replay("r1")
        self.assertEqual(status, 200)
        self.assertEqual([s["id"] for s in replayed["steps"]], ["step1", "step2"])

    def test_replay_covers_approval_pause_cancel_and_delayed_start(self):
        # Approval decision is rebuilt with the recorded actor and decision.
        self.call("POST", "/v1/workflows",
                  {"tenant": "acme", "workflow_id": "appr", "steps": APPROVAL})
        self.call("POST", "/v1/runs",
                  {"tenant": "acme", "workflow_id": "appr", "run_id": "a1"})
        status, _ = self.call("POST", "/v1/runs/a1/claim",
                              {"tenant": "acme", "worker_id": "w1", "lease_seconds": 60})
        self.assertEqual(status, 204)
        self.call("POST", "/v1/runs/a1/decision",
                  {"tenant": "acme", "step_id": "gate", "actor": "boss",
                   "decision": "approve"})
        _, replayed = self.replay("a1")
        _, detail = self.detail("a1")
        self.assertEqual(replayed, detail)
        gate = replayed["steps"][0]
        self.assertEqual(gate["status"], "succeeded")
        self.assertEqual(gate["approval"]["actor"], "boss")
        self.assertEqual(gate["approval"]["decision"], "approve")
        self.assertTrue(gate["approval"]["at"].endswith("Z"))

        # Paused run: revoked task goes back to ready, run stays paused.
        self.call("POST", "/v1/workflows",
                  {"tenant": "acme", "workflow_id": "etl", "steps": LINEAR})
        self.call("POST", "/v1/runs",
                  {"tenant": "acme", "workflow_id": "etl", "run_id": "p1"})
        self.call("POST", "/v1/runs/p1/claim",
                  {"tenant": "acme", "worker_id": "w1", "lease_seconds": 60})
        self.call("POST", "/v1/runs/p1/pause",
                  {"tenant": "acme", "actor": "ops"})
        _, replayed = self.replay("p1")
        self.assertEqual(replayed["status"], "paused")
        self.assertEqual(replayed["steps"][0]["status"], "ready")
        self.assertIsNone(replayed["steps"][0]["worker_id"])
        self.assertEqual(replayed, self.detail("p1")[1])

        # Cancelled run.
        self.call("POST", "/v1/runs",
                  {"tenant": "acme", "workflow_id": "etl", "run_id": "c1"})
        self.call("POST", "/v1/runs/c1/cancel", {"tenant": "acme", "actor": "ops"})
        _, replayed = self.replay("c1")
        self.assertEqual(replayed["status"], "cancelled")
        self.assertTrue(all(s["status"] == "cancelled" for s in replayed["steps"]))
        self.assertEqual(replayed, self.detail("c1")[1])

        # Delayed start: still sleeping runs replay as pending with only the
        # creation event and are not activated by the read.
        self.call("POST", "/v1/runs",
                  {"tenant": "acme", "workflow_id": "etl", "run_id": "d1",
                   "not_before": 9999999999})
        _, before = self.detail("d1")
        _, replayed = self.replay("d1")
        self.assertEqual(replayed, before)
        self.assertEqual(replayed["status"], "pending")
        self.assertEqual(replayed["history_length"], 1)
        self.assertEqual(replayed["not_before"], 9999999999)
        # A replay must not activate the run: a claim is still 204 and the
        # stored document is unchanged afterwards.
        status, _ = self.call("POST", "/v1/runs/d1/claim",
                              {"tenant": "acme", "worker_id": "w1", "lease_seconds": 60})
        self.assertEqual(status, 204)
        self.assertEqual(self.detail("d1")[1], before)
        _, replayed2 = self.replay("d1")
        self.assertEqual(replayed2, before)

    def test_replay_is_read_only(self):
        self.seed_linear()
        _, before = self.detail("r1")
        audit_before = self.audit_actions()
        for _ in range(3):
            status, _ = self.replay("r1")
            self.assertEqual(status, 200)
        self.assertEqual(self.detail("r1")[1], before)
        self.assertEqual(self.audit_actions(), audit_before)
        self.replay("ghost")
        self.replay("r1", tenant="globex")
        self.assertEqual(self.detail("r1")[1], before)
        self.assertEqual(self.audit_actions(), audit_before)


class SchedulerHistoryPageTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-history-page-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.scheduler = Scheduler(WorkflowStore(self.root))
        self.scheduler.submit("acme", "etl", LINEAR)
        self.run = self.scheduler.start_run("acme", "etl", run_id="r1")

    def test_page_validation(self):
        for kwargs in ({"limit": 0}, {"limit": 1001}, {"limit": 1.5},
                       {"limit": "10"}, {"limit": True}):
            with self.assertRaises(WorkflowError) as caught:
                self.scheduler.history_page("acme", "r1", **kwargs)
            self.assertEqual(caught.exception.code, "bad_limit")
        for kwargs in ({"after": -1}, {"after": 1.5}, {"after": "1"},
                       {"after": True}):
            with self.assertRaises(WorkflowError) as caught:
                self.scheduler.history_page("acme", "r1", **kwargs)
            self.assertEqual(caught.exception.code, "bad_after")
        for tenant in (None, "", "   ", 7):
            with self.assertRaises(WorkflowError) as caught:
                self.scheduler.history_page(tenant, "r1")
            self.assertEqual(caught.exception.code, "bad_tenant")

    def test_unknown_and_cross_tenant(self):
        with self.assertRaises(WorkflowError) as caught:
            self.scheduler.history_page("acme", "ghost")
        self.assertEqual(caught.exception.code, "unknown_run")
        with self.assertRaises(WorkflowError) as caught:
            self.scheduler.history_page("globex", "r1")
        self.assertEqual(caught.exception.code, "cross_tenant")

    def test_page_matches_history_without_touching_state(self):
        self.scheduler.claim("acme", "r1", "w1", 60)
        stored = self.scheduler.get_run("acme", "r1")
        expected = list(stored["history"])
        self.assertEqual([e["type"] for e in expected],
                         ["run_created", "ready", "claim", "run_started"])
        items, next_after = self.scheduler.history_page(" acme ", "r1", limit=1, after=0)
        self.assertEqual(items, expected[:1])
        self.assertEqual(next_after, 1)
        items, next_after = self.scheduler.history_page("acme", "r1", limit=1, after=1)
        self.assertEqual(items, expected[1:2])
        self.assertEqual(next_after, 2)
        items, next_after = self.scheduler.history_page("acme", "r1", limit=2, after=2)
        self.assertEqual(items, expected[2:])
        self.assertIsNone(next_after)
        self.assertEqual(self.scheduler.get_run("acme", "r1"), stored)


if __name__ == "__main__":
    unittest.main()
