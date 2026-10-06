"""Tests for per-step trigger rules (all_success / all_done / any_success)."""

import json
import os
import shutil
import tempfile
import unittest

from flowd.model import WorkflowError, plan_workflow
from flowd.scheduler import ConflictError, LeaseError, Scheduler
from flowd.store import WorkflowStore


class FakeClock:
    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value

    def sleep(self, seconds):
        self.advance(seconds)


class TriggerRuleTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-trigger-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock, backoff_base=1.0)

    def workflow(self, steps, workflow_id="etl"):
        return self.scheduler.submit("acme", workflow_id, steps)

    def start(self, steps, workflow_id="etl", run_id="r1"):
        self.workflow(steps, workflow_id)
        return self.scheduler.start_run("acme", workflow_id, run_id=run_id)

    def claim(self, run_id="r1", worker="w1"):
        return self.scheduler.claim("acme", run_id, worker, 30)

    def complete(self, step_id, run_id="r1", worker="w1"):
        return self.scheduler.complete("acme", run_id, step_id, worker)

    def fail(self, step_id, run_id="r1", worker="w1", error="boom"):
        return self.scheduler.fail("acme", run_id, step_id, worker, error)

    def exhaust(self, step_id, run_id="r1", worker="w1"):
        """Fail ``step_id`` (max_attempts 1) until it is terminally failed."""
        self.claim(run_id, worker)
        return self.fail(step_id, run_id, worker)


class ValidationTest(TriggerRuleTestBase):
    def test_omitted_trigger_rule_defaults_to_all_success(self):
        plan = plan_workflow("etl", [{"id": "a", "depends_on": []}])
        self.assertEqual(plan["steps"][0]["trigger_rule"], "all_success")

    def test_explicit_rules_are_normalized_into_the_plan(self):
        plan = plan_workflow("etl", [
            {"id": "a", "depends_on": [], "trigger_rule": "all_done"},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "any_success"},
            {"id": "c", "depends_on": ["b"], "trigger_rule": "all_success"},
        ])
        self.assertEqual([s["trigger_rule"] for s in plan["steps"]],
                         ["all_done", "any_success", "all_success"])

    def test_null_trigger_rule_defaults_to_all_success(self):
        plan = plan_workflow("etl", [{"id": "a", "depends_on": [], "trigger_rule": None}])
        self.assertEqual(plan["steps"][0]["trigger_rule"], "all_success")

    def test_bad_trigger_rules_are_rejected(self):
        for bad in (123, True, 1.5, ["all_done"], {"rule": "all_done"},
                    "", "   ", "all", "ALL_DONE", "any", "none"):
            with self.assertRaises(WorkflowError) as ctx:
                plan_workflow("etl", [{"id": "a", "depends_on": [], "trigger_rule": bad}])
            self.assertEqual(ctx.exception.code, "bad_trigger_rule", bad)

    def test_rejected_submit_does_not_save_or_audit(self):
        self.workflow([{"id": "a", "depends_on": []}], workflow_id="keep")
        before = self.store.load_workflows("acme")
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.submit("acme", "keep",
                                  [{"id": "a", "depends_on": [], "trigger_rule": "nope"}])
        self.assertEqual(ctx.exception.code, "bad_trigger_rule")
        # The stored workflow is untouched and no audit record was appended.
        self.assertEqual(self.store.load_workflows("acme"), before)
        self.assertEqual(self.store.load_audit("acme"),
                         [r for r in self.store.load_audit("acme")
                          if r["action"] == "workflow.submit"])
        self.assertEqual([r["action"] for r in self.store.load_audit("acme")],
                         ["workflow.submit"])


class SchedulingTest(TriggerRuleTestBase):
    def test_root_nodes_open_immediately_under_every_rule(self):
        run = self.start([
            {"id": "a", "depends_on": [], "trigger_rule": "all_success"},
            {"id": "b", "depends_on": [], "trigger_rule": "all_done"},
            {"id": "c", "depends_on": [], "trigger_rule": "any_success"},
        ])
        self.assertEqual([run["steps"][s]["status"] for s in ("a", "b", "c")],
                         ["ready", "ready", "ready"])

    def test_all_done_unlocks_after_dependency_fails(self):
        run = self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        run = self.exhaust("a")
        self.assertEqual(run["steps"]["a"]["status"], "failed")
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        # The run is not finished: b can still be claimed and completed.
        self.assertNotIn(run["status"], ("succeeded", "failed", "cancelled"))
        self.assertEqual(self.claim()["id"], "b")
        run = self.complete("b")
        self.assertEqual(run["steps"]["b"]["status"], "succeeded")
        # Every node terminal and a failure exists: the run is failed.
        self.assertEqual(run["status"], "failed")

    def test_all_done_unlocks_after_dependency_is_skipped(self):
        run = self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"]},  # all_success: skipped by a's failure
            {"id": "c", "depends_on": ["b"], "trigger_rule": "all_done"},
        ])
        run = self.exhaust("a")
        self.assertEqual(run["steps"]["b"]["status"], "skipped")
        self.assertEqual(run["steps"]["c"]["status"], "ready")

    def test_any_success_unlocks_on_first_success(self):
        run = self.start([
            {"id": "a", "depends_on": []},
            {"id": "b", "depends_on": []},
            {"id": "c", "depends_on": ["a", "b"], "trigger_rule": "any_success"},
        ])
        self.claim()
        run = self.complete("a")
        # b is still pending-ready, but one success already unlocks c.
        self.assertEqual(run["steps"]["c"]["status"], "ready")

    def test_any_success_skips_when_no_dependency_succeeds(self):
        run = self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": [], "max_attempts": 1},
            {"id": "c", "depends_on": ["a", "b"], "trigger_rule": "any_success"},
        ])
        self.exhaust("a")
        run = self.exhaust("b")
        step = run["steps"]["c"]
        self.assertEqual(step["status"], "skipped")
        self.assertEqual(step["attempt"], 0)
        # All nodes terminal with failures present: the run is failed.
        self.assertEqual(run["status"], "failed")

    def test_all_success_node_is_skipped_once_impossible(self):
        run = self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "all_done"},
            {"id": "c", "depends_on": ["a"]},
        ])
        run = self.exhaust("a")
        self.assertEqual(run["steps"]["c"]["status"], "skipped")
        self.assertEqual(run["steps"]["c"]["attempt"], 0)
        self.assertEqual(run["steps"]["b"]["status"], "ready")

    def test_skipped_node_is_never_claimed_and_cannot_be_completed(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "any_success"},
        ])
        self.exhaust("a")
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["b"]["status"], "skipped")
        # Nothing executable: claim returns None and changes nothing.
        self.assertIsNone(self.claim())
        with self.assertRaises(LeaseError):
            self.complete("b")
        with self.assertRaises(LeaseError):
            self.fail("b")

    def test_run_succeeds_with_only_skips_and_successes(self):
        run = self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"]},  # skipped
            {"id": "c", "depends_on": [], "trigger_rule": "all_done"},
        ])
        self.exhaust("a")
        # a failed -> run must end failed once terminal; drive c first.
        self.assertEqual(self.claim()["id"], "c")
        run = self.complete("c")
        self.assertEqual(run["status"], "failed")

    def test_flexible_run_without_failures_succeeds(self):
        run = self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "gate", "depends_on": ["a"], "kind": "approval",
             "trigger_rule": "all_done"},
            {"id": "b", "depends_on": ["gate"]},
        ])
        self.exhaust("a")
        run = self.scheduler.get_run("acme", "r1")
        # The approval unlocks even though its dependency failed.
        self.assertEqual(run["steps"]["gate"]["status"], "waiting")
        self.assertEqual(run["status"], "pending")
        run = self.scheduler.decide("acme", "r1", "gate", "alice", "reject")
        self.assertEqual(run["steps"]["b"]["status"], "skipped")
        self.assertEqual(run["status"], "failed")

    def test_approval_rejection_in_flexible_run_keeps_run_alive(self):
        run = self.start([
            {"id": "gate", "depends_on": [], "kind": "approval"},
            {"id": "b", "depends_on": ["gate"], "trigger_rule": "all_done"},
        ])
        run = self.scheduler.decide("acme", "r1", "gate", "alice", "reject")
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        self.assertNotEqual(run["status"], "failed")
        self.assertEqual(self.claim()["id"], "b")
        run = self.complete("b")
        self.assertEqual(run["status"], "failed")

    def test_run_stays_pending_until_all_terminal(self):
        run = self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        run = self.exhaust("a")
        self.assertEqual(run["status"], "pending")
        self.claim()
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["status"], "running")

    def test_explicit_cancel_of_flexible_run_is_cancelled(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        self.exhaust("a")
        run = self.scheduler.cancel("acme", "r1", "bob")
        self.assertEqual(run["status"], "cancelled")
        self.assertEqual(run["steps"]["b"]["status"], "cancelled")
        replayed = self.scheduler.replay("acme", "r1")
        self.assertEqual(replayed["status"], "cancelled")

    def test_legacy_run_still_fails_immediately(self):
        run = self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"]},
        ])
        run = self.exhaust("a")
        self.assertEqual(run["status"], "failed")
        # Legacy semantics: the dependent stays pending, not skipped.
        self.assertEqual(run["steps"]["b"]["status"], "pending")

    def test_skipped_events_are_recorded_in_topological_order(self):
        run = self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"]},
            {"id": "c", "depends_on": ["b"]},
            {"id": "d", "depends_on": ["c"], "trigger_rule": "all_done"},
        ])
        run = self.exhaust("a")
        skipped = [e for e in run["history"] if e["type"] == "skipped"]
        self.assertEqual([e["step_id"] for e in skipped], ["b", "c"])
        self.assertEqual(run["steps"]["d"]["status"], "ready")
        for event in skipped:
            self.assertEqual(event["attempt"], 0)
            self.assertIsNone(event["worker_id"])
        # The audit stream carries the same transitions in the same order.
        audit = [r for r in self.store.load_audit("acme") if r["action"] == "skipped"]
        self.assertEqual([r["step_id"] for r in audit], ["b", "c"])


class ReplayTest(TriggerRuleTestBase):
    def test_replay_matches_stored_state(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "any_success"},
            {"id": "c", "depends_on": ["b"], "trigger_rule": "all_done"},
        ])
        self.exhaust("a")
        stored = self.scheduler.get_run("acme", "r1")
        replayed = self.scheduler.replay("acme", "r1")
        self.assertEqual(replayed["status"], stored["status"])
        for sid in ("a", "b", "c"):
            self.assertEqual(replayed["steps"][sid]["status"],
                             stored["steps"][sid]["status"], sid)
            self.assertEqual(replayed["steps"][sid]["trigger_rule"],
                             stored["steps"][sid]["trigger_rule"], sid)
        self.assertEqual(replayed["steps"]["b"]["status"], "skipped")

    def test_replay_and_restart_rebuild_terminal_flexible_run(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        self.exhaust("a")
        self.claim()
        self.complete("b")
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        stored = restarted.get_run("acme", "r1")
        replayed = restarted.replay("acme", "r1")
        self.assertEqual(stored["status"], "failed")
        self.assertEqual(replayed["status"], stored["status"])
        self.assertEqual(replayed["steps"]["b"]["status"], "succeeded")

    def test_old_document_without_trigger_rule_reads_as_all_success(self):
        self.start([{"id": "a", "depends_on": []}], run_id="legacy-1")
        run = self.scheduler.get_run("acme", "legacy-1")
        del run["steps"]["a"]["trigger_rule"]
        del run["plan"]["steps"][0]["trigger_rule"]
        self.store.save_run(run)
        reloaded = self.scheduler.get_run("acme", "legacy-1")
        self.assertEqual(reloaded["steps"]["a"]["trigger_rule"], "all_success")
        self.assertEqual(self.claim("legacy-1")["id"], "a")
        replayed = self.scheduler.replay("acme", "legacy-1")
        self.assertEqual(replayed["steps"]["a"]["trigger_rule"], "all_success")

    def test_frozen_plan_is_immune_to_later_workflow_updates(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        # Resubmit the same workflow id with a different rule for b.
        self.workflow([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "any_success"},
        ])
        self.exhaust("a")
        run = self.scheduler.get_run("acme", "r1")
        # The old run keeps its frozen all_done rule: b unlocked.
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        replayed = self.scheduler.replay("acme", "r1")
        self.assertEqual(replayed["steps"]["b"]["trigger_rule"], "all_done")
        self.assertEqual(replayed["steps"]["b"]["status"], "ready")
        # A new run uses the new definition: b is skipped after a fails.
        run2 = self.scheduler.start_run("acme", "etl", run_id="r2")
        self.claim("r2")
        run2 = self.fail("a", "r2")
        self.assertEqual(run2["steps"]["b"]["status"], "skipped")


class HttpTriggerRuleTest(unittest.TestCase):
    """HTTP surface: validation errors, run details, 204 on nothing ready."""

    def setUp(self):
        import threading
        from flowd.http_app import create_server

        self.root = tempfile.mkdtemp(prefix="flowd-trigger-http-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)
        self.server = create_server(self.store, "127.0.0.1", 0,
                                    scheduler=self.scheduler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, body=None):
        import urllib.error
        import urllib.request

        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (self.port, path), data=data, method=method)
        if data is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                payload = response.read()
                return response.status, (json.loads(payload) if payload else None)
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            return exc.code, (json.loads(payload) if payload else None)

    def test_bad_trigger_rule_returns_400_and_saves_nothing(self):
        status, body = self.call("POST", "/v1/workflows", {
            "tenant": "acme", "workflow_id": "etl",
            "steps": [{"id": "a", "depends_on": [], "trigger_rule": "sometimes"}]})
        self.assertEqual(status, 400)
        self.assertIn("error", body)
        # The workflow was not saved and no audit record was written.
        self.assertEqual(self.store.load_workflows("acme"), {})
        self.assertEqual(self.store.load_audit("acme"), [])
        # A valid submission afterwards still works.
        status, body = self.call("POST", "/v1/workflows", {
            "tenant": "acme", "workflow_id": "etl",
            "steps": [{"id": "a", "depends_on": [], "trigger_rule": "all_done"}]})
        self.assertEqual(status, 201)
        self.assertEqual(body["steps"][0]["trigger_rule"], "all_done")

    def test_bad_trigger_rule_does_not_overwrite_existing_workflow(self):
        self.call("POST", "/v1/workflows", {
            "tenant": "acme", "workflow_id": "etl",
            "steps": [{"id": "a", "depends_on": []}]})
        status, _ = self.call("POST", "/v1/workflows", {
            "tenant": "acme", "workflow_id": "etl",
            "steps": [{"id": "a", "depends_on": [], "trigger_rule": 42}]})
        self.assertEqual(status, 400)
        stored = self.store.get_workflow("acme", "etl")
        self.assertEqual([s["id"] for s in stored["steps"]], ["a"])

    def test_run_details_present_rules_and_skipped_states(self):
        self.call("POST", "/v1/workflows", {
            "tenant": "acme", "workflow_id": "etl",
            "steps": [
                {"id": "a", "depends_on": [], "max_attempts": 1},
                {"id": "b", "depends_on": ["a"], "trigger_rule": "any_success"},
            ]})
        self.call("POST", "/v1/runs",
                  {"tenant": "acme", "workflow_id": "etl", "run_id": "r1"})
        status, body = self.call("POST", "/v1/runs/r1/claim",
                                 {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 200)
        self.assertEqual(body["step"]["trigger_rule"], "all_success")
        self.call("POST", "/v1/runs/r1/fail",
                  {"tenant": "acme", "step_id": "a", "worker_id": "w1"})
        # b is skipped; nothing is claimable anymore.
        status, body = self.call("POST", "/v1/runs/r1/claim",
                                 {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 204)
        status, body = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(status, 200)
        steps = {s["id"]: s for s in body["steps"]}
        self.assertEqual(steps["b"]["status"], "skipped")
        self.assertEqual(steps["b"]["trigger_rule"], "any_success")
        self.assertEqual(body["status"], "failed")


if __name__ == "__main__":
    unittest.main()
