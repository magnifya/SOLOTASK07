"""Tests for per-step trigger rules: all_success / all_done / any_success.

Covers validation (``bad_trigger_rule``), the normalized plan and its
freezing into runs, the three unlock rules, the ``skipped`` terminal
state, the run-completion semantics of runs using custom rules, replay
and restart, and the HTTP surface.
"""

import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.request

from flowd.http_app import _run_view, create_server
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


class TriggerTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-trigger-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock, backoff_base=1.0)

    def submit(self, steps, workflow_id="wf", tenant="acme"):
        return self.scheduler.submit(tenant, workflow_id, steps)

    def start(self, steps=None, workflow_id="wf", run_id="r1", tenant="acme", **kwargs):
        if steps is not None:
            self.submit(steps, workflow_id, tenant)
        return self.scheduler.start_run(tenant, workflow_id, run_id=run_id, **kwargs)

    def claim(self, run_id="r1", worker="w1", lease=30, tenant="acme"):
        return self.scheduler.claim(tenant, run_id, worker, lease)

    def complete(self, step_id, run_id="r1", worker="w1", tenant="acme"):
        return self.scheduler.complete(tenant, run_id, step_id, worker)

    def fail(self, step_id, run_id="r1", worker="w1", tenant="acme", error="boom"):
        return self.scheduler.fail(tenant, run_id, step_id, worker, error)

    def history_types(self, run_id="r1", tenant="acme"):
        return [e["type"] for e in self.scheduler.history(run_id, tenant)]


class TriggerRuleValidationTest(unittest.TestCase):
    def test_omitted_trigger_rule_defaults_to_all_success(self):
        plan = plan_workflow("w", [{"id": "a", "depends_on": []}])
        self.assertEqual(plan["steps"][0]["trigger_rule"], "all_success")

    def test_explicit_rules_are_accepted(self):
        plan = plan_workflow("w", [
            {"id": "a", "depends_on": [], "trigger_rule": "all_success"},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "all_done"},
            {"id": "c", "depends_on": ["a"], "trigger_rule": "any_success"},
        ])
        self.assertEqual([s["trigger_rule"] for s in plan["steps"]],
                         ["all_success", "all_done", "any_success"])

    def test_bad_trigger_rule_rejected(self):
        for bad in (None, "", "   ", "sometimes", "ALL_DONE", "all-success",
                    3, 1.5, True, ["all_done"], {"rule": "all_done"}):
            with self.assertRaises(WorkflowError) as ctx:
                plan_workflow("w", [{"id": "a", "depends_on": [], "trigger_rule": bad}])
            self.assertEqual(ctx.exception.code, "bad_trigger_rule", "value %r" % (bad,))

    def test_rejected_submission_does_not_overwrite_workflow(self):
        root = tempfile.mkdtemp(prefix="flowd-trigger-")
        self.addCleanup(shutil.rmtree, root, True)
        store = WorkflowStore(root)
        scheduler = Scheduler(store)
        good = [{"id": "a", "depends_on": [], "trigger_rule": "all_done"}]
        scheduler.submit("acme", "wf", good)
        with self.assertRaises(WorkflowError) as ctx:
            scheduler.submit("acme", "wf", [{"id": "a", "trigger_rule": "nope"}])
        self.assertEqual(ctx.exception.code, "bad_trigger_rule")
        plan = store.get_workflow("acme", "wf")
        self.assertEqual(plan["steps"][0]["trigger_rule"], "all_done")
        # No audit record was written for the rejected submission.
        items, _ = scheduler.list_audit("acme")
        self.assertEqual([i["action"] for i in items], ["workflow.submit"])


class AllDoneTest(TriggerTestBase):
    def test_all_done_unlocks_after_dependency_fails(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        self.claim()
        run = self.fail("a")
        self.assertEqual(run["steps"]["a"]["status"], "failed")
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        # The run is not failed yet: b is still waiting to be executed.
        self.assertEqual(run["status"], "pending")
        self.assertNotIn("run_failed", self.history_types())

        claimed = self.claim()
        self.assertEqual(claimed["id"], "b")
        run = self.complete("b")
        self.assertEqual(run["steps"]["b"]["status"], "succeeded")
        # Every node is terminal now and one failed: the run is failed.
        self.assertEqual(run["status"], "failed")
        self.assertEqual(self.history_types()[-1], "run_failed")

    def test_all_done_unlocks_after_dependency_succeeds(self):
        self.start([
            {"id": "a", "depends_on": []},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        self.claim()
        self.complete("a")
        self.assertEqual(self.claim()["id"], "b")
        run = self.complete("b")
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(self.history_types()[-1], "run_succeeded")

    def test_all_done_waits_for_all_dependencies(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": []},
            {"id": "c", "depends_on": ["a", "b"], "trigger_rule": "all_done"},
        ])
        self.claim()
        run = self.fail("a")
        self.assertEqual(run["steps"]["c"]["status"], "pending")  # b still open
        self.claim()
        run = self.complete("b")
        self.assertEqual(run["steps"]["c"]["status"], "ready")
        self.assertEqual(self.claim()["id"], "c")
        run = self.complete("c")
        self.assertEqual(run["status"], "failed")  # a failed earlier

    def test_all_done_approval_waits_after_dependency_fails(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "gate", "depends_on": ["a"], "kind": "approval",
             "trigger_rule": "all_done"},
        ])
        self.claim()
        run = self.fail("a")
        self.assertEqual(run["steps"]["gate"]["status"], "waiting")
        run = self.scheduler.decide("acme", "r1", "gate", "alice", "approve")
        self.assertEqual(run["steps"]["gate"]["status"], "succeeded")
        self.assertEqual(run["status"], "failed")  # a failed; all terminal


class AnySuccessTest(TriggerTestBase):
    def test_any_success_unlocks_on_first_success(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": []},
            {"id": "c", "depends_on": ["a", "b"], "trigger_rule": "any_success"},
        ])
        self.claim()
        run = self.fail("a")
        self.assertEqual(run["steps"]["c"]["status"], "pending")  # b still open
        self.claim()
        run = self.complete("b")
        self.assertEqual(run["steps"]["c"]["status"], "ready")
        self.assertEqual(self.claim()["id"], "c")
        run = self.complete("c")
        self.assertEqual(run["status"], "failed")  # a failed; all terminal

    def test_any_success_skips_when_nothing_succeeded(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": [], "max_attempts": 1},
            {"id": "c", "depends_on": ["a", "b"], "trigger_rule": "any_success"},
        ])
        self.claim()
        run = self.fail("a")
        self.assertEqual(run["steps"]["c"]["status"], "pending")
        self.claim()
        run = self.fail("b")
        step = run["steps"]["c"]
        self.assertEqual((step["status"], step["attempt"]), ("skipped", 0))
        self.assertEqual(run["status"], "failed")
        skipped = [e for e in run["history"] if e["type"] == "skipped"]
        self.assertEqual(len(skipped), 1)
        self.assertEqual(skipped[0]["step_id"], "c")
        self.assertEqual(skipped[0]["attempt"], 0)
        self.assertEqual(self.history_types()[-1], "run_failed")
        # Nothing executable remains: claims hand out nothing.
        self.assertIsNone(self.claim())


class AllSuccessSkipTest(TriggerTestBase):
    def test_all_success_dependent_is_skipped_in_mixed_run(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"]},  # all_success
            {"id": "c", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        self.claim()
        run = self.fail("a")
        self.assertEqual(run["steps"]["b"]["status"], "skipped")
        self.assertEqual(run["steps"]["b"]["attempt"], 0)
        self.assertEqual(run["steps"]["c"]["status"], "ready")
        self.assertEqual(run["status"], "pending")
        # One skipped event, emitted before the ready event of c? Both come
        # from the same settle pass in topological order (b before c).
        types = self.history_types()
        self.assertEqual(types.count("skipped"), 1)
        self.assertLess(types.index("skipped"), types.index("ready", types.index("fail")))
        self.assertEqual(self.claim()["id"], "c")
        run = self.complete("c")
        self.assertEqual(run["status"], "failed")

    def test_skip_cascades_in_topological_order(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"]},
            {"id": "c", "depends_on": ["b"]},
            {"id": "d", "depends_on": ["c"], "trigger_rule": "all_done"},
        ])
        self.claim()
        run = self.fail("a")
        self.assertEqual(run["steps"]["b"]["status"], "skipped")
        self.assertEqual(run["steps"]["c"]["status"], "skipped")
        self.assertEqual(run["steps"]["d"]["status"], "ready")
        skipped = [e["step_id"] for e in run["history"] if e["type"] == "skipped"]
        self.assertEqual(skipped, ["b", "c"])

    def test_pure_all_success_run_keeps_legacy_failure_semantics(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"]},
        ])
        self.claim()
        run = self.fail("a")
        # Legacy: the run fails immediately and the dependent stays pending.
        self.assertEqual(run["status"], "failed")
        self.assertEqual(run["steps"]["b"]["status"], "pending")
        self.assertNotIn("skipped", self.history_types())

    def test_skipped_step_cannot_be_completed_or_failed(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"]},
            {"id": "c", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        self.claim()
        self.fail("a")
        self.assertEqual(self.scheduler.get_run("acme", "r1")["steps"]["b"]["status"],
                         "skipped")
        with self.assertRaises(LeaseError):
            self.complete("b")
        with self.assertRaises(LeaseError):
            self.fail("b")
        # The skipped node never takes a lease and keeps attempt 0.
        step = self.scheduler.get_run("acme", "r1")["steps"]["b"]
        self.assertEqual((step["attempt"], step["worker_id"], step["lease_deadline"]),
                         (0, None, None))


class RootAndRunCompletionTest(TriggerTestBase):
    def test_roots_open_immediately_under_every_rule(self):
        run = self.start([
            {"id": "a", "depends_on": [], "trigger_rule": "all_done"},
            {"id": "b", "depends_on": [], "trigger_rule": "any_success"},
            {"id": "g", "depends_on": [], "kind": "approval",
             "trigger_rule": "any_success"},
        ])
        self.assertEqual(run["steps"]["a"]["status"], "ready")
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        self.assertEqual(run["steps"]["g"]["status"], "waiting")

    def test_mixed_run_succeeds_only_when_all_terminal(self):
        self.start([
            {"id": "a", "depends_on": []},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        self.assertEqual(self.scheduler.get_run("acme", "r1")["status"], "pending")
        self.claim()
        run = self.complete("a")
        self.assertEqual(run["status"], "running")
        self.claim()
        run = self.complete("b")
        self.assertEqual(run["status"], "succeeded")

    def test_mixed_run_stays_pending_while_approval_waits(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "gate", "depends_on": ["a"], "kind": "approval",
             "trigger_rule": "all_done"},
        ])
        self.claim()
        run = self.fail("a")
        self.assertEqual(run["steps"]["gate"]["status"], "waiting")
        self.assertEqual(run["status"], "pending")
        run = self.scheduler.decide("acme", "r1", "gate", "alice", "reject")
        self.assertEqual(run["status"], "failed")

    def test_reject_in_mixed_run_does_not_fail_immediately(self):
        self.start([
            {"id": "gate", "depends_on": [], "kind": "approval"},
            {"id": "b", "depends_on": ["gate"], "trigger_rule": "all_done"},
        ])
        run = self.scheduler.decide("acme", "r1", "gate", "alice", "reject")
        self.assertEqual(run["steps"]["gate"]["status"], "failed")
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        self.assertEqual(run["status"], "pending")
        self.assertNotIn("run_failed", self.history_types())
        self.claim()
        run = self.complete("b")
        self.assertEqual(run["status"], "failed")

    def test_explicit_cancel_of_mixed_run_is_cancelled(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        self.claim()
        self.fail("a")
        run = self.scheduler.cancel("acme", "r1", "ops")
        self.assertEqual(run["status"], "cancelled")
        self.assertEqual(run["steps"]["a"]["status"], "failed")
        self.assertEqual(run["steps"]["b"]["status"], "cancelled")
        self.assertEqual(self.scheduler.replay("acme", "r1")["status"], "cancelled")


class FrozenPlanTest(TriggerTestBase):
    def test_trigger_rule_is_frozen_into_the_run_plan(self):
        plan = self.submit([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        run = self.scheduler.start_run("acme", "wf", run_id="r1")
        self.assertEqual(run["plan"]["steps"], plan["steps"])
        self.assertEqual(run["steps"]["b"]["trigger_rule"], "all_done")
        # Overwriting the workflow with a different rule must not affect the
        # existing run.
        self.submit([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "any_success"},
        ])
        self.claim()
        run = self.fail("a")
        # all_done (frozen): b unlocked; any_success would have skipped it.
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        replayed = self.scheduler.replay("acme", "r1")
        self.assertEqual(replayed["steps"]["b"]["trigger_rule"], "all_done")
        self.assertEqual(replayed["steps"]["b"]["status"], "ready")
        # A new run uses the new definition.
        run2 = self.scheduler.start_run("acme", "wf", run_id="r2")
        self.assertEqual(run2["steps"]["b"]["trigger_rule"], "any_success")

    def test_old_document_without_trigger_rule_reads_as_all_success(self):
        self.submit([{"id": "a", "depends_on": [], "max_attempts": 1},
                     {"id": "b", "depends_on": ["a"]}])
        run = self.scheduler.start_run("acme", "wf", run_id="r1")
        for step in run["steps"].values():
            del step["trigger_rule"]
        for planned in run["plan"]["steps"]:
            del planned["trigger_rule"]
        self.store.save_run(run)
        reloaded = self.scheduler.get_run("acme", "r1")
        self.assertEqual(reloaded["steps"]["b"]["trigger_rule"], "all_success")
        self.assertEqual(reloaded["plan"]["steps"][1]["trigger_rule"], "all_success")
        # Legacy semantics apply: the exhausted root fails the run at once.
        self.claim()
        run = self.fail("a")
        self.assertEqual(run["status"], "failed")
        self.assertEqual(run["steps"]["b"]["status"], "pending")


class ReplayAndRestartTest(TriggerTestBase):
    def drive(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"]},
            {"id": "c", "depends_on": ["a"], "trigger_rule": "all_done"},
            {"id": "d", "depends_on": ["b", "c"], "trigger_rule": "any_success"},
        ])
        self.claim()
        self.fail("a")          # b skips, c becomes ready
        self.claim()
        self.complete("c")      # d unlocks (c succeeded)
        self.claim()
        return self.complete("d")

    def test_replay_matches_stored_state_through_skips(self):
        stored = self.drive()
        self.assertEqual(stored["status"], "failed")  # a failed; rest terminal
        replayed = self.scheduler.replay("acme", "r1")
        self.assertEqual(replayed, stored)
        self.assertEqual(replayed["steps"]["b"]["status"], "skipped")
        self.assertEqual(replayed["steps"]["b"]["trigger_rule"], "all_success")

    def test_state_survives_a_fresh_store(self):
        stored = self.drive()
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock),
                              clock=self.clock, backoff_base=1.0)
        again = restarted.get_run("acme", "r1")
        self.assertEqual(again["status"], "failed")
        self.assertEqual(again["steps"]["b"]["status"], "skipped")
        self.assertEqual(restarted.replay("acme", "r1"), stored)

    def test_skipped_events_enter_the_audit_stream(self):
        self.drive()
        items, _ = self.scheduler.list_audit("acme", action="skipped")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["step_id"], "b")
        self.assertEqual(items[0]["run_id"], "r1")


class FairClaimAndQuotaTest(TriggerTestBase):
    def test_fair_claim_leases_all_done_unlocked_task(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        self.claim()
        self.fail("a")
        result = self.scheduler.claim_fair("acme", "w2", 30)
        self.assertIsNotNone(result)
        run_id, step = result
        self.assertEqual((run_id, step["id"]), ("r1", "b"))

    def test_skipped_nodes_hold_no_quota_slot(self):
        self.start([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"]},
            {"id": "c", "depends_on": ["a"], "trigger_rule": "all_done"},
        ], run_id="r1", max_parallelism=1)
        self.claim()
        self.fail("a")  # b skipped, c ready; quota of 1 is free again
        claimed = self.claim(worker="w2")
        self.assertEqual(claimed["id"], "c")


class TriggerRuleHttpTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-trigger-http-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = WorkflowStore(self.root)
        self.server = create_server(self.store, port=0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.daemon = True
        self.thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)

    def call(self, method, path, body=None, raw_body=None):
        data = raw_body if raw_body is not None else (
            json.dumps(body).encode("utf-8") if body is not None else None)
        request = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (self.port, path), data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request) as response:
                raw = response.read()
                return response.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            return exc.code, json.loads(raw) if raw else None

    def submit(self, steps, workflow_id="wf"):
        return self.call("POST", "/v1/workflows",
                         {"tenant": "acme", "workflow_id": workflow_id, "steps": steps})

    def test_bad_trigger_rule_returns_400_and_saves_nothing(self):
        status, body = self.submit([{"id": "a", "trigger_rule": "all_done"}])
        self.assertEqual(status, 201)
        self.assertEqual(body["steps"][0]["trigger_rule"], "all_done")
        for bad in (None, "", "nope", 7, True):
            status, body = self.submit([{"id": "a", "trigger_rule": bad}])
            self.assertEqual(status, 400, bad)
            self.assertIn("error", body)
        # The valid workflow was not overwritten by the rejected submissions.
        status, body = self.call("POST", "/v1/runs",
                                 {"tenant": "acme", "workflow_id": "wf", "run_id": "r1"})
        self.assertEqual(status, 201)
        self.assertEqual(body["steps"][0]["trigger_rule"], "all_done")
        # No audit records beyond the single accepted submission.
        status, body = self.call("GET", "/v1/audit?tenant=acme")
        self.assertEqual([i["action"] for i in body["items"]],
                         ["workflow.submit", "run_created", "ready"])

    def test_run_detail_presents_rule_and_skipped_status(self):
        self.submit([
            {"id": "a", "depends_on": [], "max_attempts": 1},
            {"id": "b", "depends_on": ["a"]},
            {"id": "c", "depends_on": ["a"], "trigger_rule": "all_done"},
        ])
        self.call("POST", "/v1/runs", {"tenant": "acme", "workflow_id": "wf",
                                       "run_id": "r1"})
        status, body = self.call("POST", "/v1/runs/r1/claim",
                                 {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 200)
        status, body = self.call("POST", "/v1/runs/r1/fail",
                                 {"tenant": "acme", "step_id": "a", "worker_id": "w1",
                                  "error": "boom"})
        self.assertEqual(status, 200)
        steps = {s["id"]: s for s in body["steps"]}
        self.assertEqual(steps["b"]["status"], "skipped")
        self.assertEqual(steps["b"]["trigger_rule"], "all_success")
        self.assertEqual(steps["c"]["status"], "ready")
        self.assertEqual(steps["c"]["trigger_rule"], "all_done")
        self.assertEqual(body["status"], "pending")
        # Completing the skipped node conflicts; the run finishes failed
        # once c completes.
        status, body = self.call("POST", "/v1/runs/r1/complete",
                                 {"tenant": "acme", "step_id": "b", "worker_id": "w1"})
        self.assertEqual(status, 409)
        self.call("POST", "/v1/runs/r1/claim", {"tenant": "acme", "worker_id": "w1"})
        status, body = self.call("POST", "/v1/runs/r1/complete",
                                 {"tenant": "acme", "step_id": "c", "worker_id": "w1"})
        self.assertEqual(body["status"], "failed")


if __name__ == "__main__":
    unittest.main()
