"""Tests for human approval nodes: validation, waiting, decisions, replay."""

import shutil
import tempfile
import threading
import unittest

from flowd.http_app import _run_view
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


class ApprovalTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-approval-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def submit(self, steps, workflow_id="wf", tenant="acme"):
        return self.scheduler.submit(tenant, workflow_id, steps)

    def start(self, steps=None, workflow_id="wf", run_id="r1", tenant="acme"):
        if steps is not None:
            self.submit(steps, workflow_id, tenant)
        return self.scheduler.start_run(tenant, workflow_id, run_id=run_id)

    def decide(self, step_id, decision="approve", actor="alice", run_id="r1", tenant="acme"):
        return self.scheduler.decide(tenant, run_id, step_id, actor, decision)


def approval(step_id, depends_on=None):
    return {"id": step_id, "depends_on": depends_on or [], "kind": "approval"}


class KindValidationTest(unittest.TestCase):
    def test_kind_defaults_to_task(self):
        plan = plan_workflow("w", [{"id": "a", "depends_on": []}])
        self.assertEqual(plan["steps"][0]["kind"], "task")

    def test_explicit_kinds_accepted(self):
        plan = plan_workflow("w", [
            {"id": "a", "depends_on": [], "kind": "task"},
            {"id": "b", "depends_on": ["a"], "kind": "approval"},
        ])
        self.assertEqual([s["kind"] for s in plan["steps"]], ["task", "approval"])

    def test_bad_kind_rejected(self):
        for bad in ("gate", "APPROVAL", "", 3, True, ["approval"]):
            with self.assertRaises(WorkflowError) as ctx:
                plan_workflow("w", [{"id": "a", "depends_on": [], "kind": bad}])
            self.assertEqual(ctx.exception.code, "bad_kind", "value %r" % (bad,))

    def test_existing_validation_unchanged(self):
        with self.assertRaises(WorkflowError):
            plan_workflow("w", [{"id": "a", "depends_on": ["ghost"], "kind": "approval"}])


class ApprovalLifecycleTest(ApprovalTestBase):
    def test_approval_waits_at_creation_when_it_has_no_dependencies(self):
        run = self.start([approval("a")])
        step = run["steps"]["a"]
        self.assertEqual(step["status"], "waiting")
        self.assertEqual(step["attempt"], 0)
        self.assertIsNone(step["approval"])
        self.assertIsNone(self.scheduler.claim("acme", "r1", "w1"))

    def test_approval_waits_only_after_dependencies_succeed(self):
        run = self.start([
            {"id": "t", "depends_on": []},
            approval("a", ["t"]),
            {"id": "b", "depends_on": ["a"]},
        ])
        self.assertEqual([run["steps"][s]["status"] for s in ("t", "a", "b")],
                         ["ready", "pending", "pending"])
        self.assertEqual(self.scheduler.claim("acme", "r1", "w1")["id"], "t")
        run = self.scheduler.complete("acme", "r1", "t", "w1")
        self.assertEqual(run["steps"]["a"]["status"], "waiting")
        self.assertEqual(run["steps"]["b"]["status"], "pending")
        # waiting blocks only its successors: nothing else is claimable
        self.assertIsNone(self.scheduler.claim("acme", "r1", "w2"))

    def test_claim_skips_approvals_but_independent_tasks_keep_order(self):
        run = self.start([
            approval("a"),
            {"id": "b", "depends_on": []},
            {"id": "c", "depends_on": ["a"]},
            {"id": "d", "depends_on": ["b"]},
        ])
        self.assertEqual(run["steps"]["a"]["status"], "waiting")
        self.assertEqual(self.scheduler.claim("acme", "r1", "w1")["id"], "b")
        run = self.scheduler.complete("acme", "r1", "b", "w1")
        self.assertEqual(self.scheduler.claim("acme", "r1", "w1")["id"], "d")
        self.assertEqual(run["steps"]["c"]["status"], "pending")

    def test_approve_succeeds_and_unlocks_successors(self):
        run = self.start([approval("a"), {"id": "b", "depends_on": ["a"]}])
        run = self.decide("a")
        step = run["steps"]["a"]
        self.assertEqual((step["status"], step["attempt"]), ("succeeded", 0))
        self.assertEqual(step["approval"],
                         {"actor": "alice", "decision": "approve", "at": step["finished_at"]})
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        self.assertIsNone(step["worker_id"])
        self.assertIsNone(step["lease_deadline"])
        self.assertIsNone(step["result"])
        self.assertIsNone(step["error"])

    def test_reject_fails_step_and_run_leaving_successors_pending(self):
        run = self.start([approval("a"), {"id": "b", "depends_on": ["a"]}])
        run = self.decide("a", decision="reject")
        self.assertEqual(run["status"], "failed")
        step = run["steps"]["a"]
        self.assertEqual(step["status"], "failed")
        self.assertEqual(step["attempt"], 0)
        self.assertEqual(step["approval"]["decision"], "reject")
        self.assertIsNone(step["result"])
        self.assertIsNone(step["error"])
        self.assertIsNone(step["worker_id"])
        self.assertIsNone(step["lease_deadline"])
        self.assertEqual(run["steps"]["b"]["status"], "pending")
        self.assertIsNone(self.scheduler.claim("acme", "r1", "w1"))

    def test_approving_final_approval_succeeds_the_run(self):
        run = self.start([approval("a")])
        self.assertEqual(run["status"], "pending")
        run = self.decide("a")
        self.assertEqual(run["status"], "succeeded")

    def test_complete_and_fail_on_approval_conflict(self):
        self.start([approval("a")])
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.complete("acme", "r1", "a", "w1")
        self.assertEqual(ctx.exception.code, "not_a_task")
        with self.assertRaises(ConflictError):
            self.scheduler.fail("acme", "r1", "a", "w1", "boom")

    def test_decisions_targetting_tasks_or_unready_approvals_conflict(self):
        self.start([
            {"id": "t", "depends_on": []},
            approval("a", ["t"]),
        ])
        with self.assertRaises(ConflictError) as ctx:
            self.decide("t")
        self.assertEqual(ctx.exception.code, "not_an_approval")
        with self.assertRaises(ConflictError) as ctx:
            self.decide("a")
        self.assertEqual(ctx.exception.code, "not_waiting")

    def test_unknown_step_and_run_are_404_class_errors(self):
        self.start([approval("a")])
        with self.assertRaises(WorkflowError) as ctx:
            self.decide("ghost")
        self.assertEqual(ctx.exception.code, "unknown_step")
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.decide("acme", "missing", "a", "alice", "approve")
        self.assertEqual(ctx.exception.code, "unknown_run")

    def test_decision_validates_actor(self):
        self.start([approval("a")])
        for bad in ("", "   ", None, 9, ["x"]):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.decide("acme", "r1", "a", bad, "approve")
            self.assertEqual(ctx.exception.code, "bad_actor", "value %r" % (bad,))
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.decide("acme", "r1", "a", "alice", "maybe")
        self.assertEqual(ctx.exception.code, "bad_decision")


class DecisionIdempotencyTest(ApprovalTestBase):
    def test_same_actor_and_decision_is_idempotent(self):
        run = self.start([approval("a"), {"id": "b", "depends_on": ["a"]}])
        first = self.decide("a")
        self.clock.advance(500)
        second = self.decide("a", actor="  alice  ")
        self.assertEqual(first["steps"]["a"]["approval"], second["steps"]["a"]["approval"])
        self.assertEqual(len(second["history"]), len(first["history"]))
        decisions = [e for e in second["history"] if e["type"] == "decision"]
        self.assertEqual(len(decisions), 1)
        self.assertEqual(second["steps"]["b"]["status"], "ready")

    def test_different_actor_conflicts_and_changes_nothing(self):
        self.start([approval("a")])
        first = self.decide("a", actor="alice")
        with self.assertRaises(ConflictError) as ctx:
            self.decide("a", actor="bob")
        self.assertEqual(ctx.exception.code, "already_decided")
        stored = self.scheduler.get_run("acme", "r1")
        self.assertEqual(stored["steps"]["a"]["approval"]["actor"], "alice")
        self.assertEqual(len(stored["history"]), len(first["history"]))

    def test_reversed_decision_conflicts(self):
        self.start([approval("a")])
        self.decide("a", decision="approve")
        with self.assertRaises(ConflictError):
            self.decide("a", decision="reject")
        self.assertEqual(self.scheduler.get_run("acme", "r1")["status"], "succeeded")
        # a rejected decision also blocks any later approve
        self.start([approval("a2")], run_id="r2")
        self.decide("a2", decision="reject", run_id="r2")
        with self.assertRaises(ConflictError):
            self.decide("a2", decision="approve", run_id="r2")
        self.assertEqual(self.scheduler.get_run("acme", "r2")["status"], "failed")

    def test_concurrent_decisions_share_one_outcome(self):
        self.start([approval("a"), {"id": "b", "depends_on": ["a"]}])
        outcomes = []
        barrier = threading.Barrier(8)

        def worker(n):
            barrier.wait()
            try:
                # distinct actors: only the first can win, the rest conflict
                self.scheduler.decide("acme", "r1", "a", "actor-%d" % n, "approve")
                outcomes.append("ok")
            except ConflictError:
                outcomes.append("conflict")

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(outcomes.count("ok"), 1)
        self.assertEqual(outcomes.count("conflict"), 7)
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(len([e for e in run["history"] if e["type"] == "decision"]), 1)
        self.assertEqual(run["steps"]["b"]["status"], "ready")

    def test_concurrent_identical_decisions_all_return_ok(self):
        self.start([approval("a")])
        outcomes = []
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            self.decide("a")  # same actor + same decision every time
            outcomes.append(1)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(outcomes), 8)
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(len([e for e in run["history"] if e["type"] == "decision"]), 1)
        self.assertEqual(run["status"], "succeeded")


class ApprovalHistoryReplayTest(ApprovalTestBase):
    def test_waiting_and_decision_history_shape(self):
        self.start([
            {"id": "t", "depends_on": []},
            approval("a", ["t"]),
        ])
        self.scheduler.claim("acme", "r1", "w1")
        self.scheduler.complete("acme", "r1", "t", "w1")
        self.decide("a")
        history = self.scheduler.history("r1", "acme")
        waiting = [e for e in history if e["type"] == "waiting"]
        decisions = [e for e in history if e["type"] == "decision"]
        self.assertEqual(len(waiting), 1)
        self.assertEqual(waiting[0]["step_id"], "a")
        self.assertEqual(len(decisions), 1)
        event = decisions[0]
        for key in ("at", "run_id", "step_id", "type", "attempt", "worker_id"):
            self.assertIn(key, event)
        self.assertEqual(event["attempt"], 0)
        self.assertIsNone(event["worker_id"])
        self.assertEqual(event["actor"], "alice")
        self.assertEqual(event["decision"], "approve")

    def test_replay_matches_stored_state_for_approve_and_reject(self):
        self.start([approval("a"), {"id": "b", "depends_on": ["a"]}])
        self.decide("a")
        stored = self.scheduler.get_run("acme", "r1")
        replayed = self.scheduler.replay("acme", "r1")
        self.assertEqual(replayed["status"], stored["status"])
        for sid in ("a", "b"):
            lhs, rhs = replayed["steps"][sid], stored["steps"][sid]
            self.assertEqual(lhs["status"], rhs["status"], sid)
            self.assertEqual(lhs["attempt"], rhs["attempt"], sid)
        self.assertEqual(replayed["steps"]["a"]["approval"], stored["steps"]["a"]["approval"])

        self.start([approval("c"), {"id": "d", "depends_on": ["c"]}], run_id="r2")
        self.decide("c", decision="reject", run_id="r2")
        replayed = self.scheduler.replay("acme", "r2")
        stored = self.scheduler.get_run("acme", "r2")
        self.assertEqual(replayed["status"], "failed")
        self.assertEqual(replayed["steps"]["c"]["status"], "failed")
        self.assertEqual(replayed["steps"]["c"]["attempt"], 0)
        self.assertEqual(replayed["steps"]["c"]["approval"], stored["steps"]["c"]["approval"])
        self.assertEqual(replayed["steps"]["d"]["status"], "pending")

    def test_state_survives_restart(self):
        self.start([approval("a"), {"id": "b", "depends_on": ["a"]}])
        self.decide("a")
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        run = restarted.get_run("acme", "r1")
        self.assertEqual(run["steps"]["a"]["status"], "succeeded")
        self.assertEqual(run["steps"]["a"]["approval"]["actor"], "alice")
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        self.assertEqual(restarted.claim("acme", "r1", "w1")["id"], "b")
        restarted.complete("acme", "r1", "b", "w1")
        self.assertEqual(restarted.replay("acme", "r1")["status"], "succeeded")

    def test_old_document_without_kind_is_treated_as_task(self):
        self.submit([{"id": "old", "depends_on": []}], workflow_id="legacy")
        run = self.scheduler.start_run("acme", "legacy", run_id="legacy-1")
        del run["steps"]["old"]["kind"]
        del run["steps"]["old"]["approval"]
        self.store.save_run(run)
        reloaded = self.scheduler.get_run("acme", "legacy-1")
        self.assertEqual(reloaded["steps"]["old"]["kind"], "task")
        self.assertIsNone(reloaded["steps"]["old"]["approval"])
        self.assertEqual(self.scheduler.claim("acme", "legacy-1", "w1")["id"], "old")
        view = _run_view(reloaded)["steps"][0]
        self.assertEqual(view["kind"], "task")
        self.assertIsNone(view["approval"])


if __name__ == "__main__":
    unittest.main()
