"""Tests for run-level pause and resume (``Scheduler.pause``/``resume``).

A pending or running run can be paused once: every running task releases
its lease and goes back to ``ready`` (attempt kept, lease fields cleared),
each revoked task records a ``pause`` event in topological order, and a
run-level ``run_paused`` event carrying the actor closes the transition.
Completed nodes, waiting approvals and a not-yet-due delayed start are
preserved; claims hand out nothing and decisions conflict while paused.
Resume restores the derived ``pending``/``running`` status with a single
``run_resumed`` event and never leases, opens nodes or skips ``not_before``.
Repeats of both are no-ops, and replay rebuilds the identical document.
"""

import shutil
import tempfile
import unittest

from flowd.model import WorkflowError
from flowd.scheduler import ConflictError, LeaseError, Scheduler
from flowd.store import WorkflowStore

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
    {"id": "step3", "depends_on": ["step2"]},
]
GATE = [
    {"id": "work", "depends_on": []},
    {"id": "ok", "depends_on": ["work"], "kind": "approval"},
]


class FakeClock:
    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value


class PauseTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-pause-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def submit(self, steps=None, workflow_id="wf", tenant="acme"):
        return self.scheduler.submit(tenant, workflow_id, steps or LINEAR)

    def start(self, run_id="r1", tenant="acme", workflow_id="wf", **kwargs):
        return self.scheduler.start_run(tenant, workflow_id, run_id=run_id, **kwargs)

    def history_types(self, run_id="r1", tenant="acme"):
        return [e["type"] for e in self.scheduler.history(run_id, tenant)]

    def audit_actions(self, tenant="acme"):
        items, _ = self.scheduler.list_audit(tenant)
        return [item["action"] for item in items]


class PauseValidationTest(PauseTestBase):
    def test_bad_tenant_rejected(self):
        self.submit()
        self.start()
        for tenant in (None, "", "   ", 42, ["acme"]):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.pause(tenant, "r1", "ops")
            self.assertEqual(ctx.exception.code, "bad_tenant", tenant)
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.resume(tenant, "r1", "ops")
            self.assertEqual(ctx.exception.code, "bad_tenant", tenant)

    def test_bad_actor_rejected(self):
        self.submit()
        self.start()
        for actor in (None, "", "   ", 7, {}):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.pause("acme", "r1", actor)
            self.assertEqual(ctx.exception.code, "bad_actor", actor)
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.resume("acme", "r1", actor)
            self.assertEqual(ctx.exception.code, "bad_actor", actor)

    def test_unknown_run(self):
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.pause("acme", "nope", "ops")
        self.assertEqual(ctx.exception.code, "unknown_run")
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.resume("acme", "nope", "ops")
        self.assertEqual(ctx.exception.code, "unknown_run")

    def test_cross_tenant(self):
        self.submit()
        self.start()
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.pause("other", "r1", "ops")
        self.assertEqual(ctx.exception.code, "cross_tenant")
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.resume("other", "r1", "ops")
        self.assertEqual(ctx.exception.code, "cross_tenant")
        self.assertEqual(self.scheduler.get_run("acme", "r1")["status"], "pending")

    def test_actor_is_trimmed(self):
        self.submit()
        self.start()
        run = self.scheduler.pause("acme", "r1", "  ops  ")
        self.assertEqual(run["history"][-1]["actor"], "ops")
        run = self.scheduler.resume("acme", "r1", "  ops  ")
        self.assertEqual(run["history"][-1]["actor"], "ops")


class PausePendingRunTest(PauseTestBase):
    def test_pause_pending_run(self):
        self.submit()
        self.start()
        run = self.scheduler.pause("acme", "r1", "ops")
        self.assertEqual(run["status"], "paused")
        # No node is opened and no attempt is counted.
        self.assertEqual([run["steps"][sid]["status"] for sid in run["step_order"]],
                         ["ready", "pending", "pending"])
        self.assertTrue(all(run["steps"][sid]["attempt"] == 0 for sid in run["step_order"]))
        self.assertEqual(self.history_types(), ["run_created", "ready", "run_paused"])
        last = run["history"][-1]
        self.assertIsNone(last["step_id"])
        self.assertEqual(last["actor"], "ops")
        self.assertEqual(self.audit_actions(),
                         ["workflow.submit", "run_created", "ready", "run_paused"])
        items, _ = self.scheduler.list_audit("acme", action="run_paused")
        self.assertEqual(items[0]["actor"], "ops")

    def test_repeat_pause_is_a_noop(self):
        self.submit()
        self.start()
        first = self.scheduler.pause("acme", "r1", "ops")
        self.clock.advance(60)
        audit_before, _ = self.scheduler.list_audit("acme")
        second = self.scheduler.pause("acme", "r1", "someone-else")
        self.assertEqual(second, first)
        self.assertEqual(second["updated_at"], first["updated_at"])
        self.assertEqual(len(second["history"]), len(first["history"]))
        audit_after, _ = self.scheduler.list_audit("acme")
        self.assertEqual(audit_after, audit_before)


class PauseRunningRunTest(PauseTestBase):
    def test_pause_revokes_leases_in_topological_order(self):
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r1", "step1", "w1", result={"rows": 3})
        self.scheduler.claim("acme", "r1", "w2", lease_seconds=60)
        run = self.scheduler.pause("acme", "r1", "ops")
        # The completed node is preserved exactly.
        step1 = run["steps"]["step1"]
        self.assertEqual(step1["status"], "succeeded")
        self.assertEqual(step1["result"], {"rows": 3})
        self.assertEqual(step1["worker_id"], "w1")
        # The running task is back to ready with its lease revoked.
        step2 = run["steps"]["step2"]
        self.assertEqual(step2["status"], "ready")
        self.assertEqual(step2["attempt"], 0)
        self.assertIsNone(step2["worker_id"])
        self.assertIsNone(step2["lease_deadline"])
        self.assertIsNone(step2["next_attempt_at"])
        pauses = [e for e in run["history"] if e["type"] == "pause"]
        self.assertEqual([e["step_id"] for e in pauses], ["step2"])
        self.assertEqual(pauses[0]["worker_id"], "w2")
        self.assertEqual(pauses[0]["actor"], "ops")
        self.assertEqual(run["history"][-1]["type"], "run_paused")

    def test_pause_keeps_attempt_and_clears_retry_fields(self):
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.fail("acme", "r1", "step1", "w1", error="boom")
        self.clock.advance(5)
        self.scheduler.claim("acme", "r1", "w2", lease_seconds=60)
        run = self.scheduler.pause("acme", "r1", "ops")
        step1 = run["steps"]["step1"]
        self.assertEqual(step1["status"], "ready")
        self.assertEqual(step1["attempt"], 1)
        self.assertEqual(step1["error"], "boom")
        self.assertIsNone(step1["worker_id"])
        self.assertIsNone(step1["lease_deadline"])
        self.assertIsNone(step1["next_attempt_at"])

    def test_pause_releases_tenant_quota_immediately(self):
        self.submit()
        self.scheduler.set_quota("acme", 1)
        self.start(run_id="r1")
        self.start(run_id="r2")
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        # The shared budget is full: the second run cannot be claimed.
        self.assertIsNone(self.scheduler.claim("acme", "r2", "w2", lease_seconds=60))
        self.scheduler.pause("acme", "r1", "ops")
        # The revoked lease frees the budget at once.
        step = self.scheduler.claim("acme", "r2", "w2", lease_seconds=60)
        self.assertIsNotNone(step)
        self.assertEqual(step["id"], "step1")

    def test_pause_preserves_waiting_approval(self):
        self.submit(GATE)
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r1", "work", "w1")
        run = self.scheduler.pause("acme", "r1", "ops")
        self.assertEqual(run["steps"]["ok"]["status"], "waiting")
        self.assertIsNone(run["steps"]["ok"]["approval"])
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.decide("acme", "r1", "ok", "alice", "approve")
        self.assertEqual(ctx.exception.code, "run_paused")


class PauseSleepingRunTest(PauseTestBase):
    def test_sleeping_run_pauses_without_opening_roots(self):
        self.submit()
        self.start(not_before=self.clock.value + 3600)
        run = self.scheduler.pause("acme", "r1", "ops")
        self.assertEqual(run["status"], "paused")
        self.assertEqual(self.history_types(), ["run_created", "run_paused"])
        # Resume does not open nodes or skip the delayed start either.
        run = self.scheduler.resume("acme", "r1", "ops")
        self.assertEqual(run["status"], "pending")
        self.assertEqual(self.history_types(),
                         ["run_created", "run_paused", "run_resumed"])
        self.assertIsNone(self.scheduler.claim("acme", "r1", "w1", lease_seconds=30))
        # Once the start time passes, a due claim activates the run.
        self.clock.advance(3601)
        step = self.scheduler.claim("acme", "r1", "w1", lease_seconds=30)
        self.assertEqual(step["id"], "step1")


class PauseFinishedRunTest(PauseTestBase):
    def test_succeeded_run_conflicts(self):
        self.submit()
        self.start()
        for sid in ("step1", "step2", "step3"):
            self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
            self.scheduler.complete("acme", "r1", sid, "w1")
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.pause("acme", "r1", "ops")
        self.assertEqual(ctx.exception.code, "run_finished")

    def test_failed_run_conflicts(self):
        self.submit()
        self.start()
        for _ in range(3):
            self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
            self.scheduler.fail("acme", "r1", "step1", "w1", error="x")
            self.clock.advance(10)
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.pause("acme", "r1", "ops")
        self.assertEqual(ctx.exception.code, "run_finished")

    def test_cancelled_run_conflicts(self):
        self.submit()
        self.start()
        self.scheduler.cancel("acme", "r1", "ops")
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.pause("acme", "r1", "ops")
        self.assertEqual(ctx.exception.code, "run_finished")


class PauseBlocksWorkTest(PauseTestBase):
    def setUp(self):
        super().setUp()
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.pause("acme", "r1", "ops")

    def test_claim_returns_none_without_takeover(self):
        self.assertIsNone(self.scheduler.claim("acme", "r1", "w2", lease_seconds=30))
        self.assertNotIn("takeover", self.history_types())
        self.assertIsNone(self.scheduler.claim_fair("acme", "w2", lease_seconds=30))

    def test_complete_is_rejected(self):
        with self.assertRaises(LeaseError):
            self.scheduler.complete("acme", "r1", "step1", "w1")

    def test_fail_is_rejected(self):
        with self.assertRaises(LeaseError):
            self.scheduler.fail("acme", "r1", "step1", "w1", error="x")

    def test_heartbeat_is_rejected(self):
        with self.assertRaises(LeaseError):
            self.scheduler.heartbeat("acme", "r1", "step1", "w1", lease_seconds=30)

    def test_cancel_still_works(self):
        run = self.scheduler.cancel("acme", "r1", "ops")
        self.assertEqual(run["status"], "cancelled")
        self.assertEqual([run["steps"][sid]["status"] for sid in run["step_order"]],
                         ["cancelled", "cancelled", "cancelled"])


class ResumeTest(PauseTestBase):
    def test_resume_restores_schedulable_state(self):
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.pause("acme", "r1", "ops")
        self.clock.advance(10)
        run = self.scheduler.resume("acme", "r1", "ops")
        # The revoked task is ready again; nothing is leased automatically.
        self.assertIn(run["status"], ("pending", "running"))
        step1 = run["steps"]["step1"]
        self.assertEqual(step1["status"], "ready")
        self.assertIsNone(step1["worker_id"])
        self.assertEqual(run["history"][-1]["type"], "run_resumed")
        self.assertEqual(run["history"][-1]["actor"], "ops")
        # A later claim picks the task up again.
        step = self.scheduler.claim("acme", "r1", "w2", lease_seconds=60)
        self.assertEqual(step["id"], "step1")
        self.assertEqual(step["attempt"], 0)

    def test_resume_after_partial_progress(self):
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r1", "step1", "w1")
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.pause("acme", "r1", "ops")
        run = self.scheduler.resume("acme", "r1", "ops")
        self.assertEqual(run["status"], "running")
        self.assertEqual(run["steps"]["step1"]["status"], "succeeded")
        self.assertEqual(run["steps"]["step2"]["status"], "ready")
        # No node was re-opened: exactly one ready event per unlocked node.
        self.assertEqual(self.history_types().count("ready"), 2)

    def test_resume_not_paused_conflicts(self):
        self.submit()
        self.start()
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.resume("acme", "r1", "ops")
        self.assertEqual(ctx.exception.code, "not_paused")

    def test_repeat_resume_is_a_noop(self):
        self.submit()
        self.start()
        self.scheduler.pause("acme", "r1", "ops")
        first = self.scheduler.resume("acme", "r1", "ops")
        self.clock.advance(60)
        audit_before, _ = self.scheduler.list_audit("acme")
        second = self.scheduler.resume("acme", "r1", "someone-else")
        self.assertEqual(second, first)
        self.assertEqual(second["updated_at"], first["updated_at"])
        self.assertEqual(len(second["history"]), len(first["history"]))
        audit_after, _ = self.scheduler.list_audit("acme")
        self.assertEqual(audit_after, audit_before)

    def test_fair_claim_skips_paused_runs(self):
        self.submit()
        self.start(run_id="r1")
        self.start(run_id="r2")
        self.scheduler.pause("acme", "r1", "ops")
        # Only the unpaused run is eligible for the tenant-wide claim.
        run_id, step = self.scheduler.claim_fair("acme", "w1", lease_seconds=30)
        self.assertEqual(run_id, "r2")
        self.scheduler.resume("acme", "r1", "ops")
        run_id, step = self.scheduler.claim_fair("acme", "w1", lease_seconds=30)
        self.assertEqual(run_id, "r1")


class PauseReplayTest(PauseTestBase):
    def test_replay_rebuilds_paused_state(self):
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r1", "step1", "w1", result={"rows": 3})
        self.scheduler.claim("acme", "r1", "w2", lease_seconds=60)
        self.scheduler.pause("acme", "r1", "ops")
        stored = self.scheduler.get_run("acme", "r1")
        self.assertEqual(stored["status"], "paused")
        self.assertEqual(self.scheduler.replay("acme", "r1"), stored)

    def test_replay_rebuilds_resumed_state(self):
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.pause("acme", "r1", "ops")
        self.scheduler.resume("acme", "r1", "ops")
        self.scheduler.claim("acme", "r1", "w2", lease_seconds=60)
        stored = self.scheduler.get_run("acme", "r1")
        self.assertEqual(self.scheduler.replay("acme", "r1"), stored)

    def test_replay_rebuilds_pause_then_cancel(self):
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.pause("acme", "r1", "ops")
        self.scheduler.cancel("acme", "r1", "ops")
        stored = self.scheduler.get_run("acme", "r1")
        self.assertEqual(stored["status"], "cancelled")
        self.assertEqual(self.scheduler.replay("acme", "r1"), stored)

    def test_replay_agrees_after_restart(self):
        self.submit()
        self.start(not_before=self.clock.value + 3600)
        self.scheduler.pause("acme", "r1", "ops")
        stored = self.scheduler.get_run("acme", "r1")
        other = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        self.assertEqual(other.get_run("acme", "r1"), stored)
        self.assertEqual(other.replay("acme", "r1"), stored)
        # The audit stream only gained the pause actions, tenant-isolated.
        items, _ = other.list_audit("acme")
        self.assertEqual([i["action"] for i in items],
                         ["workflow.submit", "run_created", "run_paused"])
        other_items, _ = other.list_audit("other")
        self.assertEqual(other_items, [])


if __name__ == "__main__":
    unittest.main()
