"""Tests for run-level pause and resume (``Scheduler.pause``/``resume``).

A pending or running run can be paused: running tasks have their leases
revoked and go back to ``ready`` (attempt preserved, quota released), one
``pause`` event per revoked task in topological order plus a run-level
``run_paused``.  While paused, claims hand out nothing, decisions conflict
and cancel still works.  ``resume`` flips the run back to the derived
``pending``/``running`` state with one ``run_resumed`` event; it never
claims, never bypasses ``not_before`` and never opens new nodes.  Repeats
of both operations are no-ops, finished runs conflict, and replay rebuilds
the identical document after a restart.
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
        # The run itself is untouched.
        self.assertEqual(self.scheduler.get_run("acme", "r1")["status"], "pending")

    def test_actor_is_trimmed(self):
        self.submit()
        self.start()
        run = self.scheduler.pause("acme", "r1", "  ops  ")
        self.assertEqual(run["history"][-1]["actor"], "ops")
        run = self.scheduler.resume("acme", "r1", "  ops  ")
        self.assertEqual(run["history"][-1]["actor"], "ops")


class PausePendingRunTest(PauseTestBase):
    def test_pause_pending_run_appends_only_run_paused(self):
        self.submit()
        self.start()
        run = self.scheduler.pause("acme", "r1", "ops")
        self.assertEqual(run["status"], "paused")
        # No running tasks, so no step-level pause events; nodes untouched.
        self.assertEqual([run["steps"][sid]["status"] for sid in run["step_order"]],
                         ["ready", "pending", "pending"])
        self.assertEqual(self.history_types(), ["run_created", "ready", "run_paused"])
        last = run["history"][-1]
        self.assertEqual(last["type"], "run_paused")
        self.assertIsNone(last["step_id"])
        self.assertEqual(last["actor"], "ops")
        self.assertEqual(self.audit_actions(),
                         ["workflow.submit", "run_created", "ready", "run_paused"])
        items, _ = self.scheduler.list_audit("acme", action="run_paused")
        self.assertEqual([item["actor"] for item in items], ["ops"])

    def test_pause_sleeping_run_keeps_roots_unopened(self):
        self.submit()
        self.start(not_before=self.clock.value + 3600)
        run = self.scheduler.pause("acme", "r1", "ops")
        self.assertEqual(run["status"], "paused")
        self.assertEqual(self.history_types(), ["run_created", "run_paused"])
        self.assertTrue(all(s["status"] == "pending" for s in run["steps"].values()))
        # Resuming does not activate the run before its start time either.
        run = self.scheduler.resume("acme", "r1", "ops")
        self.assertEqual(run["status"], "pending")
        self.assertEqual(self.history_types(),
                         ["run_created", "run_paused", "run_resumed"])
        self.clock.advance(3601)
        # A due claim activates the roots normally after the resume.
        step = self.scheduler.claim("acme", "r1", "w1", lease_seconds=30)
        self.assertEqual(step["id"], "step1")


class PauseRunningRunTest(PauseTestBase):
    def test_pause_revokes_leases_in_order_and_keeps_attempt(self):
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.fail("acme", "r1", "step1", "w1", error="boom")
        self.clock.advance(5)
        self.scheduler.claim("acme", "r1", "w2", lease_seconds=60)
        self.clock.advance(5)
        run = self.scheduler.pause("acme", "r1", "ops")
        self.assertEqual(run["status"], "paused")
        step1 = run["steps"]["step1"]
        self.assertEqual(step1["status"], "ready")
        self.assertEqual(step1["attempt"], 1)
        self.assertEqual(step1["error"], "boom")
        self.assertIsNone(step1["worker_id"])
        self.assertIsNone(step1["lease_deadline"])
        self.assertIsNone(step1["next_attempt_at"])
        # One pause event for the revoked task, naming holder and actor.
        pauses = [e for e in run["history"] if e["type"] == "pause"]
        self.assertEqual([(e["step_id"], e["worker_id"], e["actor"], e["attempt"])
                          for e in pauses], [("step1", "w2", "ops", 1)])
        self.assertEqual(run["history"][-1]["type"], "run_paused")

    def test_pause_revokes_every_running_task_in_topological_order(self):
        self.submit([
            {"id": "a", "depends_on": []},
            {"id": "b", "depends_on": []},
            {"id": "c", "depends_on": ["a", "b"]},
        ])
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.claim("acme", "r1", "w2", lease_seconds=60)
        run = self.scheduler.pause("acme", "r1", "ops")
        self.assertEqual([run["steps"][sid]["status"] for sid in ("a", "b", "c")],
                         ["ready", "ready", "pending"])
        pauses = [e for e in run["history"] if e["type"] == "pause"]
        self.assertEqual([e["step_id"] for e in pauses], ["a", "b"])
        self.assertEqual([e["worker_id"] for e in pauses], ["w1", "w2"])

    def test_pause_keeps_succeeded_nodes_and_waiting_approvals(self):
        self.submit(GATE)
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r1", "work", "w1", result={"rows": 3})
        run = self.scheduler.pause("acme", "r1", "ops")
        self.assertEqual(run["steps"]["work"]["status"], "succeeded")
        self.assertEqual(run["steps"]["work"]["result"], {"rows": 3})
        self.assertEqual(run["steps"]["ok"]["status"], "waiting")
        self.assertNotIn("pause", self.history_types())
        self.assertEqual(run["history"][-1]["type"], "run_paused")

    def test_pause_releases_tenant_quota_immediately(self):
        self.submit()
        self.start()
        self.submit(workflow_id="wf2")
        self.start(run_id="r2", workflow_id="wf2")
        self.scheduler.set_quota("acme", 1)
        self.assertIsNotNone(self.scheduler.claim("acme", "r1", "w1", lease_seconds=60))
        # The single slot is taken: no other run can lease.
        self.assertIsNone(self.scheduler.claim("acme", "r2", "w2", lease_seconds=60))
        self.scheduler.pause("acme", "r1", "ops")
        # The slot was released with the lease.
        step = self.scheduler.claim("acme", "r2", "w2", lease_seconds=60)
        self.assertIsNotNone(step)
        self.assertEqual(step["id"], "step1")


class PauseFinishedRunTest(PauseTestBase):
    def test_succeeded_run_conflicts(self):
        self.submit()
        self.start()
        for sid in ("step1", "step2", "step3"):
            self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
            self.scheduler.complete("acme", "r1", sid, "w1")
        self.assertEqual(self.scheduler.get_run("acme", "r1")["status"], "succeeded")
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
        self.assertEqual(self.scheduler.get_run("acme", "r1")["status"], "failed")
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


class PauseRepeatTest(PauseTestBase):
    def test_repeat_pause_is_a_noop(self):
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        first = self.scheduler.pause("acme", "r1", "ops")
        self.clock.advance(60)
        audit_before, _ = self.scheduler.list_audit("acme")
        second = self.scheduler.pause("acme", "r1", "someone-else")
        self.assertEqual(second, first)
        self.assertEqual(second["updated_at"], first["updated_at"])
        self.assertEqual(len(second["history"]), len(first["history"]))
        audit_after, _ = self.scheduler.list_audit("acme")
        self.assertEqual(audit_after, audit_before)


class PauseBlocksWorkTest(PauseTestBase):
    def setUp(self):
        super().setUp()
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.pause("acme", "r1", "ops")

    def test_claim_returns_none_without_events(self):
        before = self.scheduler.get_run("acme", "r1")
        self.assertIsNone(self.scheduler.claim("acme", "r1", "w2", lease_seconds=30))
        self.assertIsNone(self.scheduler.claim_fair("acme", "w2", lease_seconds=30))
        after = self.scheduler.get_run("acme", "r1")
        self.assertEqual(after, before)

    def test_fair_claim_skips_paused_run_for_other_runs(self):
        self.submit(workflow_id="wf2")
        self.start(run_id="r2", workflow_id="wf2")
        result = self.scheduler.claim_fair("acme", "w2", lease_seconds=30)
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "r2")

    def test_complete_is_rejected(self):
        with self.assertRaises(LeaseError):
            self.scheduler.complete("acme", "r1", "step1", "w1")

    def test_fail_is_rejected(self):
        with self.assertRaises(LeaseError):
            self.scheduler.fail("acme", "r1", "step1", "w1", error="x")

    def test_heartbeat_is_rejected(self):
        with self.assertRaises(LeaseError):
            self.scheduler.heartbeat("acme", "r1", "step1", "w1", lease_seconds=30)

    def test_decision_conflicts_run_paused(self):
        self.submit(GATE, workflow_id="gate")
        self.start(run_id="r2", workflow_id="gate")
        self.scheduler.claim("acme", "r2", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r2", "work", "w1")
        self.scheduler.pause("acme", "r2", "ops")
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.decide("acme", "r2", "ok", "alice", "approve")
        self.assertEqual(ctx.exception.code, "run_paused")

    def test_cancel_still_works(self):
        run = self.scheduler.cancel("acme", "r1", "ops")
        self.assertEqual(run["status"], "cancelled")
        # A cancelled run can no longer be resumed.
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.resume("acme", "r1", "ops")
        self.assertEqual(ctx.exception.code, "not_paused")


class ResumeTest(PauseTestBase):
    def test_resume_restores_schedulable_state(self):
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r1", "step1", "w1")
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.pause("acme", "r1", "ops")
        run = self.scheduler.resume("acme", "r1", "ops")
        # step1 succeeded and step2's lease was revoked: derived as running.
        self.assertEqual(run["status"], "running")
        self.assertEqual(run["history"][-1]["type"], "run_resumed")
        self.assertEqual(run["history"][-1]["actor"], "ops")
        self.assertIsNone(run["history"][-1]["step_id"])
        # The revoked task is claimable again with its attempt preserved.
        step = self.scheduler.claim("acme", "r1", "w2", lease_seconds=60)
        self.assertEqual(step["id"], "step2")
        self.assertEqual(step["attempt"], 0)

    def test_resume_pending_run_returns_to_pending(self):
        self.submit()
        self.start()
        self.scheduler.pause("acme", "r1", "ops")
        run = self.scheduler.resume("acme", "r1", "ops")
        self.assertEqual(run["status"], "pending")
        self.assertEqual(self.history_types(),
                         ["run_created", "ready", "run_paused", "run_resumed"])
        self.assertEqual(self.audit_actions(),
                         ["workflow.submit", "run_created", "ready",
                          "run_paused", "run_resumed"])

    def test_resume_does_not_open_nodes_or_claim(self):
        self.submit()
        self.start(not_before=self.clock.value + 3600)
        self.scheduler.pause("acme", "r1", "ops")
        self.clock.advance(3601)
        run = self.scheduler.resume("acme", "r1", "ops")
        # Past not_before now, but resume itself opens nothing and claims
        # nothing: activation still needs a claim.
        self.assertEqual(self.history_types(),
                         ["run_created", "run_paused", "run_resumed"])
        self.assertTrue(all(s["status"] == "pending" for s in run["steps"].values()))
        step = self.scheduler.claim("acme", "r1", "w1", lease_seconds=30)
        self.assertEqual(step["id"], "step1")

    def test_not_paused_conflicts(self):
        self.submit()
        self.start()
        for run_id in ("r1",):
            with self.assertRaises(ConflictError) as ctx:
                self.scheduler.resume("acme", run_id, "ops")
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

    def test_pause_resume_pause_again(self):
        self.submit()
        self.start()
        self.scheduler.pause("acme", "r1", "ops")
        self.scheduler.resume("acme", "r1", "ops")
        run = self.scheduler.pause("acme", "r1", "ops")
        self.assertEqual(run["status"], "paused")
        self.assertEqual(self.history_types(),
                         ["run_created", "ready", "run_paused", "run_resumed",
                          "run_paused"])
        run = self.scheduler.resume("acme", "r1", "ops")
        self.assertEqual(run["status"], "pending")


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
        stored = self.scheduler.get_run("acme", "r1")
        self.assertEqual(self.scheduler.replay("acme", "r1"), stored)
        # And the run is executable again after the resume.
        step = self.scheduler.claim("acme", "r1", "w2", lease_seconds=60)
        self.assertEqual(step["id"], "step1")

    def test_replay_agrees_after_restart(self):
        self.submit()
        self.start(not_before=self.clock.value + 3600)
        self.scheduler.pause("acme", "r1", "ops")
        stored = self.scheduler.get_run("acme", "r1")
        # A fresh store + scheduler over the same directory sees the same
        # document and replays it identically.
        other = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        self.assertEqual(other.get_run("acme", "r1"), stored)
        self.assertEqual(other.replay("acme", "r1"), stored)
        # Audit pagination is stable across the restart as well.
        items, next_after = other.list_audit("acme", limit=2)
        self.assertEqual([i["action"] for i in items], ["workflow.submit", "run_created"])
        rest, end = other.list_audit("acme", after=next_after)
        self.assertIsNone(end)
        self.assertEqual([i["action"] for i in rest], ["run_paused"])
        # Tenant isolation: the other tenant's stream stays empty.
        items, _ = other.list_audit("other")
        self.assertEqual(items, [])


if __name__ == "__main__":
    unittest.main()
