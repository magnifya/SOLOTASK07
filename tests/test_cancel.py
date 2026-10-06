"""Tests for run-level cancellation (``Scheduler.cancel``).

A pending, running or still-sleeping run can be cancelled once: every
non-terminal node becomes ``cancelled`` in ``step_order`` (leases released,
``attempt``/``result``/``error`` kept), each cancelled node records a
``cancel`` event with its original holder and the actor, and a final
``run_cancelled`` event closes the transition.  Finished runs conflict, a
repeat cancel is a no-op, and replay rebuilds the identical document after
a restart.
"""

import shutil
import tempfile
import unittest

from flowd.model import WorkflowError
from flowd.scheduler import ConflictError, Scheduler
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


class CancelTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-cancel-")
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


class CancelValidationTest(CancelTestBase):
    def test_bad_tenant_rejected(self):
        self.submit()
        self.start()
        for tenant in (None, "", "   ", 42, ["acme"]):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.cancel(tenant, "r1", "ops")
            self.assertEqual(ctx.exception.code, "bad_tenant", tenant)

    def test_bad_actor_rejected(self):
        self.submit()
        self.start()
        for actor in (None, "", "   ", 7, {}):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.cancel("acme", "r1", actor)
            self.assertEqual(ctx.exception.code, "bad_actor", actor)

    def test_unknown_run(self):
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.cancel("acme", "nope", "ops")
        self.assertEqual(ctx.exception.code, "unknown_run")

    def test_cross_tenant(self):
        self.submit()
        self.start()
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.cancel("other", "r1", "ops")
        self.assertEqual(ctx.exception.code, "cross_tenant")
        # The run itself is untouched.
        self.assertEqual(self.scheduler.get_run("acme", "r1")["status"], "pending")

    def test_actor_is_trimmed(self):
        self.submit()
        self.start()
        run = self.scheduler.cancel("acme", "r1", "  ops  ")
        event = run["history"][-1]
        self.assertEqual(event["type"], "run_cancelled")
        self.assertEqual(event["actor"], "ops")


class CancelPendingRunTest(CancelTestBase):
    def test_cancel_pending_run_cancels_every_node_in_order(self):
        self.submit()
        self.start()
        run = self.scheduler.cancel("acme", "r1", "ops")
        self.assertEqual(run["status"], "cancelled")
        self.assertEqual([run["steps"][sid]["status"] for sid in run["step_order"]],
                         ["cancelled", "cancelled", "cancelled"])
        types = self.history_types()
        self.assertEqual(types, ["run_created", "ready",
                                 "cancel", "cancel", "cancel", "run_cancelled"])
        # The cancel events follow step_order and carry the actor.
        cancels = [e for e in run["history"] if e["type"] == "cancel"]
        self.assertEqual([e["step_id"] for e in cancels], ["step1", "step2", "step3"])
        self.assertTrue(all(e["actor"] == "ops" for e in cancels))
        self.assertTrue(all(e["worker_id"] is None for e in cancels))
        last = run["history"][-1]
        self.assertEqual(last["type"], "run_cancelled")
        self.assertIsNone(last["step_id"])
        self.assertEqual(last["actor"], "ops")
        # The audit stream follows the same order with the actor recorded.
        self.assertEqual(self.audit_actions(),
                         ["workflow.submit", "run_created", "ready",
                          "cancel", "cancel", "cancel", "run_cancelled"])
        items, _ = self.scheduler.list_audit("acme", action="cancel")
        self.assertTrue(all(item["actor"] == "ops" for item in items))

    def test_cancel_keeps_run_files_and_idempotency_binding(self):
        self.submit()
        self.start(idempotency_key="key-1")
        self.scheduler.cancel("acme", "r1", "ops")
        # The idempotency binding still replays to the same (cancelled) run.
        replayed = self.scheduler.start_run("acme", "wf", idempotency_key="key-1")
        self.assertEqual(replayed["run_id"], "r1")
        self.assertEqual(replayed["status"], "cancelled")
        self.assertEqual(self.scheduler.get_run("acme", "r1")["status"], "cancelled")


class CancelRunningRunTest(CancelTestBase):
    def test_cancel_releases_lease_and_keeps_attempt_result_error(self):
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.fail("acme", "r1", "step1", "w1", error="boom")
        # step1 is ready again with a pending backoff and attempt 1.
        self.clock.advance(5)
        self.scheduler.claim("acme", "r1", "w2", lease_seconds=60)
        self.clock.advance(5)
        run = self.scheduler.cancel("acme", "r1", "ops")
        step1 = run["steps"]["step1"]
        self.assertEqual(step1["status"], "cancelled")
        self.assertEqual(step1["attempt"], 1)
        self.assertEqual(step1["error"], "boom")
        self.assertIsNone(step1["worker_id"])
        self.assertIsNone(step1["lease_deadline"])
        self.assertIsNone(step1["next_attempt_at"])
        # The cancel event names the worker that held the lease.
        event = [e for e in run["history"]
                 if e["type"] == "cancel" and e["step_id"] == "step1"][0]
        self.assertEqual(event["worker_id"], "w2")
        self.assertEqual(event["actor"], "ops")
        self.assertEqual(event["attempt"], 1)

    def test_succeeded_nodes_are_left_untouched(self):
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r1", "step1", "w1", result={"rows": 3})
        run = self.scheduler.cancel("acme", "r1", "ops")
        step1 = run["steps"]["step1"]
        self.assertEqual(step1["status"], "succeeded")
        self.assertEqual(step1["result"], {"rows": 3})
        self.assertEqual(step1["worker_id"], "w1")
        self.assertEqual(run["status"], "cancelled")
        # No cancel event for the succeeded node.
        cancels = [e for e in run["history"] if e["type"] == "cancel"]
        self.assertEqual([e["step_id"] for e in cancels], ["step2", "step3"])

    def test_cancel_waiting_approval_node(self):
        self.submit(GATE)
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r1", "work", "w1")
        run = self.scheduler.cancel("acme", "r1", "ops")
        self.assertEqual(run["steps"]["ok"]["status"], "cancelled")
        self.assertIsNone(run["steps"]["ok"]["approval"])
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.decide("acme", "r1", "ok", "alice", "approve")
        self.assertEqual(ctx.exception.code, "run_cancelled")


class CancelSleepingRunTest(CancelTestBase):
    def test_sleeping_run_cancels_without_opening_roots(self):
        self.submit()
        self.start(not_before=self.clock.value + 3600)
        run = self.scheduler.cancel("acme", "r1", "ops")
        self.assertEqual(run["status"], "cancelled")
        # No ready/waiting events: the roots were never opened.
        self.assertEqual(self.history_types(),
                         ["run_created", "cancel", "cancel", "cancel", "run_cancelled"])
        # Once the start time passes, claims still hand out nothing and the
        # cancelled run is not activated.
        self.clock.advance(3601)
        self.assertIsNone(self.scheduler.claim("acme", "r1", "w1", lease_seconds=30))
        self.assertEqual(self.history_types(),
                         ["run_created", "cancel", "cancel", "cancel", "run_cancelled"])
        self.assertIsNone(self.scheduler.claim_fair("acme", "w1", lease_seconds=30))
        self.assertEqual(self.history_types(),
                         ["run_created", "cancel", "cancel", "cancel", "run_cancelled"])


class CancelFinishedRunTest(CancelTestBase):
    def test_succeeded_run_conflicts(self):
        self.submit()
        self.start()
        for sid in ("step1", "step2", "step3"):
            self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
            self.scheduler.complete("acme", "r1", sid, "w1")
        self.assertEqual(self.scheduler.get_run("acme", "r1")["status"], "succeeded")
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.cancel("acme", "r1", "ops")
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
            self.scheduler.cancel("acme", "r1", "ops")
        self.assertEqual(ctx.exception.code, "run_finished")


class CancelRepeatTest(CancelTestBase):
    def test_repeat_cancel_is_a_noop(self):
        self.submit()
        self.start()
        first = self.scheduler.cancel("acme", "r1", "ops")
        self.clock.advance(60)
        audit_before, _ = self.scheduler.list_audit("acme")
        second = self.scheduler.cancel("acme", "r1", "someone-else")
        self.assertEqual(second, first)
        self.assertEqual(second["updated_at"], first["updated_at"])
        self.assertEqual(len(second["history"]), len(first["history"]))
        audit_after, _ = self.scheduler.list_audit("acme")
        self.assertEqual(audit_after, audit_before)


class CancelBlocksWorkTest(CancelTestBase):
    def setUp(self):
        super().setUp()
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.cancel("acme", "r1", "ops")

    def test_claim_returns_none_without_takeover(self):
        self.assertIsNone(self.scheduler.claim("acme", "r1", "w2", lease_seconds=30))
        self.assertNotIn("takeover", self.history_types())
        self.assertIsNone(self.scheduler.claim_fair("acme", "w2", lease_seconds=30))

    def test_complete_is_rejected(self):
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.complete("acme", "r1", "step1", "w1")
        self.assertEqual(ctx.exception.code, "run_cancelled")

    def test_fail_is_rejected(self):
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.fail("acme", "r1", "step1", "w1", error="x")
        self.assertEqual(ctx.exception.code, "run_cancelled")

    def test_heartbeat_is_rejected(self):
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.heartbeat("acme", "r1", "step1", "w1", lease_seconds=30)
        self.assertEqual(ctx.exception.code, "run_cancelled")


class CancelReplayTest(CancelTestBase):
    def test_replay_rebuilds_cancelled_state(self):
        self.submit()
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r1", "step1", "w1", result={"rows": 3})
        self.scheduler.claim("acme", "r1", "w2", lease_seconds=60)
        self.scheduler.cancel("acme", "r1", "ops")
        stored = self.scheduler.get_run("acme", "r1")
        self.assertEqual(self.scheduler.replay("acme", "r1"), stored)

    def test_replay_agrees_after_restart(self):
        self.submit()
        self.start(not_before=self.clock.value + 3600)
        self.scheduler.cancel("acme", "r1", "ops")
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
        self.assertEqual([i["action"] for i in rest],
                         ["cancel", "cancel", "cancel", "run_cancelled"])


if __name__ == "__main__":
    unittest.main()
