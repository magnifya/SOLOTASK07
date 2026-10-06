"""Tests for run-level cancellation: state machine, history, audit, replay."""

import shutil
import tempfile
import unittest

from flowd.model import WorkflowError
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


LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
    {"id": "step3", "depends_on": ["step2"]},
]


class CancelTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-cancel-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def start(self, steps=None, workflow_id="wf", run_id="r1", tenant="acme", **kw):
        self.scheduler.submit(tenant, workflow_id, LINEAR if steps is None else steps)
        return self.scheduler.start_run(tenant, workflow_id, run_id=run_id, **kw)

    def cancel(self, run_id="r1", tenant="acme", actor="ops"):
        return self.scheduler.cancel(tenant, run_id, actor)


class CancelValidationTest(CancelTestBase):
    def test_bad_tenant(self):
        self.start()
        for bad in (None, "", "   ", 3, True, ["acme"]):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.cancel(bad, "r1", "ops")
            self.assertEqual(ctx.exception.code, "bad_tenant", "value %r" % (bad,))

    def test_bad_actor(self):
        self.start()
        for bad in (None, "", "   ", 3, True, ["ops"]):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.cancel("acme", "r1", bad)
            self.assertEqual(ctx.exception.code, "bad_actor", "value %r" % (bad,))

    def test_tenant_and_actor_are_trimmed(self):
        self.start()
        run = self.scheduler.cancel("  acme  ", "r1", "  ops  ")
        self.assertEqual(run["status"], "cancelled")
        event = run["history"][-1]
        self.assertEqual(event["type"], "run_cancelled")
        self.assertEqual(event["actor"], "ops")

    def test_unknown_run(self):
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.cancel("acme", "ghost", "ops")
        self.assertEqual(ctx.exception.code, "unknown_run")

    def test_cross_tenant(self):
        self.start()
        self.scheduler.submit("other", "wf", LINEAR)
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.cancel("other", "r1", "ops")
        self.assertEqual(ctx.exception.code, "cross_tenant")


class CancelLifecycleTest(CancelTestBase):
    def test_cancel_pending_run(self):
        self.start()
        run = self.cancel()
        self.assertEqual(run["status"], "cancelled")
        for sid in ("step1", "step2", "step3"):
            step = run["steps"][sid]
            self.assertEqual(step["status"], "cancelled")
            self.assertIsNone(step["worker_id"])
            self.assertIsNone(step["lease_deadline"])
            self.assertIsNone(step["next_attempt_at"])
            self.assertEqual(step["attempt"], 0)
            self.assertIsNone(step["result"])
            self.assertIsNone(step["error"])
        types = [e["type"] for e in run["history"]]
        self.assertEqual(types, ["run_created", "ready", "cancel", "cancel",
                                 "cancel", "run_cancelled"])
        cancels = [e for e in run["history"] if e["type"] == "cancel"]
        self.assertEqual([e["step_id"] for e in cancels], ["step1", "step2", "step3"])
        for event in cancels:
            self.assertIsNone(event["worker_id"])
            self.assertEqual(event["actor"], "ops")
        last = run["history"][-1]
        self.assertIsNone(last["step_id"])
        self.assertEqual(last["actor"], "ops")

    def test_cancel_running_run_clears_lease_but_keeps_attempt(self):
        self.start()
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=30)
        run = self.cancel(actor="admin")
        step = run["steps"]["step1"]
        self.assertEqual(step["status"], "cancelled")
        self.assertIsNone(step["worker_id"])
        self.assertIsNone(step["lease_deadline"])
        self.assertIsNone(step["next_attempt_at"])
        cancel_event = [e for e in run["history"]
                        if e["type"] == "cancel" and e["step_id"] == "step1"][0]
        self.assertEqual(cancel_event["worker_id"], "w1")
        self.assertEqual(cancel_event["actor"], "admin")

    def test_cancel_keeps_succeeded_nodes_and_failure_state(self):
        self.start()
        self.scheduler.claim("acme", "r1", "w1")
        self.scheduler.complete("acme", "r1", "step1", "w1", result={"rows": 3})
        self.scheduler.claim("acme", "r1", "w1")
        self.scheduler.fail("acme", "r1", "step2", "w1", error="boom")
        run = self.cancel()
        step1 = run["steps"]["step1"]
        self.assertEqual(step1["status"], "succeeded")
        self.assertEqual(step1["result"], {"rows": 3})
        self.assertEqual(step1["worker_id"], "w1")
        step2 = run["steps"]["step2"]
        self.assertEqual(step2["status"], "cancelled")
        self.assertEqual(step2["attempt"], 1)
        self.assertEqual(step2["error"], "boom")
        self.assertIsNone(step2["next_attempt_at"])
        # No cancel event for the already-succeeded node.
        cancelled = [e["step_id"] for e in run["history"] if e["type"] == "cancel"]
        self.assertEqual(cancelled, ["step2", "step3"])

    def test_cancel_waiting_approval_node(self):
        self.start([{"id": "gate", "depends_on": [], "kind": "approval"}])
        run = self.cancel()
        self.assertEqual(run["steps"]["gate"]["status"], "cancelled")
        self.assertIsNone(run["steps"]["gate"]["approval"])

    def test_cancel_sleeping_run_does_not_open_roots(self):
        self.start(not_before=self.clock.value + 3600)
        run = self.cancel()
        self.assertEqual(run["status"], "cancelled")
        types = [e["type"] for e in run["history"]]
        self.assertNotIn("ready", types)
        self.assertNotIn("waiting", types)
        self.assertEqual(types, ["run_created", "cancel", "cancel", "cancel",
                                 "run_cancelled"])

    def test_repeat_cancel_is_a_noop(self):
        self.start()
        first = self.cancel()
        self.clock.advance(60)
        second = self.cancel(actor="someone-else")
        self.assertEqual(second["updated_at"], first["updated_at"])
        self.assertEqual(len(second["history"]), len(first["history"]))
        audit_before = self.store.load_audit("acme")
        self.cancel()
        self.assertEqual(self.store.load_audit("acme"), audit_before)

    def test_finished_run_conflicts(self):
        self.start()
        for _ in range(3):
            self.scheduler.claim("acme", "r1", "w1")
            run = self.scheduler.fail("acme", "r1", "step1", "w1", error="x")
            self.clock.advance(10)
        self.assertEqual(run["status"], "failed")
        with self.assertRaises(ConflictError) as ctx:
            self.cancel()
        self.assertEqual(ctx.exception.code, "run_finished")

    def test_succeeded_run_conflicts(self):
        self.start([{"id": "only", "depends_on": []}])
        self.scheduler.claim("acme", "r1", "w1")
        run = self.scheduler.complete("acme", "r1", "only", "w1")
        self.assertEqual(run["status"], "succeeded")
        with self.assertRaises(ConflictError) as ctx:
            self.cancel()
        self.assertEqual(ctx.exception.code, "run_finished")

    def test_audit_stream_records_cancel_in_order(self):
        self.start()
        self.scheduler.claim("acme", "r1", "w1")
        self.cancel(actor="admin")
        tail = self.store.load_audit("acme")[-4:]
        self.assertEqual([r["action"] for r in tail],
                         ["cancel", "cancel", "cancel", "run_cancelled"])
        self.assertEqual([r["step_id"] for r in tail],
                         ["step1", "step2", "step3", None])
        self.assertTrue(all(r["actor"] == "admin" for r in tail))
        self.assertEqual([r["worker_id"] for r in tail], ["w1", None, None, None])


class CancelInteractionTest(CancelTestBase):
    def setUp(self):
        super().setUp()
        self.start()
        self.scheduler.claim("acme", "r1", "w1")
        self.cancel()

    def test_claim_returns_none_without_takeover(self):
        history_len = len(self.scheduler.get_run("acme", "r1")["history"])
        self.assertIsNone(self.scheduler.claim("acme", "r1", "w2"))
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(len(run["history"]), history_len)
        self.assertNotIn("takeover", [e["type"] for e in run["history"]])

    def test_claim_fair_skips_cancelled_run(self):
        self.assertIsNone(self.scheduler.claim_fair("acme", "w2"))

    def test_complete_fail_heartbeat_decision_conflict(self):
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.complete("acme", "r1", "step1", "w1")
        self.assertEqual(ctx.exception.code, "run_cancelled")
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.fail("acme", "r1", "step1", "w1", error="x")
        self.assertEqual(ctx.exception.code, "run_cancelled")
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.heartbeat("acme", "r1", "step1", "w1")
        self.assertEqual(ctx.exception.code, "run_cancelled")

    def test_decision_on_cancelled_run_conflicts(self):
        self.start([{"id": "gate", "depends_on": [], "kind": "approval"}],
                   workflow_id="wf2", run_id="r2")
        self.cancel(run_id="r2")
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.decide("acme", "r2", "gate", "alice", "approve")
        self.assertEqual(ctx.exception.code, "run_cancelled")

    def test_list_and_get_show_cancelled(self):
        items, _ = self.scheduler.list_runs("acme", status="cancelled")
        self.assertEqual([r["run_id"] for r in items], ["r1"])
        items, _ = self.scheduler.list_runs("acme", status="running")
        self.assertEqual(items, [])

    def test_idempotency_binding_survives_cancel(self):
        self.scheduler.submit("acme", "wf3", LINEAR)
        self.scheduler.start_run("acme", "wf3", run_id="r3", idempotency_key="k1")
        self.scheduler.cancel("acme", "r3", "ops")
        replayed = self.scheduler.start_run("acme", "wf3", run_id="r3",
                                            idempotency_key="k1")
        self.assertEqual(replayed["status"], "cancelled")
        self.assertEqual(replayed["run_id"], "r3")


class CancelReplayTest(CancelTestBase):
    def test_replay_rebuilds_cancelled_state(self):
        self.start()
        self.scheduler.claim("acme", "r1", "w1")
        self.scheduler.complete("acme", "r1", "step1", "w1", result=1)
        self.scheduler.claim("acme", "r1", "w1")
        stored = self.cancel(actor="admin")
        rebuilt = self.scheduler.replay("acme", "r1")
        self.assertEqual(rebuilt, stored)
        self.assertEqual(rebuilt["status"], "cancelled")
        self.assertEqual(rebuilt["steps"]["step1"]["status"], "succeeded")
        self.assertEqual(rebuilt["steps"]["step2"]["status"], "cancelled")
        self.assertEqual(rebuilt["steps"]["step3"]["status"], "cancelled")

    def test_replay_after_restart(self):
        self.start(not_before=self.clock.value + 3600)
        stored = self.cancel()
        fresh = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        rebuilt = fresh.replay("acme", "r1")
        self.assertEqual(rebuilt, stored)
        self.assertEqual(rebuilt["status"], "cancelled")
        loaded = fresh.get_run("acme", "r1")
        self.assertEqual(loaded["updated_at"], stored["updated_at"])
        audit = [r["action"] for r in fresh.list_audit("acme")[0]]
        self.assertEqual(audit[-4:], ["cancel", "cancel", "cancel", "run_cancelled"])


if __name__ == "__main__":
    unittest.main()
