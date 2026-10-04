"""Tests for the run state machine, leases, retries, history and replay."""

import os
import shutil
import tempfile
import unittest

from flowd.model import WorkflowError
from flowd.scheduler import ConflictError, LeaseError, Scheduler, backoff_seconds
from flowd.store import WorkflowStore

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
    {"id": "step3", "depends_on": ["step2"]},
]

# a -- approve -- b, plus an independent task c
APPROVAL_PLAN = [
    {"id": "a", "depends_on": []},
    {"id": "g", "depends_on": ["a"], "kind": "approval"},
    {"id": "b", "depends_on": ["g"]},
    {"id": "c", "depends_on": []},
]


class FakeClock:
    """Injectible deterministic clock; sleeping advances virtual time."""

    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value

    def sleep(self, seconds):
        self.advance(seconds)


class SchedulerTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-test-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock, backoff_base=1.0)

    def workflow(self, workflow_id="etl", steps=None):
        return self.scheduler.submit("acme", workflow_id, steps or LINEAR)

    def claim(self, run_id, worker="w1", lease=30, tenant="acme"):
        return self.scheduler.claim(tenant, run_id, worker, lease)

    def complete(self, run_id, step_id, worker="w1", result=None, tenant="acme"):
        return self.scheduler.complete(tenant, run_id, step_id, worker, result)

    def finish_next(self, run_id, worker="w1"):
        """Claim and complete the next ready step."""
        step = self.claim(run_id, worker)
        return step, self.complete(run_id, step["id"], worker)


class StateMachineTest(SchedulerTestBase):
    def test_step_becomes_ready_only_after_dependencies_succeed(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "etl")
        self.assertEqual(run["status"], "pending")
        self.assertEqual([s["status"] for s in run["steps"].values()],
                         ["ready", "pending", "pending"])

        self.assertEqual(self.claim(run["run_id"])["id"], "step1")
        self.assertIsNone(self.claim(run["run_id"]))
        run = self.complete(run["run_id"], "step1", result={"rows": 3})
        self.assertEqual(run["status"], "running")
        self.assertEqual(run["steps"]["step2"]["status"], "ready")
        self.assertEqual(run["steps"]["step3"]["status"], "pending")

        for sid in ("step2", "step3"):
            _, run = self.finish_next(run["run_id"])
            self.assertEqual(run["steps"][sid]["status"], "succeeded")
        self.assertEqual(run["status"], "succeeded")
        self.assertTrue(all(s["status"] == "succeeded" for s in run["steps"].values()))

    def test_claim_orders_ready_steps_deterministically(self):
        self.workflow("fan", [{"id": "b", "depends_on": []}, {"id": "a", "depends_on": []}])
        run = self.scheduler.start_run("acme", "fan")
        self.assertEqual(self.claim(run["run_id"])["id"], "a")
        self.assertEqual(self.claim(run["run_id"])["id"], "b")
        self.assertIsNone(self.claim(run["run_id"]))

    def test_at_most_one_active_lease_per_step(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "etl")
        self.assertEqual(self.claim(run["run_id"], "w1")["id"], "step1")
        self.assertIsNone(self.claim(run["run_id"], "w2"))

    def test_complete_is_idempotent_for_same_worker(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "etl")
        self.claim(run["run_id"])
        first = self.complete(run["run_id"], "step1", result={"rows": 1})
        second = self.complete(run["run_id"], "step1", result={"rows": 1})
        self.assertEqual(second["steps"]["step1"]["status"], first["steps"]["step1"]["status"])
        self.assertEqual(second["steps"]["step1"]["attempt"], 1)
        self.assertEqual(len(second["history"]), len(first["history"]))
        self.assertEqual([e["type"] for e in second["history"]].count("complete"), 1)

    def test_complete_without_lease_is_rejected(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "etl")
        with self.assertRaises(LeaseError):
            self.complete(run["run_id"], "step1")
        self.claim(run["run_id"], "w1")
        with self.assertRaises(LeaseError):
            self.complete(run["run_id"], "step1", worker="w2")
        with self.assertRaises(WorkflowError) as ctx:
            self.complete(run["run_id"], "nope")
        self.assertEqual(ctx.exception.code, "unknown_step")


class RetryTest(SchedulerTestBase):
    def test_fail_requeues_with_exponential_backoff(self):
        self.workflow("flaky", [{"id": "s", "depends_on": [], "max_attempts": 3}])
        run = self.scheduler.start_run("acme", "flaky")
        run_id = run["run_id"]
        self.claim(run_id)
        run = self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        step = run["steps"]["s"]
        self.assertEqual((step["status"], step["attempt"]), ("ready", 1))
        self.assertEqual(step["next_attempt_at"], "2023-11-14T22:13:21.000000Z")
        # no step is running or succeeded, so the run falls back to pending
        self.assertEqual(run["status"], "pending")

        claimed = self.claim(run_id)  # attempt 2
        self.assertEqual((claimed["id"], claimed["attempt"]), ("s", 1))
        run = self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        self.assertEqual(run["steps"]["s"]["next_attempt_at"], "2023-11-14T22:13:22.000000Z")

        self.claim(run_id)  # attempt 3
        run = self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        self.assertEqual((run["steps"]["s"]["status"], run["steps"]["s"]["attempt"]),
                         ("failed", 3))
        self.assertEqual(run["status"], "failed")
        self.assertIsNone(self.claim(run_id))

    def test_backoff_formula(self):
        self.assertEqual([backoff_seconds(n) for n in (1, 2, 3, 4)], [1.0, 2.0, 4.0, 8.0])
        self.assertEqual(backoff_seconds(3, base=0.5), 2.0)

    def test_fail_after_exhaustion_marks_run_failed(self):
        self.workflow("one", [{"id": "s", "depends_on": [], "max_attempts": 1}])
        run = self.scheduler.start_run("acme", "one")
        self.claim(run["run_id"])
        run = self.scheduler.fail("acme", run["run_id"], "s", "w1", "boom")
        self.assertEqual((run["status"], run["steps"]["s"]["status"]), ("failed", "failed"))

    def test_replay_matches_stored_state_after_retries(self):
        self.workflow("flaky", [{"id": "s", "depends_on": [], "max_attempts": 3}])
        run = self.scheduler.start_run("acme", "flaky")
        self.claim(run["run_id"])
        self.scheduler.fail("acme", run["run_id"], "s", "w1", "boom")
        self.claim(run["run_id"])
        stored = self.complete(run["run_id"], "s", result={"ok": True})
        replayed = self.scheduler.replay("acme", run["run_id"])
        self.assertEqual(replayed["status"], stored["status"])
        self.assertEqual((replayed["steps"]["s"]["status"], replayed["steps"]["s"]["attempt"]),
                         ("succeeded", 2))


class LeaseExpiryTest(SchedulerTestBase):
    def test_expired_lease_is_reclaimable_by_another_worker(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "etl")
        self.assertEqual(self.claim(run["run_id"], "w1", lease=5)["id"], "step1")
        self.assertIsNone(self.claim(run["run_id"], "w2", lease=5))

        self.clock.advance(6)
        taken = self.claim(run["run_id"], "w2", lease=5)
        self.assertEqual((taken["id"], taken["worker_id"]), ("step1", "w2"))
        events = self.scheduler.history(run["run_id"], "acme")
        takeovers = [e for e in events if e["type"] == "takeover"]
        self.assertEqual(len(takeovers), 1)
        self.assertEqual(takeovers[0]["worker_id"], "w1")
        self.assertEqual(events[-1]["type"], "claim")

    def test_lease_expiry_boundary_is_clock_driven(self):
        self.workflow("one", [{"id": "s", "depends_on": []}, {"id": "t", "depends_on": ["s"]}])
        run = self.scheduler.start_run("acme", "one")
        self.claim(run["run_id"], "w1", lease=10)
        self.assertIsNone(self.claim(run["run_id"], "w1", lease=10))
        self.clock.advance(10)  # deadline reached -> expired
        self.assertIsNotNone(self.claim(run["run_id"], "w2", lease=10))

    def test_complete_after_expiry_is_rejected(self):
        self.workflow("one", [{"id": "s", "depends_on": []}])
        run = self.scheduler.start_run("acme", "one")
        self.claim(run["run_id"], "w1", lease=1)
        self.clock.advance(2)
        with self.assertRaises(LeaseError):
            self.complete(run["run_id"], "s")

    def test_scheduler_never_sleeps_on_its_own(self):
        self.workflow("one", [{"id": "s", "depends_on": []}])
        before = self.clock.value
        run = self.scheduler.start_run("acme", "one")
        self.claim(run["run_id"])
        self.complete(run["run_id"], "s")
        self.assertEqual(self.clock.value, before)


class HistoryTest(SchedulerTestBase):
    def test_history_shape_and_replay_equality(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "etl")
        self.claim(run["run_id"])
        self.complete(run["run_id"], "step1", result={"rows": 1})
        for _ in range(2):
            self.finish_next(run["run_id"])
        history = self.scheduler.history(run["run_id"], "acme")
        required = {"at", "run_id", "step_id", "type", "attempt", "worker_id"}
        for event in history:
            self.assertTrue(required.issubset(set(event)), sorted(event))
            self.assertEqual(event["run_id"], run["run_id"])
        self.assertEqual(history[-1]["type"], "run_succeeded")
        stated = self.scheduler.get_run("acme", run["run_id"])
        replayed = self.scheduler.replay("acme", run["run_id"])
        self.assertEqual(replayed["status"], stated["status"])
        self.assertEqual({k: v["status"] for k, v in replayed["steps"].items()},
                         {k: v["status"] for k, v in stated["steps"].items()})

    def test_history_is_append_only(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "etl")
        first = self.scheduler.history(run["run_id"], "acme")
        self.claim(run["run_id"])
        second = self.scheduler.history(run["run_id"], "acme")
        self.assertEqual(second[: len(first)], first)
        self.assertGreater(len(second), len(first))


class TenancyTest(SchedulerTestBase):
    def test_runs_are_isolated_per_tenant(self):
        self.workflow()
        self.scheduler.submit("globex", "etl", LINEAR)
        acme = self.scheduler.start_run("acme", "etl")
        globex = self.scheduler.start_run("globex", "etl")
        self.assertNotEqual(acme["run_id"], globex["run_id"])
        self.assertEqual([r["run_id"] for r in self.scheduler.list_runs("acme")[0]],
                         [acme["run_id"]])
        self.assertEqual([r["run_id"] for r in self.scheduler.list_runs("globex")[0]],
                         [globex["run_id"]])
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.get_run("globex", acme["run_id"])
        self.assertEqual(ctx.exception.code, "unknown_run")

    def test_workflows_are_isolated_per_tenant(self):
        self.workflow()
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.start_run("globex", "etl")
        self.assertEqual(ctx.exception.code, "unknown_workflow")

    def test_listing_filters_and_paginates(self):
        self.workflow()
        for _ in range(3):
            self.scheduler.start_run("acme", "etl")
        page1, next_after = self.scheduler.list_runs("acme", limit=2)
        self.assertEqual(len(page1), 2)
        self.assertIsNotNone(next_after)
        page2, next_after2 = self.scheduler.list_runs("acme", limit=2, after=next_after)
        self.assertEqual(len(page2), 1)
        self.assertIsNone(next_after2)
        self.assertEqual(self.scheduler.list_runs("acme", status="failed")[0], [])


class PersistenceTest(SchedulerTestBase):
    def test_state_survives_a_fresh_store(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "etl", params={"day": "2024-01-01"})
        self.claim(run["run_id"])
        self.complete(run["run_id"], "step1", result={"rows": 9})

        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        again = restarted.get_run("acme", run["run_id"])
        self.assertEqual(again["status"], "running")
        self.assertEqual(again["params"], {"day": "2024-01-01"})
        self.assertEqual(again["steps"]["step1"]["result"], {"rows": 9})
        self.assertEqual(again["steps"]["step2"]["status"], "ready")
        self.assertEqual(restarted.claim("acme", run["run_id"], "w2", 30)["id"], "step2")
        self.assertEqual(restarted.replay("acme", run["run_id"])["status"], "running")

    def test_files_are_written_where_expected(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "etl")
        self.assertTrue(os.path.isfile(os.path.join(self.root, "acme", "workflows.json")))
        self.assertTrue(
            os.path.isfile(os.path.join(self.root, "acme", "runs", run["run_id"] + ".json"))
        )
        self.assertEqual([n for _, _, files in os.walk(self.root) for n in files
                          if n.endswith(".tmp")], [])


class ApprovalTest(SchedulerTestBase):
    def approval_workflow(self, workflow_id="signoff", steps=None):
        return self.scheduler.submit("acme", workflow_id, steps or APPROVAL_PLAN)

    def decide(self, run_id, step_id, actor="alice", decision="approve", tenant="acme"):
        return self.scheduler.decide(tenant, run_id, step_id, actor, decision)

    def finish_a(self, run_id, worker="w1"):
        """Claim and complete prerequisite task ``a`` so gate ``g`` can wait."""
        self.claim(run_id, worker)
        return self.complete(run_id, "a", worker)

    def test_approval_without_dependencies_waits_at_creation(self):
        self.scheduler.submit("acme", "lone", [{"id": "g", "depends_on": [], "kind": "approval"}])
        run = self.scheduler.start_run("acme", "lone")
        gate = run["steps"]["g"]
        self.assertEqual(gate["status"], "waiting")
        self.assertEqual(run["status"], "running")  # waiting counts as work outstanding
        self.assertEqual(gate["attempt"], 0)
        self.assertIsNone(gate["worker_id"])
        self.assertIsNone(gate["lease_deadline"])
        self.assertIsNone(gate["result"])
        self.assertIsNone(gate["error"])
        self.assertIsNone(gate["approval"])
        events = [e for e in run["history"] if e["step_id"] == "g"]
        self.assertEqual([e["type"] for e in events], ["waiting"])

    def test_approval_waits_only_after_dependencies_succeed(self):
        self.approval_workflow()
        run = self.scheduler.start_run("acme", "signoff")
        status = {sid: s["status"] for sid, s in run["steps"].items()}
        self.assertEqual(status, {"a": "ready", "c": "ready", "g": "pending", "b": "pending"})
        self.assertEqual(self.claim(run["run_id"])["id"], "a")
        self.assertEqual(self.claim(run["run_id"], "w2")["id"], "c")
        self.assertIsNone(self.claim(run["run_id"]))  # gate not claimable
        run = self.complete(run["run_id"], "a")
        self.assertEqual(run["steps"]["g"]["status"], "waiting")
        self.assertEqual(run["steps"]["b"]["status"], "pending")

    def test_approve_succeeds_gate_and_unlocks_successors(self):
        self.approval_workflow()
        run = self.scheduler.start_run("acme", "signoff")
        run = self.finish_a(run["run_id"])
        before = run["steps"]["g"]["ready_at"]
        run = self.decide(run["run_id"], "g", actor="alice", decision="approve")
        gate = run["steps"]["g"]
        self.assertEqual(gate["status"], "succeeded")
        self.assertEqual(gate["attempt"], 0)
        self.assertEqual(gate["approval"],
                         {"actor": "alice", "decision": "approve", "at": gate["finished_at"]})
        self.assertEqual(gate["ready_at"], before)
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        decision_events = [e for e in run["history"] if e["type"] == "decision"]
        self.assertEqual(len(decision_events), 1)
        event = decision_events[0]
        self.assertEqual(event["step_id"], "g")
        self.assertEqual(event["attempt"], 0)
        self.assertEqual(event["actor"], "alice")
        self.assertEqual(event["decision"], "approve")
        self.assertIn("at", event)

    def test_reject_fails_gate_and_run_while_successors_stay_pending(self):
        self.approval_workflow()
        run = self.scheduler.start_run("acme", "signoff")
        self.finish_a(run["run_id"])
        run = self.decide(run["run_id"], "g", actor="bob", decision="reject")
        self.assertEqual(run["steps"]["g"]["status"], "failed")
        self.assertEqual(run["steps"]["b"]["status"], "pending")
        self.assertEqual(run["status"], "failed")
        self.assertEqual(run["steps"]["g"]["approval"]["decision"], "reject")
        self.assertEqual(run["history"][-1]["type"], "run_failed")
        # the failed gate never turns into a task; only the independent task is claimable
        self.assertEqual(self.claim(run["run_id"], "w2")["id"], "c")
        self.assertIsNone(self.claim(run["run_id"], "w3"))

    def test_claim_complete_fail_skip_or_reject_approval(self):
        self.scheduler.submit("acme", "lone", [{"id": "g", "depends_on": [], "kind": "approval"}])
        run = self.scheduler.start_run("acme", "lone")
        self.assertIsNone(self.claim(run["run_id"]))
        with self.assertRaises(ConflictError) as ctx:
            self.complete(run["run_id"], "g")
        self.assertEqual(ctx.exception.code, "not_a_task")
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.fail("acme", run["run_id"], "g", "w1", "boom")
        self.assertEqual(ctx.exception.code, "not_a_task")

    def test_same_decision_is_idempotent_but_other_repeats_conflict(self):
        self.scheduler.submit("acme", "lone", [{"id": "g", "depends_on": [], "kind": "approval"}])
        run = self.scheduler.start_run("acme", "lone")
        first = self.decide(run["run_id"], "g", "alice", "approve")
        length = len(first["history"])
        at = first["steps"]["g"]["approval"]["at"]

        repeat = self.decide(run["run_id"], "g", " alice ", "approve")  # trimmed actor
        self.assertEqual(repeat["steps"]["g"]["approval"], {"actor": "alice", "decision": "approve",
                                                           "at": at})
        self.assertEqual(len(repeat["history"]), length)

        with self.assertRaises(ConflictError) as ctx:
            self.decide(run["run_id"], "g", "carol", "approve")
        self.assertEqual(ctx.exception.code, "decision_conflict")
        with self.assertRaises(ConflictError) as ctx:
            self.decide(run["run_id"], "g", "alice", "reject")
        self.assertEqual(ctx.exception.code, "decision_conflict")
        # failed conflicts leave the first record intact
        self.assertEqual(self.scheduler.get_run("acme", run["run_id"])["steps"]["g"]["approval"],
                         {"actor": "alice", "decision": "approve", "at": at})

    def test_decide_before_waiting_or_on_a_task_conflicts(self):
        self.approval_workflow()
        run = self.scheduler.start_run("acme", "signoff")
        with self.assertRaises(ConflictError) as ctx:
            self.decide(run["run_id"], "g")  # still pending
        self.assertEqual(ctx.exception.code, "not_waiting")
        with self.assertRaises(ConflictError) as ctx:
            self.decide(run["run_id"], "a")  # ordinary task
        self.assertEqual(ctx.exception.code, "not_an_approval")
        with self.assertRaises(WorkflowError) as ctx:
            self.decide(run["run_id"], "ghost")
        self.assertEqual(ctx.exception.code, "unknown_step")

    def test_decide_unknown_and_cross_tenant_raise_unknown_run(self):
        self.scheduler.submit("globex", "lone", [{"id": "g", "depends_on": [], "kind": "approval"}])
        gx = self.scheduler.start_run("globex", "lone")
        with self.assertRaises(WorkflowError) as ctx:
            self.decide("missing", "g", tenant="acme")
        self.assertEqual(ctx.exception.code, "unknown_run")
        # the scheduler can only see a run inside its own tenant dir, so this is 404 too;
        # the HTTP layer turns it into 403 via run_exists
        with self.assertRaises(WorkflowError) as ctx:
            self.decide(gx["run_id"], "g", tenant="acme")
        self.assertEqual(ctx.exception.code, "unknown_run")

    def test_replay_preserves_waiting_decisions_and_attempts(self):
        self.approval_workflow()
        run = self.scheduler.start_run("acme", "signoff")
        self.finish_a(run["run_id"])
        stored = self.decide(run["run_id"], "g", "alice", "approve")
        replayed = self.scheduler.replay("acme", run["run_id"])
        for sid in run["step_order"]:
            self.assertEqual(replayed["steps"][sid]["status"], stored["steps"][sid]["status"], sid)
            self.assertEqual(replayed["steps"][sid]["attempt"], stored["steps"][sid]["attempt"], sid)
        self.assertEqual(replayed["steps"]["g"]["approval"], stored["steps"]["g"]["approval"])
        self.assertEqual(replayed["status"], stored["status"])

        # a still-waiting gate replays to waiting with attempt 0
        self.scheduler.submit("acme", "lone", [{"id": "g", "depends_on": [], "kind": "approval"}])
        lone = self.scheduler.start_run("acme", "lone")
        rebuilt = self.scheduler.replay("acme", lone["run_id"])
        self.assertEqual(rebuilt["steps"]["g"]["status"], "waiting")
        self.assertEqual(rebuilt["steps"]["g"]["attempt"], 0)
        self.assertIsNone(rebuilt["steps"]["g"]["approval"])

    def test_approval_state_survives_restart(self):
        self.approval_workflow()
        run = self.scheduler.start_run("acme", "signoff")
        self.finish_a(run["run_id"])
        self.decide(run["run_id"], "g", "alice", "approve")

        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        again = restarted.get_run("acme", run["run_id"])
        self.assertEqual(again["steps"]["g"]["status"], "succeeded")
        self.assertEqual(again["steps"]["g"]["attempt"], 0)
        self.assertEqual(again["steps"]["g"]["approval"]["actor"], "alice")
        self.assertEqual(restarted.replay("acme", run["run_id"])["steps"]["g"]["approval"],
                         again["steps"]["g"]["approval"])

    def test_old_run_documents_without_kind_are_treated_as_tasks(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "etl")
        # simulate a document written by the pre-approval version
        for step in run["steps"].values():
            del step["kind"]
            del step["approval"]
        self.store.save_run(run)
        reloaded = self.scheduler.get_run("acme", run["run_id"])
        self.assertEqual(reloaded["steps"]["step1"]["kind"], "task")
        self.assertIsNone(reloaded["steps"]["step1"]["approval"])
        self.assertEqual(self.claim(run["run_id"])["id"], "step1")


if __name__ == "__main__":
    unittest.main()
