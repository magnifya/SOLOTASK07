"""Tests for the run state machine, leases, retries, history and replay."""

import os
import shutil
import tempfile
import unittest

from flowd.model import WorkflowError
from flowd.scheduler import LeaseError, Scheduler, backoff_seconds
from flowd.store import WorkflowStore

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
    {"id": "step3", "depends_on": ["step2"]},
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


if __name__ == "__main__":
    unittest.main()
