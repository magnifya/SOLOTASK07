"""Tests for persistent periodic schedules and dispatch-driven run creation."""

import shutil
import tempfile
import threading
import unittest

from flowd.model import WorkflowError
from flowd.scheduler import ConflictError, Scheduler
from flowd.store import WorkflowStore

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
]


class FakeClock:
    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value


class ScheduleTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-sched-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def submit(self, tenant="acme", workflow_id="etl", steps=None):
        return self.scheduler.submit(tenant, workflow_id, steps or LINEAR)

    def create(self, tenant="acme", schedule_id="s1", workflow_id="etl",
               interval_seconds=60, first_at="OMIT", params="OMIT",
               max_parallelism="OMIT"):
        kwargs = {}
        if first_at != "OMIT":
            kwargs["first_at"] = first_at
        if params != "OMIT":
            kwargs["params"] = params
        if max_parallelism != "OMIT":
            kwargs["max_parallelism"] = max_parallelism
        return self.scheduler.create_schedule(
            tenant, schedule_id, workflow_id, interval_seconds, **kwargs
        )


class CreateScheduleTest(ScheduleTestBase):
    def test_first_at_omitted_defaults_to_now_and_record_has_next_at(self):
        self.submit()
        record = self.create()
        self.assertEqual(record["first_at"], self.clock())
        self.assertEqual(record["next_at"], self.clock())
        self.assertEqual(record["interval_seconds"], 60.0)
        self.assertEqual(record["params"], {})
        self.assertIsNone(record["max_parallelism"])
        self.assertEqual(record["schedule_id"], "s1")
        self.assertEqual(record["workflow_id"], "etl")

    def test_explicit_first_at_is_next_at(self):
        self.submit()
        future = self.clock() + 300
        record = self.create(first_at=future, interval_seconds=12.5,
                             params={"k": 1}, max_parallelism=3)
        self.assertEqual(record["first_at"], future)
        self.assertEqual(record["next_at"], future)
        self.assertEqual(record["interval_seconds"], 12.5)
        self.assertEqual(record["params"], {"k": 1})
        self.assertEqual(record["max_parallelism"], 3)

    def test_zero_first_at_means_due_immediately(self):
        self.submit()
        record = self.create(first_at=0)
        self.assertEqual(record["next_at"], 0)
        run, scheduled_at = self.scheduler.dispatch_schedule("acme", "s1")
        self.assertIsNotNone(run)
        self.assertEqual(scheduled_at, 0)

    def test_schedule_id_is_trimmed(self):
        self.submit()
        record = self.create(schedule_id="  s1 \t")
        self.assertEqual(record["schedule_id"], "s1")
        self.assertIn("s1", self.store.load_schedules("acme"))

    # -- validation ----------------------------------------------------
    def test_bad_schedule_id(self):
        self.submit()
        for bad in ("", "   ", 5, True, None, ["s"]):
            with self.assertRaises(WorkflowError) as caught:
                self.create(schedule_id=bad, interval_seconds=10)
            self.assertEqual(caught.exception.code, "bad_schedule_id", repr(bad))
        self.assertEqual(self.store.load_schedules("acme"), {})

    def test_bad_interval(self):
        self.submit()
        for bad in (0, -1, "60", True, False, float("nan"), float("inf"),
                    float("-inf"), [], 10 ** 400):
            with self.assertRaises(WorkflowError) as caught:
                self.create(interval_seconds=bad)
            self.assertEqual(caught.exception.code, "bad_interval", repr(bad))
        self.assertEqual(self.store.load_schedules("acme"), {})

    def test_bad_first_at(self):
        self.submit()
        for bad in (-1, -0.01, "1700000000", True, False, float("nan"),
                    float("inf"), [], 10 ** 400):
            with self.assertRaises(WorkflowError) as caught:
                self.create(first_at=bad)
            self.assertEqual(caught.exception.code, "bad_first_at", repr(bad))
        self.assertEqual(self.store.load_schedules("acme"), {})

    def test_null_first_at_uses_now(self):
        self.submit()
        self.assertEqual(self.create(first_at=None)["next_at"], self.clock())

    def test_bad_params(self):
        self.submit()
        for bad in ([1], "x", 7, True, False):
            with self.assertRaises(WorkflowError) as caught:
                self.create(schedule_id="s-%r" % (bad,), params=bad)
            self.assertEqual(caught.exception.code, "bad_params", repr(bad))
        self.assertEqual(self.store.load_schedules("acme"), {})

    def test_bad_max_parallelism_uses_existing_validation(self):
        self.submit()
        for bad in (0, -1, 1.5, "2", True):
            with self.assertRaises(WorkflowError) as caught:
                self.create(schedule_id="q-%r" % (bad,), max_parallelism=bad)
            self.assertEqual(caught.exception.code, "bad_max_parallelism", repr(bad))
        self.assertEqual(self.store.load_schedules("acme"), {})

    def test_missing_tenant_is_bad_tenant(self):
        self.submit()
        for bad in (None, "", "   ", 7):
            with self.assertRaises(WorkflowError) as caught:
                self.create(tenant=bad)
            self.assertEqual(caught.exception.code, "bad_tenant", repr(bad))

    def test_unknown_workflow_and_duplicate_id(self):
        with self.assertRaises(WorkflowError) as caught:
            self.create(workflow_id="ghost")
        self.assertEqual(caught.exception.code, "unknown_workflow")
        self.submit()
        self.create()
        with self.assertRaises(ConflictError) as caught:
            self.create()
        self.assertEqual(caught.exception.code, "schedule_exists")
        # only the one schedule exists
        self.assertEqual(list(self.store.load_schedules("acme")), ["s1"])

    def test_same_schedule_id_is_independent_per_tenant(self):
        self.submit("acme")
        self.submit("globex")
        self.create("acme", "s1")
        record = self.create("globex", "s1", interval_seconds=7)
        self.assertEqual(record["tenant"], "globex")
        self.assertEqual(
            [r["schedule_id"] for r in self.scheduler.list_schedules("acme")], ["s1"]
        )

    def test_list_is_sorted_by_schedule_id(self):
        self.submit()
        self.create(schedule_id="b")
        self.create(schedule_id="a", interval_seconds=10)
        self.create(schedule_id="c", interval_seconds=20)
        self.assertEqual(
            [r["schedule_id"] for r in self.scheduler.list_schedules("acme")],
            ["a", "b", "c"],
        )

    def test_list_blank_tenant_rejected(self):
        with self.assertRaises(WorkflowError) as caught:
            self.scheduler.list_schedules("  ")
        self.assertEqual(caught.exception.code, "bad_tenant")


class DispatchScheduleTest(ScheduleTestBase):
    def test_dispatch_before_next_at_creates_nothing_and_keeps_state(self):
        self.submit()
        due = self.clock() + 60
        record = self.create(first_at=due, interval_seconds=60)
        self.clock.advance(59)
        self.assertIsNone(self.scheduler.dispatch_schedule("acme", "s1")[0])
        # not due yet: next_at and updated_at are both untouched
        reloaded = self.store.load_schedules("acme")["s1"]
        self.assertEqual(reloaded["next_at"], due)
        self.assertEqual(reloaded["updated_at"], record["updated_at"])
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])

    def test_due_dispatch_creates_one_run_tagged_with_schedule(self):
        self.submit()
        due = self.clock() + 10
        self.create(first_at=due, interval_seconds=60, params={"x": 9},
                    max_parallelism=2)
        self.clock.advance(10)
        run, scheduled_at = self.scheduler.dispatch_schedule("acme", "s1")
        self.assertEqual(scheduled_at, due)
        self.assertEqual(run["schedule_id"], "s1")
        self.assertEqual(run["scheduled_at"], due)
        self.assertEqual(run["params"], {"x": 9})
        self.assertEqual(run["max_parallelism"], 2)
        # roots open exactly as for a normal run
        self.assertEqual(run["steps"]["step1"]["status"], "ready")
        record = self.store.load_schedules("acme")["s1"]
        self.assertEqual(record["next_at"], due + 60)

    def test_dispatch_is_atomic_and_idempotent_within_one_period(self):
        self.submit()
        due = self.clock()
        self.create(first_at=due, interval_seconds=600)
        first, fired_at = self.scheduler.dispatch_schedule("acme", "s1")
        self.assertIsNotNone(first)
        # any later dispatch strictly before the next trigger is a no-op
        self.clock.advance(599)
        for _ in range(3):
            run, scheduled_at = self.scheduler.dispatch_schedule("acme", "s1")
            self.assertIsNone(run)
            self.assertIsNone(scheduled_at)
        self.assertEqual(len(self.scheduler.list_runs("acme")[0]), 1)
        # next trigger fires exactly when due
        self.clock.advance(1)
        second, fired_at = self.scheduler.dispatch_schedule("acme", "s1")
        self.assertEqual(fired_at, due + 600)
        self.assertNotEqual(second["run_id"], first["run_id"])
        self.assertEqual(second["scheduled_at"], due + 600)
        self.assertEqual(len(self.scheduler.list_runs("acme")[0]), 2)

    def test_missed_trigger_points_are_caught_up_one_per_call_in_order(self):
        self.submit()
        due = self.clock()
        self.create(first_at=due, interval_seconds=100)
        # miss three trigger points; the schedule is 350s behind
        self.clock.advance(350)
        fired = []
        for _ in range(3):
            run, scheduled_at = self.scheduler.dispatch_schedule("acme", "s1")
            self.assertIsNotNone(run)
            fired.append(scheduled_at)
        self.assertEqual(fired, [due, due + 100, due + 200])
        record = self.store.load_schedules("acme")["s1"]
        self.assertEqual(record["next_at"], due + 300)
        # the fourth catch-up point is still in the past -> one more run
        run, scheduled_at = self.scheduler.dispatch_schedule("acme", "s1")
        self.assertEqual(scheduled_at, due + 300)
        self.assertEqual(self.store.load_schedules("acme")["s1"]["next_at"], due + 400)
        # now caught up: next dispatch is a no-op
        self.assertIsNone(self.scheduler.dispatch_schedule("acme", "s1")[0])

    def test_unknown_and_cross_tenant_schedule(self):
        self.submit()
        with self.assertRaises(WorkflowError) as caught:
            self.scheduler.dispatch_schedule("acme", "ghost")
        self.assertEqual(caught.exception.code, "unknown_schedule")
        self.create("acme", "s1")
        with self.assertRaises(WorkflowError) as caught:
            self.scheduler.dispatch_schedule("globex", "s1")
        self.assertEqual(caught.exception.code, "cross_tenant")
        # the foreign dispatch must not create a run or move next_at
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])

    def test_blank_schedule_id_on_dispatch_is_bad_schedule_id(self):
        with self.assertRaises(WorkflowError) as caught:
            self.scheduler.dispatch_schedule("acme", "  ")
        self.assertEqual(caught.exception.code, "bad_schedule_id")

    def test_scheduled_run_keeps_normal_execution_and_replay(self):
        self.submit()
        self.create(first_at=self.clock(), interval_seconds=600)
        run, _ = self.scheduler.dispatch_schedule("acme", "s1")
        self.assertEqual(self.scheduler.claim("acme", run["run_id"], "w1", 30)["id"],
                         "step1")
        self.scheduler.complete("acme", run["run_id"], "step1", "w1")
        self.scheduler.claim("acme", run["run_id"], "w1", 30)
        stored = self.scheduler.complete("acme", run["run_id"], "step2", "w1")
        self.assertEqual(stored["status"], "succeeded")
        replayed = self.scheduler.replay("acme", run["run_id"])
        self.assertEqual(replayed["schedule_id"], "s1")
        self.assertEqual(replayed["scheduled_at"], stored["scheduled_at"])
        self.assertEqual(replayed["status"], "succeeded")


class SchedulePersistenceTest(ScheduleTestBase):
    def test_schedule_and_next_at_survive_restart(self):
        self.submit()
        due = self.clock() + 60
        self.create(first_at=due, interval_seconds=25, params={"a": 1},
                    max_parallelism=2)
        self.clock.advance(90)
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock),
                              clock=self.clock)
        items = restarted.list_schedules("acme")
        self.assertEqual(len(items), 1)
        record = items[0]
        self.assertEqual(record["first_at"], due)
        self.assertEqual(record["next_at"], due)
        run, scheduled_at = restarted.dispatch_schedule("acme", "s1")
        self.assertEqual(scheduled_at, due)
        self.assertEqual(run["params"], {"a": 1})
        self.assertEqual(run["max_parallelism"], 2)
        # one more restart: next_at persisted and advanced
        restarted2 = Scheduler(WorkflowStore(self.root, clock=self.clock),
                               clock=self.clock)
        self.assertEqual(restarted2.dispatch_schedule("acme", "s1")[1], due + 25)

    def test_concurrent_dispatches_create_one_run_per_period(self):
        self.submit()
        self.create(first_at=self.clock(), interval_seconds=6000)
        outcomes = []
        barrier = threading.Barrier(8)

        def fire():
            barrier.wait()
            outcomes.append(self.scheduler.dispatch_schedule("acme", "s1"))

        threads = [threading.Thread(target=fire) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        created = [run for run, _ in outcomes if run is not None]
        self.assertEqual(len(created), 1)
        self.assertEqual(len(self.scheduler.list_runs("acme")[0]), 1)
        self.assertEqual(self.store.load_schedules("acme")["s1"]["next_at"],
                         self.clock() + 6000)


if __name__ == "__main__":
    unittest.main()
