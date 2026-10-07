"""Tests for pausing and resuming periodic schedules."""

import shutil
import tempfile
import unittest

from flowd.model import WorkflowError
from flowd.scheduler import ConflictError, Scheduler
from flowd.store import WorkflowStore, atomic_write_json

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


class SchedulePauseTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-sched-pause-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)
        self.scheduler.submit("acme", "etl", LINEAR)

    def create(self, tenant="acme", schedule_id="s1", **kwargs):
        kwargs.setdefault("interval_seconds", 60)
        return self.scheduler.create_schedule(tenant, schedule_id, "etl", **kwargs)

    def audit_actions(self, tenant="acme"):
        return [r["action"] for r in self.store.load_audit(tenant)]


class ScheduleStatusTest(SchedulePauseTestBase):
    def test_new_schedule_is_active_and_status_is_returned(self):
        record = self.create()
        self.assertEqual(record["status"], "active")
        items = self.scheduler.list_schedules("acme")
        self.assertEqual([r["status"] for r in items], ["active"])
        self.assertEqual(self.store.load_schedules("acme")["s1"]["status"], "active")

    def test_record_without_status_reads_back_as_active(self):
        self.create()
        path = self.store.schedules_path("acme")
        data = self.store.load_schedules("acme")
        del data["s1"]["status"]
        atomic_write_json(path, data)
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock),
                              clock=self.clock)
        items = restarted.list_schedules("acme")
        self.assertEqual(items[0]["status"], "active")
        # dispatch still fires for a legacy record
        run, _ = restarted.dispatch_schedule("acme", "s1")
        self.assertIsNotNone(run)


class PauseScheduleTest(SchedulePauseTestBase):
    def test_pause_sets_status_updates_timestamp_and_audits(self):
        record = self.create()
        self.clock.advance(5)
        paused = self.scheduler.pause_schedule("acme", "s1", "ops")
        self.assertEqual(paused["status"], "paused")
        self.assertGreater(paused["updated_at"], record["updated_at"])
        self.assertEqual(paused["created_at"], record["created_at"])
        self.assertEqual(paused["next_at"], record["next_at"])
        self.assertEqual(self.store.load_schedules("acme")["s1"]["status"], "paused")
        audit = self.store.load_audit("acme")
        self.assertEqual([r["action"] for r in audit],
                         ["workflow.submit", "schedule.create", "schedule.pause"])
        self.assertEqual(audit[-1]["actor"], "ops")
        self.assertEqual(audit[-1]["schedule_id"], "s1")

    def test_repeat_pause_is_a_noop(self):
        self.create()
        paused = self.scheduler.pause_schedule("acme", "s1", "ops")
        self.clock.advance(5)
        again = self.scheduler.pause_schedule("acme", "s1", "someone-else")
        self.assertEqual(again["status"], "paused")
        self.assertEqual(again["updated_at"], paused["updated_at"])
        self.assertEqual(self.audit_actions().count("schedule.pause"), 1)

    def test_pause_does_not_delete_schedule_or_runs(self):
        self.create(first_at=self.clock())
        run, _ = self.scheduler.dispatch_schedule("acme", "s1")
        self.scheduler.pause_schedule("acme", "s1", "ops")
        self.assertEqual([r["schedule_id"] for r in self.scheduler.list_schedules("acme")],
                         ["s1"])
        runs, _ = self.scheduler.list_runs("acme")
        self.assertEqual([r["run_id"] for r in runs], [run["run_id"]])

    def test_paused_dispatch_conflicts_without_touching_state(self):
        record = self.create(first_at=self.clock())
        self.scheduler.pause_schedule("acme", "s1", "ops")
        before_audit = self.store.load_audit("acme")
        self.clock.advance(600)
        with self.assertRaises(ConflictError) as caught:
            self.scheduler.dispatch_schedule("acme", "s1")
        self.assertEqual(caught.exception.code, "schedule_paused")
        reloaded = self.store.load_schedules("acme")["s1"]
        self.assertEqual(reloaded["next_at"], record["next_at"])
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])
        self.assertEqual(self.store.load_audit("acme"), before_audit)

    def test_pause_validation_and_ownership(self):
        self.create()
        for bad in (None, "", "   ", 7):
            with self.assertRaises(WorkflowError) as caught:
                self.scheduler.pause_schedule(bad, "s1", "ops")
            self.assertEqual(caught.exception.code, "bad_tenant", repr(bad))
        for bad in (None, "", "   ", 5):
            with self.assertRaises(WorkflowError) as caught:
                self.scheduler.pause_schedule("acme", bad, "ops")
            self.assertEqual(caught.exception.code, "bad_schedule_id", repr(bad))
        for bad in (None, "", "   ", 9):
            with self.assertRaises(WorkflowError) as caught:
                self.scheduler.pause_schedule("acme", "s1", bad)
            self.assertEqual(caught.exception.code, "bad_actor", repr(bad))
        with self.assertRaises(WorkflowError) as caught:
            self.scheduler.pause_schedule("acme", "ghost", "ops")
        self.assertEqual(caught.exception.code, "unknown_schedule")
        with self.assertRaises(WorkflowError) as caught:
            self.scheduler.pause_schedule("globex", "s1", "ops")
        self.assertEqual(caught.exception.code, "cross_tenant")
        # no failure wrote anything: still active, only the create audit
        self.assertEqual(self.store.load_schedules("acme")["s1"]["status"], "active")
        self.assertEqual(self.audit_actions(), ["workflow.submit", "schedule.create"])

    def test_ids_and_actor_are_trimmed(self):
        self.create(schedule_id="s2")
        paused = self.scheduler.pause_schedule(" acme ", " s2 ", " ops ")
        self.assertEqual(paused["status"], "paused")
        self.assertEqual(self.store.load_audit("acme")[-1]["actor"], "ops")


class ResumeScheduleTest(SchedulePauseTestBase):
    def test_resume_restores_active_and_audits(self):
        self.create()
        paused = self.scheduler.pause_schedule("acme", "s1", "ops")
        self.clock.advance(5)
        resumed = self.scheduler.resume_schedule("acme", "s1", "ops")
        self.assertEqual(resumed["status"], "active")
        self.assertGreater(resumed["updated_at"], paused["updated_at"])
        self.assertEqual(self.audit_actions(),
                         ["workflow.submit", "schedule.create",
                          "schedule.pause", "schedule.resume"])
        self.assertEqual(self.store.load_audit("acme")[-1]["actor"], "ops")

    def test_repeat_resume_is_a_noop(self):
        self.create()
        self.scheduler.pause_schedule("acme", "s1", "ops")
        resumed = self.scheduler.resume_schedule("acme", "s1", "ops")
        self.clock.advance(5)
        again = self.scheduler.resume_schedule("acme", "s1", "someone-else")
        self.assertEqual(again["status"], "active")
        self.assertEqual(again["updated_at"], resumed["updated_at"])
        self.assertEqual(self.audit_actions().count("schedule.resume"), 1)

    def test_resume_keeps_next_at_and_creates_no_run(self):
        due = self.clock() + 60
        self.create(first_at=due)
        self.scheduler.pause_schedule("acme", "s1", "ops")
        self.clock.advance(600)
        resumed = self.scheduler.resume_schedule("acme", "s1", "ops")
        self.assertEqual(resumed["next_at"], due)
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])

    def test_missed_points_caught_up_in_order_after_resume(self):
        due = self.clock()
        self.create(first_at=due, interval_seconds=100)
        self.scheduler.pause_schedule("acme", "s1", "ops")
        self.clock.advance(350)
        self.scheduler.resume_schedule("acme", "s1", "ops")
        fired = []
        for _ in range(3):
            run, scheduled_at = self.scheduler.dispatch_schedule("acme", "s1")
            self.assertIsNotNone(run)
            fired.append(scheduled_at)
        self.assertEqual(fired, [due, due + 100, due + 200])
        self.assertEqual(self.store.load_schedules("acme")["s1"]["next_at"], due + 300)

    def test_resume_validation_and_ownership(self):
        self.create()
        self.scheduler.pause_schedule("acme", "s1", "ops")
        for bad in (None, "", "   ", 7):
            with self.assertRaises(WorkflowError) as caught:
                self.scheduler.resume_schedule(bad, "s1", "ops")
            self.assertEqual(caught.exception.code, "bad_tenant", repr(bad))
        for bad in (None, "", "   ", 5):
            with self.assertRaises(WorkflowError) as caught:
                self.scheduler.resume_schedule("acme", bad, "ops")
            self.assertEqual(caught.exception.code, "bad_schedule_id", repr(bad))
        for bad in (None, "", "   ", 9):
            with self.assertRaises(WorkflowError) as caught:
                self.scheduler.resume_schedule("acme", "s1", bad)
            self.assertEqual(caught.exception.code, "bad_actor", repr(bad))
        with self.assertRaises(WorkflowError) as caught:
            self.scheduler.resume_schedule("acme", "ghost", "ops")
        self.assertEqual(caught.exception.code, "unknown_schedule")
        with self.assertRaises(WorkflowError) as caught:
            self.scheduler.resume_schedule("globex", "s1", "ops")
        self.assertEqual(caught.exception.code, "cross_tenant")
        # no failure wrote anything: still paused, no resume audit
        self.assertEqual(self.store.load_schedules("acme")["s1"]["status"], "paused")
        self.assertNotIn("schedule.resume", self.audit_actions())


class SchedulePausePersistenceTest(SchedulePauseTestBase):
    def test_status_and_times_survive_restart(self):
        due = self.clock() + 60
        self.create(first_at=due, interval_seconds=25)
        paused = self.scheduler.pause_schedule("acme", "s1", "ops")
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock),
                              clock=self.clock)
        record = restarted.list_schedules("acme")[0]
        self.assertEqual(record["status"], "paused")
        self.assertEqual(record["next_at"], due)
        self.assertEqual(record["updated_at"], paused["updated_at"])
        # dispatch still refused after the restart
        self.clock.advance(600)
        with self.assertRaises(ConflictError) as caught:
            restarted.dispatch_schedule("acme", "s1")
        self.assertEqual(caught.exception.code, "schedule_paused")
        # resume persists too, and the schedule fires again
        restarted.resume_schedule("acme", "s1", "ops")
        restarted2 = Scheduler(WorkflowStore(self.root, clock=self.clock),
                               clock=self.clock)
        self.assertEqual(restarted2.list_schedules("acme")[0]["status"], "active")
        run, scheduled_at = restarted2.dispatch_schedule("acme", "s1")
        self.assertEqual(scheduled_at, due)
        self.assertIsNotNone(run)


if __name__ == "__main__":
    unittest.main()
