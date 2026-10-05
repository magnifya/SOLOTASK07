"""Tests for delayed run starts (``not_before``) and claim-time activation."""

import io
import json
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout

from flowd.cli import main
from flowd.model import WorkflowError
from flowd.scheduler import ConflictError, Scheduler
from flowd.store import WorkflowStore, atomic_write_json

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
    {"id": "step3", "depends_on": ["step2"]},
]

FAN_TASKS = [{"id": "a", "depends_on": []}, {"id": "b", "depends_on": []}]

APPROVAL_ONLY = [{"id": "g", "depends_on": [], "kind": "approval"}]

MIXED = [
    {"id": "t", "depends_on": []},
    {"id": "g", "depends_on": [], "kind": "approval"},
]


class FakeClock:
    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value


class DelayedStartTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-delayed-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def submit(self, steps=None, workflow_id="wf", tenant="acme"):
        return self.scheduler.submit(tenant, workflow_id, steps or LINEAR)

    def start(self, not_before="OMIT", workflow_id="wf", run_id="r1", tenant="acme", **kwargs):
        if not_before != "OMIT":
            kwargs["not_before"] = not_before
        return self.scheduler.start_run(tenant, workflow_id, run_id=run_id, **kwargs)

    def claim(self, run_id="r1", worker="w1", lease=30, tenant="acme"):
        return self.scheduler.claim(tenant, run_id, worker, lease)

    def restart(self):
        return Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)


class ValidationTest(DelayedStartTestBase):
    def test_bad_values_raise_bad_not_before_and_create_nothing(self):
        self.submit()
        for bad in (True, False, "1700000060", [1700000060], {"seconds": 1},
                    -1, -0.5, float("inf"), float("-inf"), float("nan")):
            with self.assertRaises(WorkflowError) as ctx:
                self.start(not_before=bad, run_id="bad-%r" % (bad,))
            self.assertEqual(ctx.exception.code, "bad_not_before", repr(bad))
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])

    def test_none_and_omitted_mean_immediate(self):
        self.submit()
        omitted = self.start(not_before="OMIT", run_id="r1")
        explicit = self.start(not_before=None, run_id="r2")
        self.assertIsNone(omitted["not_before"])
        self.assertIsNone(explicit["not_before"])
        self.assertEqual([s["status"] for s in omitted["steps"].values()],
                         ["ready", "pending", "pending"])

    def test_non_negative_finite_numbers_are_accepted_as_floats(self):
        self.submit()
        run = self.start(not_before=1_700_000_060, run_id="r1")
        self.assertEqual(run["not_before"], 1_700_000_060.0)
        run = self.start(not_before=1_700_000_060.25, run_id="r2")
        self.assertEqual(run["not_before"], 1_700_000_060.25)

    def test_past_or_equal_time_starts_immediately_but_keeps_the_value(self):
        self.submit()
        now = self.clock.value
        for value in (0, now - 10, now, now - 0.001):
            run = self.start(not_before=value, run_id="past-%r" % (value,))
            self.assertEqual(run["not_before"], float(value))
            self.assertEqual([s["status"] for s in run["steps"].values()],
                             ["ready", "pending", "pending"])
            ready_events = [e for e in run["history"] if e["type"] == "ready"]
            self.assertEqual(len(ready_events), 1)

    def test_invalid_not_before_fails_before_workflow_lookup(self):
        # workflow "ghost" is never submitted; bad time must win over unknown workflow
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.start_run("acme", "ghost", run_id="r1", not_before="soon")
        self.assertEqual(ctx.exception.code, "bad_not_before")

    def test_existing_validation_order_unchanged(self):
        # bad idempotency key first, then bad quota, then bad params, then bad time
        with self.assertRaises(WorkflowError) as ctx:
            self.start(not_before=-1, run_id="x", max_parallelism=0,
                       idempotency_key=7, params=[])
        self.assertEqual(ctx.exception.code, "bad_idempotency_key")
        with self.assertRaises(WorkflowError) as ctx:
            self.start(not_before=-1, run_id="x", max_parallelism=0,
                       idempotency_key="k", params=[])
        self.assertEqual(ctx.exception.code, "bad_max_parallelism")
        with self.assertRaises(WorkflowError) as ctx:
            self.start(not_before=-1, run_id="x", idempotency_key="k", params=[])
        self.assertEqual(ctx.exception.code, "bad_params")
        with self.assertRaises(WorkflowError) as ctx:
            self.start(not_before=-1, run_id="x", idempotency_key="k", params={})
        self.assertEqual(ctx.exception.code, "bad_not_before")
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])
        self.assertEqual(self.store.load_idempotency("acme"), {})


class PendingUntilClaimedTest(DelayedStartTestBase):
    def test_future_run_stays_fully_pending_with_creation_history_only(self):
        self.submit()
        run = self.start(not_before=self.clock.value + 60)
        self.assertEqual(run["status"], "pending")
        self.assertEqual(run["not_before"], self.clock.value + 60.0)
        for step in run["steps"].values():
            self.assertEqual(step["status"], "pending")
            self.assertEqual(step["attempt"], 0)
            self.assertIsNone(step["ready_at"])
        self.assertEqual([e["type"] for e in run["history"]], ["run_created"])

    def test_approval_roots_stay_pending_not_waiting(self):
        self.submit(APPROVAL_ONLY)
        run = self.start(not_before=self.clock.value + 60)
        self.assertEqual(run["steps"]["g"]["status"], "pending")

    def test_field_reported_on_get_and_list_and_null_for_old_runs(self):
        self.submit()
        run = self.start(not_before=self.clock.value + 60)
        self.assertEqual(self.scheduler.get_run("acme", "r1")["not_before"],
                         self.clock.value + 60.0)
        items = self.scheduler.list_runs("acme")[0]
        self.assertEqual(items[0]["not_before"], self.clock.value + 60.0)

        legacy = self.start(not_before="OMIT", run_id="old")
        path = os.path.join(self.root, "acme", "runs", "old.json")
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        del doc["not_before"]
        atomic_write_json(path, doc)
        self.assertIsNone(self.scheduler.get_run("acme", "old")["not_before"])
        self.assertIsNone(self.scheduler.replay("acme", "old")["not_before"])
        self.assertEqual(run["run_id"], "r1")

    def test_claim_before_due_returns_none_and_changes_nothing(self):
        self.submit(FAN_TASKS)
        run = self.start(not_before=self.clock.value + 60)
        before = self.scheduler.get_run("acme", "r1")
        for _ in range(3):
            self.assertIsNone(self.claim())
        after = self.scheduler.get_run("acme", "r1")
        self.assertEqual(after, before)
        self.assertEqual([e["type"] for e in after["history"]], ["run_created"])
        self.assertEqual(after["updated_at"], before["updated_at"])

    def test_get_list_and_restart_do_not_activate(self):
        self.submit()
        self.start(not_before=self.clock.value + 60)
        self.scheduler.get_run("acme", "r1")
        self.scheduler.list_runs("acme")
        restarted = self.restart()
        restarted.get_run("acme", "r1")
        run = self.scheduler.get_run("acme", "r1")
        self.assertTrue(all(s["status"] == "pending" for s in run["steps"].values()))
        self.assertEqual([e["type"] for e in run["history"]], ["run_created"])
        # even once due, passive access must not activate
        self.clock.advance(120)
        self.restart().get_run("acme", "r1")
        self.restart().list_runs("acme")
        run = self.scheduler.get_run("acme", "r1")
        self.assertTrue(all(s["status"] == "pending" for s in run["steps"].values()))
        self.assertEqual([e["type"] for e in run["history"]], ["run_created"])

    def test_delayed_run_holds_no_quota_slot(self):
        self.scheduler.submit("acme", "two", [
            {"id": "x", "depends_on": []}, {"id": "y", "depends_on": []}])
        delayed = self.scheduler.start_run("acme", "two", run_id="d1",
                                           max_parallelism=1,
                                           not_before=self.clock.value + 60)
        self.assertEqual(delayed["status"], "pending")
        self.assertIsNone(self.scheduler.claim("acme", "d1", "w1"))
        held = [s for s in delayed["steps"].values() if s["status"] == "running"]
        self.assertEqual(held, [])

    def test_decision_before_activation_is_409(self):
        self.submit(APPROVAL_ONLY)
        self.start(not_before=self.clock.value + 60)
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.decide("acme", "r1", "g", "boss", "approve")
        self.assertEqual(ctx.exception.code, "not_waiting")
        # still due-but-unclaimed: passive expiry of the delay does not open it
        self.clock.advance(120)
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.decide("acme", "r1", "g", "boss", "approve")
        self.assertEqual(ctx.exception.code, "not_waiting")
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["g"]["status"], "pending")
        self.assertEqual([e["type"] for e in run["history"]], ["run_created"])


class ClaimActivationTest(DelayedStartTestBase):
    def test_first_due_claim_opens_roots_then_leases_one_task(self):
        self.submit()
        run = self.start(not_before=self.clock.value + 60)
        self.assertIsNone(self.claim())
        self.clock.advance(60)  # boundary: now == not_before
        step = self.claim()
        self.assertEqual(step["id"], "step1")
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual([s["status"] for s in run["steps"].values()],
                         ["running", "pending", "pending"])
        types = [e["type"] for e in run["history"]]
        self.assertEqual(types, ["run_created", "ready", "claim", "run_started"])

    def test_activation_records_each_root_once_and_never_repeats(self):
        self.submit(FAN_TASKS)
        self.start(not_before=self.clock.value + 60)
        self.clock.advance(61)
        self.assertEqual(self.claim(worker="w1")["id"], "a")
        self.assertEqual(self.claim(worker="w2")["id"], "b")
        self.assertIsNone(self.claim(worker="w3"))
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual([e["type"] for e in run["history"]].count("ready"), 2)
        self.assertEqual([e["step_id"] for e in run["history"] if e["type"] == "ready"],
                         ["a", "b"])
        for event in run["history"]:
            if event["type"] == "ready":
                self.assertEqual(event["attempt"], 0)

    def test_activation_promotes_root_tasks_and_approvals_in_one_claim(self):
        self.submit(MIXED)
        self.start(not_before=self.clock.value + 10)
        self.clock.advance(10)
        step = self.claim()
        self.assertEqual(step["id"], "t")
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["g"]["status"], "waiting")
        types = [e["type"] for e in run["history"]]
        self.assertEqual(types, ["run_created", "ready", "waiting", "claim", "run_started"])
        # the waiting approval can now be decided
        run = self.scheduler.decide("acme", "r1", "g", "boss", "approve")
        self.assertEqual(run["steps"]["g"]["status"], "succeeded")

    def test_approval_only_run_activates_and_returns_no_task(self):
        self.submit(APPROVAL_ONLY)
        self.start(not_before=self.clock.value + 30)
        self.clock.advance(30)
        self.assertIsNone(self.claim())
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["g"]["status"], "waiting")
        self.assertEqual(run["status"], "pending")
        self.assertEqual([e["type"] for e in run["history"]], ["run_created", "waiting"])
        # a second claim adds nothing and leases nothing
        self.assertIsNone(self.claim())
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual([e["type"] for e in run["history"]], ["run_created", "waiting"])

    def test_activation_respects_quota(self):
        self.submit(FAN_TASKS)
        self.start(not_before=self.clock.value + 30, max_parallelism=1)
        self.clock.advance(30)
        self.assertEqual(self.claim(worker="w1")["id"], "a")
        self.assertIsNone(self.claim(worker="w2"))
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["b"]["status"], "ready")  # opened, just not leased

    def test_lifecycle_after_activation_is_unchanged(self):
        self.submit()
        self.start(not_before=self.clock.value + 30)
        self.clock.advance(30)
        for sid in ("step1", "step2", "step3"):
            self.assertEqual(self.claim()["id"], sid)
            run = self.scheduler.complete("acme", "r1", sid, "w1")
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(self.scheduler.replay("acme", "r1")["status"], "succeeded")


class ReplayRestartTest(DelayedStartTestBase):
    def test_replay_preserves_schedule_and_state_before_activation(self):
        self.submit()
        run = self.start(not_before=self.clock.value + 60)
        self.assertIsNone(self.claim())
        replayed = self.scheduler.replay("acme", "r1")
        self.assertEqual(replayed["not_before"], run["not_before"])
        self.assertEqual(replayed["status"], "pending")
        self.assertTrue(all(s["status"] == "pending" for s in replayed["steps"].values()))

    def test_replay_preserves_schedule_and_state_after_activation(self):
        self.submit(MIXED)
        self.start(not_before=self.clock.value + 60)
        self.clock.advance(60)
        claimed = self.claim()
        self.assertEqual(claimed["id"], "t")
        stored = self.scheduler.get_run("acme", "r1")
        replayed = self.scheduler.replay("acme", "r1")
        self.assertEqual(replayed["not_before"], stored["not_before"])
        self.assertEqual(replayed["status"], stored["status"])
        self.assertEqual({k: v["status"] for k, v in replayed["steps"].items()},
                         {k: v["status"] for k, v in stored["steps"].items()})
        self.assertEqual((replayed["steps"]["t"]["status"],
                          replayed["steps"]["t"]["worker_id"]),
                         ("running", "w1"))
        self.assertEqual(replayed["steps"]["g"]["status"], "waiting")

    def test_scheduled_time_survives_restart_before_and_after_due(self):
        self.submit()
        self.start(not_before=self.clock.value + 60)
        restarted = self.restart()
        self.assertIsNone(restarted.claim("acme", "r1", "w1", 30))
        self.clock.advance(60)
        self.assertEqual(restarted.claim("acme", "r1", "w1", 30)["id"], "step1")
        run = restarted.get_run("acme", "r1")
        self.assertEqual(run["not_before"], 1_700_000_060.0)


class DelayedIdempotencyTest(DelayedStartTestBase):
    def start_keyed(self, not_before="OMIT", key="k", run_id="r1", **kwargs):
        if not_before != "OMIT":
            kwargs["not_before"] = not_before
        return self.scheduler.start_run("acme", "wf", run_id=run_id,
                                        idempotency_key=key, **kwargs)

    def test_omit_and_null_are_equivalent(self):
        self.submit()
        first = self.start_keyed(not_before=None)
        again = self.scheduler.start_run("acme", "wf", run_id="r1", idempotency_key="k")
        self.assertEqual(again["run_id"], first["run_id"])
        self.assertIsNone(again["not_before"])

    def test_same_numeric_time_replays_even_int_vs_float(self):
        self.submit()
        first = self.start_keyed(not_before=1_700_000_060)
        hit = self.start_keyed(not_before=1_700_000_060.0)
        self.assertEqual(hit["run_id"], first["run_id"])
        self.assertEqual(hit["not_before"], 1_700_000_060.0)

    def test_explicit_time_never_matches_null(self):
        self.submit()
        self.start_keyed(not_before=1_700_000_060)
        with self.assertRaises(ConflictError) as ctx:
            self.start_keyed(not_before=None)
        self.assertEqual(ctx.exception.code, "idempotency_conflict")
        with self.assertRaises(ConflictError):
            self.scheduler.start_run("acme", "wf", run_id="r1", idempotency_key="k")

        self.scheduler.submit("acme", "other", LINEAR)
        immediate = self.scheduler.start_run("acme", "other", run_id="r2",
                                             idempotency_key="k2")
        self.assertIsNone(immediate["not_before"])
        with self.assertRaises(ConflictError):
            self.scheduler.start_run("acme", "other", run_id="r2", idempotency_key="k2",
                                     not_before=0)

    def test_different_time_conflicts_and_leaves_run_untouched(self):
        self.submit()
        first = self.start_keyed(not_before=1_700_000_060)
        with self.assertRaises(ConflictError) as ctx:
            self.start_keyed(not_before=1_700_000_120)
        self.assertEqual(ctx.exception.code, "idempotency_conflict")
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["not_before"], first["not_before"])
        # identical request still replays the original
        self.assertEqual(self.start_keyed(not_before=1_700_000_060)["run_id"],
                         first["run_id"])
        self.assertEqual(len(self.scheduler.list_runs("acme")[0]), 1)

    def test_repeat_does_not_reset_or_activate_even_when_due(self):
        self.submit()
        first = self.start_keyed(not_before=self.clock.value + 60)
        self.clock.advance(120)
        hit = self.start_keyed(not_before=self.clock.value - 60)  # same numeric time
        self.assertEqual(hit["run_id"], first["run_id"])
        self.assertTrue(all(s["status"] == "pending" for s in hit["steps"].values()))
        self.assertEqual([e["type"] for e in hit["history"]], ["run_created"])
        # no state reset, no updated_at movement
        self.assertEqual(hit["updated_at"], first["updated_at"])

    def test_bad_time_on_repeat_is_400_before_binding_match(self):
        self.submit()
        self.start_keyed(not_before=1_700_000_060)
        with self.assertRaises(WorkflowError) as ctx:
            self.start_keyed(not_before="soon")
        self.assertEqual(ctx.exception.code, "bad_not_before")
        run = self.scheduler.get_run("acme", "r1")
        self.assertTrue(all(s["status"] == "pending" for s in run["steps"].values()))


class CliDelayedTest(DelayedStartTestBase):
    def cli(self, *argv):
        output = io.StringIO()
        with redirect_stdout(output):
            code = main(["--data-dir", self.root] + list(argv))
        self.assertEqual(code, 0, output.getvalue())
        return json.loads(output.getvalue())

    def test_status_reports_not_before(self):
        self.submit()
        self.start(not_before=self.clock.value + 60)
        body = self.cli("status", "--run-id", "r1", "--tenant", "acme")
        self.assertEqual(body["not_before"], self.clock.value + 60.0)
        legacy = self.start(not_before="OMIT", run_id="old")
        path = os.path.join(self.root, "acme", "runs", "old.json")
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        del doc["not_before"]
        atomic_write_json(path, doc)
        body = self.cli("status", "--run-id", "old", "--tenant", "acme")
        self.assertIsNone(body["not_before"])

    def test_claim_prints_step_null_before_due(self):
        self.submit()
        # the CLI builds its own store on the real wall clock, so schedule
        # relative to real time rather than the injected fake clock
        import time as _time
        self.start(not_before=_time.time() + 300)
        body = self.cli("claim", "--run-id", "r1", "--tenant", "acme", "--worker", "w1")
        self.assertEqual(body, {"run_id": "r1", "step": None})


if __name__ == "__main__":
    unittest.main()
