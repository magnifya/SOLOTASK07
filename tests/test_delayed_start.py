"""Tests for delayed run start (``not_before``).

A run created with a future ``not_before`` (UTC Unix seconds) stays fully
``pending`` until the first ``claim`` arrives at or after that time.  The
scheduled time is part of the run document and the idempotency match, and it
survives replay and restart.
"""

import io
import json
import os
import shutil
import tempfile
import time
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
SOLO_APPROVAL = [{"id": "g", "depends_on": [], "kind": "approval"}]
MIXED_ROOTS = [{"id": "t", "depends_on": []}, {"id": "g", "depends_on": [], "kind": "approval"}]


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
        self.root = tempfile.mkdtemp(prefix="flowd-delay-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def submit(self, steps=None, workflow_id="wf", tenant="acme"):
        return self.scheduler.submit(tenant, workflow_id, steps or LINEAR)

    def start(self, not_before="OMIT", run_id="r1", workflow_id="wf", tenant="acme",
              idempotency_key="OMIT", **kwargs):
        params = {"tenant": tenant, "workflow_id": workflow_id, "run_id": run_id}
        params.update(kwargs)
        if not_before != "OMIT":
            params["not_before"] = not_before
        if idempotency_key != "OMIT":
            params["idempotency_key"] = idempotency_key
        return self.scheduler.start_run(**params)

    def claim(self, run_id="r1", worker="w1", lease=30, tenant="acme"):
        return self.scheduler.claim(tenant, run_id, worker, lease)

    def history_types(self, run_id="r1"):
        return [e["type"] for e in self.scheduler.history(run_id, "acme")]


class NotBeforeValidationTest(DelayedStartTestBase):
    def test_none_and_omitted_mean_immediate(self):
        self.submit()
        omitted = self.start()
        explicit = self.start(run_id="r2", not_before=None)
        self.assertIsNone(omitted["not_before"])
        self.assertIsNone(explicit["not_before"])
        # roots open immediately
        self.assertEqual([s["status"] for s in omitted["steps"].values()],
                         ["ready", "pending", "pending"])

    def test_ints_and_floats_accepted_and_stored_as_number(self):
        self.submit()
        run = self.start(not_before=self.clock() + 60)
        self.assertEqual(run["not_before"], self.clock() + 60)
        runf = self.start(run_id="r2", not_before=self.clock() + 1.5)
        self.assertEqual(runf["not_before"], self.clock() + 1.5)

    def test_bad_values_raise_bad_not_before_and_create_nothing(self):
        self.submit()
        for bad in (True, False, "1700000060", -1, -0.5, float("inf"), float("-inf"),
                    float("nan"), [], {}, [10], 10 ** 400):
            with self.assertRaises(WorkflowError) as ctx:
                self.start(run_id="bad-%r" % (bad,), not_before=bad)
            self.assertEqual(ctx.exception.code, "bad_not_before", repr(bad))
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])

    def test_existing_field_validation_order_is_unchanged(self):
        # bad idempotency key beats bad quota, bad params and bad not_before
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.start_run("acme", "wf", run_id="x", max_parallelism=0,
                                     idempotency_key=9, params=[1], not_before=-1)
        self.assertEqual(ctx.exception.code, "bad_idempotency_key")
        self.submit()
        # then bad quota
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.start_run("acme", "wf", run_id="x", max_parallelism=0,
                                     idempotency_key="k", params=[1], not_before=-1)
        self.assertEqual(ctx.exception.code, "bad_max_parallelism")
        # then bad params (only validated for keyed creates)
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.start_run("acme", "wf", run_id="x", idempotency_key="k",
                                     params=[1], not_before=-1)
        self.assertEqual(ctx.exception.code, "bad_params")
        # then bad not_before, before the workflow lookup / binding match
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.start_run("acme", "ghost", run_id="x", idempotency_key="k",
                                     params={}, not_before=-1)
        self.assertEqual(ctx.exception.code, "bad_not_before")
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])

    def test_past_or_present_time_starts_immediately(self):
        self.submit()
        past = self.start(run_id="r1", not_before=self.clock() - 10)
        self.assertEqual(past["not_before"], self.clock() - 10)
        self.assertEqual([s["status"] for s in past["steps"].values()],
                         ["ready", "pending", "pending"])
        equal = self.start(run_id="r2", not_before=self.clock())
        self.assertEqual([s["status"] for s in equal["steps"].values()],
                         ["ready", "pending", "pending"])


class DelayedCreationTest(DelayedStartTestBase):
    def test_future_run_keeps_every_node_pending(self):
        self.submit(FAN_TASKS)
        run = self.start(not_before=self.clock() + 60)
        self.assertEqual(run["status"], "pending")
        for step in run["steps"].values():
            self.assertEqual(step["status"], "pending")
            self.assertEqual(step["attempt"], 0)
            self.assertIsNone(step["worker_id"])
            self.assertIsNone(step["lease_deadline"])
            self.assertIsNone(step["ready_at"])
        self.assertEqual(self.history_types(), ["run_created"])

    def test_future_approval_roots_are_not_waiting(self):
        self.submit(MIXED_ROOTS)
        run = self.start(not_before=self.clock() + 60)
        self.assertEqual(run["steps"]["t"]["status"], "pending")
        self.assertEqual(run["steps"]["g"]["status"], "pending")
        self.assertIsNone(run["steps"]["g"]["approval"])

    def test_not_before_reported_on_detail_and_listing(self):
        self.submit()
        when = self.clock() + 60
        self.start(not_before=when)
        self.assertEqual(self.scheduler.get_run("acme", "r1")["not_before"], when)
        items, _ = self.scheduler.list_runs("acme")
        self.assertEqual(items[0]["not_before"], when)

    def test_old_run_without_field_reads_as_null(self):
        self.submit()
        run = self.start(run_id="old")
        path = os.path.join(self.root, "acme", "runs", "old.json")
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        del doc["not_before"]
        atomic_write_json(path, doc)
        self.assertIsNone(self.scheduler.get_run("acme", "old")["not_before"])
        self.assertIsNone(self.scheduler.replay("acme", "old")["not_before"])


class ClaimActivationTest(DelayedStartTestBase):
    def test_claim_before_due_returns_none_and_changes_nothing(self):
        self.submit(FAN_TASKS)
        run = self.start(not_before=self.clock() + 60)
        before = self.scheduler.get_run("acme", "r1")
        for _ in range(3):
            self.assertIsNone(self.claim(worker="w1"))
            self.assertIsNone(self.claim(worker="w2"))
        after = self.scheduler.get_run("acme", "r1")
        self.assertEqual(after, before)
        self.assertEqual(after["updated_at"], run["updated_at"])
        self.assertEqual(self.history_types(), ["run_created"])

    def test_claim_at_or_after_due_activates_roots_and_leases_one_task(self):
        self.submit(FAN_TASKS)
        when = self.clock() + 60
        self.start(not_before=when)
        self.clock.advance(60)  # not_before == now -> due
        step = self.claim()
        self.assertEqual(step["id"], "a")
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["a"]["status"], "running")
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        types = self.history_types()
        ready_ids = [e["step_id"] for e in self.scheduler.history("r1", "acme")
                     if e["type"] == "ready"]
        self.assertEqual(ready_ids, ["a", "b"])
        self.assertIn("run_started", types)
        self.assertEqual(run["status"], "running")

    def test_activation_opens_each_root_exactly_once(self):
        self.submit(FAN_TASKS)
        self.start(not_before=self.clock() + 60)
        self.clock.advance(61)
        self.assertEqual(self.claim()["id"], "a")
        self.assertEqual(self.claim(worker="w2")["id"], "b")
        # nothing left to claim; repeated claims must not duplicate ready events
        self.assertIsNone(self.claim(worker="w3"))
        events = self.scheduler.history("r1", "acme")
        self.assertEqual([e["step_id"] for e in events if e["type"] == "ready"], ["a", "b"])

    def test_approval_only_run_activates_and_returns_no_task(self):
        self.submit(SOLO_APPROVAL)
        self.start(not_before=self.clock() + 60)
        self.clock.advance(60)
        self.assertIsNone(self.claim())
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["g"]["status"], "waiting")
        self.assertEqual(run["status"], "pending")
        self.assertEqual(self.history_types(), ["run_created", "waiting"])
        # a second claim does not record a second waiting event
        self.assertIsNone(self.claim())
        self.assertEqual(self.history_types(), ["run_created", "waiting"])

    def test_mixed_roots_record_ready_and_waiting_on_activation(self):
        self.submit(MIXED_ROOTS)
        self.start(not_before=self.clock() + 60)
        self.clock.advance(60)
        self.assertEqual(self.claim()["id"], "t")
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["g"]["status"], "waiting")
        self.assertEqual(self.history_types().count("waiting"), 1)
        self.assertEqual(self.history_types().count("ready"), 1)

    def test_quota_is_not_consumed_before_activation(self):
        self.scheduler.submit("acme", "quota", FAN_TASKS)
        run = self.start(workflow_id="quota", run_id="q", not_before=self.clock() + 60,
                         max_parallelism=1)
        self.assertIsNone(self.claim(run_id="q"))
        # the delayed run must not be occupying anything
        self.assertEqual([s["status"] for s in run["steps"].values()], ["pending", "pending"])
        self.clock.advance(60)
        self.assertEqual(self.claim(run_id="q")["id"], "a")
        self.assertIsNone(self.claim(run_id="q", worker="w2"))

    def test_query_listing_and_repeat_create_do_not_activate_even_when_due(self):
        self.submit()
        keyed = self.start(not_before=self.clock() + 60, idempotency_key="k")
        self.clock.advance(90)
        self.assertEqual(self.scheduler.get_run("acme", "r1")["steps"]["step1"]["status"],
                         "pending")
        self.assertEqual(self.scheduler.list_runs("acme")[0][0]["steps"]["step1"]["status"],
                         "pending")
        repeat = self.scheduler.start_run("acme", "wf", run_id="r1", idempotency_key="k",
                                          not_before=keyed["not_before"])
        self.assertEqual(repeat["steps"]["step1"]["status"], "pending")
        self.assertEqual(self.history_types(), ["run_created"])
        # the first claim is what activates
        self.assertEqual(self.claim()["id"], "step1")
        self.assertEqual(self.history_types().count("ready"), 1)

    def test_decision_before_activation_is_409_even_when_due(self):
        self.submit(SOLO_APPROVAL)
        self.start(not_before=self.clock() + 60)
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.decide("acme", "r1", "g", "boss", "approve")
        self.assertEqual(ctx.exception.code, "not_waiting")
        self.clock.advance(90)
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.decide("acme", "r1", "g", "boss", "approve")
        self.assertEqual(ctx.exception.code, "not_waiting")
        # claim activates; the approval is now decidable
        self.assertIsNone(self.claim())
        run = self.scheduler.decide("acme", "r1", "g", "boss", "approve")
        self.assertEqual(run["status"], "succeeded")

    def test_lifecycle_after_activation_matches_ordinary_run(self):
        self.submit(LINEAR)
        self.start(not_before=self.clock() + 60)
        self.clock.advance(60)
        for sid in ("step1", "step2", "step3"):
            self.assertEqual(self.claim()["id"], sid)
            self.scheduler.complete("acme", "r1", sid, "w1")
        self.assertEqual(self.scheduler.get_run("acme", "r1")["status"], "succeeded")


class DelayedIdempotencyTest(DelayedStartTestBase):
    def start_keyed(self, when, key="k", run_id="r1", workflow_id="wf"):
        return self.scheduler.start_run("acme", workflow_id, run_id=run_id,
                                        idempotency_key=key, not_before=when)

    def test_omitted_and_null_are_equivalent(self):
        self.submit()
        first = self.scheduler.start_run("acme", "wf", run_id="r1", idempotency_key="k")
        self.assertIsNone(first["not_before"])
        hit = self.scheduler.start_run("acme", "wf", run_id="r1", idempotency_key="k",
                                       not_before=None)
        self.assertEqual(hit["run_id"], first["run_id"])

    def test_int_and_float_compare_numerically(self):
        self.submit()
        when = self.clock() + 60
        first = self.start_keyed(when)
        hit = self.start_keyed(when + 0.0)  # float equal to the stored value
        self.assertEqual(hit["run_id"], first["run_id"])
        hit = self.start_keyed(int(when) if float(when).is_integer() else when)
        self.assertEqual(hit["run_id"], first["run_id"])

    def test_explicit_time_differs_from_null(self):
        self.submit()
        self.start_keyed(None)
        with self.assertRaises(ConflictError) as ctx:
            self.start_keyed(self.clock() + 60)
        self.assertEqual(ctx.exception.code, "idempotency_conflict")

    def test_null_differs_from_first_explicit_time(self):
        self.submit()
        self.start_keyed(self.clock() + 60)
        with self.assertRaises(ConflictError):
            self.start_keyed(None)

    def test_different_time_conflicts_and_leaves_run_untouched(self):
        self.submit()
        first = self.start_keyed(self.clock() + 60)
        with self.assertRaises(ConflictError) as ctx:
            self.start_keyed(self.clock() + 90)
        self.assertEqual(ctx.exception.code, "idempotency_conflict")
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["not_before"], first["not_before"])
        self.assertEqual(len(self.scheduler.list_runs("acme")[0]), 1)

    def test_same_time_returns_original_without_resetting_state(self):
        self.submit()
        when = self.clock() + 60
        self.start_keyed(when)
        self.clock.advance(60)
        self.assertEqual(self.claim()["id"], "step1")
        hit = self.start_keyed(when)
        self.assertEqual(hit["run_id"], "r1")
        self.assertEqual(hit["steps"]["step1"]["status"], "running")


class ReplayAndRestartTest(DelayedStartTestBase):
    def _assert_docs_equal(self, left, right):
        for name in ("status", "not_before"):
            self.assertEqual(left.get(name), right.get(name), name)
        self.assertEqual({k: v["status"] for k, v in left["steps"].items()},
                         {k: v["status"] for k, v in right["steps"].items()})

    def test_replay_preserves_sleeping_state(self):
        self.submit()
        when = self.clock() + 60
        run = self.start(not_before=when)
        replayed = self.scheduler.replay("acme", "r1")
        self.assertEqual(replayed["not_before"], when)
        self._assert_docs_equal(replayed, run)
        self.assertTrue(all(s["status"] == "pending" for s in replayed["steps"].values()))

    def test_replay_preserves_activated_state(self):
        self.submit()
        when = self.clock() + 60
        self.start(not_before=when)
        self.clock.advance(60)
        self.assertEqual(self.claim()["id"], "step1")
        stored = self.scheduler.get_run("acme", "r1")
        replayed = self.scheduler.replay("acme", "r1")
        self.assertEqual(replayed["not_before"], when)
        self._assert_docs_equal(replayed, stored)

    def test_scheduled_time_survives_restart_before_and_after_activation(self):
        self.submit()
        when = self.clock() + 60
        self.start(not_before=when)
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        self.assertIsNone(restarted.claim("acme", "r1", "w1", 30))
        self.clock.advance(60)
        self.assertEqual(restarted.claim("acme", "r1", "w1", 30)["id"], "step1")
        self.assertEqual(restarted.get_run("acme", "r1")["not_before"], when)


class DelayedCliTest(DelayedStartTestBase):
    def cli(self, *argv):
        output = io.StringIO()
        with redirect_stdout(output):
            code = main(["--data-dir", self.root] + list(argv))
        self.assertEqual(code, 0, output.getvalue())
        return json.loads(output.getvalue())

    def test_status_reports_not_before_or_null(self):
        self.submit()
        when = self.clock() + 60
        self.start(not_before=when)
        body = self.cli("status", "--run-id", "r1", "--tenant", "acme")
        self.assertEqual(body["not_before"], when)

        legacy = self.start(run_id="legacy")
        path = os.path.join(self.root, "acme", "runs", "legacy.json")
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        del doc["not_before"]
        atomic_write_json(path, doc)
        body = self.cli("status", "--run-id", "legacy", "--tenant", "acme")
        self.assertIsNone(body["not_before"])

    def test_claim_keeps_step_null_output_before_due(self):
        self.submit()
        # the CLI builds its own scheduler on the real clock, so use wall time
        self.start(not_before=time.time() + 3600)
        body = self.cli("claim", "--run-id", "r1", "--tenant", "acme", "--worker", "w1")
        self.assertEqual(body, {"run_id": "r1", "step": None})


if __name__ == "__main__":
    unittest.main()
