"""Tests for the tenant-isolated audit stream (Scheduler/store level)."""

import shutil
import tempfile
import unittest

from flowd.model import WorkflowError
from flowd.scheduler import Scheduler
from flowd.store import WorkflowStore

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
]

APPROVAL = [
    {"id": "gate", "depends_on": [], "kind": "approval"},
]

AUDIT_FIELDS = {"sequence", "at", "tenant", "action", "run_id", "step_id",
                "workflow_id", "worker_id", "schedule_id", "actor"}


class FakeClock:
    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value


class AuditTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-audit-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock, backoff_base=1.0)

    def actions(self, tenant="acme"):
        return [r["action"] for r in self.scheduler.audit(tenant)["items"]]

    def items(self, tenant="acme", **kwargs):
        return self.scheduler.audit(tenant, **kwargs)["items"]


class AuditStreamTest(AuditTestBase):
    def test_workflow_submit_is_audited(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        items = self.items()
        self.assertEqual(len(items), 1)
        record = items[0]
        self.assertEqual(set(record), AUDIT_FIELDS)
        self.assertEqual(record["sequence"], 1)
        self.assertEqual(record["tenant"], "acme")
        self.assertEqual(record["action"], "workflow.submit")
        self.assertEqual(record["workflow_id"], "etl")
        self.assertIsNone(record["run_id"])
        self.assertIsNone(record["step_id"])
        self.assertIsNone(record["worker_id"])
        self.assertIsNone(record["schedule_id"])
        self.assertIsNone(record["actor"])
        self.assertTrue(record["at"].endswith("Z"))

    def test_run_lifecycle_events_use_history_types_in_order(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        run = self.scheduler.start_run("acme", "etl", run_id="r1")
        self.scheduler.claim("acme", "r1", "w1", 30)
        self.scheduler.complete("acme", "r1", "step1", "w1", result={"rows": 3})
        self.assertEqual(
            self.actions(),
            ["workflow.submit", "run_created", "ready", "claim", "run_started",
             "complete", "ready"],
        )
        sequences = [r["sequence"] for r in self.items()]
        self.assertEqual(sequences, sorted(sequences))
        self.assertEqual(len(set(sequences)), len(sequences))
        claim = self.items(action="claim")[0]
        self.assertEqual(claim["run_id"], "r1")
        self.assertEqual(claim["step_id"], "step1")
        self.assertEqual(claim["worker_id"], "w1")
        self.assertEqual(claim["actor"], "w1")
        self.assertEqual(run["run_id"], "r1")

    def test_payloads_are_never_recorded(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.start_run("acme", "etl", run_id="r1", params={"secret": 1})
        self.scheduler.claim("acme", "r1", "w1", 30)
        self.scheduler.complete("acme", "r1", "step1", "w1", result={"rows": 3})
        self.scheduler.claim("acme", "r1", "w1", 30)
        self.scheduler.fail("acme", "r1", "step2", "w1", error="boom")
        for record in self.items():
            self.assertEqual(set(record), AUDIT_FIELDS)
            self.assertNotIn("params", record)
            self.assertNotIn("result", record)
            self.assertNotIn("error", record)

    def test_fail_and_retry_events_are_audited(self):
        self.scheduler.submit("acme", "etl", [{"id": "s", "depends_on": [], "max_attempts": 2}])
        self.scheduler.start_run("acme", "etl", run_id="r1")
        self.scheduler.claim("acme", "r1", "w1", 30)
        self.scheduler.fail("acme", "r1", "s", "w1", error="boom")
        self.assertEqual(self.actions()[-2:], ["fail", "retry"])
        self.clock.advance(1)  # let the retry backoff elapse
        self.scheduler.claim("acme", "r1", "w1", 30)
        self.scheduler.fail("acme", "r1", "s", "w1", error="boom again")
        self.assertEqual(self.actions()[-3:], ["fail", "attempts_exhausted", "run_failed"])

    def test_decision_records_the_actor(self):
        self.scheduler.submit("acme", "etl", APPROVAL)
        self.scheduler.start_run("acme", "etl", run_id="r1")
        self.scheduler.decide("acme", "r1", "gate", "alice", "approve")
        decision = self.items(action="decision")[0]
        self.assertEqual(decision["actor"], "alice")
        self.assertEqual(decision["step_id"], "gate")
        self.assertEqual(self.actions(),
                         ["workflow.submit", "run_created", "waiting", "decision",
                          "run_succeeded"])

    def test_takeovers_enter_the_stream_in_occurrence_order(self):
        steps = [{"id": "a", "depends_on": []}, {"id": "b", "depends_on": []}]
        self.scheduler.submit("acme", "etl", steps)
        self.scheduler.start_run("acme", "etl", run_id="r1")
        self.scheduler.claim("acme", "r1", "w1", 5)
        self.scheduler.claim("acme", "r1", "w1", 5)
        self.clock.advance(10)
        # Both leases have expired: one claim reclaims them in step order.
        self.scheduler.claim("acme", "r1", "w2", 30)
        tail = self.actions()[len(self.actions()) - 3:]
        self.assertEqual(tail, ["takeover", "takeover", "claim"])
        takeovers = self.items(action="takeover")
        self.assertEqual([t["step_id"] for t in takeovers], ["a", "b"])
        self.assertEqual([t["worker_id"] for t in takeovers], ["w1", "w1"])

    def test_heartbeat_and_lease_renewal(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.start_run("acme", "etl", run_id="r1")
        self.scheduler.claim("acme", "r1", "w1", 30)
        before = len(self.items())
        # A renewal that does not extend the deadline appends nothing.
        self.scheduler.heartbeat("acme", "r1", "step1", "w1", 1)
        self.assertEqual(len(self.items()), before)
        self.scheduler.heartbeat("acme", "r1", "step1", "w1", 60)
        self.assertEqual(self.actions()[-1], "heartbeat")

    def test_noop_claim_appends_nothing(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.start_run("acme", "etl", run_id="r1", not_before=self.clock() + 100)
        before = len(self.items())
        # Sleeping run: the claim changes nothing and returns None.
        self.assertIsNone(self.scheduler.claim("acme", "r1", "w1", 30))
        self.assertEqual(len(self.items()), before)
        # Finished run: nothing ready, nothing reclaimed.
        self.scheduler.start_run("acme", "etl", run_id="r2")
        self.scheduler.claim("acme", "r2", "w1", 30)
        self.scheduler.complete("acme", "r2", "step1", "w1")
        self.scheduler.claim("acme", "r2", "w1", 30)
        self.scheduler.complete("acme", "r2", "step2", "w1")
        before = len(self.items())
        self.assertIsNone(self.scheduler.claim("acme", "r2", "w1", 30))
        self.assertEqual(len(self.items()), before)

    def test_idempotency_hit_appends_nothing(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.start_run("acme", "etl", run_id="r1", idempotency_key="k1")
        before = len(self.items())
        self.scheduler.start_run("acme", "etl", run_id="r1", idempotency_key="k1")
        self.assertEqual(len(self.items()), before)

    def test_failures_and_denials_append_nothing(self):
        with self.assertRaises(WorkflowError):
            self.scheduler.submit("acme", "bad", [{"id": "a", "depends_on": ["ghost"]}])
        with self.assertRaises(WorkflowError):
            self.scheduler.start_run("acme", "ghost")
        self.assertEqual(self.items(), [])
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.start_run("acme", "etl", run_id="r1")
        before = len(self.items())
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.claim("globex", "r1", "w1", 30)
        self.assertEqual(ctx.exception.code, "cross_tenant")
        with self.assertRaises(WorkflowError):
            self.scheduler.get_run("globex", "r1")
        self.assertEqual(len(self.items()), before)
        self.assertEqual(self.items("globex"), [])

    def test_read_only_queries_do_not_touch_runs(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.start_run("acme", "etl", run_id="r1")
        self.scheduler.claim("acme", "r1", "w1", 30)
        before = self.scheduler.get_run("acme", "r1")
        self.scheduler.audit("acme")
        self.scheduler.audit("acme", action="claim", run_id="r1", limit=1, after=0)
        after = self.scheduler.get_run("acme", "r1")
        self.assertEqual(before["updated_at"], after["updated_at"])
        self.assertEqual(len(before["history"]), len(after["history"]))
        self.assertEqual(before["steps"], after["steps"])


class AuditWorkerScheduleTest(AuditTestBase):
    def test_worker_register_and_heartbeat(self):
        self.scheduler.register_worker("acme", "w1", 30)
        self.assertEqual(self.actions(), ["worker.register"])
        record = self.items()[0]
        self.assertEqual(record["worker_id"], "w1")
        self.assertIsNone(record["run_id"])
        # A repeat registration is a change and is audited too.
        self.scheduler.register_worker("acme", "w1", 30)
        self.assertEqual(self.actions(), ["worker.register", "worker.register"])
        # A heartbeat that does not extend the lease is not audited.
        self.scheduler.heartbeat_worker("acme", "w1", 30)
        self.assertEqual(self.actions(), ["worker.register", "worker.register"])
        # One that extends it is.
        self.clock.advance(10)
        self.scheduler.heartbeat_worker("acme", "w1", 30)
        self.assertEqual(self.actions()[-1], "worker.heartbeat")
        self.assertEqual(self.items()[-1]["worker_id"], "w1")

    def test_unknown_or_expired_worker_heartbeat_appends_nothing(self):
        self.scheduler.register_worker("acme", "w1", 5)
        before = len(self.items())
        with self.assertRaises(WorkflowError):
            self.scheduler.heartbeat_worker("acme", "ghost", 30)
        self.clock.advance(10)
        with self.assertRaises(WorkflowError):
            self.scheduler.heartbeat_worker("acme", "w1", 30)
        self.assertEqual(len(self.items()), before)

    def test_schedule_create_and_dispatch(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.create_schedule("acme", "s1", "etl", 60,
                                       first_at=self.clock() + 60)
        self.assertEqual(self.actions(), ["workflow.submit", "schedule.create"])
        record = self.items()[-1]
        self.assertEqual(record["schedule_id"], "s1")
        self.assertIsNone(record["run_id"])
        # Not due yet: the 204 path appends nothing.
        before = len(self.items())
        run, _ = self.scheduler.dispatch_schedule("acme", "s1")
        self.assertIsNone(run)
        self.assertEqual(len(self.items()), before)
        # Due: the run's own events land first, then schedule.dispatch.
        self.clock.advance(60)
        run, scheduled_at = self.scheduler.dispatch_schedule("acme", "s1")
        self.assertIsNotNone(run)
        self.assertEqual(self.actions()[-3:], ["run_created", "ready", "schedule.dispatch"])
        dispatch = self.items()[-1]
        self.assertEqual(dispatch["schedule_id"], "s1")
        self.assertEqual(dispatch["run_id"], run["run_id"])

    def test_schedule_validation_and_cross_tenant_append_nothing(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.create_schedule("acme", "s1", "etl", 60)
        before = len(self.items())
        with self.assertRaises(WorkflowError):
            self.scheduler.create_schedule("acme", "s1", "etl", 60)
        with self.assertRaises(WorkflowError):
            self.scheduler.create_schedule("acme", "s2", "ghost", 60)
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.dispatch_schedule("globex", "s1")
        self.assertEqual(ctx.exception.code, "cross_tenant")
        with self.assertRaises(WorkflowError):
            self.scheduler.dispatch_schedule("acme", "ghost")
        self.assertEqual(len(self.items()), before)
        self.assertEqual(self.items("globex"), [])


class AuditQueryTest(AuditTestBase):
    def setUp(self):
        super().setUp()
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.start_run("acme", "etl", run_id="r1")
        self.scheduler.claim("acme", "r1", "w1", 30)
        self.scheduler.complete("acme", "r1", "step1", "w1")
        self.scheduler.start_run("acme", "etl", run_id="r2")
        self.scheduler.submit("globex", "etl", LINEAR)

    def test_defaults_return_up_to_100_in_sequence_order(self):
        result = self.scheduler.audit("acme")
        self.assertEqual(result["tenant"], "acme")
        sequences = [r["sequence"] for r in result["items"]]
        self.assertEqual(sequences, sorted(sequences))
        self.assertIsNone(result["next_after"])

    def test_tenant_isolation(self):
        acme = self.scheduler.audit("acme")["items"]
        globex = self.scheduler.audit("globex")["items"]
        self.assertTrue(all(r["tenant"] == "acme" for r in acme))
        self.assertEqual([r["action"] for r in globex], ["workflow.submit"])
        # Filters cannot probe another tenant's records.
        self.assertEqual(self.scheduler.audit("globex", run_id="r1")["items"], [])
        self.assertEqual(self.scheduler.audit("nowhere")["items"], [])

    def test_action_and_run_id_filters(self):
        claims = self.scheduler.audit("acme", action="claim")["items"]
        self.assertEqual(len(claims), 1)
        self.assertEqual(claims[0]["run_id"], "r1")
        r1 = self.scheduler.audit("acme", run_id="r1")["items"]
        self.assertTrue(all(r["run_id"] == "r1" for r in r1))
        self.assertEqual(self.scheduler.audit("acme", action="claim", run_id="r2")["items"], [])
        # Filters are trimmed before matching.
        self.assertEqual(self.scheduler.audit("acme", action=" claim ")["items"], claims)

    def test_limit_and_after_paginate_with_next_after(self):
        first = self.scheduler.audit("acme", limit=2)
        self.assertEqual(len(first["items"]), 2)
        self.assertEqual(first["next_after"], first["items"][-1]["sequence"])
        second = self.scheduler.audit("acme", limit=2, after=first["next_after"])
        self.assertEqual([r["sequence"] for r in second["items"]],
                         [r["sequence"] for r in self.scheduler.audit("acme")["items"][2:4]])
        # Walk to the end: the last page reports next_after null.
        seen, after = [], 0
        while True:
            page = self.scheduler.audit("acme", limit=3, after=after)
            seen.extend(r["sequence"] for r in page["items"])
            if page["next_after"] is None:
                break
            after = page["next_after"]
        everything = [r["sequence"] for r in self.scheduler.audit("acme", limit=1000)["items"]]
        self.assertEqual(seen, everything)
        # after beyond the last sequence returns an empty page.
        self.assertEqual(self.scheduler.audit("acme", after=everything[-1]),
                         {"tenant": "acme", "items": [], "next_after": None})

    def test_after_is_strictly_greater(self):
        items = self.scheduler.audit("acme")["items"]
        first = items[0]["sequence"]
        rest = self.scheduler.audit("acme", after=first)["items"]
        self.assertEqual([r["sequence"] for r in rest],
                         [r["sequence"] for r in items[1:]])

    def test_validation_codes(self):
        for bad in (None, "", "   ", 7):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.audit(bad)
            self.assertEqual(ctx.exception.code, "bad_tenant", repr(bad))
        for bad in (0, -1, 1001, 1.5, "10", True):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.audit("acme", limit=bad)
            self.assertEqual(ctx.exception.code, "bad_limit", repr(bad))
        for bad in (-1, 1.5, "0", True):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.audit("acme", after=bad)
            self.assertEqual(ctx.exception.code, "bad_after", repr(bad))
        for bad in ("", "   ", 7):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.audit("acme", action=bad)
            self.assertEqual(ctx.exception.code, "bad_action", repr(bad))
        for bad in ("", "  ", 9):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.audit("acme", run_id=bad)
            self.assertEqual(ctx.exception.code, "bad_run_id", repr(bad))
        # Boundary values are accepted.
        self.assertEqual(len(self.scheduler.audit("acme", limit=1)["items"]), 1)
        self.assertEqual(len(self.scheduler.audit("acme", limit=1000)["items"]),
                         len(self.scheduler.audit("acme")["items"]))
        self.assertEqual(len(self.scheduler.audit("acme", after=0)["items"]),
                         len(self.scheduler.audit("acme")["items"]))

    def test_sequence_survives_restart(self):
        before = self.scheduler.audit("acme")["items"][-1]["sequence"]
        store = WorkflowStore(self.root, clock=self.clock)
        scheduler = Scheduler(store, clock=self.clock)
        scheduler.submit("acme", "etl2", LINEAR)
        record = scheduler.audit("acme")["items"][-1]
        self.assertEqual(record["sequence"], before + 1)
        self.assertEqual(record["action"], "workflow.submit")

    def test_sequences_are_unique_per_tenant(self):
        acme = [r["sequence"] for r in self.scheduler.audit("acme")["items"]]
        globex = [r["sequence"] for r in self.scheduler.audit("globex")["items"]]
        self.assertEqual(acme, list(range(1, len(acme) + 1)))
        self.assertEqual(globex, list(range(1, len(globex) + 1)))


if __name__ == "__main__":
    unittest.main()
