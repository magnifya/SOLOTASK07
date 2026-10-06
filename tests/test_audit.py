"""Tests for the tenant-scoped audit stream (Scheduler.list_audit)."""

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


class AuditTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-audit-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def actions(self, tenant="acme", **kwargs):
        items, _ = self.scheduler.list_audit(tenant, **kwargs)
        return [item["action"] for item in items]

    def run_lifecycle(self, tenant="acme", run_id="r1"):
        self.scheduler.submit(tenant, "etl", LINEAR)
        self.scheduler.start_run(tenant, "etl", run_id=run_id)
        self.scheduler.claim(tenant, run_id, "w1", lease_seconds=60)
        self.scheduler.complete(tenant, run_id, "step1", "w1", result={"rows": 3})
        return run_id


class AuditContentTest(AuditTestBase):
    def test_workflow_submit_is_audited(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        items, next_after = self.scheduler.list_audit("acme")
        self.assertIsNone(next_after)
        self.assertEqual(len(items), 1)
        record = items[0]
        self.assertEqual(record["sequence"], 1)
        self.assertEqual(record["tenant"], "acme")
        self.assertEqual(record["action"], "workflow.submit")
        self.assertEqual(record["workflow_id"], "etl")
        for name in ("run_id", "step_id", "schedule_id", "worker_id", "actor"):
            self.assertIsNone(record[name], name)
        self.assertTrue(record["at"].endswith("Z"))

    def test_run_actions_reuse_history_types_in_order(self):
        self.run_lifecycle()
        self.assertEqual(
            self.actions(),
            ["workflow.submit", "run_created", "ready", "claim", "run_started",
             "complete", "ready"],
        )

    def test_records_carry_identifiers_and_no_payloads(self):
        self.run_lifecycle()
        items, _ = self.scheduler.list_audit("acme")
        claim = [r for r in items if r["action"] == "claim"][0]
        self.assertEqual(claim["run_id"], "r1")
        self.assertEqual(claim["step_id"], "step1")
        self.assertEqual(claim["worker_id"], "w1")
        self.assertEqual(claim["workflow_id"], "etl")
        complete = [r for r in items if r["action"] == "complete"][0]
        # No params, results or error text anywhere in the stream.
        for record in items:
            self.assertNotIn("result", record)
            self.assertNotIn("error", record)
            self.assertNotIn("params", record)
        self.assertIsNone(complete["actor"])

    def test_decision_records_actor(self):
        self.scheduler.submit("acme", "gate", GATE)
        self.scheduler.start_run("acme", "gate", run_id="r9")
        self.scheduler.claim("acme", "r9", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r9", "work", "w1")
        self.scheduler.decide("acme", "r9", "ok", "alice", "approve")
        items, _ = self.scheduler.list_audit("acme", action="decision")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["actor"], "alice")
        self.assertEqual(items[0]["step_id"], "ok")
        self.assertIsNone(items[0]["worker_id"])

    def test_worker_register_and_heartbeat_are_audited(self):
        self.scheduler.register_worker("acme", "w1", lease_seconds=30)
        self.clock.advance(5)
        self.scheduler.heartbeat_worker("acme", "w1", lease_seconds=30)
        self.scheduler.register_worker("acme", "w1", lease_seconds=30)  # refresh
        self.assertEqual(self.actions(),
                         ["worker.register", "worker.heartbeat", "worker.register"])
        items, _ = self.scheduler.list_audit("acme")
        self.assertEqual([r["worker_id"] for r in items], ["w1"] * 3)

    def test_schedule_create_and_dispatch_are_audited(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.create_schedule("acme", "nightly", "etl", 60)
        run, _ = self.scheduler.dispatch_schedule("acme", "nightly")
        actions = self.actions()
        self.assertEqual(actions[0], "workflow.submit")
        self.assertEqual(actions[1], "schedule.create")
        self.assertIn("schedule.dispatch", actions)
        dispatch = [r for r in self.scheduler.list_audit("acme")[0]
                    if r["action"] == "schedule.dispatch"][0]
        self.assertEqual(dispatch["schedule_id"], "nightly")
        self.assertEqual(dispatch["run_id"], run["run_id"])
        create = [r for r in self.scheduler.list_audit("acme")[0]
                  if r["action"] == "schedule.create"][0]
        self.assertEqual(create["workflow_id"], "etl")
        self.assertIsNone(create["run_id"])

    def test_sequences_are_gapless_and_increasing_per_tenant(self):
        self.run_lifecycle(tenant="acme", run_id="r1")
        self.run_lifecycle(tenant="globex", run_id="g1")
        acme, _ = self.scheduler.list_audit("acme")
        globex, _ = self.scheduler.list_audit("globex")
        self.assertEqual([r["sequence"] for r in acme], list(range(1, len(acme) + 1)))
        self.assertEqual([r["sequence"] for r in globex],
                         list(range(1, len(globex) + 1)))
        self.assertTrue(all(r["tenant"] == "acme" for r in acme))
        self.assertTrue(all(r["tenant"] == "globex" for r in globex))


class AuditExclusionTest(AuditTestBase):
    """No-op and rejected requests must not append audit records."""

    def test_validation_failures_are_not_audited(self):
        for call in (
            lambda: self.scheduler.submit("acme", "bad",
                                          [{"id": "a", "depends_on": ["ghost"]}]),
            lambda: self.scheduler.start_run("acme", "ghost-workflow"),
            lambda: self.scheduler.register_worker("acme", "  "),
            lambda: self.scheduler.create_schedule("acme", "s", "etl", 0),
        ):
            with self.assertRaises(WorkflowError):
                call()
        self.assertEqual(self.scheduler.list_audit("acme"), ([], None))

    def test_cross_tenant_rejections_are_not_audited(self):
        self.run_lifecycle(tenant="acme", run_id="r1")
        before = self.scheduler.list_audit("acme")[0]
        globex_before = self.scheduler.list_audit("globex")[0]
        with self.assertRaises(WorkflowError):
            self.scheduler.claim("globex", "r1", "w1")
        with self.assertRaises(WorkflowError):
            self.scheduler.get_run("globex", "r1")
        self.assertEqual(self.scheduler.list_audit("acme")[0], before)
        self.assertEqual(self.scheduler.list_audit("globex")[0], globex_before)

    def test_read_only_requests_are_not_audited(self):
        self.run_lifecycle()
        before = self.scheduler.list_audit("acme")[0]
        self.scheduler.get_run("acme", "r1")
        self.scheduler.list_runs("acme")
        self.scheduler.history("r1", tenant="acme")
        self.scheduler.replay("acme", "r1")
        self.scheduler.list_workers("acme")
        self.scheduler.list_schedules("acme")
        self.scheduler.list_audit("acme")
        self.assertEqual(self.scheduler.list_audit("acme")[0], before)

    def test_idempotency_hit_is_not_audited(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.start_run("acme", "etl", run_id="r1", idempotency_key="k1")
        before = self.actions()
        self.scheduler.start_run("acme", "etl", run_id="r1", idempotency_key="k1")
        self.assertEqual(self.actions(), before)

    def test_not_due_dispatch_is_not_audited(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.create_schedule("acme", "s1", "etl", 60,
                                       first_at=self.clock() + 600)
        before = self.actions()
        run, _ = self.scheduler.dispatch_schedule("acme", "s1")
        self.assertIsNone(run)
        self.assertEqual(self.actions(), before)

    def test_non_extending_heartbeat_is_not_audited(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.start_run("acme", "etl", run_id="r1")
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        before = self.actions()
        # lease_seconds=1 cannot extend the 60s lease: a no-op renewal.
        self.scheduler.heartbeat("acme", "r1", "step1", "w1", lease_seconds=1)
        self.assertEqual(self.actions(), before)
        # An extending renewal is audited.
        self.scheduler.heartbeat("acme", "r1", "step1", "w1", lease_seconds=3600)
        self.assertEqual(self.actions(), before + ["heartbeat"])

    def test_idle_claim_204_is_not_audited(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        self.scheduler.start_run("acme", "etl", run_id="r1",
                                 not_before=self.clock() + 600)
        before = self.actions()
        # Sleeping run: the claim activates nothing and returns None.
        self.assertIsNone(self.scheduler.claim("acme", "r1", "w1"))
        self.assertEqual(self.actions(), before)
        # Claim on a finished run changes nothing either.
        self.run_lifecycle(tenant="acme", run_id="r2")
        self.scheduler.claim("acme", "r2", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r2", "step1", "w1")
        self.scheduler.claim("acme", "r2", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r2", "step2", "w1")
        before = self.actions()
        self.assertIsNone(self.scheduler.claim("acme", "r2", "w1"))
        self.assertIsNone(self.scheduler.claim_fair("acme", "w1"))
        self.assertEqual(self.actions(), before)

    def test_idempotent_decision_repeat_is_not_audited(self):
        self.scheduler.submit("acme", "gate", GATE)
        self.scheduler.start_run("acme", "gate", run_id="r9")
        self.scheduler.claim("acme", "r9", "w1", lease_seconds=60)
        self.scheduler.complete("acme", "r9", "work", "w1")
        self.scheduler.decide("acme", "r9", "ok", "alice", "approve")
        before = self.actions()
        self.scheduler.decide("acme", "r9", "ok", "alice", "approve")
        self.assertEqual(self.actions(), before)
        with self.assertRaises(ConflictError):
            self.scheduler.decide("acme", "r9", "ok", "bob", "reject")
        self.assertEqual(self.actions(), before)


class AuditOrderTest(AuditTestBase):
    def test_takeovers_enter_the_stream_in_occurrence_order(self):
        self.scheduler.submit("acme", "etl", [
            {"id": "a", "depends_on": []},
            {"id": "b", "depends_on": []},
        ])
        self.scheduler.start_run("acme", "etl", run_id="r1", max_parallelism=2)
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=10)
        self.scheduler.claim("acme", "r1", "w2", lease_seconds=10)
        self.clock.advance(20)  # both leases expire
        self.scheduler.claim("acme", "r1", "w3", lease_seconds=60)
        actions = self.actions()
        # One takeover per reclaimed lease, in step order, then the new claim.
        self.assertEqual(actions[-3:], ["takeover", "takeover", "claim"])
        items, _ = self.scheduler.list_audit("acme", action="takeover")
        self.assertEqual([r["step_id"] for r in items], ["a", "b"])
        self.assertEqual([r["worker_id"] for r in items], ["w1", "w2"])
        sequences = [r["sequence"] for r in self.scheduler.list_audit("acme")[0]]
        self.assertEqual(sequences, sorted(sequences))

    def test_sequence_survives_restart(self):
        self.run_lifecycle()
        before, _ = self.scheduler.list_audit("acme")
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock),
                              clock=self.clock)
        restarted.submit("acme", "etl2", LINEAR)
        after, _ = restarted.list_audit("acme")
        self.assertEqual(len(after), len(before) + 1)
        self.assertEqual(after[-1]["sequence"], before[-1]["sequence"] + 1)
        self.assertEqual(after[-1]["action"], "workflow.submit")

    def test_sequences_are_unique_under_concurrency(self):
        self.scheduler.submit("acme", "etl", LINEAR)
        errors = []

        def worker(index):
            try:
                for _ in range(5):
                    self.scheduler.register_worker("acme", "w%d" % index)
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        items, _ = self.scheduler.list_audit("acme", limit=1000)
        sequences = [r["sequence"] for r in items]
        self.assertEqual(len(sequences), len(set(sequences)))
        self.assertEqual(sequences, list(range(1, len(sequences) + 1)))


class AuditQueryTest(AuditTestBase):
    def setUp(self):
        super().setUp()
        self.run_lifecycle(tenant="acme", run_id="r1")
        self.scheduler.register_worker("acme", "w1")
        self.run_lifecycle(tenant="globex", run_id="g1")

    def test_filter_by_action(self):
        items, next_after = self.scheduler.list_audit("acme", action="claim")
        self.assertIsNone(next_after)
        self.assertEqual([r["action"] for r in items], ["claim"])
        self.assertEqual(items[0]["run_id"], "r1")

    def test_filter_by_run_id(self):
        items, _ = self.scheduler.list_audit("acme", run_id="r1")
        self.assertEqual([r["action"] for r in items],
                         ["run_created", "ready", "claim", "run_started",
                          "complete", "ready"])
        self.assertEqual(self.scheduler.list_audit("acme", run_id="ghost"), ([], None))

    def test_limit_and_after_paginate_in_sequence_order(self):
        all_items, _ = self.scheduler.list_audit("acme", limit=1000)
        page1, next_after = self.scheduler.list_audit("acme", limit=3)
        self.assertEqual([r["sequence"] for r in page1], [1, 2, 3])
        self.assertEqual(next_after, 3)
        page2, next_after = self.scheduler.list_audit("acme", limit=3, after=next_after)
        self.assertEqual([r["sequence"] for r in page2], [4, 5, 6])
        self.assertEqual(next_after, 6)
        rest, next_after = self.scheduler.list_audit("acme", limit=1000, after=6)
        self.assertEqual([r["sequence"] for r in rest],
                         [r["sequence"] for r in all_items[6:]])
        self.assertIsNone(next_after)

    def test_default_limit_is_one_hundred(self):
        for index in range(150):
            self.scheduler.register_worker("acme", "bulk%d" % index)
        items, next_after = self.scheduler.list_audit("acme")
        self.assertEqual(len(items), 100)
        self.assertIsNotNone(next_after)

    def test_filters_compose_with_pagination(self):
        items, next_after = self.scheduler.list_audit("acme", action="ready", limit=1)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["action"], "ready")
        self.assertEqual(next_after, items[0]["sequence"])
        more, next_after = self.scheduler.list_audit("acme", action="ready", limit=10,
                                                     after=items[0]["sequence"])
        self.assertEqual([r["action"] for r in more], ["ready"])
        self.assertIsNone(next_after)

    def test_tenant_isolation(self):
        acme, _ = self.scheduler.list_audit("acme")
        globex, _ = self.scheduler.list_audit("globex")
        self.assertTrue(all(r["tenant"] == "acme" for r in acme))
        self.assertTrue(all(r["tenant"] == "globex" for r in globex))
        # A run_id filter cannot leak the other tenant's records.
        self.assertEqual(self.scheduler.list_audit("acme", run_id="g1"), ([], None))
        self.assertEqual(self.scheduler.list_audit("other-tenant"), ([], None))

    def test_invalid_queries_raise_coded_errors(self):
        for kwargs, code in (
            ({"tenant": ""}, "bad_tenant"),
            ({"tenant": "   "}, "bad_tenant"),
            ({"tenant": None}, "bad_tenant"),
            ({"limit": 0}, "bad_limit"),
            ({"limit": 1001}, "bad_limit"),
            ({"limit": 1.5}, "bad_limit"),
            ({"limit": True}, "bad_limit"),
            ({"limit": "10"}, "bad_limit"),
            ({"after": -1}, "bad_after"),
            ({"after": 1.5}, "bad_after"),
            ({"action": ""}, "bad_action"),
            ({"action": "  "}, "bad_action"),
            ({"run_id": ""}, "bad_run_id"),
            ({"run_id": "   "}, "bad_run_id"),
        ):
            tenant = kwargs.pop("tenant", "acme")
            with self.assertRaises(WorkflowError) as caught:
                self.scheduler.list_audit(tenant, **kwargs)
            self.assertEqual(caught.exception.code, code, (tenant, kwargs))

    def test_reading_audit_does_not_touch_runs(self):
        before = self.scheduler.get_run("acme", "r1")
        self.scheduler.list_audit("acme")
        self.scheduler.list_audit("acme", action="claim", run_id="r1", limit=1, after=1)
        after = self.scheduler.get_run("acme", "r1")
        self.assertEqual(after["updated_at"], before["updated_at"])
        self.assertEqual(len(after["history"]), len(before["history"]))
        self.assertEqual(after["steps"], before["steps"])


if __name__ == "__main__":
    unittest.main()
