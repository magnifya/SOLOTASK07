"""Tests for tenant-level concurrency quotas (``quota.set`` / claim gating)."""

import json
import os
import shutil
import tempfile
import threading
import unittest

from flowd.model import WorkflowError
from flowd.scheduler import Scheduler
from flowd.store import STEP_RUNNING, WorkflowStore

FAN = [{"id": "a", "depends_on": []}, {"id": "b", "depends_on": []},
       {"id": "c", "depends_on": []}]


class FakeClock:
    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value


class TenantQuotaTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-tenant-quota-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def workflow(self, tenant="acme", workflow_id="fan", steps=None):
        return self.scheduler.submit(tenant, workflow_id, steps or FAN)

    def start(self, run_id, tenant="acme", workflow_id="fan", **kw):
        return self.scheduler.start_run(tenant, workflow_id, run_id=run_id, **kw)

    def claim(self, run_id, worker="w1", lease=30, tenant="acme"):
        return self.scheduler.claim(tenant, run_id, worker, lease)

    def audit(self, tenant="acme"):
        items, _ = self.scheduler.list_audit(tenant)
        return items


class TenantQuotaConfigTest(TenantQuotaTestBase):
    def test_set_and_get_roundtrip(self):
        record, created = self.scheduler.set_quota("acme", 3)
        self.assertTrue(created)
        self.assertEqual(record["tenant"], "acme")
        self.assertEqual(record["max_parallelism"], 3)
        self.assertIsNotNone(record["updated_at"])
        self.assertEqual(self.scheduler.get_quota("acme"), record)

    def test_tenant_is_normalized(self):
        record, _ = self.scheduler.set_quota("  acme  ", 2)
        self.assertEqual(record["tenant"], "acme")
        self.assertEqual(self.scheduler.get_quota(" acme ")["max_parallelism"], 2)

    def test_unconfigured_reads_as_null(self):
        record = self.scheduler.get_quota("acme")
        self.assertEqual(record["tenant"], "acme")
        self.assertIsNone(record["max_parallelism"])

    def test_null_quota_means_unlimited(self):
        record, created = self.scheduler.set_quota("acme", None)
        self.assertTrue(created)
        self.assertIsNone(record["max_parallelism"])
        record, created = self.scheduler.set_quota("acme")
        self.assertFalse(created)  # null -> null is a same-value update
        self.assertIsNone(record["max_parallelism"])

    def test_first_write_201_then_updates_200(self):
        _, created = self.scheduler.set_quota("acme", 1)
        self.assertTrue(created)
        _, created = self.scheduler.set_quota("acme", 2)
        self.assertFalse(created)
        _, created = self.scheduler.set_quota("acme", None)
        self.assertFalse(created)

    def test_same_value_update_keeps_updated_at_and_skips_audit(self):
        first, _ = self.scheduler.set_quota("acme", 2)
        self.clock.advance(10)
        again, created = self.scheduler.set_quota("acme", 2)
        self.assertFalse(created)
        self.assertEqual(again["updated_at"], first["updated_at"])
        sets = [r for r in self.audit() if r["action"] == "quota.set"]
        self.assertEqual(len(sets), 1)

    def test_every_change_appends_one_audit_record(self):
        self.scheduler.set_quota("acme", 1)
        self.scheduler.set_quota("acme", 2)
        self.scheduler.set_quota("acme", None)
        sets = [r for r in self.audit() if r["action"] == "quota.set"]
        self.assertEqual(len(sets), 3)
        self.assertTrue(all(r["tenant"] == "acme" for r in sets))

    def test_failed_validation_appends_no_audit(self):
        for bad_call in (lambda: self.scheduler.set_quota("", 1),
                         lambda: self.scheduler.set_quota("  ", 1),
                         lambda: self.scheduler.set_quota(None, 1),
                         lambda: self.scheduler.set_quota(7, 1),
                         lambda: self.scheduler.set_quota("acme", True),
                         lambda: self.scheduler.set_quota("acme", 1.5),
                         lambda: self.scheduler.set_quota("acme", "2"),
                         lambda: self.scheduler.set_quota("acme", 0),
                         lambda: self.scheduler.set_quota("acme", -1)):
            with self.assertRaises(WorkflowError):
                bad_call()
        self.assertEqual(self.audit(), [])

    def test_read_queries_append_no_audit(self):
        self.scheduler.set_quota("acme", 1)
        before = len(self.audit())
        self.scheduler.get_quota("acme")
        self.scheduler.get_quota("acme")
        self.assertEqual(len(self.audit()), before)

    def test_bad_tenant_rejected(self):
        for bad in (None, "", "   ", 3, [], {}):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.set_quota(bad, 1)
            self.assertEqual(ctx.exception.code, "bad_tenant")
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.get_quota(bad)
            self.assertEqual(ctx.exception.code, "bad_tenant")

    def test_bad_max_parallelism_rejected(self):
        for bad in (True, False, 1.5, 0.0, "2", 0, -1, -3, [], {}):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.set_quota("acme", bad)
            self.assertEqual(ctx.exception.code, "bad_max_parallelism", bad)
        self.assertIsNone(self.scheduler.get_quota("acme")["max_parallelism"])

    def test_quota_is_scoped_per_tenant(self):
        self.scheduler.set_quota("acme", 1)
        self.assertIsNone(self.scheduler.get_quota("globex")["max_parallelism"])
        self.assertEqual(self.scheduler.get_quota("acme")["max_parallelism"], 1)

    def test_quota_survives_restart(self):
        self.scheduler.set_quota("acme", 2)
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        self.assertEqual(restarted.get_quota("acme")["max_parallelism"], 2)
        path = os.path.join(self.root, "acme", "quota.json")
        with open(path, "r", encoding="utf-8") as handle:
            self.assertEqual(json.load(handle)["max_parallelism"], 2)


class TenantQuotaEnforcementTest(TenantQuotaTestBase):
    def test_quota_is_shared_across_runs(self):
        self.workflow()
        self.scheduler.set_quota("acme", 2)
        self.start("r1")
        self.start("r2")
        self.assertEqual(self.claim("r1")["id"], "a")
        self.assertEqual(self.claim("r2")["id"], "a")
        # both runs are out of tenant budget, even though each run is
        # individually below its (unlimited) per-run quota
        self.assertIsNone(self.claim("r1", worker="w2"))
        self.assertIsNone(self.claim("r2", worker="w2"))
        self.scheduler.complete("acme", "r1", "a", "w1")
        self.assertEqual(self.claim("r2", worker="w2")["id"], "b")

    def test_unconfigured_tenant_keeps_legacy_behavior(self):
        self.workflow()
        self.start("r1")
        self.start("r2")
        for run_id in ("r1", "r2"):
            for worker in ("w1", "w2", "w3"):
                self.assertIsNotNone(self.claim(run_id, worker=worker))

    def test_null_quota_means_unlimited_claims(self):
        self.workflow()
        self.scheduler.set_quota("acme", None)
        self.start("r1")
        for worker in ("w1", "w2", "w3"):
            self.assertIsNotNone(self.claim("r1", worker=worker))

    def test_fair_claim_shares_the_same_budget(self):
        self.workflow()
        self.scheduler.set_quota("acme", 1)
        self.start("r1")
        self.start("r2")
        run_id, step = self.scheduler.claim_fair("acme", "w1", 30)
        self.assertEqual((run_id, step["id"]), ("r1", "a"))
        # single-run claim and fair claim both see the full budget
        self.assertIsNone(self.claim("r2", worker="w2"))
        self.assertIsNone(self.scheduler.claim_fair("acme", "w2", 30))
        self.scheduler.complete("acme", "r1", "a", "w1")
        run_id, step = self.scheduler.claim_fair("acme", "w2", 30)
        self.assertEqual((run_id, step["id"]), ("r2", "a"))

    def test_expired_leases_are_reclaimed_tenant_wide_before_counting(self):
        self.workflow()
        self.scheduler.set_quota("acme", 1)
        self.start("r1")
        self.start("r2")
        self.claim("r1", worker="w1", lease=5)
        self.clock.advance(5)  # r1's lease is expired; deadline == now counts
        # the expired lease frees the tenant slot and records a takeover on r1
        taken = self.claim("r2", worker="w2", lease=30)
        self.assertEqual((taken["id"], taken["worker_id"]), ("a", "w2"))
        takeovers = [e for e in self.scheduler.history("r1", "acme")
                     if e["type"] == "takeover"]
        self.assertEqual(len(takeovers), 1)
        self.assertEqual(takeovers[0]["worker_id"], "w1")

    def test_takeover_is_kept_when_the_claim_is_refused(self):
        self.workflow()
        self.scheduler.set_quota("acme", 3)
        self.start("r1")
        self.start("r2")
        self.start("r3")
        self.claim("r1", worker="w1", lease=5)
        self.claim("r2", worker="w2", lease=60)
        self.claim("r3", worker="w3", lease=60)
        self.scheduler.set_quota("acme", 2)  # existing leases are not revoked
        self.clock.advance(5)  # r1's lease expires; r2/r3 still fill the budget
        # the refused claim reclaims r1's expired lease (takeover kept) but
        # adds no claim event because the budget stays full
        self.assertIsNone(self.claim("r1", worker="w4"))
        events = self.scheduler.history("r1", "acme")
        takeovers = [e for e in events if e["type"] == "takeover"]
        self.assertEqual(len(takeovers), 1)
        self.assertEqual(takeovers[0]["worker_id"], "w1")
        self.assertEqual(len([e for e in events if e["type"] == "claim"]), 1)
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["a"]["status"], "ready")

    def test_approvals_ready_and_waiting_retries_do_not_count(self):
        self.scheduler.submit("acme", "mixed", [
            {"id": "g", "depends_on": [], "kind": "approval"},
            {"id": "a", "depends_on": [], "max_attempts": 3},
            {"id": "b", "depends_on": []},
        ])
        self.scheduler.set_quota("acme", 1)
        self.start("r1", workflow_id="mixed")
        self.assertEqual(self.claim("r1")["id"], "a")
        self.scheduler.fail("acme", "r1", "a", "w1", "boom")  # retry backoff
        # the waiting approval and the backed-off retry hold no slot
        self.assertEqual(self.claim("r1", worker="w2")["id"], "b")

    def test_completion_failure_and_cancel_release_slots(self):
        self.workflow()
        self.scheduler.set_quota("acme", 1)
        self.start("r1")
        self.start("r2")
        self.claim("r1", worker="w1")
        self.assertIsNone(self.claim("r2", worker="w2"))
        self.scheduler.fail("acme", "r1", "a", "w1", "boom")  # last attempt? no: retry
        self.assertEqual(self.claim("r2", worker="w2")["id"], "a")
        self.scheduler.cancel("acme", "r2", "ops")
        self.assertEqual(self.claim("r1", worker="w3")["id"], "b")

    def test_lowering_the_quota_never_revokes_existing_leases(self):
        self.workflow()
        self.scheduler.set_quota("acme", 3)
        self.start("r1")
        for worker in ("w1", "w2", "w3"):
            self.claim("r1", worker=worker)
        self.scheduler.set_quota("acme", 1)
        run = self.scheduler.get_run("acme", "r1")
        held = [s for s in run["steps"].values() if s["status"] == STEP_RUNNING]
        self.assertEqual(len(held), 3)
        # but no new claim until the count drops below the new quota
        self.assertIsNone(self.claim("r1", worker="w4"))
        self.scheduler.complete("acme", "r1", "a", "w1")
        self.scheduler.complete("acme", "r1", "b", "w2")
        self.assertIsNone(self.claim("r1", worker="w4"))  # still 1 held >= 1
        self.scheduler.complete("acme", "r1", "c", "w3")
        self.assertIsNone(self.claim("r1", worker="w4"))  # nothing ready

    def test_per_run_quota_still_applies_under_tenant_quota(self):
        self.workflow()
        self.scheduler.set_quota("acme", 5)
        self.start("r1", max_parallelism=1)
        self.assertEqual(self.claim("r1")["id"], "a")
        self.assertIsNone(self.claim("r1", worker="w2"))

    def test_quota_is_isolated_between_tenants(self):
        self.workflow()
        self.workflow(tenant="globex")
        self.scheduler.set_quota("acme", 1)
        self.start("r1")
        self.start("r2", tenant="globex")
        self.claim("r1", worker="w1")
        self.assertIsNone(self.claim("r1", worker="w2"))
        self.assertEqual(self.claim("r2", worker="w1", tenant="globex")["id"], "a")

    def test_concurrent_claims_never_oversubscribe_the_tenant(self):
        self.scheduler.submit("acme", "wide",
                              [{"id": "t%02d" % i, "depends_on": []} for i in range(4)])
        self.scheduler.set_quota("acme", 2)
        self.start("r1", workflow_id="wide")
        self.start("r2", workflow_id="wide")
        results = []
        barrier = threading.Barrier(8)

        def grab(i):
            barrier.wait()
            if i % 2:
                results.append(self.scheduler.claim("acme", "r1", "w%d" % i, 30))
            else:
                results.append(self.scheduler.claim_fair("acme", "w%d" % i, 30))

        threads = [threading.Thread(target=grab, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        held = sum(
            1
            for run_id in ("r1", "r2")
            for step in self.scheduler.get_run("acme", run_id)["steps"].values()
            if step["status"] == STEP_RUNNING
        )
        self.assertEqual(held, 2)
        self.assertEqual(sum(1 for r in results if r is None), 6)


if __name__ == "__main__":
    unittest.main()
