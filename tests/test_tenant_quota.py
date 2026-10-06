"""Tests for tenant-level concurrency quotas (shared lease budget)."""

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

    def start(self, run_id, tenant="acme", workflow_id="fan", max_parallelism=None):
        return self.scheduler.start_run(tenant, workflow_id, run_id=run_id,
                                        max_parallelism=max_parallelism)

    def claim(self, run_id, worker="w1", lease=30, tenant="acme"):
        return self.scheduler.claim(tenant, run_id, worker, lease)

    def quota_actions(self, tenant="acme"):
        items, _ = self.scheduler.list_audit(tenant, action="quota.set")
        return items


class TenantQuotaConfigTest(TenantQuotaTestBase):
    def test_set_and_get_roundtrip(self):
        record, created = self.scheduler.set_quota("acme", 3)
        self.assertTrue(created)
        self.assertEqual(record["max_parallelism"], 3)
        self.assertEqual(record["tenant"], "acme")
        self.assertIsNotNone(record["updated_at"])
        loaded = self.scheduler.get_quota("acme")
        self.assertEqual(loaded, record)

    def test_first_write_reports_created_then_updates_do_not(self):
        self.assertTrue(self.scheduler.set_quota("acme", 1)[1])
        self.assertFalse(self.scheduler.set_quota("acme", 2)[1])
        self.assertFalse(self.scheduler.set_quota("acme", None)[1])

    def test_omitted_or_null_means_unlimited(self):
        record, created = self.scheduler.set_quota("acme")
        self.assertTrue(created)
        self.assertIsNone(record["max_parallelism"])
        record, created = self.scheduler.set_quota("acme", None)
        self.assertFalse(created)
        self.assertIsNone(record["max_parallelism"])

    def test_unconfigured_tenant_reads_as_null(self):
        record = self.scheduler.get_quota("acme")
        self.assertEqual(record["tenant"], "acme")
        self.assertIsNone(record["max_parallelism"])
        self.assertIsNone(record["updated_at"])

    def test_tenant_is_normalized_and_validated(self):
        record, _ = self.scheduler.set_quota("  acme  ", 2)
        self.assertEqual(record["tenant"], "acme")
        for bad in (None, "", "   ", 7, 1.5, True, ["acme"], {"t": "acme"}):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.set_quota(bad, 2)
            self.assertEqual(ctx.exception.code, "bad_tenant")
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.get_quota("  ")
        self.assertEqual(ctx.exception.code, "bad_tenant")

    def test_invalid_quota_values_are_rejected(self):
        for bad in (True, False, 1.5, 0.0, "2", 0, -1, -3, [], {}):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.set_quota("acme", bad)
            self.assertEqual(ctx.exception.code, "bad_max_parallelism")
        self.assertIsNone(self.scheduler.get_quota("acme")["max_parallelism"])

    def test_same_value_update_keeps_updated_at_and_adds_no_audit(self):
        first, _ = self.scheduler.set_quota("acme", 2)
        self.clock.advance(10)
        again, created = self.scheduler.set_quota("acme", 2)
        self.assertFalse(created)
        self.assertEqual(again["updated_at"], first["updated_at"])
        self.assertEqual(len(self.quota_actions()), 1)

    def test_changed_value_updates_timestamp_and_audits(self):
        first, _ = self.scheduler.set_quota("acme", 2)
        self.clock.advance(10)
        changed, created = self.scheduler.set_quota("acme", 3)
        self.assertFalse(created)
        self.assertNotEqual(changed["updated_at"], first["updated_at"])
        actions = self.quota_actions()
        self.assertEqual(len(actions), 2)
        self.assertEqual([a["action"] for a in actions], ["quota.set", "quota.set"])

    def test_reads_and_failed_writes_add_no_audit(self):
        self.scheduler.get_quota("acme")
        for bad in (True, 0, -1, "2"):
            with self.assertRaises(WorkflowError):
                self.scheduler.set_quota("acme", bad)
        with self.assertRaises(WorkflowError):
            self.scheduler.set_quota(" ", 1)
        self.assertEqual(self.quota_actions(), [])

    def test_quota_survives_restart(self):
        self.scheduler.set_quota("acme", 2)
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        self.assertEqual(restarted.get_quota("acme")["max_parallelism"], 2)

    def test_quotas_are_isolated_per_tenant(self):
        self.scheduler.set_quota("acme", 1)
        self.assertIsNone(self.scheduler.get_quota("globex")["max_parallelism"])
        self.scheduler.set_quota("globex", 5)
        self.assertEqual(self.scheduler.get_quota("acme")["max_parallelism"], 1)
        self.assertEqual(self.scheduler.get_quota("globex")["max_parallelism"], 5)


class TenantQuotaEnforcementTest(TenantQuotaTestBase):
    def setUp(self):
        super().setUp()
        self.workflow()
        self.start("r1")
        self.start("r2")

    def test_budget_is_shared_across_runs(self):
        self.scheduler.set_quota("acme", 1)
        self.assertEqual(self.claim("r1")["id"], "a")
        # the single slot is held by r1, so r2 cannot lease
        self.assertIsNone(self.claim("r2", worker="w2"))
        self.assertIsNone(self.claim("r1", worker="w2"))
        # completing releases the slot for any run of the tenant
        self.scheduler.complete("acme", "r1", "a", "w1")
        self.assertEqual(self.claim("r2", worker="w2")["id"], "a")

    def test_unconfigured_tenant_keeps_legacy_behavior(self):
        self.assertEqual(self.claim("r1")["id"], "a")
        self.assertEqual(self.claim("r2", worker="w2")["id"], "a")
        self.assertEqual(self.claim("r1", worker="w3")["id"], "b")

    def test_null_quota_means_unlimited(self):
        self.scheduler.set_quota("acme", None)
        self.assertEqual(self.claim("r1")["id"], "a")
        self.assertEqual(self.claim("r2", worker="w2")["id"], "a")

    def test_fair_claim_shares_the_budget(self):
        self.scheduler.set_quota("acme", 1)
        run_id, step = self.scheduler.claim_fair("acme", "w1", 30)
        self.assertEqual(step["id"], "a")
        # budget full: neither fair nor single-run claims lease anything
        self.assertIsNone(self.scheduler.claim_fair("acme", "w2", 30))
        other = "r2" if run_id == "r1" else "r1"
        self.assertIsNone(self.claim(other, worker="w2"))
        self.scheduler.complete("acme", run_id, "a", "w1")
        run_id2, step2 = self.scheduler.claim_fair("acme", "w2", 30)
        self.assertIsNotNone(step2)

    def test_expired_leases_are_reclaimed_tenant_wide_before_counting(self):
        self.scheduler.set_quota("acme", 1)
        self.claim("r1", worker="w1", lease=5)
        self.assertIsNone(self.claim("r2", worker="w2"))
        self.clock.advance(5)  # r1's lease expired; deadline == now counts
        taken = self.claim("r2", worker="w2", lease=30)
        self.assertEqual((taken["id"], taken["worker_id"]), ("a", "w2"))
        takeovers = [e for e in self.scheduler.history("r1", "acme")
                     if e["type"] == "takeover"]
        self.assertEqual(len(takeovers), 1)
        self.assertEqual(takeovers[0]["worker_id"], "w1")

    def test_takeover_is_kept_when_budget_is_full(self):
        self.scheduler.set_quota("acme", 2)
        self.claim("r1", worker="w1", lease=5)
        self.claim("r2", worker="w2", lease=60)
        self.scheduler.set_quota("acme", 1)
        self.clock.advance(5)  # r1's lease expired; r2 still holds the budget
        # the blocked claim still reclaims r1's expired lease
        self.assertIsNone(self.claim("r1", worker="w3"))
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["a"]["status"], "ready")
        events = self.scheduler.history("r1", "acme")
        takeovers = [e for e in events if e["type"] == "takeover"]
        self.assertEqual(len(takeovers), 1)
        self.assertEqual(takeovers[0]["worker_id"], "w1")
        # the blocked claim added no claim event of its own
        self.assertEqual(len([e for e in events if e["type"] == "claim"]), 1)

    def test_approval_and_non_running_steps_do_not_count(self):
        self.scheduler.submit("acme", "gated", [
            {"id": "a", "depends_on": []},
            {"id": "g", "depends_on": [], "kind": "approval"},
        ])
        self.scheduler.set_quota("acme", 1)
        self.start("r3", workflow_id="gated")
        self.assertEqual(self.claim("r3")["id"], "a")
        # waiting approval does not consume a slot, but the running task does
        self.assertIsNone(self.claim("r1", worker="w2"))
        self.scheduler.complete("acme", "r3", "a", "w1")
        self.assertEqual(self.claim("r1", worker="w2")["id"], "a")

    def test_failure_and_cancel_release_slots(self):
        self.scheduler.set_quota("acme", 1)
        self.claim("r1", worker="w1", lease=30)
        self.scheduler.fail("acme", "r1", "a", "w1", "boom")
        # slot freed by the failure; "a" backs off, r2 can lease
        self.assertEqual(self.claim("r2", worker="w2")["id"], "a")
        self.scheduler.cancel("acme", "r2", "tester")
        self.assertEqual(self.claim("r1", worker="w3")["id"], "b")

    def test_lowering_quota_does_not_revoke_existing_leases(self):
        self.scheduler.set_quota("acme", 2)
        self.claim("r1", worker="w1", lease=30)
        self.claim("r2", worker="w2", lease=30)
        self.scheduler.set_quota("acme", 1)
        # both leases survive and can complete; no new lease until one frees
        self.assertIsNone(self.claim("r1", worker="w3"))
        self.scheduler.complete("acme", "r1", "a", "w1")
        self.assertIsNone(self.claim("r1", worker="w3"))  # r2 still holds one
        self.scheduler.complete("acme", "r2", "a", "w2")
        self.assertEqual(self.claim("r1", worker="w3")["id"], "b")

    def test_per_run_quota_still_applies_under_tenant_quota(self):
        self.start("r3", max_parallelism=1)
        self.scheduler.set_quota("acme", 5)
        self.assertEqual(self.claim("r3")["id"], "a")
        # tenant budget has room, but the run's own quota is full
        self.assertIsNone(self.claim("r3", worker="w2"))
        # other runs may still lease from the tenant budget
        self.assertEqual(self.claim("r1", worker="w2")["id"], "a")

    def test_quota_does_not_leak_across_tenants(self):
        self.workflow(tenant="globex")
        self.start("g1", tenant="globex")
        self.scheduler.set_quota("acme", 1)
        self.claim("r1", worker="w1", lease=30)
        self.assertIsNone(self.claim("r2", worker="w2"))
        self.assertEqual(self.claim("g1", worker="w1", tenant="globex")["id"], "a")

    def test_restarted_store_keeps_enforcing(self):
        self.scheduler.set_quota("acme", 1)
        self.claim("r1", worker="w1", lease=30)
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        self.assertIsNone(restarted.claim("acme", "r2", "w2", 30))
        self.clock.advance(30)
        self.assertEqual(restarted.claim("acme", "r2", "w2", 5)["id"], "a")

    def test_concurrent_claims_never_oversubscribe_the_budget(self):
        self.scheduler.submit("acme", "wide",
                              [{"id": "t%02d" % i, "depends_on": []} for i in range(4)])
        self.start("w1", workflow_id="wide")
        self.start("w2", workflow_id="wide")
        self.scheduler.set_quota("acme", 2)
        results = []
        barrier = threading.Barrier(8)

        def grab(i):
            barrier.wait()
            if i % 2:
                results.append(self.scheduler.claim("acme", "w1", "worker%d" % i, 30))
            else:
                results.append(self.scheduler.claim("acme", "w2", "worker%d" % i, 30))

        threads = [threading.Thread(target=grab, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        held = sum(
            1
            for run_id in ("w1", "w2")
            for step in self.scheduler.get_run("acme", run_id)["steps"].values()
            if step["status"] == STEP_RUNNING
        )
        self.assertEqual(held, 2)
        self.assertEqual(sum(1 for r in results if r is None), 6)


if __name__ == "__main__":
    unittest.main()
