"""Tests for tenant worker registration, health and claim gating."""

import shutil
import tempfile
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


class WorkerRegistryTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-workers-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def test_register_creates_then_refreshes(self):
        created, record = self.scheduler.register_worker("acme", "  w1  ", 30)
        self.assertTrue(created)
        self.assertEqual(record["worker_id"], "w1")  # trimmed value is stored
        self.assertEqual(record["status"], "active")
        self.assertEqual(set(record), {"worker_id", "status", "registered_at",
                                       "last_heartbeat", "expires_at"})
        self.assertAlmostEqual(record["expires_at"], self.clock() + 30)
        first_registered = record["registered_at"]

        self.clock.advance(10)
        created, again = self.scheduler.register_worker("acme", "w1", 30)
        self.assertFalse(created)
        self.assertEqual(again["status"], "active")
        self.assertEqual(again["registered_at"], first_registered)  # registration kept
        self.assertAlmostEqual(again["expires_at"], self.clock() + 30)

    def test_register_default_lease_is_thirty_seconds(self):
        _, record = self.scheduler.register_worker("acme", "w1")
        self.assertAlmostEqual(record["expires_at"], self.clock() + 30)

    def test_register_rejects_bad_input(self):
        for payload in (("", "w1"), ("acme", ""), ("acme", "   "), (None, "w1")):
            with self.assertRaises(WorkflowError):
                self.scheduler.register_worker(payload[0], payload[1])
        for lease in (0, -1, True, "30", float("nan"), float("inf")):
            with self.assertRaises(WorkflowError):
                self.scheduler.register_worker("acme", "w1", lease)
        # Nothing was written for the tenant.
        self.assertEqual(self.scheduler.list_workers("acme"), [])

    def test_heartbeat_uses_later_deadline_and_404_409(self):
        self.scheduler.register_worker("acme", "w1", 30)
        self.clock.advance(10)
        record = self.scheduler.worker_heartbeat("acme", "w1", 5)  # would shorten
        self.assertAlmostEqual(record["expires_at"], 1_700_000_030.0)
        record = self.scheduler.worker_heartbeat("acme", " w1 ", 60)  # trimmed match, extends
        self.assertAlmostEqual(record["expires_at"], self.clock() + 60)

        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.worker_heartbeat("acme", "ghost")
        self.assertEqual(ctx.exception.code, "unknown_worker")

        self.clock.advance(10_000)
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.worker_heartbeat("acme", "w1", 30)
        self.assertEqual(ctx.exception.code, "worker_expired")

        self.scheduler.register_worker("acme", "fresh", 30)
        for lease in (0, True, "x", float("inf")):
            with self.assertRaises(WorkflowError):
                self.scheduler.worker_heartbeat("acme", "fresh", lease)

    def test_list_sorted_with_live_status(self):
        self.scheduler.register_worker("acme", "b", 10)
        self.scheduler.register_worker("acme", "a", 10)
        self.scheduler.register_worker("globex", "z", 10)  # other tenant isolated
        items = self.scheduler.list_workers("acme")
        self.assertEqual([w["worker_id"] for w in items], ["a", "b"])
        self.assertTrue(all(w["status"] == "active" for w in items))

        self.clock.advance(11)
        items = self.scheduler.list_workers("acme")  # expired, records retained
        self.assertEqual([w["worker_id"] for w in items], ["a", "b"])
        self.assertTrue(all(w["status"] == "expired" for w in items))
        with self.assertRaises(WorkflowError):
            self.scheduler.list_workers("  ")

    def test_registry_survives_store_reopen(self):
        self.scheduler.register_worker("acme", "w1", 30)
        self.clock.advance(5)
        reopened = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        items = reopened.list_workers("acme")
        self.assertEqual([w["worker_id"] for w in items], ["w1"])
        self.assertEqual(items[0]["status"], "active")
        record = reopened.worker_heartbeat("acme", "w1", 60)
        self.assertAlmostEqual(record["expires_at"], self.clock() + 60)
        # A second reopen sees the heartbeat too.
        reopened2 = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        self.assertAlmostEqual(reopened2.list_workers("acme")[0]["expires_at"],
                               self.clock() + 60)


class WorkerClaimGateTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-gate-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)
        self.scheduler.submit("acme", "etl", LINEAR)
        self.run = self.scheduler.start_run("acme", "etl", run_id="r1")
        self.scheduler.submit("globex", "etl", LINEAR)
        self.other = self.scheduler.start_run("globex", "etl", run_id="r2")

    def test_legacy_rule_until_any_registration_exists(self):
        # No workers registered anywhere: non-empty id still claims.
        self.assertEqual(self.scheduler.claim("acme", "r1", "anyone")["id"], "step1")

    def test_unregistered_and_expired_workers_rejected(self):
        self.scheduler.register_worker("acme", "known", 30)
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.claim("acme", "r1", "stranger")
        self.assertEqual(ctx.exception.code, "worker_not_registered")

        self.assertEqual(self.scheduler.claim("acme", "r1", "known")["id"], "step1")
        self.scheduler.complete("acme", "r1", "step1", "known")

        self.clock.advance(31)
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.claim("acme", "r1", "known")
        self.assertEqual(ctx.exception.code, "worker_expired")

        # Re-registering refreshes expiry and claims work again.
        self.scheduler.register_worker("acme", "known", 30)
        self.assertEqual(self.scheduler.claim("acme", "r1", "known")["id"], "step2")

    def test_registration_is_per_tenant(self):
        self.scheduler.register_worker("acme", "w1", 30)
        # Claiming another tenant's run is always 403.
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.claim("globex", "r1", "w1")
        self.assertEqual(ctx.exception.code, "cross_tenant")
        # A worker registered for globex still cannot touch acme's run.
        self.scheduler.register_worker("globex", "g1", 30)
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.claim("globex", "r1", "g1")
        self.assertEqual(ctx.exception.code, "cross_tenant")
        # acme registration does not make w1 known in globex.
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.claim("globex", "r2", "w1")
        self.assertEqual(ctx.exception.code, "worker_not_registered")
        with self.assertRaises(ConflictError) as ctx:
            self.scheduler.claim("globex", "r2", "stranger")
        self.assertEqual(ctx.exception.code, "worker_not_registered")
        self.assertEqual(self.scheduler.claim("globex", "r2", "g1")["id"], "step1")

    def test_rejected_claims_write_no_history(self):
        self.scheduler.register_worker("acme", "w1", 30)
        with self.assertRaises(ConflictError):
            self.scheduler.claim("acme", "r1", "stranger")
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual([e["type"] for e in run["history"]], ["run_created", "ready"])
        self.assertEqual(run["steps"]["step1"]["status"], "ready")


if __name__ == "__main__":
    unittest.main()
