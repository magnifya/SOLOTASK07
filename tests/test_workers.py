"""Tests for tenant-scoped worker registration, heartbeats and claim gating."""

import shutil
import tempfile
import unittest

from flowd.model import WorkflowError
from flowd.scheduler import (
    ConflictError,
    Scheduler,
    WORKER_ACTIVE,
    WORKER_EXPIRED,
)
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
        self.start = self.clock()

    def fields(self, record):
        return set(record)

    # -- registration --------------------------------------------------
    def test_first_registration_is_active_and_created(self):
        record, created = self.scheduler.register_worker("acme", "w1")
        self.assertTrue(created)
        self.assertEqual(self.fields(record),
                         {"worker_id", "status", "registered_at",
                          "last_heartbeat", "expires_at"})
        self.assertEqual(record["worker_id"], "w1")
        self.assertEqual(record["status"], WORKER_ACTIVE)
        self.assertEqual(record["registered_at"], record["last_heartbeat"])
        self.assertGreater(record["expires_at"], record["registered_at"])

    def test_worker_id_and_tenant_are_trimmed(self):
        record, created = self.scheduler.register_worker("  acme  ", "  w1 \t")
        self.assertTrue(created)
        self.assertEqual(record["worker_id"], "w1")
        self.assertEqual([r["worker_id"] for r in self.scheduler.list_workers("acme")], ["w1"])

    def test_default_lease_is_thirty_seconds(self):
        record, _ = self.scheduler.register_worker("acme", "w1")
        deadline = self.clock() + 30.0
        self.assertEqual(record["expires_at"], self.scheduler._iso(deadline))

    def test_repeat_registration_returns_not_created_and_refreshes(self):
        first, created1 = self.scheduler.register_worker("acme", "w1", lease_seconds=30)
        self.assertTrue(created1)
        self.clock.advance(10)
        second, created2 = self.scheduler.register_worker("acme", "w1", lease_seconds=30)
        self.assertFalse(created2)
        self.assertEqual(second["worker_id"], "w1")
        self.assertEqual(second["registered_at"], first["registered_at"])
        self.assertGreater(second["last_heartbeat"], first["last_heartbeat"])
        self.assertGreater(second["expires_at"], first["expires_at"])
        self.assertEqual(len(self.scheduler.list_workers("acme")), 1)

    def test_invalid_registration_inputs_are_rejected(self):
        for tenant, worker_id, lease in (
            (None, "w1", 30),
            ("", "w1", 30),
            ("   ", "w1", 30),
            (7, "w1", 30),
            ("acme", None, 30),
            ("acme", "", 30),
            ("acme", "   ", 30),
            ("acme", 9, 30),
            ("acme", "w1", 0),
            ("acme", "w1", -1),
            ("acme", "w1", True),
            ("acme", "w1", "30"),
            ("acme", "w1", float("nan")),
            ("acme", "w1", float("inf")),
        ):
            with self.assertRaises(WorkflowError) as caught:
                self.scheduler.register_worker(tenant, worker_id, lease)
            self.assertIn(caught.exception.code,
                          ("bad_tenant", "bad_worker", "bad_lease"), (tenant, worker_id, lease))
        # Nothing was written for the tenant.
        self.assertEqual(self.scheduler.list_workers("acme"), [])

    def test_omitted_lease_defaults_at_python_api(self):
        # The Python method treats None as "omitted" (30s); the HTTP layer
        # rejects an explicit JSON null before it reaches the scheduler.
        record, _ = self.scheduler.register_worker("acme", "w1", None)
        self.assertEqual(record["expires_at"], self.scheduler._iso(self.clock() + 30))

    # -- live status ---------------------------------------------------
    def test_status_is_computed_live(self):
        record, _ = self.scheduler.register_worker("acme", "w1", lease_seconds=30)
        self.assertEqual(record["status"], WORKER_ACTIVE)
        self.assertEqual(self.scheduler.list_workers("acme")[0]["status"], WORKER_ACTIVE)
        self.clock.advance(30)  # now == expires_at: strictly less-than means expired
        self.assertEqual(self.scheduler.list_workers("acme")[0]["status"], WORKER_EXPIRED)

    def test_list_is_sorted_by_worker_id_and_tenant_scoped(self):
        for worker_id in ("w3", "w1", "w2"):
            self.scheduler.register_worker("acme", worker_id, lease_seconds=30)
        self.scheduler.register_worker("globex", "g1", lease_seconds=30)
        self.assertEqual([r["worker_id"] for r in self.scheduler.list_workers("acme")],
                         ["w1", "w2", "w3"])
        self.assertEqual([r["worker_id"] for r in self.scheduler.list_workers("globex")], ["g1"])
        self.assertEqual(self.scheduler.list_workers("other"), [])

    # -- worker heartbeat ----------------------------------------------
    def test_heartbeat_extends_to_later_deadline(self):
        first, _ = self.scheduler.register_worker("acme", "w1", lease_seconds=100)
        self.clock.advance(10)
        renewed = self.scheduler.heartbeat_worker("acme", "w1", lease_seconds=5)
        # now+5 < old deadline, so the expiry does not move backwards...
        self.assertEqual(renewed["expires_at"], first["expires_at"])
        self.assertGreater(renewed["last_heartbeat"], first["last_heartbeat"])
        longer = self.scheduler.heartbeat_worker("acme", "w1", lease_seconds=200)
        self.assertEqual(longer["expires_at"],
                         self.scheduler._iso(self.clock() + 200))
        self.assertGreater(longer["expires_at"], first["expires_at"])

    def test_heartbeat_defaults_to_thirty_seconds(self):
        self.scheduler.register_worker("acme", "w1", lease_seconds=1)
        record = self.scheduler.heartbeat_worker("acme", "w1")
        self.assertEqual(record["expires_at"], self.scheduler._iso(self.clock() + 30))

    def test_heartbeat_unknown_and_expired(self):
        with self.assertRaises(WorkflowError) as caught:
            self.scheduler.heartbeat_worker("acme", "ghost")
        self.assertEqual(caught.exception.code, "unknown_worker")
        self.scheduler.register_worker("acme", "w1", lease_seconds=10)
        self.clock.advance(10)
        with self.assertRaises(ConflictError) as caught:
            self.scheduler.heartbeat_worker("acme", "w1")
        self.assertEqual(caught.exception.code, "worker_expired")
        # The failed heartbeat must not have revived the record.
        self.assertEqual(self.scheduler.list_workers("acme")[0]["status"], WORKER_EXPIRED)

    def test_re_registering_an_expired_worker_refreshes(self):
        self.scheduler.register_worker("acme", "w1", lease_seconds=10)
        self.clock.advance(11)
        record, created = self.scheduler.register_worker("acme", "w1", lease_seconds=30)
        self.assertFalse(created)
        self.assertEqual(record["status"], WORKER_ACTIVE)

    # -- persistence ---------------------------------------------------
    def test_registry_survives_restart(self):
        self.scheduler.register_worker("acme", "w1", lease_seconds=100)
        self.scheduler.register_worker("acme", "w2", lease_seconds=10)
        self.clock.advance(50)
        restarted_store = WorkflowStore(self.root, clock=self.clock)
        restarted = Scheduler(restarted_store, clock=self.clock)
        items = restarted.list_workers("acme")
        self.assertEqual([(r["worker_id"], r["status"]) for r in items],
                         [("w1", WORKER_ACTIVE), ("w2", WORKER_EXPIRED)])
        # A repeat registration against the restarted process is a refresh.
        _, created = restarted.register_worker("acme", "w1", lease_seconds=100)
        self.assertFalse(created)

    # -- claim gate ----------------------------------------------------
    def _run(self, tenant="acme", run_id="r1"):
        self.scheduler.submit(tenant, "etl", LINEAR)
        return self.scheduler.start_run(tenant, "etl", run_id=run_id)

    def test_claim_keeps_legacy_rule_without_any_registration(self):
        self._run()
        self.assertEqual(self.scheduler.claim("acme", "r1", "anyone")["id"], "step1")

    def test_claim_requires_registration_once_registry_exists(self):
        self._run()
        self.scheduler.register_worker("acme", "w1", lease_seconds=30)
        with self.assertRaises(ConflictError) as caught:
            self.scheduler.claim("acme", "r1", "stranger")
        self.assertEqual(caught.exception.code, "worker_not_registered")
        # The rejected claim did not change the run.
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["step1"]["status"], "ready")

    def test_claim_rejects_expired_worker(self):
        self._run()
        self.scheduler.register_worker("acme", "w1", lease_seconds=10)
        self.clock.advance(10)
        with self.assertRaises(ConflictError) as caught:
            self.scheduler.claim("acme", "r1", "w1")
        self.assertEqual(caught.exception.code, "worker_expired")

    def test_active_registered_worker_claims(self):
        self._run()
        self.scheduler.register_worker("acme", "w1", lease_seconds=30)
        self.clock.advance(5)
        step = self.scheduler.claim("acme", "r1", "w1", lease_seconds=60)
        self.assertEqual(step["id"], "step1")
        self.assertEqual(step["worker_id"], "w1")

    def test_registry_is_tenant_scoped(self):
        self._run("acme", "r1")
        self._run("globex", "g1")
        self.scheduler.register_worker("acme", "w1", lease_seconds=30)
        # globex still has no registrations, so the legacy rule applies there.
        self.assertEqual(self.scheduler.claim("globex", "g1", "w1")["id"], "step1")
        # acme's registry does not accept that same id until globex enrolls.
        self.scheduler.register_worker("globex", "g1", lease_seconds=30)
        with self.assertRaises(ConflictError) as caught:
            self.scheduler.claim("globex", "g1", "w1")
        self.assertEqual(caught.exception.code, "worker_not_registered")

    def test_cross_tenant_claim_still_403(self):
        self._run("acme", "r1")
        self.scheduler.register_worker("globex", "w1", lease_seconds=30)
        with self.assertRaises(WorkflowError) as caught:
            self.scheduler.claim("globex", "r1", "w1")
        self.assertEqual(caught.exception.code, "cross_tenant")

    def test_registry_operations_write_no_run_history(self):
        self._run()
        before = len(self.scheduler.get_run("acme", "r1")["history"])
        updated_before = self.scheduler.get_run("acme", "r1")["updated_at"]
        self.scheduler.register_worker("acme", "w1", lease_seconds=30)
        self.scheduler.heartbeat_worker("acme", "w1")
        self.scheduler.list_workers("acme")
        self.scheduler.register_worker("acme", "w1", lease_seconds=60)
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(len(run["history"]), before)
        self.assertEqual(run["updated_at"], updated_before)

    def test_lease_heartbeat_and_takeover_ignore_registry(self):
        """Existing lease renewal / expiry takeover behavior is unchanged."""
        self._run()
        # Registration lives 5s but the task lease lives 100s.
        self.scheduler.register_worker("acme", "w1", lease_seconds=5)
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=100)
        self.clock.advance(6)  # registration expired, task lease still valid
        renewed = self.scheduler.heartbeat("acme", "r1", "step1", "w1",
                                           lease_seconds=200)
        self.assertGreater(renewed["steps"]["step1"]["lease_deadline"],
                           self.clock() + 190)
        # Another tenant worker then takes over only once the task lease ends;
        # registry expiry alone does not release the held lease.
        self.clock.advance(1000)
        self.scheduler.register_worker("acme", "w2", lease_seconds=30)
        step = self.scheduler.claim("acme", "r1", "w2", lease_seconds=30)
        self.assertEqual(step["id"], "step1")
        kinds = [e["type"] for e in self.scheduler.get_run("acme", "r1")["history"]]
        self.assertIn("takeover", kinds)


if __name__ == "__main__":
    unittest.main()
