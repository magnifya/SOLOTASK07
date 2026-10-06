"""Tests for per-run single-run concurrency quotas (``max_parallelism``)."""

import io
import json
import os
import shutil
import tempfile
import threading
import unittest
from contextlib import redirect_stdout

from flowd.cli import main
from flowd.model import WorkflowError
from flowd.scheduler import Scheduler
from flowd.store import STEP_RUNNING, WorkflowStore, atomic_write_json

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


class QuotaTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-quota-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def workflow(self, workflow_id="fan", steps=None):
        return self.scheduler.submit("acme", workflow_id, steps or FAN)

    def start(self, max_parallelism=None, workflow_id="fan", run_id="r1", tenant="acme"):
        return self.scheduler.start_run(tenant, workflow_id, run_id=run_id,
                                        max_parallelism=max_parallelism)

    def claim(self, run_id="r1", worker="w1", lease=30, tenant="acme"):
        return self.scheduler.claim(tenant, run_id, worker, lease)


class QuotaConfigTest(QuotaTestBase):
    def test_quota_is_stored_and_reported(self):
        self.workflow()
        run = self.start(max_parallelism=2)
        self.assertEqual(run["max_parallelism"], 2)
        self.assertEqual(self.scheduler.get_run("acme", "r1")["max_parallelism"], 2)

    def test_omitted_or_null_quota_means_unlimited(self):
        self.workflow()
        self.assertIsNone(self.scheduler.start_run("acme", "fan", run_id="r1")
                          ["max_parallelism"])
        self.assertIsNone(self.scheduler.start_run("acme", "fan", run_id="r2",
                                                   max_parallelism=None)["max_parallelism"])

    def test_invalid_quota_is_rejected_and_creates_no_run(self):
        self.workflow()
        for bad in (True, False, 1.5, 0.0, "2", 0, -1, -3, [], {}):
            with self.assertRaises(WorkflowError) as ctx:
                self.start(max_parallelism=bad, run_id="bad-%r" % (bad,))
            self.assertEqual(ctx.exception.code, "bad_max_parallelism")
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])

    def test_old_run_without_field_reads_as_null(self):
        self.workflow()
        run = self.start(max_parallelism=1)
        path = os.path.join(self.root, "acme", "runs", "r1.json")
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        del doc["max_parallelism"]
        atomic_write_json(path, doc)
        reloaded = self.scheduler.get_run("acme", run["run_id"])
        self.assertIsNone(reloaded["max_parallelism"])
        # unlimited: every independent task can be held at once
        self.assertEqual(self.claim()["id"], "a")
        self.assertEqual(self.claim(worker="w2")["id"], "b")
        self.assertEqual(self.claim(worker="w3")["id"], "c")

    def test_quota_is_fixed_after_creation(self):
        self.workflow()
        run = self.start(max_parallelism=1)
        self.claim()
        self.scheduler.complete("acme", "r1", "a", "w1")
        self.assertEqual(self.scheduler.get_run("acme", "r1")["max_parallelism"], 1)


class QuotaEnforcementTest(QuotaTestBase):
    def test_quota_blocks_claims_and_keeps_ready_steps_ready(self):
        self.workflow()
        self.start(max_parallelism=1)
        first = self.claim()
        self.assertEqual(first["id"], "a")
        history_before = len(self.scheduler.history("r1", "acme"))
        blocked = self.claim(worker="w2")
        self.assertIsNone(blocked)
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        self.assertEqual(run["steps"]["c"]["status"], "ready")
        # a blocked claim adds no claim history of its own
        history = self.scheduler.history("r1", "acme")
        self.assertEqual(len(history), history_before)
        self.assertEqual([e for e in history if e["type"] == "claim"],
                         [e for e in history[:history_before] if e["type"] == "claim"])

    def test_completing_releases_a_slot(self):
        self.workflow()
        self.start(max_parallelism=1)
        self.assertEqual(self.claim()["id"], "a")
        self.assertIsNone(self.claim(worker="w2"))
        self.scheduler.complete("acme", "r1", "a", "w1")
        self.assertEqual(self.claim(worker="w2")["id"], "b")

    def test_failing_releases_a_slot_via_retry(self):
        self.workflow("flaky", [{"id": "a", "depends_on": [], "max_attempts": 3},
                                {"id": "b", "depends_on": []}])
        self.start(workflow_id="flaky", max_parallelism=1)
        self.assertEqual(self.claim()["id"], "a")
        self.assertIsNone(self.claim(worker="w2"))
        self.scheduler.fail("acme", "r1", "a", "w1", "boom")
        # the retry slot is freed, but "a" is backing off; the due task "b"
        # is handed out instead of blocking behind the waiting retry
        again = self.claim(worker="w2")
        self.assertEqual((again["id"], again["status"], again["attempt"]), ("b", "running", 0))
        self.scheduler.complete("acme", "r1", "b", "w2")
        self.clock.advance(1)  # backoff of "a" elapses
        retried = self.claim(worker="w2")
        self.assertEqual((retried["id"], retried["attempt"]), ("a", 1))

    def test_same_worker_holding_multiple_tasks_is_counted_per_task(self):
        self.workflow()
        self.start(max_parallelism=2)
        self.assertEqual(self.claim(worker="w1")["id"], "a")
        self.assertEqual(self.claim(worker="w1")["id"], "b")
        self.assertIsNone(self.claim(worker="w1"))
        self.scheduler.complete("acme", "r1", "a", "w1")
        self.assertEqual(self.claim(worker="w1")["id"], "c")

    def test_approval_nodes_do_not_consume_quota(self):
        self.scheduler.submit("acme", "gated", [
            {"id": "a", "depends_on": []},
            {"id": "g", "depends_on": [], "kind": "approval"},
            {"id": "b", "depends_on": []},
        ])
        self.start(workflow_id="gated", max_parallelism=1)
        self.assertEqual(self.claim()["id"], "a")
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["g"]["status"], "waiting")
        # deciding the approval does not free the task slot
        self.scheduler.decide("acme", "r1", "g", "boss", "approve")
        self.assertIsNone(self.claim(worker="w2"))
        self.scheduler.complete("acme", "r1", "a", "w1")
        self.assertEqual(self.claim(worker="w2")["id"], "b")

    def test_heartbeat_neither_takes_nor_releases_a_slot(self):
        self.workflow()
        self.start(max_parallelism=1)
        self.claim(lease=30)
        self.clock.advance(10)
        self.scheduler.heartbeat("acme", "r1", "a", "w1", 60)
        self.assertIsNone(self.claim(worker="w2"))
        self.clock.advance(60)  # original deadline long gone, renewed deadline reached
        self.assertEqual(self.claim(worker="w2", lease=5)["id"], "a")

    def test_expired_lease_frees_the_slot_and_keeps_takeover_history(self):
        self.workflow()
        self.start(max_parallelism=1)
        self.claim(worker="w1", lease=5)
        self.assertIsNone(self.claim(worker="w2"))
        self.clock.advance(5)  # deadline == now counts as expired
        taken = self.claim(worker="w2", lease=5)
        self.assertEqual((taken["id"], taken["worker_id"]), ("a", "w2"))
        events = self.scheduler.history("r1", "acme")
        takeovers = [e for e in events if e["type"] == "takeover"]
        self.assertEqual(len(takeovers), 1)
        self.assertEqual(takeovers[0]["worker_id"], "w1")

    def test_quota_is_isolated_per_run_and_tenant(self):
        self.workflow()
        self.scheduler.submit("globex", "fan", FAN)
        self.start(max_parallelism=1, run_id="r1")
        self.start(max_parallelism=1, run_id="r2")
        globex = self.scheduler.start_run("globex", "fan", run_id="r3", max_parallelism=1)
        for run_id, tenant in (("r1", "acme"), ("r2", "acme"),
                               (globex["run_id"], "globex")):
            self.assertEqual(self.claim(run_id, tenant=tenant)["id"], "a")
            self.assertIsNone(self.claim(run_id, worker="w2", tenant=tenant))
        # completing in one run does not affect the others
        self.scheduler.complete("acme", "r1", "a", "w1")
        self.assertEqual(self.claim("r1", worker="w9")["id"], "b")
        self.assertIsNone(self.claim("r2", worker="w9"))

    def test_unlimited_quota_allows_every_ready_lease(self):
        self.workflow()
        self.start()  # no quota
        for expected, worker in (("a", "w1"), ("b", "w2"), ("c", "w3")):
            self.assertEqual(self.claim(worker=worker)["id"], expected)
        self.assertIsNone(self.claim(worker="w4"))

    def test_bad_claim_arguments_do_not_consume_a_slot(self):
        self.workflow()
        self.start(max_parallelism=1)
        for bad in (lambda: self.scheduler.claim("acme", "r1", "  ", 30),
                    lambda: self.scheduler.claim("acme", "r1", "w1", 0),
                    lambda: self.scheduler.claim("acme", "r1", "w1", True)):
            with self.assertRaises(WorkflowError):
                bad()
        self.assertEqual(self.claim(worker="w1")["id"], "a")

    def test_concurrent_claims_never_oversubscribe(self):
        self.scheduler.submit("acme", "wide",
                              [{"id": "t%02d" % i, "depends_on": []} for i in range(6)])
        self.start(workflow_id="wide", run_id="r1", max_parallelism=2)
        results = []
        barrier = threading.Barrier(6)

        def grab(worker):
            barrier.wait()
            results.append(self.claim(worker=worker))

        threads = [threading.Thread(target=grab, args=("w%d" % i,)) for i in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        held = [s for s in self.scheduler.get_run("acme", "r1")["steps"].values()
                if s["status"] == STEP_RUNNING]
        self.assertEqual(len(held), 2)
        self.assertEqual(sum(1 for r in results if r is None), 4)


class QuotaReplayAndRestartTest(QuotaTestBase):
    def test_replay_preserves_quota_config_and_lease_state(self):
        self.workflow()
        self.start(max_parallelism=1)
        self.claim(worker="w1", lease=30)
        replayed = self.scheduler.replay("acme", "r1")
        self.assertEqual(replayed["max_parallelism"], 1)
        step = replayed["steps"]["a"]
        stored = self.scheduler.get_run("acme", "r1")
        self.assertEqual((step["status"], step["worker_id"], step["lease_deadline"]),
                         ("running", "w1", stored["steps"]["a"]["lease_deadline"]))

    def test_restarted_store_keeps_counting_existing_leases(self):
        self.workflow()
        self.start(max_parallelism=1)
        self.claim(worker="w1", lease=30)
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        self.assertIsNone(restarted.claim("acme", "r1", "w2", 30))
        self.clock.advance(30)
        self.assertEqual(restarted.claim("acme", "r1", "w2", 5)["id"], "a")


class QuotaCliTest(QuotaTestBase):
    def cli(self, *argv):
        output = io.StringIO()
        with redirect_stdout(output):
            code = main(["--data-dir", self.root] + list(argv))
        self.assertEqual(code, 0, output.getvalue())
        return json.loads(output.getvalue())

    def test_status_shows_max_parallelism(self):
        self.workflow()
        self.start(max_parallelism=1)
        body = self.cli("status", "--run-id", "r1", "--tenant", "acme")
        self.assertEqual(body["max_parallelism"], 1)

    def test_status_shows_null_for_old_run(self):
        self.workflow()
        self.start(max_parallelism=1)
        path = os.path.join(self.root, "acme", "runs", "r1.json")
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        del doc["max_parallelism"]
        atomic_write_json(path, doc)
        body = self.cli("status", "--run-id", "r1", "--tenant", "acme")
        self.assertIsNone(body["max_parallelism"])


if __name__ == "__main__":
    unittest.main()
