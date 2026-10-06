"""Tests for the tenant-scoped fair claim (``Scheduler.claim_fair``).

One call leases at most one task, chosen from the tenant's least-claimed
eligible run (claim-event count, then ``created_at``, then ``run_id``).
The count is rebuilt from the append-only history, so restarts, replay and
concurrent callers all agree.
"""

import shutil
import tempfile
import threading
import unittest

from flowd.model import WorkflowError
from flowd.scheduler import ConflictError, Scheduler
from flowd.store import WorkflowStore

FAN = [{"id": "a", "depends_on": []}, {"id": "b", "depends_on": []}]
SOLO = [{"id": "only", "depends_on": [], "max_attempts": 1}]
FLAKY = [{"id": "only", "depends_on": [], "max_attempts": 3}]
APPROVAL = [{"id": "gate", "depends_on": [], "kind": "approval"}]


class FakeClock:
    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value


class FairClaimTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-fair-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def submit(self, steps=None, workflow_id="wf", tenant="acme"):
        return self.scheduler.submit(tenant, workflow_id, steps or FAN)

    def start(self, run_id, tenant="acme", workflow_id="wf", **kwargs):
        return self.scheduler.start_run(tenant, workflow_id, run_id=run_id, **kwargs)

    def claim(self, tenant="acme", worker="w1", lease=30):
        return self.scheduler.claim_fair(tenant, worker, lease)

    def history_types(self, run_id, tenant="acme"):
        return [e["type"] for e in self.scheduler.get_run(tenant, run_id)["history"]]

    # -- fairness order ----------------------------------------------
    def test_rotates_by_claim_count_then_created_at_then_run_id(self):
        self.submit()
        self.start("r-a")
        self.start("r-b")
        # 0 claims each and equal created_at -> run_id order wins.
        first = self.claim()
        self.assertEqual(first[0], "r-a")
        self.assertEqual(first[1]["id"], "a")
        # r-b now has fewer successful claims.
        self.assertEqual(self.claim()[0], "r-b")
        # Tied again at one claim each -> created_at/run_id order.
        self.assertEqual(self.claim()[0], "r-a")
        self.assertEqual(self.claim()[0], "r-b")
        # Both runs drained: no candidate, no new claim event.
        self.assertIsNone(self.claim())
        self.assertEqual(self.history_types("r-a").count("claim"), 2)
        self.assertEqual(self.history_types("r-b").count("claim"), 2)

    def test_created_at_breaks_ties_before_run_id(self):
        self.submit()
        self.start("r-z")
        self.clock.advance(5)
        self.start("r-a")
        # r-z is older, so it wins despite the later run_id.
        self.assertEqual(self.claim()[0], "r-z")

    def test_single_run_follows_topological_then_step_id_order(self):
        self.submit()
        self.start("r1")
        self.assertEqual(self.claim()[1]["id"], "a")
        self.assertEqual(self.claim()[1]["id"], "b")
        self.assertIsNone(self.claim())

    # -- eligibility ---------------------------------------------------
    def test_quota_full_run_is_not_a_candidate(self):
        self.submit()
        self.start("r1", max_parallelism=1)
        self.assertEqual(self.claim()[1]["id"], "a")
        self.assertIsNone(self.claim())  # the single slot is held
        self.clock.advance(31)  # lease expires
        again = self.claim()
        self.assertEqual(again[1]["id"], "a")  # reclaimed, then leased again
        self.assertIn("takeover", self.history_types("r1"))

    def test_takeover_recorded_on_scanned_run_that_loses_the_race(self):
        self.submit()
        self.start("r1")
        self.scheduler.claim("acme", "r1", "w1", lease_seconds=10)
        self.start("r2")
        self.clock.advance(20)  # r1's lease is expired
        # r2 has fewer claims and wins, but r1's expired lease is reclaimed.
        self.assertEqual(self.claim()[0], "r2")
        run = self.scheduler.get_run("acme", "r1")
        self.assertIn("takeover", [e["type"] for e in run["history"]])
        self.assertEqual(run["steps"]["a"]["status"], "ready")
        self.assertIsNone(run["steps"]["a"]["worker_id"])

    def test_terminal_run_is_not_a_candidate(self):
        self.submit(SOLO, workflow_id="solo")
        self.start("r1", workflow_id="solo")
        self.scheduler.claim("acme", "r1", "w1")
        self.scheduler.fail("acme", "r1", "only", "w1", error="boom")
        self.assertEqual(self.scheduler.get_run("acme", "r1")["status"], "failed")
        self.assertIsNone(self.claim())

    def test_approval_only_run_is_not_a_candidate(self):
        self.submit(APPROVAL, workflow_id="gate-wf")
        self.start("r1", workflow_id="gate-wf")
        self.assertIsNone(self.claim())
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["gate"]["status"], "waiting")

    # -- delayed start ---------------------------------------------------
    def test_future_not_before_run_is_not_a_candidate(self):
        self.submit()
        self.start("r1", not_before=self.clock.value + 100)
        self.assertIsNone(self.claim())
        # Nothing was activated: history and state are untouched.
        self.assertEqual(self.history_types("r1"), ["run_created"])

    def test_due_sleeping_run_is_activated_then_claimed(self):
        self.submit()
        self.start("r1", not_before=self.clock.value + 100)
        self.clock.advance(100)
        result = self.claim()
        self.assertEqual(result[0], "r1")
        self.assertEqual(result[1]["id"], "a")
        self.assertEqual(self.history_types("r1"),
                         ["run_created", "ready", "ready", "claim", "run_started"])

    def test_all_due_sleeping_runs_activate_even_when_not_chosen(self):
        self.submit()
        self.start("r-b", not_before=self.clock.value + 50)
        self.start("r-a", not_before=self.clock.value + 50)
        self.clock.advance(50)
        # Equal claim counts and created_at -> r-a wins on run_id.
        self.assertEqual(self.claim()[0], "r-a")
        # Both sleeping runs were activated by the same call.
        self.assertIn("ready", self.history_types("r-a"))
        self.assertIn("ready", self.history_types("r-b"))

    def test_due_sleeping_approval_run_activates_but_is_not_a_candidate(self):
        self.submit(APPROVAL, workflow_id="gate-wf")
        self.start("r1", workflow_id="gate-wf", not_before=self.clock.value + 10)
        self.clock.advance(10)
        self.assertIsNone(self.claim())
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["gate"]["status"], "waiting")
        self.assertIn("waiting", self.history_types("r1"))

    # -- retry backoff ---------------------------------------------------
    def test_run_with_only_a_waiting_retry_is_skipped(self):
        self.submit(FLAKY, workflow_id="flaky")
        self.submit()
        self.start("r-retry", workflow_id="flaky")
        self.start("r-fresh")
        self.scheduler.claim("acme", "r-retry", "w1")
        self.scheduler.fail("acme", "r-retry", "only", "w1", error="boom")
        # r-retry's only task is waiting out its backoff: r-fresh wins, and
        # r-retry's fairness count (one claim) is unchanged.
        self.assertEqual(self.claim()[0], "r-fresh")
        self.assertEqual(self.history_types("r-retry").count("claim"), 1)
        self.assertEqual(self.claim()[0], "r-fresh")
        # r-fresh is drained and r-retry is still waiting: nothing to lease.
        self.assertIsNone(self.claim())
        self.clock.advance(1)  # backoff (1s) elapsed
        result = self.claim()
        self.assertEqual(result[0], "r-retry")
        self.assertEqual(result[1]["id"], "only")
        self.assertIsNone(result[1]["next_attempt_at"])

    def test_waiting_retry_inside_winning_run_does_not_block_due_steps(self):
        self.submit()
        self.start("r1")
        self.scheduler.claim("acme", "r1", "w1")
        self.scheduler.fail("acme", "r1", "a", "w1", error="boom")
        # "a" backs off, but "b" is due in the same run.
        result = self.claim()
        self.assertEqual((result[0], result[1]["id"]), ("r1", "b"))
        self.assertIsNone(self.claim())

    # -- worker registry gate -------------------------------------------
    def test_unregistered_worker_is_rejected_once_registry_exists(self):
        self.submit()
        self.start("r1")
        self.scheduler.register_worker("acme", "w1", lease_seconds=100)
        with self.assertRaises(ConflictError) as ctx:
            self.claim(worker="stranger")
        self.assertEqual(ctx.exception.code, "worker_not_registered")
        self.assertEqual(self.claim(worker="w1")[0], "r1")

    def test_expired_worker_registration_is_rejected(self):
        self.submit()
        self.start("r1")
        self.scheduler.register_worker("acme", "w1", lease_seconds=10)
        self.clock.advance(20)
        with self.assertRaises(ConflictError) as ctx:
            self.claim(worker="w1")
        self.assertEqual(ctx.exception.code, "worker_expired")

    # -- validation ------------------------------------------------------
    def test_validation(self):
        self.submit()
        self.start("r1")
        for bad_tenant in ("", "  ", 7, None):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.claim_fair(bad_tenant, "w1")
            self.assertEqual(ctx.exception.code, "bad_tenant")
        for bad_worker in ("", "   ", 9, None):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.claim_fair("acme", bad_worker)
            self.assertEqual(ctx.exception.code, "bad_worker")
        for bad_lease in (0, -1, "30", True, float("nan"), float("inf")):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.claim_fair("acme", "w1", bad_lease)
            self.assertEqual(ctx.exception.code, "bad_lease")
        # Omitted lease defaults to 30; tenant/worker_id are trimmed.
        run_id, step = self.scheduler.claim_fair(" acme ", " w1 ")
        self.assertEqual(run_id, "r1")
        self.assertEqual(step["worker_id"], "w1")
        self.assertEqual(step["lease_deadline"], self.clock.value + 30)

    # -- determinism -----------------------------------------------------
    def test_restart_and_replay_keep_the_same_choice(self):
        self.submit()
        self.start("r-a")
        self.start("r-b")
        self.assertEqual(self.claim()[0], "r-a")
        self.assertEqual(self.claim()[0], "r-b")
        # Replay agrees with the stored documents.
        for rid in ("r-a", "r-b"):
            stored = self.scheduler.get_run("acme", rid)
            rebuilt = self.scheduler.replay("acme", rid)
            self.assertEqual(rebuilt["status"], stored["status"])
            for sid in stored["steps"]:
                self.assertEqual(rebuilt["steps"][sid]["status"],
                                 stored["steps"][sid]["status"])
        # A fresh scheduler over the same directory rebuilds the fairness
        # counts from history and makes the same next pick.
        fresh = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        self.assertEqual(fresh.claim_fair("acme", "w1")[0], "r-a")
        # The fresh claim persisted: r-a now has 2 claims, r-b has 1.
        self.assertEqual(self.claim()[0], "r-b")

    def test_concurrent_claims_are_serialized(self):
        self.submit()
        self.start("r1")
        self.start("r2")
        results = []

        def worker():
            try:
                results.append(self.claim())
            except Exception as exc:  # pragma: no cover - defensive
                results.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertFalse([r for r in results if isinstance(r, Exception)])
        got = [r for r in results if r is not None]
        self.assertEqual(len(got), 4)  # two runs x two tasks, one lease per call
        self.assertEqual(len({(r[0], r[1]["id"]) for r in got}), 4)  # no double lease

    # -- isolation ---------------------------------------------------------
    def test_runs_of_other_tenants_are_invisible(self):
        self.submit()
        self.start("r1")
        self.submit(tenant="globex")
        self.start("g1", tenant="globex")
        self.assertEqual(self.claim()[0], "r1")
        self.assertIsNone(self.claim(tenant="other"))  # tenant without runs
        # The other tenant's run was never touched.
        self.assertEqual(self.history_types("g1", tenant="globex"),
                         ["run_created", "ready", "ready"])


if __name__ == "__main__":
    unittest.main()
