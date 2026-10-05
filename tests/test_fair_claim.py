"""Scheduler tests for the tenant-scoped fair claim (``claim_next``).

Fairness key: fewest successful ``claim`` events in a run's append-only
history, then ``created_at``, then ``run_id``.  Due sleeping runs activate in
run_id order; terminal, still-sleeping, approval-only and quota-full runs are
never candidates; at most one task is leased per call.
"""

import shutil
import tempfile
import threading
import unittest

from flowd.model import WorkflowError
from flowd.scheduler import ConflictError, Scheduler
from flowd.store import STEP_READY, STEP_RUNNING, WorkflowStore

FAN = [{"id": "a", "depends_on": []}, {"id": "b", "depends_on": []},
       {"id": "c", "depends_on": []}]
SOLO = [{"id": "t", "depends_on": []}]
APPROVAL_ONLY = [{"id": "g", "depends_on": [], "kind": "approval"}]
MIXED = [{"id": "t", "depends_on": []}, {"id": "g", "depends_on": [], "kind": "approval"}]


class FakeClock:
    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value


class FairClaimTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-fair-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def submit(self, workflow_id="fan", steps=None, tenant="acme"):
        return self.scheduler.submit(tenant, workflow_id, FAN if steps is None else steps)

    def start(self, run_id, workflow_id="fan", tenant="acme", **kwargs):
        return self.scheduler.start_run(tenant, workflow_id, run_id=run_id, **kwargs)

    def claim_next(self, worker="w1", lease=30, tenant="acme"):
        return self.scheduler.claim_next(tenant, worker, lease)

    def claim(self, run_id, worker="w1", lease=30, tenant="acme"):
        return self.scheduler.claim(tenant, run_id, worker, lease)

    def history_types(self, run_id, tenant="acme"):
        return [e["type"] for e in self.scheduler.history(run_id, tenant)]

    def claim_count(self, run_id, tenant="acme"):
        return sum(1 for e in self.scheduler.history(run_id, tenant) if e["type"] == "claim")


class FairSelectionTest(FairClaimTestBase):
    def test_empty_tenant_returns_none_pair(self):
        self.submit()
        self.assertEqual(self.claim_next(), (None, None))

    def test_single_run_leases_first_root(self):
        self.submit()
        self.start("r1")
        run_id, step = self.claim_next()
        self.assertEqual((run_id, step["id"]), ("r1", "a"))
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["status"], "running")
        self.assertIn("run_started", self.history_types("r1"))

    def test_fewest_claim_events_wins_regardless_of_run_id(self):
        self.submit()
        self.start("r-zzz")
        self.clock.advance(1)
        self.start("r-aaa")
        # Both start with zero claim events; the tie breaks by created_at, so
        # the older run r-zzz wins first despite its lexicographically later id.
        self.assertEqual(self.claim_next()[0], "r-zzz")
        # Now r-aaa has fewer claims and is picked even though it sorts first.
        self.assertEqual(self.claim_next(worker="w2")[0], "r-aaa")
        # One each: the next two leases round-robin by run_id after counts tie.
        third = self.claim_next(worker="w3")[0]
        self.assertEqual(self.claim_next(worker="w4")[0],
                         "r-aaa" if third == "r-zzz" else "r-zzz")

    def test_created_at_tie_breaks_by_run_id(self):
        self.submit()
        self.start("r2")  # identical fake clock -> identical created_at
        self.start("r1")
        self.assertEqual(self.claim_next()[0], "r1")
        self.assertEqual(self.claim_next(worker="w2")[0], "r2")

    def test_single_run_claims_count_toward_fairness(self):
        self.submit()
        self.start("r1")
        self.start("r2")
        # Drain two tasks from r1 through the legacy entry point; its history
        # then holds two claim events, so the fair picker must prefer r2.
        self.assertEqual(self.claim("r1")["id"], "a")
        self.assertEqual(self.claim("r1", worker="w2")["id"], "b")
        self.assertEqual(self.claim_next()[0], "r2")

    def test_retried_step_keeps_accumulating_claim_events(self):
        self.submit(workflow_id="flaky",
                    steps=[{"id": "a", "depends_on": [], "max_attempts": 3}])
        self.start("busy", workflow_id="flaky")
        self.submit(workflow_id="solo", steps=SOLO)
        self.start("fresh", workflow_id="solo")
        self.assertEqual(self.claim("busy")["id"], "a")
        self.scheduler.fail("acme", "busy", "a", "w1", "boom")
        self.assertEqual(self.claim("busy", worker="w2")["id"], "a")
        # busy has two claim events; fresh has none.
        self.assertEqual(self.claim_next()[0], "fresh")

    def test_topology_order_inside_winning_run(self):
        self.submit()
        self.start("r1")
        self.assertEqual(self.claim_next()[1]["id"], "a")
        self.assertEqual(self.claim_next(worker="w2")[1]["id"], "b")
        self.assertEqual(self.claim_next(worker="w3")[1]["id"], "c")
        self.assertEqual(self.claim_next(worker="w4"), (None, None))

    def test_one_task_leased_per_call(self):
        self.submit()
        self.start("r1")
        run_id, _ = self.claim_next()
        self.assertEqual(run_id, "r1")
        held = [s for s in self.scheduler.get_run("acme", "r1")["steps"].values()
                if s["status"] == STEP_RUNNING]
        self.assertEqual(len(held), 1)


class FairCandidateFilterTest(FairClaimTestBase):
    def test_terminal_runs_are_skipped(self):
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("done", workflow_id="solo")
        self.assertEqual(self.claim("done")["id"], "t")
        self.scheduler.complete("acme", "done", "t", "w1")
        self.start("alive", workflow_id="solo")
        run_id, step = self.claim_next()
        self.assertEqual((run_id, step["id"]), ("alive", "t"))

    def test_failed_run_with_ready_siblings_is_not_a_candidate(self):
        self.submit(workflow_id="fatality",
                    steps=[{"id": "a", "depends_on": [], "max_attempts": 1},
                           {"id": "b", "depends_on": []}])
        self.start("dead", workflow_id="fatality")
        self.assertEqual(self.claim("dead")["id"], "a")
        self.scheduler.fail("acme", "dead", "a", "w1", "fatal")
        dead = self.scheduler.get_run("acme", "dead")
        self.assertEqual(dead["status"], "failed")
        self.assertEqual(dead["steps"]["b"]["status"], "ready")
        # The terminal run's ready sibling must never be leased by fair claim.
        self.assertEqual(self.claim_next(), (None, None))
        self.assertEqual(self.scheduler.get_run("acme", "dead")["steps"]["b"]["status"],
                         "ready")

    def test_takeover_persisted_on_losing_run_in_same_call(self):
        self.submit()
        self.start("r-old", max_parallelism=1)
        self.assertEqual(self.claim("r-old", worker="w1", lease=5)["id"], "a")
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("r-new", workflow_id="solo")
        self.clock.advance(6)  # r-old's lease expired; r-new has zero claims
        run_id, step = self.claim_next(worker="w2")
        self.assertEqual((run_id, step["id"]), ("r-new", "t"))
        # r-old lost the fairness comparison, but its expired lease was still
        # reclaimed and its takeover event persisted during the same scan.
        takeovers = [e for e in self.scheduler.history("r-old") if e["type"] == "takeover"]
        self.assertEqual(len(takeovers), 1)
        self.assertEqual(takeovers[0]["step_id"], "a")
        self.assertEqual(self.scheduler.get_run("acme", "r-old")["steps"]["a"]["status"],
                         "ready")

    def test_approval_only_run_activates_but_is_not_a_candidate(self):
        self.submit(steps=APPROVAL_ONLY, workflow_id="approvals")
        self.start("gated", workflow_id="approvals")
        self.assertEqual(self.claim_next(), (None, None))
        run = self.scheduler.get_run("acme", "gated")
        self.assertEqual(run["steps"]["g"]["status"], "waiting")
        self.assertEqual(self.history_types("gated"), ["run_created", "waiting"])
        # The activation is durable: a second call neither re-opens nor claims.
        self.assertEqual(self.claim_next(), (None, None))
        self.assertEqual(self.history_types("gated"), ["run_created", "waiting"])

    def test_waiting_approval_run_with_task_still_offers_the_task(self):
        self.submit(steps=MIXED, workflow_id="mixed")
        self.start("r1", workflow_id="mixed")
        run_id, step = self.claim_next()
        self.assertEqual((run_id, step["id"]), ("r1", "t"))
        run = self.scheduler.get_run("acme", "r1")
        self.assertEqual(run["steps"]["g"]["status"], "waiting")

    def test_quota_full_run_is_skipped_until_a_lease_expires(self):
        self.submit()
        self.start("full", max_parallelism=1)
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("other", workflow_id="solo")
        self.assertEqual(self.claim("full")["id"], "a")
        # full is at quota; its ready b/c must not be selected.
        self.assertEqual(self.claim_next()[0], "other")
        self.assertEqual(self.claim_next(), (None, None))
        self.clock.advance(30)  # full's lease deadline reached
        run_id, step = self.claim_next()
        self.assertEqual((run_id, step["id"]), ("full", "a"))
        takeovers = [e for e in self.scheduler.history("full") if e["type"] == "takeover"]
        self.assertEqual(len(takeovers), 1)
        self.assertEqual(takeovers[0]["worker_id"], "w1")

    def test_quota_full_with_no_other_candidate_reclaims_then_offers_task(self):
        self.submit()
        self.start("r1", max_parallelism=1)
        self.assertEqual(self.claim("r1", worker="w1", lease=5)["id"], "a")
        self.assertEqual(self.claim_next(worker="w2"), (None, None))
        self.clock.advance(5)
        run_id, step = self.claim_next(worker="w2", lease=10)
        self.assertEqual((run_id, step["id"]), ("r1", "a"))
        self.assertEqual(step["worker_id"], "w2")


class FairDelayedActivationTest(FairClaimTestBase):
    def test_future_sleeping_run_is_never_touched(self):
        self.submit()
        self.start("later", not_before=self.clock() + 60)
        before = self.scheduler.get_run("acme", "later")
        self.assertEqual(self.claim_next(), (None, None))
        after = self.scheduler.get_run("acme", "later")
        self.assertEqual(after, before)
        self.assertEqual(self.history_types("later"), ["run_created"])

    def test_due_sleeping_runs_activate_in_run_id_order_then_share_fairly(self):
        self.submit()
        self.start("r2", not_before=self.clock() + 60)
        self.start("r1", not_before=self.clock() + 60)
        self.assertEqual(self.claim_next(), (None, None))  # still sleeping
        self.clock.advance(60)
        # Both due runs are activated before selection; r1 wins the tie
        # (equal claim count and created_at -> run_id), but r2's roots opened
        # in the same scan.
        run_id, step = self.claim_next()
        self.assertEqual((run_id, step["id"]), ("r1", "a"))
        for rid in ("r1", "r2"):
            ready = [e["step_id"] for e in self.scheduler.history(rid)
                     if e["type"] == "ready"]
            self.assertEqual(ready, ["a", "b", "c"], rid)
        self.assertEqual(self.claim_next(worker="w2")[0], "r2")

    def test_due_approval_only_sleeping_run_activates_without_task(self):
        self.submit(steps=APPROVAL_ONLY, workflow_id="approvals")
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("gated", workflow_id="approvals", not_before=self.clock() + 60)
        self.start("worker-run", workflow_id="solo", not_before=self.clock() + 60)
        self.clock.advance(60)
        run_id, step = self.claim_next()
        self.assertEqual((run_id, step["id"]), ("worker-run", "t"))
        gated = self.scheduler.get_run("acme", "gated")
        self.assertEqual(gated["steps"]["g"]["status"], "waiting")
        self.assertIn("waiting", self.history_types("gated"))


class FairPersistenceTest(FairClaimTestBase):
    def test_restarted_scheduler_makes_the_same_choice(self):
        self.submit()
        self.start("r1")
        self.start("r2")
        self.assertEqual(self.claim("r1")["id"], "a")
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        # r1 has one historical claim; r2 has none -> r2 must win post-restart.
        run_id, step = restarted.claim_next("acme", "w9", 30)
        self.assertEqual((run_id, step["id"]), ("r2", "a"))

    def test_replay_rebuilds_state_fair_claim_wrote(self):
        self.submit()
        self.start("r1")
        run_id, step = self.claim_next(worker="w1", lease=45)
        self.assertEqual(run_id, "r1")
        stored = self.scheduler.get_run("acme", "r1")
        rebuilt = self.scheduler.replay("acme", "r1")
        self.assertEqual(rebuilt["status"], stored["status"])
        self.assertEqual({k: v["status"] for k, v in rebuilt["steps"].items()},
                         {k: v["status"] for k, v in stored["steps"].items()})
        self.assertEqual(rebuilt["steps"]["a"]["worker_id"], "w1")
        self.assertEqual(rebuilt["steps"]["a"]["lease_deadline"],
                         stored["steps"]["a"]["lease_deadline"])
        # The claim + run_started events that drove the rebuild are the ones
        # appended by the fair claim.
        types = [e["type"] for e in stored["history"]]
        self.assertEqual(types[-2:], ["claim", "run_started"])

    def test_concurrent_fair_claims_pick_distinct_runs(self):
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("r1", workflow_id="solo")
        self.start("r2", workflow_id="solo")
        results = []
        barrier = threading.Barrier(2)

        def grab(worker):
            barrier.wait()
            results.append(self.claim_next(worker=worker))

        threads = [threading.Thread(target=grab, args=(w,)) for w in ("w1", "w2")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(run_id for run_id, _ in results), ["r1", "r2"])

    def test_concurrent_fair_claims_never_double_lease_one_step(self):
        self.submit(workflow_id="wide",
                    steps=[{"id": "t%02d" % i, "depends_on": []} for i in range(6)])
        self.start("wide", workflow_id="wide", max_parallelism=6)
        results = []
        barrier = threading.Barrier(6)

        def grab(worker):
            barrier.wait()
            results.append(self.claim_next(worker=worker))

        threads = [threading.Thread(target=grab, args=("w%d" % i,)) for i in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        leased = [(run_id, step["id"]) for run_id, step in results]
        self.assertEqual(len(leased), len(set(leased)))
        held = [s for s in self.scheduler.get_run("acme", "wide")["steps"].values()
                if s["status"] == STEP_RUNNING]
        self.assertEqual(len(held), 6)

    def test_concurrent_claims_round_robin_three_runs(self):
        self.submit(steps=SOLO, workflow_id="solo")
        for rid in ("r1", "r2", "r3"):
            self.start(rid, workflow_id="solo")
        results = []
        barrier = threading.Barrier(3)

        def grab(worker):
            barrier.wait()
            results.append(self.claim_next(worker=worker)[0])

        threads = [threading.Thread(target=grab, args=("w%d" % i,)) for i in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(results), ["r1", "r2", "r3"])


class FairValidationTest(FairClaimTestBase):
    def test_bad_tenant(self):
        for bad in (None, "", "   ", 7, True):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.claim_next(bad, "w1", 30)
            self.assertEqual(ctx.exception.code, "bad_tenant", repr(bad))

    def test_tenant_is_trimmed(self):
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("r1", workflow_id="solo")
        run_id, step = self.scheduler.claim_next("  acme  ", "w1", 30)
        self.assertEqual(run_id, "r1")

    def test_bad_worker(self):
        for bad in (None, "", "   ", 9, True):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.claim_next("acme", bad, 30)
            self.assertEqual(ctx.exception.code, "bad_worker", repr(bad))

    def test_bad_lease(self):
        for bad in (0, -1, True, "30", float("nan"), float("inf"),
                    float("-inf"), [30], {}):
            with self.assertRaises(WorkflowError) as ctx:
                self.scheduler.claim_next("acme", "w1", bad)
            self.assertEqual(ctx.exception.code, "bad_lease", repr(bad))

    def test_explicit_none_lease_also_defaults_to_thirty(self):
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("r1", workflow_id="solo")
        run_id, _ = self.scheduler.claim_next("acme", "w1", None)
        self.assertEqual(run_id, "r1")
        deadline = self.scheduler.get_run("acme", "r1")["steps"]["t"]["lease_deadline"]
        self.assertEqual(deadline, self.clock() + 30.0)

    def test_lease_defaults_to_thirty(self):
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("r1", workflow_id="solo")
        run_id, step = self.scheduler.claim_next("acme", "w1")
        self.assertEqual(run_id, "r1")
        deadline = self.scheduler.get_run("acme", "r1")["steps"]["t"]["lease_deadline"]
        self.assertEqual(deadline, self.clock() + 30.0)

    def test_worker_registry_gating(self):
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("r1", workflow_id="solo")
        self.scheduler.register_worker("acme", "w1", lease_seconds=30)
        with self.assertRaises(ConflictError) as ctx:
            self.claim_next(worker="stranger")
        self.assertEqual(ctx.exception.code, "worker_not_registered")
        self.clock.advance(30)
        with self.assertRaises(ConflictError) as ctx:
            self.claim_next(worker="w1")
        self.assertEqual(ctx.exception.code, "worker_expired")
        # The rejected gated calls leased nothing.
        self.assertEqual(self.scheduler.get_run("acme", "r1")["steps"]["t"]["status"],
                         STEP_READY)

    def test_tenant_isolation(self):
        self.submit(steps=SOLO, workflow_id="solo", tenant="acme")
        self.scheduler.submit("globex", "solo", SOLO)
        self.start("acme-run", workflow_id="solo", tenant="acme")
        self.scheduler.start_run("globex", "solo", run_id="globex-run")
        run_id, _ = self.scheduler.claim_next("globex", "w1", 30)
        self.assertEqual(run_id, "globex-run")
        run_id, _ = self.claim_next(tenant="acme")
        self.assertEqual(run_id, "acme-run")
        # Empty other tenant has no candidates.
        self.assertEqual(self.scheduler.claim_next("nomansland", "w1", 30), (None, None))


if __name__ == "__main__":
    unittest.main()
