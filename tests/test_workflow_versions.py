"""Tests for per-run DAG versioning: same-name workflow overwrites.

The normalized DAG definition used when a run is created is frozen into the
run document.  Submitting a different workflow under the same
``workflow_id`` later only affects runs created afterwards; existing runs
keep their step order, node kinds, dependencies, ``max_attempts`` and
state-machine semantics.  ``Scheduler.replay`` rebuilds exclusively from
the run's frozen definition (and append-only history), so it neither drops
old nodes nor introduces new ones, agrees with the persisted document on
every observable field, and produces the same result after a restart.
"""

import json
import os
import shutil
import tempfile
import unittest

from flowd.model import WorkflowError
from flowd.scheduler import Scheduler
from flowd.store import WorkflowStore, atomic_write_json, new_run, run_plan

# Old definition: a root task with one attempt, an approval gate, then a
# flaky task with two attempts.
OLD_STEPS = [
    {"id": "a", "depends_on": [], "max_attempts": 1},
    {"id": "gate", "depends_on": ["a"], "kind": "approval"},
    {"id": "b", "depends_on": ["gate"], "max_attempts": 2},
]
# Same workflow_id, incompatible definition: the gate is gone, "b" unlocks
# straight from "a", "x" is new, and "a"'s max_attempts changed.
NEW_STEPS = [
    {"id": "a", "depends_on": [], "max_attempts": 5},
    {"id": "b", "depends_on": ["a"], "max_attempts": 5},
    {"id": "x", "depends_on": ["b"], "max_attempts": 5},
]

RUN_FIELDS = ("run_id", "workflow_id", "status", "step_order", "params",
              "max_parallelism", "idempotency_key", "not_before", "schedule_id",
              "scheduled_at", "created_at", "updated_at")
STEP_FIELDS = ("id", "kind", "status", "attempt", "max_attempts", "depends_on",
               "worker_id", "lease_deadline", "next_attempt_at", "ready_at",
               "started_at", "finished_at", "result", "error", "approval")


class FakeClock:
    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value


class WorkflowVersionTestBase(unittest.TestCase):
    workflow_id = "etl"

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-version-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def submit_old(self, steps=None, workflow_id=None):
        return self.scheduler.submit("acme", workflow_id or self.workflow_id,
                                     steps or OLD_STEPS)

    def overwrite(self, steps=None, workflow_id=None):
        return self.scheduler.submit("acme", workflow_id or self.workflow_id,
                                     steps or NEW_STEPS)

    def start_old(self, run_id="r1", **kwargs):
        return self.scheduler.start_run("acme", self.workflow_id, run_id=run_id, **kwargs)

    def restart(self):
        return Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)

    def assertRunDocEqual(self, rebuilt, stored):
        """Replay must agree with the stored run on every observable field."""
        for name in RUN_FIELDS:
            self.assertEqual(rebuilt.get(name), stored.get(name), name)
        self.assertEqual(rebuilt["step_order"], stored["step_order"])
        # Node iteration everywhere follows step_order; the stored mapping
        # comes back key-sorted from disk, so compare node identity by set
        # and each node field-wise in canonical step_order.
        self.assertEqual(set(rebuilt["steps"]), set(stored["steps"]))
        for sid in stored["step_order"]:
            lhs, rhs = rebuilt["steps"][sid], stored["steps"][sid]
            for name in STEP_FIELDS:
                self.assertEqual(lhs.get(name), rhs.get(name), "%s.%s" % (sid, name))
        self.assertEqual(rebuilt["history"], stored["history"])

    def assertUsesOldDefinition(self, run):
        self.assertEqual(run["step_order"], ["a", "gate", "b"])
        for sid, kind, attempts, deps in (
            ("a", "task", 1, []),
            ("gate", "approval", 3, ["a"]),
            ("b", "task", 2, ["gate"]),
        ):
            step = run["steps"][sid]
            self.assertEqual((step["kind"], step["max_attempts"], step["depends_on"]),
                             (kind, attempts, deps), sid)
        self.assertNotIn("x", run["steps"])

    def assertUsesNewDefinition(self, run):
        self.assertEqual(run["step_order"], ["a", "b", "x"])
        self.assertNotIn("gate", run["steps"])
        self.assertEqual(run["steps"]["a"]["max_attempts"], 5)
        self.assertEqual(run["steps"]["b"]["depends_on"], ["a"])


class PlanSnapshotTest(WorkflowVersionTestBase):
    def test_run_document_freezes_the_normalized_plan_at_creation(self):
        plan = self.submit_old()
        run = self.start_old()
        self.assertEqual(run["plan"]["steps"], plan["steps"])
        self.assertEqual(run["plan"]["order"], ["a", "gate", "b"])
        self.assertEqual(run["plan"]["workflow_id"], self.workflow_id)
        # The snapshot is an independent copy of the submitted plan.
        run["plan"]["steps"][0]["max_attempts"] = 99
        reloaded = self.scheduler.get_run("acme", "r1")
        self.assertEqual(reloaded["steps"]["a"]["max_attempts"], 1)
        self.assertEqual(reloaded["plan"]["steps"][0]["max_attempts"], 1)

    def test_run_plan_helper_reads_the_snapshot_without_touching_registry(self):
        self.submit_old()
        run = self.start_old()
        snapshot = run_plan(run)
        self.assertEqual([s["id"] for s in snapshot["steps"]], ["a", "gate", "b"])
        # mutating the returned plan never mutates the run document
        snapshot["steps"].append({"id": "intruder"})
        self.assertEqual(run["plan"]["steps"][-1]["id"], "b")


class ReplayAfterOverwriteTest(WorkflowVersionTestBase):
    def drive_old_run_to_waiting_gate(self):
        self.submit_old()
        run = self.start_old()
        self.assertEqual(self.scheduler.claim("acme", "r1", "w1", 30)["id"], "a")
        run = self.scheduler.complete("acme", "r1", "a", "w1", result={"v": 1})
        self.assertEqual(run["steps"]["gate"]["status"], "waiting")
        self.assertEqual(run["steps"]["b"]["status"], "pending")
        return run

    def test_overwriting_workflow_leaves_stored_run_untouched(self):
        self.drive_old_run_to_waiting_gate()
        self.overwrite()
        stored = self.scheduler.get_run("acme", "r1")
        self.assertUsesOldDefinition(stored)
        self.assertEqual(stored["steps"]["gate"]["status"], "waiting")
        self.assertEqual(stored["steps"]["b"]["status"], "pending")

    def test_replay_rebuilds_old_nodes_never_the_new_dag(self):
        self.drive_old_run_to_waiting_gate()
        self.overwrite()
        replayed = self.scheduler.replay("acme", "r1")
        self.assertUsesOldDefinition(replayed)
        self.assertEqual(replayed["steps"]["a"]["status"], "succeeded")
        self.assertEqual(replayed["steps"]["a"]["result"], {"v": 1})
        self.assertEqual(replayed["steps"]["gate"]["status"], "waiting")
        # "b" stays pending behind the old approval gate even though the new
        # DAG unlocks "b" straight from "a"; "x" must never appear.
        self.assertEqual(replayed["steps"]["b"]["status"], "pending")
        self.assertNotIn("x", replayed["steps"])

    def test_replay_matches_stored_document_field_by_field(self):
        self.drive_old_run_to_waiting_gate()
        self.overwrite()
        stored = self.scheduler.get_run("acme", "r1")
        self.assertRunDocEqual(self.scheduler.replay("acme", "r1"), stored)

    def test_old_run_keeps_scheduling_against_old_dag_after_overwrite(self):
        self.drive_old_run_to_waiting_gate()
        self.overwrite()
        # Approving the old gate unlocks "b"; failing it once retries under
        # the old max_attempts of 2 (backoff recorded by the retry event).
        run = self.scheduler.decide("acme", "r1", "gate", "boss", "approve")
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        self.assertEqual(self.scheduler.claim("acme", "r1", "w1", 30)["id"], "b")
        run = self.scheduler.fail("acme", "r1", "b", "w1", error="boom")
        self.assertEqual(run["steps"]["b"]["status"], "ready")
        self.assertIsNotNone(run["steps"]["b"]["next_attempt_at"])
        # Before backoff elapses nothing is claimable; afterwards the retry
        # succeeds and the run defined by the OLD dag completes.
        self.assertIsNone(self.scheduler.claim("acme", "r1", "w2", 30))
        self.clock.advance(2)
        self.assertEqual(self.scheduler.claim("acme", "r1", "w2", 30)["id"], "b")
        run = self.scheduler.complete("acme", "r1", "b", "w2", result="ok")
        self.assertEqual(run["status"], "succeeded")
        stored = self.scheduler.get_run("acme", "r1")
        self.assertUsesOldDefinition(stored)
        self.assertRunDocEqual(self.scheduler.replay("acme", "r1"), stored)

    def test_attempts_exhausted_uses_old_max_attempts(self):
        # "a" allows a single attempt in the old DAG but five in the new one.
        self.submit_old()
        self.start_old()
        self.scheduler.claim("acme", "r1", "w1", 30)
        self.overwrite()
        run = self.scheduler.fail("acme", "r1", "a", "w1", error="nope")
        self.assertEqual((run["status"], run["steps"]["a"]["status"]),
                         ("failed", "failed"))
        replayed = self.scheduler.replay("acme", "r1")
        self.assertEqual(replayed["status"], "failed")
        self.assertEqual(replayed["steps"]["a"]["max_attempts"], 1)
        self.assertEqual(replayed["steps"]["a"]["status"], "failed")
        self.assertEqual(replayed["steps"]["gate"]["status"], "pending")

    def test_rejected_old_gate_fails_old_run_regardless_of_new_dag(self):
        self.drive_old_run_to_waiting_gate()
        self.overwrite()
        run = self.scheduler.decide("acme", "r1", "gate", "boss", "reject")
        self.assertEqual(run["status"], "failed")
        self.assertEqual(run["steps"]["b"]["status"], "pending")
        stored = self.scheduler.get_run("acme", "r1")
        self.assertRunDocEqual(self.scheduler.replay("acme", "r1"), stored)

    def test_replay_works_when_workflow_is_deleted_after_run_creation(self):
        self.drive_old_run_to_waiting_gate()
        os.remove(os.path.join(self.root, "acme", "workflows.json"))
        with self.assertRaises(WorkflowError) as ctx:
            self.store.get_workflow("acme", self.workflow_id)
        self.assertEqual(ctx.exception.code, "unknown_workflow")
        replayed = self.scheduler.replay("acme", "r1")
        self.assertUsesOldDefinition(replayed)
        self.assertEqual(replayed["steps"]["gate"]["status"], "waiting")

    def test_replay_is_idempotent_and_deterministic_across_restarts(self):
        self.drive_old_run_to_waiting_gate()
        self.overwrite()
        first = self.scheduler.replay("acme", "r1")
        second = self.scheduler.replay("acme", "r1")
        self.assertEqual(second, first)
        restarted = self.restart()
        third = restarted.replay("acme", "r1")
        self.assertEqual(third, first)
        stored = self.scheduler.get_run("acme", "r1")
        self.assertRunDocEqual(third, stored)

    def test_new_run_created_after_overwrite_uses_new_definition(self):
        self.drive_old_run_to_waiting_gate()
        self.overwrite()
        new_run_doc = self.scheduler.start_run("acme", self.workflow_id, run_id="r2")
        self.assertUsesNewDefinition(new_run_doc)
        # The old run is untouched and still replays its own definition.
        old = self.scheduler.replay("acme", "r1")
        self.assertUsesOldDefinition(old)
        new_stored = self.scheduler.get_run("acme", "r2")
        self.assertRunDocEqual(self.scheduler.replay("acme", "r2"), new_stored)

    def test_history_events_remain_append_only_and_unchanged(self):
        self.drive_old_run_to_waiting_gate()
        before = self.scheduler.history("r1", tenant="acme")
        self.overwrite()
        self.assertEqual(self.scheduler.history("r1", tenant="acme"), before)
        replayed = self.scheduler.replay("acme", "r1")
        self.assertEqual(replayed["history"], before)
        types = [e["type"] for e in before]
        self.assertEqual(types, ["run_created", "ready", "claim", "run_started",
                                 "complete", "waiting"])


class DependencyUnlockTest(WorkflowVersionTestBase):
    def test_old_dependencies_keep_governing_claims_after_overwrite(self):
        # Old DAG: c depends on r.  New DAG: c is itself a root.
        old = [{"id": "r", "depends_on": []},
               {"id": "c", "depends_on": ["r"]}]
        new = [{"id": "c", "depends_on": []}, {"id": "e", "depends_on": ["c"]}]
        self.submit_old(old)
        self.start_old()
        self.overwrite(new)
        self.assertEqual(self.scheduler.claim("acme", "r1", "w1", 30)["id"], "r")
        self.assertIsNone(self.scheduler.claim("acme", "r1", "w1", 30))
        self.scheduler.complete("acme", "r1", "r", "w1")
        # Only the OLD child "c" unlocks; the new node "e" never exists.
        self.assertEqual(self.scheduler.claim("acme", "r1", "w1", 30)["id"], "c")
        run = self.scheduler.complete("acme", "r1", "c", "w1")
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(set(run["steps"]), {"r", "c"})

    def test_old_independent_roots_stay_independent_after_dependency_added(self):
        # Old DAG: a and b are independent roots.  New DAG: b depends on a.
        old = [{"id": "a", "depends_on": []}, {"id": "b", "depends_on": []}]
        new = [{"id": "a", "depends_on": []}, {"id": "b", "depends_on": ["a"]}]
        self.submit_old(old)
        self.start_old()
        self.overwrite(new)
        self.assertEqual(self.scheduler.claim("acme", "r1", "w1", 30)["id"], "a")
        # The new edge must not retroactively block the old run's node "b".
        self.assertEqual(self.scheduler.claim("acme", "r1", "w2", 30)["id"], "b")


class LeaseAndRetryReplayTest(WorkflowVersionTestBase):
    def test_replay_preserves_heartbeat_takeover_lease_and_backoff(self):
        self.submit_old([{"id": "s", "depends_on": [], "max_attempts": 3}])
        self.start_old()
        self.assertEqual(self.scheduler.claim("acme", "r1", "w1", 5)["id"], "s")
        self.clock.advance(2)
        self.scheduler.heartbeat("acme", "r1", "s", "w1", 100)
        self.clock.advance(200)  # extended lease expires; w2 takes over
        taken = self.scheduler.claim("acme", "r1", "w2", 15)
        self.assertEqual(taken["worker_id"], "w2")
        run = self.scheduler.fail("acme", "r1", "s", "w2", error="boom")
        self.assertEqual(run["steps"]["s"]["status"], "ready")
        self.overwrite([{"id": "other", "depends_on": [], "max_attempts": 9}])
        stored = self.scheduler.get_run("acme", "r1")
        replayed = self.scheduler.replay("acme", "r1")
        self.assertRunDocEqual(replayed, stored)
        self.assertEqual(set(replayed["steps"]), {"s"})
        self.assertEqual(replayed["steps"]["s"]["attempt"], 1)
        self.assertEqual(replayed["steps"]["s"]["max_attempts"], 3)
        self.assertEqual(replayed["steps"]["s"]["worker_id"], None)
        # Restart and wait out the backoff: the old run keeps executing.
        fresh = self.restart()
        self.assertIsNone(fresh.claim("acme", "r1", "w3", 30))
        self.clock.advance(2)
        self.assertEqual(fresh.claim("acme", "r1", "w3", 30)["id"], "s")
        fresh.complete("acme", "r1", "s", "w3")
        self.assertEqual(fresh.replay("acme", "r1")["status"], "succeeded")


class LegacyDocumentTest(WorkflowVersionTestBase):
    def _write_legacy_run(self, run):
        path = os.path.join(self.root, "acme", "runs", "%s.json" % run["run_id"])
        doc = json.loads(json.dumps(run))
        del doc["plan"]  # documents written before plan snapshots existed
        atomic_write_json(path, doc)

    def test_old_document_without_plan_snapshot_rebuilds_from_its_own_nodes(self):
        self.submit_old()
        self.start_old()
        self.assertEqual(self.scheduler.claim("acme", "r1", "w1", 30)["id"], "a")
        self.scheduler.complete("acme", "r1", "a", "w1")
        self._write_legacy_run(self.scheduler.get_run("acme", "r1"))
        reloaded = self.scheduler.get_run("acme", "r1")
        with open(os.path.join(self.root, "acme", "runs", "r1.json")) as handle:
            self.assertNotIn("plan", json.load(handle))
        self.assertUsesOldDefinition(reloaded)
        # Overwrite the registry, then replay the legacy document: its plan
        # is reconstructed from the nodes/order stored inside the run.
        self.overwrite()
        replayed = self.scheduler.replay("acme", "r1")
        self.assertUsesOldDefinition(replayed)
        self.assertEqual(replayed["steps"]["a"]["status"], "succeeded")
        self.assertEqual(replayed["steps"]["gate"]["status"], "waiting")
        self.assertEqual(replayed["steps"]["b"]["status"], "pending")

    def test_legacy_document_replays_even_when_workflow_is_gone(self):
        self.submit_old()
        run = self.start_old()
        self._write_legacy_run(run)
        os.remove(os.path.join(self.root, "acme", "workflows.json"))
        replayed = self.scheduler.replay("acme", "r1")
        self.assertUsesOldDefinition(replayed)
        self.restart().replay("acme", "r1")


class TenantIsolationTest(WorkflowVersionTestBase):
    def test_other_tenant_same_named_workflow_does_not_change_replay(self):
        self.submit_old()
        self.start_old()
        self.scheduler.submit("globex", self.workflow_id, NEW_STEPS)
        replayed = self.scheduler.replay("acme", "r1")
        self.assertUsesOldDefinition(replayed)
        # globex's own new run uses globex's current definition
        globex_run = self.scheduler.start_run("globex", self.workflow_id, run_id="g1")
        self.assertEqual(globex_run["step_order"], ["a", "b", "x"])

    def test_replay_unknown_run_and_cross_tenant_codes_are_unchanged(self):
        self.submit_old()
        self.start_old()
        self.overwrite()
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.replay("acme", "ghost-run")
        self.assertEqual(ctx.exception.code, "unknown_run")
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.replay("globex", "r1")
        self.assertEqual(ctx.exception.code, "cross_tenant")


class SnapshotConstructionTest(unittest.TestCase):
    def test_new_run_builds_snapshot_and_fresh_nodes_from_one_plan(self):
        plan = {
            "workflow_id": "wf",
            "steps": [{"id": "a", "depends_on": [], "max_attempts": 2, "kind": "task"}],
            "order": ["a"],
        }
        run = new_run("t", "wf", "r1", {}, plan, "2024-01-01T00:00:00.000000Z")
        self.assertEqual(run["plan"]["steps"], plan["steps"])
        self.assertIsNot(run["plan"]["steps"][0], plan["steps"][0])
        self.assertEqual(run["step_order"], ["a"])


if __name__ == "__main__":
    unittest.main()
