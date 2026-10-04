"""Tests for tenant scoped, idempotent run creation (``idempotency_key``)."""

import json
import os
import shutil
import tempfile
import threading
import unittest

from flowd.model import WorkflowError
from flowd.scheduler import ConflictError, Scheduler
from flowd.store import WorkflowStore, atomic_write_json

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
]


class IdempotencyTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-idem-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def submit(self, tenant="acme", workflow_id="etl", steps=None):
        return self.scheduler.submit(tenant, workflow_id, steps or LINEAR)

    def start(self, *args, **kwargs):
        return self.scheduler.start_run(*args, **kwargs)


class FakeClock:
    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value


class IdempotencyReuseTest(IdempotencyTestBase):
    def test_repeat_returns_same_run_without_generating_a_new_id(self):
        self.submit()
        first = self.start("acme", "etl", idempotency_key="k1")
        second = self.start("acme", "etl", idempotency_key=" k1 ")
        self.assertEqual(second["run_id"], first["run_id"])
        self.assertEqual(second["idempotency_key"], "k1")
        self.assertEqual(second["created_at"], first["created_at"])
        self.assertEqual([r["run_id"] for r in self.scheduler.list_runs("acme")[0]],
                         [first["run_id"]])

    def test_repeat_reports_latest_state_including_terminal_runs(self):
        self.submit()
        first = self.start("acme", "etl", idempotency_key="k1")
        self.scheduler.claim("acme", first["run_id"], "w1", 30)
        self.scheduler.complete("acme", first["run_id"], "step1", "w1")
        self.scheduler.claim("acme", first["run_id"], "w1", 30)
        self.scheduler.complete("acme", first["run_id"], "step2", "w1")
        self.clock.advance(100)
        repeat = self.start("acme", "etl", idempotency_key="k1")
        self.assertEqual(repeat["run_id"], first["run_id"])
        self.assertEqual(repeat["status"], "succeeded")

    def test_hit_appends_no_history_and_keeps_updated_at(self):
        self.submit()
        first = self.start("acme", "etl", idempotency_key="k1", params={"x": 1})
        self.scheduler.claim("acme", first["run_id"], "w1", 30)
        stored = self.scheduler.get_run("acme", first["run_id"])
        history_len, updated_at = len(stored["history"]), stored["updated_at"]
        self.clock.advance(120)
        repeat = self.start("acme", "etl", idempotency_key="k1",
                            params={"x": 1}, max_parallelism=None)
        self.assertEqual(len(repeat["history"]), history_len)
        self.assertEqual(repeat["updated_at"], updated_at)
        self.assertEqual(repeat["steps"]["step1"]["worker_id"], "w1")

    def test_run_id_omitted_is_reused_explicit_match_accepted(self):
        self.submit()
        first = self.start("acme", "etl", idempotency_key="k1")
        again = self.start("acme", "etl", idempotency_key="k1", run_id=first["run_id"])
        self.assertEqual(again["run_id"], first["run_id"])

    def test_explicit_different_run_id_conflicts(self):
        self.submit()
        self.start("acme", "etl", idempotency_key="k1")
        with self.assertRaises(ConflictError) as ctx:
            self.start("acme", "etl", idempotency_key="k1", run_id="something-else")
        self.assertEqual(ctx.exception.code, "idempotency_conflict")
        with self.assertRaises(WorkflowError):
            self.scheduler.get_run("acme", "something-else")

    def test_same_key_in_different_tenants_is_independent(self):
        self.submit("acme")
        self.submit("globex")
        acme = self.start("acme", "etl", idempotency_key="dup")
        globex = self.start("globex", "etl", idempotency_key="dup")
        self.assertNotEqual(acme["run_id"], globex["run_id"])
        self.assertEqual(self.start("globex", "etl", idempotency_key="dup")["run_id"],
                         globex["run_id"])
        self.assertEqual([r["run_id"] for r in self.scheduler.list_runs("acme")[0]],
                         [acme["run_id"]])
        self.assertEqual([r["run_id"] for r in self.scheduler.list_runs("globex")[0]],
                         [globex["run_id"]])


class IdempotencyParamsTest(IdempotencyTestBase):
    def test_params_must_be_an_object_when_keyed(self):
        self.submit()
        for bad in ([1, 2], "string", 42, 3.5, True):
            with self.assertRaises(WorkflowError) as ctx:
                self.start("acme", "etl", idempotency_key="k", params=bad)
            self.assertEqual(ctx.exception.code, "bad_params", bad)
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])

    def test_omitted_or_null_params_become_empty_object_and_match(self):
        self.submit()
        first = self.start("acme", "etl", idempotency_key="k1")
        self.assertEqual(first["params"], {})
        for params in (None, {}):
            self.assertEqual(self.start("acme", "etl", idempotency_key="k1",
                                       params=params)["run_id"], first["run_id"])

    def test_key_order_ignored_array_order_kept(self):
        self.submit()
        first = self.start("acme", "etl", idempotency_key="k1",
                           params={"a": 1, "nested": {"x": [1, 2], "y": True}})
        repeat = self.start("acme", "etl", idempotency_key="k1",
                           params={"nested": {"y": True, "x": [1, 2]}, "a": 1})
        self.assertEqual(repeat["run_id"], first["run_id"])
        with self.assertRaises(ConflictError):
            self.start("acme", "etl", idempotency_key="k1",
                       params={"a": 1, "nested": {"x": [2, 1], "y": True}})

    def test_boolean_distinct_from_number_and_numeric_equality(self):
        self.submit()
        self.start("acme", "etl", idempotency_key="kb", params={"v": True})
        with self.assertRaises(ConflictError):
            self.start("acme", "etl", idempotency_key="kb", params={"v": 1})
        self.start("acme", "etl", idempotency_key="kn", params={"v": 1})
        numeric = [r for r in self.scheduler.list_runs("acme")[0]
                   if r["idempotency_key"] == "kn"][0]
        self.assertEqual(self.start("acme", "etl", idempotency_key="kn",
                                    params={"v": 1.0})["run_id"], numeric["run_id"])

    def test_different_params_workflow_or_quota_conflict(self):
        self.submit()
        self.scheduler.submit("acme", "other", [{"id": "s", "depends_on": []}])
        self.start("acme", "etl", idempotency_key="k1", params={"a": 1}, max_parallelism=2)
        with self.assertRaises(ConflictError):
            self.start("acme", "etl", idempotency_key="k1", params={"a": 2}, max_parallelism=2)
        with self.assertRaises(ConflictError):
            self.start("acme", "other", idempotency_key="k1", params={"a": 1},
                       max_parallelism=2)
        with self.assertRaises(ConflictError):
            self.start("acme", "etl", idempotency_key="k1", params={"a": 1},
                       max_parallelism=3)
        # omitted/null quota means unlimited, which differs from a quota of 2
        with self.assertRaises(ConflictError):
            self.start("acme", "etl", idempotency_key="k1", params={"a": 1})
        with self.assertRaises(ConflictError):
            self.start("acme", "etl", idempotency_key="k1", params={"a": 1},
                       max_parallelism=None)
        self.assertEqual(len(self.scheduler.list_runs("acme")[0]), 1)


class IdempotencyValidationTest(IdempotencyTestBase):
    def test_bad_keys_rejected_before_any_create(self):
        self.submit()
        for bad in ("", "   ", 5, True, 1.0, [], {}):
            with self.assertRaises(WorkflowError) as ctx:
                self.start("acme", "etl", idempotency_key=bad)
            self.assertEqual(ctx.exception.code, "bad_idempotency_key", bad)
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])
        self.assertFalse(os.path.exists(self.store.idempotency_path("acme")))

    def test_bad_params_and_quota_are_400_even_for_unknown_workflow(self):
        with self.assertRaises(WorkflowError) as ctx:
            self.start("acme", "ghost", idempotency_key="k1", params=[1])
        self.assertEqual(ctx.exception.code, "bad_params")
        with self.assertRaises(WorkflowError) as ctx:
            self.start("acme", "ghost", idempotency_key="k1", max_parallelism=0)
        self.assertEqual(ctx.exception.code, "bad_max_parallelism")
        with self.assertRaises(WorkflowError) as ctx:
            self.start("acme", "ghost", idempotency_key="  ")
        self.assertEqual(ctx.exception.code, "bad_idempotency_key")

    def test_new_key_unknown_workflow_is_404_and_creates_nothing(self):
        with self.assertRaises(WorkflowError) as ctx:
            self.start("acme", "ghost", idempotency_key="k1", params={"a": 1})
        self.assertEqual(ctx.exception.code, "unknown_workflow")
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])
        self.assertFalse(os.path.exists(self.store.idempotency_path("acme")))

    def test_legal_content_differs_is_409_not_404_when_workflow_vanishes(self):
        self.submit()
        self.start("acme", "etl", idempotency_key="k1", params={"a": 1})
        # hit with same content still works, and is decided by first content
        self.assertEqual(self.start("acme", "etl", idempotency_key="k1",
                                    params={"a": 1})["status"], "pending")
        os.remove(self.store.workflows_path("acme"))
        self.assertEqual(self.start("acme", "etl", idempotency_key="k1",
                                    params={"a": 1})["status"], "pending")
        with self.assertRaises(ConflictError):
            self.start("acme", "etl", idempotency_key="k1", params={"a": 2})

    def test_overwriting_same_named_workflow_does_not_change_reuse(self):
        self.submit(steps=[{"id": "old", "depends_on": []}])
        first = self.start("acme", "etl", idempotency_key="k1")
        self.scheduler.submit("acme", "etl",
                              [{"id": "new", "depends_on": []},
                               {"id": "new2", "depends_on": ["new"]}])
        repeat = self.start("acme", "etl", idempotency_key="k1")
        self.assertEqual(repeat["run_id"], first["run_id"])
        self.assertEqual(set(repeat["steps"]), {"old"})


class IdempotencyConcurrencyTest(IdempotencyTestBase):
    def test_concurrent_same_content_creates_one_run(self):
        self.submit()
        results, errors = [], []
        barrier = threading.Barrier(8)

        def fire():
            barrier.wait()
            try:
                results.append(self.start("acme", "etl", idempotency_key="k1",
                                          params={"x": 1}, max_parallelism=2))
            except WorkflowError as exc:
                errors.append(exc)

        threads = [threading.Thread(target=fire) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        run_ids = {r["run_id"] for r in results}
        self.assertEqual(len(run_ids), 1)
        self.assertEqual(len(self.scheduler.list_runs("acme")[0]), 1)

    def test_concurrent_different_content_only_first_wins(self):
        self.submit()
        outcomes = []
        barrier = threading.Barrier(6)

        def fire(value):
            barrier.wait()
            try:
                run = self.start("acme", "etl", idempotency_key="k1", params={"v": value})
                outcomes.append(("ok", value, run["run_id"]))
            except ConflictError:
                outcomes.append(("conflict", value, None))

        threads = [threading.Thread(target=fire, args=(v,)) for v in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        accepted = [o for o in outcomes if o[0] == "ok"]
        self.assertEqual(len(accepted), 1)
        winning_value = accepted[0][1]
        self.assertEqual(len(self.scheduler.list_runs("acme")[0]), 1)
        # a follow-up repeat only reuses the first-created content
        repeat = self.start("acme", "etl", idempotency_key="k1", params={"v": winning_value})
        self.assertEqual(repeat["run_id"], accepted[0][2])
        with self.assertRaises(ConflictError):
            self.start("acme", "etl", idempotency_key="k1",
                       params={"v": (winning_value + 1) % 6})


class IdempotencyPersistenceTest(IdempotencyTestBase):
    def test_key_and_first_content_survive_restart(self):
        self.submit()
        first = self.start("acme", "etl", idempotency_key="k1", params={"day": "monday"},
                           max_parallelism=3)
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        again = restarted.start_run("acme", "etl", idempotency_key="k1",
                                    params={"day": "monday"}, max_parallelism=3)
        self.assertEqual(again["run_id"], first["run_id"])
        with self.assertRaises(ConflictError):
            restarted.start_run("acme", "etl", idempotency_key="k1",
                                params={"day": "tuesday"}, max_parallelism=3)

    def test_index_file_lives_next_to_workflows(self):
        self.submit()
        self.start("acme", "etl", idempotency_key="k1")
        path = self.store.idempotency_path("acme")
        self.assertTrue(os.path.isfile(path))
        with open(path, "r", encoding="utf-8") as handle:
            records = json.load(handle)
        self.assertEqual(set(records["k1"]), {"run_id", "workflow_id", "params",
                                              "max_parallelism"})

    def test_key_reported_everywhere_and_null_for_old_runs(self):
        self.submit()
        run = self.start("acme", "etl", idempotency_key="k1", params={"a": 1})
        self.assertEqual(self.scheduler.get_run("acme", run["run_id"])["idempotency_key"], "k1")
        listed = [r for r in self.scheduler.list_runs("acme")[0]
                  if r["run_id"] == run["run_id"]][0]
        self.assertEqual(listed["idempotency_key"], "k1")
        self.assertEqual(self.scheduler.replay("acme", run["run_id"])["idempotency_key"], "k1")

        unkeyed = self.start("acme", "etl", run_id="plain")
        self.assertIsNone(unkeyed["idempotency_key"])
        # simulate a run document written by an older version: no field at all
        path = os.path.join(self.root, "acme", "runs", "plain.json")
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        del doc["idempotency_key"]
        atomic_write_json(path, doc)
        reloaded = self.scheduler.get_run("acme", "plain")
        self.assertIsNone(reloaded["idempotency_key"])
        self.assertIsNone(self.scheduler.replay("acme", "plain")["idempotency_key"])
        plain_listed = [r for r in self.scheduler.list_runs("acme")[0]
                        if r["run_id"] == "plain"][0]
        self.assertIsNone(plain_listed["idempotency_key"])

    def test_unkeyed_create_is_completely_unchanged(self):
        self.submit()
        run = self.start("acme", "etl", params={"x": 1}, max_parallelism=2)
        self.assertIsNone(run["idempotency_key"])
        # no key -> no index, and a second unkeyed create makes a new run
        self.assertFalse(os.path.exists(self.store.idempotency_path("acme")))
        second = self.start("acme", "etl", params={"x": 1}, max_parallelism=2)
        self.assertNotEqual(second["run_id"], run["run_id"])


if __name__ == "__main__":
    unittest.main()
