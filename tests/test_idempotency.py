"""Tests for tenant-scoped idempotent run creation (``idempotency_key``)."""

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
from flowd.scheduler import ConflictError, Scheduler
from flowd.store import WorkflowStore, atomic_write_json

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
    {"id": "step3", "depends_on": ["step2"]},
]

SOLO = [{"id": "solo", "depends_on": []}]


class IdempotencyTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-idem-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = WorkflowStore(self.root)
        self.scheduler = Scheduler(self.store)

    def submit(self, tenant="acme", workflow_id="etl", steps=None):
        return self.scheduler.submit(tenant, workflow_id, steps or LINEAR)

    def start(self, tenant="acme", workflow_id="etl", run_id=None, params=None,
              max_parallelism=None, idempotency_key="NOTSET"):
        kwargs = {"run_id": run_id, "params": params, "max_parallelism": max_parallelism}
        if idempotency_key != "NOTSET":
            kwargs["idempotency_key"] = idempotency_key
        return self.scheduler.start_run(tenant, workflow_id, **kwargs)

    def restart(self):
        return Scheduler(WorkflowStore(self.root))


class KeyValidationTest(IdempotencyTestBase):
    def test_none_and_omitted_keep_legacy_behavior(self):
        self.submit()
        first = self.start(idempotency_key=None)
        second = self.start()  # omitted
        self.assertNotEqual(first["run_id"], second["run_id"])
        self.assertIsNone(first["idempotency_key"])
        self.assertIsNone(second["idempotency_key"])
        # legacy creates never populate the dedup index
        self.assertEqual(self.store.load_idempotency("acme"), {})

    def test_key_must_be_a_non_blank_string(self):
        self.submit()
        for bad in (123, True, 3.5, [], {}, ["k"]):
            with self.assertRaises(WorkflowError) as ctx:
                self.start(idempotency_key=bad)
            self.assertEqual(ctx.exception.code, "bad_idempotency_key", repr(bad))
        for bad in ("", "   ", "\t\n"):
            with self.assertRaises(WorkflowError) as ctx:
                self.start(idempotency_key=bad)
            self.assertEqual(ctx.exception.code, "bad_idempotency_key", repr(bad))
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])

    def test_key_is_trimmed_before_storage_and_match(self):
        self.submit()
        first = self.start(idempotency_key="  order-42 ")
        self.assertEqual(first["idempotency_key"], "order-42")
        again = self.start(idempotency_key="order-42")
        self.assertEqual(again["run_id"], first["run_id"])
        # whitespace-only difference never creates a second run
        self.assertEqual(len(self.scheduler.list_runs("acme")[0]), 1)
        self.assertEqual(list(self.store.load_idempotency("acme")), ["order-42"])


class DedupLifecycleTest(IdempotencyTestBase):
    def test_repeat_returns_same_run_with_latest_state(self):
        self.submit()
        first = self.start(params={"day": "mon"}, idempotency_key="k1")
        second = self.start(params={"day": "mon"}, idempotency_key="k1")
        self.assertEqual(second["run_id"], first["run_id"])
        self.assertEqual(second["idempotency_key"], "k1")

        self.scheduler.claim("acme", first["run_id"], "w1", 30)
        self.scheduler.complete("acme", first["run_id"], "step1", "w1")
        third = self.start(params={"day": "mon"}, idempotency_key="k1")
        self.assertEqual(third["run_id"], first["run_id"])
        self.assertEqual(third["status"], "running")
        self.assertEqual(third["steps"]["step1"]["status"], "succeeded")

    def test_repeat_reuses_succeeded_and_failed_runs(self):
        self.scheduler.submit("acme", "one", SOLO)
        run = self.start(workflow_id="one", idempotency_key="ok")
        self.scheduler.claim("acme", run["run_id"], "w1", 30)
        self.scheduler.complete("acme", run["run_id"], "solo", "w1")
        hit = self.start(workflow_id="one", idempotency_key="ok")
        self.assertEqual((hit["run_id"], hit["status"]), (run["run_id"], "succeeded"))

        self.scheduler.submit("acme", "flaky",
                              [{"id": "s", "depends_on": [], "max_attempts": 1}])
        dead = self.start(workflow_id="flaky", idempotency_key="dead")
        self.scheduler.claim("acme", dead["run_id"], "w1", 30)
        self.scheduler.fail("acme", dead["run_id"], "s", "w1", "boom")
        hit = self.start(workflow_id="flaky", idempotency_key="dead")
        self.assertEqual((hit["run_id"], hit["status"]), (dead["run_id"], "failed"))

    def test_repeat_changes_nothing(self):
        self.submit()
        run = self.start(params={"a": 1}, max_parallelism=1, idempotency_key="k1")
        self.scheduler.claim("acme", run["run_id"], "w1", 30)
        before = self.scheduler.get_run("acme", run["run_id"])
        hit = self.start(params={"a": 1}, max_parallelism=1, idempotency_key="k1")
        after = self.scheduler.get_run("acme", run["run_id"])
        self.assertEqual(hit, before)
        self.assertEqual(after, before)  # no history, no updated_at, lease intact

    def test_repeat_after_approval_decision_changes_nothing(self):
        self.scheduler.submit("acme", "gate", [{"id": "g", "kind": "approval"}])
        run = self.start(workflow_id="gate", idempotency_key="gk")
        self.scheduler.decide("acme", run["run_id"], "g", "boss", "approve")
        before = self.scheduler.get_run("acme", run["run_id"])
        hit = self.start(workflow_id="gate", idempotency_key="gk")
        self.assertEqual(hit, before)

    def test_explicit_run_id_is_reused_or_conflicts(self):
        self.submit()
        first = self.start(run_id="r-explicit", idempotency_key="k1")
        self.assertEqual(first["run_id"], "r-explicit")
        # same run_id omitted or equal -> reuse
        self.assertEqual(self.start(run_id="r-explicit", idempotency_key="k1")["run_id"],
                         "r-explicit")
        self.assertEqual(self.start(idempotency_key="k1")["run_id"], "r-explicit")
        # equal content but a different explicit run_id -> 409
        with self.assertRaises(ConflictError) as ctx:
            self.start(run_id="r-other", idempotency_key="k1")
        self.assertEqual(ctx.exception.code, "idempotency_conflict")
        self.assertEqual(len(self.scheduler.list_runs("acme")[0]), 1)

    def test_unkeyed_create_ignores_existing_key_binding(self):
        self.submit()
        first = self.start(run_id="r1", idempotency_key="k1")
        # a create without a key is legacy and always makes a new run
        second = self.start(run_id="r2")
        self.assertNotEqual(second["run_id"], first["run_id"])
        self.assertEqual(self.store.load_idempotency("acme")["k1"]["run_id"], "r1")


class ParamsEqualityTest(IdempotencyTestBase):
    def test_params_default_to_empty_object(self):
        self.submit()
        first = self.start(idempotency_key="k")
        self.assertEqual(first["params"], {})
        for repeated in (None, {}):
            hit = self.start(params=repeated, idempotency_key="k")
            self.assertEqual(hit["run_id"], first["run_id"])

    def test_params_must_be_object_when_keyed(self):
        self.submit()
        for bad in ([1, 2], "x", 5, 1.5, True):
            with self.assertRaises(WorkflowError) as ctx:
                self.start(params=bad, idempotency_key="k-%r" % (bad,))
            self.assertEqual(ctx.exception.code, "bad_params", repr(bad))
        # no runs, and no index entries, were created
        self.assertEqual(self.scheduler.list_runs("acme")[0], [])
        self.assertEqual(self.store.load_idempotency("acme"), {})

    def test_key_order_ignored_array_order_kept(self):
        self.submit()
        first = self.start(params={"o": {"a": 1, "b": 2}, "l": [1, 2]}, idempotency_key="k")
        hit = self.start(params={"l": [1, 2], "o": {"b": 2, "a": 1}}, idempotency_key="k")
        self.assertEqual(hit["run_id"], first["run_id"])
        with self.assertRaises(ConflictError):
            self.start(params={"o": {"a": 1, "b": 2}, "l": [2, 1]}, idempotency_key="k")

    def test_booleans_are_not_numbers_but_numbers_compare_numerically(self):
        self.submit()
        self.start(params={"v": True}, idempotency_key="kb")
        with self.assertRaises(ConflictError):
            self.start(params={"v": 1}, idempotency_key="kb")
        with self.assertRaises(ConflictError):
            self.start(params={"v": "true"}, idempotency_key="kb")

        numeric = self.start(params={"v": 1}, idempotency_key="kn")
        self.assertEqual(self.start(params={"v": 1.0}, idempotency_key="kn")["run_id"],
                         numeric["run_id"])
        with self.assertRaises(ConflictError):
            self.start(params={"v": False}, idempotency_key="kn")

    def test_nested_structures_compare_semantically(self):
        self.submit()
        value = {"outer": {"list": [{"z": 1, "a": 2}, True, None], "n": 2.0}}
        self.start(params=value, idempotency_key="k")
        same = {"outer": {"n": 2, "list": [{"a": 2, "z": 1}, True, None]}}
        self.assertEqual(self.start(params=same, idempotency_key="k")["params"], value)
        with self.assertRaises(ConflictError):
            self.start(params={"outer": {"list": [{"z": 1, "a": 2}, 1, None], "n": 2.0}},
                       idempotency_key="k")


class ContentConflictTest(IdempotencyTestBase):
    def test_different_workflow_params_or_quota_conflicts(self):
        self.submit()
        self.scheduler.submit("acme", "other", SOLO)
        first = self.start(workflow_id="etl", params={"a": 1}, max_parallelism=2,
                           idempotency_key="k")

        with self.assertRaises(ConflictError):
            self.start(workflow_id="other", params={"a": 1}, max_parallelism=2,
                       idempotency_key="k")
        with self.assertRaises(ConflictError):
            self.start(workflow_id="etl", params={"a": 2}, max_parallelism=2,
                       idempotency_key="k")
        with self.assertRaises(ConflictError):
            self.start(workflow_id="etl", params={"a": 1}, max_parallelism=3,
                       idempotency_key="k")
        # omitted/null quota means unlimited, which differs from quota 2
        with self.assertRaises(ConflictError):
            self.start(workflow_id="etl", params={"a": 1}, idempotency_key="k")
        with self.assertRaises(ConflictError):
            self.start(workflow_id="etl", params={"a": 1}, max_parallelism=None,
                       idempotency_key="k")

        # identical request still replays, and the conflict created nothing
        hit = self.start(workflow_id="etl", params={"a": 1}, max_parallelism=2,
                         idempotency_key="k")
        self.assertEqual(hit["run_id"], first["run_id"])
        runs = self.scheduler.list_runs("acme")[0]
        self.assertEqual(len(runs), 1)
        unchanged = self.scheduler.get_run("acme", first["run_id"])
        self.assertEqual((unchanged["workflow_id"], unchanged["params"],
                          unchanged["max_parallelism"]),
                         ("etl", {"a": 1}, 2))

    def test_unlimited_quota_then_explicit_quota_conflicts(self):
        self.submit()
        self.start(workflow_id="etl", idempotency_key="k")
        with self.assertRaises(ConflictError):
            self.start(workflow_id="etl", max_parallelism=1, idempotency_key="k")

    def test_validation_errors_take_precedence(self):
        # bad key first, even when everything else is also wrong/unknown
        with self.assertRaises(WorkflowError) as ctx:
            self.start(workflow_id="ghost", params=[1], max_parallelism=0,
                       idempotency_key=9)
        self.assertEqual(ctx.exception.code, "bad_idempotency_key")
        self.submit()
        with self.assertRaises(WorkflowError) as ctx:
            self.start(workflow_id="ghost", params=[1], max_parallelism=0,
                       idempotency_key="k")
        self.assertEqual(ctx.exception.code, "bad_max_parallelism")
        with self.assertRaises(WorkflowError) as ctx:
            self.start(workflow_id="ghost", params=[1], idempotency_key="k")
        self.assertEqual(ctx.exception.code, "bad_params")
        # a valid request body against a new key + unknown workflow is a 404
        with self.assertRaises(WorkflowError) as ctx:
            self.start(workflow_id="ghost", params={}, idempotency_key="k")
        self.assertEqual(ctx.exception.code, "unknown_workflow")

    def test_overwriting_workflow_does_not_change_replay(self):
        self.submit(workflow_id="etl", steps=LINEAR)
        first = self.start(workflow_id="etl", params={"v": 1}, idempotency_key="k")
        # same workflow name, incompatible definition
        self.scheduler.submit("acme", "etl", [{"id": "only", "depends_on": []}])
        hit = self.start(workflow_id="etl", params={"v": 1}, idempotency_key="k")
        self.assertEqual(hit["run_id"], first["run_id"])
        self.assertEqual(hit["step_order"], ["step1", "step2", "step3"])
        with self.assertRaises(ConflictError):
            self.start(workflow_id="etl", params={"v": 2}, idempotency_key="k")


class TenantIsolationTest(IdempotencyTestBase):
    def test_same_key_in_different_tenants_is_independent(self):
        self.submit("acme")
        self.submit("globex")
        acme = self.start("acme", params={"t": "a"}, idempotency_key="shared")
        globex = self.start("globex", params={"t": "g"}, idempotency_key="shared")
        self.assertNotEqual(acme["run_id"], globex["run_id"])
        self.assertEqual(self.start("acme", params={"t": "a"}, idempotency_key="shared")
                         ["run_id"], acme["run_id"])
        self.assertEqual(self.start("globex", params={"t": "g"}, idempotency_key="shared")
                         ["run_id"], globex["run_id"])
        # identical payload under the other tenant still conflicts on its own run
        with self.assertRaises(ConflictError):
            self.start("globex", params={"t": "a"}, idempotency_key="shared")

    def test_index_file_lives_under_tenant_dir(self):
        self.submit()
        self.start(idempotency_key="k")
        path = os.path.join(self.root, "acme", "idempotency.json")
        self.assertTrue(os.path.isfile(path))
        with open(path, "r", encoding="utf-8") as handle:
            self.assertEqual(json.load(handle), {"k": {"run_id": self.scheduler.list_runs(
                "acme")[0][0]["run_id"]}})


class PersistenceTest(IdempotencyTestBase):
    def test_binding_survives_reopen(self):
        self.submit()
        first = self.start(params={"a": 1}, idempotency_key="persist")
        restarted = self.restart()
        hit = restarted.start_run("acme", "etl", params={"a": 1}, idempotency_key="persist")
        self.assertEqual(hit["run_id"], first["run_id"])
        with self.assertRaises(ConflictError):
            restarted.start_run("acme", "etl", params={"a": 2}, idempotency_key="persist")

    def test_old_run_without_field_reports_null_and_never_matches(self):
        self.submit()
        run = self.start(run_id="old")
        path = os.path.join(self.root, "acme", "runs", "old.json")
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        # simulate a document written before idempotency keys existed
        del doc["idempotency_key"]
        atomic_write_json(path, doc)
        with open(path, "r", encoding="utf-8") as handle:
            self.assertNotIn("idempotency_key", json.load(handle))
        reloaded = self.scheduler.get_run("acme", "old")
        self.assertIsNone(reloaded["idempotency_key"])
        self.assertIsNone(self.scheduler.replay("acme", "old")["idempotency_key"])
        # a keyed create does not dedup against the keyless run
        keyed = self.start(idempotency_key="k")
        self.assertNotEqual(keyed["run_id"], run["run_id"])
        self.assertEqual(len(self.scheduler.list_runs("acme")[0]), 2)

    def test_replay_reports_key(self):
        self.submit()
        run = self.start(params={"a": 1}, idempotency_key="rk")
        replayed = self.scheduler.replay("acme", run["run_id"])
        self.assertEqual(replayed["idempotency_key"], "rk")
        self.assertEqual(replayed["run_id"], run["run_id"])

    def test_listing_reports_key(self):
        self.submit()
        run = self.start(idempotency_key="lk")
        items = self.scheduler.list_runs("acme")[0]
        self.assertEqual([r["idempotency_key"] for r in items], ["lk"])
        self.assertEqual(items[0]["run_id"], run["run_id"])


class ConcurrencyTest(IdempotencyTestBase):
    def test_concurrent_identical_requests_create_one_run(self):
        self.submit()
        results, errors = [], []
        barrier = threading.Barrier(8)

        def fire():
            barrier.wait()
            try:
                results.append(self.scheduler.start_run(
                    "acme", "etl", params={"a": 1}, idempotency_key="hot"))
            except ConflictError as exc:  # pragma: no cover - must not happen
                errors.append(exc)

        threads = [threading.Thread(target=fire) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual({r["run_id"] for r in results}, {results[0]["run_id"]})
        self.assertEqual(len(results), 8)
        self.assertEqual(len(self.scheduler.list_runs("acme")[0]), 1)

    def test_concurrent_different_requests_accept_only_first_content(self):
        self.submit()
        winners, rejected = [], []
        barrier = threading.Barrier(8)

        def fire(index):
            barrier.wait()
            try:
                winners.append(self.scheduler.start_run(
                    "acme", "etl", params={"i": index}, idempotency_key="hot"))
            except ConflictError:
                rejected.append(index)

        threads = [threading.Thread(target=fire, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(rejected), 7)
        runs = self.scheduler.list_runs("acme")[0]
        self.assertEqual(len(runs), 1)
        bound = self.store.load_idempotency("acme")["hot"]["run_id"]
        self.assertEqual(bound, winners[0]["run_id"])
        self.assertEqual(self.scheduler.get_run("acme", bound)["params"],
                         winners[0]["params"])


class CliIdempotencyTest(IdempotencyTestBase):
    def cli(self, *argv):
        output = io.StringIO()
        with redirect_stdout(output):
            code = main(["--data-dir", self.root] + list(argv))
        self.assertEqual(code, 0, output.getvalue())
        return json.loads(output.getvalue())

    def test_status_reports_key_or_null(self):
        self.submit()
        keyed = self.start(idempotency_key="cli-k")
        body = self.cli("status", "--run-id", keyed["run_id"], "--tenant", "acme")
        self.assertEqual(body["idempotency_key"], "cli-k")

        legacy = self.start(run_id="legacy")
        path = os.path.join(self.root, "acme", "runs", "legacy.json")
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        del doc["idempotency_key"]
        atomic_write_json(path, doc)
        body = self.cli("status", "--run-id", "legacy", "--tenant", "acme")
        self.assertIsNone(body["idempotency_key"])


if __name__ == "__main__":
    unittest.main()
