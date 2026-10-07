"""Tests for per-step retry backoff bases (``retry_backoff_seconds``)."""

import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from flowd.cli import main as cli_main
from flowd.http_app import create_server
from flowd.model import WorkflowError, format_time, plan_workflow
from flowd.scheduler import Scheduler
from flowd.store import WorkflowStore


class FakeClock:
    """Injectible deterministic clock."""

    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value


class ValidationTest(unittest.TestCase):
    def plan(self, steps):
        return plan_workflow("w", steps)

    def test_omitted_and_null_mean_scheduler_default(self):
        plan = self.plan([{"id": "a"}, {"id": "b", "retry_backoff_seconds": None}])
        self.assertIsNone(plan["steps"][0]["retry_backoff_seconds"])
        self.assertIsNone(plan["steps"][1]["retry_backoff_seconds"])

    def test_positive_numbers_are_normalized_to_float(self):
        plan = self.plan([{"id": "a", "retry_backoff_seconds": 5},
                          {"id": "b", "retry_backoff_seconds": 0.5}])
        self.assertEqual(plan["steps"][0]["retry_backoff_seconds"], 5.0)
        self.assertEqual(plan["steps"][1]["retry_backoff_seconds"], 0.5)

    def test_bad_values_are_rejected(self):
        for value in (True, False, "5", "1.5", 0, 0.0, -1, -0.5,
                      float("nan"), float("inf"), -float("inf"), [], {}):
            with self.assertRaises(WorkflowError) as ctx:
                self.plan([{"id": "a", "retry_backoff_seconds": value}])
            self.assertEqual(ctx.exception.code, "bad_retry_backoff", value)

    def test_approval_steps_cannot_set_a_backoff(self):
        with self.assertRaises(WorkflowError) as ctx:
            self.plan([{"id": "a", "kind": "approval", "retry_backoff_seconds": 2}])
        self.assertEqual(ctx.exception.code, "bad_retry_backoff")
        # An explicit null on an approval step is fine (means default).
        plan = self.plan([{"id": "a", "kind": "approval", "retry_backoff_seconds": None}])
        self.assertIsNone(plan["steps"][0]["retry_backoff_seconds"])


class SchedulerBackoffTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-test-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock, backoff_base=1.0)

    def workflow(self, workflow_id="flaky", steps=None):
        return self.scheduler.submit("acme", workflow_id,
                                     steps or [{"id": "s", "max_attempts": 3,
                                                "retry_backoff_seconds": 4}])

    def test_fail_uses_the_step_backoff_base(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "flaky")
        run_id = run["run_id"]
        self.scheduler.claim("acme", run_id, "w1")
        run = self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        step = run["steps"]["s"]
        self.assertEqual((step["status"], step["attempt"]), ("ready", 1))
        # First failure waits exactly the configured base (4s, not the 1s
        # scheduler default).
        self.assertEqual(step["next_attempt_at"], format_time(self.clock.value + 4))

        self.clock.advance(3.9)
        self.assertIsNone(self.scheduler.claim("acme", run_id, "w1"))
        self.clock.advance(0.1)  # exactly at next_attempt_at: claimable
        self.assertIsNotNone(self.scheduler.claim("acme", run_id, "w1"))
        run = self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        # Second failure: base * 2 ** (attempt - 1) = 4 * 2 = 8.
        self.assertEqual(run["steps"]["s"]["next_attempt_at"],
                         format_time(self.clock.value + 8))

    def test_unconfigured_step_keeps_the_scheduler_base(self):
        self.workflow(steps=[{"id": "s", "max_attempts": 2}])
        run = self.scheduler.start_run("acme", "flaky")
        self.scheduler.claim("acme", run["run_id"], "w1")
        run = self.scheduler.fail("acme", run["run_id"], "s", "w1", "boom")
        self.assertIsNone(run["steps"]["s"]["retry_backoff_seconds"])
        self.assertEqual(run["steps"]["s"]["next_attempt_at"],
                         format_time(self.clock.value + 1))

    def test_max_attempts_still_fails_immediately(self):
        self.workflow(steps=[{"id": "s", "max_attempts": 1, "retry_backoff_seconds": 4}])
        run = self.scheduler.start_run("acme", "flaky")
        self.scheduler.claim("acme", run["run_id"], "w1")
        run = self.scheduler.fail("acme", run["run_id"], "s", "w1", "boom")
        step = run["steps"]["s"]
        self.assertEqual((step["status"], step["next_attempt_at"]), ("failed", None))
        self.assertEqual(run["status"], "failed")

    def test_fair_claim_respects_the_pending_backoff(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "flaky")
        run_id = run["run_id"]
        self.scheduler.claim("acme", run_id, "w1")
        self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        self.assertIsNone(self.scheduler.claim_fair("acme", "w1"))
        self.clock.advance(4)
        run_id2, step = self.scheduler.claim_fair("acme", "w1")
        self.assertEqual((run_id2, step["id"]), (run_id, "s"))

    def test_policy_is_frozen_at_run_creation(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "flaky")
        run_id = run["run_id"]
        # A later same-name submission must not rewrite the run's policy.
        self.workflow(steps=[{"id": "s", "max_attempts": 3, "retry_backoff_seconds": 100}])
        self.scheduler.claim("acme", run_id, "w1")
        run = self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        step = run["steps"]["s"]
        self.assertEqual(step["retry_backoff_seconds"], 4.0)
        self.assertEqual(step["next_attempt_at"], format_time(self.clock.value + 4))
        # Runs created after the resubmission use the new policy.
        run2 = self.scheduler.start_run("acme", "flaky")
        self.assertEqual(run2["steps"]["s"]["retry_backoff_seconds"], 100.0)

    def test_idempotent_and_scheduled_runs_freeze_the_policy(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "flaky", idempotency_key="k1")
        self.assertEqual(run["steps"]["s"]["retry_backoff_seconds"], 4.0)
        again = self.scheduler.start_run("acme", "flaky", idempotency_key="k1")
        self.assertEqual(again["run_id"], run["run_id"])
        self.scheduler.create_schedule("acme", "sch", "flaky", 60)
        dispatched, _ = self.scheduler.dispatch_schedule("acme", "sch")
        self.assertEqual(dispatched["steps"]["s"]["retry_backoff_seconds"], 4.0)

    def test_replay_reproduces_policy_and_state_after_restart(self):
        self.workflow()
        run = self.scheduler.start_run("acme", "flaky")
        run_id = run["run_id"]
        self.scheduler.claim("acme", run_id, "w1")
        self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        # Restart from the same data directory and replay.
        scheduler = Scheduler(WorkflowStore(self.root, clock=self.clock),
                              clock=self.clock, backoff_base=1.0)
        rebuilt = scheduler.replay("acme", run_id)
        stored = scheduler.get_run("acme", run_id)
        self.assertEqual(rebuilt["status"], stored["status"])
        step = rebuilt["steps"]["s"]
        self.assertEqual(step["retry_backoff_seconds"], 4.0)
        self.assertEqual(step["next_attempt_at"],
                         stored["steps"]["s"]["next_attempt_at"])
        self.assertEqual(step["status"], "ready")
        # The retry history event kept the computed time.
        retries = [e for e in rebuilt["history"] if e["type"] == "retry"]
        self.assertEqual(retries[-1]["next_attempt_at"], step["next_attempt_at"])
        # Still not claimable until the frozen backoff elapses.
        self.assertIsNone(scheduler.claim("acme", run_id, "w1"))
        self.clock.advance(4)
        self.assertIsNotNone(scheduler.claim("acme", run_id, "w1"))

    def test_legacy_documents_without_the_field_use_the_default(self):
        self.workflow(steps=[{"id": "s", "max_attempts": 2}])
        run = self.scheduler.start_run("acme", "flaky")
        run_id = run["run_id"]
        # Strip the field as a document written by an older version would.
        path = self.store.run_path("acme", run_id)
        with open(path, "r", encoding="utf-8") as handle:
            doc = json.load(handle)
        for step in doc["steps"].values():
            del step["retry_backoff_seconds"]
        for planned in doc["plan"]["steps"]:
            del planned["retry_backoff_seconds"]
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(doc, handle)
        scheduler = Scheduler(WorkflowStore(self.root, clock=self.clock),
                              clock=self.clock, backoff_base=1.0)
        scheduler.claim("acme", run_id, "w1")
        run = scheduler.fail("acme", run_id, "s", "w1", "boom")
        self.assertIsNone(run["steps"]["s"]["retry_backoff_seconds"])
        self.assertEqual(run["steps"]["s"]["next_attempt_at"],
                         format_time(self.clock.value + 1))


class HttpBackoffTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request("http://127.0.0.1:%d%s" % (self.port, path),
                                         data=data, method=method)
        if data is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                payload = response.read()
                return response.status, (json.loads(payload) if payload else None)
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            return exc.code, (json.loads(payload) if payload else None)

    def submit(self, steps, workflow_id="etl"):
        return self.call("POST", "/v1/workflows",
                         {"tenant": "acme", "workflow_id": workflow_id, "steps": steps})

    def test_submit_returns_the_normalized_field(self):
        status, body = self.submit([{"id": "a", "retry_backoff_seconds": 5}, {"id": "b"}])
        self.assertEqual(status, 201, body)
        steps = {s["id"]: s for s in body["steps"]}
        self.assertEqual(steps["a"]["retry_backoff_seconds"], 5.0)
        self.assertIsNone(steps["b"]["retry_backoff_seconds"])

    def test_bad_values_return_400_without_overwriting_or_auditing(self):
        status, body = self.submit([{"id": "a", "retry_backoff_seconds": 5}])
        self.assertEqual(status, 201, body)
        for value in (True, "5", 0, -1, 1.7976931348623157e308 * 10):
            status, body = self.submit([{"id": "a", "retry_backoff_seconds": value}])
            self.assertEqual(status, 400, (value, body))
            self.assertIn("retry_backoff", body["error"])
        status, body = self.submit([{"id": "a", "kind": "approval",
                                     "retry_backoff_seconds": 2}])
        self.assertEqual(status, 400, body)
        # The stored workflow is unchanged and no audit records were added.
        status, body = self.call("GET", "/v1/audit?tenant=acme")
        self.assertEqual([r["action"] for r in body["items"]], ["workflow.submit"])
        status, body = self.call("POST", "/v1/runs",
                                 {"tenant": "acme", "workflow_id": "etl", "run_id": "r1"})
        self.assertEqual(status, 201, body)
        self.assertEqual(body["steps"][0]["retry_backoff_seconds"], 5.0)

    def test_run_views_expose_the_frozen_policy(self):
        self.submit([{"id": "a", "max_attempts": 2, "retry_backoff_seconds": 0.5}])
        self.call("POST", "/v1/runs", {"tenant": "acme", "workflow_id": "etl", "run_id": "r1"})
        status, body = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(status, 200, body)
        self.assertEqual(body["steps"][0]["retry_backoff_seconds"], 0.5)
        status, body = self.call("GET", "/v1/runs?tenant=acme")
        self.assertEqual(body["items"][0]["steps"][0]["retry_backoff_seconds"], 0.5)


class CliBackoffTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-cli-")
        self.addCleanup(shutil.rmtree, self.root, True)

    def write_steps(self, steps):
        path = os.path.join(self.root, "steps.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(steps, handle)
        return path

    def test_cli_submit_follows_the_same_rules(self):
        path = self.write_steps([{"id": "a", "retry_backoff_seconds": -2}])
        rc = cli_main(["--data-dir", self.root, "submit",
                       "--tenant", "acme", "--workflow", "w", "--file", path])
        self.assertEqual(rc, 1)
        store = WorkflowStore(self.root)
        self.assertEqual(store.load_workflows("acme"), {})
        path = self.write_steps([{"id": "a", "retry_backoff_seconds": 3}])
        rc = cli_main(["--data-dir", self.root, "submit",
                       "--tenant", "acme", "--workflow", "w", "--file", path])
        self.assertEqual(rc, 0)
        plan = WorkflowStore(self.root).get_workflow("acme", "w")
        self.assertEqual(plan["steps"][0]["retry_backoff_seconds"], 3.0)


if __name__ == "__main__":
    unittest.main()
