"""Tests for per-step retry backoff bases (retry_backoff_seconds)."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from flowd.http_app import create_server
from flowd.model import WorkflowError, plan_workflow
from flowd.scheduler import Scheduler
from flowd.store import WorkflowStore

FLAKY = [{"id": "s", "depends_on": [], "max_attempts": 3, "retry_backoff_seconds": 5}]


class FakeClock:
    """Injectible deterministic clock; sleeping advances virtual time."""

    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value


class ValidationTest(unittest.TestCase):
    def plan(self, step):
        return plan_workflow("w", [step])["steps"][0]

    def test_omitted_and_null_mean_scheduler_default(self):
        self.assertIsNone(self.plan({"id": "a"})["retry_backoff_seconds"])
        self.assertIsNone(
            self.plan({"id": "a", "retry_backoff_seconds": None})["retry_backoff_seconds"])

    def test_valid_values_are_normalized_to_float(self):
        self.assertEqual(self.plan({"id": "a", "retry_backoff_seconds": 5})[
                         "retry_backoff_seconds"], 5.0)
        self.assertEqual(self.plan({"id": "a", "retry_backoff_seconds": 0.5})[
                         "retry_backoff_seconds"], 0.5)

    def test_bad_values_rejected(self):
        for bad in (True, False, "5", "1.5", 0, -1, -0.5, float("nan"),
                    float("inf"), float("-inf")):
            with self.assertRaises(WorkflowError) as ctx:
                self.plan({"id": "a", "retry_backoff_seconds": bad})
            self.assertEqual(ctx.exception.code, "bad_retry_backoff", "value %r" % (bad,))

    def test_approval_step_cannot_set_backoff(self):
        with self.assertRaises(WorkflowError) as ctx:
            self.plan({"id": "a", "kind": "approval", "retry_backoff_seconds": 5})
        self.assertEqual(ctx.exception.code, "bad_retry_backoff")
        # A null value on an approval step is fine: it means "no override".
        self.assertIsNone(self.plan({"id": "a", "kind": "approval",
                                     "retry_backoff_seconds": None})["retry_backoff_seconds"])


class SchedulerBackoffTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-test-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock, backoff_base=1.0)

    def start(self, steps, workflow_id="flaky"):
        self.scheduler.submit("acme", workflow_id, steps)
        return self.scheduler.start_run("acme", workflow_id)

    def test_fail_uses_the_step_backoff_base(self):
        run = self.start(FLAKY)
        run_id = run["run_id"]
        self.assertEqual(run["steps"]["s"]["retry_backoff_seconds"], 5.0)
        self.scheduler.claim("acme", run_id, "w1", 30)
        run = self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        step = run["steps"]["s"]
        self.assertEqual((step["status"], step["attempt"]), ("ready", 1))
        # First failure waits exactly the base: 5 seconds.
        self.assertEqual(step["next_attempt_at"], "2023-11-14T22:13:25.000000Z")
        retry = [e for e in run["history"] if e["type"] == "retry"][0]
        self.assertEqual(retry["next_attempt_at"], step["next_attempt_at"])

        self.clock.advance(4.9)  # still waiting: ready but not claimable
        self.assertIsNone(self.scheduler.claim("acme", run_id, "w1", 30))
        self.clock.advance(0.1)  # exactly at next_attempt_at: claimable
        claimed = self.scheduler.claim("acme", run_id, "w1", 30)
        self.assertEqual((claimed["id"], claimed["attempt"]), ("s", 1))

        run = self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        # Second failure: base * 2 ** (attempt - 1) = 5 * 2 = 10 seconds.
        self.assertEqual(run["steps"]["s"]["next_attempt_at"],
                         "2023-11-14T22:13:35.000000Z")
        self.clock.advance(10)
        self.scheduler.claim("acme", run_id, "w1", 30)
        run = self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        # max_attempts reached: immediate failure, no next attempt.
        self.assertEqual(run["steps"]["s"]["status"], "failed")
        self.assertIsNone(run["steps"]["s"]["next_attempt_at"])
        self.assertEqual(run["status"], "failed")

    def test_unconfigured_step_keeps_scheduler_base(self):
        run = self.start([{"id": "s", "depends_on": [], "max_attempts": 2}])
        run_id = run["run_id"]
        self.scheduler.claim("acme", run_id, "w1", 30)
        run = self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        self.assertEqual(run["steps"]["s"]["next_attempt_at"],
                         "2023-11-14T22:13:21.000000Z")  # 1s scheduler base

    def test_mixed_steps_use_their_own_bases(self):
        run = self.start([{"id": "a", "depends_on": [], "max_attempts": 2,
                           "retry_backoff_seconds": 10},
                          {"id": "b", "depends_on": [], "max_attempts": 2}])
        run_id = run["run_id"]
        self.scheduler.claim("acme", run_id, "w1", 30)
        self.scheduler.fail("acme", run_id, "a", "w1", "boom")
        self.scheduler.claim("acme", run_id, "w1", 30)
        run = self.scheduler.fail("acme", run_id, "b", "w1", "boom")
        self.assertEqual(run["steps"]["a"]["next_attempt_at"],
                         "2023-11-14T22:13:30.000000Z")
        self.assertEqual(run["steps"]["b"]["next_attempt_at"],
                         "2023-11-14T22:13:21.000000Z")

    def test_policy_is_frozen_at_run_creation(self):
        run = self.start(FLAKY)
        run_id = run["run_id"]
        # A later same-name submission must not rewrite the run's policy.
        self.scheduler.submit("acme", "flaky",
                              [{"id": "s", "depends_on": [], "max_attempts": 3,
                                "retry_backoff_seconds": 100}])
        self.scheduler.claim("acme", run_id, "w1", 30)
        run = self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        self.assertEqual(run["steps"]["s"]["next_attempt_at"],
                         "2023-11-14T22:13:25.000000Z")
        # Runs created after the resubmission use the new policy.
        later = self.scheduler.start_run("acme", "flaky")
        self.assertEqual(later["steps"]["s"]["retry_backoff_seconds"], 100.0)

    def test_idempotent_and_scheduled_runs_freeze_the_policy(self):
        self.scheduler.submit("acme", "flaky", FLAKY)
        keyed = self.scheduler.start_run("acme", "flaky", idempotency_key="k1")
        self.assertEqual(keyed["steps"]["s"]["retry_backoff_seconds"], 5.0)
        self.scheduler.create_schedule("acme", "every", "flaky", 60)
        scheduled, _ = self.scheduler.dispatch_schedule("acme", "every")
        self.assertEqual(scheduled["steps"]["s"]["retry_backoff_seconds"], 5.0)

    def test_fair_claim_respects_pending_backoff(self):
        run = self.start(FLAKY)
        run_id = run["run_id"]
        self.scheduler.claim("acme", run_id, "w1", 30)
        self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        self.clock.advance(4)  # custom 5s backoff not yet elapsed
        self.assertIsNone(self.scheduler.claim_fair("acme", "w1", 30))
        self.clock.advance(1)
        claimed_run, step = self.scheduler.claim_fair("acme", "w1", 30)
        self.assertEqual((claimed_run, step["id"]), (run_id, "s"))

    def test_replay_reproduces_the_same_policy_and_state(self):
        run = self.start(FLAKY)
        run_id = run["run_id"]
        self.scheduler.claim("acme", run_id, "w1", 30)
        self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        # Restart over the same data directory.
        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock),
                              clock=self.clock, backoff_base=1.0)
        replayed = restarted.replay("acme", run_id)
        stored = restarted.get_run("acme", run_id)
        step = replayed["steps"]["s"]
        self.assertEqual(step["retry_backoff_seconds"], 5.0)
        self.assertEqual(step["next_attempt_at"],
                         stored["steps"]["s"]["next_attempt_at"])
        self.assertEqual(step["status"], stored["steps"]["s"]["status"])
        # The restarted scheduler still cannot claim before the frozen time.
        self.assertIsNone(restarted.claim("acme", run_id, "w1", 30))
        self.clock.advance(5)
        self.assertEqual(restarted.claim("acme", run_id, "w1", 30)["id"], "s")

    def test_legacy_runs_without_the_field_keep_the_old_policy(self):
        run = self.start([{"id": "s", "depends_on": [], "max_attempts": 2}])
        run_id = run["run_id"]
        # Simulate a document written before retry_backoff_seconds existed.
        stored = self.store.load_run("acme", run_id)
        for node in stored["steps"].values():
            node.pop("retry_backoff_seconds", None)
        for planned in stored["plan"]["steps"]:
            planned.pop("retry_backoff_seconds", None)
        self.store.save_run(stored)
        self.scheduler.claim("acme", run_id, "w1", 30)
        run = self.scheduler.fail("acme", run_id, "s", "w1", "boom")
        self.assertIsNone(run["steps"]["s"]["retry_backoff_seconds"])
        self.assertEqual(run["steps"]["s"]["next_attempt_at"],
                         "2023-11-14T22:13:21.000000Z")  # 1s scheduler base
        replayed = self.scheduler.replay("acme", run_id)
        self.assertEqual(replayed["steps"]["s"]["next_attempt_at"],
                         run["steps"]["s"]["next_attempt_at"])


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

    def submit(self, steps, workflow_id="flaky"):
        return self.call("POST", "/v1/workflows",
                         {"tenant": "acme", "workflow_id": workflow_id, "steps": steps})

    def test_submit_returns_the_normalized_field(self):
        status, body = self.submit([
            {"id": "a", "depends_on": [], "retry_backoff_seconds": 5},
            {"id": "b", "depends_on": ["a"], "retry_backoff_seconds": 0.5},
            {"id": "c", "depends_on": ["a"]},
        ])
        self.assertEqual(status, 201, body)
        by_id = {s["id"]: s for s in body["steps"]}
        self.assertEqual(by_id["a"]["retry_backoff_seconds"], 5.0)
        self.assertEqual(by_id["b"]["retry_backoff_seconds"], 0.5)
        self.assertIsNone(by_id["c"]["retry_backoff_seconds"])

    def test_bad_values_return_400_and_change_nothing(self):
        status, body = self.submit([{"id": "a", "depends_on": [],
                                     "retry_backoff_seconds": 7}])
        self.assertEqual(status, 201, body)
        audit_before = self.call("GET", "/v1/audit?tenant=acme")[1]["items"]
        for bad in (True, "5", 0, -2, float("inf"), float("nan")):
            status, body = self.submit([{"id": "a", "depends_on": [],
                                         "retry_backoff_seconds": bad}])
            self.assertEqual(status, 400, "value %r" % (bad,))
            self.assertIn("retry_backoff_seconds", body["error"])
        # Approval steps reject a non-null backoff too.
        status, body = self.submit([{"id": "a", "depends_on": [], "kind": "approval",
                                     "retry_backoff_seconds": 3}])
        self.assertEqual(status, 400)
        # No failed submit appended an audit record.
        audit_after = self.call("GET", "/v1/audit?tenant=acme")[1]["items"]
        self.assertEqual(audit_after, audit_before)
        # The stored workflow is untouched: a new run still uses base 7.
        status, body = self.call("POST", "/v1/runs",
                                 {"tenant": "acme", "workflow_id": "flaky", "run_id": "r1"})
        self.assertEqual(status, 201, body)
        self.assertEqual(body["steps"][0]["retry_backoff_seconds"], 7.0)

    def test_run_views_expose_the_field(self):
        self.submit([{"id": "s", "depends_on": [], "max_attempts": 2,
                      "retry_backoff_seconds": 2}])
        status, body = self.call("POST", "/v1/runs",
                                 {"tenant": "acme", "workflow_id": "flaky", "run_id": "r1"})
        self.assertEqual(body["steps"][0]["retry_backoff_seconds"], 2.0)
        status, body = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(body["steps"][0]["retry_backoff_seconds"], 2.0)
        status, body = self.call("GET", "/v1/runs?tenant=acme")
        self.assertEqual(body["items"][0]["steps"][0]["retry_backoff_seconds"], 2.0)


class CliBackoffTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-cli-")
        self.addCleanup(shutil.rmtree, self.root, True)

    def run_cli(self, *argv):
        return subprocess.run(
            [sys.executable, "-m", "flowd", "--data-dir", self.root] + list(argv),
            capture_output=True, text=True,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    def write_steps(self, steps):
        path = os.path.join(self.root, "steps.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(steps, handle)
        return path

    def test_cli_submit_follows_the_same_rules(self):
        path = self.write_steps([{"id": "s", "depends_on": [], "retry_backoff_seconds": 4}])
        result = self.run_cli("submit", "--tenant", "acme", "--workflow", "flaky",
                              "--file", path)
        self.assertEqual(result.returncode, 0, result.stderr)
        bad = self.write_steps([{"id": "s", "depends_on": [], "retry_backoff_seconds": 0}])
        result = self.run_cli("submit", "--tenant", "acme", "--workflow", "flaky",
                              "--file", bad)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("retry_backoff_seconds", result.stderr)


if __name__ == "__main__":
    unittest.main()
