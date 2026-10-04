"""Tests for lease heartbeat renewal: Scheduler.heartbeat and the HTTP route."""

import json
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from flowd.http_app import create_server
from flowd.model import WorkflowError
from flowd.scheduler import ConflictError, LeaseError, Scheduler
from flowd.store import WorkflowStore

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
]
APPROVAL = [
    {"id": "gate", "depends_on": [], "kind": "approval"},
    {"id": "deploy", "depends_on": ["gate"]},
]


class FakeClock:
    def __init__(self, start=1_700_000_000.0):
        self.value = float(start)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)
        return self.value


class HeartbeatTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-hb-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)

    def running_step(self, steps=None, lease=30, worker="w1"):
        self.scheduler.submit("acme", "etl", steps or LINEAR)
        run = self.scheduler.start_run("acme", "etl")
        step = self.scheduler.claim("acme", run["run_id"], worker, lease)
        return run["run_id"], step["id"]


class HeartbeatRenewalTest(HeartbeatTestBase):
    def test_heartbeat_extends_deadline_and_records_event(self):
        run_id, sid = self.running_step(lease=30)
        self.clock.advance(10)
        before = self.scheduler.get_run("acme", run_id)
        updated_at = before["updated_at"]
        history_len = len(before["history"])

        run = self.scheduler.heartbeat("acme", run_id, sid, "w1", 60)
        step = run["steps"][sid]
        self.assertEqual(step["lease_deadline"], self.clock.value + 60)
        self.assertGreater(step["lease_deadline"], before["steps"][sid]["lease_deadline"])
        # only the deadline and updated_at change
        self.assertEqual(step["status"], "running")
        self.assertEqual(step["worker_id"], "w1")
        self.assertEqual(step["attempt"], 0)
        self.assertEqual(step["started_at"], before["steps"][sid]["started_at"])
        self.assertIsNone(step["result"])
        self.assertEqual(run["steps"]["step2"]["status"], "pending")
        self.assertGreater(run["updated_at"], updated_at)
        self.assertEqual(len(run["history"]), history_len + 1)
        event = run["history"][-1]
        self.assertEqual(event["type"], "heartbeat")
        self.assertEqual(event["step_id"], sid)
        self.assertEqual(event["worker_id"], "w1")
        self.assertEqual(event["attempt"], 0)
        self.assertEqual(event["lease_deadline"], step["lease_deadline"])
        for field in ("at", "run_id", "step_id", "type", "attempt", "worker_id"):
            self.assertIn(field, event)

    def test_heartbeat_defaults_to_thirty_seconds(self):
        run_id, sid = self.running_step(lease=5)
        run = self.scheduler.heartbeat("acme", run_id, sid, "w1")
        self.assertEqual(run["steps"][sid]["lease_deadline"], self.clock.value + 30)

    def test_heartbeat_keeps_the_larger_deadline(self):
        run_id, sid = self.running_step(lease=100)
        before = self.scheduler.get_run("acme", run_id)
        deadline = before["steps"][sid]["lease_deadline"]
        run = self.scheduler.heartbeat("acme", run_id, sid, "w1", 10)
        # now + 10 < current deadline -> unchanged, no event, same updated_at
        self.assertEqual(run["steps"][sid]["lease_deadline"], deadline)
        self.assertEqual(run["updated_at"], before["updated_at"])
        self.assertEqual(len(run["history"]), len(before["history"]))
        self.assertEqual(
            [e["type"] for e in self.scheduler.get_run("acme", run_id)["history"]].count(
                "heartbeat"),
            0,
        )

    def test_heartbeat_does_not_unlock_successors(self):
        run_id, sid = self.running_step()
        run = self.scheduler.heartbeat("acme", run_id, sid, "w1", 60)
        self.assertEqual(run["steps"]["step2"]["status"], "pending")
        self.assertIsNone(self.scheduler.claim("acme", run_id, "w2", 30))

    def test_heartbeat_survives_restart_and_gates_takeover(self):
        run_id, sid = self.running_step(lease=30)
        self.clock.advance(10)
        run = self.scheduler.heartbeat("acme", run_id, sid, "w1", 60)
        deadline = run["steps"][sid]["lease_deadline"]

        restarted = Scheduler(WorkflowStore(self.root, clock=self.clock), clock=self.clock)
        again = restarted.get_run("acme", run_id)
        self.assertEqual(again["steps"][sid]["lease_deadline"], deadline)
        # before the renewed deadline the lease is still held
        self.clock.advance(31)  # past the original 30s deadline, before the renewed one
        self.assertIsNone(restarted.claim("acme", run_id, "w2", 30))
        # at the renewed deadline the lease can be taken over via claim
        self.clock.advance(29)
        taken = restarted.claim("acme", run_id, "w2", 30)
        self.assertEqual((taken["id"], taken["worker_id"]), (sid, "w2"))

    def test_replay_matches_stored_state_after_heartbeat(self):
        run_id, sid = self.running_step(lease=30)
        self.clock.advance(5)
        stored = self.scheduler.heartbeat("acme", run_id, sid, "w1", 90)
        replayed = self.scheduler.replay("acme", run_id)
        self.assertEqual(replayed["status"], stored["status"])
        self.assertEqual(replayed["steps"][sid]["attempt"], stored["steps"][sid]["attempt"])
        self.assertEqual(replayed["steps"][sid]["worker_id"], "w1")
        self.assertEqual(replayed["steps"][sid]["lease_deadline"],
                         stored["steps"][sid]["lease_deadline"])

    def test_replay_matches_after_heartbeat_then_takeover(self):
        run_id, sid = self.running_step(lease=10)
        self.scheduler.heartbeat("acme", run_id, sid, "w1", 20)
        self.clock.advance(25)  # renewed lease expired
        taken = self.scheduler.claim("acme", run_id, "w2", 15)
        self.assertEqual(taken["worker_id"], "w2")
        stored = self.scheduler.get_run("acme", run_id)
        replayed = self.scheduler.replay("acme", run_id)
        step = replayed["steps"][sid]
        self.assertEqual(step["status"], "running")
        self.assertEqual(step["worker_id"], "w2")
        self.assertEqual(step["lease_deadline"], stored["steps"][sid]["lease_deadline"])
        self.assertEqual(replayed["status"], stored["status"])

    def test_old_holder_cannot_heartbeat_after_takeover(self):
        run_id, sid = self.running_step(lease=5)
        self.clock.advance(6)
        self.scheduler.claim("acme", run_id, "w2", 40)
        before = self.scheduler.get_run("acme", run_id)
        with self.assertRaises(LeaseError):
            self.scheduler.heartbeat("acme", run_id, sid, "w1", 100)
        after = self.scheduler.get_run("acme", run_id)
        self.assertEqual(after["steps"][sid]["worker_id"], "w2")
        self.assertEqual(after["steps"][sid]["lease_deadline"],
                         before["steps"][sid]["lease_deadline"])
        self.assertEqual(len(after["history"]), len(before["history"]))


class HeartbeatValidationTest(HeartbeatTestBase):
    def test_bad_string_fields_raise_workflow_error(self):
        run_id, sid = self.running_step()
        for bad in (None, 5, True, "", "   ", ["x"]):
            with self.assertRaises(WorkflowError):
                self.scheduler.heartbeat("acme", run_id, bad, "w1")
            with self.assertRaises(WorkflowError):
                self.scheduler.heartbeat("acme", run_id, sid, bad)

    def test_bad_lease_seconds_raise_workflow_error(self):
        run_id, sid = self.running_step()
        for bad in (None, True, False, 0, -1, "30", float("nan"), float("inf"), -float("inf")):
            with self.assertRaises(WorkflowError):
                self.scheduler.heartbeat("acme", run_id, sid, "w1", bad)

    def test_unknown_run_and_step_raise_workflow_error(self):
        run_id, sid = self.running_step()
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.heartbeat("acme", "nope", sid, "w1")
        self.assertEqual(ctx.exception.code, "unknown_run")
        with self.assertRaises(WorkflowError) as ctx:
            self.scheduler.heartbeat("acme", run_id, "ghost", "w1")
        self.assertEqual(ctx.exception.code, "unknown_step")

    def test_cross_tenant_raises_workflow_error(self):
        run_id, sid = self.running_step()
        self.scheduler.submit("globex", "etl", LINEAR)
        with self.assertRaises(WorkflowError):
            self.scheduler.heartbeat("globex", run_id, sid, "w1")

    def test_terminal_run_conflicts(self):
        self.scheduler.submit("acme", "one", [{"id": "s", "depends_on": []}])
        run = self.scheduler.start_run("acme", "one")
        self.scheduler.claim("acme", run["run_id"], "w1", 30)
        self.scheduler.complete("acme", run["run_id"], "s", "w1")
        with self.assertRaises(ConflictError):
            self.scheduler.heartbeat("acme", run["run_id"], "s", "w1")

    def test_approval_node_conflicts(self):
        self.scheduler.submit("acme", "appr", APPROVAL)
        run = self.scheduler.start_run("acme", "appr")
        self.assertEqual(run["steps"]["gate"]["status"], "waiting")
        with self.assertRaises(ConflictError):
            self.scheduler.heartbeat("acme", run["run_id"], "gate", "w1")

    def test_non_running_step_and_wrong_holder_raise_lease_error(self):
        run_id, sid = self.running_step()
        with self.assertRaises(LeaseError):
            self.scheduler.heartbeat("acme", run_id, sid, "w2")
        with self.assertRaises(LeaseError):
            self.scheduler.heartbeat("acme", run_id, "step2", "w1")

    def test_expired_lease_raises_lease_error(self):
        run_id, sid = self.running_step(lease=5)
        self.clock.advance(5)  # deadline reached exactly
        with self.assertRaises(LeaseError):
            self.scheduler.heartbeat("acme", run_id, sid, "w1")

    def test_rejected_heartbeat_changes_nothing(self):
        run_id, sid = self.running_step(lease=5)
        before = self.scheduler.get_run("acme", run_id)
        self.clock.advance(6)
        with self.assertRaises(LeaseError):
            self.scheduler.heartbeat("acme", run_id, sid, "w1")
        after = self.scheduler.get_run("acme", run_id)
        self.assertEqual(after["updated_at"], before["updated_at"])
        self.assertEqual(len(after["history"]), len(before["history"]))
        self.assertEqual(after["steps"][sid]["lease_deadline"],
                         before["steps"][sid]["lease_deadline"])

    def test_concurrent_heartbeats_never_shorten_the_deadline(self):
        run_id, sid = self.running_step(lease=30)
        errors = []

        def beat(seconds):
            try:
                self.scheduler.heartbeat("acme", run_id, sid, "w1", seconds)
            except WorkflowError as exc:  # pragma: no cover - defensive
                errors.append(exc)

        threads = [threading.Thread(target=beat, args=(s,)) for s in (50, 90, 40, 120, 60)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        step = self.scheduler.get_run("acme", run_id)["steps"][sid]
        self.assertEqual(step["lease_deadline"], self.clock.value + 120)


class HeartbeatHttpTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-hb-http-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.clock = FakeClock()
        self.store = WorkflowStore(self.root, clock=self.clock)
        self.scheduler = Scheduler(self.store, clock=self.clock)
        self.server = create_server(self.store, "127.0.0.1", 0, scheduler=self.scheduler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def call(self, method, path, body=None, raw_body=None):
        data = raw_body
        if body is not None:
            data = json.dumps(body).encode("utf-8")
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

    def start_running(self, steps=None, lease=60, run_id="r1"):
        self.call("POST", "/v1/workflows",
                  {"tenant": "acme", "workflow_id": "etl", "steps": steps or LINEAR})
        self.call("POST", "/v1/runs",
                  {"tenant": "acme", "workflow_id": "etl", "run_id": run_id})
        status, body = self.call("POST", "/v1/runs/%s/claim" % run_id,
                                 {"tenant": "acme", "worker_id": "w1",
                                  "lease_seconds": lease})
        self.assertEqual(status, 200, body)
        return body["step"]["id"]

    def heartbeat(self, run_id="r1", step_id="step1", tenant="acme", worker="w1", **extra):
        body = {"tenant": tenant, "step_id": step_id, "worker_id": worker}
        body.update(extra)
        return self.call("POST", "/v1/runs/%s/heartbeat" % run_id, body)

    def test_heartbeat_returns_run_view(self):
        self.start_running(lease=30)
        self.clock.advance(10)
        status, body = self.heartbeat(lease_seconds=60)
        self.assertEqual(status, 200, body)
        self.assertEqual(body["run_id"], "r1")
        self.assertEqual(body["status"], "running")
        step = body["steps"][0]
        self.assertEqual(step["lease_deadline"], self.clock.value + 60)
        self.assertEqual(step["worker_id"], "w1")

    def test_heartbeat_omitted_lease_defaults_to_thirty(self):
        self.start_running(lease=5)
        status, body = self.heartbeat()
        self.assertEqual(status, 200, body)
        self.assertEqual(body["steps"][0]["lease_deadline"], self.clock.value + 30)

    def test_bad_requests_are_400(self):
        self.start_running()
        for body in ({"tenant": "acme", "step_id": "step1"},  # missing worker_id
                     {"tenant": "acme", "worker_id": "w1"},  # missing step_id
                     {"tenant": "acme", "step_id": "", "worker_id": "w1"},
                     {"tenant": "acme", "step_id": "  ", "worker_id": "w1"},
                     {"tenant": "acme", "step_id": 7, "worker_id": "w1"},
                     {"tenant": "acme", "step_id": "step1", "worker_id": " "},
                     {"tenant": "acme", "step_id": "step1", "worker_id": "w1",
                      "lease_seconds": None},
                     {"tenant": "acme", "step_id": "step1", "worker_id": "w1",
                      "lease_seconds": True},
                     {"tenant": "acme", "step_id": "step1", "worker_id": "w1",
                      "lease_seconds": 0},
                     {"tenant": "acme", "step_id": "step1", "worker_id": "w1",
                      "lease_seconds": -3},
                     {"tenant": "acme", "step_id": "step1", "worker_id": "w1",
                      "lease_seconds": "30"}):
            status, body_out = self.call("POST", "/v1/runs/r1/heartbeat", body)
            self.assertEqual(status, 400, (body, body_out))
            self.assertIn("error", body_out)
        status, _ = self.call("POST", "/v1/runs/r1/heartbeat", raw_body=b"{nope")
        self.assertEqual(status, 400)
        status, _ = self.call("POST", "/v1/runs/r1/heartbeat", raw_body=b"[1, 2]")
        self.assertEqual(status, 400)

    def test_cross_tenant_is_403_and_unknown_is_404(self):
        self.start_running()
        status, _ = self.heartbeat(tenant="globex")
        self.assertEqual(status, 403)
        status, _ = self.heartbeat(run_id="ghost")
        self.assertEqual(status, 404)
        status, _ = self.heartbeat(step_id="ghost")
        self.assertEqual(status, 404)

    def test_conflicts_are_409(self):
        self.start_running(lease=30)
        # wrong holder
        status, _ = self.heartbeat(worker="w2")
        self.assertEqual(status, 409)
        # non-running step
        status, _ = self.heartbeat(step_id="step2")
        self.assertEqual(status, 409)
        # expired lease
        self.clock.advance(31)
        status, _ = self.heartbeat()
        self.assertEqual(status, 409)

    def test_terminal_run_and_approval_are_409(self):
        self.start_running(steps=[{"id": "s", "depends_on": []}], run_id="r1")
        status, body = self.call("POST", "/v1/runs/r1/complete",
                                 {"tenant": "acme", "step_id": "s", "worker_id": "w1"})
        self.assertEqual(status, 200, body)
        status, _ = self.heartbeat(step_id="s")
        self.assertEqual(status, 409)

        self.call("POST", "/v1/workflows",
                  {"tenant": "acme", "workflow_id": "appr", "steps": APPROVAL})
        self.call("POST", "/v1/runs",
                  {"tenant": "acme", "workflow_id": "appr", "run_id": "r2"})
        status, _ = self.heartbeat(run_id="r2", step_id="gate")
        self.assertEqual(status, 409)


if __name__ == "__main__":
    unittest.main()
