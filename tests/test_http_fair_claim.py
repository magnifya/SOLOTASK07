"""End-to-end HTTP tests for the tenant-scoped fair claim.

``POST /v1/tasks/claim`` takes ``{"tenant","worker_id","lease_seconds"?}``
and returns 200 ``{"run_id":...,"step":...}`` (the same step payload as the
single-run claim) or 204 when no run has a leasable task.
"""

import json
import os
import shutil
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from flowd.http_app import create_server
from flowd.store import WorkflowStore

FAN = [{"id": "a", "depends_on": []}, {"id": "b", "depends_on": []},
       {"id": "c", "depends_on": []}]
SOLO = [{"id": "t", "depends_on": []}]
APPROVAL_ONLY = [{"id": "g", "depends_on": [], "kind": "approval"}]


class FairClaimHttpTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-fair-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
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

    def submit(self, workflow_id="fan", steps=None, tenant="acme"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": workflow_id,
                                  "steps": FAN if steps is None else steps})
        self.assertEqual(status, 201, body)

    def start(self, run_id, workflow_id="fan", tenant="acme", extra=None):
        payload = {"tenant": tenant, "workflow_id": workflow_id, "run_id": run_id}
        if extra:
            payload.update(extra)
        status, body = self.call("POST", "/v1/runs", payload)
        self.assertEqual(status, 201, body)
        return body

    def fair(self, worker="w1", lease="omit", tenant="acme"):
        body = {"tenant": tenant, "worker_id": worker}
        if lease != "omit":
            body["lease_seconds"] = lease
        return self.call("POST", "/v1/tasks/claim", body)

    def solo_claim(self, run_id, worker="w1", tenant="acme"):
        return self.call("POST", "/v1/runs/%s/claim" % run_id,
                         {"tenant": tenant, "worker_id": worker, "lease_seconds": 30})

    def history(self, run_id, tenant="acme"):
        path = os.path.join(self.root, tenant, "runs", "%s.json" % run_id)
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)["history"]

    # -- basic responses ------------------------------------------------
    def test_no_runs_returns_204(self):
        self.submit()
        self.assertEqual(self.fair(), (204, None))

    def test_success_payload_matches_single_run_claim_step_shape(self):
        self.submit()
        self.start("r1")
        status, body = self.fair()
        self.assertEqual(status, 200)
        self.assertEqual(set(body), {"run_id", "step"})
        self.assertEqual(body["run_id"], "r1")
        step_keys = {"id", "kind", "status", "attempt", "max_attempts", "depends_on",
                     "worker_id", "lease_deadline", "next_attempt_at", "result",
                     "error", "approval"}
        self.assertEqual(set(body["step"]), step_keys)
        self.assertEqual(body["step"]["id"], "a")
        self.assertEqual(body["step"]["status"], "running")
        self.assertEqual(body["step"]["worker_id"], "w1")
        # The next single-run claim returns the same step field set.
        status2, solo = self.solo_claim("r1", worker="w2")
        self.assertEqual(status2, 200)
        self.assertEqual(set(solo["step"]), step_keys)
        self.assertEqual(solo["step"]["id"], "b")

    def test_leases_only_one_task_per_call(self):
        self.submit()
        self.start("r1")
        self.assertEqual(self.fair()[0], 200)
        status, run = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(len([s for s in run["steps"] if s["status"] == "running"]), 1)

    # -- fairness --------------------------------------------------------
    def test_fewest_claim_events_then_created_at_then_run_id(self):
        self.submit()
        self.start("r-busy")
        self.start("r-idle", extra={"max_parallelism": 3})
        # Give r-busy two historical claims through the legacy endpoint.
        self.assertEqual(self.solo_claim("r-busy")[1]["step"]["id"], "a")
        self.assertEqual(self.solo_claim("r-busy", worker="w2")[1]["step"]["id"], "b")
        status, body = self.fair(worker="w3")
        self.assertEqual(status, 200)
        self.assertEqual((body["run_id"], body["step"]["id"]), ("r-idle", "a"))

    def test_round_robin_between_equal_runs(self):
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("r1", workflow_id="solo")
        self.start("r2", workflow_id="solo")
        winners = []
        for i in range(3):
            status, body = self.fair(worker="w%d" % i)
            if status == 204:
                break
            winners.append(body["run_id"])
        self.assertEqual(sorted(winners), ["r1", "r2"])
        self.assertEqual(self.fair(worker="w9"), (204, None))

    def test_fairness_is_rebuilt_from_history_after_restart(self):
        self.submit()
        self.start("r1")
        self.solo_claim("r1")  # one claim event for r1
        self.start("r2")
        self.server.shutdown()
        self.server.server_close()
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        status, body = self.fair()
        self.assertEqual(status, 200)
        self.assertEqual(body["run_id"], "r2")

    # -- candidate filtering --------------------------------------------
    def test_terminal_and_approval_only_runs_yield_204(self):
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("done", workflow_id="solo")
        self.assertEqual(self.solo_claim("done")[0], 200)
        self.assertEqual(self.call("POST", "/v1/runs/done/complete",
                                   {"tenant": "acme", "step_id": "t",
                                    "worker_id": "w1"})[0], 200)
        self.submit(steps=APPROVAL_ONLY, workflow_id="gated")
        self.start("g1", workflow_id="gated")
        self.assertEqual(self.fair(), (204, None))
        # The approval-only run was activated, never claimed.
        status, run = self.call("GET", "/v1/runs/g1?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(run["steps"][0]["status"], "waiting")
        self.assertEqual([e["type"] for e in self.history("g1")],
                         ["run_created", "waiting"])

    def test_quota_full_run_is_skipped(self):
        self.submit()
        self.start("full", extra={"max_parallelism": 1})
        self.solo_claim("full")
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("open", workflow_id="solo")
        status, body = self.fair(worker="w2")
        self.assertEqual(status, 200)
        self.assertEqual(body["run_id"], "open")
        self.assertEqual(self.fair(worker="w3"), (204, None))

    def test_sleeping_run_activates_when_due(self):
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("soon", workflow_id="solo", extra={"not_before": time.time() + 1})
        self.assertEqual(self.fair(), (204, None))
        time.sleep(1.2)
        status, body = self.fair()
        self.assertEqual(status, 200)
        self.assertEqual((body["run_id"], body["step"]["id"]), ("soon", "t"))

    # -- lease handling --------------------------------------------------
    def test_lease_seconds_defaults_to_thirty_and_is_applied(self):
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("r1", workflow_id="solo")
        status, body = self.fair()
        self.assertEqual(status, 200)
        before = time.time()
        self.assertGreaterEqual(body["step"]["lease_deadline"], before + 29)
        self.assertLessEqual(body["step"]["lease_deadline"], before + 31)

    def test_takeover_recorded_when_fair_call_reclaims_expired_lease(self):
        self.submit()
        self.start("r1")
        self.assertEqual(self.solo_claim("r1")[0], 200)
        self.assertEqual(self.call("POST", "/v1/runs/r1/complete",
                                   {"tenant": "acme", "step_id": "a",
                                    "worker_id": "w1"})[0], 200)
        status, body = self.fair(worker="w1", lease=1)
        self.assertEqual(status, 200)
        self.assertEqual(body["step"]["id"], "b")
        time.sleep(1.2)
        status, body = self.fair(worker="w2", lease=30)
        self.assertEqual(status, 200)
        self.assertEqual((body["run_id"], body["step"]["id"]), ("r1", "b"))
        self.assertEqual(body["step"]["worker_id"], "w2")
        events = self.history("r1")
        takeovers = [e for e in events if e["type"] == "takeover"]
        self.assertEqual(len(takeovers), 1)
        self.assertEqual(takeovers[0]["step_id"], "b")
        self.assertEqual(takeovers[0]["worker_id"], "w1")

    # -- validation ------------------------------------------------------
    def test_bad_json_payloads(self):
        for raw in (b"{", b"[1,2]", b'"oops"', b"42", b"true"):
            status, body = self.call("POST", "/v1/tasks/claim", raw_body=raw)
            self.assertEqual(status, 400, raw)
            self.assertIn("error", body)

    def test_empty_body_is_bad_tenant_not_bad_json(self):
        status, body = self.call("POST", "/v1/tasks/claim", raw_body=b"")
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_bad_tenant(self):
        for tenant in (None, "", "   ", 7, True, ["acme"]):
            status, body = self.call("POST", "/v1/tasks/claim",
                                     {"tenant": tenant, "worker_id": "w1"})
            self.assertEqual(status, 400, tenant)
            self.assertIn("error", body)

    def test_tenant_and_worker_are_trimmed(self):
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("r1", workflow_id="solo")
        status, body = self.call("POST", "/v1/tasks/claim",
                                 {"tenant": "  acme  ", "worker_id": " w1 "})
        self.assertEqual(status, 200, body)
        self.assertEqual(body["run_id"], "r1")
        self.assertEqual(body["step"]["worker_id"], "w1")

    def test_bad_worker(self):
        for worker in (None, "", "  \t", 9, False, {}):
            status, body = self.call("POST", "/v1/tasks/claim",
                                     {"tenant": "acme", "worker_id": worker})
            self.assertEqual(status, 400, worker)
            self.assertIn("error", body)

    def test_bad_lease(self):
        for lease in (0, -1, True, "30", None, float("nan"), [30], {}):
            status, body = self.call("POST", "/v1/tasks/claim",
                                     {"tenant": "acme", "worker_id": "w1",
                                      "lease_seconds": lease})
            self.assertEqual(status, 400, repr(lease))
            self.assertIn("error", body)

    def test_overflow_number_is_bad_lease(self):
        raw = b'{"tenant":"acme","worker_id":"w1","lease_seconds":1e400}'
        status, body = self.call("POST", "/v1/tasks/claim", raw_body=raw)
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    # -- worker registry -------------------------------------------------
    def test_unregistered_and_expired_worker_conflict(self):
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("r1", workflow_id="solo")
        self.assertEqual(self.call("POST", "/v1/workers/register",
                                   {"tenant": "acme", "worker_id": "known",
                                    "lease_seconds": 1})[0], 201)
        status, body = self.fair(worker="stranger")
        self.assertEqual(status, 409)
        self.assertIn("worker_not_registered", body["error"])
        time.sleep(1.2)
        status, body = self.fair(worker="known")
        self.assertEqual(status, 409)
        self.assertIn("worker_expired", body["error"])
        status, run = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(run["steps"][0]["status"], "ready")

    def test_registry_gate_checked_before_activation(self):
        self.submit(steps=SOLO, workflow_id="solo")
        self.start("r1", workflow_id="solo")
        self.assertEqual(self.call("POST", "/v1/workers/register",
                                   {"tenant": "acme", "worker_id": "w1"})[0], 201)
        status, body = self.fair(worker="ghost")
        self.assertEqual(status, 409)
        self.assertIn("worker_not_registered", body["error"])
        # The rejected call must not have claimed anything: the run still has
        # just its creation-time events (run_created + ready), no claim.
        status, run = self.call("GET", "/v1/runs/r1?tenant=acme")
        self.assertEqual(run["history_length"], 2)
        self.assertEqual([e["type"] for e in self.history("r1")],
                         ["run_created", "ready"])
        self.assertEqual(run["steps"][0]["status"], "ready")

    # -- isolation --------------------------------------------------------
    def test_other_tenants_runs_are_not_seen(self):
        self.submit(steps=SOLO, workflow_id="solo", tenant="globex")
        self.start("g1", workflow_id="solo", tenant="globex")
        self.assertEqual(self.fair(tenant="acme"), (204, None))
        status, body = self.fair(tenant="globex")
        self.assertEqual(status, 200)
        self.assertEqual(body["run_id"], "g1")

    def test_single_run_claim_still_works_alongside_fair_claim(self):
        self.submit()
        self.start("r1")
        self.assertEqual(self.fair(worker="w1")[1]["step"]["id"], "a")
        status, body = self.solo_claim("r1", worker="w2")
        self.assertEqual(status, 200)
        self.assertEqual(body["step"]["id"], "b")


if __name__ == "__main__":
    unittest.main()
