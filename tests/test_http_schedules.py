"""End-to-end HTTP tests for persistent interval schedules."""

import json
import shutil
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from flowd.http_app import create_server
from flowd.store import WorkflowStore

LINEAR = [
    {"id": "step1", "depends_on": []},
    {"id": "step2", "depends_on": ["step1"]},
]


class HttpSchedulesTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-sched-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self._start_server()

    def _start_server(self):
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def _stop_server(self):
        self.server.shutdown()
        self.server.server_close()

    def tearDown(self):
        self._stop_server()

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

    def submit(self, steps=LINEAR, workflow_id="wf", tenant="acme"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": workflow_id, "steps": steps})
        self.assertEqual(status, 201, body)

    def create(self, schedule_id="s1", tenant="acme", **extra):
        body = {"tenant": tenant, "schedule_id": schedule_id, "workflow_id": "wf",
                "interval_seconds": 60}
        body.update(extra)
        return self.call("POST", "/v1/schedules", body)

    def dispatch(self, schedule_id="s1", tenant="acme"):
        return self.call("POST", "/v1/schedules/%s/dispatch" % schedule_id,
                         {"tenant": tenant})

    def schedules(self, tenant="acme"):
        return self.call("GET", "/v1/schedules?tenant=%s" % tenant)

    def run_ids(self, tenant="acme"):
        status, body = self.call("GET", "/v1/runs?tenant=%s" % tenant)
        self.assertEqual(status, 200)
        return [item["run_id"] for item in body["items"]]

    # -- creation validation --------------------------------------------
    def test_create_returns_record_with_next_at(self):
        self.submit()
        before = time.time()
        status, body = self.create()
        after = time.time()
        self.assertEqual(status, 201, body)
        self.assertEqual(body["tenant"], "acme")
        self.assertEqual(body["schedule_id"], "s1")
        self.assertEqual(body["workflow_id"], "wf")
        self.assertEqual(body["interval_seconds"], 60)
        self.assertEqual(body["params"], {})
        self.assertIsNone(body["max_parallelism"])
        self.assertEqual(body["first_at"], body["next_at"])
        self.assertTrue(before <= body["next_at"] <= after, body)

    def test_first_at_and_params_and_quota_are_stored(self):
        self.submit()
        first = time.time() + 3600
        status, body = self.create(first_at=first, params={"a": 1}, max_parallelism=2)
        self.assertEqual(status, 201, body)
        self.assertEqual(body["first_at"], first)
        self.assertEqual(body["next_at"], first)
        self.assertEqual(body["params"], {"a": 1})
        self.assertEqual(body["max_parallelism"], 2)

    def test_bad_tenant(self):
        self.submit()
        for body in ({}, {"tenant": ""}, {"tenant": "  "}):
            status, _ = self.call("POST", "/v1/schedules",
                                  dict(body, schedule_id="s1", workflow_id="wf",
                                       interval_seconds=60))
            self.assertEqual(status, 400, body)

    def test_bad_schedule_id(self):
        self.submit()
        for bad in (None, "", "   ", 12, True, [], {}):
            status, _ = self.create(schedule_id=bad)
            self.assertEqual(status, 400, repr(bad))
        self.assertEqual(self.schedules()[1]["items"], [])

    def test_bad_interval(self):
        self.submit()
        for bad in (None, 0, -1, -0.5, True, "60", float("inf"), float("nan"), [], {},
                    10 ** 400):
            status, _ = self.create(interval_seconds=bad)
            self.assertEqual(status, 400, repr(bad))
        self.assertEqual(self.schedules()[1]["items"], [])

    def test_bad_first_at(self):
        self.submit()
        for bad in (True, "1700000060", -1, -0.01, float("inf"), float("nan"), [], {},
                    10 ** 400):
            status, _ = self.create(first_at=bad)
            self.assertEqual(status, 400, repr(bad))
        self.assertEqual(self.schedules()[1]["items"], [])

    def test_bad_params(self):
        self.submit()
        for bad in ([], "x", 1, True):
            status, _ = self.create(params=bad)
            self.assertEqual(status, 400, repr(bad))
        self.assertEqual(self.schedules()[1]["items"], [])

    def test_bad_max_parallelism(self):
        self.submit()
        for bad in (0, -1, 1.5, True, "2"):
            status, _ = self.create(max_parallelism=bad)
            self.assertEqual(status, 400, repr(bad))
        self.assertEqual(self.schedules()[1]["items"], [])

    def test_unknown_workflow(self):
        status, body = self.create()
        self.assertEqual(status, 404)
        self.assertIn("error", body)

    def test_duplicate_schedule_id_conflicts_per_tenant(self):
        self.submit()
        self.submit(tenant="other")
        self.assertEqual(self.create()[0], 201)
        self.assertEqual(self.create()[0], 409)
        # the same id under another tenant is independent
        self.assertEqual(self.create(tenant="other")[0], 201)

    def test_schedule_id_is_trimmed(self):
        self.submit()
        status, body = self.create(schedule_id="  s1  ")
        self.assertEqual(status, 201, body)
        self.assertEqual(body["schedule_id"], "s1")
        self.assertEqual(self.create(schedule_id="s1")[0], 409)

    # -- listing ----------------------------------------------------------
    def test_list_is_sorted_by_schedule_id(self):
        self.submit()
        for sid in ("s3", "s1", "s2"):
            self.assertEqual(self.create(schedule_id=sid)[0], 201)
        status, body = self.schedules()
        self.assertEqual(status, 200)
        self.assertEqual([item["schedule_id"] for item in body["items"]],
                         ["s1", "s2", "s3"])
        # other tenants see their own (empty) list
        self.assertEqual(self.schedules(tenant="other")[1]["items"], [])

    def test_list_requires_tenant(self):
        self.assertEqual(self.call("GET", "/v1/schedules")[0], 400)

    # -- dispatch ---------------------------------------------------------
    def test_dispatch_unknown_and_cross_tenant(self):
        self.submit()
        self.assertEqual(self.dispatch("nope")[0], 404)
        self.assertEqual(self.create()[0], 201)
        status, body = self.dispatch("s1", tenant="other")
        self.assertEqual(status, 403)
        self.assertIn("error", body)

    def test_dispatch_before_due_is_204_and_changes_nothing(self):
        self.submit()
        first = time.time() + 3600
        self.assertEqual(self.create(first_at=first)[0], 201)
        before = self.schedules()[1]["items"][0]
        for _ in range(2):
            status, body = self.dispatch()
            self.assertEqual(status, 204)
            self.assertIsNone(body)
        after = self.schedules()[1]["items"][0]
        self.assertEqual(before, after)
        self.assertEqual(self.run_ids(), [])

    def test_due_dispatch_creates_run_and_advances_next_at(self):
        self.submit()
        first = time.time() - 10
        self.assertEqual(self.create(first_at=first, params={"k": "v"},
                                     max_parallelism=1)[0], 201)
        status, run = self.dispatch()
        self.assertEqual(status, 201, run)
        self.assertEqual(run["schedule_id"], "s1")
        self.assertEqual(run["scheduled_at"], first)
        self.assertEqual(run["params"], {"k": "v"})
        self.assertEqual(run["max_parallelism"], 1)
        self.assertEqual(run["workflow_id"], "wf")
        # next_at advanced by exactly one interval
        record = self.schedules()[1]["items"][0]
        self.assertEqual(record["next_at"], first + 60)
        # a repeat before the next slot is 204 and creates nothing
        self.assertEqual(self.dispatch()[0], 204)
        self.assertEqual(len(self.run_ids()), 1)

    def test_missed_slots_are_caught_up_in_order(self):
        self.submit()
        first = time.time() - 250
        self.assertEqual(self.create(first_at=first, interval_seconds=100)[0], 201)
        scheduled = []
        for _ in range(3):
            status, run = self.dispatch()
            self.assertEqual(status, 201, run)
            scheduled.append(run["scheduled_at"])
        self.assertEqual(scheduled, [first, first + 100, first + 200])
        # next slot (first + 300) is in the future now
        self.assertEqual(self.dispatch()[0], 204)
        self.assertEqual(len(self.run_ids()), 3)

    def test_dispatched_run_executes_normally(self):
        self.submit()
        self.assertEqual(self.create(first_at=time.time() - 1)[0], 201)
        status, run = self.dispatch()
        self.assertEqual(status, 201, run)
        run_id = run["run_id"]
        status, body = self.call("POST", "/v1/runs/%s/claim" % run_id,
                                 {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(status, 200, body)
        self.assertEqual(body["step"]["id"], "step1")
        status, body = self.call("POST", "/v1/runs/%s/complete" % run_id,
                                 {"tenant": "acme", "worker_id": "w1", "step_id": "step1"})
        self.assertEqual(status, 200, body)
        status, body = self.call("POST", "/v1/runs/%s/claim" % run_id,
                                 {"tenant": "acme", "worker_id": "w1"})
        self.assertEqual(body["step"]["id"], "step2")
        status, body = self.call("POST", "/v1/runs/%s/complete" % run_id,
                                 {"tenant": "acme", "worker_id": "w1", "step_id": "step2"})
        self.assertEqual(body["status"], "succeeded")

    def test_run_views_carry_schedule_fields_and_old_runs_report_null(self):
        self.submit()
        # a plain run has no schedule fields
        status, plain = self.call("POST", "/v1/runs",
                                  {"tenant": "acme", "workflow_id": "wf", "run_id": "plain"})
        self.assertEqual(status, 201)
        self.assertIsNone(plain["schedule_id"])
        self.assertIsNone(plain["scheduled_at"])
        # a dispatched run carries them everywhere
        first = time.time() - 1
        self.assertEqual(self.create(first_at=first)[0], 201)
        status, run = self.dispatch()
        run_id = run["run_id"]
        status, detail = self.call("GET", "/v1/runs/%s?tenant=acme" % run_id)
        self.assertEqual(detail["schedule_id"], "s1")
        self.assertEqual(detail["scheduled_at"], first)
        items = self.call("GET", "/v1/runs?tenant=acme")[1]["items"]
        by_id = {item["run_id"]: item for item in items}
        self.assertEqual(by_id[run_id]["schedule_id"], "s1")
        self.assertIsNone(by_id["plain"]["schedule_id"])

    # -- persistence ------------------------------------------------------
    def test_schedule_and_next_at_survive_restart(self):
        self.submit()
        first = time.time() - 5
        self.assertEqual(self.create(first_at=first, params={"a": 1})[0], 201)
        self.assertEqual(self.dispatch()[0], 201)  # next_at now first + 60
        self._stop_server()
        self._start_server()
        records = self.schedules()[1]["items"]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["next_at"], first + 60)
        self.assertEqual(records[0]["params"], {"a": 1})
        # dispatching after the restart still cannot duplicate the past slot
        self.assertEqual(self.dispatch()[0], 204)
        self.assertEqual(len(self.run_ids()), 1)


if __name__ == "__main__":
    unittest.main()
