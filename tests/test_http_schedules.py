"""End-to-end HTTP tests for periodic schedules and dispatch."""

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
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
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

    def submit(self, tenant="acme", workflow_id="etl"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": workflow_id,
                                  "steps": LINEAR})
        self.assertEqual(status, 201, body)

    def create(self, tenant="acme", schedule_id="s1", workflow_id="etl",
               interval_seconds=60, **extra):
        body = {"tenant": tenant, "schedule_id": schedule_id,
                "workflow_id": workflow_id, "interval_seconds": interval_seconds}
        body.update(extra)
        return self.call("POST", "/v1/schedules", body)

    def dispatch(self, schedule_id="s1", tenant="acme"):
        return self.call("POST", "/v1/schedules/%s/dispatch" % schedule_id,
                         {"tenant": tenant})

    # -- create --------------------------------------------------------
    def test_create_returns_record_with_next_at(self):
        self.submit()
        before = time.time()
        status, body = self.create(interval_seconds=12.5, params={"k": 1},
                                   max_parallelism=2)
        after = time.time()
        self.assertEqual(status, 201, body)
        self.assertEqual(body["schedule_id"], "s1")
        self.assertEqual(body["workflow_id"], "etl")
        self.assertEqual(body["interval_seconds"], 12.5)
        self.assertEqual(body["params"], {"k": 1})
        self.assertEqual(body["max_parallelism"], 2)
        self.assertGreaterEqual(body["next_at"], before - 0.001)
        self.assertLessEqual(body["next_at"], after + 0.001)
        self.assertEqual(body["first_at"], body["next_at"])

    def test_create_with_explicit_first_at(self):
        self.submit()
        when = time.time() + 600
        status, body = self.create(first_at=when, interval_seconds=30)
        self.assertEqual(status, 201)
        self.assertEqual(body["first_at"], when)
        self.assertEqual(body["next_at"], when)

    def test_schedule_id_is_trimmed(self):
        self.submit()
        status, body = self.create(schedule_id="  s1 \t")
        self.assertEqual(status, 201)
        self.assertEqual(body["schedule_id"], "s1")
        status, listing = self.call("GET", "/v1/schedules?tenant=acme")
        self.assertEqual([r["schedule_id"] for r in listing["items"]], ["s1"])

    def test_create_validation_errors(self):
        self.submit()
        cases = [
            ({"schedule_id": ""}, "bad_schedule_id"),
            ({"schedule_id": "   "}, "bad_schedule_id"),
            ({"schedule_id": 3}, "bad_schedule_id"),
            ({"interval_seconds": 0}, "bad_interval"),
            ({"interval_seconds": -2}, "bad_interval"),
            ({"interval_seconds": "60"}, "bad_interval"),
            ({"interval_seconds": float("inf")}, "bad_interval"),
            ({"first_at": -1}, "bad_first_at"),
            ({"first_at": "soon"}, "bad_first_at"),
            ({"first_at": float("nan")}, "bad_first_at"),
            ({"schedule_id": "p", "params": [1]}, "bad_params"),
            ({"schedule_id": "p", "params": "no"}, "bad_params"),
            ({"schedule_id": "q", "max_parallelism": 0}, "bad_max_parallelism"),
            ({"schedule_id": "q", "max_parallelism": 1.5}, "bad_max_parallelism"),
        ]
        for overrides, code in cases:
            kwargs = {"interval_seconds": 60}
            kwargs.update(overrides)
            status, body = self.create(**kwargs)
            self.assertEqual(status, 400, repr(overrides))
            self.assertIn("error", body)
        self.assertEqual(self.call("GET", "/v1/schedules?tenant=acme")[1]["items"], [])

    def test_missing_tenant_is_bad_tenant(self):
        self.submit()
        request_body = {"schedule_id": "s1", "workflow_id": "etl",
                        "interval_seconds": 60}
        status, body = self.call("POST", "/v1/schedules", request_body)
        self.assertEqual(status, 400)
        status, body = self.call("GET", "/v1/schedules")
        self.assertEqual(status, 400)
        status, body = self.call("POST", "/v1/schedules/s1/dispatch", {})
        self.assertEqual(status, 400)

    def test_unknown_workflow_duplicate_and_tenancy(self):
        status, body = self.create(workflow_id="ghost")
        self.assertEqual(status, 404)
        self.assertIn("error", body)
        self.submit()
        self.submit(tenant="globex")
        self.assertEqual(self.create()[0], 201)
        status, body = self.create()
        self.assertEqual(status, 409)
        self.assertIn("error", body)
        # same id in another tenant is fine
        self.assertEqual(self.create(tenant="globex", interval_seconds=7)[0], 201)
        # listing is scoped and sorted
        self.submit(tenant="acme", workflow_id="w2")
        self.assertEqual(self.create(schedule_id="a", workflow_id="w2")[0], 201)
        status, listing = self.call("GET", "/v1/schedules?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual([r["schedule_id"] for r in listing["items"]], ["a", "s1"])
        self.assertEqual(self.call("GET", "/v1/schedules?tenant=globex")[1]
                         ["items"][0]["interval_seconds"], 7)

    # -- dispatch ------------------------------------------------------
    def test_early_dispatch_is_204_and_creates_nothing(self):
        self.submit()
        self.assertEqual(self.create(first_at=time.time() + 3600)[0], 201)
        before = self.call("GET", "/v1/schedules?tenant=acme")[1]["items"][0]
        for _ in range(2):
            self.assertEqual(self.dispatch()[0], 204)
        after = self.call("GET", "/v1/schedules?tenant=acme")[1]["items"][0]
        self.assertEqual(after["next_at"], before["next_at"])
        self.assertEqual(after["updated_at"], before["updated_at"])
        self.assertEqual(self.call("GET", "/v1/runs?tenant=acme")[1]["items"], [])

    def test_due_dispatch_creates_tagged_run_and_advances(self):
        self.submit()
        when = time.time() - 1  # already due
        status, schedule = self.create(first_at=when, interval_seconds=3600,
                                       params={"x": 1}, max_parallelism=2)
        self.assertEqual(status, 201)
        status, run = self.dispatch()
        self.assertEqual(status, 201, run)
        self.assertEqual(run["schedule_id"], "s1")
        self.assertEqual(run["scheduled_at"], when)
        self.assertEqual(run["params"], {"x": 1})
        self.assertEqual(run["max_parallelism"], 2)
        # the created run behaves like any other run
        self.assertEqual(next(s for s in run["steps"] if s["id"] == "step1")
                         ["status"], "ready")
        # next_at advanced one interval; a repeat before it is due is a 204
        record = self.call("GET", "/v1/schedules?tenant=acme")[1]["items"][0]
        self.assertAlmostEqual(record["next_at"], when + 3600)
        self.assertEqual(self.dispatch()[0], 204)
        self.assertEqual(len(self.call("GET", "/v1/runs?tenant=acme")[1]["items"]), 1)
        # GET run reports the schedule binding too
        status, detail = self.call("GET", "/v1/runs/%s?tenant=acme" % run["run_id"])
        self.assertEqual(status, 200)
        self.assertEqual(detail["schedule_id"], "s1")
        self.assertEqual(detail["scheduled_at"], when)

    def test_missed_fires_are_caught_up_one_per_call_in_order(self):
        self.submit()
        first = time.time() - 250
        self.assertEqual(self.create(first_at=first, interval_seconds=100)[0], 201)
        fired = []
        for _ in range(3):
            status, run = self.dispatch()
            self.assertEqual(status, 201)
            fired.append(run["scheduled_at"])
        self.assertEqual(fired, [first, first + 100, first + 200])
        # now caught up (next_at is in the future)
        self.assertEqual(self.dispatch()[0], 204)
        self.assertEqual(len(self.call("GET", "/v1/runs?tenant=acme")[1]["items"]), 3)

    def test_dispatch_unknown_schedule_is_404_cross_tenant_is_403(self):
        status, body = self.dispatch("ghost")
        self.assertEqual(status, 404)
        self.assertIn("error", body)
        self.submit()
        self.create(first_at=time.time())
        status, body = self.dispatch(tenant="globex")
        self.assertEqual(status, 403)
        self.assertIn("error", body)
        # foreign dispatch must not have fired the schedule
        self.assertEqual(len(self.call("GET", "/v1/runs?tenant=acme")[1]["items"]), 0)
        self.assertEqual(self.dispatch()[0], 201)

    def test_manual_run_creation_is_still_one_shot(self):
        self.submit()
        status, run = self.call("POST", "/v1/runs",
                                {"tenant": "acme", "workflow_id": "etl"})
        self.assertEqual(status, 201)
        self.assertIsNone(run["schedule_id"])
        self.assertIsNone(run["scheduled_at"])

    def test_schedules_survive_restart_same_data_dir(self):
        self.submit()
        when = time.time() + 3600
        self.assertEqual(self.create(first_at=when, interval_seconds=90)[0], 201)
        self.server.shutdown()
        self.server.server_close()
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        status, listing = self.call("GET", "/v1/schedules?tenant=acme")
        self.assertEqual(status, 200)
        self.assertEqual(len(listing["items"]), 1)
        self.assertEqual(listing["items"][0]["next_at"], when)
        self.assertEqual(self.dispatch()[0], 204)

    def test_bad_json_body(self):
        status, body = self.call("POST", "/v1/schedules", raw_body=b"{not json")
        self.assertEqual(status, 400)
        self.assertIn("error", body)


if __name__ == "__main__":
    unittest.main()
