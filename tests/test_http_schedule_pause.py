"""End-to-end HTTP tests for pausing and resuming periodic schedules."""

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


class HttpSchedulePauseTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="flowd-http-sched-pause-")
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

    def submit(self, tenant="acme", workflow_id="etl"):
        status, body = self.call("POST", "/v1/workflows",
                                 {"tenant": tenant, "workflow_id": workflow_id,
                                  "steps": LINEAR})
        self.assertEqual(status, 201, body)

    def create(self, tenant="acme", schedule_id="s1", **extra):
        body = {"tenant": tenant, "schedule_id": schedule_id,
                "workflow_id": "etl", "interval_seconds": 60}
        body.update(extra)
        status, payload = self.call("POST", "/v1/schedules", body)
        self.assertEqual(status, 201, payload)
        return payload

    def pause(self, schedule_id="s1", tenant="acme", actor="ops"):
        return self.call("POST", "/v1/schedules/%s/pause" % schedule_id,
                         {"tenant": tenant, "actor": actor})

    def resume(self, schedule_id="s1", tenant="acme", actor="ops"):
        return self.call("POST", "/v1/schedules/%s/resume" % schedule_id,
                         {"tenant": tenant, "actor": actor})

    def dispatch(self, schedule_id="s1", tenant="acme"):
        return self.call("POST", "/v1/schedules/%s/dispatch" % schedule_id,
                         {"tenant": tenant})

    def schedule(self, tenant="acme"):
        status, listing = self.call("GET", "/v1/schedules?tenant=%s" % tenant)
        self.assertEqual(status, 200)
        return listing["items"][0]

    def audit(self, tenant="acme"):
        status, listing = self.call("GET", "/v1/audit?tenant=%s" % tenant)
        self.assertEqual(status, 200)
        return listing["items"]

    # -- status reporting ------------------------------------------------
    def test_create_and_list_report_active_status(self):
        self.submit()
        record = self.create()
        self.assertEqual(record["status"], "active")
        self.assertEqual(self.schedule()["status"], "active")

    # -- pause -----------------------------------------------------------
    def test_pause_returns_paused_record_and_audits(self):
        self.submit()
        self.create()
        status, body = self.pause()
        self.assertEqual(status, 200, body)
        self.assertEqual(body["status"], "paused")
        self.assertEqual(self.schedule()["status"], "paused")
        audit = self.audit()
        self.assertEqual(audit[-1]["action"], "schedule.pause")
        self.assertEqual(audit[-1]["actor"], "ops")
        self.assertEqual(audit[-1]["schedule_id"], "s1")

    def test_repeat_pause_is_idempotent(self):
        self.submit()
        self.create()
        first = self.pause()[1]
        status, second = self.pause(actor="someone-else")
        self.assertEqual(status, 200)
        self.assertEqual(second["status"], "paused")
        self.assertEqual(second["updated_at"], first["updated_at"])
        actions = [r["action"] for r in self.audit()]
        self.assertEqual(actions.count("schedule.pause"), 1)

    def test_paused_dispatch_is_409_and_changes_nothing(self):
        self.submit()
        record = self.create(first_at=time.time() - 1)
        self.assertEqual(self.pause()[0], 200)
        audit_before = self.audit()
        status, body = self.dispatch()
        self.assertEqual(status, 409, body)
        self.assertIn("error", body)
        after = self.schedule()
        self.assertEqual(after["next_at"], record["next_at"])
        self.assertEqual(self.call("GET", "/v1/runs?tenant=acme")[1]["items"], [])
        self.assertEqual(self.audit(), audit_before)

    # -- resume ----------------------------------------------------------
    def test_resume_returns_active_record_and_audits(self):
        self.submit()
        self.create()
        self.pause()
        status, body = self.resume()
        self.assertEqual(status, 200, body)
        self.assertEqual(body["status"], "active")
        self.assertEqual(self.schedule()["status"], "active")
        self.assertEqual(self.audit()[-1]["action"], "schedule.resume")

    def test_repeat_resume_is_idempotent(self):
        self.submit()
        self.create()
        self.pause()
        first = self.resume()[1]
        status, second = self.resume(actor="someone-else")
        self.assertEqual(status, 200)
        self.assertEqual(second["status"], "active")
        self.assertEqual(second["updated_at"], first["updated_at"])
        actions = [r["action"] for r in self.audit()]
        self.assertEqual(actions.count("schedule.resume"), 1)

    def test_resume_keeps_next_at_and_dispatch_fires_again(self):
        self.submit()
        when = time.time() - 1
        self.create(first_at=when)
        self.pause()
        self.assertEqual(self.dispatch()[0], 409)
        status, record = self.resume()
        self.assertEqual(status, 200)
        self.assertEqual(record["next_at"], when)
        # the resume itself created no run; dispatch fires the due point
        status, run = self.dispatch()
        self.assertEqual(status, 201, run)
        self.assertEqual(run["scheduled_at"], when)
        self.assertEqual(run["schedule_id"], "s1")

    # -- validation and tenancy ------------------------------------------
    def test_pause_resume_validation_errors(self):
        self.submit()
        self.create()
        for path in ("pause", "resume"):
            # missing / blank tenant
            status, _ = self.call("POST", "/v1/schedules/s1/%s" % path,
                                  {"actor": "ops"})
            self.assertEqual(status, 400, path)
            status, _ = self.call("POST", "/v1/schedules/s1/%s" % path,
                                  {"tenant": "  ", "actor": "ops"})
            self.assertEqual(status, 400, path)
            # missing / blank actor
            status, _ = self.call("POST", "/v1/schedules/s1/%s" % path,
                                  {"tenant": "acme"})
            self.assertEqual(status, 400, path)
            status, _ = self.call("POST", "/v1/schedules/s1/%s" % path,
                                  {"tenant": "acme", "actor": " "})
            self.assertEqual(status, 400, path)
            # non-object and invalid JSON bodies
            status, _ = self.call("POST", "/v1/schedules/s1/%s" % path,
                                  raw_body=b"[1, 2]")
            self.assertEqual(status, 400, path)
            status, _ = self.call("POST", "/v1/schedules/s1/%s" % path,
                                  raw_body=b"{not json")
            self.assertEqual(status, 400, path)
            # blank schedule id in the path
            status, _ = self.call("POST", "/v1/schedules/%20/" + path,
                                  {"tenant": "acme", "actor": "ops"})
            self.assertEqual(status, 400, path)
        # nothing changed: still active, no pause/resume audit records
        self.assertEqual(self.schedule()["status"], "active")
        actions = [r["action"] for r in self.audit()]
        self.assertNotIn("schedule.pause", actions)
        self.assertNotIn("schedule.resume", actions)

    def test_pause_resume_unknown_and_cross_tenant(self):
        self.submit()
        self.submit(tenant="globex")
        self.create()
        for path in ("pause", "resume"):
            status, body = self.call("POST", "/v1/schedules/ghost/%s" % path,
                                     {"tenant": "acme", "actor": "ops"})
            self.assertEqual(status, 404, (path, body))
            status, body = self.call("POST", "/v1/schedules/s1/%s" % path,
                                     {"tenant": "globex", "actor": "ops"})
            self.assertEqual(status, 403, (path, body))
        self.assertEqual(self.schedule()["status"], "active")

    def test_pause_survives_restart(self):
        self.submit()
        self.create(first_at=time.time() - 1)
        self.assertEqual(self.pause()[0], 200)
        self.server.shutdown()
        self.server.server_close()
        self.server = create_server(WorkflowStore(self.root), "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.assertEqual(self.schedule()["status"], "paused")
        self.assertEqual(self.dispatch()[0], 409)
        self.assertEqual(self.resume()[0], 200)
        self.assertEqual(self.dispatch()[0], 201)


if __name__ == "__main__":
    unittest.main()
