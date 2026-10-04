"""Command line interface: ``python3 -m flowd [--data-dir DIR] <command>``.

Every command prints a single line of JSON to stdout.  Failures print a
single line of JSON to stderr and exit with a non-zero status.
"""

import argparse
import json
import sys

from .http_app import _run_view, create_server
from .model import WorkflowError
from .scheduler import Scheduler
from .store import WorkflowStore

DEFAULT_DATA_DIR = "./flowd_data"


def emit(payload):
    """Print one line of JSON to stdout."""
    sys.stdout.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")
    sys.stdout.flush()


def fail(message):
    """Print one line of JSON to stderr and return a non-zero exit code."""
    sys.stderr.write(json.dumps({"error": message}, ensure_ascii=True, sort_keys=True) + "\n")
    sys.stderr.flush()
    return 1


def _step_args(parser, claim=False):
    """Shared arguments for the claim/complete/fail subcommands."""
    parser.add_argument("--run-id", required=True, dest="run_id")
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--worker", required=True)
    if claim:
        parser.add_argument("--lease-seconds", type=float, default=30.0, dest="lease_seconds")
    else:
        parser.add_argument("--step", required=True)
    return parser


def build_parser():
    parser = argparse.ArgumentParser(prog="flowd",
                                     description="distributed workflow orchestration backend")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR,
                        help="storage root (default: %(default)s)")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the HTTP API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)

    submit = sub.add_parser("submit", help="validate and store a workflow DAG")
    submit.add_argument("--tenant", required=True)
    submit.add_argument("--workflow", required=True)
    submit.add_argument("--file", required=True, help="JSON file holding a list of steps")

    status = sub.add_parser("status", help="show one run")
    status.add_argument("--run-id", required=True, dest="run_id")
    status.add_argument("--tenant", required=True)

    _step_args(sub.add_parser("claim", help="claim the next ready step"), claim=True)
    complete = _step_args(sub.add_parser("complete", help="complete a claimed step"))
    complete.add_argument("--result", help="optional JSON result")
    failure = _step_args(sub.add_parser("fail", help="fail a step (retry or exhaust attempts)"))
    failure.add_argument("--error", default="step failed")

    listing = sub.add_parser("list", help="list runs of a tenant")
    listing.add_argument("--tenant", required=True)
    listing.add_argument("--status")
    listing.add_argument("--limit", type=int, default=50)
    return parser


def _load_steps(path):
    """Read a steps file: a JSON list, or an object with a ``steps`` list."""
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except OSError as exc:
        raise WorkflowError("cannot read steps file: %s" % exc, "bad_file")
    except ValueError as exc:
        raise WorkflowError("steps file is not valid JSON: %s" % exc, "bad_json")
    if isinstance(payload, dict):
        payload = payload.get("steps")
    if not isinstance(payload, list):
        raise WorkflowError("steps file must contain a JSON list of steps", "bad_file")
    return payload


def _serve(store, scheduler, args):
    server = create_server(store, args.host, args.port, scheduler)
    emit({"ok": True, "host": args.host, "port": int(args.port), "data_dir": store.root})
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        store = WorkflowStore(args.data_dir)
        scheduler = Scheduler(store)
        if args.command == "serve":
            return _serve(store, scheduler, args)
        if args.command == "submit":
            plan = scheduler.submit(args.tenant, args.workflow, _load_steps(args.file))
            emit({"tenant": args.tenant, "workflow_id": plan["workflow_id"], "order": plan["order"]})
            return 0
        if args.command == "status":
            emit(_run_view(scheduler.get_run(args.tenant, args.run_id)))
            return 0
        if args.command == "claim":
            step = scheduler.claim(args.tenant, args.run_id, args.worker, args.lease_seconds)
            emit({"run_id": args.run_id, "step": step and {"id": step["id"],
                                                           "status": step["status"],
                                                           "attempt": step["attempt"]}})
            return 0
        if args.command == "complete":
            run = scheduler.complete(args.tenant, args.run_id, args.step, args.worker,
                                     json.loads(args.result) if args.result else None)
            emit({"run_id": run["run_id"], "status": run["status"]})
            return 0
        if args.command == "fail":
            run = scheduler.fail(args.tenant, args.run_id, args.step, args.worker, args.error)
            step = run["steps"][args.step]
            emit({"run_id": run["run_id"], "status": run["status"],
                  "step": {"id": args.step, "status": step["status"], "attempt": step["attempt"],
                           "next_attempt_at": step["next_attempt_at"]}})
            return 0
        if args.command == "list":
            items, next_after = scheduler.list_runs(args.tenant, args.status, args.limit)
            emit({"tenant": args.tenant, "next_after": next_after,
                  "items": [{"run_id": r["run_id"], "status": r["status"]} for r in items]})
            return 0
        return fail("unknown command: %s" % args.command)
    except WorkflowError as exc:
        return fail(exc.message)
    except ValueError as exc:
        return fail("invalid JSON argument: %s" % exc)
    except Exception as exc:  # pragma: no cover - defensive
        return fail("internal error: %s" % exc)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
