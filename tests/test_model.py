"""Tests for DAG validation (flowd.model)."""

import unittest

from flowd.model import DEFAULT_MAX_ATTEMPTS, WorkflowError, plan_workflow, topo_order


def steps(*specs):
    return [{"id": sid, "depends_on": list(deps)} for sid, deps in specs]


class ValidationTest(unittest.TestCase):
    def test_plan_is_topologically_sorted(self):
        plan = plan_workflow("etl", steps(("c", ["b"]), ("a", []), ("b", ["a"])))
        self.assertEqual(plan["order"], ["a", "b", "c"])
        self.assertEqual([s["id"] for s in plan["steps"]], ["a", "b", "c"])
        self.assertEqual(plan["workflow_id"], "etl")

    def test_topo_order_is_deterministic_for_independent_steps(self):
        self.assertEqual(topo_order(steps(("z", []), ("a", []), ("m", []))), ["a", "m", "z"])

    def test_default_max_attempts(self):
        self.assertEqual(plan_workflow("w", steps(("a", [])))["steps"][0]["max_attempts"],
                         DEFAULT_MAX_ATTEMPTS)

    def test_duplicate_id_rejected(self):
        with self.assertRaises(WorkflowError) as ctx:
            plan_workflow("w", steps(("a", []), ("a", [])))
        self.assertEqual(ctx.exception.code, "duplicate_step")

    def test_unknown_dependency_rejected(self):
        with self.assertRaises(WorkflowError) as ctx:
            plan_workflow("w", steps(("a", ["ghost"])))
        self.assertEqual(ctx.exception.code, "unknown_dependency")
        self.assertIn("ghost", ctx.exception.message)

    def test_self_dependency_rejected(self):
        with self.assertRaises(WorkflowError) as ctx:
            plan_workflow("w", steps(("a", ["a"])))
        self.assertEqual(ctx.exception.code, "self_dependency")

    def test_cycle_rejected(self):
        with self.assertRaises(WorkflowError) as ctx:
            plan_workflow("w", steps(("a", ["c"]), ("b", ["a"]), ("c", ["b"])))
        self.assertEqual(ctx.exception.code, "cycle")
        self.assertEqual(sorted(ctx.exception.message.split(": ")[1].split(", ")), ["a", "b", "c"])

    def test_longer_cycle_is_rejected(self):
        with self.assertRaises(WorkflowError) as ctx:
            plan_workflow("w", steps(("a", []), ("b", ["a", "c"]), ("c", ["b"])))
        self.assertEqual(ctx.exception.code, "cycle")

    def test_empty_id_rejected(self):
        with self.assertRaises(WorkflowError) as ctx:
            plan_workflow("w", [{"id": "   ", "depends_on": []}])
        self.assertIn("empty id", ctx.exception.message)
        with self.assertRaises(WorkflowError):
            plan_workflow("w", [{"depends_on": []}])

    def test_bad_max_attempts_rejected(self):
        for bad in (0, -3, "three", 2.5, True):
            with self.assertRaises(WorkflowError) as ctx:
                plan_workflow("w", [{"id": "a", "max_attempts": bad}])
            self.assertEqual(ctx.exception.code, "bad_max_attempts", "value %r" % (bad,))

    def test_duplicate_dependency_rejected(self):
        with self.assertRaises(WorkflowError):
            plan_workflow("w", steps(("a", []), ("b", ["a", "a"])))

    def test_bad_workflow_id_and_empty_steps_rejected(self):
        for bad_id in ("", "   ", None, 7):
            with self.assertRaises(WorkflowError):
                plan_workflow(bad_id, steps(("a", [])))
        with self.assertRaises(WorkflowError):
            plan_workflow("w", [])


if __name__ == "__main__":
    unittest.main()
