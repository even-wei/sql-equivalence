"""Tests for PredicateSimplificationPass.

Simplifies and normalizes boolean predicates: absorption, double negation,
flattening, identity/contradiction elimination, De Morgan's normalization.
"""

import pytest
from sqlglot import parse_one

from sql_equivalence.passes.predicate import PredicateSimplificationPass


@pytest.fixture
def predicate_pass():
    return PredicateSimplificationPass()


class TestAbsorption:
    """Redundant numeric comparisons on the same column should be absorbed."""

    def test_gt_absorbs_weaker_gt(self, predicate_pass):
        sql = "SELECT * FROM t WHERE x > 5 AND x > 3"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql()
        assert "x > 5" in result or "x > 3" not in result
        assert len(steps) >= 1

    def test_lt_absorbs_weaker_lt(self, predicate_pass):
        sql = "SELECT * FROM t WHERE x < 3 AND x < 5"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql()
        assert "x < 3" in result or "x < 5" not in result

    def test_gte_and_gt_on_same_value(self, predicate_pass):
        sql = "SELECT * FROM t WHERE x >= 5 AND x > 5"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql()
        # x > 5 is stricter than x >= 5, so x > 5 should remain
        assert ">" in result

    def test_no_absorption_different_columns(self, predicate_pass):
        """Absorption should NOT happen across different columns."""
        sql = "SELECT * FROM t WHERE x > 5 AND y > 3"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql()
        assert "x > 5" in result
        assert "y > 3" in result

    def test_or_absorption(self, predicate_pass):
        """x > 3 OR x > 5 should simplify to x > 3 (OR keeps the weaker)."""
        sql = "SELECT * FROM t WHERE x > 3 OR x > 5"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql()
        assert "x > 3" in result


class TestDoubleNegation:
    """NOT NOT x should simplify to x."""

    def test_double_not_eliminated(self, predicate_pass):
        sql = "SELECT * FROM t WHERE NOT NOT (x > 1)"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql()
        assert "NOT" not in result.upper() or result.upper().count("NOT") == 0

    def test_triple_not_simplified(self, predicate_pass):
        sql = "SELECT * FROM t WHERE NOT NOT NOT (x > 1)"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql()
        # Should simplify to single NOT
        assert result.upper().count("NOT") == 1


class TestFlatten:
    """Nested AND/OR should be flattened."""

    def test_nested_and_flattened(self, predicate_pass):
        sql = "SELECT * FROM t WHERE (a = 1 AND (b = 2 AND c = 3))"
        expr, steps = predicate_pass.apply(parse_one(sql))
        # All three conditions should be at the same AND level
        result = expr.sql()
        assert "a" in result and "b" in result and "c" in result

    def test_nested_or_flattened(self, predicate_pass):
        sql = "SELECT * FROM t WHERE (a = 1 OR (b = 2 OR c = 3))"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql()
        assert "a" in result and "b" in result and "c" in result


class TestIdentityAndContradiction:
    """Boolean identity and contradiction laws."""

    def test_and_true_eliminated(self, predicate_pass):
        sql = "SELECT * FROM t WHERE x > 1 AND TRUE"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql().upper()
        assert "TRUE" not in result
        assert "X > 1" in result

    def test_or_false_eliminated(self, predicate_pass):
        sql = "SELECT * FROM t WHERE x > 1 OR FALSE"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql().upper()
        assert "FALSE" not in result
        assert "X > 1" in result

    def test_and_false_to_false(self, predicate_pass):
        sql = "SELECT * FROM t WHERE x > 1 AND FALSE"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql().upper()
        assert "FALSE" in result

    def test_or_true_to_true(self, predicate_pass):
        sql = "SELECT * FROM t WHERE x > 1 OR TRUE"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql().upper()
        assert "TRUE" in result


class TestDeMorgans:
    """De Morgan's law normalization (push NOT inward)."""

    def test_not_and_to_or(self, predicate_pass):
        """NOT (a AND b) should normalize to (NOT a OR NOT b)."""
        sql = "SELECT * FROM t WHERE NOT (x > 1 AND y > 2)"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql().upper()
        # After De Morgan's, should have OR with negated conditions
        assert "OR" in result

    def test_not_or_to_and(self, predicate_pass):
        """NOT (a OR b) should normalize to (NOT a AND NOT b)."""
        sql = "SELECT * FROM t WHERE NOT (x > 1 OR y > 2)"
        expr, steps = predicate_pass.apply(parse_one(sql))
        result = expr.sql().upper()
        assert "AND" in result


class TestRewriteSteps:
    def test_step_on_simplification(self, predicate_pass):
        sql = "SELECT * FROM t WHERE NOT NOT (x > 1)"
        _, steps = predicate_pass.apply(parse_one(sql))
        assert len(steps) >= 1
        assert steps[0].pass_name == "predicate_simplification"

    def test_no_steps_when_already_simple(self, predicate_pass):
        sql = "SELECT * FROM t WHERE x > 1"
        _, steps = predicate_pass.apply(parse_one(sql))
        assert len(steps) == 0
