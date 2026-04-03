"""Tests for SubqueryJoinPass.

Normalizes IN/EXISTS/semi-join forms to a canonical EXISTS representation.
"""

import pytest
from sqlglot import parse_one

from sql_equivalence.passes.subquery_join import SubqueryJoinPass


@pytest.fixture
def subquery_pass():
    return SubqueryJoinPass()


class TestINtoEXISTS:
    """IN (SELECT ...) should normalize to EXISTS form."""

    def test_simple_in_subquery(self, subquery_pass):
        sql = "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)"
        expr, steps = subquery_pass.apply(parse_one(sql))
        result = expr.sql().upper()
        assert "EXISTS" in result
        assert "IN" not in result.replace("JOIN", "")  # don't match JOIN
        assert len(steps) >= 1

    def test_in_with_column_alias(self, subquery_pass):
        sql = "SELECT * FROM orders WHERE customer_id IN (SELECT customer_id FROM customers WHERE active = 1)"
        expr, steps = subquery_pass.apply(parse_one(sql))
        result = expr.sql().upper()
        assert "EXISTS" in result

    def test_in_and_exists_normalize_same(self, subquery_pass):
        """IN and equivalent EXISTS should produce the same normalized form."""
        sql_in = "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)"
        sql_exists = "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)"
        expr_in, _ = subquery_pass.apply(parse_one(sql_in))
        expr_exists, _ = subquery_pass.apply(parse_one(sql_exists))
        # Both should normalize to the same canonical EXISTS form
        assert expr_in.sql() == expr_exists.sql()


class TestEXISTSNormalization:
    """EXISTS subqueries should be normalized to canonical form (SELECT 1)."""

    def test_exists_select_star_to_select_1(self, subquery_pass):
        sql = "SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t2.id = t1.id)"
        expr, steps = subquery_pass.apply(parse_one(sql))
        result = expr.sql()
        # Should use SELECT 1 instead of SELECT *
        assert "SELECT 1" in result or "select 1" in result.lower()

    def test_exists_select_column_to_select_1(self, subquery_pass):
        sql = "SELECT * FROM t1 WHERE EXISTS (SELECT t2.name FROM t2 WHERE t2.id = t1.id)"
        expr, steps = subquery_pass.apply(parse_one(sql))
        result = expr.sql()
        assert "SELECT 1" in result or "select 1" in result.lower()


class TestNOTINtoNOTEXISTS:
    """NOT IN should normalize to NOT EXISTS."""

    def test_not_in_to_not_exists(self, subquery_pass):
        sql = "SELECT * FROM t1 WHERE id NOT IN (SELECT id FROM t2)"
        expr, steps = subquery_pass.apply(parse_one(sql))
        result = expr.sql().upper()
        assert "NOT EXISTS" in result


class TestScalarINPreserved:
    """IN with a literal value list should NOT be transformed."""

    def test_in_literal_list_unchanged(self, subquery_pass):
        sql = "SELECT * FROM t WHERE status IN ('active', 'pending')"
        expr, steps = subquery_pass.apply(parse_one(sql))
        result = expr.sql().upper()
        assert "IN" in result
        assert "EXISTS" not in result
        assert len(steps) == 0


class TestRewriteSteps:
    def test_step_records_transformation(self, subquery_pass):
        sql = "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)"
        _, steps = subquery_pass.apply(parse_one(sql))
        assert len(steps) >= 1
        assert steps[0].pass_name == "subquery_join"

    def test_no_steps_when_no_subquery_predicates(self, subquery_pass):
        sql = "SELECT * FROM t WHERE x > 1"
        _, steps = subquery_pass.apply(parse_one(sql))
        assert len(steps) == 0
