"""Tests for ColumnReorderPass.

Sorts SELECT columns alphabetically unless referenced positionally
(e.g. ORDER BY 1, GROUP BY 2).
"""

import pytest
from sqlglot import parse_one

from sql_equivalence.passes.column_reorder import ColumnReorderPass


@pytest.fixture
def col_pass():
    return ColumnReorderPass()


class TestBasicColumnReordering:
    """Columns should be sorted alphabetically by output name."""

    def test_two_columns_reordered(self, col_pass):
        sql_a = "SELECT b, a FROM t"
        sql_b = "SELECT a, b FROM t"
        expr_a, _ = col_pass.apply(parse_one(sql_a))
        expr_b, _ = col_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()

    def test_many_columns_reordered(self, col_pass):
        sql_a = "SELECT d, b, c, a FROM t"
        sql_b = "SELECT a, b, c, d FROM t"
        expr_a, _ = col_pass.apply(parse_one(sql_a))
        expr_b, _ = col_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()

    def test_aliased_columns_sorted_by_alias(self, col_pass):
        sql_a = "SELECT x AS z_col, y AS a_col FROM t"
        sql_b = "SELECT y AS a_col, x AS z_col FROM t"
        expr_a, _ = col_pass.apply(parse_one(sql_a))
        expr_b, _ = col_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()


class TestPositionalReferenceBlocksReorder:
    """When ORDER BY or GROUP BY uses positional refs, skip column reordering."""

    def test_order_by_position_blocks_reorder(self, col_pass):
        sql_a = "SELECT b, a FROM t ORDER BY 1"
        sql_b = "SELECT a, b FROM t ORDER BY 1"
        expr_a, _ = col_pass.apply(parse_one(sql_a))
        expr_b, _ = col_pass.apply(parse_one(sql_b))
        # Should NOT be equal — positional ref means column order matters
        assert expr_a.sql() != expr_b.sql()

    def test_group_by_position_blocks_reorder(self, col_pass):
        sql_a = "SELECT b, COUNT(*) FROM t GROUP BY 1"
        sql_b = "SELECT a, COUNT(*) FROM t GROUP BY 1"
        expr_a, _ = col_pass.apply(parse_one(sql_a))
        expr_b, _ = col_pass.apply(parse_one(sql_b))
        # Different columns, both with GROUP BY 1 — should remain different
        assert expr_a.sql() != expr_b.sql()

    def test_order_by_name_allows_reorder(self, col_pass):
        """ORDER BY column_name (not positional) should still allow reordering."""
        sql_a = "SELECT b, a FROM t ORDER BY a"
        sql_b = "SELECT a, b FROM t ORDER BY a"
        expr_a, _ = col_pass.apply(parse_one(sql_a))
        expr_b, _ = col_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()


class TestSubqueryColumns:
    """Column reordering should apply recursively to subqueries."""

    def test_subquery_columns_reordered(self, col_pass):
        sql_a = "SELECT * FROM (SELECT b, a FROM t) AS sub"
        sql_b = "SELECT * FROM (SELECT a, b FROM t) AS sub"
        expr_a, _ = col_pass.apply(parse_one(sql_a))
        expr_b, _ = col_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()


class TestSelectStar:
    """SELECT * should not be affected — nothing to reorder."""

    def test_select_star_unchanged(self, col_pass):
        sql = "SELECT * FROM t"
        expr, steps = col_pass.apply(parse_one(sql))
        assert expr.sql() == parse_one(sql).sql()
        assert len(steps) == 0


class TestSingleColumn:
    """Single column — nothing to reorder."""

    def test_single_column_unchanged(self, col_pass):
        sql = "SELECT a FROM t"
        expr, steps = col_pass.apply(parse_one(sql))
        assert len(steps) == 0


class TestRewriteSteps:
    def test_step_on_reordering(self, col_pass):
        sql = "SELECT b, a FROM t"
        _, steps = col_pass.apply(parse_one(sql))
        assert len(steps) >= 1
        assert steps[0].pass_name == "column_reorder"

    def test_no_steps_when_already_ordered(self, col_pass):
        sql = "SELECT a, b FROM t"
        _, steps = col_pass.apply(parse_one(sql))
        assert len(steps) == 0
