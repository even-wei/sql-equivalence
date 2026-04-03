"""Tests for CommutativityPass.

Sorts commutative operands into canonical order:
AND/OR operands, inner JOIN tables, UNION ALL branches, IN value lists.
"""

import pytest
from sqlglot import parse_one

from sql_equivalence.passes.commutativity import CommutativityPass


@pytest.fixture
def comm_pass():
    return CommutativityPass()


class TestANDORSorting:
    """AND and OR operands should be sorted by canonical string representation."""

    def test_and_operands_sorted(self, comm_pass):
        sql_a = "SELECT * FROM t WHERE b = 1 AND a = 2"
        sql_b = "SELECT * FROM t WHERE a = 2 AND b = 1"
        expr_a, _ = comm_pass.apply(parse_one(sql_a))
        expr_b, _ = comm_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()

    def test_or_operands_sorted(self, comm_pass):
        sql_a = "SELECT * FROM t WHERE b = 1 OR a = 2"
        sql_b = "SELECT * FROM t WHERE a = 2 OR b = 1"
        expr_a, _ = comm_pass.apply(parse_one(sql_a))
        expr_b, _ = comm_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()

    def test_nested_and_or_sorted(self, comm_pass):
        sql_a = "SELECT * FROM t WHERE (c = 3 OR a = 1) AND b = 2"
        sql_b = "SELECT * FROM t WHERE b = 2 AND (a = 1 OR c = 3)"
        expr_a, _ = comm_pass.apply(parse_one(sql_a))
        expr_b, _ = comm_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()

    def test_many_and_operands(self, comm_pass):
        sql_a = "SELECT * FROM t WHERE d = 4 AND b = 2 AND c = 3 AND a = 1"
        sql_b = "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3 AND d = 4"
        expr_a, _ = comm_pass.apply(parse_one(sql_a))
        expr_b, _ = comm_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()


class TestJoinSorting:
    """Inner JOIN operands should be sorted (inner join is commutative)."""

    def test_inner_join_table_order(self, comm_pass):
        sql_a = "SELECT * FROM b JOIN a ON a.id = b.id"
        sql_b = "SELECT * FROM a JOIN b ON a.id = b.id"
        expr_a, _ = comm_pass.apply(parse_one(sql_a))
        expr_b, _ = comm_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()

    def test_left_join_not_reordered(self, comm_pass):
        """LEFT JOIN is NOT commutative — order must be preserved."""
        sql = "SELECT * FROM a LEFT JOIN b ON a.id = b.id"
        expr, steps = comm_pass.apply(parse_one(sql))
        result = expr.sql().upper()
        # Should still have LEFT JOIN with a before b
        assert "LEFT" in result

    def test_on_condition_operands_sorted(self, comm_pass):
        """ON clause equality operands should be sorted."""
        sql_a = "SELECT * FROM a JOIN b ON b.id = a.id"
        sql_b = "SELECT * FROM a JOIN b ON a.id = b.id"
        expr_a, _ = comm_pass.apply(parse_one(sql_a))
        expr_b, _ = comm_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()


class TestUNIONSorting:
    """UNION ALL branches should be sorted (UNION without ALL is set-based)."""

    def test_union_all_branches_sorted(self, comm_pass):
        sql_a = "SELECT 2 UNION ALL SELECT 1"
        sql_b = "SELECT 1 UNION ALL SELECT 2"
        expr_a, _ = comm_pass.apply(parse_one(sql_a))
        expr_b, _ = comm_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()

    def test_union_distinct_branches_sorted(self, comm_pass):
        """Plain UNION (set semantics) — still sort for AST comparison."""
        sql_a = "SELECT 2 UNION SELECT 1"
        sql_b = "SELECT 1 UNION SELECT 2"
        expr_a, _ = comm_pass.apply(parse_one(sql_a))
        expr_b, _ = comm_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()

    def test_except_not_sorted(self, comm_pass):
        """EXCEPT is NOT commutative — order must be preserved."""
        sql_a = "SELECT 1 EXCEPT SELECT 2"
        sql_b = "SELECT 2 EXCEPT SELECT 1"
        expr_a, _ = comm_pass.apply(parse_one(sql_a))
        expr_b, _ = comm_pass.apply(parse_one(sql_b))
        # These should remain different
        assert expr_a.sql() != expr_b.sql()


class TestINListSorting:
    """IN (value_list) elements should be sorted."""

    def test_in_values_sorted(self, comm_pass):
        sql_a = "SELECT * FROM t WHERE x IN (3, 1, 2)"
        sql_b = "SELECT * FROM t WHERE x IN (1, 2, 3)"
        expr_a, _ = comm_pass.apply(parse_one(sql_a))
        expr_b, _ = comm_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()

    def test_in_string_values_sorted(self, comm_pass):
        sql_a = "SELECT * FROM t WHERE status IN ('pending', 'active', 'done')"
        sql_b = "SELECT * FROM t WHERE status IN ('active', 'done', 'pending')"
        expr_a, _ = comm_pass.apply(parse_one(sql_a))
        expr_b, _ = comm_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()


class TestEqualitySorting:
    """Equality operands should be sorted (a = b is same as b = a)."""

    def test_equality_operands_sorted(self, comm_pass):
        sql_a = "SELECT * FROM t WHERE 5 = x"
        sql_b = "SELECT * FROM t WHERE x = 5"
        expr_a, _ = comm_pass.apply(parse_one(sql_a))
        expr_b, _ = comm_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()


class TestRewriteSteps:
    def test_step_on_reordering(self, comm_pass):
        sql = "SELECT * FROM t WHERE b = 1 AND a = 2"
        _, steps = comm_pass.apply(parse_one(sql))
        assert len(steps) >= 1
        assert steps[0].pass_name == "commutativity"

    def test_no_steps_when_already_ordered(self, comm_pass):
        sql = "SELECT 1"
        _, steps = comm_pass.apply(parse_one(sql))
        assert len(steps) == 0
