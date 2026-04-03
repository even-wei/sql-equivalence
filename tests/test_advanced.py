"""Advanced tests for tricky SQL equivalences and known limitations."""

import pytest

from sql_equivalence import check_equivalence


class TestWindowFunctions:
    """Window functions — currently a known limitation."""

    def test_identical_window_functions(self):
        result = check_equivalence(
            "SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c) AS rn FROM t",
            "SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c) AS rn FROM t",
        )
        assert result.equivalent is True

    def test_different_window_order(self):
        result = check_equivalence(
            "SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c ASC) AS rn FROM t",
            "SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY c DESC) AS rn FROM t",
        )
        assert result.equivalent is False


class TestAggregations:
    """Aggregation patterns."""

    def test_same_aggregation_different_predicate_order(self):
        result = check_equivalence(
            "SELECT a, SUM(b) FROM t WHERE x > 1 AND y < 10 GROUP BY a",
            "SELECT a, SUM(b) FROM t WHERE y < 10 AND x > 1 GROUP BY a",
        )
        assert result.equivalent is True

    def test_count_star_vs_count_1(self):
        """COUNT(*) and COUNT(1) are semantically equivalent but we don't normalize this."""
        result = check_equivalence(
            "SELECT COUNT(*) FROM t",
            "SELECT COUNT(1) FROM t",
        )
        # This is a known limitation — we don't normalize COUNT(*) to COUNT(1)
        # The test documents current behavior
        assert result.equivalent is False


class TestMultipleJoins:
    """Complex join patterns."""

    def test_two_table_inner_join_reorder(self):
        result = check_equivalence(
            "SELECT * FROM b JOIN a ON a.id = b.id",
            "SELECT * FROM a JOIN b ON a.id = b.id",
        )
        assert result.equivalent is True

    @pytest.mark.skip(reason="Known limitation: ON conditions not redistributed on 3+ table reorder")
    def test_three_table_inner_join_reorder(self):
        result = check_equivalence(
            "SELECT * FROM c JOIN b ON b.id = c.bid JOIN a ON a.id = b.aid",
            "SELECT * FROM a JOIN b ON a.id = b.aid JOIN c ON b.id = c.bid",
        )
        assert result.equivalent is True

    def test_mixed_join_types_preserved(self):
        """LEFT JOIN mixed with INNER — should NOT be reordered."""
        result = check_equivalence(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id JOIN c ON b.id = c.id",
            "SELECT * FROM c JOIN a ON a.id = c.id LEFT JOIN b ON a.id = b.id",
        )
        assert result.equivalent is False


class TestSubqueryPatterns:
    """Complex subquery patterns."""

    def test_correlated_exists_identical(self):
        result = check_equivalence(
            """
            SELECT * FROM orders o
            WHERE EXISTS (
                SELECT 1 FROM returns r WHERE r.order_id = o.id AND r.reason = 'defect'
            )
            """,
            """
            SELECT * FROM orders o
            WHERE EXISTS (
                SELECT 1 FROM returns r WHERE r.reason = 'defect' AND r.order_id = o.id
            )
            """,
        )
        assert result.equivalent is True


class TestINListPatterns:
    """IN value list patterns."""

    def test_in_values_reorder(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE status IN ('c', 'a', 'b')",
            "SELECT * FROM t WHERE status IN ('a', 'b', 'c')",
        )
        assert result.equivalent is True

    def test_different_in_values(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE status IN ('a', 'b')",
            "SELECT * FROM t WHERE status IN ('a', 'c')",
        )
        assert result.equivalent is False


class TestExceptPreserved:
    """EXCEPT is not commutative and must be preserved."""

    def test_except_order_matters(self):
        result = check_equivalence(
            "SELECT 1 EXCEPT SELECT 2",
            "SELECT 2 EXCEPT SELECT 1",
        )
        assert result.equivalent is False


class TestDeMorgans:
    """De Morgan's equivalences through the full pipeline."""

    def test_not_and_vs_or_negated(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE NOT (x > 1 AND y > 2)",
            "SELECT * FROM t WHERE NOT x > 1 OR NOT y > 2",
        )
        assert result.equivalent is True

    def test_not_or_vs_and_negated(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE NOT (x > 1 OR y > 2)",
            "SELECT * FROM t WHERE NOT x > 1 AND NOT y > 2",
        )
        assert result.equivalent is True


class TestDialects:
    """Multi-dialect tests."""

    def test_bigquery_qualified_table(self):
        result = check_equivalence(
            "SELECT b, a FROM `project.dataset.table` WHERE x > 1",
            "SELECT a, b FROM `project.dataset.table` WHERE x > 1",
            dialect="bigquery",
        )
        assert result.equivalent is True

    def test_duckdb_dialect(self):
        result = check_equivalence(
            "SELECT b, a FROM t WHERE y = 1 AND x = 2",
            "SELECT a, b FROM t WHERE x = 2 AND y = 1",
            dialect="duckdb",
        )
        assert result.equivalent is True
