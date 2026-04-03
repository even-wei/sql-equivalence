"""Edge case and advanced equivalence tests.

Tests for complex real-world SQL patterns that combine multiple rewrites.
"""

import pytest

from sql_equivalence import check_equivalence


class TestComplexCTEPatterns:
    """Real-world CTE patterns."""

    def test_cte_with_different_names_and_predicate_order(self):
        result = check_equivalence(
            """
            WITH filtered AS (
                SELECT id, name FROM users WHERE active = 1 AND age > 18
            )
            SELECT name, id FROM filtered
            """,
            """
            WITH src AS (
                SELECT id, name FROM users WHERE age > 18 AND active = 1
            )
            SELECT id, name FROM src
            """,
        )
        assert result.equivalent is True

    def test_nested_ctes_single_use(self):
        result = check_equivalence(
            """
            WITH
                step1 AS (SELECT id FROM users),
                step2 AS (SELECT id FROM step1)
            SELECT id FROM step2
            """,
            "SELECT id FROM (SELECT id FROM (SELECT id FROM users) AS step1) AS step2",
        )
        assert result.equivalent is True


class TestComplexPredicates:
    """Complex predicate equivalences."""

    def test_range_predicates_different_order(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE x > 5 AND x < 10 AND y = 1",
            "SELECT * FROM t WHERE y = 1 AND x < 10 AND x > 5",
        )
        assert result.equivalent is True

    def test_redundant_predicates_with_reorder(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE x > 5 AND x > 3 AND y = 1",
            "SELECT * FROM t WHERE y = 1 AND x > 5",
        )
        assert result.equivalent is True

    def test_double_negation_with_commutativity(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE NOT NOT (a > 1) AND b = 2",
            "SELECT * FROM t WHERE b = 2 AND a > 1",
        )
        assert result.equivalent is True


class TestJoinEquivalences:
    """Join pattern equivalences."""

    def test_inner_join_table_swap_with_on_swap(self):
        result = check_equivalence(
            "SELECT * FROM orders JOIN customers ON orders.cid = customers.id",
            "SELECT * FROM customers JOIN orders ON customers.id = orders.cid",
        )
        assert result.equivalent is True

    def test_in_subquery_vs_exists(self):
        result = check_equivalence(
            "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE active = 1)",
            "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM customers WHERE customers.id = orders.customer_id AND active = 1)",
        )
        assert result.equivalent is True


class TestNonEquivalentEdgeCases:
    """Edge cases that should NOT be equivalent."""

    def test_left_join_vs_inner_join(self):
        result = check_equivalence(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id",
            "SELECT * FROM a JOIN b ON a.id = b.id",
        )
        assert result.equivalent is False

    def test_union_all_vs_union(self):
        result = check_equivalence(
            "SELECT 1 UNION ALL SELECT 2",
            "SELECT 1 UNION SELECT 2",
        )
        assert result.equivalent is False

    def test_different_having_clause(self):
        result = check_equivalence(
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 5",
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 10",
        )
        assert result.equivalent is False

    def test_different_limit(self):
        result = check_equivalence(
            "SELECT * FROM t LIMIT 10",
            "SELECT * FROM t LIMIT 20",
        )
        assert result.equivalent is False

    def test_order_by_asc_vs_desc(self):
        result = check_equivalence(
            "SELECT * FROM t ORDER BY a ASC",
            "SELECT * FROM t ORDER BY a DESC",
        )
        assert result.equivalent is False


class TestWhitespaceAndFormatting:
    """Formatting differences that shouldn't matter."""

    def test_multiline_vs_singleline(self):
        result = check_equivalence(
            """
            SELECT
                a,
                b,
                c
            FROM
                my_table
            WHERE
                x > 1
            """,
            "SELECT a, b, c FROM my_table WHERE x > 1",
        )
        assert result.equivalent is True

    def test_extra_parentheses(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE (x > 1)",
            "SELECT * FROM t WHERE x > 1",
        )
        assert result.equivalent is True


class TestRealWorldDbtPatterns:
    """Patterns commonly seen in dbt model changes."""

    def test_reformatted_model_with_cte_rename(self):
        """Model reformatted with different CTE names and column order."""
        result = check_equivalence(
            """
            WITH source AS (
                SELECT customer_id, order_date, amount
                FROM raw_orders
                WHERE status = 'completed' AND amount > 0
            )
            SELECT order_date, customer_id, amount FROM source
            """,
            """
            WITH orders AS (
                SELECT customer_id, order_date, amount
                FROM raw_orders
                WHERE amount > 0 AND status = 'completed'
            )
            SELECT amount, customer_id, order_date FROM orders
            """,
        )
        assert result.equivalent is True

    def test_subquery_extracted_to_cte(self):
        """Refactor: inline subquery extracted to CTE."""
        result = check_equivalence(
            "SELECT * FROM (SELECT id, name FROM users WHERE active = 1) AS active_users",
            "WITH active_users AS (SELECT id, name FROM users WHERE active = 1) SELECT * FROM active_users",
        )
        assert result.equivalent is True

    def test_predicate_cleanup(self):
        """Removed redundant predicate — semantically identical."""
        result = check_equivalence(
            "SELECT * FROM orders WHERE amount > 100 AND amount > 50 AND status = 'paid'",
            "SELECT * FROM orders WHERE amount > 100 AND status = 'paid'",
        )
        assert result.equivalent is True


class TestProofTraceQuality:
    """Verify proof traces are informative."""

    def test_multi_pass_proof_shows_all_transforms(self):
        result = check_equivalence(
            "WITH src AS (SELECT b, a FROM t WHERE y = 1 AND x = 2) SELECT b, a FROM src",
            "SELECT a, b FROM (SELECT a, b FROM t WHERE x = 2 AND y = 1) AS sub",
        )
        assert result.equivalent is True
        all_steps = result.proof_a + result.proof_b
        pass_names = {s.pass_name for s in all_steps}
        # Should involve at least alias normalization + commutativity + column reorder
        assert len(pass_names) >= 2

    def test_non_equivalent_diff_is_specific(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE x > 5",
            "SELECT * FROM t WHERE x > 10",
        )
        assert result.equivalent is False
        assert result.remaining_diff is not None
        # Diff should mention the actual difference
        assert "5" in result.remaining_diff or "10" in result.remaining_diff
