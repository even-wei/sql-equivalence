"""Comprehensive CTE equivalence tests.

Based on real-world SQL refactoring patterns:
- CTE ↔ inline subquery conversions
- CTE chaining (sequential pipeline)
- Multi-reference CTEs (must NOT inline)
- Nested CTEs
- CTE with aggregation, filtering, joins
- dbt-style model refactoring patterns

Sources:
- https://datalemur.com/sql-tutorial/sql-cte-subquery
- https://learnsql.com/blog/cte-vs-subquery/
- https://www.metabase.com/learn/sql/working-with-sql/sql-cte
- https://docs.getdbt.com/guides/refactoring-legacy-sql
"""

import pytest

from sql_equivalence import check_equivalence


class TestCTEToSubquery:
    """Basic CTE ↔ FROM subquery conversions."""

    def test_simple_select_all(self):
        """WITH cte AS (SELECT ...) SELECT * FROM cte ↔ SELECT * FROM (SELECT ...) sub"""
        result = check_equivalence(
            "WITH cte AS (SELECT id, name FROM users) SELECT * FROM cte",
            "SELECT * FROM (SELECT id, name FROM users) AS cte",
        )
        assert result.equivalent is True

    def test_cte_with_filter(self):
        """CTE that filters, then outer query selects from it."""
        result = check_equivalence(
            """
            WITH active AS (
                SELECT id, name, email FROM users WHERE status = 'active'
            )
            SELECT name, email FROM active
            """,
            "SELECT name, email FROM (SELECT id, name, email FROM users WHERE status = 'active') AS active",
        )
        assert result.equivalent is True

    def test_cte_with_aggregation(self):
        """CTE that aggregates, outer query reads the summary."""
        result = check_equivalence(
            """
            WITH order_totals AS (
                SELECT customer_id, SUM(amount) AS total
                FROM orders
                GROUP BY customer_id
            )
            SELECT customer_id, total FROM order_totals
            """,
            """
            SELECT customer_id, total
            FROM (
                SELECT customer_id, SUM(amount) AS total
                FROM orders
                GROUP BY customer_id
            ) AS order_totals
            """,
        )
        assert result.equivalent is True

    def test_cte_with_join_inside(self):
        """CTE body contains a JOIN."""
        result = check_equivalence(
            """
            WITH enriched_orders AS (
                SELECT o.id, o.amount, c.name
                FROM orders o
                JOIN customers c ON o.customer_id = c.id
            )
            SELECT id, name, amount FROM enriched_orders
            """,
            """
            SELECT id, name, amount
            FROM (
                SELECT o.id, o.amount, c.name
                FROM orders o
                JOIN customers c ON o.customer_id = c.id
            ) AS enriched_orders
            """,
        )
        assert result.equivalent is True

    def test_cte_with_window_function(self):
        """CTE body uses a window function."""
        result = check_equivalence(
            """
            WITH ranked AS (
                SELECT id, name, ROW_NUMBER() OVER (ORDER BY created_at) AS rn
                FROM users
            )
            SELECT id, name, rn FROM ranked
            """,
            """
            SELECT id, name, rn
            FROM (
                SELECT id, name, ROW_NUMBER() OVER (ORDER BY created_at) AS rn
                FROM users
            ) AS ranked
            """,
        )
        assert result.equivalent is True


class TestCTEChaining:
    """CTE chains: sequential CTEs where each references the prior one."""

    def test_two_step_pipeline(self):
        """step1 → step2 → final, all single-use → fully inlineable."""
        result = check_equivalence(
            """
            WITH
                step1 AS (SELECT id, amount FROM orders WHERE amount > 0),
                step2 AS (SELECT id, amount * 1.1 AS taxed FROM step1)
            SELECT id, taxed FROM step2
            """,
            """
            SELECT id, taxed
            FROM (
                SELECT id, amount * 1.1 AS taxed
                FROM (
                    SELECT id, amount FROM orders WHERE amount > 0
                ) AS step1
            ) AS step2
            """,
        )
        assert result.equivalent is True

    def test_three_step_pipeline(self):
        """Three-level CTE chain ↔ triple-nested subquery."""
        result = check_equivalence(
            """
            WITH
                raw AS (SELECT id, status, amount FROM orders),
                filtered AS (SELECT id, amount FROM raw WHERE status = 'paid'),
                summarized AS (SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM filtered)
            SELECT cnt, total FROM summarized
            """,
            """
            SELECT cnt, total
            FROM (
                SELECT COUNT(*) AS cnt, SUM(amount) AS total
                FROM (
                    SELECT id, amount
                    FROM (
                        SELECT id, status, amount FROM orders
                    ) AS raw
                    WHERE status = 'paid'
                ) AS filtered
            ) AS summarized
            """,
        )
        assert result.equivalent is True

    def test_chain_with_different_cte_names(self):
        """Same pipeline, different CTE names → should be equivalent."""
        result = check_equivalence(
            """
            WITH
                src AS (SELECT id, name FROM users WHERE active = 1),
                result AS (SELECT name FROM src)
            SELECT name FROM result
            """,
            """
            WITH
                base_data AS (SELECT id, name FROM users WHERE active = 1),
                final AS (SELECT name FROM base_data)
            SELECT name FROM final
            """,
        )
        assert result.equivalent is True

    def test_chain_with_predicate_reorder(self):
        """CTE chain + predicate order difference."""
        result = check_equivalence(
            """
            WITH
                base AS (SELECT * FROM orders WHERE status = 'active' AND amount > 100),
                summary AS (SELECT customer_id, SUM(amount) AS total FROM base GROUP BY customer_id)
            SELECT customer_id, total FROM summary
            """,
            """
            WITH
                src AS (SELECT * FROM orders WHERE amount > 100 AND status = 'active'),
                agg AS (SELECT customer_id, SUM(amount) AS total FROM src GROUP BY customer_id)
            SELECT total, customer_id FROM agg
            """,
        )
        assert result.equivalent is True


class TestMultiReferenceCTE:
    """CTEs referenced more than once — should NOT be inlined."""

    @pytest.mark.skip(reason="Known limitation: multi-ref CTE not expanded to match duplicated subqueries")
    def test_cte_in_union_branches(self):
        """Same CTE used in both UNION branches — not equivalent to double subquery.

        The CTE version keeps the CTE (multi-ref) while the subquery version
        has two independent subqueries. Proving equivalence requires expanding
        multi-ref CTEs, which is not yet implemented.
        """
        result = check_equivalence(
            """
            WITH src AS (SELECT id FROM users)
            SELECT id FROM src
            UNION ALL
            SELECT id FROM src
            """,
            """
            SELECT id FROM (SELECT id FROM users) AS s1
            UNION ALL
            SELECT id FROM (SELECT id FROM users) AS s2
            """,
        )
        assert result.equivalent is True

    def test_cte_self_join(self):
        """CTE joined with itself."""
        result = check_equivalence(
            """
            WITH nums AS (SELECT id FROM generate_series(1, 10) AS id)
            SELECT a.id, b.id FROM nums a JOIN nums b ON a.id < b.id
            """,
            """
            WITH nums AS (SELECT id FROM generate_series(1, 10) AS id)
            SELECT a.id, b.id FROM nums a JOIN nums b ON a.id < b.id
            """,
        )
        assert result.equivalent is True

    def test_mixed_single_and_multi_reference(self):
        """One CTE used once, another used twice — only the single-use gets inlined."""
        result = check_equivalence(
            """
            WITH
                single_use AS (SELECT 1 AS x),
                multi_use AS (SELECT 2 AS y)
            SELECT x FROM single_use
            UNION ALL
            SELECT y FROM multi_use
            UNION ALL
            SELECT y FROM multi_use
            """,
            """
            WITH multi_use AS (SELECT 2 AS y)
            SELECT x FROM (SELECT 1 AS x) AS single_use
            UNION ALL
            SELECT y FROM multi_use
            UNION ALL
            SELECT y FROM multi_use
            """,
        )
        assert result.equivalent is True


class TestCTENonEquivalences:
    """Cases that look similar but are NOT semantically equivalent."""

    def test_cte_with_different_filter(self):
        result = check_equivalence(
            "WITH cte AS (SELECT * FROM t WHERE x > 5) SELECT * FROM cte",
            "WITH cte AS (SELECT * FROM t WHERE x > 10) SELECT * FROM cte",
        )
        assert result.equivalent is False

    def test_cte_with_extra_column(self):
        result = check_equivalence(
            "WITH cte AS (SELECT a, b FROM t) SELECT a FROM cte",
            "WITH cte AS (SELECT a, b, c FROM t) SELECT a FROM cte",
        )
        assert result.equivalent is False

    def test_cte_with_different_aggregation(self):
        result = check_equivalence(
            "WITH agg AS (SELECT SUM(x) AS total FROM t) SELECT total FROM agg",
            "WITH agg AS (SELECT AVG(x) AS total FROM t) SELECT total FROM agg",
        )
        assert result.equivalent is False


class TestCTEWithDbtPatterns:
    """Patterns from dbt model refactoring (source: dbt docs)."""

    def test_import_rename_logic_pattern(self):
        """dbt's import-rename-logic CTE pattern ↔ nested subqueries."""
        result = check_equivalence(
            """
            WITH
                source AS (SELECT * FROM raw_payments),
                renamed AS (
                    SELECT
                        id AS payment_id,
                        order_id,
                        amount
                    FROM source
                ),
                final AS (
                    SELECT payment_id, order_id, amount
                    FROM renamed
                    WHERE amount > 0
                )
            SELECT payment_id, order_id, amount FROM final
            """,
            """
            SELECT payment_id, order_id, amount
            FROM (
                SELECT payment_id, order_id, amount
                FROM (
                    SELECT id AS payment_id, order_id, amount
                    FROM (
                        SELECT * FROM raw_payments
                    ) AS source
                ) AS renamed
                WHERE amount > 0
            ) AS final
            """,
        )
        assert result.equivalent is True

    def test_staging_model_equivalent_reformat(self):
        """Same dbt staging model, reformatted with different CTE names + column order."""
        result = check_equivalence(
            """
            WITH stg AS (
                SELECT
                    id,
                    created_at,
                    UPPER(status) AS status
                FROM raw_orders
                WHERE created_at >= '2024-01-01'
            )
            SELECT id, status, created_at FROM stg
            """,
            """
            WITH raw AS (
                SELECT
                    UPPER(status) AS status,
                    created_at,
                    id
                FROM raw_orders
                WHERE created_at >= '2024-01-01'
            )
            SELECT created_at, id, status FROM raw
            """,
        )
        assert result.equivalent is True
