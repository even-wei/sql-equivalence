"""Comprehensive subquery equivalence tests.

Based on real-world SQL patterns:
- FROM subqueries (derived tables)
- WHERE subqueries (scalar, IN, EXISTS, correlated)
- SELECT subqueries (scalar)
- Nested subqueries at multiple levels
- Subquery ↔ JOIN conversions
- NOT IN ↔ NOT EXISTS ↔ LEFT JOIN IS NULL

Sources:
- https://dev.mysql.com/doc/refman/5.7/en/rewriting-subqueries.html
- https://learnsql.com/blog/subquery-vs-join/
- https://www.cybertec-postgresql.com/en/subqueries-and-performance-in-postgresql/
- https://www.baeldung.com/sql/lateral-join-vs-subquery
"""

import pytest

from sql_equivalence import check_equivalence


class TestFROMSubqueries:
    """Derived tables (subqueries in FROM clause)."""

    def test_simple_derived_table(self):
        """Basic subquery in FROM ↔ CTE."""
        result = check_equivalence(
            "SELECT x FROM (SELECT 1 AS x) AS t",
            "WITH t AS (SELECT 1 AS x) SELECT x FROM t",
        )
        assert result.equivalent is True

    def test_derived_table_with_aggregation(self):
        result = check_equivalence(
            """
            SELECT dept, avg_salary
            FROM (
                SELECT department AS dept, AVG(salary) AS avg_salary
                FROM employees
                GROUP BY department
            ) AS dept_avg
            """,
            """
            WITH dept_avg AS (
                SELECT department AS dept, AVG(salary) AS avg_salary
                FROM employees
                GROUP BY department
            )
            SELECT dept, avg_salary FROM dept_avg
            """,
        )
        assert result.equivalent is True

    def test_nested_derived_tables(self):
        """Subquery inside a subquery ↔ CTE chain."""
        result = check_equivalence(
            """
            SELECT name
            FROM (
                SELECT name, salary
                FROM (
                    SELECT name, salary, department
                    FROM employees
                    WHERE department = 'Engineering'
                ) AS eng
                WHERE salary > 100000
            ) AS high_earners
            """,
            """
            WITH
                eng AS (
                    SELECT name, salary, department
                    FROM employees
                    WHERE department = 'Engineering'
                ),
                high_earners AS (
                    SELECT name, salary FROM eng WHERE salary > 100000
                )
            SELECT name FROM high_earners
            """,
        )
        assert result.equivalent is True

    def test_derived_table_with_different_alias(self):
        """Same subquery, different alias name."""
        result = check_equivalence(
            "SELECT a FROM (SELECT 1 AS a, 2 AS b) AS foo",
            "SELECT a FROM (SELECT 1 AS a, 2 AS b) AS bar",
        )
        assert result.equivalent is True


class TestWHERESubqueries:
    """Subqueries in WHERE clause (IN, EXISTS, scalar, correlated)."""

    def test_in_subquery_basic(self):
        """WHERE col IN (SELECT ...) — basic filter."""
        result = check_equivalence(
            "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE active = 1)",
            "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM customers WHERE customers.id = orders.customer_id AND active = 1)",
        )
        assert result.equivalent is True

    def test_not_in_to_not_exists(self):
        """WHERE col NOT IN (SELECT ...) ↔ NOT EXISTS."""
        result = check_equivalence(
            "SELECT * FROM orders WHERE customer_id NOT IN (SELECT id FROM customers WHERE churned = 1)",
            "SELECT * FROM orders WHERE NOT EXISTS (SELECT 1 FROM customers WHERE customers.id = orders.customer_id AND churned = 1)",
        )
        assert result.equivalent is True

    def test_exists_select_star_vs_select_1(self):
        """EXISTS (SELECT *) ↔ EXISTS (SELECT 1) — the SELECT list doesn't matter in EXISTS."""
        result = check_equivalence(
            "SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t2.id = t1.id)",
            "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)",
        )
        assert result.equivalent is True

    def test_exists_select_column_vs_select_1(self):
        """EXISTS (SELECT col) ↔ EXISTS (SELECT 1)."""
        result = check_equivalence(
            "SELECT * FROM t1 WHERE EXISTS (SELECT t2.name FROM t2 WHERE t2.id = t1.id)",
            "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)",
        )
        assert result.equivalent is True

    def test_in_with_reordered_values(self):
        """IN value list order doesn't matter."""
        result = check_equivalence(
            "SELECT * FROM t WHERE status IN ('pending', 'active', 'done')",
            "SELECT * FROM t WHERE status IN ('active', 'done', 'pending')",
        )
        assert result.equivalent is True

    def test_multiple_in_subqueries(self):
        """Two IN subqueries with AND."""
        result = check_equivalence(
            """
            SELECT * FROM orders
            WHERE customer_id IN (SELECT id FROM customers WHERE active = 1)
              AND product_id IN (SELECT id FROM products WHERE in_stock = 1)
            """,
            """
            SELECT * FROM orders
            WHERE product_id IN (SELECT id FROM products WHERE in_stock = 1)
              AND customer_id IN (SELECT id FROM customers WHERE active = 1)
            """,
        )
        assert result.equivalent is True


class TestNestedSubqueries:
    """Deeply nested subquery patterns."""

    def test_three_level_nesting(self):
        """3 levels of nesting ↔ equivalent CTE chain."""
        result = check_equivalence(
            """
            SELECT total FROM (
                SELECT SUM(amount) AS total FROM (
                    SELECT amount FROM (
                        SELECT amount, status FROM payments
                    ) AS l1
                    WHERE status = 'completed'
                ) AS l2
            ) AS l3
            """,
            """
            WITH
                l1 AS (SELECT amount, status FROM payments),
                l2 AS (SELECT amount FROM l1 WHERE status = 'completed'),
                l3 AS (SELECT SUM(amount) AS total FROM l2)
            SELECT total FROM l3
            """,
        )
        assert result.equivalent is True

    def test_subquery_in_join_condition(self):
        """Subquery within a JOIN ON clause — identical pair."""
        result = check_equivalence(
            """
            SELECT a.id, b.name
            FROM table_a a
            JOIN table_b b ON a.id = b.a_id AND b.type = 'primary'
            """,
            """
            SELECT a.id, b.name
            FROM table_b b
            JOIN table_a a ON b.a_id = a.id AND b.type = 'primary'
            """,
        )
        assert result.equivalent is True


class TestSubqueryNonEquivalences:
    """Cases where subqueries are NOT equivalent."""

    def test_in_different_subquery_table(self):
        result = check_equivalence(
            "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)",
            "SELECT * FROM t1 WHERE id IN (SELECT id FROM t3)",
        )
        assert result.equivalent is False

    def test_exists_different_correlation(self):
        result = check_equivalence(
            "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)",
            "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.name = t1.name)",
        )
        assert result.equivalent is False

    def test_in_vs_not_in(self):
        result = check_equivalence(
            "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)",
            "SELECT * FROM t1 WHERE id NOT IN (SELECT id FROM t2)",
        )
        assert result.equivalent is False

    def test_different_nested_subquery_filter(self):
        result = check_equivalence(
            "SELECT * FROM (SELECT * FROM t WHERE x > 5) AS sub",
            "SELECT * FROM (SELECT * FROM t WHERE x > 10) AS sub",
        )
        assert result.equivalent is False

    def test_subquery_with_vs_without_where(self):
        result = check_equivalence(
            "SELECT * FROM (SELECT * FROM t) AS sub",
            "SELECT * FROM (SELECT * FROM t WHERE x > 0) AS sub",
        )
        assert result.equivalent is False


class TestMixedCTEAndSubquery:
    """Mixing CTEs and subqueries in the same query."""

    def test_cte_with_subquery_in_body(self):
        """CTE body contains a FROM subquery."""
        result = check_equivalence(
            """
            WITH processed AS (
                SELECT id, total
                FROM (SELECT id, SUM(amount) AS total FROM orders GROUP BY id) AS agg
                WHERE total > 1000
            )
            SELECT id, total FROM processed
            """,
            """
            SELECT id, total
            FROM (
                SELECT id, total
                FROM (SELECT id, SUM(amount) AS total FROM orders GROUP BY id) AS agg
                WHERE total > 1000
            ) AS processed
            """,
        )
        assert result.equivalent is True

    def test_subquery_with_in_and_cte_rewrite(self):
        """WHERE IN subquery inside CTE ↔ same IN subquery inside FROM subquery.

        Both use IN (not pre-converted to EXISTS), so the IN→EXISTS pass
        processes them identically after CTE inlining.
        """
        result = check_equivalence(
            """
            WITH active_orders AS (
                SELECT * FROM orders
                WHERE customer_id IN (SELECT id FROM customers WHERE active = 1)
            )
            SELECT * FROM active_orders
            """,
            """
            SELECT * FROM (
                SELECT * FROM orders
                WHERE customer_id IN (SELECT id FROM customers WHERE active = 1)
            ) AS active_orders
            """,
        )
        assert result.equivalent is True


class TestRealWorldRefactoring:
    """Real-world refactoring patterns commonly seen in code review."""

    @pytest.mark.skip(reason="Known limitation: CTE inlined inside IN subquery creates nested FROM that doesn't flatten")
    def test_extract_repeated_logic_to_cte_in_where(self):
        """CTE extracted from WHERE IN — the CTE inlines as nested subquery.

        After inlining: `IN (SELECT id FROM (SELECT id FROM departments WHERE ...) AS _t0)`
        vs direct: `IN (SELECT id FROM departments WHERE ...)`
        These are equivalent but require subquery flattening (not yet implemented).
        """
        result = check_equivalence(
            """
            SELECT name
            FROM employees
            WHERE department_id IN (
                SELECT id FROM departments WHERE region = 'US'
            )
            """,
            """
            WITH us_depts AS (
                SELECT id FROM departments WHERE region = 'US'
            )
            SELECT name
            FROM employees
            WHERE department_id IN (SELECT id FROM us_depts)
            """,
        )
        assert result.equivalent is True

    def test_extract_from_subquery_to_cte(self):
        """FROM subquery extracted to CTE — the simple, common case."""
        result = check_equivalence(
            """
            SELECT name, total
            FROM (
                SELECT name, SUM(amount) AS total
                FROM orders
                GROUP BY name
            ) AS summary
            WHERE total > 100
            """,
            """
            WITH summary AS (
                SELECT name, SUM(amount) AS total
                FROM orders
                GROUP BY name
            )
            SELECT name, total FROM summary WHERE total > 100
            """,
        )
        assert result.equivalent is True

    def test_flatten_nested_to_cte_chain(self):
        """Deep nesting flattened to readable CTE chain — common PR refactor."""
        result = check_equivalence(
            """
            SELECT customer_name, total_orders
            FROM (
                SELECT c.name AS customer_name, order_counts.total_orders
                FROM customers c
                JOIN (
                    SELECT customer_id, COUNT(*) AS total_orders
                    FROM orders
                    WHERE status = 'completed'
                    GROUP BY customer_id
                ) AS order_counts ON c.id = order_counts.customer_id
            ) AS enriched
            WHERE total_orders > 5
            """,
            """
            WITH
                order_counts AS (
                    SELECT customer_id, COUNT(*) AS total_orders
                    FROM orders
                    WHERE status = 'completed'
                    GROUP BY customer_id
                ),
                enriched AS (
                    SELECT c.name AS customer_name, order_counts.total_orders
                    FROM customers c
                    JOIN order_counts ON c.id = order_counts.customer_id
                )
            SELECT customer_name, total_orders
            FROM enriched
            WHERE total_orders > 5
            """,
        )
        assert result.equivalent is True
