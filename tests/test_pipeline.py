"""End-to-end tests for the full equivalence checking pipeline.

These tests exercise check_equivalence() with pairs of SQL queries
that combine multiple rewrite passes to prove equivalence.
"""

import pytest

from sql_equivalence import check_equivalence


class TestIdenticalQueries:
    """Identical SQL should always be equivalent."""

    def test_exact_same_sql(self):
        result = check_equivalence(
            "SELECT a, b FROM t WHERE x > 1",
            "SELECT a, b FROM t WHERE x > 1",
        )
        assert result.equivalent is True
        assert result.remaining_diff is None

    def test_whitespace_differences(self):
        result = check_equivalence(
            "SELECT  a,  b  FROM  t",
            "SELECT a, b FROM t",
        )
        assert result.equivalent is True

    def test_case_differences(self):
        result = check_equivalence(
            "select a from t where x > 1",
            "SELECT a FROM t WHERE x > 1",
        )
        assert result.equivalent is True


class TestSinglePassEquivalences:
    """Queries that differ by exactly one rewrite pass."""

    def test_alias_difference(self):
        result = check_equivalence(
            "SELECT * FROM (SELECT 1 AS x) AS foo",
            "SELECT * FROM (SELECT 1 AS x) AS bar",
        )
        assert result.equivalent is True

    def test_cte_vs_inline(self):
        result = check_equivalence(
            "WITH cte AS (SELECT a FROM t) SELECT a FROM cte",
            "SELECT a FROM (SELECT a FROM t) AS cte",
        )
        assert result.equivalent is True

    def test_and_commutativity(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE b = 1 AND a = 2",
            "SELECT * FROM t WHERE a = 2 AND b = 1",
        )
        assert result.equivalent is True

    def test_predicate_absorption(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE x > 5 AND x > 3",
            "SELECT * FROM t WHERE x > 5",
        )
        assert result.equivalent is True

    def test_column_reorder(self):
        result = check_equivalence(
            "SELECT b, a FROM t",
            "SELECT a, b FROM t",
        )
        assert result.equivalent is True

    def test_in_vs_exists(self):
        result = check_equivalence(
            "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)",
            "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)",
        )
        assert result.equivalent is True


class TestMultiPassEquivalences:
    """Queries that require multiple passes to prove equivalence."""

    def test_cte_alias_and_commutativity(self):
        """Different CTE names + different AND order."""
        result = check_equivalence(
            "WITH src AS (SELECT * FROM t WHERE b = 1 AND a = 2) SELECT * FROM src",
            "WITH data AS (SELECT * FROM t WHERE a = 2 AND b = 1) SELECT * FROM data",
        )
        assert result.equivalent is True

    def test_column_reorder_and_predicate(self):
        """Different column order + redundant predicate."""
        result = check_equivalence(
            "SELECT b, a FROM t WHERE x > 5 AND x > 3",
            "SELECT a, b FROM t WHERE x > 5",
        )
        assert result.equivalent is True

    def test_cte_inline_and_alias_and_column_reorder(self):
        """CTE → inline + different alias + different column order."""
        result = check_equivalence(
            "WITH my_src AS (SELECT b, a FROM t) SELECT b, a FROM my_src",
            "SELECT a, b FROM (SELECT b, a FROM t) AS sub",
        )
        assert result.equivalent is True


class TestNonEquivalentQueries:
    """Queries that are genuinely different should NOT be marked equivalent."""

    def test_different_tables(self):
        result = check_equivalence(
            "SELECT a FROM t1",
            "SELECT a FROM t2",
        )
        assert result.equivalent is False
        assert result.remaining_diff is not None

    def test_different_predicates(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE x > 5",
            "SELECT * FROM t WHERE x > 10",
        )
        assert result.equivalent is False

    def test_different_columns(self):
        result = check_equivalence(
            "SELECT a FROM t",
            "SELECT b FROM t",
        )
        assert result.equivalent is False

    def test_extra_join(self):
        result = check_equivalence(
            "SELECT * FROM t1",
            "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
        )
        assert result.equivalent is False

    def test_different_aggregation(self):
        result = check_equivalence(
            "SELECT a, COUNT(*) FROM t GROUP BY a",
            "SELECT a, SUM(b) FROM t GROUP BY a",
        )
        assert result.equivalent is False

    def test_where_vs_no_where(self):
        result = check_equivalence(
            "SELECT * FROM t",
            "SELECT * FROM t WHERE x > 1",
        )
        assert result.equivalent is False


class TestProofTrace:
    """The proof trace should document transformations applied."""

    def test_proof_lists_passes_applied(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE b = 1 AND a = 2",
            "SELECT * FROM t WHERE a = 2 AND b = 1",
        )
        assert result.equivalent is True
        # At least one proof should have commutativity steps
        all_steps = result.proof_a + result.proof_b
        pass_names = {s.pass_name for s in all_steps}
        assert "commutativity" in pass_names

    def test_proof_empty_for_identical(self):
        result = check_equivalence(
            "SELECT a FROM t",
            "SELECT a FROM t",
        )
        assert result.equivalent is True
        assert len(result.proof_a) == 0
        assert len(result.proof_b) == 0

    def test_remaining_diff_shows_divergence(self):
        result = check_equivalence(
            "SELECT * FROM t WHERE x > 5",
            "SELECT * FROM t WHERE x > 10",
        )
        assert result.equivalent is False
        assert result.remaining_diff is not None
        assert len(result.remaining_diff) > 0


class TestDialectSupport:
    """Dialect parameter should be forwarded to sqlglot parser."""

    def test_bigquery_backtick_identifiers(self):
        result = check_equivalence(
            "SELECT a FROM `project.dataset.table`",
            "SELECT a FROM `project.dataset.table`",
            dialect="bigquery",
        )
        assert result.equivalent is True

    def test_snowflake_case_insensitivity(self):
        result = check_equivalence(
            "SELECT A FROM T",
            "SELECT a FROM t",
            dialect="snowflake",
        )
        # Snowflake identifiers are case-insensitive by default
        assert result.equivalent is True
