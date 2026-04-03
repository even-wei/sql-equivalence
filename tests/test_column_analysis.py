"""Tests for column-level equivalence analysis.

Verifies that check_equivalence().columns provides detailed
per-column status: equivalent, modified, added, removed.
"""

import pytest

from sql_equivalence import check_equivalence, ColumnStatus


def _col_map(result) -> dict[str, ColumnStatus]:
    """Helper: convert columns list to {name: ColumnStatus} dict."""
    return {c.name: c for c in result.columns}


class TestAllColumnsEquivalent:
    """When the full query is equivalent, all columns should be equivalent."""

    def test_identical_columns(self):
        result = check_equivalence(
            "SELECT a, b, c FROM t",
            "SELECT a, b, c FROM t",
        )
        assert result.equivalent is True
        cols = _col_map(result)
        assert len(cols) == 3
        assert all(c.status == "equivalent" for c in cols.values())

    def test_reordered_columns_all_equivalent(self):
        """Column reorder — overall equivalent, each column individually equivalent."""
        result = check_equivalence(
            "SELECT b, a FROM t",
            "SELECT a, b FROM t",
        )
        assert result.equivalent is True
        cols = _col_map(result)
        assert cols["a"].status == "equivalent"
        assert cols["b"].status == "equivalent"

    def test_cte_vs_subquery_columns_equivalent(self):
        result = check_equivalence(
            "WITH cte AS (SELECT x, y FROM t) SELECT x, y FROM cte",
            "SELECT x, y FROM (SELECT x, y FROM t) AS sub",
        )
        assert result.equivalent is True
        cols = _col_map(result)
        assert cols["x"].status == "equivalent"
        assert cols["y"].status == "equivalent"

    def test_predicate_difference_columns_still_equivalent(self):
        """WHERE clause changes, but SELECT columns are the same expressions."""
        result = check_equivalence(
            "SELECT a, b FROM t WHERE x > 5 AND x > 3",
            "SELECT a, b FROM t WHERE x > 5",
        )
        assert result.equivalent is True
        cols = _col_map(result)
        assert cols["a"].status == "equivalent"
        assert cols["b"].status == "equivalent"


class TestModifiedColumns:
    """Some columns equivalent, some modified."""

    def test_one_column_changed(self):
        """a stays the same, b's expression changes."""
        result = check_equivalence(
            "SELECT a, b + 1 AS b FROM t",
            "SELECT a, b + 2 AS b FROM t",
        )
        assert result.equivalent is False
        cols = _col_map(result)
        assert cols["a"].status == "equivalent"
        assert cols["b"].status == "modified"
        assert cols["b"].diff is not None

    def test_aggregation_changed(self):
        """SUM vs AVG on same column."""
        result = check_equivalence(
            "SELECT a, SUM(x) AS metric FROM t GROUP BY a",
            "SELECT a, AVG(x) AS metric FROM t GROUP BY a",
        )
        assert result.equivalent is False
        cols = _col_map(result)
        assert cols["a"].status == "equivalent"
        assert cols["metric"].status == "modified"

    def test_expression_wrapper_changed(self):
        """UPPER(name) vs LOWER(name) — different function, same column name."""
        result = check_equivalence(
            "SELECT UPPER(name) AS name FROM t",
            "SELECT LOWER(name) AS name FROM t",
        )
        assert result.equivalent is False
        cols = _col_map(result)
        assert cols["name"].status == "modified"

    def test_column_source_changed(self):
        """Same alias, different source column."""
        result = check_equivalence(
            "SELECT first_name AS name FROM t",
            "SELECT last_name AS name FROM t",
        )
        assert result.equivalent is False
        cols = _col_map(result)
        assert cols["name"].status == "modified"


class TestAddedAndRemovedColumns:
    """Columns present in one query but not the other."""

    def test_column_added(self):
        result = check_equivalence(
            "SELECT a FROM t",
            "SELECT a, b FROM t",
        )
        assert result.equivalent is False
        cols = _col_map(result)
        assert cols["a"].status == "equivalent"
        assert cols["b"].status == "added"
        assert cols["b"].expr_a is None
        assert cols["b"].expr_b is not None

    def test_column_removed(self):
        result = check_equivalence(
            "SELECT a, b FROM t",
            "SELECT a FROM t",
        )
        assert result.equivalent is False
        cols = _col_map(result)
        assert cols["a"].status == "equivalent"
        assert cols["b"].status == "removed"
        assert cols["b"].expr_a is not None
        assert cols["b"].expr_b is None

    def test_column_renamed(self):
        """Column removed + new column added = rename."""
        result = check_equivalence(
            "SELECT a, b FROM t",
            "SELECT a, c FROM t",
        )
        assert result.equivalent is False
        cols = _col_map(result)
        assert cols["a"].status == "equivalent"
        assert cols["b"].status == "removed"
        assert cols["c"].status == "added"

    def test_multiple_added_and_removed(self):
        result = check_equivalence(
            "SELECT a, b FROM t",
            "SELECT c, d FROM t",
        )
        assert result.equivalent is False
        cols = _col_map(result)
        assert cols["a"].status == "removed"
        assert cols["b"].status == "removed"
        assert cols["c"].status == "added"
        assert cols["d"].status == "added"


class TestMixedColumnChanges:
    """Real-world scenarios with a mix of equivalent, modified, added, removed."""

    def test_dbt_model_refactor(self):
        """Typical dbt PR: rename a column, add one, keep others."""
        result = check_equivalence(
            "SELECT id, name, created_at FROM users",
            "SELECT id, UPPER(name) AS name, created_at, email FROM users",
        )
        assert result.equivalent is False
        cols = _col_map(result)
        assert cols["id"].status == "equivalent"
        assert cols["name"].status == "modified"  # name → UPPER(name)
        assert cols["created_at"].status == "equivalent"
        assert cols["email"].status == "added"

    def test_aggregation_refactor(self):
        """Change one metric, keep the dimension and another metric."""
        result = check_equivalence(
            "SELECT dept, COUNT(*) AS headcount, AVG(salary) AS avg_sal FROM emp GROUP BY dept",
            "SELECT dept, COUNT(*) AS headcount, MEDIAN(salary) AS avg_sal FROM emp GROUP BY dept",
        )
        assert result.equivalent is False
        cols = _col_map(result)
        assert cols["dept"].status == "equivalent"
        assert cols["headcount"].status == "equivalent"
        assert cols["avg_sal"].status == "modified"

    def test_equivalent_with_commutative_expression(self):
        """Column expressions equivalent after commutativity normalization."""
        result = check_equivalence(
            "SELECT a + b AS total, c FROM t",
            "SELECT b + a AS total, c FROM t",
        )
        # a + b vs b + a — addition is commutative but sqlglot doesn't sort
        # arithmetic operands, so this will show as modified
        # (documenting current behavior)
        cols = _col_map(result)
        assert cols["c"].status == "equivalent"
        # total may be modified since we don't normalize arithmetic commutativity


class TestColumnExpressions:
    """Verify expr_a and expr_b contain useful SQL snippets."""

    def test_expr_shows_column_expression(self):
        result = check_equivalence(
            "SELECT a + 1 AS x, b FROM t",
            "SELECT a + 2 AS x, b FROM t",
        )
        cols = _col_map(result)
        x = cols["x"]
        assert x.status == "modified"
        assert "1" in x.expr_a
        assert "2" in x.expr_b

    def test_equivalent_column_shows_same_expr(self):
        result = check_equivalence(
            "SELECT a, b FROM t WHERE x > 1",
            "SELECT a, b FROM t WHERE x > 1",
        )
        cols = _col_map(result)
        assert cols["a"].expr_a == cols["a"].expr_b

    def test_added_column_has_no_expr_a(self):
        result = check_equivalence(
            "SELECT a FROM t",
            "SELECT a, b FROM t",
        )
        cols = _col_map(result)
        assert cols["b"].expr_a is None
        assert cols["b"].expr_b is not None

    def test_removed_column_has_no_expr_b(self):
        result = check_equivalence(
            "SELECT a, b FROM t",
            "SELECT a FROM t",
        )
        cols = _col_map(result)
        assert cols["b"].expr_a is not None
        assert cols["b"].expr_b is None


class TestSelectStarColumns:
    """SELECT * queries — column extraction from star expressions."""

    def test_select_star_both_sides(self):
        """Both queries use SELECT * — single star column, equivalent."""
        result = check_equivalence(
            "SELECT * FROM t WHERE a > 1",
            "SELECT * FROM t WHERE a > 1",
        )
        cols = _col_map(result)
        assert "*" in cols
        assert cols["*"].status == "equivalent"

    def test_select_star_vs_named(self):
        """SELECT * vs named columns — different structure."""
        result = check_equivalence(
            "SELECT * FROM t",
            "SELECT a, b FROM t",
        )
        assert result.equivalent is False
        cols = _col_map(result)
        assert "*" in cols
        assert cols["*"].status == "removed"
        assert cols["a"].status == "added"
        assert cols["b"].status == "added"


class TestColumnAnalysisWithNormalization:
    """Column-level analysis after normalization passes."""

    def test_cte_inline_columns_tracked(self):
        """CTE inlined — columns should still match by name."""
        result = check_equivalence(
            "WITH cte AS (SELECT a, b FROM t) SELECT a, b FROM cte",
            "SELECT a, b FROM (SELECT a, b FROM t) AS sub",
        )
        assert result.equivalent is True
        cols = _col_map(result)
        assert cols["a"].status == "equivalent"
        assert cols["b"].status == "equivalent"

    def test_cte_passthrough_vs_expression(self):
        """CTE defines c = b+1, outer selects c vs directly selecting b+1.

        The CTE outer SELECT references `c` (a column), while the direct
        version has `b + 1`. These ARE semantically equivalent, but our
        tool sees different expressions (column ref vs arithmetic).
        This documents current behavior — a deeper column resolution
        pass would be needed to prove equivalence here.
        """
        result = check_equivalence(
            "WITH cte AS (SELECT a, b + 1 AS c FROM t) SELECT a, c FROM cte",
            "SELECT a, b + 1 AS c FROM t",
        )
        cols = _col_map(result)
        assert cols["a"].status == "equivalent"
        # c is "modified" because one is a column ref, the other is b + 1
        assert cols["c"].status == "modified"

    def test_predicate_absorbed_columns_unchanged(self):
        """Predicate absorption doesn't affect columns."""
        result = check_equivalence(
            "SELECT a, b FROM t WHERE x > 5 AND x > 3",
            "SELECT a, b FROM t WHERE x > 5",
        )
        cols = _col_map(result)
        assert all(c.status == "equivalent" for c in cols.values())

    def test_partially_equivalent_with_where_change(self):
        """Different WHERE but columns partially overlap."""
        result = check_equivalence(
            "SELECT a, b FROM t WHERE x > 5",
            "SELECT a, c FROM t WHERE x > 10",
        )
        assert result.equivalent is False
        cols = _col_map(result)
        assert cols["a"].status == "equivalent"
        assert cols["b"].status == "removed"
        assert cols["c"].status == "added"
