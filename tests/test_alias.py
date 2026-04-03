"""Tests for AliasNormalizationPass.

Canonicalizes internal aliases (subquery aliases, CTE names) to deterministic
names (_t0, _t1, _cte0, _cte1) while preserving output-facing column aliases.
"""

import pytest
from sqlglot import parse_one

from sql_equivalence.passes.alias import AliasNormalizationPass


@pytest.fixture
def alias_pass():
    return AliasNormalizationPass()


class TestSubqueryAliases:
    """Internal subquery aliases should be canonicalized."""

    def test_single_subquery_alias_renamed(self, alias_pass):
        sql = "SELECT * FROM (SELECT 1 AS x) AS foo"
        expr, steps = alias_pass.apply(parse_one(sql))
        result = expr.sql()
        assert "_t0" in result
        assert "foo" not in result
        assert len(steps) >= 1

    def test_different_aliases_same_structure_normalize_equally(self, alias_pass):
        sql_a = "SELECT * FROM (SELECT 1 AS x) AS foo"
        sql_b = "SELECT * FROM (SELECT 1 AS x) AS bar"
        expr_a, _ = alias_pass.apply(parse_one(sql_a))
        expr_b, _ = alias_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()

    def test_multiple_subqueries_get_sequential_names(self, alias_pass):
        sql = "SELECT * FROM (SELECT 1) AS a JOIN (SELECT 2) AS b ON 1=1"
        expr, steps = alias_pass.apply(parse_one(sql))
        result = expr.sql()
        assert "_t0" in result
        assert "_t1" in result

    def test_nested_subqueries(self, alias_pass):
        sql = "SELECT * FROM (SELECT * FROM (SELECT 1) AS inner_q) AS outer_q"
        expr, steps = alias_pass.apply(parse_one(sql))
        result = expr.sql()
        assert "inner_q" not in result
        assert "outer_q" not in result


class TestCTEAliases:
    """CTE names should be canonicalized."""

    def test_single_cte_renamed(self, alias_pass):
        sql = "WITH my_cte AS (SELECT 1 AS x) SELECT * FROM my_cte"
        expr, steps = alias_pass.apply(parse_one(sql))
        result = expr.sql()
        assert "my_cte" not in result
        assert "_cte0" in result

    def test_different_cte_names_normalize_equally(self, alias_pass):
        sql_a = "WITH alpha AS (SELECT 1 AS x) SELECT * FROM alpha"
        sql_b = "WITH beta AS (SELECT 1 AS x) SELECT * FROM beta"
        expr_a, _ = alias_pass.apply(parse_one(sql_a))
        expr_b, _ = alias_pass.apply(parse_one(sql_b))
        assert expr_a.sql() == expr_b.sql()

    def test_multiple_ctes_sequential(self, alias_pass):
        sql = """
        WITH cte_a AS (SELECT 1 AS x),
             cte_b AS (SELECT 2 AS y)
        SELECT * FROM cte_a JOIN cte_b ON 1=1
        """
        expr, steps = alias_pass.apply(parse_one(sql))
        result = expr.sql()
        assert "_cte0" in result
        assert "_cte1" in result

    def test_cte_references_updated(self, alias_pass):
        """References to renamed CTEs in the query body must also be updated."""
        sql = "WITH src AS (SELECT 1 AS x) SELECT src.x FROM src"
        expr, steps = alias_pass.apply(parse_one(sql))
        result = expr.sql()
        # The reference in SELECT and FROM should both use the new name
        assert "src" not in result


class TestOutputAliasesPreserved:
    """Output-facing column aliases in the root SELECT must NOT be renamed."""

    def test_root_select_alias_preserved(self, alias_pass):
        sql = "SELECT 1 AS my_output_col"
        expr, steps = alias_pass.apply(parse_one(sql))
        assert "my_output_col" in expr.sql()

    def test_root_select_multiple_aliases_preserved(self, alias_pass):
        sql = "SELECT a AS col1, b AS col2 FROM t"
        expr, steps = alias_pass.apply(parse_one(sql))
        result = expr.sql()
        assert "col1" in result
        assert "col2" in result


class TestRewriteSteps:
    """Each renaming should produce a RewriteStep."""

    def test_step_records_before_and_after(self, alias_pass):
        sql = "SELECT * FROM (SELECT 1) AS original_name"
        _, steps = alias_pass.apply(parse_one(sql))
        assert len(steps) >= 1
        step = steps[0]
        assert step.pass_name == "alias_normalization"
        assert "original_name" in step.before_sql or "original_name" in step.description

    def test_no_steps_when_nothing_to_rename(self, alias_pass):
        sql = "SELECT 1"
        _, steps = alias_pass.apply(parse_one(sql))
        assert len(steps) == 0
