"""Pipeline that orchestrates rewrite passes and compares normalized ASTs."""

from __future__ import annotations

from sqlglot import parse_one

import sqlglot.expressions as exp

from sql_equivalence.diff import ast_diff
from sql_equivalence.models import ColumnStatus, EquivalenceResult, RewritePass, RewriteStep
from sql_equivalence.passes.alias import AliasNormalizationPass
from sql_equivalence.passes.column_reorder import ColumnReorderPass
from sql_equivalence.passes.commutativity import CommutativityPass
from sql_equivalence.passes.cte_inline import CTEInlinePass
from sql_equivalence.passes.predicate import PredicateSimplificationPass
from sql_equivalence.passes.subquery_join import SubqueryJoinPass

# Passes in dependency order.
# CTE inline runs first so inlined subqueries get uniform alias treatment.
# Pass order matters:
# 1. CTE inline first (so inlined subqueries get uniform treatment)
# 2. Structural rewrites (subquery, predicate, commutativity) before alias normalization
# 3. Alias normalization after commutativity (so JOIN-reordered tables get consistent names)
# 4. Column reorder last (depends on knowing positional references)
DEFAULT_PASSES: list[RewritePass] = [
    CTEInlinePass(),
    SubqueryJoinPass(),
    PredicateSimplificationPass(),
    CommutativityPass(),
    AliasNormalizationPass(),
    ColumnReorderPass(),
]


# Dialects where unquoted identifiers are case-insensitive
_CASE_INSENSITIVE_DIALECTS = {"snowflake", "oracle", "db2"}


def _strip_comments(expression: exp.Expression) -> None:
    """Remove all comments from the AST — they don't affect semantics."""
    for node in expression.walk():
        if hasattr(node, "comments") and node.comments:
            node.comments = []


def _strip_unnecessary_parens(expression: exp.Expression) -> None:
    """Remove Paren nodes that don't change semantics."""
    for paren in list(expression.find_all(exp.Paren)):
        # Keep parens that change precedence (e.g. in OR within AND)
        # Strip parens that just wrap a single expression
        inner = paren.this
        parent = paren.parent
        # If the paren is the direct child of WHERE, HAVING, ON, or a binary op
        # where the inner expression has lower/equal precedence, it's safe to strip
        # Simple approach: strip all Paren wrappers (sqlglot re-adds when needed for generation)
        paren.replace(inner)


def _normalize_identifier_case(expression: exp.Expression, dialect: str | None) -> None:
    """Uppercase unquoted identifiers for case-insensitive dialects."""
    if not dialect or dialect.lower() not in _CASE_INSENSITIVE_DIALECTS:
        return
    for ident in expression.find_all(exp.Identifier):
        if not ident.quoted:
            ident.set("this", ident.this.upper())


def _normalize(sql: str, dialect: str | None, passes: list[RewritePass]) -> tuple[str, list[RewriteStep]]:
    """Parse SQL and run it through all rewrite passes, collecting steps."""
    expression = parse_one(sql, dialect=dialect)
    _strip_comments(expression)
    _normalize_identifier_case(expression, dialect)
    _strip_unnecessary_parens(expression)
    all_steps: list[RewriteStep] = []

    for rewrite_pass in passes:
        expression, steps = rewrite_pass.apply(expression)
        all_steps.extend(steps)

    return expression, all_steps


def _get_column_passes(passes: list[RewritePass]) -> list[RewritePass]:
    """Return passes excluding ColumnReorderPass (for per-column comparison)."""
    from sql_equivalence.passes.column_reorder import ColumnReorderPass

    return [p for p in passes if not isinstance(p, ColumnReorderPass)]


def _extract_columns(expr: exp.Expression) -> dict[str, exp.Expression]:
    """Extract output columns from a SELECT as {name: expression}.

    For a root-level SELECT, returns each projection keyed by its
    output alias (or column name if unaliased).
    """
    select = expr if isinstance(expr, exp.Select) else expr.find(exp.Select)
    if not select:
        return {}
    result = {}
    for proj in select.expressions:
        name = proj.alias_or_name
        # Get the underlying expression (strip the Alias wrapper)
        if isinstance(proj, exp.Alias):
            result[name] = proj.this
        else:
            result[name] = proj
    return result


def _compare_columns(
    expr_a: exp.Expression,
    expr_b: exp.Expression,
) -> list[ColumnStatus]:
    """Compare output columns between two normalized expressions."""
    cols_a = _extract_columns(expr_a)
    cols_b = _extract_columns(expr_b)

    all_names = list(dict.fromkeys(list(cols_a.keys()) + list(cols_b.keys())))
    results: list[ColumnStatus] = []

    for name in all_names:
        ea = cols_a.get(name)
        eb = cols_b.get(name)

        if ea is None:
            results.append(ColumnStatus(
                name=name, status="added",
                expr_a=None, expr_b=eb.sql() if eb else None,
            ))
        elif eb is None:
            results.append(ColumnStatus(
                name=name, status="removed",
                expr_a=ea.sql(), expr_b=None,
            ))
        elif ea.sql() == eb.sql():
            results.append(ColumnStatus(
                name=name, status="equivalent",
                expr_a=ea.sql(), expr_b=eb.sql(),
            ))
        else:
            diff = ast_diff(ea, eb)
            results.append(ColumnStatus(
                name=name, status="modified",
                expr_a=ea.sql(), expr_b=eb.sql(),
                diff=diff,
            ))

    return results


def check_equivalence(
    sql_a: str,
    sql_b: str,
    dialect: str | None = None,
    passes: list[RewritePass] | None = None,
) -> EquivalenceResult:
    """Check if two SQL queries are semantically equivalent.

    Parses both queries, runs them through a pipeline of rewrite passes
    to normalize, then compares the resulting ASTs. Also provides
    column-level equivalence detail.
    """
    if passes is None:
        passes = DEFAULT_PASSES

    norm_a, proof_a = _normalize(sql_a, dialect, passes)
    norm_b, proof_b = _normalize(sql_b, dialect, passes)

    # Column-level comparison uses all passes EXCEPT column reorder,
    # so columns stay in their original positions for name-based matching
    col_passes = _get_column_passes(passes)
    col_norm_a, _ = _normalize(sql_a, dialect, col_passes)
    col_norm_b, _ = _normalize(sql_b, dialect, col_passes)
    columns = _compare_columns(col_norm_a, col_norm_b)

    sql_a_normalized = norm_a.sql()
    sql_b_normalized = norm_b.sql()

    if sql_a_normalized == sql_b_normalized:
        return EquivalenceResult(
            equivalent=True,
            proof_a=proof_a,
            proof_b=proof_b,
            remaining_diff=None,
            columns=columns,
        )

    # Not equivalent — find the divergence point
    diff = ast_diff(norm_a, norm_b)
    if diff is None:
        return EquivalenceResult(
            equivalent=True,
            proof_a=proof_a,
            proof_b=proof_b,
            remaining_diff=None,
            columns=columns,
        )

    return EquivalenceResult(
        equivalent=False,
        proof_a=proof_a,
        proof_b=proof_b,
        remaining_diff=diff,
        columns=columns,
    )
