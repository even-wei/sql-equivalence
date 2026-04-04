"""Enhanced breaking change analysis with semantic equivalence.

Wraps a structural AST diff with semantic normalization to reduce
false positives. When structural analysis says "breaking", semantic
analysis may refine it to "semantically_equivalent".

Also detects column renames by matching removed/added columns with
identical normalized expressions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import sqlglot.expressions as exp
from sqlglot import Dialect, parse_one
from sqlglot.errors import SqlglotError

from sql_equivalence.pipeline import check_equivalence

SemanticStatus = Literal[
    "equivalent",       # code changed but semantics unchanged
    "modified",         # semantics actually changed
    "renamed",          # column was renamed
    "added",            # new column
    "removed",          # column removed
]

ChangeCategory = Literal[
    "breaking",
    "non_breaking",
    "partial_breaking",
    "semantically_equivalent",
    "unknown",
]


@dataclass(frozen=True)
class ColumnChange:
    """Semantic analysis result for a single output column."""
    name: str
    status: SemanticStatus
    old_expr: str | None = None
    new_expr: str | None = None
    rename_from: str | None = None


@dataclass(frozen=True)
class EnhancedNodeChange:
    """Enhanced change analysis combining structural + semantic results."""
    structural_category: str
    semantic_category: ChangeCategory
    columns: list[ColumnChange] = field(default_factory=list)

    @property
    def category(self) -> ChangeCategory:
        return self.semantic_category


def _detect_renames(columns: list[ColumnChange]) -> list[ColumnChange]:
    """Detect renames: a removed column + an added column with the same expression."""
    removed = {c.name: c for c in columns if c.status == "removed" and c.old_expr}
    added = {c.name: c for c in columns if c.status == "added" and c.new_expr}

    renames: dict[str, str] = {}  # added_name → removed_name
    for add_name, add_col in added.items():
        for rm_name, rm_col in removed.items():
            if rm_name in renames.values():
                continue
            if rm_col.old_expr == add_col.new_expr:
                renames[add_name] = rm_name
                break

    if not renames:
        return columns

    matched_removed = set(renames.values())
    matched_added = set(renames.keys())
    result = []
    for col in columns:
        if col.name in matched_removed:
            continue
        if col.name in matched_added:
            result.append(ColumnChange(
                name=col.name, status="renamed",
                old_expr=col.new_expr, new_expr=col.new_expr,
                rename_from=renames[col.name],
            ))
        else:
            result.append(col)
    return result


def _structural_classify(old_sql: str, new_sql: str, dialect: str | None) -> str:
    """Quick structural classification without semantic analysis.

    Returns: 'identical', 'breaking', 'partial_breaking', 'non_breaking', 'unknown'.
    """
    if old_sql.strip() == new_sql.strip():
        return "identical"

    try:
        d = Dialect.get(dialect) if dialect else None
        old_expr = parse_one(old_sql, dialect=d)
        new_expr = parse_one(new_sql, dialect=d)
    except SqlglotError:
        return "unknown"

    if old_expr.sql() == new_expr.sql():
        return "identical"

    # Has actual code differences — need further analysis
    return "changed"


def analyze_change(
    old_sql: str,
    new_sql: str,
    dialect: str | None = None,
) -> EnhancedNodeChange:
    """Analyze a SQL change with both structural and semantic analysis.

    Strategy:
    1. Quick structural check — if identical, return non_breaking immediately
    2. Run full semantic equivalence check
    3. If fully equivalent → semantically_equivalent
    4. If not → classify per-column, detect renames, determine category
    """
    # Step 1: Quick structural check
    structural = _structural_classify(old_sql, new_sql, dialect)

    if structural == "identical":
        return EnhancedNodeChange(
            structural_category="non_breaking",
            semantic_category="non_breaking",
            columns=[],
        )

    if structural == "unknown":
        return EnhancedNodeChange(
            structural_category="unknown",
            semantic_category="unknown",
            columns=[],
        )

    # Step 2: Full semantic equivalence check
    equiv = check_equivalence(old_sql, new_sql, dialect=dialect)

    if equiv.equivalent:
        return EnhancedNodeChange(
            structural_category="changed",
            semantic_category="semantically_equivalent",
            columns=[],
        )

    # Step 3: Not fully equivalent — do per-column analysis
    col_changes: list[ColumnChange] = []
    has_non_select_diff = False

    # Check if there are non-column differences (WHERE, JOIN, GROUP BY, etc.)
    # by comparing overall equivalence vs column-level equivalence
    col_statuses = set()
    for col in equiv.columns:
        if col.status == "equivalent":
            col_changes.append(ColumnChange(
                name=col.name, status="equivalent",
                old_expr=col.expr_a, new_expr=col.expr_b,
            ))
        elif col.status == "modified":
            col_changes.append(ColumnChange(
                name=col.name, status="modified",
                old_expr=col.expr_a, new_expr=col.expr_b,
            ))
        elif col.status == "added":
            col_changes.append(ColumnChange(
                name=col.name, status="added",
                new_expr=col.expr_b,
            ))
        elif col.status == "removed":
            col_changes.append(ColumnChange(
                name=col.name, status="removed",
                old_expr=col.expr_a,
            ))
        col_statuses.add(col.status)

    # If all columns are equivalent but overall query isn't,
    # then non-SELECT clauses changed (WHERE, JOIN, ORDER BY, etc.) → breaking
    all_cols_equiv = col_statuses <= {"equivalent"}
    if all_cols_equiv and not equiv.equivalent:
        has_non_select_diff = True

    # Step 4: Detect renames
    col_changes = _detect_renames(col_changes)

    # Step 5: Determine final category
    final_statuses = {c.status for c in col_changes}

    if has_non_select_diff:
        # Non-SELECT clause changed (WHERE, JOIN, etc.) → breaking
        semantic_cat: ChangeCategory = "breaking"
    elif final_statuses <= {"equivalent", "renamed"}:
        semantic_cat = "semantically_equivalent"
    elif final_statuses <= {"equivalent", "added", "renamed"}:
        semantic_cat = "non_breaking"
    elif "removed" in final_statuses or "modified" in final_statuses:
        semantic_cat = "partial_breaking"
    else:
        semantic_cat = "unknown"

    return EnhancedNodeChange(
        structural_category="changed",
        semantic_category=semantic_cat,
        columns=col_changes,
    )
