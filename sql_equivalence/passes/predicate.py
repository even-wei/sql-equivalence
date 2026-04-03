from __future__ import annotations

from typing import Union

import sqlglot.expressions as exp

from sql_equivalence.models import RewritePass, RewriteStep

# Comparison types grouped by direction
_LOWER_BOUND_TYPES = (exp.GT, exp.GTE)  # x > N, x >= N
_UPPER_BOUND_TYPES = (exp.LT, exp.LTE)  # x < N, x <= N
_COMPARISON_TYPES = (*_LOWER_BOUND_TYPES, *_UPPER_BOUND_TYPES, exp.EQ)

ComparisonExpr = Union[exp.GT, exp.GTE, exp.LT, exp.LTE, exp.EQ]


def _is_boolean_true(node: exp.Expression) -> bool:
    return isinstance(node, exp.Boolean) and node.this


def _is_boolean_false(node: exp.Expression) -> bool:
    return isinstance(node, exp.Boolean) and not node.this


def _get_column_key(node: exp.Expression) -> str | None:
    """Return a string key for a Column expression, or None."""
    if isinstance(node, exp.Column):
        return node.sql()
    return None


def _get_numeric_value(node: exp.Expression) -> float | None:
    """Extract numeric value from a Literal, or None."""
    if isinstance(node, exp.Literal) and node.is_number:
        return float(node.this)
    return None


def _parse_comparison(node: exp.Expression) -> tuple[str, type, float] | None:
    """Parse a simple comparison into (column_key, comparison_type, numeric_value)."""
    if not isinstance(node, _COMPARISON_TYPES):
        return None
    col_key = _get_column_key(node.this)
    num_val = _get_numeric_value(node.expression)
    if col_key is not None and num_val is not None:
        return (col_key, type(node), num_val)
    return None


def _strictness_key(cmp_type: type, value: float) -> tuple[float, int]:
    """Return a sortable key where 'stricter lower bound' is greater.

    For lower bounds (GT, GTE): higher value = stricter, GT stricter than GTE at same value.
    For upper bounds (LT, LTE): lower value = stricter, LT stricter than LTE at same value.
    """
    if cmp_type in (exp.GT, exp.GTE):
        # Higher = stricter; GT is stricter than GTE at same value
        return (value, 1 if cmp_type is exp.GT else 0)
    else:
        # Lower = stricter; LT is stricter than LTE at same value
        # Negate so that lower value sorts higher
        return (-value, 1 if cmp_type is exp.LT else 0)


def _absorb_comparisons(operands: list[exp.Expression], is_and: bool) -> list[exp.Expression]:
    """Remove redundant comparisons from a list of AND/OR operands.

    For AND: keep the strictest bound per column per direction.
    For OR: keep the weakest bound per column per direction.
    """
    # Group comparisons by (column, direction)
    groups: dict[tuple[str, str], list[tuple[int, type, float]]] = {}
    for i, op in enumerate(operands):
        parsed = _parse_comparison(op)
        if parsed is None:
            continue
        col_key, cmp_type, value = parsed
        if cmp_type in _LOWER_BOUND_TYPES:
            direction = "lower"
        elif cmp_type in _UPPER_BOUND_TYPES:
            direction = "upper"
        else:
            continue  # skip EQ for absorption
        key = (col_key, direction)
        groups.setdefault(key, []).append((i, cmp_type, value))

    indices_to_remove: set[int] = set()
    for group_items in groups.values():
        if len(group_items) <= 1:
            continue
        # Sort by strictness
        sorted_items = sorted(group_items, key=lambda x: _strictness_key(x[1], x[2]))
        if is_and:
            # AND keeps the strictest (last after sort)
            keep_idx = sorted_items[-1][0]
        else:
            # OR keeps the weakest (first after sort)
            keep_idx = sorted_items[0][0]
        for idx, _, _ in sorted_items:
            if idx != keep_idx:
                indices_to_remove.add(idx)

    if not indices_to_remove:
        return operands
    return [op for i, op in enumerate(operands) if i not in indices_to_remove]


def _collect_operands(node: exp.Expression, target_type: type) -> list[exp.Expression]:
    """Flatten nested AND or OR into a list of operands."""
    result: list[exp.Expression] = []
    if isinstance(node, target_type):
        result.extend(_collect_operands(node.this, target_type))
        result.extend(_collect_operands(node.expression, target_type))
    else:
        result.append(node)
    return result


def _build_chain(operands: list[exp.Expression], node_type: type) -> exp.Expression:
    """Build a left-associative chain of AND/OR from operands."""
    result = operands[0]
    for op in operands[1:]:
        result = node_type(this=result, expression=op)
    return result


def _unwrap_parens(node: exp.Expression) -> exp.Expression:
    """Strip outer Paren wrappers."""
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _simplify_expression(node: exp.Expression) -> tuple[exp.Expression, bool]:
    """Recursively simplify a boolean expression. Returns (simplified, changed)."""
    changed = False

    # Unwrap parens for analysis
    inner = _unwrap_parens(node)

    # Double negation elimination
    if isinstance(inner, exp.Not):
        inner_child = _unwrap_parens(inner.this)
        if isinstance(inner_child, exp.Not):
            # NOT NOT x -> x
            result, sub_changed = _simplify_expression(inner_child.this)
            return result, True

    # De Morgan's: NOT (a AND b) -> (NOT a OR NOT b), NOT (a OR b) -> (NOT a AND NOT b)
    if isinstance(inner, exp.Not):
        inner_child = _unwrap_parens(inner.this)
        if isinstance(inner_child, (exp.And, exp.Or)):
            left = inner_child.this
            right = inner_child.expression
            not_left = exp.Not(this=left.copy())
            not_right = exp.Not(this=right.copy())
            if isinstance(inner_child, exp.And):
                new_node = exp.Or(this=not_left, expression=not_right)
            else:
                new_node = exp.And(this=not_left, expression=not_right)
            result, _ = _simplify_expression(new_node)
            return result, True

    # Recurse into NOT
    if isinstance(inner, exp.Not):
        child, sub_changed = _simplify_expression(inner.this)
        if sub_changed:
            return exp.Not(this=child), True
        return inner, False

    # AND / OR processing
    if isinstance(inner, (exp.And, exp.Or)):
        is_and = isinstance(inner, exp.And)
        target_type = exp.And if is_and else exp.Or

        # Flatten
        operands = _collect_operands(inner, target_type)
        flat_changed = len(operands) > 2  # flattening happened if > 2

        # Recursively simplify each operand
        simplified_operands: list[exp.Expression] = []
        for op in operands:
            s_op, s_changed = _simplify_expression(op)
            if s_changed:
                changed = True
            simplified_operands.append(s_op)

        # Identity / contradiction elimination
        filtered: list[exp.Expression] = []
        for op in simplified_operands:
            unwrapped = _unwrap_parens(op)
            if is_and:
                if _is_boolean_true(unwrapped):
                    changed = True
                    continue  # AND TRUE is identity
                if _is_boolean_false(unwrapped):
                    return exp.Boolean(this=False), True  # AND FALSE is contradiction
            else:
                if _is_boolean_false(unwrapped):
                    changed = True
                    continue  # OR FALSE is identity
                if _is_boolean_true(unwrapped):
                    return exp.Boolean(this=True), True  # OR TRUE is domination
            filtered.append(op)

        if not filtered:
            # All were identity elements
            return exp.Boolean(this=is_and), True

        # Absorption
        absorbed = _absorb_comparisons(filtered, is_and)
        if len(absorbed) < len(filtered):
            changed = True
            filtered = absorbed

        if flat_changed:
            changed = True

        if len(filtered) == 1:
            return filtered[0], True

        result = _build_chain(filtered, target_type)
        return result, changed

    # No transformation applicable
    return inner, False


class PredicateSimplificationPass(RewritePass):
    name = "predicate_simplification"

    def apply(self, expression: exp.Expression) -> tuple[exp.Expression, list[RewriteStep]]:
        result = expression.copy()
        steps: list[RewriteStep] = []

        # Find all predicate-bearing clauses
        for clause in result.find_all(exp.Where, exp.Having):
            before_sql = clause.this.sql()
            simplified, changed = _simplify_expression(clause.this.copy())
            if changed:
                clause.set("this", simplified)
                steps.append(
                    RewriteStep(
                        pass_name=self.name,
                        description=f"Simplified predicate: {before_sql} -> {simplified.sql()}",
                        before_sql=before_sql,
                        after_sql=simplified.sql(),
                    )
                )

        # Handle JOIN ON conditions
        for join in result.find_all(exp.Join):
            on_expr = join.args.get("on")
            if on_expr is not None:
                before_sql = on_expr.sql()
                simplified, changed = _simplify_expression(on_expr.copy())
                if changed:
                    join.set("on", simplified)
                    steps.append(
                        RewriteStep(
                            pass_name=self.name,
                            description=f"Simplified predicate: {before_sql} -> {simplified.sql()}",
                            before_sql=before_sql,
                            after_sql=simplified.sql(),
                        )
                    )

        return result, steps
