"""AST diff utility for finding divergence points between two normalized SQL expressions."""

from __future__ import annotations

import sqlglot.expressions as exp


def ast_diff(expr_a: exp.Expression, expr_b: exp.Expression) -> str | None:
    """Walk two ASTs in parallel and return a human-readable description of the first divergence.

    Returns None if the trees are identical.
    """
    diff = _find_divergence(expr_a, expr_b, depth=0)
    if diff is None:
        return None
    return diff


def _find_divergence(a: exp.Expression, b: exp.Expression, depth: int) -> str | None:
    if a is None and b is None:
        return None
    if a is None:
        return f"Left is missing, right has: {b.sql()}"
    if b is None:
        return f"Right is missing, left has: {a.sql()}"

    if type(a) != type(b):
        return (
            f"Different node types at depth {depth}: "
            f"{type(a).__name__} ({a.sql()}) vs {type(b).__name__} ({b.sql()})"
        )

    # For leaf nodes (literals, identifiers), compare directly
    if isinstance(a, (exp.Literal, exp.Identifier, exp.Boolean)):
        if a.sql() != b.sql():
            return f"Different values: {a.sql()} vs {b.sql()}"
        return None

    # Compare args recursively
    all_keys = set(a.args.keys()) | set(b.args.keys())
    for key in sorted(all_keys):
        val_a = a.args.get(key)
        val_b = b.args.get(key)

        if val_a is None and val_b is None:
            continue

        if isinstance(val_a, exp.Expression) and isinstance(val_b, exp.Expression):
            diff = _find_divergence(val_a, val_b, depth + 1)
            if diff:
                return diff
        elif isinstance(val_a, list) and isinstance(val_b, list):
            if len(val_a) != len(val_b):
                return (
                    f"Different number of {key} elements: "
                    f"{len(val_a)} vs {len(val_b)} in {type(a).__name__}"
                )
            for i, (item_a, item_b) in enumerate(zip(val_a, val_b)):
                if isinstance(item_a, exp.Expression) and isinstance(item_b, exp.Expression):
                    diff = _find_divergence(item_a, item_b, depth + 1)
                    if diff:
                        return diff
                elif item_a != item_b:
                    return f"Different {key}[{i}]: {item_a} vs {item_b}"
        elif val_a != val_b:
            # Scalar args
            if val_a is not None and val_b is not None:
                return f"Different {key} in {type(a).__name__}: {val_a} vs {val_b}"
            elif val_a is not None:
                return f"Left has {key}={val_a} in {type(a).__name__}, right does not"
            else:
                return f"Right has {key}={val_b} in {type(b).__name__}, left does not"

    return None
