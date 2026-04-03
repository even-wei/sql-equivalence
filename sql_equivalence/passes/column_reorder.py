from __future__ import annotations

import sqlglot.expressions as exp

from sql_equivalence.models import RewritePass, RewriteStep


class ColumnReorderPass(RewritePass):
    name = "column_reorder"

    def apply(self, expression: exp.Expression) -> tuple[exp.Expression, list[RewriteStep]]:
        tree = expression.copy()
        steps: list[RewriteStep] = []

        for select in tree.find_all(exp.Select):
            self._reorder_select(select, steps)

        return tree, steps

    def _has_positional_refs(self, select: exp.Select) -> bool:
        """Check if ORDER BY, GROUP BY, or HAVING use positional integer references."""
        for clause_key in ("order", "group"):
            clause = select.args.get(clause_key)
            if clause is None:
                continue
            for lit in clause.find_all(exp.Literal):
                if lit.is_int:
                    return True
        return False

    def _reorder_select(self, select: exp.Select, steps: list[RewriteStep]) -> None:
        projections = select.expressions
        # Skip SELECT * or single column
        if len(projections) <= 1:
            return
        if any(isinstance(p, exp.Star) for p in projections):
            return

        if self._has_positional_refs(select):
            return

        sorted_projections = sorted(projections, key=lambda p: (p.alias_or_name or "").lower())

        # Check if order actually changed
        if [id(p) for p in projections] == [id(p) for p in sorted_projections]:
            return

        before_sql = select.sql()
        select.set("expressions", sorted_projections)
        after_sql = select.sql()

        steps.append(
            RewriteStep(
                pass_name=self.name,
                description="Reordered SELECT columns alphabetically",
                before_sql=before_sql,
                after_sql=after_sql,
            )
        )
