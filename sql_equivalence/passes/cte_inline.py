from __future__ import annotations

import sqlglot.expressions as exp

from sql_equivalence.models import RewritePass, RewriteStep


def _count_table_refs(node: exp.Expression, cte_name: str) -> int:
    """Count how many times a CTE name is referenced as a table in the expression."""
    count = 0
    for table in node.find_all(exp.Table):
        if table.name.lower() == cte_name.lower():
            count += 1
    return count


class CTEInlinePass(RewritePass):
    name = "cte_inline"

    def apply(self, expression: exp.Expression) -> tuple[exp.Expression, list[RewriteStep]]:
        tree = expression.copy()
        all_steps: list[RewriteStep] = []

        # Iterate until no more single-use CTEs can be inlined
        # (handles chained CTEs like step2 -> step1)
        for _ in range(10):  # safety limit
            steps = self._inline_pass(tree)
            if not steps:
                break
            all_steps.extend(steps)

        return tree, all_steps

    def _inline_pass(self, tree: exp.Expression) -> list[RewriteStep]:
        """Run one pass of CTE inlining. Returns steps taken."""
        with_clause = tree.find(exp.With)
        if not with_clause:
            return []

        is_recursive = with_clause.args.get("recursive", False)
        ctes: list[exp.CTE] = list(with_clause.find_all(exp.CTE))
        if not ctes:
            return []

        steps: list[RewriteStep] = []
        ctes_to_keep: list[exp.CTE] = []

        for cte in ctes:
            cte_name = cte.alias
            cte_body = cte.this

            if is_recursive:
                ctes_to_keep.append(cte)
                continue

            # Count references outside CTE definitions
            ref_count = _count_table_refs(tree, cte_name)
            for other_cte in ctes:
                ref_count -= _count_table_refs(other_cte, cte_name)

            if ref_count == 1:
                before_sql = tree.sql()

                for table in tree.find_all(exp.Table):
                    if table.name.lower() == cte_name.lower():
                        parent = table.parent
                        inside_cte = False
                        while parent:
                            if isinstance(parent, exp.CTE):
                                inside_cte = True
                                break
                            parent = parent.parent
                        if inside_cte:
                            continue

                        subquery = exp.Subquery(
                            this=cte_body.copy(),
                            alias=exp.TableAlias(this=exp.to_identifier(cte_name)),
                        )
                        table.replace(subquery)
                        break

                after_sql = tree.sql()
                steps.append(
                    RewriteStep(
                        pass_name=self.name,
                        description=f"Inlined single-use CTE '{cte_name}' as subquery",
                        before_sql=before_sql,
                        after_sql=after_sql,
                    )
                )
            else:
                ctes_to_keep.append(cte)

        if not ctes_to_keep:
            with_clause.pop()
        else:
            with_clause.set("expressions", ctes_to_keep)

        return steps
