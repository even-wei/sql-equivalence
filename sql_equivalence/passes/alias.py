from __future__ import annotations

from typing import Dict

import sqlglot.expressions as exp

from sql_equivalence.models import RewritePass, RewriteStep


class AliasNormalizationPass(RewritePass):
    name = "alias_normalization"

    def apply(self, expression: exp.Expression) -> tuple[exp.Expression, list[RewriteStep]]:
        tree = expression.copy()
        steps: list[RewriteStep] = []

        # Mapping from old name -> new canonical name
        rename_map: Dict[str, str] = {}
        subquery_counter = 0
        cte_counter = 0

        # Collect root SELECT column aliases to preserve
        root_column_aliases: set[str] = set()
        if isinstance(tree, exp.Select):
            for expr in tree.expressions:
                if isinstance(expr, exp.Alias):
                    root_column_aliases.add(expr.alias)

        # Phase 1: Rename CTE definitions and build rename map
        for cte_node in tree.find_all(exp.CTE):
            old_name = cte_node.alias
            if old_name and old_name not in root_column_aliases:
                new_name = f"_cte{cte_counter}"
                cte_counter += 1
                rename_map[old_name] = new_name

        # Phase 2: Rename subquery aliases (depth-first, left-to-right via find_all)
        for subquery in tree.find_all(exp.Subquery):
            old_alias = subquery.alias
            if old_alias and old_alias not in root_column_aliases:
                new_name = f"_t{subquery_counter}"
                subquery_counter += 1
                rename_map[old_alias] = new_name

        if not rename_map:
            return tree, steps

        before_sql = tree.sql()

        # Phase 3: Apply all renames throughout the tree

        # Rename CTE definitions
        for cte_node in tree.find_all(exp.CTE):
            old_name = cte_node.alias
            if old_name in rename_map:
                # CTE alias is stored in the alias property via the TableAlias child
                table_alias = cte_node.args.get("alias")
                if isinstance(table_alias, exp.TableAlias):
                    table_alias.set("this", exp.to_identifier(rename_map[old_name]))

        # Rename subquery aliases
        for subquery in tree.find_all(exp.Subquery):
            old_alias = subquery.alias
            if old_alias in rename_map:
                table_alias = subquery.args.get("alias")
                if isinstance(table_alias, exp.TableAlias):
                    table_alias.set("this", exp.to_identifier(rename_map[old_alias]))

        # Rename table references (FROM, JOIN clauses referencing CTEs/subqueries)
        for table in tree.find_all(exp.Table):
            table_name = table.name
            if table_name in rename_map:
                table.set("this", exp.to_identifier(rename_map[table_name]))

        # Rename column references (e.g., src.x -> _cte0.x)
        for column in tree.find_all(exp.Column):
            if column.table and column.table in rename_map:
                column.set("table", exp.to_identifier(rename_map[column.table]))

        after_sql = tree.sql()

        for old_name, new_name in rename_map.items():
            steps.append(
                RewriteStep(
                    pass_name=self.name,
                    description=f"Renamed alias '{old_name}' to '{new_name}'",
                    before_sql=before_sql,
                    after_sql=after_sql,
                )
            )

        return tree, steps
