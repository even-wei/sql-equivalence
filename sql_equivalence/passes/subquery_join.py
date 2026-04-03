from __future__ import annotations

import sqlglot.expressions as exp

from sql_equivalence.models import RewritePass, RewriteStep


class SubqueryJoinPass(RewritePass):
    name = "subquery_join"

    def apply(self, expression: exp.Expression) -> tuple[exp.Expression, list[RewriteStep]]:
        tree = expression.copy()
        steps: list[RewriteStep] = []

        # Process IN / NOT IN subqueries -> EXISTS / NOT EXISTS
        for in_node in list(tree.find_all(exp.In)):
            if isinstance(in_node.parent, exp.Not):
                # NOT IN case — handle via the Not wrapper
                self._transform_in(tree, in_node, steps, negated=True, not_wrapper=in_node.parent)
            else:
                self._transform_in(tree, in_node, steps, negated=False)

        # Normalize EXISTS subqueries: SELECT ... -> SELECT 1
        for exists_node in list(tree.find_all(exp.Exists)):
            self._normalize_exists_select(exists_node, steps, tree)

        return tree, steps

    def _transform_in(
        self,
        tree: exp.Expression,
        in_node: exp.In,
        steps: list[RewriteStep],
        negated: bool,
        not_wrapper: exp.Not | None = None,
    ) -> None:
        """Transform IN (SELECT ...) to EXISTS (SELECT 1 FROM ... WHERE ...)."""
        # The subquery is in in_node.args["query"] for IN with subquery
        query = in_node.args.get("query")
        if query is None:
            # This is an IN with a literal list, not a subquery — skip
            return

        # Get the subquery Select node
        subquery: exp.Select
        if isinstance(query, exp.Subquery):
            subquery = query.this
        elif isinstance(query, exp.Select):
            subquery = query
        else:
            return

        if not isinstance(subquery, exp.Select):
            return

        before_sql = tree.sql()

        # The column on the left side of IN
        outer_col = in_node.this

        # Get the column from the subquery's SELECT list
        inner_select_expr = subquery.expressions[0]
        inner_col = inner_select_expr

        # Determine inner table name from subquery's FROM
        inner_from = subquery.args.get("from_")
        if not inner_from:
            return
        inner_table = inner_from.this
        inner_table_name = inner_table.alias_or_name if isinstance(inner_table, (exp.Table, exp.Subquery)) else str(inner_table)

        # Determine outer table name from the outer query's FROM
        outer_table_name = self._find_outer_table_name(tree)
        if not outer_table_name:
            return

        # Build correlation predicate: inner_table.col = outer_table.col
        # Get the column name from the inner select expression
        if isinstance(inner_col, exp.Column):
            col_name = inner_col.name
        else:
            col_name = str(inner_col)

        # Also get outer column name
        if isinstance(outer_col, exp.Column):
            outer_col_name = outer_col.name
        else:
            outer_col_name = str(outer_col)

        correlation = exp.EQ(
            this=exp.Column(this=exp.to_identifier(col_name), table=exp.to_identifier(inner_table_name)),
            expression=exp.Column(this=exp.to_identifier(outer_col_name), table=exp.to_identifier(outer_table_name)),
        )

        # Build the EXISTS subquery: SELECT 1 FROM inner_table WHERE correlation AND original_where
        existing_where = subquery.args.get("where")
        if existing_where:
            new_where_condition = exp.And(this=correlation, expression=existing_where.this)
        else:
            new_where_condition = correlation

        exists_subquery = exp.Select(
            expressions=[exp.Literal.number(1)],
        ).from_(inner_table.copy()).where(new_where_condition)

        exists_node = exp.Exists(this=exists_subquery)

        if negated:
            replacement = exp.Not(this=exists_node)
            target = not_wrapper if not_wrapper else in_node
        else:
            replacement = exists_node
            target = in_node

        target.replace(replacement)

        after_sql = tree.sql()
        steps.append(
            RewriteStep(
                pass_name=self.name,
                description="IN subquery" + (" (negated)" if negated else "") + " → EXISTS",
                before_sql=before_sql,
                after_sql=after_sql,
            )
        )

    def _normalize_exists_select(
        self,
        exists_node: exp.Exists,
        steps: list[RewriteStep],
        tree: exp.Expression,
    ) -> None:
        """Normalize SELECT expressions inside EXISTS to SELECT 1."""
        subquery = exists_node.this
        if isinstance(subquery, exp.Subquery):
            select = subquery.this
        elif isinstance(subquery, exp.Select):
            select = subquery
        else:
            return

        if not isinstance(select, exp.Select):
            return

        # Check if already SELECT 1
        exprs = select.expressions
        if len(exprs) == 1 and isinstance(exprs[0], exp.Literal) and exprs[0].this == "1":
            return

        before_sql = tree.sql()

        # Replace expressions with SELECT 1
        select.set("expressions", [exp.Literal.number(1)])

        after_sql = tree.sql()
        steps.append(
            RewriteStep(
                pass_name=self.name,
                description="EXISTS SELECT ... → SELECT 1",
                before_sql=before_sql,
                after_sql=after_sql,
            )
        )

    def _find_outer_table_name(self, tree: exp.Expression) -> str | None:
        """Find the table name from the outermost query's FROM clause."""
        # Find the top-level Select
        if isinstance(tree, exp.Select):
            select = tree
        else:
            select = tree.find(exp.Select)

        if not select:
            return None

        from_clause = select.args.get("from_")
        if not from_clause:
            return None

        table = from_clause.this
        if isinstance(table, exp.Table):
            return table.alias_or_name
        return None
