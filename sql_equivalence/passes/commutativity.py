from __future__ import annotations

from typing import Sequence

import sqlglot.expressions as exp

from sql_equivalence.models import RewritePass, RewriteStep


class CommutativityPass(RewritePass):
    name = "commutativity"

    def apply(
        self, expression: exp.Expression
    ) -> tuple[exp.Expression, list[RewriteStep]]:
        tree = expression.copy()
        before_sql = tree.sql()

        tree = self._sort_and_or(tree)
        tree = self._sort_equality(tree)
        tree = self._sort_in_lists(tree)
        tree = self._sort_inner_joins(tree)
        tree = self._sort_unions(tree)

        steps: list[RewriteStep] = []
        after_sql = tree.sql()
        if before_sql != after_sql:
            steps.append(
                RewriteStep(
                    pass_name=self.name,
                    description="Sorted commutative operands into canonical order",
                    before_sql=before_sql,
                    after_sql=after_sql,
                )
            )

        return tree, steps

    def _sort_and_or(self, tree: exp.Expression) -> exp.Expression:
        """Flatten and sort AND/OR binary trees.

        Runs iteratively because replacing an outer AND/OR can detach
        inner nodes from the tree, leaving them unsorted.
        """
        for _ in range(5):  # converges in 2-3 iterations max
            changed = False
            for node_type in (exp.And, exp.Or):
                for node in list(tree.find_all(node_type)):
                    if isinstance(node.parent, node_type):
                        continue
                    operands = self._flatten_binary(node, node_type)
                    sorted_operands = sorted(operands, key=lambda n: n.sql())
                    if [o.sql() for o in operands] != [o.sql() for o in sorted_operands]:
                        rebuilt = self._rebuild_binary(sorted_operands, node_type)
                        if node is tree:
                            tree = rebuilt
                        else:
                            node.replace(rebuilt)
                        changed = True
            if not changed:
                break
        return tree

    def _flatten_binary(
        self, node: exp.Expression, node_type: type[exp.Expression]
    ) -> list[exp.Expression]:
        result: list[exp.Expression] = []
        if isinstance(node, node_type):
            result.extend(self._flatten_binary(node.args.get("this"), node_type))
            result.extend(self._flatten_binary(node.args.get("expression"), node_type))
        else:
            result.append(node)
        return result

    def _rebuild_binary(
        self,
        operands: Sequence[exp.Expression],
        node_type: type[exp.Expression],
    ) -> exp.Expression:
        result = operands[0].copy()
        for operand in operands[1:]:
            result = node_type(this=result, expression=operand.copy())
        return result

    def _sort_equality(self, tree: exp.Expression) -> exp.Expression:
        for eq in list(tree.find_all(exp.EQ)):
            left = eq.args.get("this")
            right = eq.args.get("expression")
            if left and right:
                left_key = self._eq_sort_key(left)
                right_key = self._eq_sort_key(right)
                if left_key > right_key:
                    eq.set("this", right.copy())
                    eq.set("expression", left.copy())
        return tree

    @staticmethod
    def _eq_sort_key(node: exp.Expression) -> tuple[int, str]:
        """Sort key for equality operands: columns first, then literals."""
        if isinstance(node, (exp.Column, exp.Identifier)):
            return (0, node.sql())
        if isinstance(node, exp.Literal):
            return (1, node.sql())
        return (0, node.sql())

    def _sort_in_lists(self, tree: exp.Expression) -> exp.Expression:
        for in_node in list(tree.find_all(exp.In)):
            expressions = in_node.args.get("expressions")
            if expressions:
                sorted_exprs = sorted(expressions, key=lambda n: n.sql())
                if [e.sql() for e in expressions] != [e.sql() for e in sorted_exprs]:
                    in_node.set("expressions", [e.copy() for e in sorted_exprs])
        return tree

    def _sort_inner_joins(self, tree: exp.Expression) -> exp.Expression:
        for select in list(tree.find_all(exp.Select)):
            joins = list(select.find_all(exp.Join))
            if not joins:
                continue
            all_inner = all(
                not join.args.get("side") and not join.args.get("kind")
                for join in joins
            )
            if not all_inner:
                continue
            from_clause = select.args.get("from") or select.args.get("from_")
            if not from_clause:
                continue
            from_table = from_clause.args.get("this")
            if not from_table:
                continue

            tables: list[tuple[str, exp.Expression, exp.Expression | None]] = []
            tables.append((from_table.sql(), from_table, None))
            for join in joins:
                join_table = join.args.get("this")
                on_cond = join.args.get("on")
                if join_table:
                    tables.append((join_table.sql(), join_table, on_cond))

            sorted_tables = sorted(tables, key=lambda t: t[0])
            if [t[0] for t in tables] == [t[0] for t in sorted_tables]:
                continue

            all_on_conditions = [t[2] for t in tables if t[2] is not None]
            from_clause.set("this", sorted_tables[0][1].copy())

            new_joins: list[exp.Join] = []
            for i, (_, tbl, _) in enumerate(sorted_tables[1:]):
                on_cond = all_on_conditions[i] if i < len(all_on_conditions) else None
                new_join = exp.Join(this=tbl.copy())
                if on_cond:
                    new_join.set("on", on_cond.copy())
                new_joins.append(new_join)
            select.set("joins", new_joins)
        return tree

    def _sort_unions(self, tree: exp.Expression) -> exp.Expression:
        for node_type in (exp.Union, exp.Intersect):
            for node in list(tree.find_all(node_type)):
                if isinstance(node.parent, node_type):
                    continue
                if self._contains_except(node):
                    continue
                branches = self._flatten_set_op(node, node_type)
                sorted_branches = sorted(branches, key=lambda n: n.sql())
                if [b.sql() for b in branches] != [b.sql() for b in sorted_branches]:
                    distinct = node.args.get("distinct")
                    rebuilt = self._rebuild_set_op(sorted_branches, node_type, distinct)
                    if node is tree:
                        tree = rebuilt
                    else:
                        node.replace(rebuilt)
        return tree

    def _contains_except(self, node: exp.Expression) -> bool:
        if isinstance(node, exp.Except):
            return True
        if isinstance(node, (exp.Union, exp.Intersect)):
            left = node.args.get("this")
            right = node.args.get("expression")
            return self._contains_except(left) or self._contains_except(right)
        return False

    def _flatten_set_op(
        self, node: exp.Expression, node_type: type[exp.Expression]
    ) -> list[exp.Expression]:
        result: list[exp.Expression] = []
        if isinstance(node, node_type):
            result.extend(self._flatten_set_op(node.args.get("this"), node_type))
            result.extend(self._flatten_set_op(node.args.get("expression"), node_type))
        else:
            result.append(node)
        return result

    def _rebuild_set_op(
        self,
        branches: Sequence[exp.Expression],
        node_type: type[exp.Expression],
        distinct: bool | None,
    ) -> exp.Expression:
        result = branches[0].copy()
        for branch in branches[1:]:
            kwargs: dict = {"this": result, "expression": branch.copy()}
            if distinct is not None:
                kwargs["distinct"] = distinct
            result = node_type(**kwargs)
        return result
