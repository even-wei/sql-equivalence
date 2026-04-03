from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import sqlglot.expressions as exp


@dataclass(frozen=True)
class RewriteStep:
    pass_name: str
    description: str
    before_sql: str
    after_sql: str


@dataclass(frozen=True)
class ColumnStatus:
    """Equivalence status for a single output column."""

    name: str
    status: str  # "equivalent", "modified", "added", "removed"
    expr_a: str | None = None  # normalized expression from SQL A
    expr_b: str | None = None  # normalized expression from SQL B
    diff: str | None = None  # human-readable diff if modified


@dataclass(frozen=True)
class EquivalenceResult:
    equivalent: bool
    proof_a: list[RewriteStep] = field(default_factory=list)
    proof_b: list[RewriteStep] = field(default_factory=list)
    remaining_diff: str | None = None
    columns: list[ColumnStatus] = field(default_factory=list)


class RewritePass(ABC):
    name: str

    @abstractmethod
    def apply(self, expression: exp.Expression) -> tuple[exp.Expression, list[RewriteStep]]:
        ...
