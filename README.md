# sql-equivalence

A standalone Python tool that checks whether two SQL queries are **semantically equivalent** via algebraic rewriting. Dialect-agnostic, powered by [sqlglot](https://github.com/tobymao/sqlglot).

Unlike simple text diffing or AST equality checks, this tool **normalizes** both queries through a pipeline of rewrite passes — proving that cosmetic differences (CTE renames, predicate reordering, alias changes) don't affect query semantics.

## Quick Start

```bash
pip install -e .
```

```python
from sql_equivalence import check_equivalence

result = check_equivalence(
    sql_a="SELECT a, b FROM t WHERE x > 5 AND x > 3",
    sql_b="SELECT b, a FROM t WHERE x > 5",
    dialect="bigquery",  # optional, any sqlglot-supported dialect
)

result.equivalent     # True
result.proof_a        # List[RewriteStep] — transformations applied to sql_a
result.proof_b        # List[RewriteStep] — transformations applied to sql_b
result.remaining_diff # None if equivalent, AST diff otherwise
```

### Column-Level Analysis

Every result includes per-column equivalence detail:

```python
result = check_equivalence(
    "SELECT id, name, SUM(amount) AS total FROM orders GROUP BY id, name",
    "SELECT id, UPPER(name) AS name, AVG(amount) AS total FROM orders GROUP BY id, name",
)

for col in result.columns:
    print(f"{col.name}: {col.status}")
    # id: equivalent
    # name: modified        (expr_a="name", expr_b="UPPER(name)")
    # total: modified       (expr_a="SUM(amount)", expr_b="AVG(amount)")
```

Each `ColumnStatus` provides:
- `name` — output column name
- `status` — `"equivalent"` / `"modified"` / `"added"` / `"removed"`
- `expr_a` / `expr_b` — the normalized expression from each query
- `diff` — human-readable AST diff when `modified`

## How It Works

Both SQL inputs are parsed, run through an ordered pipeline of **rewrite passes**, then the normalized ASTs are compared. Each pass records its transformations, forming a step-by-step proof trace.

```
SQL_A ──→ [Parse] ──→ [Pass 1] ──→ [Pass 2] ──→ ... ──→ Normalized AST_A
SQL_B ──→ [Parse] ──→ [Pass 1] ──→ [Pass 2] ──→ ... ──→ Normalized AST_B
                                                                │
                                                    Compare ◄───┘
                                                        │
                                               EquivalenceResult
```

### Rewrite Passes

| # | Pass | What it normalizes |
|---|------|--------------------|
| 1 | **CTE Inline** | Inline single-use CTEs as subqueries (iterative, handles chains) |
| 2 | **Alias Normalization** | Canonicalize internal aliases to `_t0`, `_cte0`, etc. |
| 3 | **Subquery ↔ JOIN** | `IN (SELECT ...)` ↔ `EXISTS (SELECT 1 ...)`, normalize EXISTS body |
| 4 | **Predicate Simplification** | Absorption, double negation, De Morgan's, flatten, identity/contradiction |
| 5 | **Commutativity** | Sort AND/OR, equality operands, JOINs, UNIONs, IN lists |
| 6 | **Column Reorder** | Sort SELECT columns alphabetically (unless positional refs exist) |

## What It Can Prove Equivalent

| Pattern | Example |
|---------|---------|
| CTE ↔ inline subquery | `WITH cte AS (...) SELECT FROM cte` ≡ `SELECT FROM (...)` |
| CTE/subquery alias rename | `AS foo` ≡ `AS bar` |
| CTE chaining (2-3 levels) | Chain of CTEs ≡ nested subqueries |
| AND/OR commutativity | `WHERE a AND b` ≡ `WHERE b AND a` |
| Equality commutativity | `WHERE 5 = x` ≡ `WHERE x = 5` |
| JOIN table reorder | `FROM a JOIN b` ≡ `FROM b JOIN a` (inner joins) |
| IN ↔ EXISTS | `IN (SELECT ...)` ≡ `EXISTS (SELECT 1 ...)` |
| NOT IN ↔ NOT EXISTS | Same conversion |
| Predicate absorption | `x > 5 AND x > 3` ≡ `x > 5` |
| Double negation | `NOT NOT x` ≡ `x` |
| De Morgan's law | `NOT (a AND b)` ≡ `NOT a OR NOT b` |
| Boolean identity | `x AND TRUE` ≡ `x` |
| Column reorder | `SELECT b, a` ≡ `SELECT a, b` |
| IN value list reorder | `IN (3,1,2)` ≡ `IN (1,2,3)` |
| UNION/INTERSECT reorder | Branch order normalized |
| Extra parentheses | `WHERE (x > 1)` ≡ `WHERE x > 1` |
| Dialect case sensitivity | Snowflake: `SELECT A` ≡ `SELECT a` |

## Known Limitations

### Not yet handled

| Limitation | Example | Difficulty to fix |
|------------|---------|-------------------|
| **Multi-ref CTE expansion** | CTE used in 2 UNION branches ≢ duplicated subqueries | Medium |
| **Subquery flattening** | `FROM (SELECT FROM t WHERE ...) WHERE ...` ≢ `FROM t WHERE ... AND ...` | Medium |
| **Column ref resolution** | CTE defines `c = b+1`, outer selects `c` — not traced through scopes | Hard |
| **Schema-aware SELECT \*** | Can't expand `SELECT *` without schema info | Medium |
| **Arithmetic commutativity** | `a + b` ≢ `b + a` (only AND/OR/EQ are sorted) | Easy |
| **Complex predicate absorption** | `f(x) > 5 AND f(x) > 3` — only simple numeric literals absorbed | Medium |
| **Window function normalization** | Identical PARTITION BY with different internal order | Medium |
| **COUNT(\*) vs COUNT(1)** | Semantically equivalent but not normalized | Easy |
| **3+ table JOIN reorder** | ON conditions not redistributed across table pairs | Hard |

### By design

- **Not complete** — algebraic SQL equivalence is undecidable in general. This tool handles common patterns.
- **NOT IN with NULLs** — `NOT IN` → `NOT EXISTS` has different NULL semantics. Documented but not handled.
- **UDFs** — cannot reason about user-defined function semantics.
- **EXCEPT order** — correctly preserved (EXCEPT is not commutative).

## Project Structure

```
sql_equivalence/
├── __init__.py              # Public API: check_equivalence()
├── models.py                # RewriteStep, EquivalenceResult, ColumnStatus, RewritePass ABC
├── pipeline.py              # Pass orchestration, column-level comparison
├── diff.py                  # AST diff utility
└── passes/
    ├── alias.py             # AliasNormalizationPass
    ├── cte_inline.py        # CTEInlinePass
    ├── subquery_join.py     # SubqueryJoinPass
    ├── predicate.py         # PredicateSimplificationPass
    ├── commutativity.py     # CommutativityPass
    └── column_reorder.py    # ColumnReorderPass
```

## Running Tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

191 tests covering:
- Per-pass unit tests (alias, CTE, subquery, predicate, commutativity, column reorder)
- End-to-end pipeline tests (single-pass, multi-pass, non-equivalent, dialect)
- Real-world patterns (dbt refactoring, CTE chaining, nested subqueries)
- Column-level analysis tests
- Edge cases and advanced patterns

## Dependencies

- **[sqlglot](https://github.com/tobymao/sqlglot)** — SQL parsing, AST manipulation, dialect support
- No other runtime dependencies
