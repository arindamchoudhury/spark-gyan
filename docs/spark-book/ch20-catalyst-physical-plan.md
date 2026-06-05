# Chapter 20 — Query Optimisation: Catalyst and the Physical Plan

> *Learning-path topic: A1 (Advanced)*
> *Status: ⬜ Not yet written*

> **Note "📌 Topics deferred here from Chapter 1"
    Chapter 1 introduces the QueryExecution pipeline (Unresolved → Analyzed → Optimized → Physical → Codegen → RDD) and explains that Catalyst applies 100+ optimization rules (source-verified against Optimizer.scala v4.1.2). The following are covered in full here:

    - **Why QueryExecution phases are separated** — the Analyzer must resolve column references and validate types before the Optimizer can safely rewrite the plan; the Planner is separate from the Optimizer because physical planning involves cost estimation, not algebraic equivalence
    - **Catalyst rule categories and execution order** — rules are grouped into named batches (`Substitution`, `Analysis`, `Operator Optimizations`, `Join Reorder`, etc.) and applied in fixed-point iteration until no batch fires any rule; ordering matters (predicate pushdown must precede projection pruning to avoid pushing down on a wider schema)
    - **QueryPlan tree structure** — logical and physical plans are trees of `QueryPlan` nodes; each node is an operator with zero or more child nodes; Catalyst rewrites the tree by pattern-matching subtrees and replacing them with equivalent but cheaper subtrees; algebraic equivalence laws (commutativity, associativity, pushdown) translate directly into rewrite rules
    - **Column resolution in the Analyzer** — unresolved column references are `UnresolvedAttribute` nodes; the Analyzer walks the tree bottom-up, resolving each against the parent operator's output schema; an `AnalysisException` is raised if a column is not found in any ancestor's output
    - **Cost-based optimizer (CBO)** — uses table and column statistics (`ANALYZE TABLE`) to estimate row counts and data sizes; the `JoinReorderDP` rule uses dynamic programming over estimated costs to find the optimal join order; falls back to heuristics when statistics are absent
    - **Physical plan selection** — the `SparkPlanner` generates multiple candidate physical plans for each logical operator (e.g. SortMergeJoin, BroadcastHashJoin, ShuffledHashJoin for a join node) and selects the lowest-cost candidate; see also Chapter 22 (A3) for execution-level join differences

*This chapter is not yet written. The above topics will form its core.*
