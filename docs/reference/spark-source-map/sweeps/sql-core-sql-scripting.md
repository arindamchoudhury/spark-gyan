---
subsystem: sql/core
spark_version: "4.2.0"
swept_at: 2026-08-07
group: sql-scripting
all_groups: [query-execution, joins-exec, adaptive, datasources, agg-window-exchange, python-arrow, streaming-exec, classic-api, sql-scripting]
status: complete
concepts:
  - name: Script entry — the CompoundBody that never reaches the analyzer
    topics: [I12]
  - name: Interpretation — turning the parse tree into an execution tree
    topics: [I12]
  - name: The execution-node tree and its in-order iterator
    topics: [I12]
  - name: The result protocol — why a script returns exactly one DataFrame
    topics: [I12]
  - name: Frames, scopes and generated labels
    topics: [I12]
  - name: Script-local variables and the FakeLocalCatalog
    topics: [I12]
  - name: Control flow — loops, branches, and LEAVE/ITERATE label propagation
    topics: [I12]
  - name: FOR — a per-row variable scope over a driver-side iterator
    topics: [I12]
  - name: Declaration ordering — the compound-body state machine
    topics: [I12]
  - name: Condition handlers — EXIT, CONTINUE, and how a handler is chosen
    topics: []
    propose:
      code: I31
      level: Intermediate
      title: "SQL Scripting Condition Handlers: EXIT, CONTINUE and SQLSTATE Matching"
      what: "The DECLARE ... HANDLER mechanism inside a SQL script: named conditions, SQLSTATE matching, the NOT FOUND and SQLEXCEPTION catch-alls, and the difference between an EXIT handler (which leaves the enclosing block) and a CONTINUE handler (which resumes after the failing statement)."
      why: "It is the only error handling a pure-SQL pipeline has, and its resolution order is not obvious — a handler on a SQLSTATE can silently outrank the one you thought you wrote, an unhandled '02' condition does not fail the script at all, and CONTINUE handlers change which statement runs next."
  - name: Cursors — DECLARE, OPEN, FETCH, CLOSE and the snapshot taken at OPEN
    topics: []
    propose:
      code: I32
      level: Intermediate
      title: "SQL Cursors: Row-at-a-Time Iteration and Where the Snapshot Is Taken"
      what: "The 4.2.0 cursor statements — DECLARE CURSOR, OPEN (with USING parameters), FETCH ... INTO variables, CLOSE — their four-state lifecycle, and the fact that OPEN starts execution and locks in the files that will be read."
      why: "A cursor is the one place in Spark where you consume a query row by row on the driver, and its semantics are surprising in both directions: the data snapshot is fixed at OPEN rather than at FETCH, and running off the end raises a condition that is silently ignored unless you declared a NOT FOUND handler."
  - name: Parameters and SET — what a statement inside a script may be
    topics: [I12]
  - name: Errors, line numbers, and the three feature gates
    topics: [I12]
  - name: The edges — stored procedures, the spark-sql CLI, EXECUTE IMMEDIATE, Connect
    topics: [I12, E9]
---

# sql/core — SQL scripting

> Source sweep of the `sql/core` **sql-scripting** group at Spark **4.2.0**.
> Scope (from `groups.yaml`): `scripting/` — `SqlScriptingInterpreter`, `SqlScriptingExecution`,
> `SqlScriptingExecutionNode`, `SqlScriptingLocalVariableManager`.
>
> The group's scope is small — seven files, 2 471 lines — but the feature is not contained by it.
> The grammar, the logical plan nodes, the parse-time validation, the analyzer rules and the error
> catalogue all live in `sql/catalyst`, and the four cursor commands live in
> `sql/core/execution/command/v2/`. This page follows the concept, and says which module each
> anchor is in.

!!! note "Re-swept 2026-08-07 at an unchanged Spark 4.2.0"

    The first pass (2026-08-06) was breadth-complete and stayed breadth-complete on the re-sweep:
    7/7 files in `scripting/` cited, no nested sub-packages, all three `spark.sql.scripting.*`
    configs attributed. **Neither breadth check found work** — the second pass was driven by depth,
    specifically by the rule that a concept recorded as an entry point plus one class is a
    placeholder. Four layers had been *cited but never opened*: `ResolveCursors`,
    `ResolveFetchCursor`, `ParameterizedQueryExecutor`, and the parser's condition/SQLSTATE
    validation.

    Opening them **corrected one claim on this page** — cursor *existence* is resolved in the
    analyzer, not at runtime as originally written (see [Cursors](#cursors-declare-open-fetch-close-and-the-snapshot-taken-at-open))
    — and added the parameters-and-`SET` concept below. No concept mapped to a new topic; everything
    landed on I12, I31 or I32, which is a normal re-sweep outcome rather than a failed one.

!!! info "Prior coverage, and how this page differs"

    Two earlier sweeps already touch SQL scripting from the outside, and this page agrees with
    both. [sql/core — query-execution](sql-core-query-execution.md#sql-cursors-and-session-variables)
    covers the four cursor *commands* as V2 physical operators;
    [sql/catalyst — types-parser](sql-catalyst-types-parser.md) covers the grammar and the parse-time
    feature gates. Neither covers the runtime: the interpreter, the frame/scope stack, the handler
    search, or the iterator protocol that drives statement execution. That runtime is this page.

## Config slice

`sql/core` registers no configs of its own — every SQL config is declared in catalyst's
`SQLConf.scala` — so the slice below was taken from the whole catalog and filtered to the group's
namespace plus the near-misses that a name search turns up:

```
pattern: \.scripting\.|scriptTransformation|allowSessionVariableInPersistedView|variable\.substitute
```

| Config | Default | Since | Concept |
|---|---|---|---|
| `spark.sql.scripting.enabled` | `true` | 4.0.0 | script entry — gates the whole feature at parse time |
| `spark.sql.scripting.continueHandlerEnabled` | **`false`** (internal) | 4.1.0 | condition handlers — `CONTINUE` handlers only |
| `spark.sql.scripting.cursorEnabled` | **`false`** (internal) | 4.2.0 | cursors — all four statements |

Three more keys matched the pattern and are **out of scope**; they are recorded here so the next
refresh does not chase them again:

| Config | Belongs to |
|---|---|
| `spark.sql.scriptTransformation.exitTimeoutInSeconds` | the `TRANSFORM` operator (`SparkScriptTransformationExec`), an unrelated feature that shares the word "script" |
| `spark.sql.variable.substitute` | `VariableSubstitution`, the `${var}` text substitution applied to SQL *before* parsing — nothing to do with `DECLARE` |
| `spark.sql.legacy.allowSessionVariableInPersistedView` | session variables in persisted views; script-local variables cannot appear in a view at all |

One config the group reads but does not own: **`spark.sql.caseSensitive`**. It decides how variable
names, cursor names and `FOR`-loop variable names are normalised, at *declaration* time — see the
warning under [Cursors](#cursors-declare-open-fetch-close-and-the-snapshot-taken-at-open).

## Script entry — the CompoundBody that never reaches the analyzer

**What it is:** a SQL script is a `BEGIN … END` block. The parser produces a `CompoundBody` logical
plan, which is not a plan the analyzer can resolve — it is a *container* of unresolved statements.
`QueryExecution` therefore intercepts it before analysis, runs the whole script, and substitutes the
result of the last statement as the plan.

**Code path:** `SparkSession.sql` → `AstBuilder.visitCompoundOrSingleStatement` → `CompoundBody` →
`QueryExecution.lazySqlScriptExecuted` → `SqlScriptingExecution.executeSqlScript` → `LocalRelation`
→ *then* the normal analyzer runs on that `LocalRelation`.

**Anchor files:**

- [AstBuilder.scala:173](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala#L173) — the fork: a `singleCompoundStatement` becomes a `CompoundBody`, anything else takes the ordinary path. The `spark.sql.scripting.enabled` check is here, *after* parsing
- [SqlBaseParser.g4:90](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4#L90) — `singleCompoundStatement : BEGIN (NOT ATOMIC)? compoundBody? END`. `NOT ATOMIC` parses and is ignored: there are no script-level transactions in 4.2.0
- [QueryExecution.scala:173](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala#L173) — `lazySqlScriptExecuted`, a `LazyTry` that runs the script; [:191](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala#L191) `lazyAnalyzed` calls it *first* and then analyzes whatever it returned
- [QueryExecution.scala:914](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala#L914) — `isUnresolvedPlanSqlScript`, matching bare `CompoundBody` and `NameParameterizedQuery(CompoundBody, …)`
- [SqlScriptingExecution.scala:295](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala#L295) — `executeSqlScript`, the single public entry point

**Configs:** `spark.sql.scripting.enabled`

**Maps to topics:** I12 (SQL Scripting)

!!! warning "The whole script runs during *analysis*, not during an action"

    `lazyAnalyzed` calls `assertSqlScriptExecuted()` before it touches the analyzer. Every statement
    in the script — inserts, table drops, the lot — has already executed by the time
    `spark.sql("BEGIN … END")` returns a DataFrame, exactly as `df.explain()` on a script would also
    have run it. There is no lazy script. The `LazyTry` only guarantees it runs *once*.

    This is why the returned plan is a `LocalRelation`: the rows have already been collected to the
    driver. A script's result is materialised, not a lazily-evaluated query.

## Interpretation — turning the parse tree into an execution tree

**What it is:** a separate pass, between parsing and execution, that rewrites the `CompoundBody`
tree of *logical plans* into a parallel tree of `…Exec` **execution nodes**. These are not Spark
physical operators — nothing about them is distributed. They are a driver-side interpreter's
state machine, one node per SQL statement or control-flow construct.

**Code path:** `SqlScriptingExecution` constructor → `SqlScriptingInterpreter.buildExecutionPlan` →
`transformTreeIntoExecutable` (one `case` per statement type) → `CompoundBodyExec`

**Anchor files:**

- [SqlScriptingInterpreter.scala:50](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingInterpreter.scala#L50) — `buildExecutionPlan`
- [SqlScriptingInterpreter.scala:170](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingInterpreter.scala#L170) — `transformTreeIntoExecutable`, the whole dispatch table: `IfElseStatement`, `SearchedCaseStatement`, `SimpleCaseStatement`, `WhileStatement`, `RepeatStatement`, `LoopStatement`, `ForStatement`, `LeaveStatement`, `IterateStatement`, `SingleStatement`, and a nested `CompoundBody`
- [SqlScriptingInterpreter.scala:70](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingInterpreter.scala#L70) — `transformBodyIntoExec`, which additionally builds the block's handler map
- [SqlScriptingLogicalPlans.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/SqlScriptingLogicalPlans.scala) — the parse-side counterparts, in catalyst: `CompoundBody:75`, `IfElseStatement:100`, `WhileStatement:141`, `RepeatStatement:171`, `LeaveStatement:197`, `IterateStatement:213`, `SearchedCaseStatement:240`, `SimpleCaseStatement:290`, `LoopStatement:318`, `ForStatement:344`, `ErrorCondition:366`, `ExceptionHandler:440`

**Maps to topics:** I12

!!! info "`SimpleCaseStatement` is desugared, not interpreted"

    `CASE x WHEN 1 THEN … WHEN 2 THEN …` does not compare `x` against each branch directly. The
    interpreter wraps the case expression in `Project(Alias(caseExpr, "caseVariable"), OneRowRelation)`
    and executes it as a real query to get a `Literal`
    ([SqlScriptingExecutionNode.scala:739](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L739)),
    then builds one `EqualTo(literal, branchExpr)` query **per branch**
    ([:813](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L813)).

    A simple `CASE` with *n* branches therefore costs up to *n* + 1 driver-side Spark jobs. The
    `Origin` on each generated condition is faked to the text `(1 = 5)` so that an error message
    names the comparison rather than an internal plan.

## The execution-node tree and its in-order iterator

**What it is:** execution is a single-threaded, driver-side in-order traversal. Every node exposes
`getTreeIterator`, and the top-level loop repeatedly asks for the next `LeafStatementExec`. Control
flow is implemented *inside* the iterators: a `WhileStatementExec`'s iterator hands back its
condition statement, then the body's statements, then the condition again.

**Code path:** `SqlScriptingExecutionFrame.next()` → `CompoundBodyExec.getTreeIterator.next()` →
(recursively) the child node's iterator → a `SingleStatementExec`

**Anchor files:**

- [SqlScriptingExecutionNode.scala:40](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L40) — `CompoundStatementExec`, with `LeafStatementExec:56` and `NonLeafStatementExec:61`
- [SqlScriptingExecutionNode.scala:157](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L157) — `SingleStatementExec`, the only node that runs SQL; [:198](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L198) `buildDataFrame` = `Dataset.ofRows(session, preparedPlan)`, i.e. each statement is analyzed and planned **independently**, at the moment it runs
- [SqlScriptingExecutionNode.scala:268](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L268) — `CompoundBodyExec`, and [:317](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L317) its `treeIterator` — the recursion, the scope enter/exit calls, and the `stopIteration` flag
- [SqlScriptingExecutionNode.scala:82](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L82) — `evaluateBooleanCondition`: a condition must be a single row of a single `BooleanType` column; `NULL` is false, two rows is an internal error
- [SqlScriptingExecutionNode.scala:209](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L209) — `NoOpStatementExec`, emitted by an empty `BEGIN END` and by several edge cases below

**Maps to topics:** I12

!!! warning "Each statement is analyzed on its own, so a script sees its own side effects"

    Because `buildDataFrame` builds a fresh `Dataset` per statement at execution time, a
    `CREATE TABLE` earlier in the script is visible to a `SELECT` later in it — the later statement
    is analyzed after the earlier one has run. That is what makes procedural SQL work at all, and it
    is also why nothing in a script can be optimised across statement boundaries: there is no plan
    that contains two statements.

!!! info "`NoOpStatementExec` exists because iterators must not run dry"

    It shows up in three places, all the same problem: a construct that must yield *something* so
    that its parent's `next()` does not fail. An empty `BEGIN END`
    ([SqlScriptingInterpreter.scala:146](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingInterpreter.scala#L146)),
    a `FOR` over an empty result set
    ([:1145](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L1145)),
    and a `WHILE`/`REPEAT` body whose last statement was interrupted by a `CONTINUE` handler
    ([:564](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L564)).
    It is also appended to every `FOR` body so the loop variables are not dropped before the last
    real statement has run.

## The result protocol — why a script returns exactly one DataFrame

**What it is:** a script can contain many `SELECT`s, but `spark.sql(script)` returns one DataFrame.
The executor iterates statements, executing each one, and *returns to its caller* whenever a
statement produced a result (as opposed to a `CommandResult`). `executeSqlScript` drives that loop
to exhaustion, keeping only the **last** result, and wraps it as a `LocalRelation`.

**Code path:** `executeSqlScript` → loop over `getNextResult` → `getNextResultInternal` →
`getNextStatement` → `stmt.buildDataFrame(session)` → `df.collect()`

**Anchor files:**

- [SqlScriptingExecution.scala:196](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala#L196) — `getNextResultInternal`: executes statements until one yields a non-`CommandResult` plan, then returns it
- [SqlScriptingExecution.scala:227](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala#L227) — `getNextResult`, the exception-handling wrapper that recurses after installing a handler frame
- [SqlScriptingExecution.scala:300](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala#L300) — the driver loop: `result` is overwritten on every iteration, so only the last survives; an empty script yields `LocalRelation.fromExternalRows(Seq.empty, Seq.empty)`
- [SqlScriptingExecution.scala:70](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala#L70) — `withContextManager`, which every caller must wrap the iteration in

**Maps to topics:** I12

!!! warning "The API contract is caller-enforced, and violating it silently reorders your script"

    `getNextResult` *advances and executes* the script to find the next result. The scaladoc at
    [SqlScriptingExecution.scala:216](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala#L216)
    states it plainly: the returned DataFrame must be executed *before* the next call, or execution
    order is wrong. Nothing enforces this — `executeSqlScript` calls `df.get.collect()` immediately
    for exactly this reason.

    Consequence for the user-facing behaviour: a script's intermediate `SELECT`s are executed and
    **discarded**. Only the final one is returned, and it is returned as already-collected rows.
    Producing several result sets from one script is not possible through `spark.sql`.

## Frames, scopes and generated labels

**What it is:** two nested stacks. A **frame** is a call context — the script itself, or a running
exception handler. A **scope** is a lexical `BEGIN … END` block within a frame, holding that block's
local variables, cursors and handlers. Name lookup walks scopes inward-out within a frame, then
walks *outer frames* — but in an outer frame only the scopes that were visible where the handler was
declared, which is what makes handler bodies obey lexical scoping.

**Code path:** `SqlScriptingExecutionContext.frames` → `SqlScriptingExecutionFrame.scopes` →
`SqlScriptingExecutionScope.{variables, cursors, cursorStates}`

**Anchor files:**

- [SqlScriptingExecutionContext.scala:33](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala#L33) — the context, a `ListBuffer` of frames
- [SqlScriptingExecutionContext.scala:72](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala#L72) — `searchAcrossFrames`, the shared lookup used by both variables and cursors, with the `dropWhile(scope => !previousFrameDefinitionLabel.contains(scope.label))` that implements the lexical rule
- [SqlScriptingExecutionContext.scala:298](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala#L298) — `SqlScriptingExecutionFrame`, itself an `Iterator[CompoundStatementExec]`
- [SqlScriptingExecutionContext.scala:459](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala#L459) — `SqlScriptingExecutionScope`, and [:470](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala#L470) `cleanup()`, which closes every cursor in the scope on exit
- [SqlScriptingExecutionNode.scala:293](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L293) — `enterScope` / [:306](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L306) `exitScope`, both deliberately idempotent via a three-value `ScopeStatus`
- [ParserUtils.scala:314](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParserUtils.scala#L314) — `SqlScriptingLabelContext`: label matching (`beginLabel` must equal `endLabel`), duplicate detection, and the ban on qualified labels
- [SqlScriptingContextManager.scala:57](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/SqlScriptingContextManager.scala#L57) — the context is published to the rest of Spark through a `LexicalThreadLocal`, which is why analyzer rules and V2 commands can find it without a parameter

**Maps to topics:** I12

!!! info "Every block has a label, whether you wrote one or not"

    An unlabelled `BEGIN … END` gets a random lowercase UUID as its label
    ([SqlScriptingLabelContext](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParserUtils.scala#L314);
    a `FOR` without a variable name does the same at
    [SqlScriptingExecutionNode.scala:1166](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L1166)).
    Labels are lowercased at parse time and are always case-insensitive, per SQL/PSM. They are not
    cosmetic: the scope stack, `LEAVE`/`ITERATE` matching, qualified variable references
    (`label.var`) and qualified cursor references all key on them.

!!! warning "Three label names are reserved: `builtin`, `session`, and anything starting `sys`"

    `SqlScriptingLabelContext.forbiddenLabelNames`
    ([ParserUtils.scala:477](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParserUtils.scala#L477))
    is the regex set `builtin`, `session`, `sys.*` — so `sys`, `system` and `sysadmin` are all
    banned, as a prefix match rather than an exact one. A block label or a `FOR` variable name from
    that set raises `LABEL_OR_FOR_VARIABLE_NAME_FORBIDDEN`.

    The reason is the two-part name space: a qualified local variable is written `label.var`, and
    `session.x` / `system.session.x` must keep meaning *session variable*. The same predicate is what
    `VariableResolution.lookupVariable` uses to route a two-part name past the local manager, so
    allowing a block called `session` would make `session.x` ambiguous.

!!! warning "`exitScope` pops *through* to the named label"

    [SqlScriptingExecutionContext.scala:397](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala#L397)
    removes scopes from the top of the stack until it has removed the one whose label matched. A
    `LEAVE outer_label` from three blocks deep therefore discards three scopes' worth of variables
    and closes three scopes' worth of cursors in one step — the intended behaviour, but it means a
    cursor you meant to keep open does not survive a `LEAVE` past its declaring block.

## Script-local variables and the FakeLocalCatalog

**What it is:** `DECLARE x INT` inside a script creates a *local* variable in the current scope,
not a session variable. The analyzer resolves it through a `VariableManager` obtained from the
thread-local context manager, and marks the reference with the sentinel catalog `FakeLocalCatalog`
so that the physical commands know which manager to write back to. Session variables use
`FakeSystemCatalog` and the `SYSTEM.SESSION` namespace.

**Code path:** `DECLARE` → `CreateVariable` → `CreateVariableExec` → `SqlScriptingLocalVariableManager.create`
· reference → `VariableResolution.lookupVariable` → `VariableReference(FakeLocalCatalog, …)` ·
`SET VAR` → `SetVariableExec` → `…VariableManager.set`

**Anchor files:**

- [SqlScriptingLocalVariableManager.scala:29](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingLocalVariableManager.scala#L29) — the whole manager: `create`, `set`, `get`, `qualify`, `remove`, `clear`
- [SqlScriptingLocalVariableManager.scala:72](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingLocalVariableManager.scala#L72) — `findScopeOfVariable`: one name part means "search by name", two mean "`label.name`, search by scope label"
- [SqlScriptingLocalVariableManager.scala:97](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingLocalVariableManager.scala#L97) — `remove` throws `internalError`: **local variables cannot be dropped**, only scoped out
- [VariableResolution.scala:131](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/VariableResolution.scala#L131) — `lookupVariable`: local first, session as fallback, with `session.x` and any 3-part name forced to the session manager
- [CreateVariableExec.scala:44](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/command/v2/CreateVariableExec.scala#L44) — the same fork at execution time: local manager if `FakeLocalCatalog`, otherwise `tempVariableManager`
- [SqlScriptingContextManagerImpl.scala:26](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingContextManagerImpl.scala#L26) — the three-line class that binds the two together
- [ResolveCatalogs.scala:80](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/ResolveCatalogs.scala#L80) — `DROP TEMPORARY VARIABLE` inside a script raises `UNSUPPORTED_FEATURE.SQL_SCRIPTING_DROP_TEMPORARY_VARIABLE`

**Configs:** `spark.sql.caseSensitive` (name normalisation)

**Maps to topics:** I12

!!! warning "A local variable shadows a session variable of the same name, and you cannot unshadow it by dropping"

    `lookupVariable` consults the scripting manager first and only falls back to the session
    manager. Inside a script, `DECLARE x` therefore hides an existing session `x` for the rest of the
    block — and `DROP TEMPORARY VARIABLE` is rejected outright inside a script, so there is no way
    back except leaving the scope. To reach the session variable explicitly, qualify it:
    `session.x` (or `system.session.x`), which `lookupVariable` routes past the local manager via
    `isForbiddenLabelOrForVariableName`.

## Control flow — loops, branches, and LEAVE/ITERATE label propagation

**What it is:** each construct is a small state machine alternating between a `Condition` state and a
`Body` state. `LEAVE` and `ITERATE` are *returned as statements* and propagate up the iterator tree,
each enclosing node checking whether its own label matches and setting `hasBeenMatched` when it does.

**Anchor files:**

- [SqlScriptingExecutionNode.scala:434](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L434) — `IfElseStatementExec` (`IF`/`ELSEIF`/`ELSE`, one condition query per clause)
- [SqlScriptingExecutionNode.scala:520](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L520) — `WhileStatementExec`; [:863](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L863) `RepeatStatementExec`, which starts in the `Body` state and exits when the condition is **true** (`UNTIL`, not `WHILE`); [:1005](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L1005) `LoopStatementExec`, which has no condition at all and can only be left by `LEAVE`
- [SqlScriptingExecutionNode.scala:626](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L626) — `SearchedCaseStatementExec`; [:714](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L714) `SimpleCaseStatementExec`
- [SqlScriptingExecutionNode.scala:964](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L964) — `LeaveStatementExec` and [:984](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L984) `IterateStatementExec`, both carrying only a label and a `hasBeenMatched` flag
- [SqlScriptingExecutionNode.scala:388](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L388) — `CompoundBodyExec.handleLeaveStatement`, the propagation step: stop iterating, exit the scope, then check the label
- [SqlBaseParser.g4:102](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4#L102) — the `compoundStatement` alternatives, i.e. the definitive list of what may appear in a script body

**Maps to topics:** I12

!!! info "`LEAVE` unwinds by *returning*, which is why it exits scopes correctly"

    A `LEAVE outer` deep in a nest is handed back from `next()` as an ordinary statement. Each
    enclosing `CompoundBodyExec` sees it, sets `stopIteration = true`, calls `exitScope()`, and
    checks whether *its* label matches; if not, the statement keeps travelling up and the next block
    does the same. So the scope teardown is a natural consequence of the unwinding, not a separate
    cleanup pass. `ITERATE` uses the same mechanism but is rejected on a plain compound
    (`INVALID_LABEL_USAGE.ITERATE_IN_COMPOUND`) since there is nothing to iterate.

## FOR — a per-row variable scope over a driver-side iterator

**What it is:** `FOR row AS SELECT … DO … END FOR` runs the query, pulls rows to the driver one at a
time, and for each row synthesises a fresh scope containing one local variable per result column
(plus, if named, a struct-valued variable of that name). Those variables are created by *generating*
`DECLARE` and `SET` statements and prepending them to the body.

**Code path:** `ForStatementExec.cachedQueryResult()` → `df.toLocalIterator()` → per row:
`createDeclareVarExec` + `createSetVarExec` → new `CompoundBodyExec(isScope = true)` → body

**Anchor files:**

- [SqlScriptingExecutionNode.scala:1068](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L1068) — `ForStatementExec`
- [SqlScriptingExecutionNode.scala:1088](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L1088) — `cachedQueryResult()`: `toLocalIterator()`, evaluated once, on first use
- [SqlScriptingExecutionNode.scala:1153](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L1153) — the per-row variable construction, and [:1173](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L1173) the `NoOpStatementExec` appended to the body
- [SqlScriptingExecutionNode.scala:1234](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L1234) — `createExpressionFromValue`, which turns a Scala value back into a Catalyst expression, recursing through `Map` → `CreateMap`, `Row` → `CreateNamedStruct`, `Seq` → `CreateArray`
- [SqlScriptingExecutionNode.scala:1220](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L1220) — `handleLeaveStatement`, which must call `exitScope()` explicitly because the normal exit point is skipped

**Maps to topics:** I12

!!! warning "`FOR` is a driver-side loop, and every row costs at least two Spark jobs"

    `toLocalIterator()` fetches one partition at a time to the driver, so the loop does not OOM the
    way `collect()` would — but each iteration then executes a generated `DECLARE` and a generated
    `SET` as full statements before your body runs, and every statement in the body is its own
    query. A `FOR` over a million-row result is a million driver-side round trips. Its use is
    per-*group* orchestration (loop over table names, over partitions to process), never per-record
    data processing.

!!! info "The loop variable is a struct; the columns are also bound individually"

    Naming the loop variable (`FOR r AS SELECT a, b …`) makes `r` the scope label, so `r.a` resolves
    as a qualified local variable. The columns `a` and `b` are *also* declared as bare locals in that
    scope. Without a name, only the bare columns exist and the scope carries a UUID label.

## Declaration ordering — the compound-body state machine

**What it is:** SQL/PSM requires declarations at the top of a block, in a fixed order. Spark enforces
it with a monotone state machine in the parser: `INIT` → `VARIABLE`/`CONDITION` → `CURSOR` →
`HANDLER` → `STATEMENT`, with transitions only ever moving forward.

**Anchor files:**

- [ParserUtils.scala:153](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParserUtils.scala#L153) — `CompoundBodyParsingContext` and its `transitionTo`
- [AstBuilder.scala:338](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala#L338) — `visitCompoundBodyImpl`, which drives the state machine while walking the block
- [SqlBaseParser.g4:128](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4#L128) — `declareConditionStatement`; [:143](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4#L143) `declareHandlerStatement`
- [SqlScriptingErrors.scala:66](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/errors/SqlScriptingErrors.scala#L66) — the errors this produces: `INVALID_VARIABLE_DECLARATION.{NOT_ALLOWED_IN_SCOPE, ONLY_AT_BEGINNING}`, `INVALID_ERROR_CONDITION_DECLARATION.NOT_AT_START_OF_COMPOUND_STATEMENT`, `INVALID_CURSOR_DECLARATION`, `INVALID_HANDLER_DECLARATION.WRONG_PLACE_OF_DECLARATION`

**Maps to topics:** I12

!!! info "Variables can only be declared in a *scope*, not in every compound body"

    `CompoundBody` carries an `isScope` flag, and only scoped bodies accept declarations. A handler
    body written as a bare statement, and the implicit bodies the parser builds for loop bodies, are
    `isScope = false` — declaring a variable there raises
    `INVALID_VARIABLE_DECLARATION.NOT_ALLOWED_IN_SCOPE`. Wrapping the body in its own
    `BEGIN … END` makes it a scope and the declaration legal.

## Condition handlers — EXIT, CONTINUE, and how a handler is chosen

**What it is:** `DECLARE {EXIT | CONTINUE} HANDLER FOR <conditions> <body>` inside a block. When a
statement throws a `SparkThrowable`, the executor searches for a handler, pushes the handler's body
as a **new frame**, and resumes. An `EXIT` handler, once its body finishes, injects a `LEAVE` for the
block it was declared in; a `CONTINUE` handler simply pops and execution resumes after the failing
statement.

**Code path:** statement throws → `SqlScriptingExecution.getNextResult` catch →
`handleException` → `context.findHandler(condition, sqlState)` → push
`SqlScriptingExecutionFrame(handler.body, EXIT_HANDLER | CONTINUE_HANDLER)` → … → frame exhausted →
`injectLeaveStatement` (EXIT) or `interruptConditionalStatements` (CONTINUE)

**Anchor files:**

- [SqlScriptingExecution.scala:239](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala#L239) — `handleException`, including the unhandled-condition rule
- [SqlScriptingExecution.scala:146](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala#L146) — `getNextStatement`, where an exhausted handler frame is popped and its aftermath applied
- [SqlScriptingExecution.scala:79](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala#L79) — `injectLeaveStatement`: descend to the current leaf and *replace* the next statement with a `LeaveStatementExec`
- [SqlScriptingExecution.scala:95](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala#L95) — `interruptConditionalStatements`, the CONTINUE counterpart, with the three different "was the condition being evaluated?" flags it has to consult
- [SqlScriptingExecutionContext.scala:495](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala#L495) — `SqlScriptingExecutionScope.findHandler`, the four-step precedence
- [SqlScriptingExecutionContext.scala:419](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala#L419) — the frame-level search, and the `firstHandlerScopeLabel` bookkeeping that stops a handler catching its own exception
- [SqlScriptingExecutionNode.scala:226](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L226) — `TriggerToExceptionHandlerMap`; [:1306](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L1306) `ExceptionHandlerExec`
- [SqlScriptingInterpreter.scala:83](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingInterpreter.scala#L83) — where the map is built, and where duplicate handlers for the same condition or SQLSTATE are rejected
- [AstBuilder.scala:298](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala#L298) — `visitDeclareHandlerStatementImpl`, including the `CONTINUE` feature gate

**Configs:** `spark.sql.scripting.continueHandlerEnabled` (**false** by default — `CONTINUE HANDLER`
raises `UNSUPPORTED_FEATURE.CONTINUE_EXCEPTION_HANDLER` unless you turn it on)

**Maps to topics:** *(none)* → proposes **I31**

**Handler selection, in order** ([SqlScriptingExecutionContext.scala:495](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala#L495)):

1. a handler for the exact error condition (`DIVIDE_BY_ZERO`);
2. failing that, and only if the condition has a subclass, a handler for its **main class**
   (`TABLE_OR_VIEW_NOT_FOUND` for `TABLE_OR_VIEW_NOT_FOUND.SOMETHING`);
3. failing that, a handler for the exception's SQLSTATE;
4. failing that, `NOT FOUND` — but only for SQLSTATE class `02`;
5. failing that, `SQLEXCEPTION` — but **not** for SQLSTATE classes `XX` or `02`.

Scopes are searched innermost-first within the frame, then outward through frames.

!!! warning "An unhandled `02` condition does not fail the script — it is swallowed"

    [SqlScriptingExecution.scala:256](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala#L256):
    when no handler matches, Spark checks the SQLSTATE. Class `02` ("no data") is a *completion*
    condition per the SQL standard, so execution simply continues past the statement that raised it.
    Everything else is resignalled — rethrown — and kills the script.

    In practice this matters for exactly one thing today: `CURSOR_NO_MORE_ROWS` is SQLSTATE `02000`.
    A `FETCH` past the end of a cursor with no `NOT FOUND` handler declared **does nothing and does
    not stop the loop** — the target variables keep their previous values. That is an infinite loop
    waiting to happen, and it is the reason cursors and `CONTINUE HANDLER FOR NOT FOUND` are really
    one feature.

!!! warning "`XX` errors bypass `SQLEXCEPTION` on purpose"

    SQLSTATE class `XX` is Spark's internal-error class. A catch-all
    `DECLARE EXIT HANDLER FOR SQLEXCEPTION` deliberately does **not** catch it, so an internal bug
    cannot be silently absorbed by a script's error handling. Neither does it catch `02`, which
    would otherwise turn every end-of-cursor into an error path.

!!! info "What may appear in a handler's trigger list, and when it is validated"

    Three rules, all enforced in the parser
    ([AstBuilder.scala:212](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala#L212)):

    - **`SQLEXCEPTION` and `NOT FOUND` cannot be combined with anything else.** Either one in a
      trigger list containing any named condition or SQLSTATE raises
      `INVALID_HANDLER_DECLARATION.INVALID_CONDITION_COMBINATION`.
    - **A user SQLSTATE must be exactly five alphanumerics and may not start `00`, `01` or `XX`**
      ([:202](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala#L202)) — success, warning, and internal-error classes are reserved. A bare
      `DECLARE c CONDITION` with no `FOR SQLSTATE` defaults to **`45000`**.
    - **Condition names are `^[A-Za-z0-9_]+$` and upper-cased**
      ([:271](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala#L271)), so a user condition can never contain a dot — which is
      what makes the two-stage name check below unambiguous.

    Name validation happens in **two** places. A *dotted* name is checked immediately against the
    built-in error classes, since it cannot be user-declared. An *undotted* name is deferred to
    compound-body assembly ([:358](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala#L358)), where it is accepted if it is either a
    built-in class or a condition declared in scope; otherwise
    `INVALID_HANDLER_DECLARATION.CONDITION_NOT_FOUND`. A misspelled condition name is therefore a
    parse error, not a handler that silently never fires.

!!! info "Declared conditions are scoped, and the declaration order is load-bearing"

    `SqlScriptingConditionContext` ([ParserUtils.scala:485](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParserUtils.scala#L485))
    is a parse-time map of condition name → SQLSTATE, added to as declarations are seen and
    **removed** when the block finishes ([AstBuilder.scala:393](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala#L393)),
    so a condition declared in an inner block is invisible outside it; a redeclaration in the same
    scope raises `DUPLICATE_CONDITION_IN_SCOPE`.

    This is why the declaration-ordering state machine matters: the comment at
    [AstBuilder.scala:353](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala#L353)
    — "All conditions are already visited when we encounter a handler" — is the invariant that lets
    a handler's condition names be resolved against declarations in a single forward pass. Ordering
    is a correctness requirement, not style.

!!! info "A CONTINUE handler has to decide whether the *condition* failed or the body did"

    If a `WHILE` condition throws and a CONTINUE handler catches it, resuming the loop would
    re-evaluate the same broken condition forever. So on popping a CONTINUE frame,
    `interruptConditionalStatements` walks to the current leaf and, if its parent is a conditional
    that was evaluating its condition, sets `interrupted = true` — skipping the construct entirely.
    Distinguishing "failed in the condition" from "failed in the body" needs three different flags,
    because each construct tracks it differently: `SimpleCaseStatementExec.hasStartedCaseVariableEvaluation`,
    `ForStatementExec.hasStartedQueryEvaluation`, and for the rest, `SingleStatementExec.isExecuted`
    ([SqlScriptingExecution.scala:110](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala#L110)).

## Cursors — DECLARE, OPEN, FETCH, CLOSE and the snapshot taken at OPEN

**What it is:** a named, scoped iterator over a query result, added in 4.2.0 behind an internal flag.
Unlike everything else in the scripting runtime, cursors are implemented as ordinary DSv2 **commands**
that reach into the scripting context through the thread-local, so they are planned and executed the
same way `DECLARE VARIABLE` is.

**Code path:** parse → `DeclareCursor` / `OpenCursor` / `FetchCursor` / `CloseCursor` (catalyst
`v2Commands.scala`) → analyzer `ResolveFetchCursor`, `ResolveCursors` resolve `UnresolvedCursor` to
`CursorReference` → `V2CommandStrategy` → the four `…CursorExec` operators → scope's
`cursors` / `cursorStates` maps

**Anchor files:**

- [CursorState.scala:28](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/CursorState.scala#L28) — the state machine: `CursorDeclared` → `CursorOpened(iterator, schema)` → `CursorFetching(…)` → `CursorClosed`
- [SqlBaseParser.g4:357](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4#L357) — the four statements, including `OPEN … USING (…)` and `FETCH [NEXT] [FROM] c INTO v1, v2`
- [AstBuilder.scala:7192](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala#L7192) — the cursor feature gate, repeated at each of the four visit methods
- [CursorReference.scala:33](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/CursorReference.scala#L33) — `CursorDefinition` (name + **unparsed query text**), [:45](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/CursorReference.scala#L43) `UnresolvedCursor`, [:68](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/CursorReference.scala#L68) `CursorReference`
- [ResolveCursors.scala:46](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/ResolveCursors.scala#L46) — `resolveCursor`: split a qualified `label.cursor`, normalise both halves by `spark.sql.caseSensitive`, then look up through the extension API. Wired in at [Analyzer.scala:582](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala#L582)
- [ResolveFetchCursor.scala:59](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/ResolveFetchCursor.scala#L59) — `resolveAndValidateTargetVariables`, and [:45](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/ResolveFetchCursor.scala#L45) `checkForDuplicateVariables`
- [ParameterizedQueryExecutor.scala:34](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/command/v2/ParameterizedQueryExecutor.scala#L34) — the binding trait shared with `EXECUTE IMMEDIATE`
- [v2ResolutionPlans.scala:317](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/v2ResolutionPlans.scala#L317) — `FakeLocalCatalog`, whose `name()` is `"local"` (`FakeSystemCatalog` at [:309](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/v2ResolutionPlans.scala#L309) is `"system"`) — the sentinel that tells every write path which variable manager owns the name
- [OpenCursorExec.scala:55](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/command/v2/OpenCursorExec.scala#L55) — parse the stored text, bind `USING` parameters, then `executedPlan.executeToIterator()`
- [FetchCursorExec.scala:49](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/command/v2/FetchCursorExec.scala#L49) — one row per call, ANSI store-assignment casts, and the multi-column-into-one-struct special case at [:222](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/command/v2/FetchCursorExec.scala#L222)
- [SqlScriptingExecutionContext.scala:223](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala#L223) — `updateCursorState`, which must find the *frame* owning the cursor before mutating it
- [SqlScriptingExecutionContextExtension.scala:120](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/SqlScriptingExecutionContextExtension.scala#L120) — the two-method interface catalyst uses to look cursors up without depending on sql/core

**Configs:** `spark.sql.scripting.cursorEnabled` (**false**), `spark.sql.caseSensitive`

**Maps to topics:** *(none)* → proposes **I32**

!!! warning "The snapshot is taken at OPEN, and OPEN is where the job starts"

    `OpenCursorExec` calls `executeToIterator()` — not merely `executedPlan` — and the comment at
    [OpenCursorExec.scala:36](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/command/v2/OpenCursorExec.scala#L36)
    says why: this is the only point at which Spark performs file discovery and captures the table
    snapshot. So `OPEN` does real work and fixes what will be read; a table modified between `OPEN`
    and the last `FETCH` does not affect the cursor. All cursors behave as `INSENSITIVE` in 4.2.0
    regardless of whether `ASENSITIVE` was written.

    The iterator itself is still lazy, so `OPEN` does not materialise the result — but a cursor left
    open holds an in-flight execution.

!!! warning "Existence is checked in the analyzer; *state* is checked at runtime"

    The two halves fail in different phases, and conflating them is easy — an earlier version of
    this page did.

    **Analysis.** `ResolveCursors` rewrites `UnresolvedCursor` → `CursorReference` by looking the
    cursor up in the scripting context, and raises `CURSOR_OUTSIDE_SCRIPT` (0A000) when there is no
    context at all or `CURSOR_NOT_FOUND` when the name does not resolve. `ResolveFetchCursor`
    resolves `FETCH`'s target variables in the same phase, raising `DUPLICATE_ASSIGNMENTS` for
    `FETCH c INTO v, v`.

    **Execution.** Everything about the *lifecycle* is thrown from a `…Exec.run()` method:
    `CURSOR_ALREADY_EXISTS` (42723), `CURSOR_ALREADY_OPEN`, `CURSOR_NOT_OPEN` (24501), and
    `CURSOR_NO_MORE_ROWS` (**02000**). A script that opens a cursor twice fails on the second `OPEN`
    at that point in execution, not before the script starts.

!!! warning "Cursor analysis is not pure — it reads a live, mutating stack"

    `ResolveCursors` reaches the cursor table through `SqlScriptingContextManager.get()`, a
    `LexicalThreadLocal` pointing at the *running* frame/scope stack. So whether a given `FETCH`
    statement analyzes at all depends on which scopes happen to be open at the instant that
    statement is reached — which is the only way it could work, since each script statement is
    analyzed separately at execution time. The practical consequence: a cursor reference inside a
    branch that never runs is never analyzed, so a typo in a cursor name in a dead `IF` branch does
    not fail the script.

!!! info "`FETCH … INTO` can target a session variable"

    `ResolveFetchCursor` resolves targets through `VariableResolution.lookupVariable`, the same
    lookup ordinary references use — script-locals first, session variables as a fallback. So
    `FETCH c INTO session_var` is legal, and a fetch can write out of the script's own scope. Each
    resolved reference is marked `canFold = false` so the assignment target cannot be
    constant-folded away.

!!! info "`OPEN … USING` re-enters `SparkSession.sql`"

    `OpenCursorExec` mixes in `ParameterizedQueryExecutor`, whose `executeParameterizedQuery`
    binds the `USING` arguments and then calls the public
    [`SparkSession.sql`](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/command/v2/ParameterizedQueryExecutor.scala#L63)
    on the stored cursor text, taking `queryExecution.analyzed`. The same trait serves
    `EXECUTE IMMEDIATE`, which is what makes parameter binding identical between the two — and it
    means the cursor query is parsed under whatever configs are live at `OPEN`, not at `DECLARE`.

!!! info "Cursor names, like variable names, are frozen by `spark.sql.caseSensitive` at declare time"

    `DeclareCursorExec` normalises the name using the *current* setting and stores the normalised
    form. Flipping `spark.sql.caseSensitive` in the middle of a script makes an already-declared
    cursor unfindable. The same applies to the `queryText`, which is stored raw and re-parsed at
    `OPEN` under whatever configs are in force *then*.

!!! info "The canonical cursor loop needs three features at once"

    A `FETCH`-driven loop needs the cursor flag, the continue-handler flag, and a `NOT FOUND`
    handler — because end-of-data is signalled as SQLSTATE `02000` and the only way to observe it is
    to catch it. Both flags default to false in 4.2.0, so cursors are not usable out of the box.

## Parameters and SET — what a statement inside a script may be

**What it is:** `compoundStatement` is a *restricted* grammar rule, not "any SQL". It admits ordinary
`statement`s, the scripting constructs, and a scripting-only `SET` — and nothing else. Separately,
named parameters supplied to `spark.sql(script, args)` are threaded down to every statement in the
script rather than bound once at the top.

**Anchor files:**

- [SqlBaseParser.g4:102](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4#L102) — the `compoundStatement` alternatives
- [SqlBaseParser.g4:118](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4#L118) — `setStatementInsideSqlScript`: `SET <assignments>` and `SET (a, b) = (SELECT …)`, both variable assignment
- [SqlBaseParser.g4:185](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4#L185) — `singleStatement`, the *only* rule besides `EXPLAIN` that reaches `setResetStatement`
- [SqlScriptingExecutionNode.scala:174](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L174) — `preparedPlan`: every `SingleStatementExec` wraps its plan in `NameParameterizedQuery(parsedPlan, args)` when `args` is non-empty, so the same parameter map is re-applied per statement
- [SqlScriptingInterpreter.scala:50](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingInterpreter.scala#L50) — `args` is threaded through every `transformTreeIntoExecutable` case, including loop conditions and `FOR` queries

**Maps to topics:** I12

!!! warning "You cannot change a config inside a SQL script"

    `setResetStatement` — the rule behind `SET spark.sql.…`, `SET -v` and `RESET` — is reachable only
    from `singleStatement` and from `EXPLAIN`. It is **not** an alternative of `compoundStatement`.
    Inside a `BEGIN … END`, `SET` therefore always means `setStatementInsideSqlScript`, i.e. variable
    assignment, and `SET spark.sql.shuffle.partitions = 200` parses as an assignment to a four-part
    *variable* name — which then fails as an unresolved variable rather than setting anything.

    Set your configs before invoking the script, or from the session that calls it. This includes
    the three scripting feature gates themselves, which is a small trap: you cannot turn cursors on
    from inside the script that uses them.

!!! info "Named parameters reach every statement; positional parameters reach none"

    `spark.sql(script, Map("d" -> …))` passes the map to `SqlScriptingExecution`, and each
    `SingleStatementExec` re-wraps its own plan in a `NameParameterizedQuery`. So `:d` is usable in
    any statement of the script, including a `WHILE` condition or a `FOR` query — not just the first.
    Positional (`?`) parameters are rejected outright for scripts at
    [SparkSession.scala:547](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L547)
    with `UNSUPPORTED_FEATURE.SQL_SCRIPTING_WITH_POSITIONAL_PARAMETERS`, because there is no
    sensible way to distribute one positional list across many statements.

## Errors, line numbers, and the three feature gates

**What it is:** scripting has its own error factory and its own exception type, which prefixes the
message with the script line number — a script has no other way to say *where* it failed, since each
statement is planned separately.

**Anchor files:**

- [SqlScriptingErrors.scala:33](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/errors/SqlScriptingErrors.scala#L33) — the 25-error catalogue: label rules, variable-declaration placement, condition/handler declaration rules, duplicate handlers, and the three unsupported-feature gates
- [SqlScriptingException.scala:25](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/exceptions/SqlScriptingException.scala#L25) — the `{LINE:n} ` prefix, built from `Origin.line`
- [SqlScriptingExecutionNode.scala:187](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala#L187) — `getText`, which slices the original SQL text out of the `Origin` for error messages
- [SQLConf.scala:4947](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4947) — the three gates, side by side

**Maps to topics:** I12

!!! info "The gates are checked after parsing, not before"

    All three feature flags are tested in `AstBuilder`, i.e. the script has already been parsed into
    a tree when Spark decides to reject it. A syntax error therefore wins over
    `UNSUPPORTED_FEATURE.SQL_SCRIPTING`: with `spark.sql.scripting.enabled=false` a malformed script
    reports the syntax error, not the feature gate. The
    [types-parser sweep](sql-catalyst-types-parser.md) records the same pattern for the pipe
    operator.

## The edges — stored procedures, the spark-sql CLI, EXECUTE IMMEDIATE, Connect

**What it is:** four boundary behaviours that are easier to get wrong than the feature itself. Each
is a deliberate limitation in the 4.2.0 source, not an accident.

**Anchor files:**

- [SqlScriptingExecutionContext.scala:270](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala#L270) — a `TODO` for stored procedures, and [:296](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala#L296) a scaladoc referencing a `SQL_STORED_PROCEDURE` frame type that the enum at [:283](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala#L283) does not define
- [StringUtils.scala:261](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/StringUtils.scala#L261) — `isSqlScript`, which already recognises a `CREATE PROCEDURE … BEGIN … END` body
- [StringUtils.scala:285](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/StringUtils.scala#L285) — `splitSemiColonWithIndex(line, enableSqlScripting)`, whose scripting branch returns the whole input unsplit
- [SparkSQLCLIDriver.scala:619](https://github.com/apache/spark/blob/v4.2.0/sql/hive-thriftserver/src/main/scala/org/apache/spark/sql/hive/thriftserver/SparkSQLCLIDriver.scala#L619) — the only caller, passing `enableSqlScripting = false`
- [SparkSession.scala:547](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L547) — positional parameters are rejected for scripts; [:600](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L600) named parameters are passed through as `args` to the interpreter; [:696](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L696) `EXECUTE IMMEDIATE` of a script is rejected outright
- [ResolveExecuteImmediate.scala:205](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/catalyst/analysis/ResolveExecuteImmediate.scala#L205) — `EXECUTE IMMEDIATE` *inside* a script runs under a context manager that keeps the script's variable manager, so the executed statement can still see local variables
- [SparkConnectPlanner.scala:3189](https://github.com/apache/spark/blob/v4.2.0/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/planner/SparkConnectPlanner.scala#L3189) — Connect's SQL command calls the classic `session.sql`, so scripting works over Connect unchanged

**Maps to topics:** I12, E9

!!! warning "Stored procedures do not exist in 4.2.0, despite the scaffolding"

    `SqlScriptingExecutionContext` carries `TODO`s for them, `SqlScriptingFrameType`'s scaladoc names
    a frame type it does not have, and `StringUtils` has a regex for `CREATE PROCEDURE`. But the
    grammar has no `createProcedure` rule — `PROCEDURE` appears only in `SHOW PROCEDURES` and
    `DESCRIBE PROCEDURE`. Anything you read about Spark stored procedures is describing a vendor
    extension or a future release, not this one.

!!! warning "The `spark-sql` CLI cannot run a SQL script"

    `splitSemiColonWithIndex` has a scripting mode that keeps a `BEGIN … END` block intact, and
    `SparkSQLCLIDriver` — its only caller — passes `enableSqlScripting = false`. So the CLI splits a
    script on its internal semicolons and submits the fragments as separate statements, which fails.
    Scripts must go through `spark.sql(...)`, JDBC/Thrift, or Connect.

## Breadth check

**Package breadth.** The group's scope is one package with no sub-packages. All seven files are
cited above:

| File | Lines | Concepts citing it |
|---|---|---|
| `SqlScriptingExecutionNode.scala` | 1316 | execution tree, control flow, FOR, handlers |
| `SqlScriptingExecutionContext.scala` | 537 | frames/scopes, handler search, cursor state |
| `SqlScriptingExecution.scala` | 330 | result protocol, exception handling |
| `SqlScriptingInterpreter.scala` | 282 | interpretation, handler-map construction |
| `SqlScriptingLocalVariableManager.scala` | 107 | local variables |
| `CursorState.scala` | 65 | cursors |
| `SqlScriptingContextManagerImpl.scala` | 34 | local variables, context publication |

Outside the scope but required for the feature, and cited here rather than left to another sweep:
`sql/catalyst` — `SqlScriptingLogicalPlans.scala`, `SqlScriptingErrors.scala`,
`SqlScriptingException.scala`, `SqlScriptingContextManager.scala` (×2),
`SqlScriptingExecutionContextExtension.scala`, `CursorReference.scala`, `ResolveCursors.scala`,
`ResolveFetchCursor.scala`, `VariableResolution.scala`, `AstBuilder.scala`, `ParserUtils.scala`,
`SqlBaseParser.g4`; `sql/core` — `QueryExecution.scala`, `classic/SparkSession.scala`, the five
`command/v2` cursor files.

**Config breadth.** Three configs in the group's namespace, all attributed. Three near-misses
recorded as out of scope above.

**Re-sweep delta (2026-08-07).** Both breadth checks were already green and stayed green; the second
pass was a depth pass over four layers the first had cited without opening. It produced one
correction (cursor existence resolves in the analyzer, not at runtime), one new concept (parameters
and `SET`), and seven additional findings folded into existing concepts: session variables as `FETCH`
targets, `DUPLICATE_ASSIGNMENTS`, the shared parameter binder with `EXECUTE IMMEDIATE`, the reserved
label names, the user-SQLSTATE and condition-name rules, the `SQLEXCEPTION`/`NOT FOUND` combination
ban, and the two-stage condition-name validation. No concept mapped to a new topic.

**Not covered, deliberately:** the DSv2 command machinery the cursor operators sit on
(`LeafV2CommandExec`, `V2CommandStrategy`, `ParameterizedQueryExecutor`) belongs to the
[query-execution](sql-core-query-execution.md) group and is covered there; the ANTLR grammar and the
parse pipeline belong to [sql/catalyst — types-parser](sql-catalyst-types-parser.md).
