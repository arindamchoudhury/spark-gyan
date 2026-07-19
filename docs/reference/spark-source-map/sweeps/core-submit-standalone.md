---
subsystem: core
spark_version: "4.2.0"
swept_at: 2026-07-19
group: submit-standalone
all_groups: [rdd-layer, execution-engine, shuffle-memory, storage-serializer,
  submit-standalone, monitoring, config-security, rpc-resources, api-bridge]
status: complete
concepts:
  - name: master-url-resolution
    topics: [E2, B1]
  - name: deploy-mode-matrix
    topics: [E2, B1]
  - name: argument-precedence
    topics: [E2, B2]
  - name: prepare-submit-environment
    topics: [E2, B1]
  - name: main-class-selection
    topics: [B1, E2]
  - name: dependency-resolution
    topics: []
    propose:
      code: I18
      level: Intermediate
      title: "Dependency Management at Submit Time: --packages, Ivy, and Jars"
      what: "spark-submit resolves --packages through Apache Ivy before anything touches the classpath, using a fixed resolver chain (local m2, local ivy, Maven Central, spark-packages) that --repositories and spark.jars.ivySettings modify. Resolved jars merge into spark.jars, and for Python apps into spark.submit.pyFiles too."
      why: "--packages is how nearly every connector is loaded — Kafka, Delta, Iceberg, JDBC drivers, cloud filesystems — and its failure modes are unusually opaque. A resolution failure surfaces as a bare RuntimeException carrying Ivy's problem list with no coordinate context; a package whose artifact is a pom rather than a jar resolves 'successfully' and contributes nothing; a missing or remote jar is warned about and skipped, failing later as ClassNotFoundException. Behind a corporate firewall spark.jars.ivySettings is the only way through."
  - name: resource-staging
    topics: [E2]
  - name: run-main-and-classloader
    topics: [B1, B2]
  - name: rest-submission-and-fallback
    topics: [E2]
  - name: legacy-standalone-client
    topics: [E2]
  - name: exit-code-propagation
    topics: [E2, B1]
  - name: remote-and-connect-branch
    topics: [E9, B2]
  - name: proxy-user-impersonation
    topics: [E2]
  - name: worker-registration
    topics: [E2, B1]
  - name: application-registration
    topics: [E2, B1]
  - name: driver-placement
    topics: [E2, B1]
  - name: executor-allocation-arithmetic
    topics: [E2, B1]
  - name: executor-process-supervision
    topics: [B1, E2, E3]
  - name: driver-supervision-and-restart
    topics: [E2]
  - name: heartbeat-and-worker-timeout
    topics: [E2, E3]
  - name: executor-failure-accounting
    topics: [E2]
  - name: worker-decommissioning
    topics: [E2]
  - name: persistence-engines
    topics: [E2]
  - name: leader-election-and-ha
    topics: []
    propose:
      code: E16
      level: Expert
      title: "Standalone High Availability and Recovery"
      what: "Only ZooKeeper mode has real leader election. FILESYSTEM and ROCKSDB use MonarchyLeaderAgent, which declares the master leader unconditionally in its constructor. Recovery reads persisted apps, workers and drivers, waits spark.deploy.recoveryTimeout for them to check in, then removes whatever did not."
      why: "Three traps compound. FILESYSTEM mode on shared storage is what people reach for as 'HA without ZooKeeper' and is not HA — both masters believe they are leader. An unrecognised recoveryMode string falls silently into the no-HA default case. And RevokedLeadership exits with code 0, so a supervisor configured with Restart=on-failure will not restart the master, quietly halving your redundancy."
  - name: recovery-state-machine
    topics: [E2]
  - name: work-directory-cleanup
    topics: [E2, E3]
  - name: application-completion
    topics: [E2, B1]
  - name: standalone-observability
    topics: [E3, E2]
---

How an application gets from a `spark-submit` command line to running processes, and how the standalone cluster manager places and supervises them. Swept in two halves — the submission path, and the standalone Master/Worker.

!!! info "This group is the one a practitioner touches most and reads least"

    Everything here runs before any Spark code the user wrote. When it goes wrong the symptom is usually a stack trace with no Spark context at all, or a job that never starts.

---

## Master URL resolution

**What it is:** `spark-submit` turns `--master` into one of four integer constants by **prefix matching**, not a registry. The constants are bit flags (`YARN=1`, `STANDALONE=2`, `LOCAL=8`, `KUBERNETES=16`) so a later config table can test membership with a bitwise AND. There is no plugin point for a new cluster manager.

**Code path:** `SparkSubmit.main` → `doSubmit` → `parseArguments` → `prepareSubmitEnvironment` → `args.maybeMaster match`

**Anchor files:**

- [SparkSubmit.scala:254](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L254) — the prefix dispatch
- [SparkSubmit.scala:259](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L259) — `case m if m.startsWith("spark")` — matches bare `spark`, not only `spark://`
- [SparkSubmit.scala:266](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L266) — no `--master` silently means `LOCAL`
- [SparkSubmit.scala:1106](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L1106) — the cluster-manager bit constants
- [Utils.scala:2915](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/Utils.scala#L2915) — `checkAndGetK8sMasterUrl` rewrites `k8s://host:port` to `k8s://https://host:port`

**Configs:** `spark.master`, `spark.remote`

**Maps to topics:** E2, B1

---

## The deploy-mode rejection matrix

**What it is:** a fixed list of `(clusterManager, deployMode)` pairs rejected outright. Every rejection calls `error()`, which **throws `SparkException`** rather than exiting with a usage code — so these surface as stack traces, not clean errors.

**Anchor files:**

- [SparkSubmit.scala:298](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L298) — the fail-fast match
- [SparkSubmit.scala:299](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L299) — standalone + cluster + Python rejected; `:302` R; `:305` `(LOCAL, CLUSTER)`; `:307` shells
- [SparkSubmit.scala:313](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L313) — Connect server in cluster mode allowed **only** under YARN
- [SparkSubmit.scala:1068](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L1068) — `error(msg) = throw new SparkException(msg)`

**Maps to topics:** E2, B1

---

## Argument precedence

**What it is:** a four-stage pipeline whose precedence is a consequence of *ordering* plus `if (!sparkProperties.contains(k))` guards. `--conf` wins over `--properties-file`, which wins over `--extra-properties-file`, which wins over `spark-defaults.conf` — and `spark-defaults.conf` is **skipped entirely** once `--properties-file` is given, unless `--load-spark-defaults` is passed.

**Code path:** `parse` → `mergeDefaultSparkProperties` → `ignoreNonSparkProperties` → `loadEnvironmentArguments` → `validateArguments`

**Anchor files:**

- [SparkSubmitArguments.scala:94](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmitArguments.scala#L94) — the four-call constructor pipeline
- [SparkSubmitArguments.scala:118](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmitArguments.scala#L118) — the `if (!contains(k))` guard that makes earlier sources win
- [SparkSubmitArguments.scala:168](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmitArguments.scala#L168) — `spark-defaults.conf` gated on `propertiesFile == null || loadSparkDefaults`
- [SparkSubmitArguments.scala:176](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmitArguments.scala#L176) — non-`spark.` keys dropped with a warning
- [SparkSubmit.scala:867](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L867) — the final `setIfMissing` for `args.sparkProperties`

!!! warning "`--conf` cannot override what the option table already set"

    `--conf` values are applied **last**, via `setIfMissing`. Any key the `OptionAssigner` table already wrote — including `spark.jars`, `spark.files`, `spark.master` and `spark.app.name` — is immune. A `--conf spark.jars=…` alongside `--jars` does not do what it looks like it does.

!!! info "A typo'd config namespace vanishes silently"

    Anything not starting with `spark.` is dropped before the env-var stage, with only a warning.

**Maps to topics:** E2, B2

---

## prepareSubmitEnvironment

**What it is:** the single method turning parsed arguments into `(childArgs, childClasspath, sparkConf, childMainClass)`. The core mechanism is the `OptionAssigner` table — entries carrying a value, a cluster-manager bitmask, a deploy-mode bitmask and a target config key, applied only where both masks match.

**Anchor files:**

- [SparkSubmit.scala:243](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L243) — the signature and documented 4-tuple
- [SparkSubmit.scala:644](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L644) — the `OptionAssigner` table
- [SparkSubmit.scala:750](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L750) — the bitmask application loop
- [SparkSubmit.scala:384](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L384) — Kerberos block, explicitly skipped for standalone

**Maps to topics:** E2, B1

---

## Main class selection

**What it is:** two different things share the name. `args.mainClass` is the *user's* entry point; `childMainClass` is what `runMain` reflects into. They are the same only in client mode — every cluster mode substitutes a manager-specific wrapper and demotes the user class to a command-line argument.

**Anchor files:**

- [SparkSubmit.scala:537](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L537) — reading `Main-Class` from the JAR manifest when `--class` is omitted
- [SparkSubmit.scala:562](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L562) — Python client mode substitutes `PythonRunner`; `:621` R substitutes `RRunner`
- [SparkSubmit.scala:818](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L818) — YARN cluster wrapper; `:838` Kubernetes

**Maps to topics:** B1, E2

---

## Dependency resolution

**What it is:** `--packages` is resolved through Apache Ivy at submit time, before anything touches the classpath. The resolver chain is fixed — local `~/.m2`, local Ivy cache, Maven Central, spark-packages — and `--repositories` becomes a *new* default resolver layered on top. Spark auto-excludes its own artifacts and `scala-library` so a user package cannot drag in a conflicting Spark.

**Code path:** `prepareSubmitEnvironment` → `DependencyUtils.resolveMavenDependencies` → `MavenUtils.buildIvySettings` → `resolveMavenCoordinates` → merge into `args.jars`

**Anchor files:**

- [SparkSubmit.scala:338](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L338) — resolution **skipped** for standalone cluster mode; the configs are forwarded so the driver resolves instead
- [MavenUtils.scala:251](https://github.com/apache/spark/blob/v4.2.0/common/utils/src/main/scala/org/apache/spark/util/MavenUtils.scala#L251) — auto-exclusion of `scala-library` and every `org.apache.spark:spark-*`
- [MavenUtils.scala:344](https://github.com/apache/spark/blob/v4.2.0/common/utils/src/main/scala/org/apache/spark/util/MavenUtils.scala#L344) — the default Ivy dir is `~/.ivy2.5.2`, **not** `~/.ivy2`
- [MavenUtils.scala:355](https://github.com/apache/spark/blob/v4.2.0/common/utils/src/main/scala/org/apache/spark/util/MavenUtils.scala#L355) — `--repositories` becomes the default resolver
- [MavenUtils.scala:539](https://github.com/apache/spark/blob/v4.2.0/common/utils/src/main/scala/org/apache/spark/util/MavenUtils.scala#L539) — `throw new RuntimeException(rr.getAllProblemMessages.toString)`

!!! warning "Three ways a dependency silently isn't there"

    A resolution failure throws a bare `RuntimeException` whose message is the `toString` of Ivy's problem list — no coordinate context. A package whose artifact is a `pom` or `bundle` rather than a `jar` is **filtered out at info level** and resolves "successfully" while contributing nothing. And `addJarToClasspath` warns-and-continues for a missing local jar *or any remote jar*, so the failure lands later as `ClassNotFoundException`.

**Configs:** `spark.jars.packages`, `.excludes`, `.repositories`, `.ivy`, `.ivySettings`

**Maps to topics:** none — proposed as I18

---

## Resource staging

**What it is:** jars, files, archives and pyFiles follow different routes per manager and land in different config keys. Client mode downloads remote resources locally first. YARN puts them in `spark.yarn.dist.*` rather than the generic keys.

**Anchor files:**

- [SparkSubmit.scala:409](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L409) — client-mode download block
- [SparkSubmit.scala:707](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L707) — **there is no `spark.jars` assigner for YARN** — it uses `spark.yarn.dist.jars` exclusively
- [DependencyUtils.scala:238](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/DependencyUtils.scala#L238) — `mergeFileLists` returns **null** on an empty merge

**Maps to topics:** E2

---

## runMain, the classloader, and SparkApplication

**What it is:** builds a `MutableURLClassLoader` (or child-first when `spark.driver.userClassPathFirst`), loads `childMainClass` by name, and branches: a `SparkApplication` gets `start(args, conf)` with the conf as an object; anything else is wrapped in `JavaMainApplication`, which **copies every conf entry into JVM system properties** before reflecting into `main`.

**Anchor files:**

- [SparkSubmit.scala:939](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L939) — `getSubmitClassLoader` and the child-first branch
- [SparkSubmit.scala:1014](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L1014) — the `SparkApplication` test
- [SparkApplication.scala:48](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkApplication.scala#L48) — `sys.props(k) = v`, the system-property leak
- [SparkSubmit.scala:1020](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L1020) — `findCause` unwrapping reflection wrappers so users see their own exception
- [SparkSubmit.scala:994](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L994) — targeted `ClassNotFoundException` hints for thriftserver and Connect

!!! info "That system-property leak is why `SparkApplication` exists"

    It propagates globally, which makes running two apps in one JVM undefined. It is also load-bearing — the legacy standalone client reads `sys.props` before the SparkConf.

**Maps to topics:** B1, B2

---

## REST submission and the legacy fallback

**What it is:** standalone cluster mode has two gateways — REST (`POST /v1/submissions/create`) and the legacy RPC client. `submit` is `@tailrec`: it tries REST, and on a `SubmitRestConnectionException` flips `useRest = false` and recurses.

**Anchor files:**

- [SparkSubmit.scala:218](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L218) — the fallback catch and recursion
- [SparkSubmitArguments.scala:103](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmitArguments.scala#L103) — client-side `useRest` defaults to **`"false"`**
- [package.scala:2048](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/internal/config/package.scala#L2048) — server-side `MASTER_REST_SERVER_ENABLED` defaults to **`true`**
- [RestSubmissionClient.scala:372](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/rest/RestSubmissionClient.scala#L372) — only connection failures become `SubmitRestConnectionException`
- [RestSubmissionClient.scala:428](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/rest/RestSubmissionClient.scala#L428) — the base URL is hardcoded `http://`; there is no HTTPS submission path

!!! warning "One config key, two different defaults"

    `spark.master.rest.enabled` is read client-side with a default of `"false"` and server-side with a default of `true`. So a plain `--master spark://… --deploy-mode cluster` takes the **legacy** path in 4.2.0, even though the in-source comment still calls REST "the default behavior as of Spark 1.3". The fallback machinery only runs if you opt in.

!!! info "Only unreachability falls back"

    A protocol error — malformed response, version mismatch, HTTP 500 with a non-JSON body — is a `SubmitRestProtocolException`, not a connection exception, so it propagates and fails the submit.

**Maps to topics:** E2

---

## The legacy standalone client and exit codes

**What it is:** the non-REST gateway stands up its own `RpcEnv` and sends `RequestSubmitDriver` to every master concurrently. It calls `System.exit` directly from RPC callback threads, unrelated to `SparkSubmit`'s exit-code machinery.

**Anchor files:**

- [Client.scala:189](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/Client.scala#L189) — `if (!waitAppCompletion) { … System.exit(0) }`
- [Client.scala:76](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/Client.scala#L76) — reads `sys.props` **before** the SparkConf
- [SparkSubmit.scala:1030](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L1030) — `var exitCode: Int = 1`, the pessimistic default
- [SparkSubmit.scala:1039](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L1039) — only a `SparkUserAppException` sets a specific code
- [SparkExitCode.scala:49](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/SparkExitCode.scala#L49) — `CLASS_NOT_FOUND = 101`

!!! warning "In cluster mode, exit code 0 means 'accepted', not 'succeeded'"

    By default `spark.standalone.submit.waitAppCompletion` is false, so the client exits 0 as soon as it sees any driver state. Only client mode propagates the user's own exit code. CI that gates on `spark-submit`'s return value is not checking what it thinks it is.

**Maps to topics:** E2, B1

---

## Worker registration

**What it is:** a Worker mints its own ID locally, then sends `RegisterWorker` to every configured master in parallel and takes whichever responds. Re-registration deliberately targets only the *known active* master (SPARK-4592) because unconditional fan-out races a takeover.

**Anchor files:**

- [Worker.scala:323](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala#L323) — `tryRegisterAllMasters`, one task per master
- [Worker.scala:346](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala#L346) — `reregisterWithMaster` and the SPARK-4592 rationale; `System.exit(1)` when retries are exhausted
- [Master.scala:1000](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L1000) — `registerWorker` purging DEAD and UNKNOWN workers at the same address
- [WorkerInfo.scala:99](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/WorkerInfo.scala#L99) — a deserialized `WorkerInfo` arrives with **zero resource accounting**, rebuilt only by the worker's state response

**Maps to topics:** E2, B1

---

## Driver placement and executor allocation

**What it is:** `schedule()` runs after every state change: waiting drivers first, then executors. `spark.deploy.spreadOutApps` changes **exactly one thing** — whether the inner allocation loop may iterate more than once on the same worker before moving on.

**Anchor files:**

- [Master.scala:936](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L936) — `schedule()` is a **no-op unless state is ALIVE**
- [Master.scala:794](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L794) — the allocation loop; `if (spreadOutApps) keepScheduling = false` is the entire effect
- [Master.scala:747](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L747) — `coresPerExecutor` unset → `minCoresPerExecutor = 1`, `oneExecutorPerWorker = true`
- [Master.scala:765](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L765) — memory and resource checks run **only on the first core** when one-executor-per-worker
- [Master.scala:836](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L836) — a remainder smaller than `coresPerExecutor` is never allocated
- [Master.scala:965](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L965) — the "requires more resource than any Worker" warning fires **only when the cluster is idle**

!!! warning "`spark.deploy.defaultCores` defaults to unlimited"

    An app that does not set `spark.cores.max` claims **every core in the cluster**. Later apps get nothing, and the `appMayHang` diagnostic is suppressed whenever more than one app is waiting — so the most common standalone complaint has no warning attached to it.

**Configs:** `spark.deploy.spreadOutApps`, `.spreadOutDrivers`, `.defaultCores`, `.workerSelectionPolicy`, `.maxDrivers`

**Maps to topics:** E2, B1

---

## Process supervision: executors and drivers

**What it is:** `ExecutorRunner` and `DriverRunner` fork OS processes and block on `waitFor()`. Executor output goes through a rolling `FileAppender`; driver output does not.

**Anchor files:**

- [ExecutorRunner.scala:183](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/worker/ExecutorRunner.scala#L183) — process start plus two `FileAppender`s
- [ExecutorRunner.scala:97](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/worker/ExecutorRunner.scala#L97) — a 10 s terminate timeout, then "This process will likely be orphaned"
- [DriverRunner.scala:224](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala#L224) — `runCommandWithRetry`: **no retry cap** under `spark.driver.supervise`
- [CommandUtils.scala:113](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/worker/CommandUtils.scala#L113) — driver streams use a plain appending `FileOutputStream` with **no rolling**
- [Master.scala:1113](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L1113) — SPARK-19900: a relaunched driver gets a **new ID** so a stale KILLED status cannot delete it

!!! warning "A long-lived supervised driver fills the disk silently"

    Executor logs roll; driver logs do not. Nothing removes `workDir/driverId` while the driver is running, so a driver up for weeks accumulates an unbounded `stdout`/`stderr`.

!!! info "An orphaned executor still holds its cores in the OS"

    If terminate times out, the Master's accounting has already released the resources while the process is still running. One WARN line is the only trace.

**Maps to topics:** B1, E2, E3

---

## Heartbeats and worker timeout

**What it is:** one config, `spark.worker.timeout`, drives two independent timers in two different processes — the worker heartbeats every `timeout/4`, the master sweeps every `timeout`.

**Anchor files:**

- [Worker.scala:96](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala#L96) — the interval, read from the **worker's** conf
- [Master.scala:62](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L62) — the timeout, read from the **master's** conf
- [Master.scala:1301](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L1301) — `timeOutDeadWorkers` and the reaper delay

!!! warning "Setting the timeout on one side only is undetectable"

    Raise it on the master alone and dead workers linger; raise it on the worker alone and healthy workers get reaped. Nothing cross-checks the two values. Detection latency is also between 1× and 2× the timeout, because the sweep runs at fixed rate.

**Maps to topics:** E2, E3

---

## Executor failure accounting

**What it is:** an app is removed after `spark.deploy.maxExecutorRetries` abnormal executor exits — but only if **no executor is currently RUNNING**, and the counter resets to zero every time an executor reaches RUNNING.

**Anchor files:**

- [Master.scala:563](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L563) — the four-part removal condition
- [Master.scala:543](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L543) — `resetRetryCount()` on LAUNCHING → RUNNING
- [Worker.scala:811](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala#L811) — SPARK-34245: state sync uses `ask` not `send`, and the worker `System.exit(1)`s after 5 consecutive failures

!!! info "A flapping network reaps apps through the crash counter"

    Decommissioned executors are exempt, but an executor lost to a *timed-out* worker arrives as `LOST` with no exit status, which counts as abnormal. So network instability and genuine crashes share one budget.

**Maps to topics:** E2

---

## Worker decommissioning

**What it is:** a graceful drain — the worker stops accepting work and the Master tells drivers to write off that host's shuffle data, so the fetch-failure cascade that follows a hard kill is avoided.

**Anchor files:**

- [Worker.scala:74](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala#L74) — the `SIGPWR` handler, registered **only** when `spark.decommission.enabled`
- [Worker.scala:899](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala#L899) — `decommissionSelf` logs and does nothing when the feature is disabled
- [Master.scala:1055](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L1055) — executors reported with `Some(worker.host)` so the driver unregisters that host's map output

!!! warning "The feature is off by default, so `SIGPWR` just kills the process"

    `spark.decommission.enabled` defaults to false, which means the signal handler is never installed. A rolling restart that sends `SIGPWR` expecting a drain gets a hard kill and pays for the recomputation.

!!! info "A decommissioned worker refuses executors but not drivers"

    `LaunchExecutor` checks the flag; `LaunchDriver` does not.

**Maps to topics:** E2

---

## Persistence, leader election, and recovery

**What it is:** four `PersistenceEngine` implementations behind one interface, selected by `spark.deploy.recoveryMode`. Only apps, drivers and workers are persisted. Only ZooKeeper mode has real leader election.

**Anchor files:**

- [Master.scala:177](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L177) — the mode dispatch, and the `case _` catch-all that yields **no HA and no error**
- [LeaderElectionAgent.scala:40](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/LeaderElectionAgent.scala#L40) — `MonarchyLeaderAgent` calls `electedLeader()` in its **constructor body**
- [Master.scala:252](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L252) — `RevokedLeadership` → `System.exit(0)`
- [Master.scala:67](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L67) — `recoveryTimeout` **defaults to `spark.worker.timeout`**
- [Master.scala:640](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L640) — `completeRecovery` removes every worker still UNKNOWN
- [ZooKeeperPersistenceEngine.scala:69](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/ZooKeeperPersistenceEngine.scala#L69) — a record that fails to deserialize is **deleted** with a warning

!!! warning "FILESYSTEM mode is not HA"

    It is what people reach for as "HA without ZooKeeper". `MonarchyLeaderAgent` makes **both** masters leader; they both accept registrations, and the persistence engine throws when the second writes a key the first already wrote. The failure is partial and confusing rather than immediate.

!!! warning "Losing leadership exits with code 0"

    A supervisor configured `Restart=on-failure` treats that as a clean shutdown and does not restart the master — silently reducing redundancy exactly when you need it.

!!! info "Recovery removes slow workers, not just dead ones"

    `recoveryTimeout` defaults to `spark.worker.timeout` (60 s). A large cluster whose workers cannot all re-register in 60 s loses the stragglers on every failover, with their executors reported LOST though the processes are still running. Raising it independently is the fix, and is why the config was split out in 4.0.0.

**Configs:** `spark.deploy.recoveryMode`, `.recoveryDirectory`, `.recoveryTimeout`, `.zookeeper.url`, `.zookeeper.dir`

**Maps to topics:** none for the HA half — proposed as E16

---

## Work directory cleanup and observability

**What it is:** two independent cleaners — a periodic age-based sweep of `workDir`, and an event-driven cleanup of an app's local dirs on completion. Standalone exposes four master gauges and five worker gauges, and nothing else.

**Anchor files:**

- [Worker.scala:509](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala#L509) — the cleanup task is scheduled **only inside `handleRegisterResponse`**
- [Worker.scala:541](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala#L541) — the TTL test is on **file mtime**, not app completion time
- [MasterSource.scala:23](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/MasterSource.scala#L23) — exactly four gauges: `workers`, `aliveWorkers`, `apps`, `waitingApps`
- [Master.scala:511](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/deploy/master/Master.scala#L511) — the readiness probe reports ready while RECOVERING, when `schedule()` is a no-op

!!! warning "The states that matter most have no metric"

    Recovery state, per-app retry count, DEAD-but-not-culled workers and driver counts are all absent from the metrics system. Alerting on standalone health means scraping the master's JSON endpoint. And a worker that never successfully registers never schedules its cleanup task at all.

**Maps to topics:** E3, E2

---

## Sweep log

| Date | Spark | What changed |
|---|---|---|
| 2026-07-19 | 4.2.0 | Initial sweep, in two halves (submission path; standalone Master/Worker). 27 concepts. Two gaps proposed: I18 (submit-time dependency management) and E16 (standalone HA and recovery). Two further gaps folded into existing topics rather than proposed, and the folding was done: submission-time config precedence into **B2**, and worker decommissioning into **E2**. Four high-consequence claims were verified at source before writing — the client/server split default on `spark.master.rest.enabled`, `RevokedLeadership` exiting 0, `MonarchyLeaderAgent` electing in its constructor, and `spark.deploy.defaultCores` being unlimited. |
