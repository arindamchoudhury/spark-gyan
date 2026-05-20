# Logging Setup: `log4j2.xml` and `setLogLevel()`

> *Cross-chapter synthesis — Rioux (2022), Chapters 2, 8.*
>
> Spark's logging is controlled by Log4j 2 configuration. Getting it right separates a shell session that produces pages of INFO chatter from one where only meaningful output appears. Ch 2 covers the configuration file for interactive use; Ch 8 adds the RDD-specific loggers needed to suppress task-failure noise.

---

## Ch 2 — `log4j2.xml` structure and the three decisions

Spark 3.3+ ships with Log4j 2 (the earlier `log4j.properties` format is no longer used). The configuration file lives at `$SPARK_HOME/conf/log4j2.xml` (copy `log4j2.xml.template` to create it).

Every effective `log4j2.xml` for interactive PySpark work answers three questions:

**Decision 1 — What is the root (default) level?**

```xml
<Root level="WARN">
    <AppenderRef ref="console" />
</Root>
```

`WARN` is the recommended default for interactive work. `ERROR` is quieter; `INFO` is very verbose (Spark emits many INFO messages per stage).

**Decision 2 — Which Spark loggers should be elevated or suppressed?**

```xml
<Loggers>
    <!-- Suppress noisy Spark internals -->
    <Logger name="org.apache.spark" level="WARN" additivity="false">
        <AppenderRef ref="console" />
    </Logger>
    <!-- Suppress Hadoop I/O chatter -->
    <Logger name="org.apache.hadoop" level="WARN" additivity="false">
        <AppenderRef ref="console" />
    </Logger>
    <!-- Keep your own application logger at INFO or DEBUG -->
    <Logger name="com.mycompany.myapp" level="INFO" additivity="false">
        <AppenderRef ref="console" />
    </Logger>
    <Root level="WARN">
        <AppenderRef ref="console" />
    </Root>
</Loggers>
```

**Decision 3 — Does the path to the file use `os.path.abspath()`?**

Spark resolves the log4j2.xml path relative to the working directory at JVM startup, not relative to `$SPARK_HOME/conf`. If you specify the path in `spark-submit` or in `SparkConf`, always use an absolute path:

```python
import os
log4j_path = os.path.abspath("conf/log4j2.xml")
spark = (
    SparkSession.builder
    .config(
        "spark.driver.extraJavaOptions",
        f"-Dlog4j.configurationFile={log4j_path}",
    )
    .getOrCreate()
)
```

Omitting `os.path.abspath()` is a frequent source of "config not found" bugs that silently fall back to the default verbose logging.

---

## Runtime override with `setLogLevel()`

For ad-hoc suppression in an interactive session without editing a file:

```python
spark.sparkContext.setLogLevel("ERROR")   # suppress WARN and INFO
spark.sparkContext.setLogLevel("WARN")    # restore to WARN
```

`setLogLevel()` overrides the log4j2.xml level at runtime. It affects only the driver-side console output; executor log levels require either a log4j2.xml change or broadcasting the config.

---

## Ch 8 — Suppressing RDD task-failure noise

When a Python UDF or lambda raises inside an RDD stage, Spark retries the task up to `spark.task.maxFailures` times (default 4) before aborting the job. Each retry logs a stack trace at the `WARN` level from `org.apache.spark.scheduler.TaskSetManager`. In a multi-partition RDD this produces a wall of duplicate stack traces.

Suppress these specific loggers if you want to see only the final error:

```xml
<!-- Add to the <Loggers> block in log4j2.xml -->
<Logger name="org.apache.spark.executor.Executor" level="FATAL" additivity="false">
    <AppenderRef ref="console" />
</Logger>
<Logger name="org.apache.spark.scheduler.TaskSetManager" level="FATAL" additivity="false">
    <AppenderRef ref="console" />
</Logger>
```

**Ordering requirement.** Log4j 2 applies the *most specific* matching logger first. Both loggers above have `additivity="false"`, so their output does not propagate to the parent `org.apache.spark` logger or the root logger. Place them *before* the `<Root>` element in the `<Loggers>` block — if `<Root>` appears first and a broader `org.apache.spark` logger is configured, the specificity rule still applies, but XML ordering makes the intent readable.

---

## Summary

- Spark 3.3+ uses Log4j 2 (`log4j2.xml`); the old `log4j.properties` format is no longer valid.
- Set root level to `WARN`; add named loggers for specific packages you want to elevate or suppress.
- Always use `os.path.abspath()` when providing a log4j2.xml path to avoid silent fallback to defaults.
- `spark.sparkContext.setLogLevel()` overrides levels at runtime without editing files.
- For RDD task-failure noise, set `org.apache.spark.executor.Executor` and `org.apache.spark.scheduler.TaskSetManager` to `FATAL` in `log4j2.xml`.

---

## Chapter links

- [Ch 2 — First data program](../books/rioux/chapters/02-first-data-program.md)
- [Ch 8 — RDDs and UDFs](../books/rioux/chapters/08-rdd-udfs.md)
