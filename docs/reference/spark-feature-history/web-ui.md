# Web UI / History / Metrics

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 0.x era — origins

The web UI began life in 0.7.0 as a simple dashboard for monitoring RDD memory usage, with its port configurable via `spark.ui.port`; the standalone cluster's own web UI gained JSON endpoints for querying cluster state that same release, and 0.6.1 improved it to show more job information. 0.7.2 added the `SparkListener` metrics interface for collecting per-stage computation info like task lengths and bytes shuffled.

0.8.0 was the biggest leap: a dedicated web UI at port 4040, a job dashboard with percentile statistics for task runtime, shuffle, and GC, an extended per-executor storage/task dashboard, and a new metrics library exposing internal metrics through JMX and Ganglia. 0.8.1 added a "result fetching" state to the UI's job view.

### 1.x era — REST API and richer progress reporting

1.0.0 added a history server so users could inspect application data after a job finished, not just while it ran. 1.1.0 brought named accumulators visible in the UI, dynamic task-progress updates, input-metric reporting, and graceful handling of stage resubmissions. 1.2.0 added a job-level progress page and a stable progress-reporting API other tools could build on. 1.4.0 introduced a REST API for application information (SPARK-3644) and a dedicated UI for the SQL JDBC server (SPARK-5100). 1.5.0 added pagination for jobs with large task counts (SPARK-4598) and folded streaming storage into the main UI (SPARK-4072), and 1.6.0 closed the era with per-operator SQL execution metrics (SPARK-10412) and failure visibility throughout the streaming tab.

### 2.x era — a scalable History Server backend

2.0.0's web UI work was broad but incremental: HTTPS support (SPARK-2750), per-executor core counts (SPARK-3611), SQL UI support on the History Server (SPARK-11206), and visualization plus metrics for whole-stage-codegen operators (SPARK-12902, SPARK-12915) — tying the new SQL execution model into the existing UI. The notable 2.x change was structural rather than cosmetic: 2.3.0 shipped a new History Server backend, Spark History Server V2 (SPARK-18085), built around a more efficient event-storage mechanism designed to scale to large applications that the original file-replay-based backend struggled with — event volume having grown considerably since 2.0.0's codegen and optimizer instrumentation landed.

### 3.x era — RocksDB-backed history server, Connect gets a UI page

3.0.0 improved History Server concurrency (SPARK-29043) and added stage-level-scheduling UI support alongside observable metrics. 3.1.1 exposed executor memory metrics in the web UI (SPARK-23432). 3.2.0 added task/executor metrics distributions to the REST API (SPARK-34488) and a hash-aggregate fallback metric. 3.3.0 added RocksDB as a backend option for the History Server (SPARK-37680) and renamed the SQL tab to "SQL / DataFrame" to reflect pandas-API usage (SPARK-38657). 3.4.0 made RocksDB the default History Server disk backend (SPARK-42277) and improved UI scalability and driver stability for large applications (SPARK-41053). 3.5.0 added a dedicated Spark UI page for Spark Connect (SPARK-44394) — giving the newly introduced Connect architecture its own observability surface — plus per-query error-message display and a heap-histogram column in the Executors tab.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 0.6.1 | — | prose | Improved standalone cluster web UI shows more job information |
| 0.7.0 | — | prose | Web-based memory usage dashboard for RDDs |
| 0.7.0 | — | prose | Configurable UI port via spark.ui.port |
| 0.7.0 | — | prose | Standalone web UI adds JSON endpoints for cluster state |
| 0.7.2 | — | prose | SparkListener metrics interface for per-stage computation info |
| 0.8.0 | — | prose | Web UI for monitoring at port 4040 |
| 0.8.0 | — | prose | Job dashboard with percentile stats for task runtime, shuffle, GC |
| 0.8.0 | — | prose | Extended storage dashboard with per-executor storage/task pages |
| 0.8.0 | — | prose | New metrics library exposing internal metrics via JMX and Ganglia |
| 0.8.1 | — | prose | New result-fetching state shown in UI |
| 1.0.0 | — | prose | History server added for Spark's web UI |
| 1.1.0 | — | prose | Named accumulators displayed in Spark's UI |
| 1.1.0 | — | prose | Dynamic updating of task progress metrics |
| 1.1.0 | — | prose | Reporting of input metrics for tasks reading input data |
| 1.1.0 | — | prose | Stage resubmissions handled gracefully in Spark UI |
| 1.2.0 | — | prose | Job-level progress page, stable progress reporting API, dynamic output metrics |
| 1.3.0 | — | prose | Realtime GC metrics and record counts added to the UI |
| 1.4.0 | [SPARK-3644](https://issues.apache.org/jira/browse/SPARK-3644) | prose | A REST API for application information |
| 1.4.0 | [SPARK-5100](https://issues.apache.org/jira/browse/SPARK-5100) | prose | Dedicated UI for the SQL JDBC server |
| 1.5.0 | [SPARK-4072](https://issues.apache.org/jira/browse/SPARK-4072) | prose | Streaming storage included in web UI |
| 1.5.0 | [SPARK-4598](https://issues.apache.org/jira/browse/SPARK-4598) | prose | Pagination for jobs with large number of tasks in web UI |
| 1.5.0 | [SPARK-5768](https://issues.apache.org/jira/browse/SPARK-5768) | Improvement | Spark UI Shows incorrect memory under Yarn |
| 1.5.0 | [SPARK-6942](https://issues.apache.org/jira/browse/SPARK-6942) | Umbrella | Umbrella: UI Visualizations for Core and Dataframes |
| 1.5.0 | [SPARK-7161](https://issues.apache.org/jira/browse/SPARK-7161) | Improvement | Provide REST api to download event logs from History Server |
| 1.5.0 | [SPARK-7169](https://issues.apache.org/jira/browse/SPARK-7169) | Improvement | Allow to specify metrics configuration more flexibly |
| 1.5.0 | [SPARK-7657](https://issues.apache.org/jira/browse/SPARK-7657) | Improvement | [YARN] Show driver link in Spark UI |
| 1.5.0 | [SPARK-8145](https://issues.apache.org/jira/browse/SPARK-8145) | Github Integration | Trigger a double click on the span to show full job description |
| 1.5.0 | [SPARK-8344](https://issues.apache.org/jira/browse/SPARK-8344) | New Feature | Add internal metrics / logging for DAGScheduler to detect long pauses / blocking |
| 1.5.0 | [SPARK-8735](https://issues.apache.org/jira/browse/SPARK-8735) | Improvement | Expose metrics for runtime memory usage |
| 1.6.0 | — | prose | Streaming tab shows failures in timelines, batch list, and batch details page |
| 1.6.0 | — | prose | Streaming tab shows output operations as progress bars |
| 1.6.0 | [SPARK-2533](https://issues.apache.org/jira/browse/SPARK-2533) | Improvement | Show summary of locality level of completed tasks in the each stage page of web UI |
| 1.6.0 | [SPARK-9790](https://issues.apache.org/jira/browse/SPARK-9790) | Improvement | [YARN] Expose in WebUI if NodeManager is the reason why executors were killed. |
| 1.6.0 | [SPARK-10411](https://issues.apache.org/jira/browse/SPARK-10411) | Improvement | In SQL tab move visualization above explain output |
| 1.6.0 | [SPARK-10412](https://issues.apache.org/jira/browse/SPARK-10412) | prose | Per-operator metrics for SQL execution |
| 1.6.0 | [SPARK-10531](https://issues.apache.org/jira/browse/SPARK-10531) | Improvement | AppId is set as AppName in status rest api |
| 1.6.0 | [SPARK-10652](https://issues.apache.org/jira/browse/SPARK-10652) | Improvement | Set meaningful job descriptions for streaming related jobs |
| 1.6.0 | [SPARK-10742](https://issues.apache.org/jira/browse/SPARK-10742) | Improvement | Add the ability to embed HTML relative links in job descriptions |
| 1.6.0 | [SPARK-10876](https://issues.apache.org/jira/browse/SPARK-10876) | Improvement | display total application time in spark history UI |
| 1.6.0 | [SPARK-10974](https://issues.apache.org/jira/browse/SPARK-10974) | Improvement | Add progress bar for output operation column and use red dots for failed batches |
| 1.6.0 | [SPARK-11129](https://issues.apache.org/jira/browse/SPARK-11129) | New Feature | Link Spark WebUI in Mesos WebUI |
| 1.6.0 | [SPARK-11742](https://issues.apache.org/jira/browse/SPARK-11742) | Improvement | Show batch failures in the Streaming UI landing page |
| 1.6.0 | [SPARK-11824](https://issues.apache.org/jira/browse/SPARK-11824) | Improvement | WebUI throws console error for descriptions with 'bad' HTML |
| 1.6.3 | [SPARK-16796](https://issues.apache.org/jira/browse/SPARK-16796) | Improvement | Visible passwords on Spark environment page |
| 2.0.0 | [SPARK-1832](https://issues.apache.org/jira/browse/SPARK-1832) | Improvement | Executor UI improvement suggestions |
| 2.0.0 | [SPARK-2750](https://issues.apache.org/jira/browse/SPARK-2750) | New Feature | Add Https support for Web UI |
| 2.0.0 | [SPARK-3611](https://issues.apache.org/jira/browse/SPARK-3611) | New Feature | Show number of cores for each executor in application web UI |
| 2.0.0 | [SPARK-7729](https://issues.apache.org/jira/browse/SPARK-7729) | Improvement | Executor which has been killed should also be displayed on Executors Tab. |
| 2.0.0 | [SPARK-7889](https://issues.apache.org/jira/browse/SPARK-7889) | Improvement | Jobs progress of apps on complete page of HistoryServer shows uncompleted |
| 2.0.0 | [SPARK-10775](https://issues.apache.org/jira/browse/SPARK-10775) | Improvement | add search keywords in history page ui |
| 2.0.0 | [SPARK-11206](https://issues.apache.org/jira/browse/SPARK-11206) | New Feature | Support SQL UI on the history server |
| 2.0.0 | [SPARK-11824](https://issues.apache.org/jira/browse/SPARK-11824) | Improvement | WebUI throws console error for descriptions with 'bad' HTML |
| 2.0.0 | [SPARK-12857](https://issues.apache.org/jira/browse/SPARK-12857) | Improvement | Streaming tab in web UI uses records and events interchangeably |
| 2.0.0 | [SPARK-12902](https://issues.apache.org/jira/browse/SPARK-12902) | Improvement | Visualization and metrics for generated operators |
| 2.0.0 | [SPARK-12915](https://issues.apache.org/jira/browse/SPARK-12915) | Improvement | SQL metrics for generated operators |
| 2.0.0 | [SPARK-13152](https://issues.apache.org/jira/browse/SPARK-13152) | Improvement | Fix task metrics deprecation warning |
| 2.0.0 | [SPARK-13234](https://issues.apache.org/jira/browse/SPARK-13234) | Improvement | Remove duplicated SQL metrics |
| 2.0.0 | [SPARK-13267](https://issues.apache.org/jira/browse/SPARK-13267) | Improvement | Document ?params for the v1 REST API |
| 2.0.0 | [SPARK-13364](https://issues.apache.org/jira/browse/SPARK-13364) | Improvement | history server application column Id not sorting as number |
| 2.0.0 | [SPARK-13481](https://issues.apache.org/jira/browse/SPARK-13481) | Improvement | History server page with a default sorting as "desc" |
| 2.0.0 | [SPARK-13492](https://issues.apache.org/jira/browse/SPARK-13492) | Improvement | Configure a custom webui_url for the Spark Mesos Framework |
| 2.0.0 | [SPARK-13775](https://issues.apache.org/jira/browse/SPARK-13775) | Improvement | history server sort by completed time by default |
| 2.0.0 | [SPARK-14025](https://issues.apache.org/jira/browse/SPARK-14025) | Improvement | Fix streaming job descriptions on the event line |
| 2.0.0 | [SPARK-14062](https://issues.apache.org/jira/browse/SPARK-14062) | Improvement | Put metrics.properties to distributed cache for Spark running on Yarn. Also Fix log4j propagation issue |
| 2.0.0 | [SPARK-14245](https://issues.apache.org/jira/browse/SPARK-14245) | Improvement | webUI should display the user |
| 2.0.0 | [SPARK-14576](https://issues.apache.org/jira/browse/SPARK-14576) | Improvement | Spark console should display Web UI url |
| 2.0.0 | [SPARK-14626](https://issues.apache.org/jira/browse/SPARK-14626) | Improvement | Simplify accumulators and task metrics |
| 2.0.0 | [SPARK-14669](https://issues.apache.org/jira/browse/SPARK-14669) | Improvement | Some SQL metrics is broken when whole-stage codegen enabled |
| 2.0.0 | [SPARK-15641](https://issues.apache.org/jira/browse/SPARK-15641) | Improvement | Incorrect Completed for Incomplete applications in HistoryServer |
| 2.0.0 | [SPARK-15860](https://issues.apache.org/jira/browse/SPARK-15860) | Improvement | Metrics for codegen size and perf |
| 2.0.0 | [SPARK-16238](https://issues.apache.org/jira/browse/SPARK-16238) | Improvement | Metrics for generated method bytecode size |
| 2.0.1 | [SPARK-15923](https://issues.apache.org/jira/browse/SPARK-15923) | Improvement | Spark Application rest api returns "no such app: <appId>" |
| 2.0.1 | [SPARK-16796](https://issues.apache.org/jira/browse/SPARK-16796) | Improvement | Visible passwords on Spark environment page |
| 2.0.1 | [SPARK-17486](https://issues.apache.org/jira/browse/SPARK-17486) | Improvement | Remove unused TaskMetricsUIData.updatedBlockStatuses field |
| 2.1.0 | [SPARK-1301](https://issues.apache.org/jira/browse/SPARK-1301) | Improvement | Add UI elements to collapse "Aggregated Metrics by Executor" pane on stage page |
| 2.1.0 | [SPARK-4411](https://issues.apache.org/jira/browse/SPARK-4411) | New Feature | Add "kill" link for jobs in the UI |
| 2.1.0 | [SPARK-5847](https://issues.apache.org/jira/browse/SPARK-5847) | Improvement | Allow for configuring MetricsSystem's use of app ID to namespace all metrics |
| 2.1.0 | [SPARK-11272](https://issues.apache.org/jira/browse/SPARK-11272) | New Feature | Support importing and exporting event logs from HistoryServer web portal |
| 2.1.0 | [SPARK-12221](https://issues.apache.org/jira/browse/SPARK-12221) | Improvement | Add CPU time metric to TaskMetrics |
| 2.1.0 | [SPARK-15487](https://issues.apache.org/jira/browse/SPARK-15487) | Improvement | Spark Master UI to reverse proxy Application and Workers UI |
| 2.1.0 | [SPARK-15885](https://issues.apache.org/jira/browse/SPARK-15885) | Improvement | Provide links to executor logs from stage details page in UI |
| 2.1.0 | [SPARK-15951](https://issues.apache.org/jira/browse/SPARK-15951) | Improvement | Change Executors Page to use datatables to support sorting columns and searching |
| 2.1.0 | [SPARK-16166](https://issues.apache.org/jira/browse/SPARK-16166) | Improvement | Correctly honor off heap memory usage in web ui and log display |
| 2.1.0 | [SPARK-16405](https://issues.apache.org/jira/browse/SPARK-16405) | Improvement | Add metrics and source for external shuffle service |
| 2.1.0 | [SPARK-16796](https://issues.apache.org/jira/browse/SPARK-16796) | Improvement | Visible passwords on Spark environment page |
| 2.1.0 | [SPARK-16809](https://issues.apache.org/jira/browse/SPARK-16809) | New Feature | Link Mesos Dispatcher and History Server |
| 2.1.0 | [SPARK-16919](https://issues.apache.org/jira/browse/SPARK-16919) | Improvement | Configurable update interval for console progress bar |
| 2.1.0 | [SPARK-17486](https://issues.apache.org/jira/browse/SPARK-17486) | Improvement | Remove unused TaskMetricsUIData.updatedBlockStatuses field |
| 2.1.0 | [SPARK-17532](https://issues.apache.org/jira/browse/SPARK-17532) | Improvement | Add thread lock information from JMX to thread dump UI |
| 2.1.0 | [SPARK-17671](https://issues.apache.org/jira/browse/SPARK-17671) | Improvement | Spark 2.0 history server summary page is slow even set spark.history.ui.maxApplications |
| 2.1.0 | [SPARK-17843](https://issues.apache.org/jira/browse/SPARK-17843) | Improvement | History Server gives no feedback about the application list being incomplete |
| 2.1.0 | [SPARK-18010](https://issues.apache.org/jira/browse/SPARK-18010) | Improvement | Remove unneeded heavy work performed by FsHistoryProvider for building up the application listing UI page |
| 2.2.0 | [SPARK-16122](https://issues.apache.org/jira/browse/SPARK-16122) | New Feature | Spark History Server REST API missing an environment endpoint per application |
| 2.2.0 | [SPARK-16654](https://issues.apache.org/jira/browse/SPARK-16654) | Improvement | UI Should show blacklisted executors & nodes |
| 2.2.0 | [SPARK-17843](https://issues.apache.org/jira/browse/SPARK-17843) | Improvement | History Server gives no feedback about the application list being incomplete |
| 2.2.0 | [SPARK-18171](https://issues.apache.org/jira/browse/SPARK-18171) | Improvement | Show correct framework address in mesos master web ui when the advertised address is used |
| 2.2.0 | [SPARK-18204](https://issues.apache.org/jira/browse/SPARK-18204) | Improvement | Remove SparkUI.appUIAddress |
| 2.2.0 | [SPARK-18236](https://issues.apache.org/jira/browse/SPARK-18236) | Improvement | Reduce memory usage of Spark UI and HistoryServer by reducing duplicate objects |
| 2.2.0 | [SPARK-18256](https://issues.apache.org/jira/browse/SPARK-18256) | Improvement | Improve performance of event log replay in HistoryServer based on profiler results |
| 2.2.0 | [SPARK-18495](https://issues.apache.org/jira/browse/SPARK-18495) | Improvement | Web UI should document meaning of green dot in DAG visualization |
| 2.2.0 | [SPARK-18537](https://issues.apache.org/jira/browse/SPARK-18537) | New Feature | Add a REST api to spark streaming |
| 2.2.0 | [SPARK-18606](https://issues.apache.org/jira/browse/SPARK-18606) | Improvement | [HISTORYSERVER]It will check html elems while searching HistoryServer |
| 2.2.0 | [SPARK-18836](https://issues.apache.org/jira/browse/SPARK-18836) | Improvement | Serialize Task Metrics once per stage |
| 2.2.0 | [SPARK-18837](https://issues.apache.org/jira/browse/SPARK-18837) | Improvement | Very long stage descriptions do not wrap in the UI |
| 2.2.0 | [SPARK-19009](https://issues.apache.org/jira/browse/SPARK-19009) | Improvement | Add doc for Streaming Rest API |
| 2.2.0 | [SPARK-19182](https://issues.apache.org/jira/browse/SPARK-19182) | Improvement | Optimize the lock in StreamingJobProgressListener to not block UI when generating Streaming jobs |
| 2.2.0 | [SPARK-19554](https://issues.apache.org/jira/browse/SPARK-19554) | Improvement | YARN backend should use history server URL for tracking when UI is disabled |
| 2.2.0 | [SPARK-19807](https://issues.apache.org/jira/browse/SPARK-19807) | Improvement | Add reason for cancellation when a stage is killed using web UI |
| 2.2.0 | [SPARK-20136](https://issues.apache.org/jira/browse/SPARK-20136) | Improvement | Add num files and metadata operation timing to scan metrics |
| 2.2.0 | [SPARK-20151](https://issues.apache.org/jira/browse/SPARK-20151) | Improvement | Account for partition pruning in scan metadataTime metrics |
| 2.2.0 | [SPARK-20218](https://issues.apache.org/jira/browse/SPARK-20218) | Improvement | '/applications/[app-id]/stages' in REST API,add description. |
| 2.2.0 | [SPARK-20391](https://issues.apache.org/jira/browse/SPARK-20391) | Improvement | Properly rename the memory related fields in ExecutorSummary REST API |
| 2.2.0 | [SPARK-20776](https://issues.apache.org/jira/browse/SPARK-20776) | Improvement | Fix JobProgressListener perf. problems caused by empty TaskMetrics initialization |
| 2.2.0 | [SPARK-20942](https://issues.apache.org/jira/browse/SPARK-20942) | Improvement | The title style about field is error in the history server web ui. |
| 2.3.0 | [SPARK-18085](https://issues.apache.org/jira/browse/SPARK-18085) | prose | Spark History Server V2 (scalable event storage) |
| 3.0.0 | [SPARK-18364](https://issues.apache.org/jira/browse/SPARK-18364) | Improvement | Expose metrics for YarnShuffleService |
| 3.0.0 | [SPARK-21809](https://issues.apache.org/jira/browse/SPARK-21809) | Improvement | Change Stage Page to use datatables to support sorting columns and searching |
| 3.0.0 | [SPARK-24851](https://issues.apache.org/jira/browse/SPARK-24851) | Improvement | Map a Stage ID to it's Associated Job ID in UI |
| 3.0.0 | [SPARK-25285](https://issues.apache.org/jira/browse/SPARK-25285) | Improvement | Add executor task metrics to track the number of tasks started and of tasks successfully completed |
| 3.0.0 | [SPARK-25392](https://issues.apache.org/jira/browse/SPARK-25392) | Improvement | [Spark Job History]Inconsistent behaviour for pool details in spark web UI and history server page |
| 3.0.0 | [SPARK-25394](https://issues.apache.org/jira/browse/SPARK-25394) | Improvement | Expose App status metrics as Source |
| 3.0.0 | [SPARK-25566](https://issues.apache.org/jira/browse/SPARK-25566) | Improvement | [Spark Job History] SQL UI Page does not support Pagination |
| 3.0.0 | [SPARK-25642](https://issues.apache.org/jira/browse/SPARK-25642) | Improvement | Add new Metrics in External Shuffle Service to help determine Network performance and Connection Handling capabilities of the Shuffle Service |
| 3.0.0 | [SPARK-25696](https://issues.apache.org/jira/browse/SPARK-25696) | Improvement | The storage memory displayed on spark Application UI is incorrect. |
| 3.0.0 | [SPARK-25711](https://issues.apache.org/jira/browse/SPARK-25711) | Improvement | Allow history server to show usage and remove deprecated options |
| 3.0.0 | [SPARK-25719](https://issues.apache.org/jira/browse/SPARK-25719) | Improvement | Search functionality in datatables in stages page should search over formatted data rather than the raw data |
| 3.0.0 | [SPARK-25855](https://issues.apache.org/jira/browse/SPARK-25855) | Improvement | Don't use Erasure Coding for event log files |
| 3.0.0 | [SPARK-26139](https://issues.apache.org/jira/browse/SPARK-26139) | New Feature | Support passing shuffle metrics to exchange operator |
| 3.0.0 | [SPARK-26156](https://issues.apache.org/jira/browse/SPARK-26156) | Improvement | Revise summary section of stage page |
| 3.0.0 | [SPARK-26260](https://issues.apache.org/jira/browse/SPARK-26260) | Improvement | Task Summary Metrics for Stage Page: Efficient implementation for SHS when using disk store. |
| 3.0.0 | [SPARK-26316](https://issues.apache.org/jira/browse/SPARK-26316) | Improvement | Because of the perf degradation in TPC-DS, we currently partial revert SPARK-21052：Add hash map metrics to join, |
| 3.0.0 | [SPARK-26792](https://issues.apache.org/jira/browse/SPARK-26792) | Improvement | Apply custom log URL to Spark UI |
| 3.0.0 | [SPARK-26890](https://issues.apache.org/jira/browse/SPARK-26890) | Improvement | Add Dropwizard metrics list and additional configuration details to the documentation |
| 3.0.0 | [SPARK-26928](https://issues.apache.org/jira/browse/SPARK-26928) | Improvement | Add driver CPU Time to the metrics system |
| 3.0.0 | [SPARK-26967](https://issues.apache.org/jira/browse/SPARK-26967) | Improvement | Put MetricsSystem instance names together for clearer management |
| 3.0.0 | [SPARK-27045](https://issues.apache.org/jira/browse/SPARK-27045) | Improvement | SQL tab in UI shows actual SQL instead of callsite in case of SparkSQLDriver |
| 3.0.0 | [SPARK-27071](https://issues.apache.org/jira/browse/SPARK-27071) | Improvement | Expose additional metrics in status.api.v1.StageData |
| 3.0.0 | [SPARK-27189](https://issues.apache.org/jira/browse/SPARK-27189) | Improvement | Add Executor metrics and memory usage instrumentation to the metrics system |
| 3.0.0 | [SPARK-27324](https://issues.apache.org/jira/browse/SPARK-27324) | Improvement | document configurations related to executor metrics |
| 3.0.0 | [SPARK-27486](https://issues.apache.org/jira/browse/SPARK-27486) | Improvement | Enable History server storage information test |
| 3.0.0 | [SPARK-27489](https://issues.apache.org/jira/browse/SPARK-27489) | Story | UI updates to show executor resource information |
| 3.0.0 | [SPARK-27678](https://issues.apache.org/jira/browse/SPARK-27678) | New Feature | Support Knox user impersonation in UI |
| 3.0.0 | [SPARK-27830](https://issues.apache.org/jira/browse/SPARK-27830) | Improvement | Show Spark version at app lists of Spark History UI |
| 3.0.0 | [SPARK-28091](https://issues.apache.org/jira/browse/SPARK-28091) | Improvement | Extend Spark metrics system with user-defined metrics using executor plugins |
| 3.0.0 | [SPARK-28372](https://issues.apache.org/jira/browse/SPARK-28372) | Umbrella | Document Spark WEB UI |
| 3.0.0 | [SPARK-28475](https://issues.apache.org/jira/browse/SPARK-28475) | Improvement | Add regex MetricFilter to GraphiteSink |
| 3.0.0 | [SPARK-28594](https://issues.apache.org/jira/browse/SPARK-28594) | Improvement | Allow event logs for running streaming apps to be rolled over |
| 3.0.0 | [SPARK-28942](https://issues.apache.org/jira/browse/SPARK-28942) | Improvement | [Spark][WEB UI]Spark in local mode hostname display localhost in the Host Column of Task Summary Page |
| 3.0.0 | [SPARK-29043](https://issues.apache.org/jira/browse/SPARK-29043) | prose | Improve the concurrent performance of History Server |
| 3.0.0 | [SPARK-29168](https://issues.apache.org/jira/browse/SPARK-29168) | Improvement | Use a unique color on selected item on timeline view |
| 3.0.0 | [SPARK-29273](https://issues.apache.org/jira/browse/SPARK-29273) | Improvement | Spark peakExecutionMemory metrics is zero |
| 3.0.0 | [SPARK-29348](https://issues.apache.org/jira/browse/SPARK-29348) | New Feature | Add observable metrics |
| 3.0.0 | [SPARK-29429](https://issues.apache.org/jira/browse/SPARK-29429) | Umbrella | Support Prometheus monitoring natively |
| 3.0.0 | [SPARK-29449](https://issues.apache.org/jira/browse/SPARK-29449) | Umbrella | Add tooltip to Spark WebUI |
| 3.0.0 | [SPARK-29466](https://issues.apache.org/jira/browse/SPARK-29466) | Improvement | Show `Duration` for running drivers in Standalone master web UI |
| 3.0.0 | [SPARK-29557](https://issues.apache.org/jira/browse/SPARK-29557) | Improvement | Upgrade dropwizard metrics library to 3.2.6 |
| 3.0.0 | [SPARK-29562](https://issues.apache.org/jira/browse/SPARK-29562) | Improvement | SQLAppStatusListener metrics aggregation is slow and memory hungry |
| 3.0.0 | [SPARK-29654](https://issues.apache.org/jira/browse/SPARK-29654) | Improvement | Add configuration to allow disabling registration of static sources to the metrics system |
| 3.0.0 | [SPARK-29731](https://issues.apache.org/jira/browse/SPARK-29731) | Improvement | Use public JIRA REST API to read-only access |
| 3.0.0 | [SPARK-29766](https://issues.apache.org/jira/browse/SPARK-29766) | Improvement | Aggregate metrics asynchronously in SQL listener |
| 3.0.0 | [SPARK-29795](https://issues.apache.org/jira/browse/SPARK-29795) | Improvement | Possible 'leak' of Metrics with dropwizard metrics 4.x |
| 3.0.0 | [SPARK-29857](https://issues.apache.org/jira/browse/SPARK-29857) | Improvement | [WEB UI] Support defer render the spark history summary page. |
| 3.0.0 | [SPARK-29894](https://issues.apache.org/jira/browse/SPARK-29894) | Improvement | Add Codegen Stage Id to Spark plan graphs in Web UI SQL Tab |
| 3.0.0 | [SPARK-29997](https://issues.apache.org/jira/browse/SPARK-29997) | Improvement | Show job name for empty jobs in WebUI |
| 3.0.0 | [SPARK-30041](https://issues.apache.org/jira/browse/SPARK-30041) | Improvement | Add Codegen Stage Id to Stage DAG visualization in Web UI |
| 3.0.0 | [SPARK-30060](https://issues.apache.org/jira/browse/SPARK-30060) | Improvement | Uniform naming for Spark Metrics configuration parameters |
| 3.0.0 | [SPARK-30209](https://issues.apache.org/jira/browse/SPARK-30209) | Improvement | Display stageId, attemptId, taskId with SQL max metric in UI |
| 3.0.0 | [SPARK-30240](https://issues.apache.org/jira/browse/SPARK-30240) | Improvement | Spark UI redirects do not always work behind (dumb) proxies |
| 3.0.0 | [SPARK-30383](https://issues.apache.org/jira/browse/SPARK-30383) | Improvement | Remove meaning less tooltip from Executor Tab |
| 3.0.0 | [SPARK-30531](https://issues.apache.org/jira/browse/SPARK-30531) | Improvement | Duplicate query plan on Spark UI SQL page |
| 3.0.0 | [SPARK-30684](https://issues.apache.org/jira/browse/SPARK-30684) | Improvement | Show the descripton of metrics for WholeStageCodegen in DAG viz |
| 3.0.0 | [SPARK-31079](https://issues.apache.org/jira/browse/SPARK-31079) | Improvement | Add RuleExecutor metrics in Explain Formatted |
| 3.0.0 | [SPARK-31081](https://issues.apache.org/jira/browse/SPARK-31081) | Improvement | Make display of stageId/stageAttemptId/taskId of sql metrics toggleable |
| 3.0.0 | [SPARK-31275](https://issues.apache.org/jira/browse/SPARK-31275) | Improvement | Improve the metrics format in ExecutionPage for StageId |
| 3.1.1 | [SPARK-23432](https://issues.apache.org/jira/browse/SPARK-23432) | prose | Expose executor memory metrics in the web UI for executors |
| 3.1.1 | [SPARK-29303](https://issues.apache.org/jira/browse/SPARK-29303) | prose | Add UI support for stage level scheduling |
| 3.2.0 | [SPARK-26399](https://issues.apache.org/jira/browse/SPARK-26399) | prose | Add new stage-level REST APIs and parameters |
| 3.2.0 | [SPARK-33763](https://issues.apache.org/jira/browse/SPARK-33763) | Improvement | Add metrics for better tracking of dynamic allocation |
| 3.2.0 | [SPARK-33991](https://issues.apache.org/jira/browse/SPARK-33991) | Improvement | Repair enumeration conversion error for AllJobsPage |
| 3.2.0 | [SPARK-34005](https://issues.apache.org/jira/browse/SPARK-34005) | Improvement | Update peak memory metrics for each Executor on task end. |
| 3.2.0 | [SPARK-34092](https://issues.apache.org/jira/browse/SPARK-34092) | Improvement | support filtering by task status in REST API call for a specific stage |
| 3.2.0 | [SPARK-34123](https://issues.apache.org/jira/browse/SPARK-34123) | Improvement | Faster way to display/render entries in HistoryPage (Spark history server summary page) |
| 3.2.0 | [SPARK-34288](https://issues.apache.org/jira/browse/SPARK-34288) | Improvement | Add a tip info for the `resources` column in the executors page |
| 3.2.0 | [SPARK-34488](https://issues.apache.org/jira/browse/SPARK-34488) | prose | Support task and executor Metrics Distributions in the REST API |
| 3.2.0 | [SPARK-34592](https://issues.apache.org/jira/browse/SPARK-34592) | Improvement | Mark indeterminate RDD in Web UI |
| 3.2.0 | [SPARK-34764](https://issues.apache.org/jira/browse/SPARK-34764) | Improvement | Propagate reason for executor loss to the UI |
| 3.2.0 | [SPARK-34787](https://issues.apache.org/jira/browse/SPARK-34787) | Improvement | Option variable in Spark historyServer log should be displayed as actual value instead of Some(XX) |
| 3.2.0 | [SPARK-34848](https://issues.apache.org/jira/browse/SPARK-34848) | Improvement | add duration in TaskMetricsDistribution |
| 3.2.0 | [SPARK-35215](https://issues.apache.org/jira/browse/SPARK-35215) | Improvement | Update custom metrics per certain rows |
| 3.2.0 | [SPARK-35229](https://issues.apache.org/jira/browse/SPARK-35229) | Improvement | Spark Job web page is extremely slow while there are more than 1500 events in timeline |
| 3.2.0 | [SPARK-35258](https://issues.apache.org/jira/browse/SPARK-35258) | Improvement | Enhance ESS ExternalBlockHandler with additional block rate-based metrics and histograms |
| 3.2.0 | [SPARK-35311](https://issues.apache.org/jira/browse/SPARK-35311) | Improvement | Add exposed SS UI state information metrics to the documentation |
| 3.2.0 | [SPARK-35402](https://issues.apache.org/jira/browse/SPARK-35402) | Improvement | Increase the max thread pool size of jetty server in HistoryServer UI |
| 3.2.0 | [SPARK-35487](https://issues.apache.org/jira/browse/SPARK-35487) | Improvement | Upgrade dropwizard metrics to 4.2.0 |
| 3.2.0 | [SPARK-35529](https://issues.apache.org/jira/browse/SPARK-35529) | prose | Add fallback metrics for hash aggregate |
| 3.2.0 | [SPARK-35639](https://issues.apache.org/jira/browse/SPARK-35639) | Improvement | Add metrics about coalesced partitions to CustomShuffleReader in AQE |
| 3.2.0 | [SPARK-36030](https://issues.apache.org/jira/browse/SPARK-36030) | Improvement | Support DS v2 metrics at writing path |
| 3.2.0 | [SPARK-39153](https://issues.apache.org/jira/browse/SPARK-39153) | Improvement | When we look at spark UI or History, we can see the failed tasks first |
| 3.2.1 | [SPARK-34399](https://issues.apache.org/jira/browse/SPARK-34399) | prose | Add file commit time to metrics and shown in SQL Tab UI |
| 3.3.0 | [SPARK-34399](https://issues.apache.org/jira/browse/SPARK-34399) | prose | Add commit duration to SQL tab’s graph node |
| 3.3.0 | [SPARK-34735](https://issues.apache.org/jira/browse/SPARK-34735) | prose | Add modified configs for SQL execution in UI |
| 3.3.0 | [SPARK-36038](https://issues.apache.org/jira/browse/SPARK-36038) | prose | Speculation metrics summary at stage level |
| 3.3.0 | [SPARK-36237](https://issues.apache.org/jira/browse/SPARK-36237) | prose | Attach and start handler after application started in UI |
| 3.3.0 | [SPARK-36400](https://issues.apache.org/jira/browse/SPARK-36400) | prose | Make ThriftServer recognize spark.sql.redaction.string.regex |
| 3.3.0 | [SPARK-37469](https://issues.apache.org/jira/browse/SPARK-37469) | prose | Unified shuffle read block time to shuffle read fetch wait time in StagePage |
| 3.3.0 | [SPARK-37680](https://issues.apache.org/jira/browse/SPARK-37680) | prose | Support RocksDB backend in Spark History Server |
| 3.3.0 | [SPARK-38656](https://issues.apache.org/jira/browse/SPARK-38656) | prose | Show options for Pandas API on Spark in UI |
| 3.3.0 | [SPARK-38657](https://issues.apache.org/jira/browse/SPARK-38657) | prose | Rename ‘SQL’ to ‘SQL / DataFrame’ in SQL UI page |
| 3.4.0 | [SPARK-39110](https://issues.apache.org/jira/browse/SPARK-39110) | prose | Show metrics properties in the environment tab |
| 3.4.0 | [SPARK-39225](https://issues.apache.org/jira/browse/SPARK-39225) | prose | Support spark.history.fs.update.batchSize |
| 3.4.0 | [SPARK-39489](https://issues.apache.org/jira/browse/SPARK-39489) | prose | Improve event logging JsonProtocol performance by using Jackson instead of Json4s |
| 3.4.0 | [SPARK-41053](https://issues.apache.org/jira/browse/SPARK-41053) | prose | Better Spark UI scalability and Driver stability for large applications |
| 3.4.0 | [SPARK-41752](https://issues.apache.org/jira/browse/SPARK-41752) | prose | Group nested executions under the root execution |
| 3.4.0 | [SPARK-42277](https://issues.apache.org/jira/browse/SPARK-42277) | prose | Use RocksDB for spark.history.store.hybridStore.diskBackend by default |
| 3.5.0 | [SPARK-44153](https://issues.apache.org/jira/browse/SPARK-44153) | prose | Support Heap Histogram column in Executors tab |
| 3.5.0 | [SPARK-44309](https://issues.apache.org/jira/browse/SPARK-44309) | prose | Display Add/Remove Time of Executors on Executors Tab |
| 3.5.0 | [SPARK-44367](https://issues.apache.org/jira/browse/SPARK-44367) | prose | Show error message on UI for each failed query |
| 3.5.0 | [SPARK-44394](https://issues.apache.org/jira/browse/SPARK-44394) | prose | Add a Spark UI page for Spark Connect |
| 3.5.4 | [SPARK-49294](https://issues.apache.org/jira/browse/SPARK-49294) | prose | Add width attribute for shuffle-write-time checkbox |
<!-- AUTO:timeline END -->
