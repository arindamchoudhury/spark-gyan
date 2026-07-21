# Deploy (Standalone/YARN/Mesos/K8s)

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

_TODO: connective prose added during the era passes._

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 1.5.0 | [SPARK-4352](https://issues.apache.org/jira/browse/SPARK-4352) | Improvement | Incorporate locality preferences in dynamic allocation requests |
| 1.5.0 | [SPARK-4751](https://issues.apache.org/jira/browse/SPARK-4751) | New Feature | Support dynamic allocation for standalone mode |
| 1.5.0 | [SPARK-6287](https://issues.apache.org/jira/browse/SPARK-6287) | Improvement | Add support for dynamic allocation in the Mesos coarse-grained scheduler |
| 1.5.0 | [SPARK-6470](https://issues.apache.org/jira/browse/SPARK-6470) | Improvement | Allow Spark apps to put YARN node labels in their requests |
| 1.5.0 | [SPARK-6707](https://issues.apache.org/jira/browse/SPARK-6707) | Improvement | Mesos Scheduler should allow the user to specify constraints based on slave attributes |
| 1.5.0 | [SPARK-6797](https://issues.apache.org/jira/browse/SPARK-6797) | Improvement | Add support for YARN cluster mode |
| 1.5.0 | [SPARK-7699](https://issues.apache.org/jira/browse/SPARK-7699) | Improvement | Dynamic allocation: initial executors may be canceled before first job |
| 1.5.0 | [SPARK-7795](https://issues.apache.org/jira/browse/SPARK-7795) | Improvement | Speed up task serialization in standalone mode |
| 1.5.0 | [SPARK-8302](https://issues.apache.org/jira/browse/SPARK-8302) | New Feature | Support heterogeneous cluster nodes on YARN |
| 1.5.0 | [SPARK-8395](https://issues.apache.org/jira/browse/SPARK-8395) | Improvement | spark-submit documentation is incorrect |
| 1.5.0 | [SPARK-8475](https://issues.apache.org/jira/browse/SPARK-8475) | Improvement | SparkSubmit with Ivy jars is very slow to load with no internet access |
| 1.5.0 | [SPARK-8798](https://issues.apache.org/jira/browse/SPARK-8798) | New Feature | Allow additional uris to be fetched with mesos |
| 1.5.0 | [SPARK-9092](https://issues.apache.org/jira/browse/SPARK-9092) | Improvement | Make --num-executors compatible with dynamic allocation |
| 1.5.0 | [SPARK-9180](https://issues.apache.org/jira/browse/SPARK-9180) | Improvement | Accept --name option in spark-submit |
| 1.5.0 | [SPARK-9263](https://issues.apache.org/jira/browse/SPARK-9263) | New Feature | Add Spark Submit flag to exclude dependencies when using --packages |
| 1.5.0 | [SPARK-9352](https://issues.apache.org/jira/browse/SPARK-9352) | Improvement | Add tests for standalone scheduling code |
| 1.5.0 | [SPARK-9477](https://issues.apache.org/jira/browse/SPARK-9477) | Improvement | Adding IBM Platform Application Service Controller into Spark documentation as a supported Cluster Manager (beside Yarn and Mesos). |
| 1.5.0 | [SPARK-9562](https://issues.apache.org/jira/browse/SPARK-9562) | Improvement | Move spark-ec2 from mesos to amplab |
| 1.5.0 | [SPARK-9566](https://issues.apache.org/jira/browse/SPARK-9566) | Umbrella | Spark 1.5.0 YARN testing umbrella |
| 1.5.0 | [SPARK-9737](https://issues.apache.org/jira/browse/SPARK-9737) | Improvement | Add the suggested configuration when required executor memory is above the max threshold of this cluster on YARN mode |
| 1.5.0 | [SPARK-10295](https://issues.apache.org/jira/browse/SPARK-10295) | Improvement | Dynamic allocation in Mesos does not release when RDDs are cached |
| 1.5.0 | [SPARK-18391](https://issues.apache.org/jira/browse/SPARK-18391) | Improvement | Openstack deployment scenarios |
| 1.6.0 | [SPARK-6350](https://issues.apache.org/jira/browse/SPARK-6350) | Improvement | Make mesosExecutorCores configurable in mesos "fine-grained" mode |
| 1.6.0 | [SPARK-9669](https://issues.apache.org/jira/browse/SPARK-9669) | New Feature | Support PySpark with Mesos Cluster mode |
| 1.6.0 | [SPARK-9737](https://issues.apache.org/jira/browse/SPARK-9737) | Improvement | Add the suggested configuration when required executor memory is above the max threshold of this cluster on YARN mode |
| 1.6.0 | [SPARK-9782](https://issues.apache.org/jira/browse/SPARK-9782) | Improvement | Add support for YARN application tags running Spark on YARN |
| 1.6.0 | [SPARK-9817](https://issues.apache.org/jira/browse/SPARK-9817) | Improvement | Improve the container placement strategy by considering the localities of pending container requests |
| 1.6.0 | [SPARK-10471](https://issues.apache.org/jira/browse/SPARK-10471) | Improvement | Mesos Scheduler should reject offers not matching constraints for longer period of time |
| 1.6.0 | [SPARK-10481](https://issues.apache.org/jira/browse/SPARK-10481) | Improvement | SPARK_PREPEND_CLASSES make spark-yarn related jar could not be found |
| 1.6.0 | [SPARK-10739](https://issues.apache.org/jira/browse/SPARK-10739) | Improvement | Add attempt window for long running Spark application on Yarn |
| 1.6.0 | [SPARK-11344](https://issues.apache.org/jira/browse/SPARK-11344) | Improvement | ApplicationDescription should be immutable case class |
| 1.6.0 | [SPARK-11686](https://issues.apache.org/jira/browse/SPARK-11686) | Improvement | Issue WARN when dynamic allocation is disabled due to spark.dynamicAllocation.enabled and spark.executor.instances both set |
| 1.6.0 | [SPARK-11809](https://issues.apache.org/jira/browse/SPARK-11809) | Improvement | Switch the default Mesos mode to coarse-grained mode |
| 1.6.0 | [SPARK-18391](https://issues.apache.org/jira/browse/SPARK-18391) | Improvement | Openstack deployment scenarios |
| 1.6.3 | [SPARK-2424](https://issues.apache.org/jira/browse/SPARK-2424) | Improvement | ApplicationState.MAX_NUM_RETRY should be configurable |
| 1.6.3 | [SPARK-16956](https://issues.apache.org/jira/browse/SPARK-16956) | New Feature | Make ApplicationState.MAX_NUM_RETRY configurable |
| 1.6.3 | [SPARK-17316](https://issues.apache.org/jira/browse/SPARK-17316) | Improvement | Don't block StandaloneSchedulerBackend.executorRemoved |
| 2.0.0 | [SPARK-2930](https://issues.apache.org/jira/browse/SPARK-2930) | Improvement | clarify docs on using webhdfs with spark.yarn.access.namenodes |
| 2.0.0 | [SPARK-4117](https://issues.apache.org/jira/browse/SPARK-4117) | Improvement | Spark on Yarn handle AM being told command from RM |
| 2.0.0 | [SPARK-5095](https://issues.apache.org/jira/browse/SPARK-5095) | Improvement | Support launching multiple mesos executors in coarse grained mesos mode |
| 2.0.0 | [SPARK-10570](https://issues.apache.org/jira/browse/SPARK-10570) | New Feature | Add Spark version endpoint to standalone JSON API |
| 2.0.0 | [SPARK-10749](https://issues.apache.org/jira/browse/SPARK-10749) | Improvement | Support multiple roles with Spark Mesos dispatcher |
| 2.0.0 | [SPARK-12241](https://issues.apache.org/jira/browse/SPARK-12241) | Improvement | Improve failure reporting in Yarn client obtainTokenForHBase() |
| 2.0.0 | [SPARK-12248](https://issues.apache.org/jira/browse/SPARK-12248) | Improvement | Make Spark Coarse Mesos Scheduler obey limits on memory/cpu ratios |
| 2.0.0 | [SPARK-12263](https://issues.apache.org/jira/browse/SPARK-12263) | Improvement | IllegalStateException: Memory can't be 0 for SPARK_WORKER_MEMORY without unit |
| 2.0.0 | [SPARK-12471](https://issues.apache.org/jira/browse/SPARK-12471) | Improvement | Spark daemons should log their pid in the log file |
| 2.0.0 | [SPARK-13001](https://issues.apache.org/jira/browse/SPARK-13001) | Improvement | Coarse-grained Mesos scheduler should reject offers for longer period of time when reached max cores |
| 2.0.0 | [SPARK-13063](https://issues.apache.org/jira/browse/SPARK-13063) | Improvement | Make the SPARK YARN STAGING DIR as configurable |
| 2.0.0 | [SPARK-13387](https://issues.apache.org/jira/browse/SPARK-13387) | Improvement | Add support for SPARK_DAEMON_JAVA_OPTS with MesosClusterDispatcher. |
| 2.0.0 | [SPARK-13414](https://issues.apache.org/jira/browse/SPARK-13414) | Improvement | Add support for launching multiple Mesos dispatchers |
| 2.0.0 | [SPARK-13723](https://issues.apache.org/jira/browse/SPARK-13723) | Improvement | YARN - Change behavior of --num-executors when spark.dynamicAllocation.enabled true |
| 2.0.0 | [SPARK-13904](https://issues.apache.org/jira/browse/SPARK-13904) | Improvement | Add support for pluggable cluster manager |
| 2.0.0 | [SPARK-13944](https://issues.apache.org/jira/browse/SPARK-13944) | New Feature | Separate out local linear algebra as a standalone module without Spark dependency |
| 2.0.0 | [SPARK-14729](https://issues.apache.org/jira/browse/SPARK-14729) | Improvement | Implement an existing cluster manager with New ExternalClusterManager interface |
| 2.0.0 | [SPARK-15806](https://issues.apache.org/jira/browse/SPARK-15806) | Improvement | Deprecate SPARK_MASTER_IP in favor of SPARK_MASTER_HOST |
| 2.0.0 | [SPARK-18391](https://issues.apache.org/jira/browse/SPARK-18391) | Improvement | Openstack deployment scenarios |
| 2.0.1 | [SPARK-2424](https://issues.apache.org/jira/browse/SPARK-2424) | Improvement | ApplicationState.MAX_NUM_RETRY should be configurable |
| 2.0.1 | [SPARK-16956](https://issues.apache.org/jira/browse/SPARK-16956) | New Feature | Make ApplicationState.MAX_NUM_RETRY configurable |
| 2.0.1 | [SPARK-17316](https://issues.apache.org/jira/browse/SPARK-17316) | Improvement | Don't block StandaloneSchedulerBackend.executorRemoved |
| 2.0.1 | [SPARK-18391](https://issues.apache.org/jira/browse/SPARK-18391) | Improvement | Openstack deployment scenarios |
| 2.1.0 | [SPARK-2424](https://issues.apache.org/jira/browse/SPARK-2424) | Improvement | ApplicationState.MAX_NUM_RETRY should be configurable |
| 2.1.0 | [SPARK-11653](https://issues.apache.org/jira/browse/SPARK-11653) | Improvement | Would be very useful if spark-daemon.sh supported foreground operations |
| 2.1.0 | [SPARK-11714](https://issues.apache.org/jira/browse/SPARK-11714) | Improvement | Make Spark on Mesos honor port restrictions |
| 2.1.0 | [SPARK-14082](https://issues.apache.org/jira/browse/SPARK-14082) | Improvement | Add support for GPU resource when running on Mesos |
| 2.1.0 | [SPARK-15990](https://issues.apache.org/jira/browse/SPARK-15990) | Improvement | Support rolling log aggregation for Spark running on YARN |
| 2.1.0 | [SPARK-16194](https://issues.apache.org/jira/browse/SPARK-16194) | Improvement | No way to dynamically set env vars on driver in cluster mode |
| 2.1.0 | [SPARK-16927](https://issues.apache.org/jira/browse/SPARK-16927) | New Feature | Mesos Cluster Dispatcher default properties |
| 2.1.0 | [SPARK-16956](https://issues.apache.org/jira/browse/SPARK-16956) | New Feature | Make ApplicationState.MAX_NUM_RETRY configurable |
| 2.1.0 | [SPARK-17316](https://issues.apache.org/jira/browse/SPARK-17316) | Improvement | Don't block StandaloneSchedulerBackend.executorRemoved |
| 2.1.0 | [SPARK-17329](https://issues.apache.org/jira/browse/SPARK-17329) | Improvement | Don't build PRs with -Pyarn unless YARN code changed |
| 2.1.0 | [SPARK-17686](https://issues.apache.org/jira/browse/SPARK-17686) | Improvement | Propose to print Scala version in "spark-submit --version" command |
| 2.2.0 | [SPARK-10643](https://issues.apache.org/jira/browse/SPARK-10643) | New Feature | Support remote application download in client mode spark submit |
| 2.2.0 | [SPARK-10748](https://issues.apache.org/jira/browse/SPARK-10748) | Improvement | Log error instead of crashing Spark Mesos dispatcher when a job is misconfigured |
| 2.2.0 | [SPARK-15288](https://issues.apache.org/jira/browse/SPARK-15288) | Improvement | Mesos dispatcher should handle gracefully when any thread gets UncaughtException |
| 2.2.0 | [SPARK-17062](https://issues.apache.org/jira/browse/SPARK-17062) | Improvement | Add --conf to mesos dispatcher process |
| 2.2.0 | [SPARK-17568](https://issues.apache.org/jira/browse/SPARK-17568) | Improvement | Add spark-submit option for user to override ivy settings used to resolve packages/artifacts |
| 2.2.0 | [SPARK-17979](https://issues.apache.org/jira/browse/SPARK-17979) | Improvement | Remove deprecated support for config SPARK_YARN_USER_ENV |
| 2.2.0 | [SPARK-18232](https://issues.apache.org/jira/browse/SPARK-18232) | Improvement | Support Mesos CNI |
| 2.2.0 | [SPARK-18417](https://issues.apache.org/jira/browse/SPARK-18417) | Improvement | Define 'spark.yarn.am.port' in yarn config object |
| 2.2.0 | [SPARK-18662](https://issues.apache.org/jira/browse/SPARK-18662) | Improvement | Move cluster managers into their own sub-directory |
| 2.2.0 | [SPARK-19501](https://issues.apache.org/jira/browse/SPARK-19501) | Improvement | Slow checking if there are many spark.yarn.jars, which are already on HDFS |
| 2.2.0 | [SPARK-19702](https://issues.apache.org/jira/browse/SPARK-19702) | New Feature | Increasse refuse_seconds timeout in the Mesos Spark Dispatcher |
| 2.2.0 | [SPARK-19740](https://issues.apache.org/jira/browse/SPARK-19740) | Improvement | Spark executor always runs as root when running on mesos |
| 2.2.0 | [SPARK-20078](https://issues.apache.org/jira/browse/SPARK-20078) | Improvement | Mesos executor configurability for task name and labels |
| 2.2.0 | [SPARK-20085](https://issues.apache.org/jira/browse/SPARK-20085) | Improvement | Configurable mesos labels for executors |
| 2.2.0 | [SPARK-20521](https://issues.apache.org/jira/browse/SPARK-20521) | Improvement | The default of 'spark.worker.cleanup.appDataTtl' should be 604800 in spark-standalone.md. |
| 2.2.0 | [SPARK-20796](https://issues.apache.org/jira/browse/SPARK-20796) | Improvement | the location of start-master.sh in spark-standalone.md is wrong |
| 3.0.0 | [SPARK-20327](https://issues.apache.org/jira/browse/SPARK-20327) | Improvement | Add CLI support for YARN custom resources, like GPUs |
| 3.0.0 | [SPARK-22404](https://issues.apache.org/jira/browse/SPARK-22404) | Improvement | Provide an option to use unmanaged AM in yarn-client mode |
| 3.0.0 | [SPARK-23155](https://issues.apache.org/jira/browse/SPARK-23155) | Improvement | YARN-aggregated executor/driver logs appear unavailable when NM is down |
| 3.0.0 | [SPARK-24434](https://issues.apache.org/jira/browse/SPARK-24434) | New Feature | Support user-specified driver and executor pod templates |
| 3.0.0 | [SPARK-24516](https://issues.apache.org/jira/browse/SPARK-24516) | Improvement | PySpark Bindings for K8S - make Python 3 the default |
| 3.0.0 | [SPARK-24793](https://issues.apache.org/jira/browse/SPARK-24793) | Improvement | Make spark-submit more useful with k8s |
| 3.0.0 | [SPARK-25222](https://issues.apache.org/jira/browse/SPARK-25222) | Improvement | Spark on Kubernetes Pod Watcher dumps raw container status |
| 3.0.0 | [SPARK-25262](https://issues.apache.org/jira/browse/SPARK-25262) | Improvement | Support tmpfs for local dirs in k8s |
| 3.0.0 | [SPARK-25282](https://issues.apache.org/jira/browse/SPARK-25282) | Improvement | Document custom builds needed for K8S development |
| 3.0.0 | [SPARK-25653](https://issues.apache.org/jira/browse/SPARK-25653) | Improvement | Add tag ExtendedHiveTest for HiveSparkSubmitSuite |
| 3.0.0 | [SPARK-25809](https://issues.apache.org/jira/browse/SPARK-25809) | Improvement | Support additional K8S cluster types for integration tests |
| 3.0.0 | [SPARK-25828](https://issues.apache.org/jira/browse/SPARK-25828) | Improvement | Bumping Version of kubernetes.client to latest version |
| 3.0.0 | [SPARK-25874](https://issues.apache.org/jira/browse/SPARK-25874) | Umbrella | Simplify abstractions in the K8S backend |
| 3.0.0 | [SPARK-25876](https://issues.apache.org/jira/browse/SPARK-25876) | Improvement | Simplify configuration types in k8s backend |
| 3.0.0 | [SPARK-25887](https://issues.apache.org/jira/browse/SPARK-25887) | Improvement | Allow specifying Kubernetes context to use |
| 3.0.0 | [SPARK-25897](https://issues.apache.org/jira/browse/SPARK-25897) | Improvement | Cannot run k8s integration tests in sbt |
| 3.0.0 | [SPARK-25960](https://issues.apache.org/jira/browse/SPARK-25960) | New Feature | Support subpath mounting with Kubernetes |
| 3.0.0 | [SPARK-26192](https://issues.apache.org/jira/browse/SPARK-26192) | Improvement | MesosClusterScheduler reads options from dispatcher conf instead of submission conf |
| 3.0.0 | [SPARK-26194](https://issues.apache.org/jira/browse/SPARK-26194) | Improvement | Support automatic spark.authenticate secret in Kubernetes backend |
| 3.0.0 | [SPARK-26235](https://issues.apache.org/jira/browse/SPARK-26235) | Improvement | Change log level for ClassNotFoundException/NoClassDefFoundError in SparkSubmit to Error |
| 3.0.0 | [SPARK-26343](https://issues.apache.org/jira/browse/SPARK-26343) | Improvement | Speed up running the kubernetes integration tests locally |
| 3.0.0 | [SPARK-26420](https://issues.apache.org/jira/browse/SPARK-26420) | Improvement | [kubernetes] driver service id is not random |
| 3.0.0 | [SPARK-26600](https://issues.apache.org/jira/browse/SPARK-26600) | Improvement | Update spark-submit usage message |
| 3.0.0 | [SPARK-26603](https://issues.apache.org/jira/browse/SPARK-26603) | Improvement | Update minikube backend in K8s integration tests |
| 3.0.0 | [SPARK-26642](https://issues.apache.org/jira/browse/SPARK-26642) | Improvement | Add --num-executors option to spark-submit for Spark on K8S |
| 3.0.0 | [SPARK-26688](https://issues.apache.org/jira/browse/SPARK-26688) | Improvement | Provide configuration of initially blacklisted YARN nodes |
| 3.0.0 | [SPARK-26729](https://issues.apache.org/jira/browse/SPARK-26729) | Improvement | Spark on Kubernetes tooling hardcodes default image names |
| 3.0.0 | [SPARK-26775](https://issues.apache.org/jira/browse/SPARK-26775) | Improvement | Update Jenkins nodes to support local volumes for K8s integration tests |
| 3.0.0 | [SPARK-26790](https://issues.apache.org/jira/browse/SPARK-26790) | Improvement | Yarn executor to self-retrieve log urls and attributes |
| 3.0.0 | [SPARK-26843](https://issues.apache.org/jira/browse/SPARK-26843) | Improvement | Use ConfigEntry for hardcoded configs for "mesos" resource manager |
| 3.0.0 | [SPARK-26877](https://issues.apache.org/jira/browse/SPARK-26877) | Improvement | Support user-level app staging directory in yarn mode when spark.yarn.stagingDir specified |
| 3.0.0 | [SPARK-27023](https://issues.apache.org/jira/browse/SPARK-27023) | Improvement | Kubernetes client timeouts should be configurable |
| 3.0.0 | [SPARK-27024](https://issues.apache.org/jira/browse/SPARK-27024) | Story | Executor interface for cluster managers to support GPU resources |
| 3.0.0 | [SPARK-27122](https://issues.apache.org/jira/browse/SPARK-27122) | Improvement | YARN test failures in Java 9+ |
| 3.0.0 | [SPARK-27192](https://issues.apache.org/jira/browse/SPARK-27192) | Improvement | spark.task.cpus should be less or equal than spark.task.cpus when use static executor allocation |
| 3.0.0 | [SPARK-27261](https://issues.apache.org/jira/browse/SPARK-27261) | Improvement | Spark submit passing multiple configurations not documented clearly |
| 3.0.0 | [SPARK-27334](https://issues.apache.org/jira/browse/SPARK-27334) | Improvement | Support specify scheduler name for executor pods when submit |
| 3.0.0 | [SPARK-27360](https://issues.apache.org/jira/browse/SPARK-27360) | Story | Standalone cluster mode support for GPU-aware scheduling |
| 3.0.0 | [SPARK-27361](https://issues.apache.org/jira/browse/SPARK-27361) | Story | YARN support for GPU-aware scheduling |
| 3.0.0 | [SPARK-27362](https://issues.apache.org/jira/browse/SPARK-27362) | Story | Kubernetes support for GPU-aware scheduling |
| 3.0.0 | [SPARK-27754](https://issues.apache.org/jira/browse/SPARK-27754) | Improvement | Introduce spark on k8s config for driver request cores |
| 3.0.0 | [SPARK-27959](https://issues.apache.org/jira/browse/SPARK-27959) | Story | Change YARN resource configs to use .amount |
| 3.0.0 | [SPARK-28145](https://issues.apache.org/jira/browse/SPARK-28145) | Improvement | Executor pods polling source can fail to replace dead executors |
| 3.0.0 | [SPARK-28487](https://issues.apache.org/jira/browse/SPARK-28487) | Improvement | K8S pod allocator behaves poorly with dynamic allocation |
| 3.0.0 | [SPARK-28936](https://issues.apache.org/jira/browse/SPARK-28936) | Improvement | Simplify Spark K8s tests by replacing race condition during command execution |
| 3.0.0 | [SPARK-28938](https://issues.apache.org/jira/browse/SPARK-28938) | Improvement | Move to supported OpenJDK docker image for Kubernetes |
| 3.0.0 | [SPARK-29070](https://issues.apache.org/jira/browse/SPARK-29070) | Improvement | Make SparkLauncher log full spark-submit command line |
| 3.0.0 | [SPARK-29436](https://issues.apache.org/jira/browse/SPARK-29436) | Improvement | Support executor for selecting scheduler through scheduler name in the case of k8s multi-scheduler scenario. |
| 3.0.0 | [SPARK-29603](https://issues.apache.org/jira/browse/SPARK-29603) | Improvement | Support application priority for spark on yarn |
| 3.0.0 | [SPARK-29833](https://issues.apache.org/jira/browse/SPARK-29833) | Improvement | Add FileNotFoundException check for spark.yarn.jars |
| 3.0.0 | [SPARK-29865](https://issues.apache.org/jira/browse/SPARK-29865) | Improvement | k8s executor pods all have different prefixes in client mode |
| 3.0.0 | [SPARK-29950](https://issues.apache.org/jira/browse/SPARK-29950) | Improvement | Deleted excess executors can connect back to driver in K8S with dyn alloc on |
| 3.0.0 | [SPARK-30243](https://issues.apache.org/jira/browse/SPARK-30243) | Improvement | Upgrade K8s client dependency to 4.6.4 |
| 3.0.0 | [SPARK-30371](https://issues.apache.org/jira/browse/SPARK-30371) | Improvement | make KUBERNETES_MASTER_INTERNAL_URL configurable |
| 3.0.0 | [SPARK-30387](https://issues.apache.org/jira/browse/SPARK-30387) | Improvement | Improve YarnClientSchedulerBackend log message |
| 3.0.0 | [SPARK-30626](https://issues.apache.org/jira/browse/SPARK-30626) | Improvement | Add SPARK_APPLICATION_ID into driver pod env |
| 3.0.0 | [SPARK-30689](https://issues.apache.org/jira/browse/SPARK-30689) | Improvement | Allow custom resource scheduling to work with YARN versions that don't support custom resource scheduling |
| 3.0.0 | [SPARK-30715](https://issues.apache.org/jira/browse/SPARK-30715) | Improvement | Upgrade fabric8 to 4.7.1 to support K8s 1.17 |
| 3.0.0 | [SPARK-31696](https://issues.apache.org/jira/browse/SPARK-31696) | New Feature | Support spark.kubernetes.driver.service.annotation |
| 3.0.0 | [SPARK-31766](https://issues.apache.org/jira/browse/SPARK-31766) | Improvement | Add Spark version prefix to K8s UUID test image tag |
| 3.0.0 | [SPARK-31780](https://issues.apache.org/jira/browse/SPARK-31780) | Improvement | Add R test tag to exclude R K8s image building and test |
| 3.1.1 | [SPARK-33005](https://issues.apache.org/jira/browse/SPARK-33005) | Umbrella | Kubernetes GA Preparation |
| 3.1.1 | [SPARK-35222](https://issues.apache.org/jira/browse/SPARK-35222) | Improvement | [SPARK-35222] In YARN mode, for better user experience, when Spark is started, not only the AppID is printed, but the Tracking URL is also printed to allow users to better track Spark Job |
| 3.2.0 | [SPARK-595](https://issues.apache.org/jira/browse/SPARK-595) | New Feature | Document "local-cluster" mode |
| 3.2.0 | [SPARK-33724](https://issues.apache.org/jira/browse/SPARK-33724) | Improvement | Allow decommissioning script location to be configured |
| 3.2.0 | [SPARK-33908](https://issues.apache.org/jira/browse/SPARK-33908) | Improvement | Refact SparkSubmitUtils.resolveMavenCoordinates return parameter |
| 3.2.0 | [SPARK-34104](https://issues.apache.org/jira/browse/SPARK-34104) | Improvement | Allow users to specify a maximum decommissioning time |
| 3.2.0 | [SPARK-34105](https://issues.apache.org/jira/browse/SPARK-34105) | Improvement | In addition to killing exlcuded/flakey executors which should support decommissioning |
| 3.2.0 | [SPARK-34316](https://issues.apache.org/jira/browse/SPARK-34316) | New Feature | Support spark.kubernetes.executor.disableConfigMap |
| 3.2.0 | [SPARK-34486](https://issues.apache.org/jira/browse/SPARK-34486) | Improvement | Upgrade kubernetes-client to 4.13.2 |
| 3.2.0 | [SPARK-34539](https://issues.apache.org/jira/browse/SPARK-34539) | Improvement | Zinc standalone server is useless after scala-maven-plugin 4.x |
| 3.2.0 | [SPARK-34869](https://issues.apache.org/jira/browse/SPARK-34869) | Improvement | Extend k8s "EXTRA LOGS FOR THE FAILED TEST" section with describe pods output |
| 3.2.0 | [SPARK-34877](https://issues.apache.org/jira/browse/SPARK-34877) | Improvement | Add Spark AM Log link in case of master as yarn and deploy mode as client |
| 3.2.0 | [SPARK-35125](https://issues.apache.org/jira/browse/SPARK-35125) | New Feature | Upgrade K8s client to 5.3.0 to support K8s 1.20 |
| 3.2.0 | [SPARK-35131](https://issues.apache.org/jira/browse/SPARK-35131) | New Feature | Support early driver service clean-up during app termination |
| 3.2.0 | [SPARK-35227](https://issues.apache.org/jira/browse/SPARK-35227) | Improvement | Replace Bintray with the new repository service for the spark-packages resolver in SparkSubmit |
| 3.2.0 | [SPARK-35280](https://issues.apache.org/jira/browse/SPARK-35280) | Improvement | Promote KubernetesUtils to DeveloperApi |
| 3.2.0 | [SPARK-35315](https://issues.apache.org/jira/browse/SPARK-35315) | Improvement | Keep benchmark result consistent between spark-submit and SBT |
| 3.2.0 | [SPARK-35394](https://issues.apache.org/jira/browse/SPARK-35394) | Improvement | Move kubernetes-client.version to root pom file |
| 3.2.0 | [SPARK-35443](https://issues.apache.org/jira/browse/SPARK-35443) | Improvement | Mark K8s secrets and config maps as immutable |
| 3.2.0 | [SPARK-35462](https://issues.apache.org/jira/browse/SPARK-35462) | Improvement | Upgrade Kubernetes-client to 5.4.0 to support K8s 1.21 models |
| 3.2.0 | [SPARK-35501](https://issues.apache.org/jira/browse/SPARK-35501) | Improvement | Add a feature for removing pulled container image for docker integration tests |
| 3.2.0 | [SPARK-35577](https://issues.apache.org/jira/browse/SPARK-35577) | Improvement | Allow to log container output for docker integration tests |
| 3.2.0 | [SPARK-35660](https://issues.apache.org/jira/browse/SPARK-35660) | Improvement | Upgrade Kubernetes-client to 5.4.1 |
| 3.2.0 | [SPARK-35692](https://issues.apache.org/jira/browse/SPARK-35692) | Improvement | Use int to replace long for EXECUTOR_ID_COUNTER in Kubernetes |
| 3.2.0 | [SPARK-35699](https://issues.apache.org/jira/browse/SPARK-35699) | Improvement | Improve error message when creating k8s pod failed. |
| 3.2.0 | [SPARK-35969](https://issues.apache.org/jira/browse/SPARK-35969) | Improvement | Make the pod prefix more readable and tallied with K8S DNS Label Names |
| 3.2.0 | [SPARK-36774](https://issues.apache.org/jira/browse/SPARK-36774) | Improvement | Use SparkSubmitTestUtils to core and use it in SparkSubmitSuite |
<!-- AUTO:timeline END -->
