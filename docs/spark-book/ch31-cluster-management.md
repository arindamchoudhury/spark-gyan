# Chapter 31 — Production Deployment: Cluster Management

> *Learning-path topic: E2 (Expert)*
> *Status: ⬜ Not yet written*

!!! note "📌 Topics deferred here from Chapter 1"
    Chapter 1 states executors stay alive for the entire application and that the cluster manager decides how many to launch. The following lifecycle details are covered in full here:

    - **Executor lifecycle** — when executors are launched (at `SparkContext` creation vs on-demand), what triggers shutdown (application end, `spark.executor.heartbeatInterval` timeout, explicit removal), and how the driver detects executor loss
    - **Dynamic allocation** — `spark.dynamicAllocation.enabled`; Spark requests additional executors from the cluster manager when tasks are queued and releases idle executors after `spark.dynamicAllocation.executorIdleTimeout`; requires External Shuffle Service (ESS) so idle executor shuffle files remain accessible after the executor is removed
    - **External Shuffle Service requirement** — why dynamic allocation cannot work without ESS on YARN/Standalone: removing an executor without ESS loses its shuffle files, forcing stage resubmission; ESS decouples executor lifecycle from shuffle file availability
    - **Resource profiles** — Spark 3.1+; requesting different memory/CPU configurations for different stages within one application
    - **YARN, Kubernetes, and Standalone** — cluster-manager-specific executor launch mechanics, scheduling queue interaction, and recommended configurations

*This chapter is not yet written. The above topics will form its core.*
