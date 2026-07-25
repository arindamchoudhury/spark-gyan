---
subsystem: core
spark_version: "4.2.0"
swept_at: 2026-07-25
group: rpc-resources
all_groups: [rdd-layer, execution-engine, shuffle-memory, storage-serializer, submit-standalone, monitoring, config-security, rpc-resources, api-bridge]
status: complete
concepts:
  - name: RpcEnv and endpoint registration
    topics: [E1]
  - name: Dispatcher, Inbox, and the MessageLoop threading model
    topics: [E1]
  - name: RpcEndpointRef send/ask — local shortcut vs Outbox remote send
    topics: [E1]
  - name: RpcTimeout and the network.timeout fallback chain
    topics: [E1]
  - name: RPC message-size enforcement
    topics: [E1]
  - name: Endpoint lookup and RpcEndpointVerifier
    topics: [E1]
  - name: Transport layer — connection setup and thread sizing
    topics: [E1, E2]
  - name: Transport timeouts, fetch-to-mem, and Netty-OOM retry
    topics: [E1, E2]
  - name: ResourceProfile — the default profile and executor/task requests
    topics: [E2]
  - name: ResourceProfileManager — validation, dedup, and merge conflicts
    topics: [E2]
  - name: ResourceUtils — discovery (resourcesFile vs script) and the dynamic-key resource configs
    topics: [E2]
  - name: ResourceAllocator — address assignment and the executor/task arithmetic
    topics: [E2]
  - name: Stage-level scheduling and accelerator-aware resources (GPU/FPGA)
    topics: []
    propose:
      code: A16
      level: Advanced
      title: "Stage-Level Scheduling and Accelerator-Aware Resources (GPU/FPGA)"
      what: "Attaching a custom ResourceProfile to an RDD so a stage requests different CPUs/memory/accelerators than the app default, and how the scheduler discovers, validates, and assigns GPU/FPGA addresses to its tasks."
      why: "GPU inference/ML stages and mixed CPU/GPU pipelines are a real production pattern; no existing topic teaches ResourceProfiles, resource discovery, fractional-GPU sharing, or profile-merge conflicts."
  - name: BlockTransferService — the data plane
    topics: [E1, E2, A4, I6]
  - name: The RpcEnv file server
    topics: [E2, B1]
  - name: RpcCallContext and the reply contract
    topics: [E1]
---

Spark's RPC layer is the actor-like messaging substrate underneath every driver↔executor exchange — task launches, status updates, heartbeats, BlockManager registration, map-output requests all ride it. The resources model layered on the same core is how Spark reasons about custom accelerators (GPU/FPGA) and how it counts how many tasks fit on an executor. This sweep traces both, plus the Netty transport plumbing they share.

The whole layer is Netty-based: `NettyRpcEnv` is the only `RpcEnv` implementation, and it builds a `TransportContext` (from `common/network-common`) whose `TransportClientFactory` / `TransportServer` do the wire work. RPC framing configs live under `spark.rpc.io.*` and map into a `TransportConf` with module name `"rpc"`.

```mermaid
flowchart LR
  ref[RpcEndpointRef.ask/send] -->|local| disp[Dispatcher]
  ref -->|remote| ob[Outbox]
  ob -->|TransportClient| net[Netty transport]
  net --> handler[NettyRpcHandler]
  handler --> disp
  disp --> loop[MessageLoop]
  loop --> inbox[Inbox per endpoint]
  inbox --> ep[RpcEndpoint.receive / receiveAndReply]
```

---

## RpcEnv and endpoint registration
**What it is:** `RpcEnv` is the abstract RPC environment: endpoints register under a name, and the env routes messages to them and hands back an `RpcEndpointRef`. The only concrete impl is `NettyRpcEnv`, created via `NettyRpcEnvFactory`. An `RpcEndpoint` is the message handler (`receive` for one-way, `receiveAndReply` for request/response), with a strict `constructor → onStart → receive* → onStop` lifecycle. Thread-safety variants are `ThreadSafeRpcEndpoint`, `IsolatedRpcEndpoint` (dedicated pool), and `IsolatedThreadSafeRpcEndpoint` (dedicated pool pinned to one thread).

**Code path:** `RpcEnv.create` → `NettyRpcEnvFactory.create` (builds `NettyRpcEnv`, optionally `startServer`) → `RpcEnv.setupEndpoint(name, endpoint)` → `NettyRpcEnv.setupEndpoint` → `Dispatcher.registerRpcEndpoint(name, endpoint)` → returns `NettyRpcEndpointRef`.

**Anchor files:**

- [RpcEnv.scala:37](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/RpcEnv.scala#L37) — `RpcEnv.create` → `NettyRpcEnvFactory`
- [RpcEnv.scala:91](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/RpcEnv.scala#L91) — `setupEndpoint`
- [RpcEndpoint.scala:46](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/RpcEndpoint.scala#L46) — endpoint lifecycle + `receive`/`receiveAndReply`
- [RpcEndpoint.scala:148](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/RpcEndpoint.scala#L148) — `ThreadSafeRpcEndpoint` / `IsolatedRpcEndpoint` / `IsolatedThreadSafeRpcEndpoint`
- [NettyRpcEnv.scala:138](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala#L138) — `NettyRpcEnv.setupEndpoint`
- [NettyRpcEnv.scala:489](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala#L489) — `NettyRpcEnvFactory.create`

**Configs:** none directly (wiring). Threading is sized by `numUsableCores` passed to the env.

**Maps to topics:** E1

---

## Dispatcher, Inbox, and the MessageLoop threading model
**What it is:** The `Dispatcher` owns the name→`MessageLoop` and endpoint→`RpcEndpointRef` maps and routes every inbound message. Each registered endpoint gets an `Inbox` — a per-endpoint FIFO of `InboxMessage`s (`OnStart`, `RpcMessage`, `OneWayMessage`, `OnStop`, connect/disconnect events) processed thread-safely. Two `MessageLoop` flavours drive inboxes: `SharedMessageLoop` runs a fixed thread pool serving many endpoints (the default for ordinary endpoints), while `DedicatedMessageLoop` gives an `IsolatedRpcEndpoint` its own pool sized by `threadCount()`. The loop keeps a `LinkedBlockingQueue[Inbox]` of "active" inboxes; a worker `take()`s one and calls `inbox.process`. `OnStart` is always the inbox's first message; `Inbox.stop` appends `OnStop` and guarantees it is last.

**Code path:** `Dispatcher.registerRpcEndpoint` (dedicated vs shared decision) → `postMessage(name, msg)` → `MessageLoop.post` → `Inbox.post` + `setActive(inbox)` → worker thread `receiveLoop` → `Inbox.process(dispatcher)` → `endpoint.receive` / `receiveAndReply`.

**Anchor files:**

- [Dispatcher.scala:56](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/Dispatcher.scala#L56) — `registerRpcEndpoint`: `IsolatedRpcEndpoint` → `DedicatedMessageLoop`, else `sharedLoop.register`
- [Dispatcher.scala:172](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/Dispatcher.scala#L172) — `postMessage` (bounces with `RpcEnvStoppedException` when stopped, `SparkException("Could not find …")` when no loop)
- [MessageLoop.scala:35](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/MessageLoop.scala#L35) — abstract loop + `receiveLoop`/`PoisonPill`
- [MessageLoop.scala:103](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/MessageLoop.scala#L103) — `SharedMessageLoop` + `getNumOfThreads`
- [MessageLoop.scala:160](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/MessageLoop.scala#L160) — `DedicatedMessageLoop`
- [Inbox.scala:86](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/Inbox.scala#L86) — `Inbox.process` (concurrency gating, `OnStart`/`OnStop` handling)

**Configs:**

- `spark.rpc.netty.dispatcher.numThreads` — shared-loop pool size; falls back to `max(2, availableCores)`, then can be overridden per-role by `spark.{driver,executor}.rpc.netty.dispatcher.numThreads` ([MessageLoop.scala:111](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/MessageLoop.scala#L111)).

!!! note "Silent/edge paths"

    On dispatcher `stop()` every endpoint is unregistered and a `PoisonPill` inbox is queued so worker threads exit cleanly ([MessageLoop.scala:53](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/MessageLoop.scala#L53)). Messages posted after stop are dropped and logged at debug (`RpcEnvStoppedException`). If a non-`ThreadSafeRpcEndpoint` finishes `OnStart`, the inbox enables concurrent processing — meaning its `receive` can run on multiple threads at once.

**Maps to topics:** E1

---

## RpcEndpointRef send/ask — local shortcut vs Outbox remote send
**What it is:** `RpcEndpointRef` is the serializable handle callers use. `send` is fire-and-forget; `ask`/`askSync` expect a reply; `askAbortable` returns an `AbortableRpcFuture`. `NettyRpcEndpointRef` wraps a `RequestMessage(senderAddress, receiver, content)` and hands it to `NettyRpcEnv`. If the receiver address equals the local env address, the message is posted straight into the local `Dispatcher` (no serialization, no socket); otherwise it is serialized and pushed to a per-remote-`RpcAddress` `Outbox`. The `Outbox` caches messages, lazily opens a `TransportClient` on `clientConnectionExecutor`, then drains its queue over that connection.

**Code path (remote ask):** `NettyRpcEndpointRef.ask` → `NettyRpcEnv.askAbortable` → (remote branch) `RpcOutboxMessage(serialize)` → `postToOutbox` → `Outbox.send` → `drainOutbox` → `launchConnectTask` → `NettyRpcEnv.createClient` → `client.sendRpc`. **Local ask:** `askAbortable` → `dispatcher.postLocalMessage(message, promise)`.

**Anchor files:**

- [RpcEndpointRef.scala:45](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/RpcEndpointRef.scala#L45) — `send`/`ask`/`askSync`/`askAbortable` API
- [NettyRpcEnv.scala:191](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala#L191) — `send`: local-address shortcut vs `postToOutbox`
- [NettyRpcEnv.scala:210](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala#L210) — `askAbortable`: local `postLocalMessage` vs remote `RpcOutboxMessage`, plus the timeout-scheduler
- [Outbox.scala:125](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/Outbox.scala#L125) — `Outbox.send` / `drainOutbox` / `launchConnectTask`
- [Outbox.scala:229](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/Outbox.scala#L229) — `handleNetworkFailure`

**Configs:**

- `spark.rpc.connect.threads` — size of the cached `clientConnectionExecutor` used to open outbound connections ([NettyRpcEnv.scala:97](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala#L97)).
- `spark.rpc.io.numConnectionsPerPeer` — is in the slice but **forced to 1** for the RPC module: `NettyRpcEnv` clones the conf with `RPC_IO_NUM_CONNECTIONS_PER_PEER = 1` before building `transportConf` ([NettyRpcEnv.scala:56](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala#L56)). The knob still governs the shuffle/block transports; for RPC it is effectively pinned.

!!! warning "Connection-failure edge paths"

    `Outbox.handleNetworkFailure` marks the outbox stopped, removes it from `NettyRpcEnv.outboxes` (so the *next* message rebuilds a fresh connection), and fails every queued message with the cause. A one-way send that fails logs a warning and is lost — there is no retry at this layer. On env shutdown, remaining outbox messages fail with `SparkException("Message is dropped because Outbox is stopped")`.

**Maps to topics:** E1

---

## RpcTimeout and the network.timeout fallback chain
**What it is:** `RpcTimeout` couples a `FiniteDuration` with the config key that set it, so a `TimeoutException` can be rewritten into an `RpcTimeoutException` that names the responsible property. Two default timeouts are built from a *prioritized key list*: ask uses `[spark.rpc.askTimeout, spark.network.timeout]`, lookup uses `[spark.rpc.lookupTimeout, spark.network.timeout]`, both defaulting to `120s` if neither is set. The actual timeout in `askAbortable` is enforced by a scheduled task on `timeoutScheduler` that fails the promise with a `TimeoutException` after `timeout.duration`.

**Code path:** `RpcEndpointRef` construction → `RpcUtils.askRpcTimeout(conf)` → `RpcTimeout(conf, Seq(RPC_ASK_TIMEOUT.key, NETWORK_TIMEOUT.key), "120s")` → first set key wins → `awaitResult` wraps timeout via `addMessageIfTimeout`.

**Anchor files:**

- [RpcTimeout.scala:42](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/RpcTimeout.scala#L42) — `RpcTimeout` + `addMessageIfTimeout`
- [RpcTimeout.scala:120](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/RpcTimeout.scala#L120) — prioritized-key `apply` (the fallback list)
- [RpcUtils.scala:47](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/RpcUtils.scala#L47) — `askRpcTimeout`
- [RpcUtils.scala:52](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/RpcUtils.scala#L52) — `lookupRpcTimeout`
- [NettyRpcEnv.scala:258](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala#L258) — the scheduled `TimeoutException` that fires the failure

**Configs:**

- `spark.rpc.askTimeout` (default None → falls to `spark.network.timeout`)
- `spark.rpc.lookupTimeout` (default None → falls to `spark.network.timeout`)
- `spark.network.timeout` (default `120s`) — the base both fall back to; also the transport `connectionTimeoutMs` ([TransportConf.java:102](https://github.com/apache/spark/blob/v4.2.0/common/network-common/src/main/java/org/apache/spark/network/util/TransportConf.java#L102)).
- `spark.network.timeoutInterval` (default = `STORAGE_BLOCKMANAGER_TIMEOUTINTERVAL`) — the *check* interval, consumed by `HeartbeatReceiver.checkTimeoutIntervalMs` to expire dead executors ([HeartbeatReceiver.scala:88](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/HeartbeatReceiver.scala#L88)).

!!! info "What a timeout actually throws"

    A blocking `askSync` that times out throws `RpcTimeoutException` (a `TimeoutException` subclass) whose message ends with `". This timeout is controlled by <the winning key>"`. This is why misconfigured heartbeats surface as "controlled by spark.network.timeout" errors.

**Maps to topics:** E1

---

## RPC message-size enforcement
**What it is:** `spark.rpc.message.maxSize` (default `128` MB) caps a single RPC frame. `RpcUtils.maxMessageSizeBytes` converts it to bytes and rejects a value above `Int.MaxValue/1024/1024`. Call sites that assemble large messages (map-output statuses, task descriptions, scheduler results) check against it and fail loudly rather than overflow the frame.

**Code path:** `RpcUtils.maxMessageSizeBytes(conf)` → consumed by `MapOutputTracker`, `CoarseGrainedSchedulerBackend`, `Executor` (task result guardrails).

**Anchor files:**

- [RpcUtils.scala:59](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/RpcUtils.scala#L59) — `maxMessageSizeBytes` + the `IllegalArgumentException` guard
- [MapOutputTracker.scala:740](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/MapOutputTracker.scala#L740) — `maxRpcMessageSize` consumer
- [CoarseGrainedSchedulerBackend.scala:65](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala#L65) — task-launch size check

**Configs:** `spark.rpc.message.maxSize`

**Maps to topics:** E1

---

## Endpoint lookup and RpcEndpointVerifier
**What it is:** Before a caller can trust a remote `RpcEndpointRef`, it verifies the name exists on the remote env. Every server-mode env auto-registers one `RpcEndpointVerifier` under the name `"endpoint-verifier"`; a lookup sends it a `CheckExistence(name)` ask. If the endpoint is missing, the lookup future fails with `RpcEndpointNotFoundException`.

**Code path:** `RpcEnv.setupEndpointRef` → `setupEndpointRefByURI` → `NettyRpcEnv.asyncSetupEndpointRefByURI` → `verifier.ask[Boolean](CheckExistence(name))` → `RpcEndpointVerifier.receiveAndReply` → `dispatcher.verify(name)` → true ⇒ ref, false ⇒ `RpcEndpointNotFoundException`.

**Anchor files:**

- [NettyRpcEnv.scala:142](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala#L142) — `asyncSetupEndpointRefByURI` + the not-found failure
- [RpcEndpointVerifier.scala:27](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/RpcEndpointVerifier.scala#L27) — the verifier endpoint
- [RpcEndpointAddress.scala:60](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/RpcEndpointAddress.scala#L60) — `spark://name@host:port` URL parsing
- [RpcUtils.scala:30](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/util/RpcUtils.scala#L30) — `makeDriverRef` (the common lookup helper)

**Configs:** lookup uses the `spark.rpc.lookupTimeout` → `spark.network.timeout` chain (see above).

**Maps to topics:** E1

---

## Transport layer — connection setup and thread sizing
**What it is:** `NettyRpcEnv` builds one `TransportContext` around a `TransportConf` produced by `SparkTransportConf.fromSparkConf(conf, module="rpc", …)`. That context mints a `TransportClientFactory` (pooled outbound clients, keyed by peer) and, in server mode, a `TransportServer`. Each channel is wired through a `TransportChannelHandler`. The `spark.rpc.io.*` keys become the module-scoped `spark.rpc.io.{serverThreads,clientThreads,numConnectionsPerPeer,…}` that `TransportConf` reads; `spark.rpc.io.threads` (custom, read in `NettyRpcEnv`) supplies the default thread count, defaulting to `numUsableCores`.

**Code path:** `NettyRpcEnv` init → `SparkTransportConf.fromSparkConf(clone, "rpc", RPC_IO_THREADS.getOrElse(numUsableCores), role, sslOptions)` → `new TransportConf("rpc", provider)` → `TransportContext.createClientFactory` / `createServer` → `TransportClientFactory.createClient` (blocking connect, guarded by `connectionCreationTimeoutMs`).

**Anchor files:**

- [SparkTransportConf.scala:43](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/network/netty/SparkTransportConf.scala#L43) — `fromSparkConf`; role > module > default thread precedence
- [NettyRpcEnv.scala:56](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala#L56) — the RPC `transportConf` (forces `numConnectionsPerPeer=1`, wires `spark.rpc.io.threads`)
- [TransportContext.java:157](https://github.com/apache/spark/blob/v4.2.0/common/network-common/src/main/java/org/apache/spark/network/TransportContext.java#L157) — `createClientFactory` / `createServer`
- [TransportClientFactory.java:234](https://github.com/apache/spark/blob/v4.2.0/common/network-common/src/main/java/org/apache/spark/network/client/TransportClientFactory.java#L234) — `createClient` (pool, connect, timeout)
- [TransportConf.java:119](https://github.com/apache/spark/blob/v4.2.0/common/network-common/src/main/java/org/apache/spark/network/util/TransportConf.java#L119) — `numConnectionsPerPeer` / `serverThreads` / `clientThreads`

**Configs:**

- `spark.rpc.io.threads` (default None → `numUsableCores`) — thread count seed for the RPC transport.
- `spark.rpc.io.numConnectionsPerPeer` (default 1; pinned to 1 for RPC as noted).
- `spark.network.crypto.enabled` / `spark.network.crypto.saslFallback` — transport-setup only here: they select `AuthClientBootstrap`/`AuthServerBootstrap` when `securityManager.isAuthenticationEnabled()` ([NettyRpcEnv.scala:71](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala#L71)).

!!! info "`spark.network.crypto.*` is owned by the config-security sweep"

    These two keys appear in this slice only because the RPC transport *chooses whether to install the auth bootstraps*. The X25519 `AuthEngine` handshake, SASL fallback semantics, and IO encryption are derived in the [config & security sweep](core-config-security.md) — treat them there as the authority; here they are purely a connection-setup toggle.

**Maps to topics:** E1, E2

---

## Transport timeouts, fetch-to-mem, and Netty-OOM retry
**What it is:** Three transport-adjacent knobs affect RPC-relevant traffic. `spark.network.timeout` is the base connection timeout (`TransportConf.connectionTimeoutMs`, from which `connectionCreationTimeoutMs` derives). `spark.network.maxRemoteBlockSizeFetchToMem` decides when a remote block is streamed to disk instead of buffered in memory (a giant fetch would otherwise pin heap/direct memory); it is also validated at executor start against available direct memory. `spark.shuffle.maxAttemptsOnNettyOOM` bounds how many times a block fetch retries after a Netty `OutOfDirectMemoryError` before giving up with a fetch failure. `spark.network.remoteReadNioBufferConversion` toggles an older NIO-buffer conversion path in `BlockManager` and is **deprecated as of 3.5.2**.

**Code path (Netty-OOM):** `ShuffleBlockFetcherIterator` catches `OutOfDirectMemoryError` → if `blockOOMRetryCounts < maxAttemptsOnNettyOOM` it sets `isNettyOOMOnShuffle` and defers the block; `resetNettyOOMFlagIfPossible` clears the flag once direct memory frees up; exceeding the cap throws.

**Anchor files:**

- [TransportConf.java:102](https://github.com/apache/spark/blob/v4.2.0/common/network-common/src/main/java/org/apache/spark/network/util/TransportConf.java#L102) — `connectionTimeoutMs` reads `spark.network.timeout`
- [ShuffleBlockFetcherIterator.scala:345](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L345) — `OutOfDirectMemoryError` retry gate on `maxAttemptsOnNettyOOM`
- [ShuffleBlockFetcherIterator.scala:1456](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L1456) — `resetNettyOOMFlagIfPossible`
- [CoarseGrainedExecutorBackend.scala:90](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/executor/CoarseGrainedExecutorBackend.scala#L90) — startup check of `maxRemoteBlockSizeFetchToMem` vs direct memory
- [BlockManager.scala:218](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L218) — `remoteReadNioBufferConversion` read site
- [SparkConf.scala:720](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/SparkConf.scala#L720) — deprecation of `spark.network.remoteReadNioBufferConversion` (3.5.2)

**Configs:** `spark.network.timeout`, `spark.network.maxRemoteBlockSizeFetchToMem`, `spark.shuffle.maxAttemptsOnNettyOOM` (internal), `spark.network.remoteReadNioBufferConversion` (deprecated 3.5.2).

**Maps to topics:** E1, E2

---

## ResourceProfile — the default profile and executor/task requests
**What it is:** A `ResourceProfile` bundles executor-side (`ExecutorResourceRequest`) and task-side (`TaskResourceRequest`) requirements for the stage an RDD runs in; it is immutable and built with `ResourceProfileBuilder`. Built-in executor resources are `cores`, `memory`, `offHeap`, `memoryOverhead`, `pyspark.memory`; the built-in task resource is `cpus`; anything else (e.g. `gpu`) is a custom resource. The **default profile** (id `0`) is synthesized from the application-level configs (`spark.executor.cores`, `spark.task.cpus`, memory, plus any custom `spark.executor.resource.*`). `TaskResourceProfile` is a task-only profile for stage-level scheduling without dynamic allocation.

**Code path:** `ResourceProfileBuilder.build` → `new ResourceProfile(execReqs, taskReqs)` → `validate()` (task amount must be `≤ 0.5` or whole) → id assigned. Default: `ResourceProfile.getOrCreateDefaultProfile` → `getDefaultTaskResources` (`spark.task.cpus` + `ResourceUtils.addTaskResourceRequests`) + `getDefaultExecutorResources` (`spark.executor.cores`, memory, `ResourceUtils.parseAllResourceRequests`).

**Anchor files:**

- [ResourceProfile.scala:50](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceProfile.scala#L50) — the class + `validate`
- [ResourceProfile.scala:326](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceProfile.scala#L326) — built-in resource names + `getOrCreateDefaultProfile`
- [ResourceProfile.scala:402](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceProfile.scala#L402) — `getDefaultTaskResources` / `getDefaultExecutorResources`
- [ResourceProfileBuilder.scala:37](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceProfileBuilder.scala#L37) — the builder
- [ExecutorResourceRequest.scala:54](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ExecutorResourceRequest.scala#L54) — executor request (`amount` is a whole `Long`)
- [TaskResourceRequest.scala:114](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/TaskResourceRequest.scala#L114) — task request (`amount` is a `Double`; `≤ 1.0` or whole)

**Configs:** `spark.executor.cores` (default 1), `spark.task.cpus` (default 1) — the built-ins baked into the default profile.

**Maps to topics:** E2

---

## ResourceProfileManager — validation, dedup, and merge conflicts
**What it is:** One `ResourceProfileManager` per `SparkContext` holds `id → ResourceProfile`, so the rest of Spark passes ids instead of whole profiles. It validates that a non-default profile is *supported* by the cluster manager (YARN, K8s, or Standalone with dynamic allocation; `TaskResourceProfile` also on those when DA is off) and eagerly computes `limitingResource`/`maxTasks` on add. `getEquivalentProfile` dedups structurally-equal profiles. The `profileMergeConflicts` config governs what happens when two different profiles reach the same stage: default `false` throws; `true` merges by taking the max of each resource.

**Code path:** `addResourceProfile(rp)` → `isSupported(rp)` (throws `SparkException` if the master can't support it) → write-locked put → `rp.limitingResource(conf)` + `SparkListenerResourceProfileAdded`. Conflict handling lives in the scheduler stage-merge path, gated by `RESOURCE_PROFILE_MERGE_CONFLICTS`.

**Anchor files:**

- [ResourceProfileManager.scala:40](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceProfileManager.scala#L40) — the manager + lock
- [ResourceProfileManager.scala:69](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceProfileManager.scala#L69) — `isSupported` (cluster-manager gating)
- [ResourceProfileManager.scala:128](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceProfileManager.scala#L128) — `addResourceProfile`
- [ResourceProfileManager.scala:168](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceProfileManager.scala#L168) — `getEquivalentProfile` (dedup)

**Configs:**

- `spark.scheduler.resource.profileMergeConflicts` (default false) — throw vs merge on conflicting profiles into one stage.
- `spark.testing.resourceProfileManager` (default false) — test-only: forces the unsupported-profile exception even under unit tests ([ResourceProfileManager.scala:57](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceProfileManager.scala#L57)).

**Maps to topics:** E2 (merge/gating), and the stage-level-scheduling proposal below.

---

## ResourceUtils — discovery (resourcesFile vs script) and the dynamic-key resource configs
**What it is:** `ResourceUtils` is the parse-and-discover engine for custom resources. Each resource is addressed by `ResourceID(componentName, resourceName)` whose conf prefix is `spark.{driver|executor|task}.resource.{name}.` with suffixes `amount`, `discoveryScript`, `vendor`. Discovery has two sources: an explicit **resources file** (`--resourcesFile`, a JSON array of `ResourceAllocation` with concrete addresses, used by some cluster managers) parsed by `parseAllocated`, or, for anything not in the file, a **discovery script / plugin** run by `discoverResource`. Plugins from `spark.resources.discoveryPlugin` run first; the built-in `ResourceDiscoveryScriptPlugin` (which shells out to the per-resource script and parses its JSON) is always tried last.

**Code path:** `getOrDiscoverAllResources(conf, component, resourcesFileOpt)` → `parseAllResourceRequests` (reads `…{name}.amount`) + `parseAllocatedOrDiscoverResources` → for each un-allocated id: `discoverResource` → iterate `RESOURCES_DISCOVERY_PLUGIN` classes then `ResourceDiscoveryScriptPlugin` → `assertAllResourceAllocationsMeetRequests` (require addresses ≥ requested amount).

**Anchor files:**

- [ResourceUtils.scala:46](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceUtils.scala#L46) — `ResourceID` (the `{component}.resource.{name}.{amount|discoveryScript|vendor}` shape)
- [ResourceUtils.scala:142](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceUtils.scala#L142) — `parseResourceRequest` (`getAllWithPrefix`, throws if no `amount`)
- [ResourceUtils.scala:323](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceUtils.scala#L323) — `getOrDiscoverAllResources`
- [ResourceUtils.scala:391](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceUtils.scala#L391) — `discoverResource` (plugin chain, script last)
- [ResourceDiscoveryScriptPlugin.scala:41](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceDiscoveryScriptPlugin.scala#L41) — script execution + failure paths
- [ResourceDiscoveryPlugin.java:38](https://github.com/apache/spark/blob/v4.2.0/core/src/main/java/org/apache/spark/api/resource/ResourceDiscoveryPlugin.java#L38) — the plugin SPI (`Optional<ResourceInformation> discoverResource(...)`)

**Configs:**

- `spark.driver.resourcesFile` (default None, internal) — driver's allocated-resource JSON (standalone).
- `spark.worker.resourcesFile` (default None, internal) — worker's allocated-resource JSON (standalone).
- `spark.resources.discoveryPlugin` (default `Nil`) — ordered custom discovery-plugin classes, tried before the script.

!!! info "The per-resource amount/discovery configs are dynamic-key and absent from the config catalog"

    The resource *amounts and discovery scripts* are keyed by resource name embedded in the key, so the deterministic config parser cannot enumerate them. The real sub-key shapes, read by `ResourceUtils` via `SparkConf.getAllWithPrefix`, are:

    - `spark.executor.resource.{name}.amount` / `.discoveryScript` / `.vendor`
    - `spark.driver.resource.{name}.amount` / `.discoveryScript` / `.vendor`
    - `spark.task.resource.{name}.amount`

    (`{name}` is e.g. `gpu`, `fpga`.) Task amounts are fractional `Double`s; executor/driver amounts are whole integers. These are **not** in the config catalog for the same reason SSL and network-crypto per-key configs are absent from the config-security sweep — they are prefix/dynamic-read, not `ConfigEntry` constants.

**Maps to topics:** E2

---

## ResourceAllocator — address assignment and the executor/task arithmetic
**What it is:** `ResourceAllocator` is the per-executor bookkeeper that hands resource *addresses* (e.g. GPU indices) to tasks and reclaims them on completion. To avoid floating-point drift with fractional amounts it works in fixed-point units of `ONE_ENTIRE_RESOURCE = 1e16`: each address starts fully available, `acquire` subtracts, `release` adds, and both throw if an address doesn't exist or would go negative / exceed one whole unit. The executor↔task arithmetic — how many tasks fit — is computed in `ResourceProfile.calculateTasksAndLimitingResource`: `tasksBasedOnCores = executorCores / cpusPerTask`, and for each custom resource `tasks = (execAmount * numParts) / taskAmount`; the **limiting resource** is whichever yields the fewest slots. Fractional task amounts (e.g. gpu `0.5`) become `(1 address, 2 parts)` via `calculateAmountAndPartsForFraction`, i.e. 2 tasks share one address.

**Code path:** task launch → `ResourceAllocator.acquire(addressesAmounts)` → `addressAvailabilityMap` decremented; task end → `release`. Slot math: `ResourceProfile.calculateTasksAndLimitingResource` → `ResourceUtils.calculateAmountAndPartsForFraction` + `validateTaskCpusLargeEnough` + `warnOnWastedResources`.

**Anchor files:**

- [ResourceAllocator.scala:71](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceAllocator.scala#L71) — the allocator trait, `acquire`/`release`, availability map
- [ResourceAllocator.scala:25](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceAllocator.scala#L25) — `ResourceAmountUtils` fixed-point (`ONE_ENTIRE_RESOURCE`)
- [ResourceProfile.scala:195](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceProfile.scala#L195) — `calculateTasksAndLimitingResource` (the fit arithmetic)
- [ResourceUtils.scala:178](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceUtils.scala#L178) — `calculateAmountAndPartsForFraction` (fractional amounts)
- [ResourceUtils.scala:410](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceUtils.scala#L410) — `validateTaskCpusLargeEnough`
- [ResourceUtils.scala:421](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceUtils.scala#L421) — `warnOnWastedResources`

**Configs:**

- `spark.executor.cores`, `spark.task.cpus` — the CPU side of the slot arithmetic.
- `spark.resources.warnings.testing` (default false) — test-only: turns the "wasted resources" warning into a thrown `SparkException` ([ResourceUtils.scala:465](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceUtils.scala#L465)).
- prefix-read per-resource amounts (above) drive the custom-resource side.

!!! warning "Fractional and mismatch edge paths"

    A task resource amount must be `≤ 0.5` or a whole number in a full `ResourceProfile` (`validate`), while a standalone `TaskResourceRequest` allows up to `1.0`. If an executor declares a custom resource but no task requests it, `calculateTasksAndLimitingResource` logs a warning; if a task requests a resource the executor never declares, it throws `SparkException("No executor resource configs were specified…")`. A discovery script that returns fewer addresses than requested fails the `require` in `assertAllResourceAllocationsMeetRequests`.

**Maps to topics:** E2

---

## Stage-level scheduling and accelerator-aware resources (GPU/FPGA)
**What it is:** The umbrella feature that ties the resources area together — attaching a custom `ResourceProfile` to an RDD so a *stage* can request more/fewer CPUs, memory, or accelerators than the application default, and the scheduler places its tasks only on executors that satisfy the profile. This spans `ResourceProfileBuilder` (author the profile), `ResourceProfileManager` (validate/dedup/merge), `ResourceUtils` (discover GPU/FPGA addresses), and `ResourceAllocator` (assign addresses to tasks) — none of which any existing learning-path topic teaches as a subject. E1 covers memory/execution internals; E2 covers deployment and resource *sizing* but not stage-level profiles or accelerator scheduling; A-level topics cover joins/AQE/skew/streaming. This is a distinct, learnable production capability (GPU ML/inference stages, mixed CPU/GPU pipelines) with real edge behaviour (merge conflicts, fractional GPU sharing, discovery failures).

**Code path:** `rdd.withResources(profile)` → `ResourceProfileManager.addResourceProfile` → scheduler groups stages by profile id → `ResourceProfile.calculateTasksAndLimitingResource` → executor-side `ResourceUtils.getOrDiscoverAllResourcesForResourceProfile` → `ResourceAllocator.acquire`.

**Anchor files:**

- [ResourceProfile.scala:287](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceProfile.scala#L287) — `TaskResourceProfile` (stage-level task scheduling without DA)
- [ResourceProfileManager.scala:117](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceProfileManager.scala#L117) — `canBeScheduled` (task-profile ↔ executor-profile matching)
- [ResourceUtils.scala:356](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/resource/ResourceUtils.scala#L356) — `getOrDiscoverAllResourcesForResourceProfile`

**Configs:** `spark.scheduler.resource.profileMergeConflicts`, `spark.executor.cores`, `spark.task.cpus`, and the prefix-read `spark.{executor,task,driver}.resource.{name}.*` family.

**Maps to topics:** [] — genuinely homeless.

> **propose: A16 (Advanced) — "Stage-Level Scheduling and Accelerator-Aware Resources (GPU/FPGA)"**
> No existing topic (A1–A15, E1–E16) covers stage-level scheduling or accelerator resources; E2 stops at sizing/dynamic-allocation and E1 at memory/execution internals. Proposed at **Advanced** rather than Expert because it is a production scheduling capability (GPU stages, mixed pipelines) built on the DAGScheduler and dynamic allocation the A-track already teaches, not a low-level runtime internal. A16 is the next unused Advanced code (highest existing is A15). Alternatively placeable at E17 if the map prefers to keep all resource-model depth on the Expert track alongside E1/E2.

---

## BlockTransferService — the data plane

**What it is:** the *second* Netty server every executor runs. The RPC env carries control messages; `NettyBlockTransferService` carries **block bytes** — every shuffle fetch, every remote read of a cached partition, every block replication and every decommission migration. It is a distinct `TransportServer` on its own port with its own thread pools, and confusing it with the RPC env is why "the RPC layer" gets blamed for shuffle throughput problems.

**Code path:** `BlockManager` → `blockTransferService.fetchBlocks(host, port, execId, blockIds, listener, tempFileManager)` → `RetryingBlockTransferor` (if `maxIORetries > 0`) → `OneForOneBlockFetcher` → remote `NettyBlockRpcServer` → `blockManager.getLocalBlockData` → bytes back to the listener

**Anchor files:**

- [BlockTransferService.scala:36](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/network/BlockTransferService.scala#L36) — the abstract contract, extending `BlockStoreClient`; `fetchBlockSync` and `uploadBlockSync` are the blocking wrappers over the async API
- [NettyBlockTransferService.scala:69](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/network/netty/NettyBlockTransferService.scala#L69) — `init(blockDataManager)`: this service cannot start until the `BlockManager` exists, which is the other half of the initialization ordering the [storage sweep](core-storage-serializer.md) describes
- [NettyBlockTransferService.scala:97](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/network/netty/NettyBlockTransferService.scala#L97) — `createServer`: its **own** `TransportServer`, separate from the RPC env's
- [NettyBlockTransferService.scala:130](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/network/netty/NettyBlockTransferService.scala#L130) — `transportConf.maxIORetries()`, i.e. `spark.<module>.io.maxRetries` — a retry layer *below* the fetch-failure handling the [execution-engine sweep](core-execution-engine.md) traces, so a `FetchFailed` reaching the driver has already exhausted these
- [NettyBlockTransferService.scala:137](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/network/netty/NettyBlockTransferService.scala#L137) — `createClient(host, port, maxRetries > 0)`: the third argument turns on connection retry only when block retries are enabled
- [NettyBlockRpcServer.scala:79](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/network/netty/NettyBlockRpcServer.scala#L79) — the server-side message vocabulary: `OpenBlocks`, `FetchShuffleBlocks`, `UploadBlock`
- [NettyBlockRpcServer.scala:172](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/network/netty/NettyBlockRpcServer.scala#L172) — `receiveStream` for `UploadBlockStream`: a replicated block above the size threshold streams rather than materialising in memory on the receiver

!!! info "Two servers, two ports, two sets of thread configs"

    `spark.rpc.io.*` sizes the control plane; `spark.shuffle.io.*` sizes this one, and `spark.blockManager.port` is where it listens. Both go through `SparkTransportConf`, which is why the config names look interchangeable and are not. A firewall rule that opens only `spark.driver.port` leaves block transfer broken while RPC works — the classic "the job starts and then hangs at the first shuffle" shape.

!!! warning "There is a retry layer here that the driver never sees"

    `maxIORetries` retries the *transfer* before any `FetchFailedException` is raised. So a stage that fails with `FetchFailed` has already burned this budget silently, and raising `spark.shuffle.io.maxRetries` / `.retryWait` changes how long a flaky network is tolerated before the driver's stage-retry machinery is even involved.

**Configs:** `spark.blockManager.port`, `spark.shuffle.io.maxRetries`, `.retryWait`, `.preferDirectBufs`, `spark.rpc.io.*`, `spark.network.timeout`

**Maps to topics:** E1, E2, A4, I6

---

## The RpcEnv file server

**What it is:** where `SparkContext.addFile` and `addJar` actually put things when there is no external filesystem. `NettyStreamManager` implements both `StreamManager` and `RpcEnvFileServer`, registering files in three maps and serving them over the RPC transport as `spark://host:port/files/<name>` URIs — which is why an executor can fetch a driver-added jar with no HDFS or S3 involved.

**Anchor files:**

- [NettyStreamManager.scala:39](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyStreamManager.scala#L39) — the dual role, and the three registries: `files`, `jars`, `dirs`
- [NettyStreamManager.scala:54](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyStreamManager.scala#L54) — `openStream` parses `<type>/<name>` out of the URI; an unknown type is a `require` failure, and a directory registered via `addDirectory` is resolved relative to its base
- [NettyStreamManager.scala:50](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyStreamManager.scala#L50) — `getChunk` throws `UnsupportedOperationException`: this server is **stream-only**, unlike the block transfer service, so files are read sequentially and cannot be range-fetched
- [NettyStreamManager.scala:65](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyStreamManager.scala#L65) — a missing or non-regular file returns `null` rather than raising, so the failure surfaces on the fetching side
- [RpcEnv.scala:168](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/RpcEnv.scala#L168) — the `RpcEnvFileServer` contract: `addFile`, `addJar`, `addDirectory`, `addDirectoryIfAbsent`

!!! info "This is why `addFile` scales badly and `--files` on a cluster FS does not"

    Every executor fetching a driver-added file streams it from the **driver's** RPC server. On a large cluster that is one process serving N sequential reads of the same bytes, with no chunking and no peer-to-peer step — the opposite of how `TorrentBroadcast` spreads a broadcast variable. For anything large, staging on a shared filesystem and passing a URI keeps the driver out of the data path.

**Maps to topics:** E2, B1

---

## RpcCallContext and the reply contract

**What it is:** the object an endpoint's `receiveAndReply` uses to answer. Three methods only — `reply`, `sendFailure`, `senderAddress` — with two implementations chosen by whether the caller is in this JVM: `LocalNettyRpcCallContext` hands the value straight to the caller's promise, `RemoteNettyRpcCallContext` serializes it onto the transport.

**Anchor files:**

- [RpcCallContext.scala:24](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/RpcCallContext.scala#L24) — the whole trait
- [NettyRpcCallContext.scala:31](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcCallContext.scala#L31) — `reply` and `sendFailure` both funnel into one abstract `send`
- [NettyRpcCallContext.scala:44](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcCallContext.scala#L44) — the local context: no serialization, which is the same local shortcut the send/ask concept describes
- [NettyRpcCallContext.scala:57](https://github.com/apache/spark/blob/v4.2.0/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcCallContext.scala#L57) — the remote context

!!! warning "An endpoint that neither replies nor fails hangs the caller until the timeout"

    Nothing enforces that `receiveAndReply` calls exactly one of `reply` or `sendFailure`. A branch that returns without answering leaves the caller's future unresolved until its `RpcTimeout` fires — which is why several timeouts in Spark exist and why the [execution-engine sweep](core-execution-engine.md) records that exceptions thrown inside `resourceOffers`/`statusUpdate` are swallowed by this framework (SPARK-31485): a swallowed exception is also an unsent reply.

**Maps to topics:** E1

---

## Breadth check — all 22 slice configs mapped

| # | Config | Default | Concept | Topic |
|---|--------|---------|---------|-------|
| 1 | `spark.driver.resourcesFile` | None | ResourceUtils — discovery (resourcesFile) | E2 |
| 2 | `spark.executor.cores` | 1 | ResourceProfile / ResourceAllocator arithmetic | E2 |
| 3 | `spark.network.crypto.enabled` | false | Transport — connection setup (auth bootstraps); owned by config-security | E2 |
| 4 | `spark.network.crypto.saslFallback` | true | Transport — connection setup; owned by config-security | E2 |
| 5 | `spark.network.maxRemoteBlockSizeFetchToMem` | 200m | Transport — fetch-to-mem | E1/E2 |
| 6 | `spark.network.remoteReadNioBufferConversion` | false | Transport — block read path (deprecated 3.5.2) | E1 |
| 7 | `spark.network.timeout` | 120s | RpcTimeout fallback base + transport connect timeout | E1 |
| 8 | `spark.network.timeoutInterval` | = STORAGE_BLOCKMANAGER_TIMEOUTINTERVAL | HeartbeatReceiver expiry interval | E1 |
| 9 | `spark.resources.discoveryPlugin` | Nil | ResourceUtils — discovery plugin chain | E2 |
| 10 | `spark.resources.warnings.testing` | false | ResourceAllocator — wasted-resource warning (test-only) | [] test |
| 11 | `spark.rpc.askTimeout` | None | RpcTimeout — ask fallback chain | E1 |
| 12 | `spark.rpc.connect.threads` | 64 | RpcEndpointRef — Outbox connect executor | E1 |
| 13 | `spark.rpc.io.numConnectionsPerPeer` | 1 | Transport — pinned to 1 for RPC | E1 |
| 14 | `spark.rpc.io.threads` | None | Transport — RPC thread sizing | E1 |
| 15 | `spark.rpc.lookupTimeout` | None | RpcTimeout — lookup fallback chain | E1 |
| 16 | `spark.rpc.message.maxSize` | 128 | RPC message-size enforcement | E1 |
| 17 | `spark.rpc.netty.dispatcher.numThreads` | None | Dispatcher — SharedMessageLoop pool | E1 |
| 18 | `spark.scheduler.resource.profileMergeConflicts` | false | ResourceProfileManager — merge conflicts | E2 (+A16) |
| 19 | `spark.shuffle.maxAttemptsOnNettyOOM` | 10 | Transport — Netty-OOM retry | E1 |
| 20 | `spark.task.cpus` | 1 | ResourceProfile / ResourceAllocator arithmetic | E2 |
| 21 | `spark.testing.resourceProfileManager` | false | ResourceProfileManager — force-exception (test-only) | [] test |
| 22 | `spark.worker.resourcesFile` | None | ResourceUtils — discovery (worker) | E2 |

**Prefix-read (dynamic-key, NOT in the config catalog):** `spark.executor.resource.{name}.amount` · `spark.executor.resource.{name}.discoveryScript` · `spark.executor.resource.{name}.vendor` · `spark.driver.resource.{name}.amount` · `spark.driver.resource.{name}.discoveryScript` · `spark.driver.resource.{name}.vendor` · `spark.task.resource.{name}.amount` — all read by `ResourceUtils` via `SparkConf.getAllWithPrefix`, mapped under the *ResourceUtils* and *ResourceAllocator* concepts. Also role-scoped `spark.{driver,executor}.rpc.netty.dispatcher.numThreads` (read in `SharedMessageLoop.getNumOfThreads`) and `spark.{rpc,files}.io.{serverThreads,clientThreads}` (read in `TransportConf`) are dynamic/module-scoped and thus not standalone catalog entries.

---

## Sweep log

| Date | Spark | What changed |
|---|---|---|
| 2026-07-22 | 4.2.0 | Initial sweep (RPC layer; resource profiles). 13 concepts, all 22 slice configs attributed in the breadth table above. One gap proposed: A16 (stage-level scheduling and accelerator-aware resources). |
| 2026-07-25 | 4.2.0 | Re-sweep. The config slice was already exhaustive, so this run was driven by package breadth, walking nested packages by hand after the [config & security sweep](core-config-security.md) established that `--coverage` cannot see them. `network/` was 5 files with 1 cited, and the gap was substantive: the **`BlockTransferService` data plane** had no concept at all. Three added — the block transfer service (a *second* Netty server per executor, its own port and thread configs, carrying every shuffle fetch and cached-block read, with an `maxIORetries` retry layer that runs entirely below the driver's fetch-failure handling), the **`RpcEnv` file server** (`NettyStreamManager`, stream-only, and why `addFile` puts the driver in the data path for every executor), and the **`RpcCallContext` reply contract** (nothing enforces that an endpoint answers, so a missing reply hangs the caller until its `RpcTimeout`). |
