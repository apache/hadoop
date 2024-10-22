<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
========

Metrics are statistical information exposed by Hadoop daemons, used for monitoring, performance tuning and debug. There are many metrics available by default and they are very useful for troubleshooting. This page shows the details of the available metrics.

Each section describes each context into which metrics are grouped.

The documentation of Metrics 2.0 framework is [here](../../api/org/apache/hadoop/metrics2/package-summary.html).

jvm context
===========

JvmMetrics
----------

Each metrics record contains tags such as ProcessName, SessionID and Hostname as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `MemNonHeapUsedM` | Current non-heap memory used in MB |
| `MemNonHeapCommittedM` | Current non-heap memory committed in MB |
| `MemNonHeapMaxM` | Max non-heap memory size in MB |
| `MemHeapUsedM` | Current heap memory used in MB |
| `MemHeapCommittedM` | Current heap memory committed in MB |
| `MemHeapMaxM` | Max heap memory size in MB |
| `MemMaxM` | Max memory size in MB |
| `ThreadsNew` | Current number of NEW threads |
| `ThreadsRunnable` | Current number of RUNNABLE threads |
| `ThreadsBlocked` | Current number of BLOCKED threads |
| `ThreadsWaiting` | Current number of WAITING threads |
| `ThreadsTimedWaiting` | Current number of TIMED\_WAITING threads |
| `ThreadsTerminated` | Current number of TERMINATED threads |
| `GcInfo` | Total GC count and GC time in msec, grouped by the kind of GC.  ex.) GcCountPS Scavenge=6, GCTimeMillisPS Scavenge=40, GCCountPS MarkSweep=0, GCTimeMillisPS MarkSweep=0 |
| `GcCount` | Total GC count |
| `GcTimeMillis` | Total GC time in msec |
| `LogFatal` | Total number of FATAL logs |
| `LogError` | Total number of ERROR logs |
| `LogWarn` | Total number of WARN logs |
| `LogInfo` | Total number of INFO logs |
| `GcNumWarnThresholdExceeded` | Number of times that the GC warn threshold is exceeded |
| `GcNumInfoThresholdExceeded` | Number of times that the GC info threshold is exceeded |
| `GcTotalExtraSleepTime` | Total GC extra sleep time in msec |
| `GcTimePercentage` | The percentage (0..100) of time that the JVM spent in GC pauses within the observation window if `dfs.namenode.gc.time.monitor.enable` is set to true. Use `dfs.namenode.gc.time.monitor.sleep.interval.ms` to specify the sleep interval in msec. Use `dfs.namenode.gc.time.monitor.observation.window.ms` to specify the observation window in msec. |

rpc context
===========

rpc
---

Each metrics record contains tags such as Hostname and port (number to which server is bound) as additional information along with metrics.
`rpc.metrics.timeunit` config can be used to configure timeunit for RPC metrics.
The default timeunit used for RPC metrics is milliseconds (as per the below description).

| Name | Description |
|:---- |:---- |
| `ReceivedBytes` | Total number of received bytes |
| `SentBytes` | Total number of sent bytes |
| `RpcQueueTimeNumOps` | Total number of RPC calls |
| `RpcQueueTimeAvgTime` | Average queue time in milliseconds |
| `RpcLockWaitTimeNumOps` | Total number of RPC calls (same as RpcQueueTimeNumOps) |
| `RpcLockWaitTimeAvgTime` | Average time waiting for lock acquisition in milliseconds |
| `RpcProcessingTimeNumOps` | Total number of RPC calls (same to RpcQueueTimeNumOps) |
| `RpcProcessingAvgTime` | Average Processing time in milliseconds |
| `DeferredRpcProcessingTimeNumOps` | Total number of Deferred RPC calls |
| `DeferredRpcProcessingAvgTime` | Average Deferred Processing time in milliseconds |
| `RpcResponseTimeNumOps` | Total number of RPC calls (same to RpcQueueTimeNumOps) |
| `RpcResponseAvgTime` | Average Response time in milliseconds |
| `RpcAuthenticationFailures` | Total number of authentication failures |
| `RpcAuthenticationSuccesses` | Total number of authentication successes |
| `RpcAuthorizationFailures` | Total number of authorization failures |
| `RpcAuthorizationSuccesses` | Total number of authorization successes |
| `RpcClientBackoff` | Total number of client backoff requests |
| `RpcClientBackoffDisconnected` | Total number of client backoff requests that are disconnected. This is a subset of RpcClientBackoff |
| `RpcSlowCalls` | Total number of slow RPC calls |
| `RpcRequeueCalls` | Total number of requeue RPC calls |
| `RpcCallsSuccesses` | Total number of RPC calls that are successfully processed |
| `NumOpenConnections` | Current number of open connections |
| `NumInProcessHandler` | Current number of handlers on working |
| `CallQueueLength` | Current length of the call queue |
| `numDroppedConnections` | Total number of dropped connections |
| `rpcQueueTime`*num*`sNumOps` | Shows total number of RPC calls (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcQueueTime`*num*`s50thPercentileLatency` | Shows the 50th percentile of RPC queue time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcQueueTime`*num*`s75thPercentileLatency` | Shows the 75th percentile of RPC queue time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcQueueTime`*num*`s90thPercentileLatency` | Shows the 90th percentile of RPC queue time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcQueueTime`*num*`s95thPercentileLatency` | Shows the 95th percentile of RPC queue time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcQueueTime`*num*`s99thPercentileLatency` | Shows the 99th percentile of RPC queue time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcProcessingTime`*num*`sNumOps` | Shows total number of RPC calls (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcProcessingTime`*num*`s50thPercentileLatency` | Shows the 50th percentile of RPC processing time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcProcessingTime`*num*`s75thPercentileLatency` | Shows the 75th percentile of RPC processing time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcProcessingTime`*num*`s90thPercentileLatency` | Shows the 90th percentile of RPC processing time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcProcessingTime`*num*`s95thPercentileLatency` | Shows the 95th percentile of RPC processing time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcProcessingTime`*num*`s99thPercentileLatency` | Shows the 99th percentile of RPC processing time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcLockWaitTime`*num*`sNumOps` | Shows total number of RPC calls (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcLockWaitTime`*num*`s50thPercentileLatency` | Shows the 50th percentile of RPC lock wait time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcLockWaitTime`*num*`s75thPercentileLatency` | Shows the 75th percentile of RPC lock wait time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcLockWaitTime`*num*`s90thPercentileLatency` | Shows the 90th percentile of RPC lock wait time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcLockWaitTime`*num*`s95thPercentileLatency` | Shows the 95th percentile of RPC lock wait time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcLockWaitTime`*num*`s99thPercentileLatency` | Shows the 99th percentile of RPC lock wait time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcResponseTime`*num*`sNumOps` | Shows total number of RPC calls (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcResponseTime`*num*`s50thPercentileLatency` | Shows the 50th percentile of RPC response time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcResponseTime`*num*`s75thPercentileLatency` | Shows the 75th percentile of RPC response time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcResponseTime`*num*`s90thPercentileLatency` | Shows the 90th percentile of RPC response time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcResponseTime`*num*`s95thPercentileLatency` | Shows the 95th percentile of RPC response time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `rpcResponseTime`*num*`s99thPercentileLatency` | Shows the 99th percentile of RPC response time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `deferredRpcProcessingTime`*num*`sNumOps` | Shows total number of Deferred RPC calls (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `deferredRpcProcessingTime`*num*`s50thPercentileLatency` | Shows the 50th percentile of Deferred RPC processing time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `deferredRpcProcessingTime`*num*`s75thPercentileLatency` | Shows the 75th percentile of Deferred RPC processing time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `deferredRpcProcessingTime`*num*`s90thPercentileLatency` | Shows the 90th percentile of Deferred RPC processing time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `deferredRpcProcessingTime`*num*`s95thPercentileLatency` | Shows the 95th percentile of Deferred RPC processing time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `deferredRpcProcessingTime`*num*`s99thPercentileLatency` | Shows the 99th percentile of Deferred RPC processing time in milliseconds (*num* seconds granularity) if `rpc.metrics.quantile.enable` is set to true. *num* is specified by `rpc.metrics.percentiles.intervals`. |
| `TotalRequests` | Total num of requests served by the RPC server. |
| `TotalRequestsPerSeconds` | Total num of requests per second served by the RPC server. |

RetryCache/NameNodeRetryCache
-----------------------------

RetryCache metrics is useful to monitor NameNode fail-over. Each metrics record contains Hostname tag.

| Name | Description |
|:---- |:---- |
| `CacheHit` | Total number of RetryCache hit |
| `CacheCleared` | Total number of RetryCache cleared |
| `CacheUpdated` | Total number of RetryCache updated |

FairCallQueue
-------------

FairCallQueue metrics will only exist if FairCallQueue is enabled. Each metric exists for each level of priority.

| Name | Description |
|:---- |:---- |
| `FairCallQueueSize_p`*Priority* | Current number of calls in priority queue |
| `FairCallQueueOverflowedCalls_p`*Priority* | Total number of overflowed calls in priority queue |

DecayRpcSchedulerDetailed
-------------------------

DecayRpcSchedulerDetailed metrics only exist when DecayRpcScheduler is used (FairCallQueue enabled). It is an addition
to FairCallQueue metrics. For each level of priority, rpcqueue and rpcprocessing detailed metrics are exposed.

| Name | Description |
|:---- | :---- |
|  `DecayRPCSchedulerPriority.`*Priority*`.RpcQueueTime` | RpcQueueTime metrics for each priority |
|  `DecayRPCSchedulerPriority.`*Priority*`.RpcProcessingTime` | RpcProcessingTime metrics for each priority |

rpcdetailed context
===================

Metrics of rpcdetailed context are exposed in unified manner by RPC layer. Two metrics are exposed for each RPC based on its name. Metrics named "(RPC method name)NumOps" indicates total number of method calls, and metrics named "(RPC method name)AvgTime" shows average processing time for method calls in milliseconds.
Please note that the AvgTime metrics do not include time spent waiting to acquire locks on data structures (see RpcLockWaitTimeAvgTime).
Metrics named "Overall(RPC method name)AvgTime" shows the average overall processing time for method calls
in milliseconds. It is measured from request arrival to when the response is sent back to the client.

rpcdetailed
-----------

Each metrics record contains tags such as Hostname and port (number to which server is bound) as additional information along with metrics.

The Metrics about RPCs which is not called are not included in metrics record.

| Name | Description |
|:---- |:---- |
| *methodname*`NumOps` | Total number of the times the method is called |
| *methodname*`AvgTime` | Average turn around time of the method in milliseconds |

dfs context
===========

namenode
--------

Each metrics record contains tags such as ProcessName, SessionId, and Hostname as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `CreateFileOps` | Total number of files created |
| `FilesCreated` | Total number of files and directories created by create or mkdir operations |
| `FilesAppended` | Total number of files appended |
| `GetBlockLocations` | Total number of getBlockLocations operations |
| `FilesRenamed` | Total number of rename **operations** (NOT number of files/dirs renamed) |
| `GetListingOps` | Total number of directory listing operations |
| `DeleteFileOps` | Total number of delete operations |
| `FilesDeleted` | Total number of files and directories deleted by delete or rename operations |
| `FileInfoOps` | Total number of getFileInfo and getLinkFileInfo operations |
| `AddBlockOps` | Total number of addBlock operations succeeded |
| `GetAdditionalDatanodeOps` | Total number of getAdditionalDatanode operations |
| `CreateSymlinkOps` | Total number of createSymlink operations |
| `GetLinkTargetOps` | Total number of getLinkTarget operations |
| `FilesInGetListingOps` | Total number of files and directories listed by directory listing operations |
| `SuccessfulReReplications` | Total number of successful block re-replications |
| `NumTimesReReplicationNotScheduled` | Total number of times that failed to schedule a block re-replication |
| `TimeoutReReplications` | Total number of timed out block re-replications |
| `AllowSnapshotOps` | Total number of allowSnapshot operations |
| `DisallowSnapshotOps` | Total number of disallowSnapshot operations |
| `CreateSnapshotOps` | Total number of createSnapshot operations |
| `DeleteSnapshotOps` | Total number of deleteSnapshot operations |
| `RenameSnapshotOps` | Total number of renameSnapshot operations |
| `ListSnapshottableDirOps` | Total number of snapshottableDirectoryStatus operations |
| `SnapshotDiffReportOps` | Total number of getSnapshotDiffReport operations |
| `TransactionsNumOps` | Total number of Journal transactions |
| `TransactionsAvgTime` | Average time of Journal transactions in milliseconds |
| `SyncsNumOps` | Total number of Journal syncs |
| `SyncsAvgTime` | Average time of Journal syncs in milliseconds |
| `SyncsTime`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of Journal sync time in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `TransactionsBatchedInSync` | Total number of Journal transactions batched in sync |
| `TransactionsBatchedInSync`*num*`s(50/75/90/95/99)thPercentileCount` | The 50/75/90/95/99th percentile of number of batched Journal transactions (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `StorageBlockReportNumOps` | Total number of processing block reports from individual storages in DataNode |
| `StorageBlockReportAvgTime` | Average time of processing block reports in milliseconds |
| `StorageBlockReport`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of block report processing time in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `CacheReportNumOps` | Total number of processing cache reports from DataNode |
| `CacheReportAvgTime` | Average time of processing cache reports in milliseconds |
| `CacheReport`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of cached report processing time in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `SafeModeTime` | The interval between FSNameSystem starts and the last time safemode leaves in milliseconds.  (sometimes not equal to the time in SafeMode, see [HDFS-5156](https://issues.apache.org/jira/browse/HDFS-5156)) |
| `FsImageLoadTime` | Time loading FS Image at startup in milliseconds |
| `GetEditNumOps` | Total number of edits downloads from SecondaryNameNode |
| `GetEditAvgTime` | Average edits download time in milliseconds |
| `GetImageNumOps` | Total number of fsimage downloads from SecondaryNameNode |
| `GetImageAvgTime` | Average fsimage download time in milliseconds |
| `PutImageNumOps` | Total number of fsimage uploads to SecondaryNameNode |
| `PutImageAvgTime` | Average fsimage upload time in milliseconds |
| `TotalFileOps`| Total number of file operations performed |
| `NNStartedTimeInMillis`| NameNode start time in milliseconds |
| `GenerateEDEKTimeNumOps` | Total number of generating EDEK |
| `GenerateEDEKTimeAvgTime` | Average time of generating EDEK in milliseconds |
| `GenerateEDEKTime`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of time spent in generating EDEK in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `WarmUpEDEKTimeNumOps` | Total number of warming up EDEK |
| `WarmUpEDEKTimeAvgTime` | Average time of warming up EDEK in milliseconds |
| `WarmUpEDEKTime`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of time spent in warming up EDEK in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `ResourceCheckTime`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of NameNode resource check latency in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `EditLogTailTimeNumOps` | Total number of times the standby NameNode tailed the edit log |
| `EditLogTailTimeAvgTime` | Average time (in milliseconds) spent by standby NameNode in tailing edit log |
| `EditLogTailTime`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of time spent in tailing edit logs by standby NameNode in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `EditLogFetchTimeNumOps` | Total number of times the standby NameNode fetched remote edit streams from journal nodes |
| `EditLogFetchTimeAvgTime` | Average time (in milliseconds) spent by standby NameNode in fetching remote edit streams from journal nodes |
| `EditLogFetchTime`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of time spent in fetching edit streams from journal nodes by standby NameNode in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `NumEditLogLoadedNumOps` | Total number of times edits were loaded by standby NameNode |
| `NumEditLogLoadedAvgCount` | Average number of edits loaded by standby NameNode in each edit log tailing |
| `NumEditLogLoaded`*num*`s(50/75/90/95/99)thPercentileCount` | The 50/75/90/95/99th percentile of number of edits loaded by standby NameNode in each edit log tailing (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `EditLogTailIntervalNumOps` | Total number of intervals between edit log tailings by standby NameNode |
| `EditLogTailIntervalAvgTime` | Average time of intervals between edit log tailings by standby NameNode in milliseconds |
| `EditLogTailInterval`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of time between edit log tailings by standby NameNode in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `PendingEditsCount` | Current number of pending edits |
| `AvoidNotInServiceNodeCount` | Total number of avoid not in service node |
| `AvoidStaleNodeCount` | Total number of avoid stale node |
| `AvoidXceiverOverLoadNodeCount` | Total number of avoid xceiver over load node |
| `AvoidVolumeOverLoadNodeCount` | Total number of avoid volume over load node |
| `AvoidPerRackOverStorageLimitNodeCount` | Total number of avoid per rack over storage limit node |
| `AvoidSlowNodeCount` | Total number of avoid slow node |
| `TotalAvoidDataNodeCount`| Total number of avoid datanode |

FSNamesystem
------------

Each metrics record contains tags such as HAState and Hostname as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `MissingBlocks` | Current number of missing blocks |
| `ExpiredHeartbeats` | Total number of expired heartbeats |
| `TransactionsSinceLastCheckpoint` | Total number of transactions since last checkpoint |
| `TransactionsSinceLastLogRoll` | Total number of transactions since last edit log roll |
| `LastWrittenTransactionId` | Last transaction ID written to the edit log |
| `LastCheckpointTime` | Time in milliseconds since epoch of last checkpoint |
| `CapacityTotal` | Current raw capacity of DataNodes in bytes |
| `CapacityTotalGB` | Current raw capacity of DataNodes in GB |
| `CapacityUsed` | Current used capacity across all DataNodes in bytes |
| `CapacityUsedGB` | Current used capacity across all DataNodes in GB |
| `CapacityRemaining` | Current remaining capacity in bytes |
| `CapacityRemainingGB` | Current remaining capacity in GB |
| `CapacityUsedNonDFS` | Current space used by DataNodes for non DFS purposes in bytes |
| `TotalLoad` | Current number of connections |
| `SnapshottableDirectories` | Current number of snapshottable directories |
| `Snapshots` | Current number of snapshots |
| `NumEncryptionZones` | Current number of encryption zones |
| `BlocksTotal` | Current number of allocated blocks in the system |
| `FilesTotal` | Current number of files and directories |
| `PendingReplicationBlocks` | Current number of blocks pending to be replicated |
| `UnderReplicatedBlocks` | Current number of blocks under replicated |
| `CorruptBlocks` | Current number of blocks with corrupt replicas. |
| `ScheduledReplicationBlocks` | Current number of blocks scheduled for replications |
| `PendingDeletionBlocks` | Current number of blocks pending deletion |
| `ExcessBlocks` | Current number of excess blocks |
| `PostponedMisreplicatedBlocks` | (HA-only) Current number of blocks postponed to replicate |
| `PendingDataNodeMessageCount` | (HA-only) Current number of pending block-related messages for later processing in the standby NameNode |
| `MillisSinceLastLoadedEdits` | (HA-only) Time in milliseconds since the last time standby NameNode load edit log. In active NameNode, set to 0 |
| `BlockCapacity` | Current number of block capacity |
| `NumLiveDataNodes` | Number of datanodes which are currently live |
| `NumDeadDataNodes` | Number of datanodes which are currently dead |
| `NumDecomLiveDataNodes` | Number of datanodes which have been decommissioned and are now live |
| `NumDecomDeadDataNodes` | Number of datanodes which have been decommissioned and are now dead |
| `NumDecommissioningDataNodes` | Number of datanodes in decommissioning state |
| `VolumeFailuresTotal` | Total number of volume failures across all Datanodes |
| `EstimatedCapacityLostTotal` | An estimate of the total capacity lost due to volume failures |
| `StaleDataNodes` | Current number of DataNodes marked stale due to delayed heartbeat |
| `NumStaleStorages` | Number of storages marked as content stale (after NameNode restart/failover before first block report is received) |
| `MissingReplOneBlocks` | Current number of missing blocks with replication factor 1 |
| `HighestPriorityLowRedundancyReplicatedBlocks` | Current number of non-corrupt, low redundancy replicated blocks with the highest risk of loss (have 0 or 1 replica). Will be recovered with the highest priority. |
| `HighestPriorityLowRedundancyECBlocks` | Current number of non-corrupt, low redundancy EC blocks with the highest risk of loss. Will be recovered with the highest priority. |
| `NumFilesUnderConstruction` | Current number of files under construction |
| `NumActiveClients` | Current number of active clients holding lease |
| `HAState` | (HA-only) Current state of the NameNode: initializing or active or standby or stopping state |
| `FSState` | Current state of the file system: Safemode or Operational |
| `LockQueueLength` | Number of threads waiting to acquire FSNameSystem lock |
| `ReadLockLongHoldCount` | The number of time the read lock has been held for longer than the threshold |
| `WriteLockLongHoldCount` | The number of time the write lock has been held for longer than the threshold |
| `TotalSyncCount` | Total number of sync operations performed by edit log |
| `TotalSyncTimes` | Total number of milliseconds spent by various edit logs in sync operation|
| `NameDirSize` | NameNode name directories size in bytes |
| `NumTimedOutPendingReconstructions` | The number of timed out reconstructions. Not the number of unique blocks that timed out. |
| `NumInMaintenanceLiveDataNodes` | Number of live Datanodes which are in maintenance state |
| `NumInMaintenanceDeadDataNodes` | Number of dead Datanodes which are in maintenance state |
| `NumEnteringMaintenanceDataNodes` | Number of Datanodes that are entering the maintenance state |
| `FSN(Read/Write)Lock`*OperationName*`NanosNumOps` | Total number of acquiring lock by operations |
| `FSN(Read/Write)Lock`*OperationName*`NanosAvgTime` | Average time of holding the lock by operations in nanoseconds |
| `FSN(Read/Write)LockOverallNanosNumOps`  | Total number of acquiring lock by all operations |
| `FSN(Read/Write)LockOverallNanosAvgTime` | Average time of holding the lock by all operations in nanoseconds |
| `PendingSPSPaths` | The number of paths to be processed by storage policy satisfier |

BlockManager
-------------

The metrics present statistics from the BlockManager's perspective.

| Name | Description                                                                                                                     |
|:---- |:--------------------------------------------------------------------------------------------------------------------------------|
| `StorageTypeStats` | key represents different StorageTypes, and value represents the detailed storage information corresponding to each StorageType. |

JournalNode
-----------

The server-side metrics for a journal from the JournalNode's perspective. Each metrics record contains Hostname tag as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `Syncs60sNumOps` | Number of sync operations (1 minute granularity) |
| `Syncs60s50thPercentileLatencyMicros` | The 50th percentile of sync latency in microseconds (1 minute granularity) |
| `Syncs60s75thPercentileLatencyMicros` | The 75th percentile of sync latency in microseconds (1 minute granularity) |
| `Syncs60s90thPercentileLatencyMicros` | The 90th percentile of sync latency in microseconds (1 minute granularity) |
| `Syncs60s95thPercentileLatencyMicros` | The 95th percentile of sync latency in microseconds (1 minute granularity) |
| `Syncs60s99thPercentileLatencyMicros` | The 99th percentile of sync latency in microseconds (1 minute granularity) |
| `Syncs300sNumOps` | Number of sync operations (5 minutes granularity) |
| `Syncs300s50thPercentileLatencyMicros` | The 50th percentile of sync latency in microseconds (5 minutes granularity) |
| `Syncs300s75thPercentileLatencyMicros` | The 75th percentile of sync latency in microseconds (5 minutes granularity) |
| `Syncs300s90thPercentileLatencyMicros` | The 90th percentile of sync latency in microseconds (5 minutes granularity) |
| `Syncs300s95thPercentileLatencyMicros` | The 95th percentile of sync latency in microseconds (5 minutes granularity) |
| `Syncs300s99thPercentileLatencyMicros` | The 99th percentile of sync latency in microseconds (5 minutes granularity) |
| `Syncs3600sNumOps` | Number of sync operations (1 hour granularity) |
| `Syncs3600s50thPercentileLatencyMicros` | The 50th percentile of sync latency in microseconds (1 hour granularity) |
| `Syncs3600s75thPercentileLatencyMicros` | The 75th percentile of sync latency in microseconds (1 hour granularity) |
| `Syncs3600s90thPercentileLatencyMicros` | The 90th percentile of sync latency in microseconds (1 hour granularity) |
| `Syncs3600s95thPercentileLatencyMicros` | The 95th percentile of sync latency in microseconds (1 hour granularity) |
| `Syncs3600s99thPercentileLatencyMicros` | The 99th percentile of sync latency in microseconds (1 hour granularity) |
| `NumTransactionsBatchedInSync60sNumOps` | Number of times transactions were batched in sync operation (1 minute granularity) |
| `NumTransactionsBatchedInSync60s50thPercentileLatencyMicros` | The 50th percentile of transactions batched in sync count (1 minute granularity) |
| `NumTransactionsBatchedInSync60s75thPercentileLatencyMicros` | The 75th percentile of transactions batched in sync count (1 minute granularity) |
| `NumTransactionsBatchedInSync60s90thPercentileLatencyMicros` | The 90th percentile of transactions batched in sync count (1 minute granularity) |
| `NumTransactionsBatchedInSync60s95thPercentileLatencyMicros` | The 95th percentile of transactions batched in sync count (1 minute granularity) |
| `NumTransactionsBatchedInSync60s99thPercentileLatencyMicros` | The 99th percentile of transactions batched in sync count (1 minute granularity) |
| `NumTransactionsBatchedInSync300sNumOps` | Number of times transactions were batched in sync operation (5 minutes granularity) |
| `NumTransactionsBatchedInSync300s50thPercentileLatencyMicros` | The 50th percentile of transactions batched in sync count (5 minutes granularity) |
| `NumTransactionsBatchedInSync300s75thPercentileLatencyMicros` | The 75th percentile of transactions batched in sync count (5 minutes granularity) |
| `NumTransactionsBatchedInSync300s90thPercentileLatencyMicros` | The 90th percentile of transactions batched in sync count (5 minutes granularity) |
| `NumTransactionsBatchedInSync300s95thPercentileLatencyMicros` | The 95th percentile of transactions batched in sync count (5 minutes granularity) |
| `NumTransactionsBatchedInSync300s99thPercentileLatencyMicros` | The 99th percentile of transactions batched in sync count (5 minutes granularity) |
| `NumTransactionsBatchedInSync3600sNumOps` | Number of times transactions were batched in sync operation (1 hour granularity) |
| `NumTransactionsBatchedInSync3600s50thPercentileLatencyMicros` | The 50th percentile of transactions batched in sync count (1 hour granularity) |
| `NumTransactionsBatchedInSync3600s75thPercentileLatencyMicros` | The 75th percentile of transactions batched in sync count (1 hour granularity) |
| `NumTransactionsBatchedInSync3600s90thPercentileLatencyMicros` | The 90th percentile of transactions batched in sync count (1 hour granularity) |
| `NumTransactionsBatchedInSync3600s95thPercentileLatencyMicros` | The 95th percentile of transactions batched in sync count (1 hour granularity) |
| `NumTransactionsBatchedInSync3600s99thPercentileLatencyMicros` | The 99th percentile of transactions batched in sync count (1 hour granularity) |
| `BatchesWritten` | Total number of batches written since startup |
| `TxnsWritten` | Total number of transactions written since startup |
| `BytesWritten` | Total number of bytes written since startup |
| `BatchesWrittenWhileLagging` | Total number of batches written where this node was lagging |
| `LastWriterEpoch` | Current writer's epoch number |
| `CurrentLagTxns` | The number of transactions that this JournalNode is lagging |
| `LastWrittenTxId` | The highest transaction id stored on this JournalNode |
| `LastPromisedEpoch` | The last epoch number which this node has promised not to accept any lower epoch, or 0 if no promises have been made |
| `LastJournalTimestamp` | The timestamp of last successfully written transaction |
| `TxnsServedViaRpc` | Number of transactions served via the RPC mechanism |
| `BytesServedViaRpc` | Number of bytes served via the RPC mechanism |
| `RpcRequestCacheMissAmountNumMisses` | Number of RPC requests which could not be served due to lack of data in the cache |
| `RpcRequestCacheMissAmountAvgTxns` | The average number of transactions by which a request missed the cache; for example if transaction ID 10 is requested and the cache's oldest transaction is ID 15, value 5 will be added to this average |
| `RpcEmptyResponses` | Number of RPC requests with zero edits returned |

datanode
--------

Each metrics record contains tags such as SessionId and Hostname as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `BytesWritten` | Total number of bytes written to DataNode |
| `BytesRead` | Total number of bytes read from DataNode |
| `ReadTransferRateNumOps` | Total number of data read transfers |
| `ReadTransferRateAvgTime` | Average transfer rate of bytes read from DataNode, measured in bytes per second. |
| `ReadTransferRate`*num*`s(50/75/90/95/99)thPercentileRate` | The 50/75/90/95/99th percentile of the transfer rate of bytes read from DataNode, measured in bytes per second. |
| `BlocksWritten` | Total number of blocks written to DataNode |
| `BlocksRead` | Total number of blocks read from DataNode |
| `BlocksReplicated` | Total number of blocks replicated |
| `BlocksRemoved` | Total number of blocks removed |
| `BlocksVerified` | Total number of blocks verified |
| `BlockVerificationFailures` | Total number of verifications failures |
| `BlocksCached` | Total number of blocks cached |
| `BlocksUncached` | Total number of blocks uncached |
| `ReadsFromLocalClient` | Total number of read operations from local client |
| `ReadsFromRemoteClient` | Total number of read operations from remote client |
| `WritesFromLocalClient` | Total number of write operations from local client |
| `WritesFromRemoteClient` | Total number of write operations from remote client |
| `BlocksGetLocalPathInfo` | Total number of operations to get local path names of blocks |
| `RamDiskBlocksWrite` | Total number of blocks written to memory |
| `RamDiskBlocksWriteFallback` | Total number of blocks written to memory but not satisfied (failed-over to disk) |
| `RamDiskBytesWrite` | Total number of bytes written to memory |
| `RamDiskBlocksReadHits` | Total number of times a block in memory was read |
| `RamDiskBlocksEvicted` | Total number of blocks evicted in memory |
| `RamDiskBlocksEvictedWithoutRead` | Total number of blocks evicted in memory without ever being read from memory |
| `RamDiskBlocksEvictionWindowMsNumOps` | Number of blocks evicted in memory|
| `RamDiskBlocksEvictionWindowMsAvgTime` | Average time of blocks in memory before being evicted in milliseconds |
| `RamDiskBlocksEvictionWindows`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of latency between memory write and eviction in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `RamDiskBlocksLazyPersisted` | Total number of blocks written to disk by lazy writer |
| `RamDiskBlocksDeletedBeforeLazyPersisted` | Total number of blocks deleted by application before being persisted to disk |
| `RamDiskBytesLazyPersisted` | Total number of bytes written to disk by lazy writer |
| `RamDiskBlocksLazyPersistWindowMsNumOps` | Number of blocks written to disk by lazy writer |
| `RamDiskBlocksLazyPersistWindowMsAvgTime` | Average time of blocks written to disk by lazy writer in milliseconds |
| `RamDiskBlocksLazyPersistWindows`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of latency between memory write and disk persist in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `FsyncCount` | Total number of fsync |
| `VolumeFailures` | Total number of volume failures occurred |
| `DatanodeNetworkErrors` | Count of network errors on the datanode |
| `DataNodeActiveXceiversCount` | Count of active dataNode xceivers |
| `DataNodeReadActiveXceiversCount` | Count of read active dataNode xceivers |
| `DataNodeWriteActiveXceiversCount` | Count of write active dataNode xceivers |
| `DataNodePacketResponderCount` | Count of active DataNode packetResponder |
| `DataNodeBlockRecoveryWorkerCount` | Count of active DataNode block recovery worker |
| `ReadBlockOpNumOps` | Total number of read operations |
| `ReadBlockOpAvgTime` | Average time of read operations in milliseconds |
| `WriteBlockOpNumOps` | Total number of write operations |
| `WriteBlockOpAvgTime` | Average time of write operations in milliseconds |
| `BlockChecksumOpNumOps` | Total number of blockChecksum operations |
| `BlockChecksumOpAvgTime` | Average time of blockChecksum operations in milliseconds |
| `CopyBlockOpNumOps` | Total number of block copy operations |
| `CopyBlockOpAvgTime` | Average time of block copy operations in milliseconds |
| `ReplaceBlockOpNumOps` | Total number of block replace operations |
| `ReplaceBlockOpAvgTime` | Average time of block replace operations in milliseconds |
| `HeartbeatsNumOps` | Total number of heartbeats |
| `HeartbeatsAvgTime` | Average heartbeat time in milliseconds |
| `HeartbeatsFor`*ServiceId*`-`*NNId*`NumOps` | Total number of heartbeats to specific serviceId and nnId |
| `HeartbeatsFor`*ServiceId*`-`*NNId*`AvgTime` | Average heartbeat time in milliseconds to specific serviceId and nnId |
| `HeartbeatsTotalNumOps` | Total number of heartbeats which is a duplicate of HeartbeatsNumOps |
| `HeartbeatsTotalAvgTime` | Average total heartbeat time in milliseconds |
| `HeartbeatsTotalFor`*ServiceId*`-`*NNId*`NumOps` | Total number of heartbeats to specific serviceId and nnId which is a duplicate of `HeartbeatsFor`*ServiceId*`-`*NNId*`NumOps` |
| `HeartbeatsTotalFor`*ServiceId*`-`*NNId*`AvgTime` | Average total heartbeat time in milliseconds to specific serviceId and nnId |
| `LifelinesNumOps` | Total number of lifeline messages |
| `LifelinesAvgTime` | Average lifeline message processing time in milliseconds |
| `LifelinesFor`*ServiceId*`-`*NNId*`NumOps` | Total number of lifeline messages to specific serviceId and nnId |
| `LifelinesFor`*ServiceId*`-`*NNId*`AvgTime` | Average lifeline message processing time to specific serviceId and nnId in milliseconds |
| `BlockReportsNumOps` | Total number of block report operations |
| `BlockReportsAvgTime` | Average time of block report operations in milliseconds |
| `BlockReports`*ServiceId*`-`*NNId*`NumOps` | Total number of block report operations to specific serviceId and nnId |
| `BlockReports`*ServiceId*`-`*NNId*`AvgTime` | Average time of block report operations to specific serviceId and nnId in milliseconds |
| `BlockReportsCreateCostMillsNumOps` | Total number of block report creating operations |
| `BlockReportsCreateCostMillsAvgTime` | Average time of block report creating operations in milliseconds |
| `IncrementalBlockReportsNumOps` | Total number of incremental block report operations |
| `IncrementalBlockReportsAvgTime` | Average time of incremental block report operations in milliseconds |
| `IncrementalBlockReports`*ServiceId*`-`*NNId*`NumOps` | Total number of incremental block report operations to specific serviceId and nnId |
| `IncrementalBlockReports`*ServiceId*`-`*NNId*`AvgTime` | Average time of incremental block report operations to specific serviceId and nnId in milliseconds |
| `CacheReportsNumOps` | Total number of cache report operations |
| `CacheReportsAvgTime` | Average time of cache report operations in milliseconds |
| `PacketAckRoundTripTimeNanosNumOps` | Total number of ack round trip |
| `PacketAckRoundTripTimeNanosAvgTime` | Average time from ack send to receive minus the downstream ack time in nanoseconds |
| `PacketAckRoundTripTimeNanos`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile latency from ack send to receive minus the downstream ack time in nanoseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `FlushNanosNumOps` | Total number of flushes |
| `FlushNanosAvgTime` | Average flush time in nanoseconds |
| `FlushNanos`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile flush time in nanoseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `FsyncNanosNumOps` | Total number of fsync |
| `FsyncNanosAvgTime` | Average fsync time in nanoseconds |
| `FsyncNanos`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile fsync time in nanoseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `SendDataPacketBlockedOnNetworkNanosNumOps` | Total number of sending packets |
| `SendDataPacketBlockedOnNetworkNanosAvgTime` | Average waiting time of sending packets in nanoseconds |
| `SendDataPacketBlockedOnNetworkNanos`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile waiting time of sending packets in nanoseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `SendDataPacketTransferNanosNumOps` | Total number of sending packets |
| `SendDataPacketTransferNanosAvgTime` | Average transfer time of sending packets in nanoseconds |
| `SendDataPacketTransferNanos`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile transfer time of sending packets in nanoseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `TotalWriteTime`| Total number of milliseconds spent on write operation |
| `TotalReadTime` | Total number of milliseconds spent on read operation |
| `RemoteBytesRead` | Number of bytes read by remote clients |
| `RemoteBytesWritten` | Number of bytes written by remote clients |
| `BPServiceActorInfo` | The information about a block pool service actor |
| `BlocksInPendingIBR` | Number of blocks in pending incremental block report (IBR) |
| `BlocksReceivingInPendingIBR` | Number of blocks at receiving status in pending incremental block report (IBR) |
| `BlocksReceivedInPendingIBR` | Number of blocks at received status in pending incremental block report (IBR) |
| `BlocksDeletedInPendingIBR` | Number of blocks at deleted status in pending incremental block report (IBR) |
| `EcReconstructionTasks` | Total number of erasure coding reconstruction tasks |
| `EcFailedReconstructionTasks` | Total number of erasure coding failed reconstruction tasks |
| `EcInvalidReconstructionTasks` | Total number of erasure coding invalidated reconstruction tasks |
| `EcDecodingTimeNanos` | Total number of nanoseconds spent by decoding tasks |
| `EcReconstructionBytesRead` | Total number of bytes read by erasure coding worker |
| `EcReconstructionBytesWritten` | Total number of bytes written by erasure coding worker |
| `EcReconstructionRemoteBytesRead` | Total number of bytes remote read by erasure coding worker |
| `CreateRbwOpNumOps` | Total number of create rbw operations |
| `CreateRbwOpAvgTime` | Average time of create rbw operations in milliseconds |
| `RecoverRbwOpNumOps` | Total number of recovery rbw operations |
| `RecoverRbwOpAvgTime` | Average time of recovery rbw operations in milliseconds |
| `ConvertTemporaryToRbwOpNumOps` | Total number of convert temporary to rbw operations |
| `ConvertTemporaryToRbwOpAvgTime` | Average time of convert temporary to rbw operations in milliseconds |
| `CreateTemporaryOpNumOps` | Total number of create temporary operations |
| `CreateTemporaryOpAvgTime` | Average time of create temporary operations in milliseconds |
| `FinalizeBlockOpNumOps` | Total number of finalize block operations |
| `FinalizeBlockOpAvgTime` | Average time of finalize block operations in milliseconds |
| `UnfinalizeBlockOpNumOps` | Total number of un-finalize block operations |
| `UnfinalizeBlockOpAvgTime` | Average time of un-finalize block operations in milliseconds |
| `CheckAndUpdateOpNumOps` | Total number of check and update operations |
| `CheckAndUpdateOpAvgTime` | Average time of check and update operations in milliseconds |
| `UpdateReplicaUnderRecoveryOpNumOps` | Total number of update replica under recovery operations |
| `UpdateReplicaUnderRecoveryOpAvgTime` | Average time of update replica under recovery operations in milliseconds |
| `PacketsReceived` | Total number of packets received by Datanode (excluding heartbeat packet from client) |
| `PacketsSlowWriteToMirror` | Total number of packets whose write to other Datanodes in the pipeline takes more than a certain time (300ms by default) |
| `PacketsSlowWriteToDisk` | Total number of packets whose write to disk takes more than a certain time (300ms by default) |
| `PacketsSlowWriteToOsCache` | Total number of packets whose write to os cache takes more than a certain time (300ms by default) |
| `SlowFlushOrSyncCount` | Total number of packets whose sync/flush takes more than a certain time (300ms by default) |
| `SlowAckToUpstreamCount` | Total number of packets whose upstream ack takes more than a certain time (300ms by default) |
| `SumOfActorCommandQueueLength` | Sum of all BPServiceActors command queue length |
| `NumProcessedCommands` | Num of processed commands of all BPServiceActors |
| `ProcessedCommandsOpNumOps` | Total number of processed commands operations |
| `ProcessedCommandsOpAvgTime` | Average time of processed commands operations in milliseconds |
| `NullStorageBlockReports` | Number of blocks in IBRs that failed due to null storage |

FsVolume
--------

Per-volume metrics contain Datanode Volume IO related statistics. Per-volume
metrics are off by default. They can be enabled by setting `dfs.datanode
.fileio.profiling.percentage.fraction` to an integer value between 1 and 100.
Setting this value to 0 would mean profiling is not enabled. But enabling
per-volume metrics may have a performance impact. Each metrics record
contains tags such as Hostname as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `TotalMetadataOperations` | Total number (monotonically increasing) of metadata operations. Metadata operations include stat, list, mkdir, delete, move, open and posix_fadvise. |
| `MetadataOperationRateNumOps` | The number of metadata operations within an interval time of metric |
| `MetadataOperationRateAvgTime` | Mean time of metadata operations in milliseconds |
| `MetadataOperationLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of metadata operations latency in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `TotalDataFileIos` | Total number (monotonically increasing) of data file io operations |
| `DataFileIoRateNumOps` | The number of data file io operations within an interval time of metric |
| `DataFileIoRateAvgTime` | Mean time of data file io operations in milliseconds |
| `DataFileIoLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of data file io operations latency in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `FlushIoRateNumOps` | The number of file flush io operations within an interval time of metric |
| `FlushIoRateAvgTime` | Mean time of file flush io operations in milliseconds |
| `FlushIoLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of file flush io operations latency in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `SyncIoRateNumOps` | The number of file sync io operations within an interval time of metric |
| `SyncIoRateAvgTime` | Mean time of file sync io operations in milliseconds |
| `SyncIoLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of file sync io operations latency in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `ReadIoRateNumOps` | The number of file read io operations within an interval time of metric |
| `ReadIoRateAvgTime` | Mean time of file read io operations in milliseconds |
| `ReadIoLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of file read io operations latency in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `WriteIoRateNumOps` | The number of file write io operations within an interval time of metric |
| `WriteIoRateAvgTime` | Mean time of file write io operations in milliseconds |
| `WriteIoLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of file write io operations latency in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `TransferIoRateNumOps` | The number of file transfer io operations within an interval time of metric |
| `TransferIoRateAvgTime` | Mean time of file transfer io operations in milliseconds |
| `TransferIoLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of file transfer io operations latency in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `NativeCopyIoRateNumOps` | The number of file nativeCopy io operations within an interval time of metric |
| `NativeCopyIoRateAvgTime` | Mean time of file nativeCopy io operations in milliseconds |
| `NativeCopyIoLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of file nativeCopy io operations latency in milliseconds (*num* seconds granularity). Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `TotalFileIoErrors` | Total number (monotonically increasing) of file io error operations |
| `FileIoErrorRateNumOps` | The number of file io error operations within an interval time of metric |
| `FileIoErrorRateAvgTime` | It measures the mean time in milliseconds from the start of an operation to hitting a failure |

RBFMetrics
----------------
RBFMetrics shows the metrics which are the aggregated values of sub-clusters' information in the Router-based federation.

| Name | Description |
|:---- |:---- |
| `NumFiles` | Current number of files and directories |
| `NumBlocks` | Current number of allocated blocks |
| `NumOfBlocksPendingReplication` | Current number of blocks pending to be replicated |
| `NumOfBlocksUnderReplicated` | Current number of blocks under replicated |
| `NumOfBlocksPendingDeletion` | Current number of blocks pending deletion |
| `ProvidedSpace` | The total remote storage capacity mounted in the federated cluster |
| `NumInMaintenanceLiveDataNodes` | Number of live Datanodes which are in maintenance state |
| `NumInMaintenanceDeadDataNodes` | Number of dead Datanodes which are in maintenance state |
| `NumEnteringMaintenanceDataNodes` | Number of Datanodes that are entering the maintenance state |
| `TotalCapacity` | Current raw capacity of DataNodes in bytes (long primitive, may overflow) |
| `UsedCapacity` | Current used capacity across all DataNodes in bytes (long primitive, may overflow) |
| `RemainingCapacity` | Current remaining capacity in bytes (long primitive, may overflow) |
| `TotalCapacityBigInt` | Current raw capacity of DataNodes in bytes (using BigInteger) |
| `UsedCapacityBigInt` | Current used capacity across all DataNodes in bytes (using BigInteger) |
| `RemainingCapacityBigInt` | Current remaining capacity in bytes (using BigInteger) |
| `NumOfMissingBlocks` | Current number of missing blocks |
| `NumLiveNodes` | Number of datanodes which are currently live |
| `NumDeadNodes` | Number of datanodes which are currently dead |
| `NumStaleNodes` | Current number of DataNodes marked stale due to delayed heartbeat |
| `NumDecomLiveNodes` | Number of datanodes which have been decommissioned and are now live |
| `NumDecomDeadNodes` | Number of datanodes which have been decommissioned and are now dead |
| `NumDecommissioningNodes` | Number of datanodes in decommissioning state |
| `Namenodes` | Current information about all the namenodes |
| `Nameservices` | Current information for each registered nameservice |
| `MountTable` | The mount table for the federated filesystem |
| `Routers` | Current information about all routers |
| `NumNameservices` | Number of nameservices |
| `NumNamenodes` | Number of namenodes |
| `NumExpiredNamenodes` | Number of expired namenodes |
| `NodeUsage` | Max, Median, Min and Standard Deviation of DataNodes usage |

RouterRPCMetrics
----------------
RouterRPCMetrics shows the statistics of the Router component in Router-based federation.

| Name | Description |
|:---- |:---- |
| `ProcessingOp` | Number of operations the Router processed internally |
| `ProxyOp` | Number of operations the Router proxied to a Namenode |
| `ProxyOpFailureStandby` | Number of operations to hit a standby NN |
| `ProxyOpFailureCommunicate` | Number of operations to fail to reach NN |
| `ProxyOpNotImplemented` | Number of operations not implemented |
| `RouterFailureStateStore` | Number of failed requests due to State Store unavailable |
| `RouterFailureReadOnly` | Number of failed requests due to read only mount point |
| `RouterFailureLocked` | Number of failed requests due to locked path |
| `RouterFailureSafemode` | Number of failed requests due to safe mode |
| `ProcessingNumOps` | Number of operations the Router processed internally within an interval time of metric |
| `ProcessingAvgTime` | Average time for the Router to process operations in milliseconds |
| `ProxyNumOps` | Number of times of that the Router to proxy operations to the Namenodes within an interval time of metric |
| `ProxyAvgTime` | Average time for the Router to proxy operations to the Namenodes in milliseconds |

StateStoreMetrics
-----------------
StateStoreMetrics shows the statistics of the State Store component in Router-based federation.

| Name                                      | Description                                                                        |
|:------------------------------------------|:-----------------------------------------------------------------------------------|
| `ReadsNumOps`                             | Number of GET transactions for State Store within an interval time of metric       |
| `ReadsAvgTime`                            | Average time of GET transactions for State Store in milliseconds                   |
| `WritesNumOps`                            | Number of PUT transactions for State Store within an interval time of metric       |
| `WritesAvgTime`                           | Average time of PUT transactions for State Store in milliseconds                   |
| `RemovesNumOps`                           | Number of REMOVE transactions for State Store within an interval time of metric    |
| `RemovesAvgTime`                          | Average time of REMOVE transactions for State Store in milliseconds                |
| `FailuresNumOps`                          | Number of failed transactions for State Store within an interval time of metric    |
| `FailuresAvgTime`                         | Average time of failed transactions for State Store in milliseconds                |
| `Cache`*BaseRecord*`Size`                 | Number of store records to cache in State Store                                    |
| `Cache`*BaseRecord*`LoadNumOps`           | Number of times store records are loaded in the State Store Cache from State Store |
| `Cache`*BaseRecord*`LoadAvgTime`          | Average time of loading State Store Cache from State Store in milliseconds         |

yarn context
============

ClusterMetrics
--------------

ClusterMetrics shows the metrics of the YARN cluster from the ResourceManager's perspective. Each metrics record contains Hostname tag as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `NumActiveNMs` | Current number of active NodeManagers |
| `numDecommissioningNMs` | Current number of NodeManagers being decommissioned|
| `NumDecommissionedNMs` | Current number of decommissioned NodeManagers |
| `NumShutdownNMs` | Current number of NodeManagers shut down gracefully. Note that this does not count NodeManagers that are forcefully killed. |
| `NumLostNMs` | Current number of lost NodeManagers for not sending heartbeats. |
| `NumUnhealthyNMs` | Current number of unhealthy NodeManagers |
| `NumRebootedNMs` | Current number of rebooted NodeManagers |
| `AMLaunchDelayNumOps` | Total number of AMs launched |
| `AMLaunchDelayAvgTime` | Average time in milliseconds RM spends to launch AM containers after the AM container is allocated|
| `AMRegisterDelayNumOps` | Total number of AMs registered  |
| `AMRegisterDelayAvgTime` | Average time in milliseconds AM spends to register with RM after the AM container gets launched |

QueueMetrics
------------

QueueMetrics shows an application queue from the ResourceManager's perspective. Each metrics record shows the statistics of each queue, and contains tags such as queue name and Hostname as additional information along with metrics.

In `running_`*num* metrics such as `running_0`, you can set the property `yarn.resourcemanager.metrics.runtime.buckets` in yarn-site.xml to change the buckets. The default values is `60,300,1440`.

| Name | Description |
|:---- |:---- |
| `running_0` | Current number of running applications whose elapsed time are less than 60 minutes |
| `running_60` | Current number of running applications whose elapsed time are between 60 and 300 minutes |
| `running_300` | Current number of running applications whose elapsed time are between 300 and 1440 minutes |
| `running_1440` | Current number of running applications elapsed time are more than 1440 minutes |
| `AppsSubmitted` | Total number of submitted applications |
| `AppsRunning` | Current number of running applications |
| `AppsPending` | Current number of applications that have not yet been assigned by any containers |
| `AppsCompleted` | Total number of completed applications |
| `AppsKilled` | Total number of killed applications |
| `AppsFailed` | Total number of failed applications |
| `AllocatedMB` | Current allocated memory in MB |
| `AllocatedVCores` | Current allocated CPU in virtual cores |
| `AllocatedContainers` | Current number of allocated containers |
| `AggregateContainersAllocated` | Total number of allocated containers |
| `aggregateNodeLocalContainersAllocated` | Total number of node local containers allocated  |
| `aggregateRackLocalContainersAllocated` | Total number of rack local containers allocated  |
| `aggregateOffSwitchContainersAllocated` | Total number of off switch containers allocated |
| `AggregateContainersReleased` | Total number of released containers |
| `AvailableMB` | Current available memory in MB |
| `AvailableVCores` | Current available CPU in virtual cores |
| `PendingMB` | Current memory requests in MB that are pending to be fulfilled by the scheduler |
| `PendingVCores` | Current CPU requests in virtual cores that are pending to be fulfilled by the scheduler |
| `PendingContainers` | Current number of containers that are pending to be fulfilled by the scheduler |
| `ReservedMB` | Current reserved memory in MB |
| `ReservedVCores` | Current reserved CPU in virtual cores |
| `ReservedContainers` | Current number of reserved containers |
| `ActiveUsers` | Current number of active users |
| `ActiveApplications` | Current number of active applications |
| `AppAttemptFirstContainerAllocationDelayNumOps` | Total number of first container allocated for all attempts |
| `AppAttemptFirstContainerAllocationDelayAvgTime` | Average time RM spends to allocate the first container for all attempts. For managed AM, the first container is AM container. So, this indicates the time duration to allocate AM container. For unmanaged AM, this is the time duration to allocate the first container asked by unmanaged AM. |
| `FairShareMB` | (FairScheduler only) Current fair share of memory in MB |
| `FairShareVCores` | (FairScheduler only) Current fair share of CPU in virtual cores |
| `MinShareMB` | (FairScheduler only) Minimum share of memory in MB |
| `MinShareVCores` | (FairScheduler only) Minimum share of CPU in virtual cores |
| `MaxShareMB` | (FairScheduler only) Maximum share of memory in MB |
| `MaxShareVCores` | (FairScheduler only) Maximum share of CPU in virtual cores |

NodeManagerMetrics
------------------

NodeManagerMetrics shows the statistics of the containers in the node. Each metrics record contains Hostname tag as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `containersLaunched` | Total number of launched containers |
| `containersCompleted` | Total number of successfully completed containers |
| `containersFailed` | Total number of failed containers |
| `containersKilled` | Total number of killed containers |
| `containersIniting` | Current number of initializing containers |
| `containersRunning` | Current number of running containers |
| `allocatedContainers` | Current number of allocated containers |
| `allocatedGB` | Current allocated memory in GB |
| `availableGB` | Current available memory in GB |
| `allocatedVcores` | Current used vcores|
| `availableVcores` | Current available vcores |
| `containerLaunchDuration` | Average time duration in milliseconds NM takes to launch a container|
| `badLocalDirs` | Current number of bad local directories. Currently, a disk that cannot be read/written/executed by NM process or A disk being full is considered as bad.|
| `badLogDirs` | Current number of bad log directories. Currently, a disk that cannot be read/written/executed by NM process or A disk being full is considered as bad. |
| `goodLocalDirsDiskUtilizationPerc` | Current disk utilization percentage across all good local directories |
| `goodLogDirsDiskUtilizationPerc` | Current disk utilization percentage across all good log directories |

ContainerMetrics
------------------

ContainerMetrics shows the resource utilization statistics of a container. Each metrics record contains tags such as ContainerPid and Hostname as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `pMemLimitMBs` | Physical memory limit of the container in MB |
| `vMemLimitMBs` | Virtual memory limit of the container in MB |
| `vCoreLimit` | CPU limit of the container in number of vcores |
| `launchDurationMs` | Container launch duration in msec  |
| `localizationDurationMs` | Container localization duration in msec |
| `StartTime` | Time in msec when container starts |
| `FinishTime` | Time in msec when container finishes |
| `ExitCode` | Container's exit code |
| `PMemUsageMBsNumUsage` | Total number of physical memory used metrics |
| `PMemUsageMBsAvgMBs` | Average physical memory used in MB |
| `PMemUsageMBsStdevMBs` | Standard deviation of the physical memory used in MB |
| `PMemUsageMBsMinMBs` | Minimum physical memory used in MB |
| `PMemUsageMBsMaxMBs` | Maximum physical memory used in MB |
| `PMemUsageMBsIMinMBs` | Minimum physical memory used in MB of current _interval_ (the time of _interval_ is specified by yarn.nodemanager.container-metrics.period-ms) |
| `PMemUsageMBsIMaxMBs` | Maximum physical memory used in MB of current _interval_ (the time of _interval_ is specified by yarn.nodemanager.container-metrics.period-ms) |
| `PMemUsageMBsINumUsage` | Total number of physical memory used metrics in current _interval_ (the time of _interval_ is specified by yarn.nodemanager.container-metrics.period-ms) |
| `PCpuUsagePercentNumUsage` | Total number of physical CPU cores percent used metrics |
| `PCpuUsagePercentAvgPercents` | Average physical CPU cores percent used |
| `PCpuUsagePercentStdevPercents` | Standard deviation of physical CPU cores percent used |
| `PCpuUsagePercentMinPercents` | Minimum physical CPU cores percent used|
| `PCpuUsagePercentMaxPercents` | Maximum physical CPU cores percent used |
| `PCpuUsagePercentIMinPercents` | Minimum physical CPU cores percent used in current _interval_ (the time of _interval_ is specified by yarn.nodemanager.container-metrics.period-ms) |
| `PCpuUsagePercentIMaxPercents` | Maximum physical CPU cores percent used in current _interval_ (the time of _interval_ is specified by yarn.nodemanager.container-metrics.period-ms) |
| `PCpuUsagePercentINumUsage` | Total number of physical CPU cores used metrics in current _interval_ (the time of _interval_ is specified by yarn.nodemanager.container-metrics.period-ms) |
| `MilliVcoreUsageNumUsage` | Total number of vcores used metrics |
| `MilliVcoreUsageAvgMilliVcores` | 1000 times the average vcores used |
| `MilliVcoreUsageStdevMilliVcores` | 1000 times the standard deviation of vcores used |
| `MilliVcoreUsageMinMilliVcores` | 1000 times the minimum vcores used |
| `MilliVcoreUsageMaxMilliVcores` | 1000 times the maximum vcores used |
| `MilliVcoreUsageIMinMilliVcores` | 1000 times the average vcores used in current _interval_ (the time of _interval_ is specified by yarn.nodemanager.container-metrics.period-ms) |
| `MilliVcoreUsageIMaxMilliVcores` | 1000 times the maximum vcores used in current _interval_ (the time of _interval_ is specified by yarn.nodemanager.container-metrics.period-ms) |
| `MilliVcoreUsageINumUsage` | Total number of vcores used metrics in current _interval_ (the time of _interval_ is specified by yarn.nodemanager.container-metrics.period-ms) |
| `PMemUsageMBHistogramNumUsage` | Total number of physical memory used metrics (1 second granularity) |
| `PMemUsageMBHistogram50thPercentileMBs` | The 50th percentile of physical memory used in MB (1 second granularity) |
| `PMemUsageMBHistogram75thPercentileMBs` | The 75th percentile of physical memory used in MB (1 second granularity) |
| `PMemUsageMBHistogram90thPercentileMBs` | The 90th percentile of physical memory used in MB (1 second granularity) |
| `PMemUsageMBHistogram95thPercentileMBs` | The 95th percentile of physical memory used in MB (1 second granularity) |
| `PMemUsageMBHistogram99thPercentileMBs` | The 99th percentile of physical memory used in MB (1 second granularity) |
| `PCpuUsagePercentHistogramNumUsage` | Total number of physical CPU cores used metrics (1 second granularity) |
| `PCpuUsagePercentHistogram50thPercentilePercents` | The 50th percentile of physical CPU cores percent used (1 second granularity) |
| `PCpuUsagePercentHistogram75thPercentilePercents` | The 75th percentile of physical CPU cores percent used (1 second granularity) |
| `PCpuUsagePercentHistogram90thPercentilePercents` | The 90th percentile of physical CPU cores percent used (1 second granularity) |
| `PCpuUsagePercentHistogram95thPercentilePercents` | The 95th percentile of physical CPU cores percent used (1 second granularity) |
| `PCpuUsagePercentHistogram99thPercentilePercents` | The 99th percentile of physical CPU cores percent used (1 second granularity) |

ugi context
===========

UgiMetrics
----------

UgiMetrics is related to user and group information. Each metrics record contains Hostname tag as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `LoginSuccessNumOps` | Total number of successful kerberos logins |
| `LoginSuccessAvgTime` | Average time for successful kerberos logins in milliseconds |
| `LoginFailureNumOps` | Total number of failed kerberos logins |
| `LoginFailureAvgTime` | Average time for failed kerberos logins in milliseconds |
| `getGroupsNumOps` | Total number of group resolutions |
| `getGroupsAvgTime` | Average time for group resolution in milliseconds |
| `getGroups`*num*`sNumOps` | Total number of group resolutions (*num* seconds granularity). *num* is specified by `hadoop.user.group.metrics.percentiles.intervals`. |
| `getGroups`*num*`s50thPercentileLatency` | Shows the 50th percentile of group resolution time in milliseconds (*num* seconds granularity). *num* is specified by `hadoop.user.group.metrics.percentiles.intervals`. |
| `getGroups`*num*`s75thPercentileLatency` | Shows the 75th percentile of group resolution time in milliseconds (*num* seconds granularity). *num* is specified by `hadoop.user.group.metrics.percentiles.intervals`. |
| `getGroups`*num*`s90thPercentileLatency` | Shows the 90th percentile of group resolution time in milliseconds (*num* seconds granularity). *num* is specified by `hadoop.user.group.metrics.percentiles.intervals`. |
| `getGroups`*num*`s95thPercentileLatency` | Shows the 95th percentile of group resolution time in milliseconds (*num* seconds granularity). *num* is specified by `hadoop.user.group.metrics.percentiles.intervals`. |
| `getGroups`*num*`s99thPercentileLatency` | Shows the 99th percentile of group resolution time in milliseconds (*num* seconds granularity). *num* is specified by `hadoop.user.group.metrics.percentiles.intervals`. |

metricssystem context
=====================

MetricsSystem
-------------

MetricsSystem shows the statistics for metrics snapshots and publishes. Each metrics record contains Hostname tag as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `NumActiveSources` | Current number of active metrics sources |
| `NumAllSources` | Total number of metrics sources |
| `NumActiveSinks` | Current number of active sinks |
| `NumAllSinks` | Total number of sinks  (BUT usually less than `NumActiveSinks`, see [HADOOP-9946](https://issues.apache.org/jira/browse/HADOOP-9946)) |
| `SnapshotNumOps` | Total number of operations to snapshot statistics from a metrics source |
| `SnapshotAvgTime` | Average time in milliseconds to snapshot statistics from a metrics source |
| `PublishNumOps` | Total number of operations to publish statistics to a sink |
| `PublishAvgTime` | Average time in milliseconds to publish statistics to a sink |
| `DroppedPubAll` | Total number of dropped publishes |
| `Sink_`*instance*`NumOps` | Total number of sink operations for the *instance* |
| `Sink_`*instance*`AvgTime` | Average time in milliseconds of sink operations for the *instance* |
| `Sink_`*instance*`Dropped` | Total number of dropped sink operations for the *instance* |
| `Sink_`*instance*`Qsize` | Current queue length of sink operations |

default context
===============

StartupProgress
---------------

StartupProgress metrics shows the statistics of NameNode startup. Four metrics are exposed for each startup phase based on its name. The startup *phase*s are `LoadingFsImage`, `LoadingEdits`, `SavingCheckpoint`, and `SafeMode`. Each metrics record contains Hostname tag as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `ElapsedTime` | Total elapsed time in milliseconds |
| `PercentComplete` | Current rate completed in NameNode startup progress  (The max value is not 100 but 1.0) |
| *phase*`Count` | Total number of steps completed in the phase |
| *phase*`ElapsedTime` | Total elapsed time in the phase in milliseconds |
| *phase*`Total` | Total number of steps in the phase |
| *phase*`PercentComplete` | Current rate completed in the phase  (The max value is not 100 but 1.0) |
