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

* [Overview](#Overview)
* [jvm context](#jvm_context)
    * [JvmMetrics](#JvmMetrics)
* [rpc context](#rpc_context)
    * [rpc](#rpc)
    * [RetryCache/NameNodeRetryCache](#RetryCacheNameNodeRetryCache)
* [rpcdetailed context](#rpcdetailed_context)
    * [rpcdetailed](#rpcdetailed)
* [dfs context](#dfs_context)
    * [namenode](#namenode)
    * [FSNamesystem](#FSNamesystem)
    * [JournalNode](#JournalNode)
    * [datanode](#datanode)
* [yarn context](#yarn_context)
    * [ClusterMetrics](#ClusterMetrics)
    * [QueueMetrics](#QueueMetrics)
    * [NodeManagerMetrics](#NodeManagerMetrics)
* [ugi context](#ugi_context)
    * [UgiMetrics](#UgiMetrics)
* [metricssystem context](#metricssystem_context)
    * [MetricsSystem](#MetricsSystem)
* [default context](#default_context)
    * [StartupProgress](#StartupProgress)

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

rpc context
===========

rpc
---

Each metrics record contains tags such as Hostname and port (number to which server is bound) as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `ReceivedBytes` | Total number of received bytes |
| `SentBytes` | Total number of sent bytes |
| `RpcQueueTimeNumOps` | Total number of RPC calls |
| `RpcQueueTimeAvgTime` | Average queue time in milliseconds |
| `RpcProcessingTimeNumOps` | Total number of RPC calls (same to RpcQueueTimeNumOps) |
| `RpcProcessingAvgTime` | Average Processing time in milliseconds |
| `RpcAuthenticationFailures` | Total number of authentication failures |
| `RpcAuthenticationSuccesses` | Total number of authentication successes |
| `RpcAuthorizationFailures` | Total number of authorization failures |
| `RpcAuthorizationSuccesses` | Total number of authorization successes |
| `NumOpenConnections` | Current number of open connections |
| `CallQueueLength` | Current length of the call queue |
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

RetryCache/NameNodeRetryCache
-----------------------------

RetryCache metrics is useful to monitor NameNode fail-over. Each metrics record contains Hostname tag.

| Name | Description |
|:---- |:---- |
| `CacheHit` | Total number of RetryCache hit |
| `CacheCleared` | Total number of RetryCache cleared |
| `CacheUpdated` | Total number of RetryCache updated |

rpcdetailed context
===================

Metrics of rpcdetailed context are exposed in unified manner by RPC layer. Two metrics are exposed for each RPC based on its name. Metrics named "(RPC method name)NumOps" indicates total number of method calls, and metrics named "(RPC method name)AvgTime" shows average turn around time for method calls in milliseconds.

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
| `TransactionsBatchedInSync` | Total number of Journal transactions batched in sync |
| `BlockReportNumOps` | Total number of processing block reports from DataNode |
| `BlockReportAvgTime` | Average time of processing block reports in milliseconds |
| `CacheReportNumOps` | Total number of processing cache reports from DataNode |
| `CacheReportAvgTime` | Average time of processing cache reports in milliseconds |
| `SafeModeTime` | The interval between FSNameSystem starts and the last time safemode leaves in milliseconds.  (sometimes not equal to the time in SafeMode, see [HDFS-5156](https://issues.apache.org/jira/browse/HDFS-5156)) |
| `FsImageLoadTime` | Time loading FS Image at startup in milliseconds |
| `FsImageLoadTime` | Time loading FS Image at startup in milliseconds |
| `GetEditNumOps` | Total number of edits downloads from SecondaryNameNode |
| `GetEditAvgTime` | Average edits download time in milliseconds |
| `GetImageNumOps` | Total number of fsimage downloads from SecondaryNameNode |
| `GetImageAvgTime` | Average fsimage download time in milliseconds |
| `PutImageNumOps` | Total number of fsimage uploads to SecondaryNameNode |
| `PutImageAvgTime` | Average fsimage upload time in milliseconds |
| `TotalFileOps`| Total number of file operations performed |
| `NNStartedTimeInMillis`| NameNode start time in milliseconds |

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
| `StaleDataNodes` | Current number of DataNodes marked stale due to delayed heartbeat |
| `TotalFiles` | Current number of files and directories (same as FilesTotal) |
| `MissingReplOneBlocks` | Current number of missing blocks with replication factor 1 |
| `NumFilesUnderConstruction` | Current number of files under construction |
| `NumActiveClients` | Current number of active clients holding lease |
| `HAState` | (HA-only) Current state of the NameNode: initializing or active or standby or stopping state |
| `FSState` | Current state of the file system: Safemode or Operational |
| `LockQueueLength` | Number of threads waiting to acquire FSNameSystem lock |
| `TotalSyncCount` | Total number of sync operations performed by edit log |
| `TotalSyncTimes` | Total number of milliseconds spent by various edit logs in sync operation|
| `NameDirSize` | NameNode name directories size in bytes |

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
| `BatchesWritten` | Total number of batches written since startup |
| `TxnsWritten` | Total number of transactions written since startup |
| `BytesWritten` | Total number of bytes written since startup |
| `BatchesWrittenWhileLagging` | Total number of batches written where this node was lagging |
| `LastWriterEpoch` | Current writer's epoch number |
| `CurrentLagTxns` | The number of transactions that this JournalNode is lagging |
| `LastWrittenTxId` | The highest transaction id stored on this JournalNode |
| `LastPromisedEpoch` | The last epoch number which this node has promised not to accept any lower epoch, or 0 if no promises have been made |
| `LastJournalTimestamp` | The timestamp of last successfully written transaction |

datanode
--------

Each metrics record contains tags such as SessionId and Hostname as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `BytesWritten` | Total number of bytes written to DataNode |
| `BytesRead` | Total number of bytes read from DataNode |
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
| `FsyncCount` | Total number of fsync |
| `VolumeFailures` | Total number of volume failures occurred |
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
| `BlockReportsNumOps` | Total number of block report operations |
| `BlockReportsAvgTime` | Average time of block report operations in milliseconds |
| `IncrementalBlockReportsNumOps` | Total number of incremental block report operations |
| `IncrementalBlockReportsAvgTime` | Average time of incremental block report operations in milliseconds |
| `CacheReportsNumOps` | Total number of cache report operations |
| `CacheReportsAvgTime` | Average time of cache report operations in milliseconds |
| `PacketAckRoundTripTimeNanosNumOps` | Total number of ack round trip |
| `PacketAckRoundTripTimeNanosAvgTime` | Average time from ack send to receive minus the downstream ack time in nanoseconds |
| `FlushNanosNumOps` | Total number of flushes |
| `FlushNanosAvgTime` | Average flush time in nanoseconds |
| `FsyncNanosNumOps` | Total number of fsync |
| `FsyncNanosAvgTime` | Average fsync time in nanoseconds |
| `SendDataPacketBlockedOnNetworkNanosNumOps` | Total number of sending packets |
| `SendDataPacketBlockedOnNetworkNanosAvgTime` | Average waiting time of sending packets in nanoseconds |
| `SendDataPacketTransferNanosNumOps` | Total number of sending packets |
| `SendDataPacketTransferNanosAvgTime` | Average transfer time of sending packets in nanoseconds |
| `TotalWriteTime`| Total number of milliseconds spent on write operation |
| `TotalReadTime` | Total number of milliseconds spent on read operation |
| `RemoteBytesRead` | Number of bytes read by remote clients |
| `RemoteBytesWritten` | Number of bytes written by remote clients |

yarn context
============

ClusterMetrics
--------------

ClusterMetrics shows the metrics of the YARN cluster from the ResourceManager's perspective. Each metrics record contains Hostname tag as additional information along with metrics.

| Name | Description |
|:---- |:---- |
| `NumActiveNMs` | Current number of active NodeManagers |
| `NumDecommissionedNMs` | Current number of decommissioned NodeManagers |
| `NumLostNMs` | Current number of lost NodeManagers for not sending heartbeats |
| `NumUnhealthyNMs` | Current number of unhealthy NodeManagers |
| `NumRebootedNMs` | Current number of rebooted NodeManagers |

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
| `AggregateContainersReleased` | Total number of released containers |
| `AvailableMB` | Current available memory in MB |
| `AvailableVCores` | Current available CPU in virtual cores |
| `PendingMB` | Current pending memory resource requests in MB that are not yet fulfilled by the scheduler |
| `PendingVCores` | Current pending CPU allocation requests in virtual cores that are not yet fulfilled by the scheduler |
| `PendingContainers` | Current pending resource requests that are not yet fulfilled by the scheduler |
| `ReservedMB` | Current reserved memory in MB |
| `ReservedVCores` | Current reserved CPU in virtual cores |
| `ReservedContainers` | Current number of reserved containers |
| `ActiveUsers` | Current number of active users |
| `ActiveApplications` | Current number of active applications |
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


