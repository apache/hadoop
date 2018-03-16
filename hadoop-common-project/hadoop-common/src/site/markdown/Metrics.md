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
| `TransactionsBatchedInSync` | Total number of Journal transactions batched in sync |
| `StorageBlockReportNumOps` | Total number of processing block reports from individual storages in DataNode |
| `StorageBlockReportAvgTime` | Average time of processing block reports in milliseconds |
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
| `GenerateEDEKTimeNumOps` | Total number of generating EDEK |
| `GenerateEDEKTimeAvgTime` | Average time of generating EDEK in milliseconds |
| `WarmUpEDEKTimeNumOps` | Total number of warming up EDEK |
| `WarmUpEDEKTimeAvgTime` | Average time of warming up EDEK in milliseconds |
| `ResourceCheckTime`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of NameNode resource check latency in milliseconds. Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `StorageBlockReport`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of storage block report latency in milliseconds. Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |

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
| `NumFilesUnderConstruction` | Current number of files under construction |
| `NumActiveClients` | Current number of active clients holding lease |
| `HAState` | (HA-only) Current state of the NameNode: initializing or active or standby or stopping state |
| `FSState` | Current state of the file system: Safemode or Operational |
| `LockQueueLength` | Number of threads waiting to acquire FSNameSystem lock |
| `TotalSyncCount` | Total number of sync operations performed by edit log |
| `TotalSyncTimes` | Total number of milliseconds spent by various edit logs in sync operation|
| `NameDirSize` | NameNode name directories size in bytes |
| `NumTimedOutPendingReconstructions` | The number of timed out reconstructions. Not the number of unique blocks that timed out. |
| `NumInMaintenanceLiveDataNodes` | Number of live Datanodes which are in maintenance state |
| `NumInMaintenanceDeadDataNodes` | Number of dead Datanodes which are in maintenance state |
| `NumEnteringMaintenanceDataNodes` | Number of Datanodes that are entering the maintenance state |
| `FSN(Read/Write)Lock`*OperationName*`NumOps` | Total number of acquiring lock by operations |
| `FSN(Read/Write)Lock`*OperationName*`AvgTime` | Average time of holding the lock by operations in milliseconds |

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
| `RamDiskBlocksWrite` | Total number of blocks written to memory |
| `RamDiskBlocksWriteFallback` | Total number of blocks written to memory but not satisfied (failed-over to disk) |
| `RamDiskBytesWrite` | Total number of bytes written to memory |
| `RamDiskBlocksReadHits` | Total number of times a block in memory was read |
| `RamDiskBlocksEvicted` | Total number of blocks evicted in memory |
| `RamDiskBlocksEvictedWithoutRead` | Total number of blocks evicted in memory without ever being read from memory |
| `RamDiskBlocksEvictionWindowMsNumOps` | Number of blocks evicted in memory|
| `RamDiskBlocksEvictionWindowMsAvgTime` | Average time of blocks in memory before being evicted in milliseconds |
| `RamDiskBlocksEvictionWindows`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of latency between memory write and eviction in milliseconds. Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `RamDiskBlocksLazyPersisted` | Total number of blocks written to disk by lazy writer |
| `RamDiskBlocksDeletedBeforeLazyPersisted` | Total number of blocks deleted by application before being persisted to disk |
| `RamDiskBytesLazyPersisted` | Total number of bytes written to disk by lazy writer |
| `RamDiskBlocksLazyPersistWindowMsNumOps` | Number of blocks written to disk by lazy writer |
| `RamDiskBlocksLazyPersistWindowMsAvgTime` | Average time of blocks written to disk by lazy writer in milliseconds |
| `RamDiskBlocksLazyPersistWindows`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of latency between memory write and disk persist in milliseconds. Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
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
| `HeartbeatsTotalNumOps` | Total number of heartbeats which is a duplicate of HeartbeatsNumOps |
| `HeartbeatsTotalAvgTime` | Average total heartbeat time in milliseconds |
| `LifelinesNumOps` | Total number of lifeline messages |
| `LifelinesAvgTime` | Average lifeline message processing time in milliseconds |
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
| `BPServiceActorInfo` | The information about a block pool service actor |
| `BlocksInPendingIBR` | Number of blocks in pending incremental block report (IBR) |
| `BlocksReceivingInPendingIBR` | Number of blocks at receiving status in pending incremental block report (IBR) |
| `BlocksReceivedInPendingIBR` | Number of blocks at received status in pending incremental block report (IBR) |
| `BlocksDeletedInPendingIBR` | Number of blocks at deleted status in pending incremental block report (IBR) |
| `EcReconstructionTasks` | Total number of erasure coding reconstruction tasks |
| `EcFailedReconstructionTasks` | Total number of erasure coding failed reconstruction tasks |
| `EcDecodingTimeNanos` | Total number of nanoseconds spent by decoding tasks |
| `EcReconstructionBytesRead` | Total number of bytes read by erasure coding worker |
| `EcReconstructionBytesWritten` | Total number of bytes written by erasure coding worker |
| `EcReconstructionRemoteBytesRead` | Total number of bytes remote read by erasure coding worker |

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
| `MetadataOperationLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of metadata operations latency in milliseconds. Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `TotalDataFileIos` | Total number (monotonically increasing) of data file io operations |
| `DataFileIoRateNumOps` | The number of data file io operations within an interval time of metric |
| `DataFileIoRateAvgTime` | Mean time of data file io operations in milliseconds |
| `DataFileIoLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of data file io operations latency in milliseconds. Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `FlushIoRateNumOps` | The number of file flush io operations within an interval time of metric |
| `FlushIoRateAvgTime` | Mean time of file flush io operations in milliseconds |
| `FlushIoLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of file flush io operations latency in milliseconds. Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `SyncIoRateNumOps` | The number of file sync io operations within an interval time of metric |
| `SyncIoRateAvgTime` | Mean time of file sync io operations in milliseconds |
| `SyncIoLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of file sync io operations latency in milliseconds. Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `ReadIoRateNumOps` | The number of file read io operations within an interval time of metric |
| `ReadIoRateAvgTime` | Mean time of file read io operations in milliseconds |
| `ReadIoLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of file read io operations latency in milliseconds. Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `WriteIoRateNumOps` | The number of file write io operations within an interval time of metric |
| `WriteIoRateAvgTime` | Mean time of file write io operations in milliseconds |
| `WriteIoLatency`*num*`s(50/75/90/95/99)thPercentileLatency` | The 50/75/90/95/99th percentile of file write io operations latency in milliseconds. Percentile measurement is off by default, by watching no intervals. The intervals are specified by `dfs.metrics.percentiles.intervals`. |
| `TotalFileIoErrors` | Total number (monotonically increasing) of file io error operations |
| `FileIoErrorRateNumOps` | The number of file io error operations within an interval time of metric |
| `FileIoErrorRateAvgTime` | It measures the mean time in milliseconds from the start of an operation to hitting a failure |

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
