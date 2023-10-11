/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.protocol.BlockType.CONTIGUOUS;
import static org.apache.hadoop.hdfs.protocol.BlockType.STRIPED;
import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;

import java.util.concurrent.atomic.AtomicLong;
import javax.management.ObjectName;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped.StorageAndBlockIndex;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplicasMap.Reason;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo.AddBlockResult;
import org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingDataNodeMessages.ReportedBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingReconstructionBlocks.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfyManager;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.StripedBlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.namenode.CacheManager;

import static org.apache.hadoop.hdfs.util.StripedBlockUtil.getInternalBlockLength;

import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps information related to the blocks stored in the Hadoop cluster.
 * For block state management, it tries to maintain the  safety
 * property of "# of live replicas == # of expected redundancy" under
 * any events such as decommission, namenode failover, datanode failure.
 *
 * The motivation of maintenance mode is to allow admins quickly repair nodes
 * without paying the cost of decommission. Thus with maintenance mode,
 * # of live replicas doesn't have to be equal to # of expected redundancy.
 * If any of the replica is in maintenance mode, the safety property
 * is extended as follows. These property still apply for the case of zero
 * maintenance replicas, thus we can use these safe property for all scenarios.
 * a. # of live replicas &gt;= # of min replication for maintenance.
 * b. # of live replicas &lt;= # of expected redundancy.
 * c. # of live replicas and maintenance replicas &gt;= # of expected
 * redundancy.
 *
 * For regular replication, # of min live replicas for maintenance is determined
 * by DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY. This number has to &lt;=
 * DFS_NAMENODE_REPLICATION_MIN_KEY.
 * For erasure encoding, # of min live replicas for maintenance is
 * BlockInfoStriped#getRealDataBlockNum.
 *
 * Another safety property is to satisfy the block placement policy. While the
 * policy is configurable, the replicas the policy is applied to are the live
 * replicas + maintenance replicas.
 */
@InterfaceAudience.Private
public class BlockManager implements BlockStatsMXBean {

  public static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);
  public static final Logger blockLog = NameNode.blockStateChangeLog;

  private static final String QUEUE_REASON_CORRUPT_STATE =
    "it has the wrong state or generation stamp";

  private static final String QUEUE_REASON_FUTURE_GENSTAMP =
    "generation stamp is in the future";

  private static final long BLOCK_RECOVERY_TIMEOUT_MULTIPLIER = 30;

  private final Namesystem namesystem;

  private final BlockManagerSafeMode bmSafeMode;

  private final DatanodeManager datanodeManager;
  private final HeartbeatManager heartbeatManager;
  private final BlockTokenSecretManager blockTokenSecretManager;

  // Block pool ID used by this namenode
  private String blockPoolId;

  private final PendingDataNodeMessages pendingDNMessages =
    new PendingDataNodeMessages();

  private volatile long pendingReconstructionBlocksCount = 0L;
  private volatile long corruptReplicaBlocksCount = 0L;
  private volatile long lowRedundancyBlocksCount = 0L;
  private volatile long scheduledReplicationBlocksCount = 0L;

  private final long deleteBlockLockTimeMs;
  private final long deleteBlockUnlockIntervalTimeMs;

  /** flag indicating whether replication queues have been initialized */
  private boolean initializedReplQueues;

  private final long startupDelayBlockDeletionInMs;
  private final BlockReportLeaseManager blockReportLeaseManager;
  private ObjectName mxBeanName;

  /** Used by metrics */
  public long getPendingReconstructionBlocksCount() {
    return pendingReconstructionBlocksCount;
  }
  /** Used by metrics */
  public long getLowRedundancyBlocksCount() {
    return lowRedundancyBlocksCount;
  }
  /** Used by metrics */
  public long getCorruptReplicaBlocksCount() {
    return corruptReplicaBlocksCount;
  }
  /** Used by metrics */
  public long getScheduledReplicationBlocksCount() {
    return scheduledReplicationBlocksCount;
  }
  /** Used by metrics */
  public long getPendingDeletionBlocksCount() {
    return invalidateBlocks.numBlocks();
  }
  /** Used by metrics */
  public long getStartupDelayBlockDeletionInMs() {
    return startupDelayBlockDeletionInMs;
  }
  /** Used by metrics */
  public long getExcessBlocksCount() {
    return excessRedundancyMap.size();
  }
  /** Used by metrics */
  public long getPostponedMisreplicatedBlocksCount() {
    return postponedMisreplicatedBlocks.size();
  }
  /** Used by metrics */
  public int getPendingDataNodeMessageCount() {
    return pendingDNMessages.count();
  }
  /** Used by metrics. */
  public long getNumTimedOutPendingReconstructions() {
    return pendingReconstruction.getNumTimedOuts();
  }

  /** Used by metrics. */
  public long getLowRedundancyBlocks() {
    return neededReconstruction.getLowRedundancyBlocks();
  }

  /** Used by metrics. */
  public long getCorruptBlocks() {
    return corruptReplicas.getCorruptBlocks();
  }

  /** Used by metrics. */
  public long getMissingBlocks() {
    return neededReconstruction.getCorruptBlocks();
  }

  /** Used by metrics. */
  public long getMissingReplicationOneBlocks() {
    return neededReconstruction.getCorruptReplicationOneBlocks();
  }

  /** Used by metrics. */
  public long getPendingDeletionReplicatedBlocks() {
    return invalidateBlocks.getBlocks();
  }

  /** Used by metrics. */
  public long getTotalReplicatedBlocks() {
    return blocksMap.getReplicatedBlocks();
  }

  /** Used by metrics. */
  public long getLowRedundancyECBlockGroups() {
    return neededReconstruction.getLowRedundancyECBlockGroups();
  }

  /** Used by metrics. */
  public long getCorruptECBlockGroups() {
    return corruptReplicas.getCorruptECBlockGroups();
  }

  /** Used by metrics. */
  public long getMissingECBlockGroups() {
    return neededReconstruction.getCorruptECBlockGroups();
  }

  /** Used by metrics. */
  public long getPendingDeletionECBlocks() {
    return invalidateBlocks.getECBlocks();
  }

  /** Used by metrics. */
  public long getTotalECBlockGroups() {
    return blocksMap.getECBlockGroups();
  }

  /** Used by metrics. */
  public int getPendingSPSPaths() {
    if (spsManager != null) {
      return spsManager.getPendingSPSPaths();
    }
    return 0;
  }

  /**
   * redundancyRecheckInterval is how often namenode checks for new
   * reconstruction work.
   */
  private final long redundancyRecheckIntervalMs;

  /**
   * Tracks how many calls have been made to chooseLowReduncancyBlocks since
   * the queue position was last reset to the queue head. If CallsSinceReset
   * crosses the threshold the next call will reset the iterators. A threshold
   * of zero means the queue position will only be reset once the next of the
   * queue has been reached.
   */
  private int replQueueResetToHeadThreshold;
  private int replQueueCallsSinceReset = 0;

  /**
   * Mapping: Block {@literal ->} { BlockCollection, datanodes, self ref }
   * Updated only in response to client-sent information.
   */
  final BlocksMap blocksMap;

  /** Redundancy thread. */
  private final Daemon redundancyThread = new Daemon(new RedundancyMonitor());
  /**
   * Timestamp marking the end time of {@link #redundancyThread}'s full cycle.
   * This value can be checked by the Junit tests to verify that the
   * {@link #redundancyThread} has run at least one full iteration.
   */
  private final AtomicLong lastRedundancyCycleTS = new AtomicLong(-1);
  /**
   * markedDeleteBlockScrubber thread for handling async delete blocks.
   */
  private final Daemon markedDeleteBlockScrubberThread =
      new Daemon(new MarkedDeleteBlockScrubber());

  /** Block report thread for handling async reports. */
  private final BlockReportProcessingThread blockReportThread;

  /**
   * Store blocks {@literal ->} datanodedescriptor(s) map of corrupt replicas.
   */
  final CorruptReplicasMap corruptReplicas = new CorruptReplicasMap();

  /**
   * Blocks to be invalidated.
   * For a striped block to invalidate, we should track its individual internal
   * blocks.
   */
  private final InvalidateBlocks invalidateBlocks;
  
  /**
   * After a failover, over-replicated blocks may not be handled
   * until all of the replicas have done a block report to the
   * new active. This is to make sure that this NameNode has been
   * notified of all block deletions that might have been pending
   * when the failover happened.
   */
  private final Set<Block> postponedMisreplicatedBlocks =
      new LinkedHashSet<Block>();
  private final int blocksPerPostpondedRescan;
  private final ArrayList<Block> rescannedMisreplicatedBlocks;

  /**
   * Maps a StorageID to the set of blocks that are "extra" for this
   * DataNode. We'll eventually remove these extras.
   */
  private final ExcessRedundancyMap excessRedundancyMap =
      new ExcessRedundancyMap();

  /**
   * Store set of Blocks that need to be replicated 1 or more times.
   * We also store pending reconstruction-orders.
   */
  public final LowRedundancyBlocks neededReconstruction =
      new LowRedundancyBlocks();

  @VisibleForTesting
  final PendingReconstructionBlocks pendingReconstruction;

  /** Stores information about block recovery attempts. */
  private final PendingRecoveryBlocks pendingRecoveryBlocks;

  /** The maximum number of replicas allowed for a block */
  public final short maxReplication;
  /**
   * The maximum number of outgoing replication streams a given node should have
   * at one time considering all but the highest priority replications needed.
    */
  private volatile int maxReplicationStreams;
  /**
   * The maximum number of outgoing replication streams a given node should have
   * at one time.
   */
  private volatile int replicationStreamsHardLimit;
  /** Minimum copies needed or else write is disallowed */
  public final short minReplication;
  /** Default number of replicas */
  public final int defaultReplication;
  /** value returned by MAX_CORRUPT_FILES_RETURNED */
  final int maxCorruptFilesReturned;

  final float blocksInvalidateWorkPct;
  private volatile int blocksReplWorkMultiplier;

  // whether or not to issue block encryption keys.
  final boolean encryptDataTransfer;
  
  // Max number of blocks to log info about during a block report.
  private final long maxNumBlocksToLog;

  // Max write lock hold time for BlockReportProcessingThread(ms).
  private final long maxLockHoldTime;

  /**
   * When running inside a Standby node, the node may receive block reports
   * from datanodes before receiving the corresponding namespace edits from
   * the active NameNode. Thus, it will postpone them for later processing,
   * instead of marking the blocks as corrupt.
   */
  private boolean shouldPostponeBlocksFromFuture = false;

  /**
   * Process reconstruction queues asynchronously to allow namenode safemode
   * exit and failover to be faster. HDFS-5496.
   */
  private Daemon reconstructionQueuesInitializer = null;
  /**
   * Number of blocks to process asychronously for reconstruction queues
   * initialization once aquired the namesystem lock. Remaining blocks will be
   * processed again after aquiring lock again.
   */
  private int numBlocksPerIteration;

  /**
   * The blocks of deleted files are put into the queue,
   * and the cleanup thread processes these blocks periodically.
   */
  private final ConcurrentLinkedQueue<List<BlockInfo>> markedDeleteQueue;

  /**
   * Progress of the Reconstruction queues initialisation.
   */
  private float reconstructionQueuesInitProgress = 0.0f;

  /** for block replicas placement */
  private volatile BlockPlacementPolicies placementPolicies;
  private final BlockStoragePolicySuite storagePolicySuite;

  /** Check whether name system is running before terminating */
  private boolean checkNSRunning = true;

  /** Check whether there are any non-EC blocks using StripedID */
  private boolean hasNonEcBlockUsingStripedID = false;

  private final BlockIdManager blockIdManager;

  /**
   * For satisfying block storage policies. Instantiates if sps is enabled
   * internally or externally.
   */
  private StoragePolicySatisfyManager spsManager;

  /** Minimum live replicas needed for the datanode to be transitioned
   * from ENTERING_MAINTENANCE to IN_MAINTENANCE.
   */
  private final short minReplicationToBeInMaintenance;
  /**
   * Whether to delete corrupt replica immediately irrespective of other
   * replicas available on stale storages.
   */
  private final boolean deleteCorruptReplicaImmediately;

  /** Storages accessible from multiple DNs. */
  private final ProvidedStorageMap providedStorageMap;

  public BlockManager(final Namesystem namesystem, boolean haEnabled,
      final Configuration conf) throws IOException {
    this.namesystem = namesystem;
    this.datanodeManager = new DatanodeManager(this, namesystem, conf);
    this.heartbeatManager = datanodeManager.getHeartbeatManager();
    this.blockIdManager = new BlockIdManager(this);
    this.blocksPerPostpondedRescan = (int)Math.min(Integer.MAX_VALUE,
        datanodeManager.getBlocksPerPostponedMisreplicatedBlocksRescan());
    this.rescannedMisreplicatedBlocks =
        new ArrayList<Block>(blocksPerPostpondedRescan);
    this.startupDelayBlockDeletionInMs = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY,
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_DEFAULT)
        * 1000L;
    this.deleteBlockLockTimeMs = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_BLOCK_DELETION_LOCK_THRESHOLD_MS,
        DFSConfigKeys.DFS_NAMENODE_BLOCK_DELETION_LOCK_THRESHOLD_MS_DEFAULT);
    this.deleteBlockUnlockIntervalTimeMs = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_BLOCK_DELETION_UNLOCK_INTERVAL_MS,
        DFSConfigKeys.DFS_NAMENODE_BLOCK_DELETION_UNLOCK_INTERVAL_MS_DEFAULT);
    this.invalidateBlocks = new InvalidateBlocks(
        datanodeManager.getBlockInvalidateLimit(),
        startupDelayBlockDeletionInMs,
        blockIdManager);
    this.markedDeleteQueue = new ConcurrentLinkedQueue<>();
    // Compute the map capacity by allocating 2% of total memory
    this.blocksMap = new BlocksMap(
        LightWeightGSet.computeCapacity(2.0, "BlocksMap"));
    this.placementPolicies = new BlockPlacementPolicies(
        conf, datanodeManager.getFSClusterStats(),
        datanodeManager.getNetworkTopology(),
        datanodeManager.getHost2DatanodeMap());
    this.storagePolicySuite = BlockStoragePolicySuite.createDefaultSuite(conf);
    this.pendingReconstruction = new PendingReconstructionBlocks(conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY,
        DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_DEFAULT)
        * 1000L);

    createSPSManager(conf);

    this.blockTokenSecretManager = createBlockTokenSecretManager(conf);
    this.providedStorageMap = new ProvidedStorageMap(namesystem, this, conf);

    this.maxCorruptFilesReturned = conf.getInt(
        DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED_KEY,
        DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED);
    this.defaultReplication = conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);

    this.minReplication = (short) initMinReplication(conf);
    this.maxReplication = (short) initMaxReplication(conf);

    this.maxReplicationStreams =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT);
    this.replicationStreamsHardLimit =
        conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT);
    this.blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
    this.blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);

    this.redundancyRecheckIntervalMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_DEFAULT,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);

    this.encryptDataTransfer =
        conf.getBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY,
            DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT);

    this.maxNumBlocksToLog =
        conf.getLong(DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_KEY,
            DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT);
    this.maxLockHoldTime = conf.getTimeDuration(
        DFSConfigKeys.DFS_NAMENODE_BLOCKREPORT_MAX_LOCK_HOLD_TIME,
        DFSConfigKeys.DFS_NAMENODE_BLOCKREPORT_MAX_LOCK_HOLD_TIME_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.numBlocksPerIteration = conf.getInt(
        DFSConfigKeys.DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT,
        DFSConfigKeys.DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT_DEFAULT);

    this.minReplicationToBeInMaintenance =
        (short) initMinReplicationToBeInMaintenance(conf);
    this.replQueueResetToHeadThreshold =
        initReplQueueResetToHeadThreshold(conf);

    long heartbeatIntervalSecs = conf.getTimeDuration(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.SECONDS);
    long blockRecoveryTimeout = getBlockRecoveryTimeout(heartbeatIntervalSecs);
    this.pendingRecoveryBlocks = new PendingRecoveryBlocks(blockRecoveryTimeout);

    this.blockReportLeaseManager = new BlockReportLeaseManager(conf);

    this.bmSafeMode = new BlockManagerSafeMode(this, namesystem, haEnabled,
        conf);

    int queueSize = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_BLOCKREPORT_QUEUE_SIZE_KEY,
        DFSConfigKeys.DFS_NAMENODE_BLOCKREPORT_QUEUE_SIZE_DEFAULT);
    this.blockReportThread = new BlockReportProcessingThread(queueSize);

    this.deleteCorruptReplicaImmediately =
        conf.getBoolean(DFS_NAMENODE_CORRUPT_BLOCK_DELETE_IMMEDIATELY_ENABLED,
            DFS_NAMENODE_CORRUPT_BLOCK_DELETE_IMMEDIATELY_ENABLED_DEFAULT);

    printInitialConfigs();
  }

  private int initMinReplication(Configuration conf) throws IOException {
    final int minR = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
    if (minR <= 0) {
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY
          + " = " + minR + " <= 0");
    }
    return minR;
  }

  private int initMaxReplication(Configuration conf) throws IOException {
    final int maxR = conf.getInt(DFSConfigKeys.DFS_REPLICATION_MAX_KEY,
        DFSConfigKeys.DFS_REPLICATION_MAX_DEFAULT);
    if (maxR > Short.MAX_VALUE) {
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_REPLICATION_MAX_KEY
          + " = " + maxR + " > " + Short.MAX_VALUE);
    }
    if (minReplication > maxR) {
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY
          + " = " + minReplication + " > "
          + DFSConfigKeys.DFS_REPLICATION_MAX_KEY
          + " = " + maxR);
    }
    return maxR;
  }

  private int initMinReplicationToBeInMaintenance(Configuration conf)
      throws IOException {
    final int minMaintenanceR = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_DEFAULT);

    if (minMaintenanceR < 0) {
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY
          + " = " + minMaintenanceR + " < 0");
    }
    if (minMaintenanceR > defaultReplication) {
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY
          + " = " + minMaintenanceR + " > "
          + DFSConfigKeys.DFS_REPLICATION_KEY
          + " = " + defaultReplication);
    }
    return minMaintenanceR;
  }

  private int initReplQueueResetToHeadThreshold(Configuration conf) {
    int threshold = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_QUEUE_RESTART_ITERATIONS,
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_QUEUE_RESTART_ITERATIONS_DEFAULT);
    if (threshold < 0) {
      LOG.warn("{} is set to {} and it must be >= 0. Resetting to default {}",
          DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_QUEUE_RESTART_ITERATIONS,
          threshold, DFSConfigKeys.
              DFS_NAMENODE_REDUNDANCY_QUEUE_RESTART_ITERATIONS_DEFAULT);
      threshold = DFSConfigKeys.
          DFS_NAMENODE_REDUNDANCY_QUEUE_RESTART_ITERATIONS_DEFAULT;
    }
    return threshold;
  }

  private void printInitialConfigs() {
    LOG.info("defaultReplication         = {}", defaultReplication);
    LOG.info("maxReplication             = {}", maxReplication);
    LOG.info("minReplication             = {}", minReplication);
    LOG.info("maxReplicationStreams      = {}", maxReplicationStreams);
    LOG.info("redundancyRecheckInterval  = {}ms", redundancyRecheckIntervalMs);
    LOG.info("encryptDataTransfer        = {}", encryptDataTransfer);
    LOG.info("maxNumBlocksToLog          = {}", maxNumBlocksToLog);
  }

  private static BlockTokenSecretManager createBlockTokenSecretManager(
      final Configuration conf) throws IOException {
    final boolean isEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);
    LOG.info("{} = {}", DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY,
            isEnabled);

    if (!isEnabled) {
      if (UserGroupInformation.isSecurityEnabled()) {
        String errMessage = "Security is enabled but block access tokens " +
            "(via " + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + ") " +
            "aren't enabled. This may cause issues " +
            "when clients attempt to connect to a DataNode. Aborting NameNode";
        throw new IOException(errMessage);
      }
      return null;
    }

    final long updateMin = conf.getLong(
        DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_DEFAULT);
    final long lifetimeMin = conf.getLong(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_DEFAULT);
    final String encryptionAlgorithm = conf.get(
        DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
    LOG.info("{}={} min(s), {}={} min(s), {}={}",
        DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, updateMin,
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, lifetimeMin,
        DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY, encryptionAlgorithm);
    
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    boolean isHaEnabled = HAUtil.isHAEnabled(conf, nsId);
    boolean shouldWriteProtobufToken = conf.getBoolean(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_PROTOBUF_ENABLE,
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_PROTOBUF_ENABLE_DEFAULT);

    boolean shouldWrapQOP = conf.getBoolean(
        DFS_NAMENODE_SEND_QOP_ENABLED, DFS_NAMENODE_SEND_QOP_ENABLED_DEFAULT);

    if (isHaEnabled) {
      // figure out which index we are of the nns
      Collection<String> nnIds = DFSUtilClient.getNameNodeIds(conf, nsId);
      String nnId = HAUtil.getNameNodeId(conf, nsId);
      int nnIndex = 0;
      for (String id : nnIds) {
        if (id.equals(nnId)) {
          break;
        }
        nnIndex++;
      }
      return new BlockTokenSecretManager(updateMin * 60 * 1000L,
          lifetimeMin * 60 * 1000L, nnIndex, nnIds.size(), null,
          encryptionAlgorithm, shouldWriteProtobufToken, shouldWrapQOP);
    } else {
      return new BlockTokenSecretManager(updateMin*60*1000L,
          lifetimeMin*60*1000L, 0, 1, null, encryptionAlgorithm,
          shouldWriteProtobufToken, shouldWrapQOP);
    }
  }

  public BlockStoragePolicy getStoragePolicy(final String policyName) {
    return storagePolicySuite.getPolicy(policyName);
  }

  public BlockStoragePolicy getStoragePolicy(final byte policyId) {
    return storagePolicySuite.getPolicy(policyId);
  }

  public BlockStoragePolicy[] getStoragePolicies() {
    return storagePolicySuite.getAllPolicies();
  }

  public void setBlockPoolId(String blockPoolId) {
    this.blockPoolId = blockPoolId;
    if (isBlockTokenEnabled()) {
      blockTokenSecretManager.setBlockPoolId(blockPoolId);
    }
  }

  public String getBlockPoolId() {
    return blockPoolId;
  }

  public BlockStoragePolicySuite getStoragePolicySuite() {
    return storagePolicySuite;
  }

  /** get the BlockTokenSecretManager */
  @VisibleForTesting
  public BlockTokenSecretManager getBlockTokenSecretManager() {
    return blockTokenSecretManager;
  }

  /** Allow silent termination of redundancy monitor for testing. */
  @VisibleForTesting
  void enableRMTerminationForTesting() {
    checkNSRunning = false;
  }

  private boolean isBlockTokenEnabled() {
    return blockTokenSecretManager != null;
  }

  /** Should the access keys be updated? */
  boolean shouldUpdateBlockKey(final long updateTime) throws IOException {
    return isBlockTokenEnabled() && blockTokenSecretManager.updateKeys(updateTime);
  }

  public void activate(Configuration conf, long blockTotal) {
    pendingReconstruction.start();
    datanodeManager.activate(conf);
    this.redundancyThread.setName("RedundancyMonitor");
    this.redundancyThread.start();
    this.markedDeleteBlockScrubberThread.
        setName("MarkedDeleteBlockScrubberThread");
    this.markedDeleteBlockScrubberThread.start();
    this.blockReportThread.start();
    mxBeanName = MBeans.register("NameNode", "BlockStats", this);
    bmSafeMode.activate(blockTotal);
  }

  public void close() {
    if (getSPSManager() != null) {
      getSPSManager().stop();
    }
    bmSafeMode.close();
    try {
      redundancyThread.interrupt();
      blockReportThread.interrupt();
      markedDeleteBlockScrubberThread.interrupt();
      redundancyThread.join(3000);
      blockReportThread.join(3000);
      markedDeleteBlockScrubberThread.join(3000);
    } catch (InterruptedException ie) {
    }
    datanodeManager.close();
    pendingReconstruction.stop();
    blocksMap.close();
  }

  /** @return the datanodeManager */
  public DatanodeManager getDatanodeManager() {
    return datanodeManager;
  }

  @VisibleForTesting
  public BlockPlacementPolicy getBlockPlacementPolicy() {
    return placementPolicies.getPolicy(CONTIGUOUS);
  }

  @VisibleForTesting
  public BlockPlacementPolicy getStriptedBlockPlacementPolicy() {
    return placementPolicies.getPolicy(STRIPED);
  }

  public void refreshBlockPlacementPolicy(Configuration conf) {
    BlockPlacementPolicies bpp =
        new BlockPlacementPolicies(conf, datanodeManager.getFSClusterStats(),
            datanodeManager.getNetworkTopology(),
            datanodeManager.getHost2DatanodeMap());
    placementPolicies = bpp;
  }

  /** Dump meta data to out. */
  public void metaSave(PrintWriter out) {
    assert namesystem.hasReadLock(); // TODO: block manager read lock and NS write lock
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    datanodeManager.fetchDatanodes(live, dead, false);
    out.println("Live Datanodes: " + live.size());
    out.println("Dead Datanodes: " + dead.size());

    //
    // Need to iterate over all queues from neededReplications
    // except for the QUEUE_WITH_CORRUPT_BLOCKS)
    //
    synchronized (neededReconstruction) {
      out.println("Metasave: Blocks waiting for reconstruction: "
          + neededReconstruction.getLowRedundancyBlockCount());
      for (int i = 0; i < neededReconstruction.LEVEL; i++) {
        if (i != neededReconstruction.QUEUE_WITH_CORRUPT_BLOCKS) {
          for (Iterator<BlockInfo> it = neededReconstruction.iterator(i);
               it.hasNext();) {
            Block block = it.next();
            dumpBlockMeta(block, out);
          }
        }
      }
      //
      // Now prints corrupt blocks separately
      //
      out.println("Metasave: Blocks currently missing: " +
          neededReconstruction.getCorruptBlockSize());
      for (Iterator<BlockInfo> it = neededReconstruction.
          iterator(neededReconstruction.QUEUE_WITH_CORRUPT_BLOCKS);
           it.hasNext();) {
        Block block = it.next();
        dumpBlockMeta(block, out);
      }
    }

    // Dump any postponed over-replicated blocks
    out.println("Mis-replicated blocks that have been postponed:");
    for (Block block : postponedMisreplicatedBlocks) {
      dumpBlockMeta(block, out);
    }

    // Dump blocks from pendingReconstruction
    pendingReconstruction.metaSave(out);

    // Dump blocks that are waiting to be deleted
    invalidateBlocks.dump(out);

    //Dump corrupt blocks and their storageIDs
    Set<Block> corruptBlocks = corruptReplicas.getCorruptBlocksSet();
    out.println("Corrupt Blocks:");
    for(Block block : corruptBlocks) {
      Collection<DatanodeDescriptor> corruptNodes =
          corruptReplicas.getNodes(block);
      if (corruptNodes == null) {
        LOG.warn("{} is corrupt but has no associated node.",
                 block.getBlockId());
        continue;
      }
      int numNodesToFind = corruptNodes.size();
      for (DatanodeStorageInfo storage : blocksMap.getStorages(block)) {
        DatanodeDescriptor node = storage.getDatanodeDescriptor();
        if (corruptNodes.contains(node)) {
          String storageId = storage.getStorageID();
          DatanodeStorageInfo storageInfo = node.getStorageInfo(storageId);
          State state = (storageInfo == null) ? null : storageInfo.getState();
          out.println("Block=" + block.toString()
              + "\tSize=" + block.getNumBytes()
              + "\tNode=" + node.getName() + "\tStorageID=" + storageId
              + "\tStorageState=" + state
              + "\tTotalReplicas=" + blocksMap.numNodes(block)
              + "\tReason=" + corruptReplicas.getCorruptReason(block, node));
          numNodesToFind--;
          if (numNodesToFind == 0) {
            break;
          }
        }
      }
      if (numNodesToFind > 0) {
        String[] corruptNodesList = new String[corruptNodes.size()];
        int i = 0;
        for (DatanodeDescriptor d : corruptNodes) {
          corruptNodesList[i] = d.getHostName();
          i++;
        }
        out.println(block.getBlockId() + " corrupt on " +
            StringUtils.join(",", corruptNodesList) + " but not all nodes are" +
            "found in its block locations");
      }
    }

    // Dump all datanodes
    getDatanodeManager().datanodeDump(out);
  }

  /**
   * Dump the metadata for the given block in a human-readable
   * form.
   */
  private void dumpBlockMeta(Block block, PrintWriter out) {
    List<DatanodeDescriptor> containingNodes =
                                      new ArrayList<DatanodeDescriptor>();
    List<DatanodeStorageInfo> containingLiveReplicasNodes =
      new ArrayList<DatanodeStorageInfo>();
    
    NumberReplicas numReplicas = new NumberReplicas();
    BlockInfo blockInfo = getStoredBlock(block);
    if (blockInfo == null) {
      out.println("Block "+ block + " is Null");
      return;
    }
    // source node returned is not used
    chooseSourceDatanodes(blockInfo, containingNodes,
        containingLiveReplicasNodes, numReplicas, new ArrayList<Byte>(),
        new ArrayList<Byte>(), new ArrayList<Byte>(), LowRedundancyBlocks.LEVEL);
    
    // containingLiveReplicasNodes can include READ_ONLY_SHARED replicas which are 
    // not included in the numReplicas.liveReplicas() count
    assert containingLiveReplicasNodes.size() >= numReplicas.liveReplicas();
    int usableReplicas = numReplicas.liveReplicas() +
                         numReplicas.decommissionedAndDecommissioning();

    if (block instanceof BlockInfo) {
      BlockCollection bc = getBlockCollection((BlockInfo)block);
      String fileName = (bc == null) ? "[orphaned]" : bc.getName();
      out.print(fileName + ": ");
    }
    // l: == live:, d: == decommissioned c: == corrupt e: == excess
    out.print(block + ((usableReplicas > 0)? "" : " MISSING") +
              " (replicas:" +
              " live: " + numReplicas.liveReplicas() +
              " decommissioning and decommissioned: " +
        numReplicas.decommissionedAndDecommissioning() +
              " corrupt: " + numReplicas.corruptReplicas() +
              " in excess: " + numReplicas.excessReplicas() +
              " maintenance mode: " + numReplicas.maintenanceReplicas() + ") ");

    Collection<DatanodeDescriptor> corruptNodes = 
                                  corruptReplicas.getNodes(block);
    
    for (DatanodeStorageInfo storage : blocksMap.getStorages(block)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      String state = "";
      if (corruptNodes != null && corruptNodes.contains(node)) {
        state = "(corrupt)";
      } else if (node.isDecommissioned() || 
          node.isDecommissionInProgress()) {
        state = "(decommissioned)";
      } else if (node.isMaintenance() || node.isInMaintenance()){
        state = "(maintenance)";
      }
      
      if (storage.areBlockContentsStale()) {
        state += " (block deletions maybe out of date)";
      }
      out.print(" " + node + state + " : ");
    }
    out.println("");
  }

  /** Returns the current setting for maxReplicationStreams, which is set by
   *  {@code DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY}.
   *
   *  @return maxReplicationStreams
   */
  public int getMaxReplicationStreams() {
    return maxReplicationStreams;
  }

  static private void ensurePositiveInt(int val, String key) {
    Preconditions.checkArgument(
        (val > 0),
        key + " = '" + val + "' is invalid. " +
            "It should be a positive, non-zero integer value.");
  }

  /**
   * Updates the value used for maxReplicationStreams, which is set by
   * {@code DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY} initially.
   *
   * @param newVal - Must be a positive non-zero integer.
   */
  @VisibleForTesting
  public void setMaxReplicationStreams(int newVal, boolean ensurePositiveInt) {
    if (ensurePositiveInt) {
      ensurePositiveInt(newVal,
          DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY);
    }
    maxReplicationStreams = newVal;
  }

  public void setMaxReplicationStreams(int newVal) {
    setMaxReplicationStreams(newVal, true);
  }

  /** Returns the current setting for maxReplicationStreamsHardLimit, set by
   * {@code DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY}.
   *
   *  @return maxReplicationStreamsHardLimit
   */
  public int getReplicationStreamsHardLimit() {
    return replicationStreamsHardLimit;
  }

  /**
   * Updates the value used for replicationStreamsHardLimit, which is set by
   * {@code DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY}
   * initially.
   *
   * @param newVal - Must be a positive non-zero integer.
   */
  public void setReplicationStreamsHardLimit(int newVal) {
    ensurePositiveInt(newVal,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY);
    replicationStreamsHardLimit = newVal;
  }

  /** Returns the current setting for blocksReplWorkMultiplier, set by
   * {@code DFSConfigKeys.
   *     DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION}.
   *
   *  @return maxReplicationStreamsHardLimit
   */
  public int getBlocksReplWorkMultiplier() {
    return blocksReplWorkMultiplier;
  }

  /**
   * Updates the value used for blocksReplWorkMultiplier, set by
   * {@code DFSConfigKeys.
   *     DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION} initially.
   * @param newVal - Must be a positive non-zero integer.
   */
  public void setBlocksReplWorkMultiplier(int newVal) {
    ensurePositiveInt(newVal,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION);
    blocksReplWorkMultiplier = newVal;
  }

  /**
   * Updates the value used for pendingReconstruction timeout, which is set by
   * {@code DFSConfigKeys.
   *     DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY} initially.
   *
   * @param newVal - Must be a positive non-zero integer.
   */
  public void setReconstructionPendingTimeout(int newVal) {
    ensurePositiveInt(newVal,
        DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY);
    pendingReconstruction.setTimeout(newVal * 1000L);
  }

  /** Returns the current setting for pendingReconstruction timeout, set by
   * {@code DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY}.
   */
  public int getReconstructionPendingTimeout() {
    return (int)(pendingReconstruction.getTimeout() / 1000L);
  }

  public int getDefaultStorageNum(BlockInfo block) {
    switch (block.getBlockType()) {
    case STRIPED: return ((BlockInfoStriped) block).getRealTotalBlockNum();
    case CONTIGUOUS: return defaultReplication;
    default:
      throw new IllegalArgumentException(
          "getDefaultStorageNum called with unknown BlockType: "
          + block.getBlockType());
    }
  }

  public short getMinReplication() {
    return minReplication;
  }

  public short getMinStorageNum(BlockInfo block) {
    switch(block.getBlockType()) {
    case STRIPED: return ((BlockInfoStriped) block).getRealDataBlockNum();
    case CONTIGUOUS: return minReplication;
    default:
      throw new IllegalArgumentException(
          "getMinStorageNum called with unknown BlockType: "
          + block.getBlockType());
    }
  }

  public short getMinReplicationToBeInMaintenance() {
    return minReplicationToBeInMaintenance;
  }

  short getMinMaintenanceStorageNum(BlockInfo block) {
    if (block.isStriped()) {
      return ((BlockInfoStriped) block).getRealDataBlockNum();
    } else {
      return (short) Math.min(minReplicationToBeInMaintenance,
          block.getReplication());
    }
  }

  public boolean hasMinStorage(BlockInfo block) {
    return countNodes(block).liveReplicas() >= getMinStorageNum(block);
  }

  public boolean hasMinStorage(BlockInfo block, int liveNum) {
    return liveNum >= getMinStorageNum(block);
  }

  /**
   * Commit a block of a file
   * 
   * @param block block to be committed
   * @param commitBlock - contains client reported block length and generation
   * @return true if the block is changed to committed state.
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  private boolean commitBlock(final BlockInfo block,
      final Block commitBlock) throws IOException {
    if (block.getBlockUCState() == BlockUCState.COMMITTED)
      return false;
    assert block.getNumBytes() <= commitBlock.getNumBytes() :
        "commitBlock length is less than the stored one "
            + commitBlock.getNumBytes() + " vs. " + block.getNumBytes();
    if(block.getGenerationStamp() != commitBlock.getGenerationStamp()) {
      throw new IOException("Commit block with mismatching GS. NN has " +
          block + ", client submits " + commitBlock);
    }
    List<ReplicaUnderConstruction> staleReplicas =
        block.commitBlock(commitBlock);
    removeStaleReplicas(staleReplicas, block);
    return true;
  }
  
  /**
   * Commit the last block of the file and mark it as complete if it has
   * meets the minimum redundancy requirement
   * 
   * @param bc block collection
   * @param commitBlock - contains client reported block length and generation
   * @param iip - INodes in path to bc
   * @return true if the last block is changed to committed state.
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  public boolean commitOrCompleteLastBlock(BlockCollection bc,
      Block commitBlock, INodesInPath iip) throws IOException {
    if(commitBlock == null)
      return false; // not committing, this is a block allocation retry
    BlockInfo lastBlock = bc.getLastBlock();
    if(lastBlock == null)
      return false; // no blocks in file yet
    if(lastBlock.isComplete())
      return false; // already completed (e.g. by syncBlock)
    if(lastBlock.isUnderRecovery()) {
      throw new IOException("Commit or complete block " + commitBlock +
          ", whereas it is under recovery.");
    }
    
    final boolean committed = commitBlock(lastBlock, commitBlock);
    if (committed && lastBlock.isStriped()) {
      // update scheduled size for DatanodeStorages that do not store any
      // internal blocks
      lastBlock.getUnderConstructionFeature()
          .updateStorageScheduledSize((BlockInfoStriped) lastBlock);
    }

    // Count replicas on decommissioning nodes, as these will not be
    // decommissioned unless recovery/completing last block has finished
    NumberReplicas numReplicas = countNodes(lastBlock);
    int numUsableReplicas = numReplicas.liveReplicas() +
        numReplicas.decommissioning() +
        numReplicas.liveEnteringMaintenanceReplicas();

    if (hasMinStorage(lastBlock, numUsableReplicas)) {
      if (committed) {
        addExpectedReplicasToPending(lastBlock);
      }
      completeBlock(lastBlock, iip, false);
    } else if (pendingRecoveryBlocks.isUnderRecovery(lastBlock)) {
      // We've just finished recovery for this block, complete
      // the block forcibly disregarding number of replicas.
      // This is to ignore minReplication, the block will be closed
      // and then replicated out.
      completeBlock(lastBlock, iip, true);
      updateNeededReconstructions(lastBlock, 1, 0);
    }
    return committed;
  }

  /**
   * If IBR is not sent from expected locations yet, add the datanodes to
   * pendingReconstruction in order to keep RedundancyMonitor from scheduling
   * the block. In case of erasure coding blocks, adds only in case there
   * isn't any missing node.
   */
  public void addExpectedReplicasToPending(BlockInfo blk) {
    boolean addForStriped = false;
    DatanodeStorageInfo[] expectedStorages =
        blk.getUnderConstructionFeature().getExpectedStorageLocations();
    if (blk.isStriped()) {
      BlockInfoStriped blkStriped = (BlockInfoStriped) blk;
      addForStriped =
          blkStriped.getRealTotalBlockNum() == expectedStorages.length;
    }
    if (!blk.isStriped() || addForStriped) {
      if (expectedStorages.length - blk.numNodes() > 0) {
        ArrayList<DatanodeStorageInfo> pendingNodes = new ArrayList<>();
        for (DatanodeStorageInfo storage : expectedStorages) {
          DatanodeDescriptor dnd = storage.getDatanodeDescriptor();
          if (blk.findStorageInfo(dnd) == null) {
            pendingNodes.add(storage);
          }
        }
        pendingReconstruction.increment(blk,
            pendingNodes.toArray(new DatanodeStorageInfo[pendingNodes.size()]));
      }
    }
  }

  /**
   * Convert a specified block of the file to a complete block.
   * @param curBlock - block to be completed
   * @param iip - INodes in path to file containing curBlock; if null,
   *              this will be resolved internally
   * @param force - force completion of the block
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  private void completeBlock(BlockInfo curBlock, INodesInPath iip,
      boolean force) throws IOException {
    if (curBlock.isComplete()) {
      return;
    }

    int numNodes = curBlock.numNodes();
    if (!force && !hasMinStorage(curBlock, numNodes)) {
      throw new IOException("Cannot complete block: "
          + "block does not satisfy minimal replication requirement.");
    }
    if (!force && curBlock.getBlockUCState() != BlockUCState.COMMITTED) {
      throw new IOException(
          "Cannot complete block: block has not been COMMITTED by the client");
    }

    convertToCompleteBlock(curBlock, iip);

    // Since safe-mode only counts complete blocks, and we now have
    // one more complete block, we need to adjust the total up, and
    // also count it as safe, if we have at least the minimum replica
    // count. (We may not have the minimum replica count yet if this is
    // a "forced" completion when a file is getting closed by an
    // OP_CLOSE edit on the standby).
    bmSafeMode.adjustBlockTotals(0, 1);
    final int minStorage = curBlock.isStriped() ?
        ((BlockInfoStriped) curBlock).getRealDataBlockNum() : minReplication;
    bmSafeMode.incrementSafeBlockCount(Math.min(numNodes, minStorage),
        curBlock);
  }

  /**
   * Convert a specified block of the file to a complete block.
   * Skips validity checking and safe mode block total updates; use
   * {@link BlockManager#completeBlock} to include these.
   * @param curBlock - block to be completed
   * @param iip - INodes in path to file containing curBlock; if null,
   *              this will be resolved internally
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  private void convertToCompleteBlock(BlockInfo curBlock, INodesInPath iip)
      throws IOException {
    curBlock.convertToCompleteBlock();
    namesystem.getFSDirectory().updateSpaceForCompleteBlock(curBlock, iip);
  }

  /**
   * Force the given block in the given file to be marked as complete,
   * regardless of whether enough replicas are present. This is necessary
   * when tailing edit logs as a Standby.
   */
  public void forceCompleteBlock(final BlockInfo block) throws IOException {
    List<ReplicaUnderConstruction> staleReplicas = block.commitBlock(block);
    removeStaleReplicas(staleReplicas, block);
    completeBlock(block, null, true);
  }

  /**
   * Convert the last block of the file to an under construction block.<p>
   * The block is converted only if the file has blocks and the last one
   * is a partial block (its size is less than the preferred block size).
   * The converted block is returned to the client.
   * The client uses the returned block locations to form the data pipeline
   * for this block.<br>
   * The methods returns null if there is no partial block at the end.
   * The client is supposed to allocate a new block with the next call.
   *
   * @param bc file
   * @param bytesToRemove num of bytes to remove from block
   * @return the last block locations if the block is partial or null otherwise
   */
  public LocatedBlock convertLastBlockToUnderConstruction(
      BlockCollection bc, long bytesToRemove) throws IOException {
    BlockInfo lastBlock = bc.getLastBlock();
    if (lastBlock == null ||
       bc.getPreferredBlockSize() == lastBlock.getNumBytes() - bytesToRemove) {
      return null;
    }
    assert lastBlock == getStoredBlock(lastBlock) :
      "last block of the file is not in blocksMap";

    DatanodeStorageInfo[] targets = getStorages(lastBlock);

    // convert the last block to under construction. note no block replacement
    // is happening
    bc.convertLastBlockToUC(lastBlock, targets);

    // Remove block from reconstruction queue.
    NumberReplicas replicas = countNodes(lastBlock);
    neededReconstruction.remove(lastBlock, replicas.liveReplicas(),
        replicas.readOnlyReplicas(),
        replicas.outOfServiceReplicas(), getExpectedRedundancyNum(lastBlock));
    PendingBlockInfo remove = pendingReconstruction.remove(lastBlock);
    if (remove != null) {
      List<DatanodeStorageInfo> locations = remove.getTargets();
      DatanodeStorageInfo[] removedBlockTargets =
          new DatanodeStorageInfo[locations.size()];
      locations.toArray(removedBlockTargets);
      DatanodeStorageInfo.decrementBlocksScheduled(removedBlockTargets);
    }

    // remove this block from the list of pending blocks to be deleted. 
    for (DatanodeStorageInfo storage : targets) {
      final Block b = getBlockOnStorage(lastBlock, storage);
      if (b != null) {
        invalidateBlocks.remove(storage.getDatanodeDescriptor(), b);
      }
    }
    
    // Adjust safe-mode totals, since under-construction blocks don't
    // count in safe-mode.
    bmSafeMode.adjustBlockTotals(
        // decrement safe if we had enough
        hasMinStorage(lastBlock, targets.length) ? -1 : 0,
        // always decrement total blocks
        -1);

    final long fileLength = bc.computeContentSummary(
        getStoragePolicySuite()).getLength();
    final long pos = fileLength - lastBlock.getNumBytes();
    return createLocatedBlock(null, lastBlock, pos,
        BlockTokenIdentifier.AccessMode.WRITE);
  }

  /**
   * Get all valid locations of the block
   */
  private List<DatanodeStorageInfo> getValidLocations(BlockInfo block) {
    final List<DatanodeStorageInfo> locations
        = new ArrayList<DatanodeStorageInfo>(blocksMap.numNodes(block));
    for(DatanodeStorageInfo storage : blocksMap.getStorages(block)) {
      // filter invalidate replicas
      Block b = getBlockOnStorage(block, storage);
      if(b != null && 
          !invalidateBlocks.contains(storage.getDatanodeDescriptor(), b)) {
        locations.add(storage);
      }
    }
    return locations;
  }

  private void createLocatedBlockList(
      LocatedBlockBuilder locatedBlocks,
      final BlockInfo[] blocks,
      final long offset, final long length,
      final AccessMode mode) throws IOException {
    int curBlk;
    long curPos = 0, blkSize = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      blkSize = blocks[curBlk].getNumBytes();
      assert blkSize > 0 : "Block of size 0";
      if (curPos + blkSize > offset) {
        break;
      }
      curPos += blkSize;
    }

    if (nrBlocks > 0 && curBlk == nrBlocks)   // offset >= end of file
      return;

    long endOff = offset + length;
    do {
      locatedBlocks.addBlock(
          createLocatedBlock(locatedBlocks, blocks[curBlk], curPos, mode));
      curPos += blocks[curBlk].getNumBytes();
      curBlk++;
    } while (curPos < endOff 
          && curBlk < blocks.length
          && !locatedBlocks.isBlockMax());
    return;
  }

  private LocatedBlock createLocatedBlock(LocatedBlockBuilder locatedBlocks,
      final BlockInfo[] blocks,
      final long endPos, final AccessMode mode) throws IOException {
    int curBlk;
    long curPos = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      long blkSize = blocks[curBlk].getNumBytes();
      if (curPos + blkSize >= endPos) {
        break;
      }
      curPos += blkSize;
    }
    
    return createLocatedBlock(locatedBlocks, blocks[curBlk], curPos, mode);
  }

  private LocatedBlock createLocatedBlock(LocatedBlockBuilder locatedBlocks,
      final BlockInfo blk, final long pos, final AccessMode mode)
          throws IOException {
    final LocatedBlock lb = createLocatedBlock(locatedBlocks, blk, pos);
    if (mode != null) {
      setBlockToken(lb, mode);
    }
    return lb;
  }

  /** @return a LocatedBlock for the given block */
  private LocatedBlock createLocatedBlock(LocatedBlockBuilder locatedBlocks,
      final BlockInfo blk, final long pos) throws IOException {
    if (!blk.isComplete()) {
      final BlockUnderConstructionFeature uc = blk.getUnderConstructionFeature();
      if (blk.isStriped()) {
        final DatanodeStorageInfo[] storages = uc.getExpectedStorageLocations();
        final ExtendedBlock eb = new ExtendedBlock(getBlockPoolId(),
            blk);
        return newLocatedStripedBlock(eb, storages, uc.getBlockIndices(), pos,
            false);
      } else {
        final DatanodeStorageInfo[] storages = uc.getExpectedStorageLocations();
        final ExtendedBlock eb = new ExtendedBlock(getBlockPoolId(),
            blk);
        return null == locatedBlocks
            ? newLocatedBlock(eb, storages, pos, false)
                : locatedBlocks.newLocatedBlock(eb, storages, pos, false);
      }
    }

    // get block locations
    NumberReplicas numReplicas = countNodes(blk);
    final int numCorruptNodes = numReplicas.corruptReplicas();
    final int numCorruptReplicas = corruptReplicas.numCorruptReplicas(blk);
    if (numCorruptNodes != numCorruptReplicas) {
      LOG.warn("Inconsistent number of corrupt replicas for {}"
          + " blockMap has {} but corrupt replicas map has {}",
          blk, numCorruptNodes, numCorruptReplicas);
    }

    final int numNodes = blocksMap.numNodes(blk);
    final boolean isCorrupt;
    if (blk.isStriped()) {
      BlockInfoStriped sblk = (BlockInfoStriped) blk;
      isCorrupt = numCorruptReplicas != 0 &&
          numReplicas.liveReplicas() < sblk.getRealDataBlockNum();
    } else {
      isCorrupt = numCorruptReplicas != 0 && numCorruptReplicas == numNodes;
    }
    int numMachines = isCorrupt ? numNodes: numNodes - numCorruptReplicas;
    numMachines -= numReplicas.maintenanceNotForReadReplicas();
    DatanodeStorageInfo[] machines = new DatanodeStorageInfo[numMachines];
    final byte[] blockIndices = blk.isStriped() ? new byte[numMachines] : null;
    int j = 0, i = 0;
    if (numMachines > 0) {
      final boolean noCorrupt = (numCorruptReplicas == 0);
      for(DatanodeStorageInfo storage : blocksMap.getStorages(blk)) {
        if (storage.getState() != State.FAILED) {
          final DatanodeDescriptor d = storage.getDatanodeDescriptor();
          // Don't pick IN_MAINTENANCE or dead ENTERING_MAINTENANCE states.
          if (d.isInMaintenance()
              || (d.isEnteringMaintenance() && !d.isAlive())) {
            continue;
          }

          if (noCorrupt) {
            machines[j++] = storage;
            i = setBlockIndices(blk, blockIndices, i, storage);
          } else {
            final boolean replicaCorrupt = isReplicaCorrupt(blk, d);
            if (isCorrupt || !replicaCorrupt) {
              machines[j++] = storage;
              i = setBlockIndices(blk, blockIndices, i, storage);
            }
          }
        }
      }
    }

    if(j < machines.length) {
      machines = Arrays.copyOf(machines, j);
    }

    assert j == machines.length :
      "isCorrupt: " + isCorrupt +
      " numMachines: " + numMachines +
      " numNodes: " + numNodes +
      " numCorrupt: " + numCorruptNodes +
      " numCorruptRepls: " + numCorruptReplicas;
    final ExtendedBlock eb = new ExtendedBlock(getBlockPoolId(), blk);
    return blockIndices == null
        ? null == locatedBlocks ? newLocatedBlock(eb, machines, pos, isCorrupt)
            : locatedBlocks.newLocatedBlock(eb, machines, pos, isCorrupt)
        : newLocatedStripedBlock(eb, machines, blockIndices, pos, isCorrupt);
  }

  /** Create a LocatedBlocks. */
  public LocatedBlocks createLocatedBlocks(final BlockInfo[] blocks,
      final long fileSizeExcludeBlocksUnderConstruction,
      final boolean isFileUnderConstruction, final long offset,
      final long length, final boolean needBlockToken,
      final boolean inSnapshot, FileEncryptionInfo feInfo,
      ErasureCodingPolicy ecPolicy)
      throws IOException {
    assert namesystem.hasReadLock();
    if (blocks == null) {
      return null;
    } else if (blocks.length == 0) {
      return new LocatedBlocks(0, isFileUnderConstruction,
          Collections.<LocatedBlock> emptyList(), null, false, feInfo, ecPolicy);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("blocks = {}", java.util.Arrays.asList(blocks));
      }

      final AccessMode mode = needBlockToken? BlockTokenIdentifier.AccessMode.READ: null;

      LocatedBlockBuilder locatedBlocks = providedStorageMap
          .newLocatedBlocks(Integer.MAX_VALUE)
          .fileLength(fileSizeExcludeBlocksUnderConstruction)
          .lastUC(isFileUnderConstruction)
          .encryption(feInfo)
          .erasureCoding(ecPolicy);

      createLocatedBlockList(locatedBlocks, blocks, offset, length, mode);
      if (!inSnapshot) {
        final BlockInfo last = blocks[blocks.length - 1];
        final long lastPos = last.isComplete()?
            fileSizeExcludeBlocksUnderConstruction - last.getNumBytes()
            : fileSizeExcludeBlocksUnderConstruction;

        locatedBlocks
          .lastBlock(createLocatedBlock(locatedBlocks, last, lastPos, mode))
          .lastComplete(last.isComplete());
      } else {
        locatedBlocks
          .lastBlock(createLocatedBlock(locatedBlocks, blocks,
              fileSizeExcludeBlocksUnderConstruction, mode))
          .lastComplete(true);
      }
      LocatedBlocks locations = locatedBlocks.build();
      // Set caching information for the located blocks.
      CacheManager cm = namesystem.getCacheManager();
      if (cm != null) {
        cm.setCachedLocations(locations);
      }
      return locations;
    }
  }

  /** @return current access keys. */
  public ExportedBlockKeys getBlockKeys() {
    return isBlockTokenEnabled()? blockTokenSecretManager.exportKeys()
        : ExportedBlockKeys.DUMMY_KEYS;
  }

  /** Generate a block token for the located block. */
  public void setBlockToken(final LocatedBlock b,
      final AccessMode mode) throws IOException {
    if (isBlockTokenEnabled()) {
      // Use cached UGI if serving RPC calls.
      if (b.isStriped()) {
        Preconditions.checkState(b instanceof LocatedStripedBlock);
        LocatedStripedBlock sb = (LocatedStripedBlock) b;
        byte[] indices = sb.getBlockIndices();
        Token<BlockTokenIdentifier>[] blockTokens = new Token[indices.length];
        ExtendedBlock internalBlock = new ExtendedBlock(b.getBlock());
        for (int i = 0; i < indices.length; i++) {
          internalBlock.setBlockId(b.getBlock().getBlockId() + indices[i]);
          blockTokens[i] = blockTokenSecretManager.generateToken(
              NameNode.getRemoteUser().getShortUserName(),
              internalBlock, EnumSet.of(mode), b.getStorageTypes(),
              b.getStorageIDs());
        }
        sb.setBlockTokens(blockTokens);
      }
      b.setBlockToken(blockTokenSecretManager.generateToken(
          NameNode.getRemoteUser().getShortUserName(),
          b.getBlock(), EnumSet.of(mode), b.getStorageTypes(),
          b.getStorageIDs()));
    }
  }

  void addKeyUpdateCommand(final List<DatanodeCommand> cmds,
      final DatanodeDescriptor nodeinfo) {
    // check access key update
    if (isBlockTokenEnabled() && nodeinfo.needKeyUpdate()) {
      cmds.add(new KeyUpdateCommand(blockTokenSecretManager.exportKeys()));
      nodeinfo.setNeedKeyUpdate(false);
    }
  }
  
  public DataEncryptionKey generateDataEncryptionKey() {
    if (isBlockTokenEnabled() && encryptDataTransfer) {
      return blockTokenSecretManager.generateDataEncryptionKey();
    } else {
      return null;
    }
  }

  /**
   * Clamp the specified replication between the minimum and the maximum
   * replication levels.
   */
  public short adjustReplication(short replication) {
    return replication < minReplication? minReplication
        : replication > maxReplication? maxReplication: replication;
  }

  /**
   * Check whether the replication parameter is within the range
   * determined by system configuration and throw an exception if it's not.
   *
   * @param src the path to the target file
   * @param replication the requested replication factor
   * @param clientName the name of the client node making the request
   * @throws java.io.IOException thrown if the requested replication factor
   * is out of bounds
   */
   public void verifyReplication(String src,
                          short replication,
                          String clientName) throws IOException {
    String err = null;
    if (replication > maxReplication) {
      err = " exceeds maximum of " + maxReplication;
    } else if (replication < minReplication) {
      err = " is less than the required minimum of " + minReplication;
    }

    if (err != null) {
      throw new IOException("Requested replication factor of " + replication
          + err + " for " + src
          + (clientName == null? "": ", clientName=" + clientName));
    }
  }

  /**
   * Check if a block is replicated to at least the minimum replication.
   */
  public boolean isSufficientlyReplicated(BlockInfo b) {
    // Compare against the lesser of the minReplication and number of live DNs.
    final int liveReplicas = countNodes(b).liveReplicas();
    if (hasMinStorage(b, liveReplicas)) {
      return true;
    }
    // getNumLiveDataNodes() is very expensive and we minimize its use by
    // comparing with minReplication first.
    return liveReplicas >= getDatanodeManager().getNumLiveDataNodes();
  }

  private boolean isHotBlock(BlockInfo blockInfo, long time) {
    INodeFile iFile = (INodeFile)getBlockCollection(blockInfo);
    if(iFile == null) {
      return false;
    }
    if(iFile.isUnderConstruction()) {
      return true;
    }
    if (iFile.getAccessTime() > time || iFile.getModificationTime() > time) {
      return true;
    }
    return false;
  }

  /** Get all blocks with location information from a datanode. */
  public BlocksWithLocations getBlocksWithLocations(final DatanodeID datanode,
      final long size, final long minBlockSize, final long timeInterval) throws
      UnregisteredNodeException {
    final DatanodeDescriptor node = getDatanodeManager().getDatanode(datanode);
    if (node == null) {
      blockLog.warn("BLOCK* getBlocks: Asking for blocks from an" +
          " unrecorded node {}", datanode);
      throw new HadoopIllegalArgumentException(
          "Datanode " + datanode + " not found.");
    }

    int numBlocks = node.numBlocks();
    if(numBlocks == 0) {
      return new BlocksWithLocations(new BlockWithLocations[0]);
    }

    // skip stale storage
    DatanodeStorageInfo[] storageInfos = Arrays
        .stream(node.getStorageInfos())
        .filter(s -> !s.areBlockContentsStale())
        .toArray(DatanodeStorageInfo[]::new);

    // starting from a random block
    int startBlock = ThreadLocalRandom.current().nextInt(numBlocks);
    Iterator<BlockInfo> iter = node.getBlockIterator(startBlock, storageInfos);
    List<BlockWithLocations> results = new ArrayList<BlockWithLocations>();
    List<BlockInfo> pending = new ArrayList<BlockInfo>();
    long totalSize = 0;
    BlockInfo curBlock;
    long hotTimePos = Time.now() - timeInterval;
    while(totalSize<size && iter.hasNext()) {
      curBlock = iter.next();
      if(!curBlock.isComplete())  continue;
      if (curBlock.getNumBytes() < minBlockSize) {
        continue;
      }
      if(timeInterval > 0 && isHotBlock(curBlock, hotTimePos)) {
        pending.add(curBlock);
      } else {
        totalSize += addBlock(curBlock, results);
      }
    }
    if(totalSize<size) {
      iter = node.getBlockIterator(0, storageInfos); // start from the beginning
      for(int i = 0; i < startBlock && totalSize < size && iter.hasNext(); i++) {
        curBlock = iter.next();
        if(!curBlock.isComplete())  continue;
        if (curBlock.getNumBytes() < minBlockSize) {
          continue;
        }
        if(timeInterval > 0 && isHotBlock(curBlock, hotTimePos)) {
          pending.add(curBlock);
        } else {
          totalSize += addBlock(curBlock, results);
        }
      }
    }
    // if the cold block (access before timeInterval) is less than the
    // asked size, it will add the pending hot block in end of return list.
    for(int i = 0; i < pending.size() && totalSize < size; i++) {
      curBlock = pending.get(i);
      totalSize += addBlock(curBlock, results);
    }
    return new BlocksWithLocations(
        results.toArray(new BlockWithLocations[results.size()]));
  }

   
  /** Remove the blocks associated to the given datanode. */
  void removeBlocksAssociatedTo(final DatanodeDescriptor node) {
    providedStorageMap.removeDatanode(node);
    final Iterator<BlockInfo> it = node.getBlockIterator();
    while(it.hasNext()) {
      removeStoredBlock(it.next(), node);
    }
    // Remove all pending DN messages referencing this DN.
    pendingDNMessages.removeAllMessagesForDatanode(node);

    node.resetBlocks();
    invalidateBlocks.remove(node);
  }

  /** Remove the blocks associated to the given DatanodeStorageInfo. */
  void removeBlocksAssociatedTo(final DatanodeStorageInfo storageInfo) {
    assert namesystem.hasWriteLock();
    final Iterator<BlockInfo> it = storageInfo.getBlockIterator();
    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    while(it.hasNext()) {
      BlockInfo block = it.next();
      removeStoredBlock(block, node);
      final Block b = getBlockOnStorage(block, storageInfo);
      if (b != null) {
        invalidateBlocks.remove(node, b);
      }
    }
    checkSafeMode();
    LOG.info("Removed blocks associated with storage {} from DataNode {}",
        storageInfo, node);
  }

  /**
   * Adds block to list of blocks which will be invalidated on specified
   * datanode and log the operation
   */
  void addToInvalidates(final Block block, final DatanodeInfo datanode) {
    if (!isPopulatingReplQueues()) {
      return;
    }
    invalidateBlocks.add(block, datanode, true);
  }

  /**
   * Adds block to list of blocks which will be invalidated on all its
   * datanodes.
   */
  private void addToInvalidates(BlockInfo storedBlock) {
    if (!isPopulatingReplQueues()) {
      return;
    }
    StringBuilder datanodes = blockLog.isDebugEnabled()
        ? new StringBuilder() : null;
    for (DatanodeStorageInfo storage : blocksMap.getStorages(storedBlock)) {
      if (storage.getState() != State.NORMAL) {
        continue;
      }
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      final Block b = getBlockOnStorage(storedBlock, storage);
      if (b != null) {
        invalidateBlocks.add(b, node, false);
        if (datanodes != null) {
          datanodes.append(node).append(" ");
        }
      }
    }
    if (datanodes != null && datanodes.length() != 0) {
      blockLog.debug("BLOCK* addToInvalidates: {} {}", storedBlock, datanodes);
    }
  }

  private Block getBlockOnStorage(BlockInfo storedBlock,
      DatanodeStorageInfo storage) {
    return storedBlock.isStriped() ?
        ((BlockInfoStriped) storedBlock).getBlockOnStorage(storage) : storedBlock;
  }

  /**
   * Mark the block belonging to datanode as corrupt
   * @param blk Block to be marked as corrupt
   * @param dn Datanode which holds the corrupt replica
   * @param storageID if known, null otherwise.
   * @param reason a textual reason why the block should be marked corrupt,
   * for logging purposes
   */
  public void findAndMarkBlockAsCorrupt(final ExtendedBlock blk,
      final DatanodeInfo dn, String storageID, String reason) throws IOException {
    assert namesystem.hasWriteLock();
    final Block reportedBlock = blk.getLocalBlock();
    final BlockInfo storedBlock = getStoredBlock(reportedBlock);
    if (storedBlock == null) {
      // Check if the replica is in the blockMap, if not
      // ignore the request for now. This could happen when BlockScanner
      // thread of Datanode reports bad block before Block reports are sent
      // by the Datanode on startup
      blockLog.debug("BLOCK* findAndMarkBlockAsCorrupt: {} not found", blk);
      return;
    }

    DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot mark " + blk
          + " as corrupt because datanode " + dn + " (" + dn.getDatanodeUuid()
          + ") does not exist");
    }
    DatanodeStorageInfo storage = null;
    if (storageID != null) {
      storage = node.getStorageInfo(storageID);
    }
    if (storage == null) {
      storage = storedBlock.findStorageInfo(node);
    }

    if (storage == null) {
      blockLog.debug("BLOCK* findAndMarkBlockAsCorrupt: {} not found on {}", blk, dn);
      return;
    }
    markBlockAsCorrupt(new BlockToMarkCorrupt(reportedBlock, storedBlock,
            blk.getGenerationStamp(), reason, Reason.CORRUPTION_REPORTED),
        storage, node);
  }

  /**
   * Mark a replica (of a contiguous block) or an internal block (of a striped
   * block group) as corrupt.
   * @param b Indicating the reported bad block and the corresponding BlockInfo
   *          stored in blocksMap.
   * @param storageInfo storage that contains the block, if known. null otherwise.
   */
  private void markBlockAsCorrupt(BlockToMarkCorrupt b,
      DatanodeStorageInfo storageInfo,
      DatanodeDescriptor node) throws IOException {
    if (b.getStored().isDeleted()) {
      if (blockLog.isDebugEnabled()) {
        blockLog.debug("BLOCK markBlockAsCorrupt: {} cannot be marked as" +
            " corrupt as it does not belong to any file", b);
      }
      addToInvalidates(b.getCorrupted(), node);
      return;
    }
    short expectedRedundancies =
        getExpectedRedundancyNum(b.getStored());

    // Add replica to the data-node if it is not already there
    if (storageInfo != null) {
      storageInfo.addBlock(b.getStored(), b.getCorrupted());
    }

    // Add this replica to corruptReplicas Map. For striped blocks, we always
    // use the id of whole striped block group when adding to corruptReplicas
    Block corrupted = new Block(b.getCorrupted());
    if (b.getStored().isStriped()) {
      corrupted.setBlockId(b.getStored().getBlockId());
    }
    corruptReplicas.addToCorruptReplicasMap(corrupted, node, b.getReason(),
        b.getReasonCode(), b.getStored().isStriped());

    NumberReplicas numberOfReplicas = countNodes(b.getStored());
    final int numUsableReplicas = numberOfReplicas.liveReplicas() +
        numberOfReplicas.decommissioning() +
        numberOfReplicas.liveEnteringMaintenanceReplicas();
    boolean hasEnoughLiveReplicas = numUsableReplicas >=
        expectedRedundancies;

    boolean minReplicationSatisfied = hasMinStorage(b.getStored(),
        numUsableReplicas);

    boolean hasMoreCorruptReplicas = minReplicationSatisfied &&
        (numberOfReplicas.liveReplicas() + numberOfReplicas.corruptReplicas()) >
        expectedRedundancies;
    boolean corruptedDuringWrite = minReplicationSatisfied &&
        b.isCorruptedDuringWrite();
    // case 1: have enough number of usable replicas
    // case 2: corrupted replicas + usable replicas > Replication factor
    // case 3: Block is marked corrupt due to failure while writing. In this
    //         case genstamp will be different than that of valid block.
    // In all these cases we can delete the replica.
    // In case 3, rbw block will be deleted and valid block can be replicated.
    // Note NN only becomes aware of corrupt blocks when the block report is sent,
    // this means that by default it can take up to 6 hours for a corrupt block to
    // be invalidated, after which the valid block can be replicated.
    if (hasEnoughLiveReplicas || hasMoreCorruptReplicas
        || corruptedDuringWrite) {
      if (b.getStored().isStriped()) {
        // If the block is an EC block, the whole block group is marked
        // corrupted, so if this block is getting deleted, remove the block
        // from corrupt replica map explicitly, since removal of the
        // block from corrupt replicas may be delayed if the blocks are on
        // stale storage due to failover or any other reason.
        corruptReplicas.removeFromCorruptReplicasMap(b.getStored(), node);
        BlockInfoStriped blk = (BlockInfoStriped) getStoredBlock(b.getStored());
        storageInfo.removeBlock(blk);
      }
      // the block is over-replicated so invalidate the replicas immediately
      invalidateBlock(b, node, numberOfReplicas);
    } else if (isPopulatingReplQueues()) {
      // add the block to neededReconstruction
      updateNeededReconstructions(b.getStored(), -1, 0);
    }
  }

  /**
   * Invalidates the given block on the given datanode.
   * @return true if the block was successfully invalidated and no longer
   * present in the BlocksMap
   */
  private boolean invalidateBlock(BlockToMarkCorrupt b, DatanodeInfo dn,
      NumberReplicas nr) throws IOException {
    blockLog.debug("BLOCK* invalidateBlock: {} on {}", b, dn);
    DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot invalidate " + b
          + " because datanode " + dn + " does not exist.");
    }

    // Check how many copies we have of the block
    if (nr.replicasOnStaleNodes() > 0 && !deleteCorruptReplicaImmediately) {
      if (blockLog.isDebugEnabled()) {
        blockLog.debug("BLOCK* invalidateBlocks: postponing " +
            "invalidation of {} on {} because {} replica(s) are located on " +
            "nodes with potentially out-of-date block reports", b, dn,
            nr.replicasOnStaleNodes());
      }
      postponeBlock(b.getCorrupted());
      return false;
    } else {
      // we already checked the number of replicas in the caller of this
      // function and know there are enough live replicas, so we can delete it.
      addToInvalidates(b.getCorrupted(), dn);
      removeStoredBlock(b.getStored(), node);
      blockLog.debug("BLOCK* invalidateBlocks: {} on {} listed for deletion.", b, dn);
      return true;
    }
  }


  public void setPostponeBlocksFromFuture(boolean postpone) {
    this.shouldPostponeBlocksFromFuture  = postpone;
  }

  @VisibleForTesting
  void postponeBlock(Block blk) {
    postponedMisreplicatedBlocks.add(blk);
  }
  
  
  void updateState() {
    pendingReconstructionBlocksCount = pendingReconstruction.size();
    lowRedundancyBlocksCount = neededReconstruction.size();
    corruptReplicaBlocksCount = corruptReplicas.size();
  }

  /** Return number of low redundancy blocks but not missing blocks. */
  public int getUnderReplicatedNotMissingBlocks() {
    return neededReconstruction.getLowRedundancyBlockCount();
  }
  
  /**
   * Schedule blocks for deletion at datanodes
   * @param nodesToProcess number of datanodes to schedule deletion work
   * @return total number of block for deletion
   */
  int computeInvalidateWork(int nodesToProcess) {
    final List<DatanodeInfo> nodes = invalidateBlocks.getDatanodes();
    Collections.shuffle(nodes);

    nodesToProcess = Math.min(nodes.size(), nodesToProcess);

    int blockCnt = 0;
    for (DatanodeInfo dnInfo : nodes) {
      int blocks = invalidateWorkForOneNode(dnInfo);
      if (blocks > 0) {
        blockCnt += blocks;
        if (--nodesToProcess == 0) {
          break;
        }
      }
    }
    return blockCnt;
  }

  /**
   * Scan blocks in {@link #neededReconstruction} and assign reconstruction
   * (replication or erasure coding) work to data-nodes they belong to.
   *
   * The number of process blocks equals either twice the number of live
   * data-nodes or the number of low redundancy blocks whichever is less.
   *
   * @return number of blocks scheduled for reconstruction during this
   *         iteration.
   */
  int computeBlockReconstructionWork(int blocksToProcess) {
    List<List<BlockInfo>> blocksToReconstruct = null;
    namesystem.writeLock();
    try {
      boolean reset = false;
      if (replQueueResetToHeadThreshold > 0) {
        if (replQueueCallsSinceReset >= replQueueResetToHeadThreshold) {
          reset = true;
          replQueueCallsSinceReset = 0;
        } else {
          replQueueCallsSinceReset++;
        }
      }
        // Choose the blocks to be reconstructed
      blocksToReconstruct = neededReconstruction
          .chooseLowRedundancyBlocks(blocksToProcess, reset);
    } finally {
      namesystem.writeUnlock("computeBlockReconstructionWork");
    }
    return computeReconstructionWorkForBlocks(blocksToReconstruct);
  }

  /**
   * Reconstruct a set of blocks to full strength through replication or
   * erasure coding
   *
   * @param blocksToReconstruct blocks to be reconstructed, for each priority
   * @return the number of blocks scheduled for replication
   */
  @VisibleForTesting
  int computeReconstructionWorkForBlocks(
      List<List<BlockInfo>> blocksToReconstruct) {
    int scheduledWork = 0;
    List<BlockReconstructionWork> reconWork = new ArrayList<>();

    // Step 1: categorize at-risk blocks into replication and EC tasks
    namesystem.writeLock();
    try {
      synchronized (neededReconstruction) {
        for (int priority = 0; priority < blocksToReconstruct
            .size(); priority++) {
          for (BlockInfo block : blocksToReconstruct.get(priority)) {
            BlockReconstructionWork rw = scheduleReconstruction(block,
                priority);
            if (rw != null) {
              reconWork.add(rw);
            }
          }
        }
      }
    } finally {
      namesystem.writeUnlock("computeReconstructionWorkForBlocks");
    }

    // Step 2: choose target nodes for each reconstruction task
    for (BlockReconstructionWork rw : reconWork) {
      // Exclude all of the containing nodes from being targets.
      // This list includes decommissioning or corrupt nodes.
      final Set<Node> excludedNodes = new HashSet<>(rw.getContainingNodes());

      // Exclude all nodes which already exists as targets for the block
      List<DatanodeStorageInfo> targets =
          pendingReconstruction.getTargets(rw.getBlock());
      if (targets != null) {
        for (DatanodeStorageInfo dn : targets) {
          excludedNodes.add(dn.getDatanodeDescriptor());
        }
      }

      // choose replication targets: NOT HOLDING THE GLOBAL LOCK
      final BlockPlacementPolicy placementPolicy =
          placementPolicies.getPolicy(rw.getBlock().getBlockType());
      rw.chooseTargets(placementPolicy, storagePolicySuite, excludedNodes);
    }

    // Step 3: add tasks to the DN
    namesystem.writeLock();
    try {
      for (BlockReconstructionWork rw : reconWork) {
        final DatanodeStorageInfo[] targets = rw.getTargets();
        if (targets == null || targets.length == 0) {
          rw.resetTargets();
          continue;
        }

        synchronized (neededReconstruction) {
          if (validateReconstructionWork(rw)) {
            scheduledWork++;
          }
        }
      }
    } finally {
      namesystem.writeUnlock("computeReconstructionWorkForBlocks");
    }

    if (blockLog.isDebugEnabled()) {
      // log which blocks have been scheduled for reconstruction
      for (BlockReconstructionWork rw : reconWork) {
        DatanodeStorageInfo[] targets = rw.getTargets();
        if (targets != null && targets.length != 0) {
          StringBuilder targetList = new StringBuilder("datanode(s)");
          for (DatanodeStorageInfo target : targets) {
            targetList.append(' ').append(target.getDatanodeDescriptor());
          }
          blockLog.debug("BLOCK* ask {} to replicate {} to {}",
              rw.getSrcNodes(), rw.getBlock(), targetList);
        }
      }
      blockLog.debug("BLOCK* neededReconstruction = {} pendingReconstruction = {}",
          neededReconstruction.size(), pendingReconstruction.size());
    }

    return scheduledWork;
  }

  // Check if the number of live + pending replicas satisfies
  // the expected redundancy.
  boolean hasEnoughEffectiveReplicas(BlockInfo block,
      NumberReplicas numReplicas, int pendingReplicaNum) {
    int required = getExpectedLiveRedundancyNum(block, numReplicas);
    int numEffectiveReplicas = numReplicas.liveReplicas() + pendingReplicaNum;
    return (numEffectiveReplicas >= required) &&
        (pendingReplicaNum > 0 || isPlacementPolicySatisfied(block));
  }

  @VisibleForTesting
  BlockReconstructionWork scheduleReconstruction(BlockInfo block,
      int priority) {
    // skip abandoned block or block reopened for append
    if (block.isDeleted() || !block.isCompleteOrCommitted()) {
      // remove from neededReconstruction
      neededReconstruction.remove(block, priority);
      return null;
    }

    // get a source data-node
    List<DatanodeDescriptor> containingNodes = new ArrayList<>();
    List<DatanodeStorageInfo> liveReplicaNodes = new ArrayList<>();
    NumberReplicas numReplicas = new NumberReplicas();
    List<Byte> liveBlockIndices = new ArrayList<>();
    List<Byte> liveBusyBlockIndices = new ArrayList<>();
    List<Byte> excludeReconstructed = new ArrayList<>();
    final DatanodeDescriptor[] srcNodes = chooseSourceDatanodes(block,
        containingNodes, liveReplicaNodes, numReplicas,
        liveBlockIndices, liveBusyBlockIndices, excludeReconstructed, priority);
    short requiredRedundancy = getExpectedLiveRedundancyNum(block,
        numReplicas);
    if (srcNodes == null || srcNodes.length == 0) {
      // block can not be reconstructed from any node
      LOG.debug("Block {} cannot be reconstructed from any node", block);
      NameNode.getNameNodeMetrics().incNumTimesReReplicationNotScheduled();
      return null;
    }

    // skip if source datanodes for reconstructing ec block are not enough
    if (block.isStriped()) {
      BlockInfoStriped stripedBlock = (BlockInfoStriped) block;
      if (stripedBlock.getRealDataBlockNum() > srcNodes.length) {
        LOG.debug("Block {} cannot be reconstructed due to shortage of source datanodes ", block);
        NameNode.getNameNodeMetrics().incNumTimesReReplicationNotScheduled();
        return null;
      }
    }

    // liveReplicaNodes can include READ_ONLY_SHARED replicas which are
    // not included in the numReplicas.liveReplicas() count
    assert liveReplicaNodes.size() >= numReplicas.liveReplicas();

    int pendingNum = pendingReconstruction.getNumReplicas(block);
    if (hasEnoughEffectiveReplicas(block, numReplicas, pendingNum)) {
      neededReconstruction.remove(block, priority);
      blockLog.debug("BLOCK* Removing {} from neededReconstruction as it has enough replicas",
          block);
      NameNode.getNameNodeMetrics().incNumTimesReReplicationNotScheduled();
      return null;
    }

    int additionalReplRequired;
    if (numReplicas.liveReplicas() < requiredRedundancy) {
      additionalReplRequired = requiredRedundancy - numReplicas.liveReplicas()
          - pendingNum;
    } else {
      // Violates placement policy. Needed on a new rack or domain etc.
      BlockPlacementStatus placementStatus = getBlockPlacementStatus(block);
      additionalReplRequired = placementStatus.getAdditionalReplicasRequired();
    }

    final BlockCollection bc = getBlockCollection(block);
    if (block.isStriped()) {
      if (pendingNum > 0) {
        // Wait the previous reconstruction to finish.
        NameNode.getNameNodeMetrics().incNumTimesReReplicationNotScheduled();
        return null;
      }

      // should reconstruct all the internal blocks before scheduling
      // replication task for decommissioning node(s).
      if (additionalReplRequired - numReplicas.decommissioning() -
          numReplicas.liveEnteringMaintenanceReplicas() > 0) {
        additionalReplRequired = additionalReplRequired -
            numReplicas.decommissioning() -
            numReplicas.liveEnteringMaintenanceReplicas();
      }
      final DatanodeDescriptor[] newSrcNodes =
          new DatanodeDescriptor[srcNodes.length];
      byte[] newIndices = new byte[liveBlockIndices.size()];
      adjustSrcNodesAndIndices((BlockInfoStriped)block,
          srcNodes, liveBlockIndices, newSrcNodes, newIndices);
      byte[] busyIndices = new byte[liveBusyBlockIndices.size()];
      for (int i = 0; i < liveBusyBlockIndices.size(); i++) {
        busyIndices[i] = liveBusyBlockIndices.get(i);
      }
      byte[] excludeReconstructedIndices = new byte[excludeReconstructed.size()];
      for (int i = 0; i < excludeReconstructed.size(); i++) {
        excludeReconstructedIndices[i] = excludeReconstructed.get(i);
      }
      return new ErasureCodingWork(getBlockPoolId(), block, bc, newSrcNodes,
          containingNodes, liveReplicaNodes, additionalReplRequired,
          priority, newIndices, busyIndices, excludeReconstructedIndices);
    } else {
      return new ReplicationWork(block, bc, srcNodes,
          containingNodes, liveReplicaNodes, additionalReplRequired,
          priority);
    }
  }

  /**
   * Adjust srcNodes and indices which are used to reconstruction block.
   * We should guarantee the indexes of first minRequiredSources nodes
   + are different.
   */
  private void adjustSrcNodesAndIndices(BlockInfoStriped block,
      DatanodeDescriptor[] srcNodes, List<Byte> indices,
      DatanodeDescriptor[] newSrcNodes, byte[] newIndices) {
    BitSet bitSet = new BitSet(block.getRealTotalBlockNum());
    List<Integer> skipIndexList = new ArrayList<>();
    for (int i = 0, j = 0; i < srcNodes.length; i++) {
      if (!bitSet.get(indices.get(i))) {
        bitSet.set(indices.get(i));
        newSrcNodes[j] = srcNodes[i];
        newIndices[j++] = indices.get(i);
      } else {
        skipIndexList.add(i);
      }
    }
    for(int i = srcNodes.length - skipIndexList.size(), j = 0;
        i < srcNodes.length; i++, j++) {
      newSrcNodes[i] = srcNodes[skipIndexList.get(j)];
      newIndices[i] = indices.get(skipIndexList.get(j));
    }
  }

  @VisibleForTesting
  boolean validateReconstructionWork(BlockReconstructionWork rw) {
    BlockInfo block = rw.getBlock();
    int priority = rw.getPriority();
    // Recheck since global lock was released
    // skip abandoned block or block reopened for append
    if (block.isDeleted() || !block.isCompleteOrCommitted()) {
      neededReconstruction.remove(block, priority);
      rw.resetTargets();
      return false;
    }

    // do not schedule more if enough replicas is already pending
    NumberReplicas numReplicas = countNodes(block);
    final short requiredRedundancy =
        getExpectedLiveRedundancyNum(block, numReplicas);
    final int pendingNum = pendingReconstruction.getNumReplicas(block);
    if (hasEnoughEffectiveReplicas(block, numReplicas, pendingNum)) {
      neededReconstruction.remove(block, priority);
      rw.resetTargets();
      blockLog.debug("BLOCK* Removing {} from neededReconstruction as it has enough replicas",
          block);
      return false;
    }

    DatanodeStorageInfo[] targets = rw.getTargets();
    BlockPlacementStatus placementStatus = getBlockPlacementStatus(block);
    if ((numReplicas.liveReplicas() >= requiredRedundancy) &&
        (!placementStatus.isPlacementPolicySatisfied())) {
      BlockPlacementStatus newPlacementStatus =
          getBlockPlacementStatus(block, targets);
      if (!newPlacementStatus.isPlacementPolicySatisfied() &&
          (newPlacementStatus.getAdditionalReplicasRequired() >=
              placementStatus.getAdditionalReplicasRequired())) {
        // If the new targets do not meet the placement policy, or at least
        // reduce the number of replicas needed, then no use continuing.
        rw.resetTargets();
        return false;
      }
      // mark that the reconstruction work is to replicate internal block to a
      // new rack.
      rw.setNotEnoughRack();
    }

    // Add block to the datanode's task list
    rw.addTaskToDatanode(numReplicas);
    DatanodeStorageInfo.incrementBlocksScheduled(targets);

    // Move the block-replication into a "pending" state.
    // The reason we use 'pending' is so we can retry
    // reconstructions that fail after an appropriate amount of time.
    pendingReconstruction.increment(block, targets);
    blockLog.debug("BLOCK* block {} is moved from neededReconstruction to pendingReconstruction",
        block);

    int numEffectiveReplicas = numReplicas.liveReplicas() + pendingNum;
    // remove from neededReconstruction
    if(numEffectiveReplicas + targets.length >= requiredRedundancy) {
      neededReconstruction.remove(block, priority);
    }
    return true;
  }

  /** Choose target for WebHDFS redirection. */
  public DatanodeStorageInfo[] chooseTarget4WebHDFS(String src,
      DatanodeDescriptor clientnode, Set<Node> excludes, long blocksize) {
    return placementPolicies.getPolicy(CONTIGUOUS).chooseTarget(src, 1,
        clientnode, Collections.<DatanodeStorageInfo>emptyList(), false,
        excludes, blocksize, storagePolicySuite.getDefaultPolicy(), null);
  }

  /** Choose target for getting additional datanodes for an existing pipeline. */
  public DatanodeStorageInfo[] chooseTarget4AdditionalDatanode(String src,
      int numAdditionalNodes,
      Node clientnode,
      List<DatanodeStorageInfo> chosen,
      Set<Node> excludes,
      long blocksize,
      byte storagePolicyID,
      BlockType blockType) {
    final BlockStoragePolicy storagePolicy =
        storagePolicySuite.getPolicy(storagePolicyID);
    final BlockPlacementPolicy blockplacement =
        placementPolicies.getPolicy(blockType);
    return blockplacement.chooseTarget(src, numAdditionalNodes, clientnode,
        chosen, true, excludes, blocksize, storagePolicy, null);
  }

  /**
   * Choose target datanodes for creating a new block.
   * 
   * @throws IOException
   *           if the number of targets {@literal <} minimum replication.
   * @see BlockPlacementPolicy#chooseTarget(String, int, Node,
   *      Set, long, List, BlockStoragePolicy, EnumSet)
   */
  public DatanodeStorageInfo[] chooseTarget4NewBlock(final String src,
      final int numOfReplicas, final Node client,
      final Set<Node> excludedNodes,
      final long blocksize,
      final List<String> favoredNodes,
      final byte storagePolicyID,
      final BlockType blockType,
      final ErasureCodingPolicy ecPolicy,
      final EnumSet<AddBlockFlag> flags) throws IOException {
    List<DatanodeDescriptor> favoredDatanodeDescriptors = 
        getDatanodeDescriptors(favoredNodes);
    final BlockStoragePolicy storagePolicy =
        storagePolicySuite.getPolicy(storagePolicyID);
    final BlockPlacementPolicy blockplacement =
        placementPolicies.getPolicy(blockType);
    final DatanodeStorageInfo[] targets = blockplacement.chooseTarget(src,
        numOfReplicas, client, excludedNodes, blocksize, 
        favoredDatanodeDescriptors, storagePolicy, flags);

    final String errorMessage = "File %s could only be written to %d of " +
        "the %d %s. There are %d datanode(s) running and %s "
        + "node(s) are excluded in this operation.";
    if (blockType == BlockType.CONTIGUOUS && targets.length < minReplication) {
      throw new IOException(String.format(errorMessage, src,
          targets.length, minReplication, "minReplication nodes",
          getDatanodeManager().getNetworkTopology().getNumOfLeaves(),
          (excludedNodes == null? "no": excludedNodes.size())));
    } else if (blockType == BlockType.STRIPED &&
        targets.length < ecPolicy.getNumDataUnits()) {
      throw new IOException(
          String.format(errorMessage, src, targets.length,
              ecPolicy.getNumDataUnits(),
              String.format("required nodes for %s", ecPolicy.getName()),
              getDatanodeManager().getNetworkTopology().getNumOfLeaves(),
              (excludedNodes == null ? "no" : excludedNodes.size())));
    }
    return targets;
  }

  /**
   * Get list of datanode descriptors for given list of nodes. Nodes are
   * hostaddress:port or just hostaddress.
   */
  List<DatanodeDescriptor> getDatanodeDescriptors(List<String> nodes) {
    List<DatanodeDescriptor> datanodeDescriptors = null;
    if (nodes != null) {
      datanodeDescriptors = new ArrayList<DatanodeDescriptor>(nodes.size());
      for (int i = 0; i < nodes.size(); i++) {
        DatanodeDescriptor node = datanodeManager.getDatanodeDescriptor(nodes.get(i));
        if (node != null) {
          datanodeDescriptors.add(node);
        }
      }
    }
    return datanodeDescriptors;
  }

  /**
   * Get the associated {@link DatanodeDescriptor} for the storage.
   * If the storage is of type PROVIDED, one of the nodes that reported
   * PROVIDED storage are returned. If not, this is equivalent to
   * {@code storage.getDatanodeDescriptor()}.
   * @param storage
   * @return the associated {@link DatanodeDescriptor}.
   */
  private DatanodeDescriptor getDatanodeDescriptorFromStorage(
      DatanodeStorageInfo storage) {
    if (storage.getStorageType() == StorageType.PROVIDED) {
      return providedStorageMap.chooseProvidedDatanode();
    }
    return storage.getDatanodeDescriptor();
  }

  /**
   * Parse the data-nodes the block belongs to and choose a certain number
   * from them to be the recovery sources.
   *
   * We prefer nodes that are in DECOMMISSION_INPROGRESS state to other nodes
   * since the former do not have write traffic and hence are less busy.
   * We do not use already decommissioned nodes as a source, unless there is
   * no other choice.
   * Otherwise we randomly choose nodes among those that did not reach their
   * replication limits. However, if the recovery work is of the highest
   * priority and all nodes have reached their replication limits, we will
   * randomly choose the desired number of nodes despite the replication limit.
   *
   * In addition form a list of all nodes containing the block
   * and calculate its replication numbers.
   *
   * @param block Block for which a replication source is needed
   * @param containingNodes List to be populated with nodes found to contain
   *                        the given block
   * @param nodesContainingLiveReplicas List to be populated with nodes found
   *                                    to contain live replicas of the given
   *                                    block
   * @param numReplicas NumberReplicas instance to be initialized with the
   *                    counts of live, corrupt, excess, and decommissioned
   *                    replicas of the given block.
   * @param liveBlockIndices List to be populated with indices of healthy
   *                         blocks in a striped block group
   * @param liveBusyBlockIndices List to be populated with indices of healthy
   *                             blocks in a striped block group in busy DN,
   *                             which the recovery work have reached their
   *                             replication limits
   * @param priority integer representing replication priority of the given
   *                 block
   * @return the array of DatanodeDescriptor of the chosen nodes from which to
   *         recover the given block
   */
  @VisibleForTesting
  DatanodeDescriptor[] chooseSourceDatanodes(BlockInfo block,
      List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> nodesContainingLiveReplicas,
      NumberReplicas numReplicas, List<Byte> liveBlockIndices,
      List<Byte> liveBusyBlockIndices, List<Byte> excludeReconstructed, int priority) {
    containingNodes.clear();
    nodesContainingLiveReplicas.clear();
    List<DatanodeDescriptor> srcNodes = new ArrayList<>();
    liveBlockIndices.clear();
    final boolean isStriped = block.isStriped();
    DatanodeDescriptor decommissionedSrc = null;

    BitSet liveBitSet = null;
    BitSet decommissioningBitSet = null;
    if (isStriped) {
      int blockNum = ((BlockInfoStriped) block).getTotalBlockNum();
      liveBitSet = new BitSet(blockNum);
      decommissioningBitSet = new BitSet(blockNum);
    }

    for (DatanodeStorageInfo storage : blocksMap.getStorages(block)) {
      final DatanodeDescriptor node = getDatanodeDescriptorFromStorage(storage);
      final StoredReplicaState state = checkReplicaOnStorage(numReplicas, block,
          storage, corruptReplicas.getNodes(block), false);
      if (state == StoredReplicaState.LIVE) {
        if (storage.getStorageType() == StorageType.PROVIDED) {
          storage = new DatanodeStorageInfo(node, storage.getStorageID(),
              storage.getStorageType(), storage.getState());
        }
        nodesContainingLiveReplicas.add(storage);
      }
      containingNodes.add(node);

      // do not select the replica if it is corrupt or excess
      if (state == StoredReplicaState.CORRUPT ||
          state == StoredReplicaState.EXCESS) {
        continue;
      }

      // Never use maintenance node not suitable for read
      // or unknown state replicas.
      if (state == null
          || state == StoredReplicaState.MAINTENANCE_NOT_FOR_READ) {
        continue;
      }

      // Save the live decommissioned replica in case we need it. Such replicas
      // are normally not used for replication, but if nothing else is
      // available, one can be selected as a source.
      if (state == StoredReplicaState.DECOMMISSIONED) {
        if (decommissionedSrc == null ||
            ThreadLocalRandom.current().nextBoolean()) {
          decommissionedSrc = node;
        }
        continue;
      }

      // for EC here need to make sure the numReplicas replicates state correct
      // because in the scheduleReconstruction it need the numReplicas to check
      // whether need to reconstruct the ec internal block
      byte blockIndex = -1;
      if (isStriped) {
        blockIndex = ((BlockInfoStriped) block)
            .getStorageBlockIndex(storage);
        countLiveAndDecommissioningReplicas(numReplicas, state,
            liveBitSet, decommissioningBitSet, blockIndex);
      }

      if (priority != LowRedundancyBlocks.QUEUE_HIGHEST_PRIORITY
          && (!node.isDecommissionInProgress() && !node.isEnteringMaintenance())
          && node.getNumberOfBlocksToBeReplicated() +
          node.getNumberOfBlocksToBeErasureCoded() >= maxReplicationStreams) {
        if (isStriped && (state == StoredReplicaState.LIVE
            || state == StoredReplicaState.DECOMMISSIONING)) {
          liveBusyBlockIndices.add(blockIndex);
          //HDFS-16566 ExcludeReconstructed won't be reconstructed.
          excludeReconstructed.add(blockIndex);
        }
        continue; // already reached replication limit
      }

      if (node.getNumberOfBlocksToBeReplicated() +
          node.getNumberOfBlocksToBeErasureCoded() >= replicationStreamsHardLimit) {
        if (isStriped && (state == StoredReplicaState.LIVE
            || state == StoredReplicaState.DECOMMISSIONING)) {
          liveBusyBlockIndices.add(blockIndex);
          //HDFS-16566 ExcludeReconstructed won't be reconstructed.
          excludeReconstructed.add(blockIndex);
        }
        continue;
      }

      if(isStriped || srcNodes.isEmpty()) {
        srcNodes.add(node);
        if (isStriped) {
          liveBlockIndices.add(blockIndex);
        }
        continue;
      }
      // for replicated block, switch to a different node randomly
      // this to prevent from deterministically selecting the same node even
      // if the node failed to replicate the block on previous iterations
      if (ThreadLocalRandom.current().nextBoolean()) {
        srcNodes.set(0, node);
      }
    }

    // Pick a live decommissioned replica, if nothing else is available.
    if (!isStriped && nodesContainingLiveReplicas.isEmpty() &&
        srcNodes.isEmpty() && decommissionedSrc != null) {
      srcNodes.add(decommissionedSrc);
    }

    return srcNodes.toArray(new DatanodeDescriptor[srcNodes.size()]);
  }

  /**
   * If there were any reconstruction requests that timed out, reap them
   * and put them back into the neededReconstruction queue
   */
  void processPendingReconstructions() {
    BlockInfo[] timedOutItems = pendingReconstruction.getTimedOutBlocks();
    if (timedOutItems != null) {
      namesystem.writeLock();
      try {
        for (int i = 0; i < timedOutItems.length; i++) {
          /*
           * Use the blockinfo from the blocksmap to be certain we're working
           * with the most up-to-date block information (e.g. genstamp).
           */
          BlockInfo bi = blocksMap.getStoredBlock(timedOutItems[i]);
          if (bi == null || bi.isDeleted()) {
            continue;
          }
          NumberReplicas num = countNodes(timedOutItems[i]);
          if (isNeededReconstruction(bi, num)) {
            neededReconstruction.add(bi, num.liveReplicas(),
                num.readOnlyReplicas(), num.outOfServiceReplicas(),
                getExpectedRedundancyNum(bi));
          }
        }
      } finally {
        namesystem.writeUnlock("processPendingReconstructions");
      }
      /* If we know the target datanodes where the replication timedout,
       * we could invoke decBlocksScheduled() on it. Its ok for now.
       */
    }
  }

  public long requestBlockReportLeaseId(DatanodeRegistration nodeReg) {
    assert namesystem.hasReadLock();
    DatanodeDescriptor node = null;
    try {
      node = datanodeManager.getDatanode(nodeReg);
    } catch (UnregisteredNodeException e) {
      LOG.warn("Unregistered datanode {}", nodeReg);
      return 0;
    }
    if (node == null) {
      LOG.warn("Failed to find datanode {}", nodeReg);
      return 0;
    }
    // Request a new block report lease.  The BlockReportLeaseManager has
    // its own internal locking.
    long leaseId = blockReportLeaseManager.requestLease(node);
    BlockManagerFaultInjector.getInstance().
        requestBlockReportLease(node, leaseId);
    return leaseId;
  }

  public void registerDatanode(DatanodeRegistration nodeReg)
      throws IOException {
    assert namesystem.hasWriteLock();
    datanodeManager.registerDatanode(nodeReg);
    bmSafeMode.checkSafeMode();
  }

  /**
   * Set the total number of blocks in the system.
   * If safe mode is not currently on, this is a no-op.
   */
  public void setBlockTotal(long total) {
    if (bmSafeMode.isInSafeMode()) {
      bmSafeMode.setBlockTotal(total);
      bmSafeMode.checkSafeMode();
    }
  }

  public boolean isInSafeMode() {
    return bmSafeMode.isInSafeMode();
  }

  public String getSafeModeTip() {
    return bmSafeMode.getSafeModeTip();
  }

  public boolean leaveSafeMode(boolean force) {
    return bmSafeMode.leaveSafeMode(force);
  }

  public void checkSafeMode() {
    bmSafeMode.checkSafeMode();
  }

  public long getBytesInFuture() {
    return bmSafeMode.getBytesInFuture();
  }

  public long getBytesInFutureReplicatedBlocks() {
    return bmSafeMode.getBytesInFutureBlocks();
  }

  public long getBytesInFutureECBlockGroups() {
    return bmSafeMode.getBytesInFutureECBlockGroups();
  }

  /**
   * Removes the blocks from blocksmap and updates the safemode blocks total.
   * @param blocks An instance of {@link BlocksMapUpdateInfo} which contains a
   *               list of blocks that need to be removed from blocksMap
   */
  public void removeBlocksAndUpdateSafemodeTotal(BlocksMapUpdateInfo blocks) {
    assert namesystem.hasWriteLock();
    // In the case that we are a Standby tailing edits from the
    // active while in safe-mode, we need to track the total number
    // of blocks and safe blocks in the system.
    boolean trackBlockCounts = bmSafeMode.isSafeModeTrackingBlocks();
    int numRemovedComplete = 0, numRemovedSafe = 0;

    for (BlockInfo b : blocks.getToDeleteList()) {
      if (trackBlockCounts) {
        if (b.isComplete()) {
          numRemovedComplete++;
          if (hasMinStorage(b, b.numNodes())) {
            numRemovedSafe++;
          }
        }
      }
      removeBlock(b);
    }
    if (trackBlockCounts) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adjusting safe-mode totals for deletion."
                + "decreasing safeBlocks by {}, totalBlocks by {}",
            numRemovedSafe, numRemovedComplete);
      }
      bmSafeMode.adjustBlockTotals(-numRemovedSafe, -numRemovedComplete);
    }
  }

  public long getProvidedCapacity() {
    return providedStorageMap.getCapacity();
  }

  void updateHeartbeat(DatanodeDescriptor node, StorageReport[] reports,
      long cacheCapacity, long cacheUsed, int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) {
    BlockManagerFaultInjector.getInstance().mockAnException();
    for (StorageReport report: reports) {
      providedStorageMap.updateStorage(node, report.getStorage());
    }
    node.updateHeartbeat(reports, cacheCapacity, cacheUsed, xceiverCount,
        failedVolumes, volumeFailureSummary);
  }

  void updateHeartbeatState(DatanodeDescriptor node,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) {
    BlockManagerFaultInjector.getInstance().mockAnException();
    for (StorageReport report: reports) {
      providedStorageMap.updateStorage(node, report.getStorage());
    }
    node.updateHeartbeatState(reports, cacheCapacity, cacheUsed, xceiverCount,
        failedVolumes, volumeFailureSummary);
  }

  /**
   * StatefulBlockInfo is used to build the "toUC" list, which is a list of
   * updates to the information about under-construction blocks.
   * Besides the block in question, it provides the ReplicaState
   * reported by the datanode in the block report. 
   */
  static class StatefulBlockInfo {
    final BlockInfo storedBlock; // should be UC block
    final Block reportedBlock;
    final ReplicaState reportedState;
    
    StatefulBlockInfo(BlockInfo storedBlock,
        Block reportedBlock, ReplicaState reportedState) {
      Preconditions.checkArgument(!storedBlock.isComplete());
      this.storedBlock = storedBlock;
      this.reportedBlock = reportedBlock;
      this.reportedState = reportedState;
    }
  }

  private static class BlockInfoToAdd {
    final BlockInfo stored;
    final Block reported;

    BlockInfoToAdd(BlockInfo stored, Block reported) {
      this.stored = stored;
      this.reported = reported;
    }
  }

  /**
   * Check block report lease.
   * @return true if lease exist and not expire
   */
  public boolean checkBlockReportLease(BlockReportContext context,
      final DatanodeID nodeID) throws UnregisteredNodeException {
    if (context == null) {
      return true;
    }
    DatanodeDescriptor node = datanodeManager.getDatanode(nodeID);
    if (node == null) {
      throw new UnregisteredNodeException(nodeID, null);
    }
    final long startTime = Time.monotonicNow();
    return blockReportLeaseManager.checkLease(node, startTime,
        context.getLeaseId());
  }

  /**
   * The given storage is reporting all its blocks.
   * Update the (storage{@literal -->}block list) and
   * (block{@literal -->}storage list) maps.
   *
   * @return true if all known storages of the given DN have finished reporting.
   * @throws IOException
   */
  public boolean processReport(final DatanodeID nodeID,
      final DatanodeStorage storage,
      final BlockListAsLongs newReport,
      BlockReportContext context) throws IOException {
    namesystem.writeLock();
    final long startTime = Time.monotonicNow(); //after acquiring write lock
    final long endTime;
    DatanodeDescriptor node;
    Collection<Block> invalidatedBlocks = Collections.emptyList();
    String strBlockReportId =
        context != null ? Long.toHexString(context.getReportId()) : "";
    String fullBrLeaseId =
        context != null ? Long.toHexString(context.getLeaseId()) : "";

    try {
      node = datanodeManager.getDatanode(nodeID);
      if (node == null || !node.isRegistered()) {
        throw new IOException(
            "ProcessReport from dead or unregistered node: " + nodeID);
      }

      // To minimize startup time, we discard any second (or later) block reports
      // that we receive while still in startup phase.
      // Register DN with provided storage, not with storage owned by DN
      // DN should still have a ref to the DNStorageInfo.
      DatanodeStorageInfo storageInfo =
          providedStorageMap.getStorage(node, storage);

      if (storageInfo == null) {
        // We handle this for backwards compatibility.
        storageInfo = node.updateStorage(storage);
      }
      if (namesystem.isInStartupSafeMode()
          && !StorageType.PROVIDED.equals(storageInfo.getStorageType())
          && storageInfo.getBlockReportCount() > 0) {
        blockLog.info("BLOCK* processReport 0x{} with lease ID 0x{}: "
            + "discarded non-initial block report from {}"
            + " because namenode still in startup phase",
            strBlockReportId, fullBrLeaseId, nodeID);
        removeDNLeaseIfNeeded(node);
        return !node.hasStaleStorages();
      }

      if (!storageInfo.hasReceivedBlockReport()) {
        // The first block report can be processed a lot more efficiently than
        // ordinary block reports.  This shortens restart times.
        blockLog.info("BLOCK* processReport 0x{} with lease ID 0x{}: Processing first "
            + "storage report for {} from datanode {}",
            strBlockReportId, fullBrLeaseId,
            storageInfo.getStorageID(),
            nodeID);
        processFirstBlockReport(storageInfo, newReport);
      } else {
        // Block reports for provided storage are not
        // maintained by DN heartbeats
        if (!StorageType.PROVIDED.equals(storageInfo.getStorageType())) {
          invalidatedBlocks = processReport(storageInfo, newReport);
        }
      }
      storageInfo.receivedBlockReport();
    } finally {
      endTime = Time.monotonicNow();
      namesystem.writeUnlock("processReport");
    }

    if (blockLog.isDebugEnabled()) {
      for (Block b : invalidatedBlocks) {
        blockLog.debug("BLOCK* processReport 0x{} with lease ID 0x{}: {} on node {} size {} " +
                "does not belong to any file.", strBlockReportId, fullBrLeaseId, b,
            node, b.getNumBytes());
      }
    }

    // Log the block report processing stats from Namenode perspective
    final NameNodeMetrics metrics = NameNode.getNameNodeMetrics();
    if (metrics != null) {
      metrics.addStorageBlockReport((int) (endTime - startTime));
    }
    blockLog.info("BLOCK* processReport 0x{} with lease ID 0x{}: from storage {} node {}, " +
        "blocks: {}, hasStaleStorage: {}, processing time: {} msecs, " +
        "invalidatedBlocks: {}", strBlockReportId, fullBrLeaseId, storage.getStorageID(),
        nodeID, newReport.getNumberOfBlocks(),
        node.hasStaleStorages(), (endTime - startTime),
        invalidatedBlocks.size());
    return !node.hasStaleStorages();
  }

  /**
   * Remove the DN lease only when we have received block reports,
   * for all storages for a particular DN.
   */
  void removeDNLeaseIfNeeded(DatanodeDescriptor node) {
    boolean needRemoveLease = true;
    for (DatanodeStorageInfo sInfo : node.getStorageInfos()) {
      if (sInfo.getBlockReportCount() == 0) {
        needRemoveLease = false;
        break;
      }
    }
    if (needRemoveLease) {
      blockReportLeaseManager.removeLease(node);
    }
  }

  public void removeBRLeaseIfNeeded(final DatanodeID nodeID,
      final BlockReportContext context) throws IOException {
    namesystem.writeLock();
    DatanodeDescriptor node;
    try {
      node = datanodeManager.getDatanode(nodeID);
      if (context != null) {
        if (context.getTotalRpcs() == context.getCurRpc() + 1) {
          long leaseId = this.getBlockReportLeaseManager().removeLease(node);
          BlockManagerFaultInjector.getInstance().
              removeBlockReportLease(node, leaseId);
          node.setLastBlockReportTime(now());
          node.setLastBlockReportMonotonic(Time.monotonicNow());
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Processing RPC with index {} out of total {} RPCs in processReport 0x{}",
              context.getCurRpc(), context.getTotalRpcs(), Long.toHexString(context.getReportId()));
        }
      }
    } finally {
      namesystem.writeUnlock("removeBRLeaseIfNeeded");
    }
  }

  /**
   * Rescan the list of blocks which were previously postponed.
   */
  void rescanPostponedMisreplicatedBlocks() {
    if (getPostponedMisreplicatedBlocksCount() == 0) {
      return;
    }
    namesystem.writeLock();
    long startTime = Time.monotonicNow();
    long startSize = postponedMisreplicatedBlocks.size();
    try {
      Iterator<Block> it = postponedMisreplicatedBlocks.iterator();
      for (int i=0; i < blocksPerPostpondedRescan && it.hasNext(); i++) {
        Block b = it.next();
        it.remove();

        BlockInfo bi = getStoredBlock(b);
        if (bi == null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("BLOCK* rescanPostponedMisreplicatedBlocks: " +
                "Postponed mis-replicated block {} no longer found " +
                "in block map.", b);
          }
          continue;
        }
        MisReplicationResult res = processMisReplicatedBlock(bi);
        LOG.debug("BLOCK* rescanPostponedMisreplicatedBlocks: Re-scanned block {}, result is {}",
            b, res);
        if (res == MisReplicationResult.POSTPONE) {
          rescannedMisreplicatedBlocks.add(b);
        }
      }
    } finally {
      postponedMisreplicatedBlocks.addAll(rescannedMisreplicatedBlocks);
      rescannedMisreplicatedBlocks.clear();
      long endSize = postponedMisreplicatedBlocks.size();
      namesystem.writeUnlock("rescanPostponedMisreplicatedBlocks");
      LOG.info("Rescan of postponedMisreplicatedBlocks completed in {}" +
          " msecs. {} blocks are left. {} blocks were removed.",
          (Time.monotonicNow() - startTime), endSize, (startSize - endSize));
    }
  }
  
  Collection<Block> processReport(
      final DatanodeStorageInfo storageInfo,
      final BlockListAsLongs report) throws IOException {
    // Normal case:
    // Modify the (block-->datanode) map, according to the difference
    // between the old and new block report.
    //
    Collection<BlockInfoToAdd> toAdd = new ArrayList<>();
    Collection<BlockInfo> toRemove = new HashSet<>();
    Collection<Block> toInvalidate = new ArrayList<>();
    Collection<BlockToMarkCorrupt> toCorrupt = new ArrayList<>();
    Collection<StatefulBlockInfo> toUC = new ArrayList<>();
    reportDiff(storageInfo, report,
                 toAdd, toRemove, toInvalidate, toCorrupt, toUC);

    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    // Process the blocks on each queue
    for (StatefulBlockInfo b : toUC) { 
      addStoredBlockUnderConstruction(b, storageInfo);
    }
    for (BlockInfo b : toRemove) {
      removeStoredBlock(b, node);
    }
    int numBlocksLogged = 0;
    for (BlockInfoToAdd b : toAdd) {
      addStoredBlock(b.stored, b.reported, storageInfo, null,
          numBlocksLogged < maxNumBlocksToLog);
      numBlocksLogged++;
    }
    if (numBlocksLogged > maxNumBlocksToLog) {
      blockLog.info("BLOCK* processReport: logged info for {} of {} " +
          "reported.", maxNumBlocksToLog, numBlocksLogged);
    }
    for (Block b : toInvalidate) {
      addToInvalidates(b, node);
    }
    for (BlockToMarkCorrupt b : toCorrupt) {
      markBlockAsCorrupt(b, storageInfo, node);
    }

    return toInvalidate;
  }

  /**
   * Mark block replicas as corrupt except those on the storages in 
   * newStorages list.
   */
  public void markBlockReplicasAsCorrupt(Block oldBlock,
      BlockInfo block,
      long oldGenerationStamp, long oldNumBytes, 
      DatanodeStorageInfo[] newStorages) throws IOException {
    assert namesystem.hasWriteLock();
    BlockToMarkCorrupt b = null;
    if (block.getGenerationStamp() != oldGenerationStamp) {
      b = new BlockToMarkCorrupt(oldBlock, block, oldGenerationStamp,
          "genstamp does not match " + oldGenerationStamp
          + " : " + block.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
    } else if (block.getNumBytes() != oldNumBytes) {
      b = new BlockToMarkCorrupt(oldBlock, block,
          "length does not match " + oldNumBytes
          + " : " + block.getNumBytes(), Reason.SIZE_MISMATCH);
    } else {
      return;
    }

    for (DatanodeStorageInfo storage : getStorages(block)) {
      boolean isCorrupt = true;
      if (newStorages != null) {
        for (DatanodeStorageInfo newStorage : newStorages) {
          if (newStorage!= null && storage.equals(newStorage)) {
            isCorrupt = false;
            break;
          }
        }
      }
      if (isCorrupt) {
        if (blockLog.isDebugEnabled()) {
          blockLog.debug("BLOCK* markBlockReplicasAsCorrupt: mark block replica" +
              " {} on {} as corrupt because the dn is not in the new committed " +
              "storage list.", b, storage.getDatanodeDescriptor());
        }
        markBlockAsCorrupt(b, storage, storage.getDatanodeDescriptor());
      }
    }
  }

  /**
   * processFirstBlockReport is intended only for processing "initial" block
   * reports, the first block report received from a DN after it registers.
   * It just adds all the valid replicas to the datanode, without calculating 
   * a toRemove list (since there won't be any).  It also silently discards 
   * any invalid blocks, thereby deferring their processing until 
   * the next block report.
   * @param storageInfo - DatanodeStorageInfo that sent the report
   * @param report - the initial block report, to be processed
   * @throws IOException 
   */
  void processFirstBlockReport(
      final DatanodeStorageInfo storageInfo,
      final BlockListAsLongs report) throws IOException {
    if (report == null) return;
    assert (namesystem.hasWriteLock());
    assert (storageInfo.getBlockReportCount() == 0);

    for (BlockReportReplica iblk : report) {
      ReplicaState reportedState = iblk.getState();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Initial report of block {} on {} size {} replicaState = {}",
            iblk.getBlockName(), storageInfo.getDatanodeDescriptor(),
            iblk.getNumBytes(), reportedState);
      }

      if (shouldPostponeBlocksFromFuture && isGenStampInFuture(iblk)) {
        queueReportedBlock(storageInfo, iblk, reportedState,
            QUEUE_REASON_FUTURE_GENSTAMP);
        continue;
      }

      BlockInfo storedBlock = getStoredBlock(iblk);

      // If block does not belong to any file, we check if it violates
      // an integrity assumption of Name node
      if (storedBlock == null) {
        bmSafeMode.checkBlocksWithFutureGS(iblk);
        continue;
      }

      // If block is corrupt, mark it and continue to next block.
      BlockUCState ucState = storedBlock.getBlockUCState();
      BlockToMarkCorrupt c = checkReplicaCorrupt(
          iblk, reportedState, storedBlock, ucState,
          storageInfo.getDatanodeDescriptor());
      if (c != null) {
        if (shouldPostponeBlocksFromFuture) {
          // In the Standby, we may receive a block report for a file that we
          // just have an out-of-date gen-stamp or state for, for example.
          queueReportedBlock(storageInfo, iblk, reportedState,
              QUEUE_REASON_CORRUPT_STATE);
        } else {
          markBlockAsCorrupt(c, storageInfo, storageInfo.getDatanodeDescriptor());
        }
        continue;
      }
      
      // If block is under construction, add this replica to its list
      if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
        storedBlock.getUnderConstructionFeature()
            .addReplicaIfNotPresent(storageInfo, iblk, reportedState);
        // OpenFileBlocks only inside snapshots also will be added to safemode
        // threshold. So we need to update such blocks to safemode
        // refer HDFS-5283
        if (namesystem.isInSnapshot(storedBlock.getBlockCollectionId())) {
          int numOfReplicas = storedBlock.getUnderConstructionFeature()
              .getNumExpectedLocations();
          bmSafeMode.incrementSafeBlockCount(numOfReplicas, storedBlock);
        }
        //and fall through to next clause
      }      
      //add replica if appropriate
      if (reportedState == ReplicaState.FINALIZED) {
        addStoredBlockImmediate(storedBlock, iblk, storageInfo);
      }
    }
  }

  private void reportDiff(DatanodeStorageInfo storageInfo,
      BlockListAsLongs newReport,
      Collection<BlockInfoToAdd> toAdd,     // add to DatanodeDescriptor
      Collection<BlockInfo> toRemove,       // remove from DatanodeDescriptor
      Collection<Block> toInvalidate,       // should be removed from DN
      Collection<BlockToMarkCorrupt> toCorrupt, // add to corrupt replicas list
      Collection<StatefulBlockInfo> toUC) { // add to under-construction list

    // place a delimiter in the list which separates blocks
    // that have been reported from those that have not
    DatanodeDescriptor dn = storageInfo.getDatanodeDescriptor();
    Block delimiterBlock = new Block();
    BlockInfo delimiter = new BlockInfoContiguous(delimiterBlock,
        (short) 1);
    AddBlockResult result = storageInfo.addBlock(delimiter, delimiterBlock);
    assert result == AddBlockResult.ADDED
        : "Delimiting block cannot be present in the node";
    int headIndex = 0; //currently the delimiter is in the head of the list
    int curIndex;

    if (newReport == null) {
      newReport = BlockListAsLongs.EMPTY;
    }
    // scan the report and process newly reported blocks
    for (BlockReportReplica iblk : newReport) {
      ReplicaState iState = iblk.getState();
      LOG.debug("Reported block {} on {} size {} replicaState = {}", iblk, dn,
          iblk.getNumBytes(), iState);
      BlockInfo storedBlock = processReportedBlock(storageInfo,
          iblk, iState, toAdd, toInvalidate, toCorrupt, toUC);

      // move block to the head of the list
      if (storedBlock != null) {
        curIndex = storedBlock.findStorageInfo(storageInfo);
        if (curIndex >= 0) {
          headIndex =
              storageInfo.moveBlockToHead(storedBlock, curIndex, headIndex);
        }
      }
    }

    // collect blocks that have not been reported
    // all of them are next to the delimiter
    Iterator<BlockInfo> it =
        storageInfo.new BlockIterator(delimiter.getNext(0));
    while (it.hasNext()) {
      toRemove.add(it.next());
    }
    storageInfo.removeBlock(delimiter);
  }

  /**
   * Process a block replica reported by the data-node.
   * No side effects except adding to the passed-in Collections.
   *
   * <ol>
   * <li>If the block is not known to the system (not in blocksMap) then the
   * data-node should be notified to invalidate this block.</li>
   * <li>If the reported replica is valid that is has the same generation stamp
   * and length as recorded on the name-node, then the replica location should
   * be added to the name-node.</li>
   * <li>If the reported replica is not valid, then it is marked as corrupt,
   * which triggers replication of the existing valid replicas.
   * Corrupt replicas are removed from the system when the block
   * is fully replicated.</li>
   * <li>If the reported replica is for a block currently marked "under
   * construction" in the NN, then it should be added to the
   * BlockUnderConstructionFeature's list of replicas.</li>
   * </ol>
   *
   * @param storageInfo DatanodeStorageInfo that sent the report.
   * @param block reported block replica
   * @param reportedState reported replica state
   * @param toAdd add to DatanodeDescriptor
   * @param toInvalidate missing blocks (not in the blocks map)
   *        should be removed from the data-node
   * @param toCorrupt replicas with unexpected length or generation stamp;
   *        add to corrupt replicas
   * @param toUC replicas of blocks currently under construction
   * @return the up-to-date stored block, if it should be kept.
   *         Otherwise, null.
   */
  private BlockInfo processReportedBlock(
      final DatanodeStorageInfo storageInfo,
      final Block block, final ReplicaState reportedState,
      final Collection<BlockInfoToAdd> toAdd,
      final Collection<Block> toInvalidate,
      final Collection<BlockToMarkCorrupt> toCorrupt,
      final Collection<StatefulBlockInfo> toUC) {

    DatanodeDescriptor dn = storageInfo.getDatanodeDescriptor();

    LOG.debug("Reported block {} on {} size {} replicaState = {}", block, dn,
        block.getNumBytes(), reportedState);

    if (shouldPostponeBlocksFromFuture && isGenStampInFuture(block)) {
      queueReportedBlock(storageInfo, block, reportedState,
          QUEUE_REASON_FUTURE_GENSTAMP);
      return null;
    }

    // find block by blockId
    BlockInfo storedBlock = getStoredBlock(block);
    if(storedBlock == null) {
      // If blocksMap does not contain reported block id,
      // The replica should be removed from Datanode, and set NumBytes to BlockCommand.No_ACK to
      // avoid useless report to NameNode from Datanode when complete to process it.
      Block invalidateBlock = new Block(block);
      invalidateBlock.setNumBytes(BlockCommand.NO_ACK);
      toInvalidate.add(invalidateBlock);
      return null;
    }
    BlockUCState ucState = storedBlock.getBlockUCState();

    // Block is on the NN
    LOG.debug("In memory blockUCState = {}", ucState);

    // Ignore replicas already scheduled to be removed from the DN
    if(invalidateBlocks.contains(dn, block)) {
      return storedBlock;
    }

    BlockToMarkCorrupt c = checkReplicaCorrupt(
        block, reportedState, storedBlock, ucState, dn);
    if (c != null) {
      if (shouldPostponeBlocksFromFuture) {
        // If the block is an out-of-date generation stamp or state,
        // but we're the standby, we shouldn't treat it as corrupt,
        // but instead just queue it for later processing.
        // Storing the reported block for later processing, as that is what
        // comes from the IBR / FBR and hence what we should use to compare
        // against the memory state.
        // See HDFS-6289 and HDFS-15422 for more context.
        queueReportedBlock(storageInfo, block, reportedState,
            QUEUE_REASON_CORRUPT_STATE);
      } else {
        toCorrupt.add(c);
      }
      return storedBlock;
    }

    if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
      toUC.add(new StatefulBlockInfo(storedBlock,
          new Block(block), reportedState));
      return storedBlock;
    }

    // Add replica if appropriate. If the replica was previously corrupt
    // but now okay, it might need to be updated.
    if (reportedState == ReplicaState.FINALIZED
        && (storedBlock.findStorageInfo(storageInfo) == -1 ||
        corruptReplicas.isReplicaCorrupt(storedBlock, dn))) {
      toAdd.add(new BlockInfoToAdd(storedBlock, new Block(block)));
    }
    return storedBlock;
  }

  /**
   * Queue the given reported block for later processing in the
   * standby node. @see PendingDataNodeMessages.
   * @param reason a textual reason to report in the debug logs
   */
  private void queueReportedBlock(DatanodeStorageInfo storageInfo, Block block,
      ReplicaState reportedState, String reason) {
    assert shouldPostponeBlocksFromFuture;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Queueing reported block {} in state {}" +
              " from datanode {} for later processing because {}.",
          block, reportedState, storageInfo.getDatanodeDescriptor(), reason);
    }
    pendingDNMessages.enqueueReportedBlock(storageInfo, block, reportedState);
  }

  /**
   * Try to process any messages that were previously queued for the given
   * block. This is called from FSEditLogLoader whenever a block's state
   * in the namespace has changed or a new block has been created.
   */
  public void processQueuedMessagesForBlock(Block b) throws IOException {
    Queue<ReportedBlockInfo> queue = pendingDNMessages.takeBlockQueue(b);
    if (queue == null) {
      // Nothing to re-process
      return;
    }
    processQueuedMessages(queue);
  }
  
  private void processQueuedMessages(Iterable<ReportedBlockInfo> rbis)
      throws IOException {
    boolean isPreviousMessageProcessed = true;
    for (ReportedBlockInfo rbi : rbis) {
      LOG.debug("Processing previouly queued message {}", rbi);
      if (rbi.getReportedState() == null) {
        // This is a DELETE_BLOCK request
        DatanodeStorageInfo storageInfo = rbi.getStorageInfo();
        removeStoredBlock(getStoredBlock(rbi.getBlock()),
            storageInfo.getDatanodeDescriptor());
      } else if (!isPreviousMessageProcessed) {
        // if the previous IBR processing was skipped, skip processing all
        // further IBR's so as to ensure same sequence of processing.
        queueReportedBlock(rbi.getStorageInfo(), rbi.getBlock(),
            rbi.getReportedState(), QUEUE_REASON_FUTURE_GENSTAMP);
      } else {
        isPreviousMessageProcessed =
            processAndHandleReportedBlock(rbi.getStorageInfo(), rbi.getBlock(),
                rbi.getReportedState(), null);
      }
    }
  }
  
  /**
   * Process any remaining queued datanode messages after entering
   * active state. At this point they will not be re-queued since
   * we are the definitive master node and thus should be up-to-date
   * with the namespace information.
   */
  public void processAllPendingDNMessages() throws IOException {
    assert !shouldPostponeBlocksFromFuture :
      "processAllPendingDNMessages() should be called after disabling " +
      "block postponement.";
    int count = pendingDNMessages.count();
    if (count > 0) {
      LOG.info("Processing {} messages from DataNodes " +
          "that were previously queued during standby state", count);
    }
    processQueuedMessages(pendingDNMessages.takeAll());
    assert pendingDNMessages.count() == 0;
  }

  /**
   * The next two methods test the various cases under which we must conclude
   * the replica is corrupt, or under construction.  These are laid out
   * as switch statements, on the theory that it is easier to understand
   * the combinatorics of reportedState and ucState that way.  It should be
   * at least as efficient as boolean expressions.
   * 
   * @return a BlockToMarkCorrupt object, or null if the replica is not corrupt
   */
  private BlockToMarkCorrupt checkReplicaCorrupt(
      Block reported, ReplicaState reportedState, 
      BlockInfo storedBlock, BlockUCState ucState,
      DatanodeDescriptor dn) {
    switch(reportedState) {
    case FINALIZED:
      switch(ucState) {
      case COMPLETE:
      case COMMITTED:
        if (storedBlock.getGenerationStamp() != reported.getGenerationStamp()) {
          final long reportedGS = reported.getGenerationStamp();
          return new BlockToMarkCorrupt(new Block(reported), storedBlock, reportedGS,
              "block is " + ucState + " and reported genstamp " + reportedGS
              + " does not match genstamp in block map "
              + storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
        }
        boolean wrongSize;
        long blockMapSize;
        if (storedBlock.isStriped()) {
          assert BlockIdManager.isStripedBlockID(reported.getBlockId());
          assert storedBlock.getBlockId() ==
              BlockIdManager.convertToStripedID(reported.getBlockId());
          BlockInfoStriped stripedBlock = (BlockInfoStriped) storedBlock;
          int reportedBlkIdx = BlockIdManager.getBlockIndex(reported);
          blockMapSize = getInternalBlockLength(stripedBlock.getNumBytes(),
              stripedBlock.getCellSize(), stripedBlock.getDataBlockNum(),
              reportedBlkIdx);
          wrongSize = reported.getNumBytes() != blockMapSize;
        } else {
          blockMapSize = storedBlock.getNumBytes();
          wrongSize = blockMapSize != reported.getNumBytes();
        }
        if (wrongSize) {
          return new BlockToMarkCorrupt(new Block(reported), storedBlock,
              "block is " + ucState + " and reported length " +
              reported.getNumBytes() + " does not match " +
              "length in block map " + blockMapSize,
              Reason.SIZE_MISMATCH);
        } else {
          return null; // not corrupt
        }
      case UNDER_CONSTRUCTION:
        if (storedBlock.getGenerationStamp() > reported.getGenerationStamp()) {
          final long reportedGS = reported.getGenerationStamp();
          return new BlockToMarkCorrupt(new Block(reported), storedBlock, reportedGS,
              "block is " + ucState + " and reported state " + reportedState
              + ", But reported genstamp " + reportedGS
              + " does not match genstamp in block map "
              + storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
        }
        return null;
      default:
        return null;
      }
    case RBW:
    case RWR:
      final long reportedGS = reported.getGenerationStamp();
      if (!storedBlock.isComplete()) {
        //When DN report lesser GS than the storedBlock then mark it is corrupt,
        //As already valid replica will be present.
        if (storedBlock.getGenerationStamp() > reported.getGenerationStamp()) {
          return new BlockToMarkCorrupt(new Block(reported), storedBlock,
              reportedGS,
              "reported " + reportedState + " replica with genstamp "
                  + reportedGS
                  + " does not match Stored block's genstamp in block map "
                  + storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
        }
        return null; // not corrupt
      } else if (storedBlock.getGenerationStamp() != reported.getGenerationStamp()) {
        return new BlockToMarkCorrupt(new Block(reported), storedBlock, reportedGS,
            "reported " + reportedState + " replica with genstamp " + reportedGS
            + " does not match COMPLETE block's genstamp in block map "
            + storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
      } else { // COMPLETE block, same genstamp
        if (reportedState == ReplicaState.RBW) {
          // If it's a RBW report for a COMPLETE block, it may just be that
          // the block report got a little bit delayed after the pipeline
          // closed. So, ignore this report, assuming we will get a
          // FINALIZED replica later. See HDFS-2791
          LOG.info("Received an RBW replica for {} on {}: ignoring it, since "
                  + "it is complete with the same genstamp", storedBlock, dn);
          return null;
        } else {
          return new BlockToMarkCorrupt(new Block(reported), storedBlock,
              "reported replica has invalid state " + reportedState,
              Reason.INVALID_STATE);
        }
      }
    case RUR:       // should not be reported
    case TEMPORARY: // should not be reported
    default:
      String msg = "Unexpected replica state " + reportedState
      + " for block: " + storedBlock + 
      " on " + dn + " size " + storedBlock.getNumBytes();
      // log here at WARN level since this is really a broken HDFS invariant
      LOG.warn("{}", msg);
      return new BlockToMarkCorrupt(new Block(reported), storedBlock, msg,
          Reason.INVALID_STATE);
    }
  }

  private boolean isBlockUnderConstruction(BlockInfo storedBlock,
      BlockUCState ucState, ReplicaState reportedState) {
    switch(reportedState) {
    case FINALIZED:
      switch(ucState) {
      case UNDER_CONSTRUCTION:
      case UNDER_RECOVERY:
        return true;
      default:
        return false;
      }
    case RBW:
    case RWR:
      return (!storedBlock.isComplete());
    case RUR:       // should not be reported                                                                                             
    case TEMPORARY: // should not be reported                                                                                             
    default:
      return false;
    }
  }

  void addStoredBlockUnderConstruction(StatefulBlockInfo ucBlock,
      DatanodeStorageInfo storageInfo) throws IOException {
    BlockInfo block = ucBlock.storedBlock;
    block.getUnderConstructionFeature().addReplicaIfNotPresent(
        storageInfo, ucBlock.reportedBlock, ucBlock.reportedState);

    // Add replica if appropriate. If the replica was previously corrupt
    // but now okay, it might need to be updated.
    if (ucBlock.reportedState == ReplicaState.FINALIZED && (
        block.findStorageInfo(storageInfo) < 0) || corruptReplicas
        .isReplicaCorrupt(block, storageInfo.getDatanodeDescriptor())) {
      addStoredBlock(block, ucBlock.reportedBlock, storageInfo, null, true);
    }
  } 

  /**
   * Faster version of {@link #addStoredBlock},
   * intended for use with initial block report at startup. If not in startup
   * safe mode, will call standard addStoredBlock(). Assumes this method is
   * called "immediately" so there is no need to refresh the storedBlock from
   * blocksMap. Doesn't handle low redundancy/extra redundancy, or worry about
   * pendingReplications or corruptReplicas, because it's in startup safe mode.
   * Doesn't log every block, because there are typically millions of them.
   * 
   * @throws IOException
   */
  private void addStoredBlockImmediate(BlockInfo storedBlock, Block reported,
      DatanodeStorageInfo storageInfo)
  throws IOException {
    assert (storedBlock != null && namesystem.hasWriteLock());
    if (!namesystem.isInStartupSafeMode()
        || isPopulatingReplQueues()) {
      addStoredBlock(storedBlock, reported, storageInfo, null, false);
      return;
    }

    // just add it
    AddBlockResult result = storageInfo.addBlock(storedBlock, reported);

    // Now check for completion of blocks and safe block count
    int numCurrentReplica = countLiveNodes(storedBlock);
    if (storedBlock.getBlockUCState() == BlockUCState.COMMITTED
        && hasMinStorage(storedBlock, numCurrentReplica)) {
      completeBlock(storedBlock, null, false);
    } else if (storedBlock.isComplete() && result == AddBlockResult.ADDED) {
      // check whether safe replication is reached for the block
      // only complete blocks are counted towards that.
      // In the case that the block just became complete above, completeBlock()
      // handles the safe block count maintenance.
      bmSafeMode.incrementSafeBlockCount(numCurrentReplica, storedBlock);
    }
  }

  /**
   * Modify (block-->datanode) map. Remove block from set of
   * needed reconstruction if this takes care of the problem.
   * @return the block that is stored in blocksMap.
   */
  private Block addStoredBlock(final BlockInfo block,
                               final Block reportedBlock,
                               DatanodeStorageInfo storageInfo,
                               DatanodeDescriptor delNodeHint,
                               boolean logEveryBlock)
  throws IOException {
    assert block != null && namesystem.hasWriteLock();
    BlockInfo storedBlock;
    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    if (!block.isComplete()) {
      //refresh our copy in case the block got completed in another thread
      storedBlock = getStoredBlock(block);
    } else {
      storedBlock = block;
    }
    if (storedBlock == null || storedBlock.isDeleted()) {
      // If this block does not belong to anyfile, then we are done.
      blockLog.debug("BLOCK* addStoredBlock: {} on {} size {} but it does not belong to any file",
          reportedBlock, node, reportedBlock.getNumBytes());
      // we could add this block to invalidate set of this datanode.
      // it will happen in next block report otherwise.
      return block;
    }

    // add block to the datanode
    AddBlockResult result = storageInfo.addBlock(storedBlock, reportedBlock);

    int curReplicaDelta;
    if (result == AddBlockResult.ADDED) {
      curReplicaDelta =
          (node.isDecommissioned() || node.isDecommissionInProgress()) ? 0 : 1;
      if (logEveryBlock) {
        blockLog.info("BLOCK* addStoredBlock: {} is added to {} (size={})",
            node, reportedBlock, reportedBlock.getNumBytes());
      }
    } else if (result == AddBlockResult.REPLACED) {
      curReplicaDelta = 0;
      blockLog.warn("BLOCK* addStoredBlock: block {} moved to storageType " +
          "{} on node {} storageId {}, reportedBlock is {}", reportedBlock,
          storageInfo.getStorageType(), node, storageInfo.getStorageID(), reportedBlock);
    } else {
      // if the same block is added again and the replica was corrupt
      // previously because of a wrong gen stamp, remove it from the
      // corrupt block list.
      corruptReplicas.removeFromCorruptReplicasMap(block, node,
          Reason.GENSTAMP_MISMATCH);
      curReplicaDelta = 0;
      if (blockLog.isDebugEnabled()) {
        blockLog.debug("BLOCK* addStoredBlock: Redundant addStoredBlock request"
                + " received for {} on node {} size {}", reportedBlock, node,
            reportedBlock.getNumBytes());
      }
    }

    // Now check for completion of blocks and safe block count
    NumberReplicas num = countNodes(storedBlock);
    int numLiveReplicas = num.liveReplicas();
    int pendingNum = pendingReconstruction.getNumReplicas(storedBlock);
    int numCurrentReplica = numLiveReplicas + pendingNum;
    int numUsableReplicas = num.liveReplicas() +
        num.decommissioning() + num.liveEnteringMaintenanceReplicas();

    if(storedBlock.getBlockUCState() == BlockUCState.COMMITTED &&
        hasMinStorage(storedBlock, numUsableReplicas)) {
      addExpectedReplicasToPending(storedBlock);
      completeBlock(storedBlock, null, false);
    } else if (storedBlock.isComplete() && result == AddBlockResult.ADDED) {
      // check whether safe replication is reached for the block
      // only complete blocks are counted towards that
      // Is no-op if not in safe mode.
      // In the case that the block just became complete above, completeBlock()
      // handles the safe block count maintenance.
      bmSafeMode.incrementSafeBlockCount(numCurrentReplica, storedBlock);
    }
    
    // if block is still under construction, then done for now
    if (!storedBlock.isCompleteOrCommitted()) {
      return storedBlock;
    }

    // do not try to handle extra/low redundancy blocks during first safe mode
    if (!isPopulatingReplQueues()) {
      return storedBlock;
    }

    // handle low redundancy/extra redundancy
    short fileRedundancy = getExpectedRedundancyNum(storedBlock);
    if (!isNeededReconstruction(storedBlock, num, pendingNum)) {
      neededReconstruction.remove(storedBlock, numCurrentReplica,
          num.readOnlyReplicas(), num.outOfServiceReplicas(), fileRedundancy);
    } else {
      updateNeededReconstructions(storedBlock, curReplicaDelta, 0);
    }
    if (shouldProcessExtraRedundancy(num, fileRedundancy)) {
      processExtraRedundancyBlock(storedBlock, fileRedundancy, node,
          delNodeHint);
    }
    // If the file redundancy has reached desired value
    // we can remove any corrupt replicas the block may have
    int corruptReplicasCount = corruptReplicas.numCorruptReplicas(storedBlock);
    int numCorruptNodes = num.corruptReplicas();
    if (numCorruptNodes != corruptReplicasCount) {
      LOG.warn("Inconsistent number of corrupt replicas for {}" +
          ". blockMap has {} but corrupt replicas map has {}",
          storedBlock, numCorruptNodes, corruptReplicasCount);
    }
    if ((corruptReplicasCount > 0) && (numUsableReplicas >= fileRedundancy)) {
      invalidateCorruptReplicas(storedBlock, reportedBlock, num);
    }
    return storedBlock;
  }

  // If there is any maintenance replica, we don't have to restore
  // the condition of live + maintenance == expected. We allow
  // live + maintenance >= expected. The extra redundancy will be removed
  // when the maintenance node changes to live.
  private boolean shouldProcessExtraRedundancy(NumberReplicas num,
      int expectedNum) {
    final int numCurrent = num.liveReplicas();
    return numCurrent > expectedNum || num.redundantInternalBlocks() > 0;
  }

  /**
   * Invalidate corrupt replicas.
   * <p>
   * This will remove the replicas from the block's location list,
   * add them to {@link #invalidateBlocks} so that they could be further
   * deleted from the respective data-nodes,
   * and remove the block from corruptReplicasMap.
   * <p>
   * This method should be called when the block has sufficient
   * number of live replicas.
   *
   * @param blk Block whose corrupt replicas need to be invalidated
   */
  private void invalidateCorruptReplicas(BlockInfo blk, Block reported,
      NumberReplicas numberReplicas) {
    Collection<DatanodeDescriptor> nodes = corruptReplicas.getNodes(blk);
    boolean removedFromBlocksMap = true;
    if (nodes == null)
      return;
    // make a copy of the array of nodes in order to avoid
    // ConcurrentModificationException, when the block is removed from the node
    DatanodeDescriptor[] nodesCopy =
        nodes.toArray(new DatanodeDescriptor[nodes.size()]);

    DatanodeStorageInfo[] storages = null;
    if (blk.isStriped()) {
      storages = getStorages(blk);
    }

    for (DatanodeDescriptor node : nodesCopy) {
      Block blockToInvalidate = reported;
      if (storages != null && blk.isStriped()) {
        for (DatanodeStorageInfo s : storages) {
          if (s.getDatanodeDescriptor().equals(node)) {
            blockToInvalidate = getBlockOnStorage(blk, s);
            break;
          }
        }
      }
      try {
        if (!invalidateBlock(new BlockToMarkCorrupt(blockToInvalidate, blk, null,
            Reason.ANY), node, numberReplicas)) {
          removedFromBlocksMap = false;
        }
      } catch (IOException e) {
        blockLog.debug("invalidateCorruptReplicas error in deleting bad block {} on {}",
            blk, node, e);
        removedFromBlocksMap = false;
      }
    }
    // Remove the block from corruptReplicasMap
    if (removedFromBlocksMap) {
      corruptReplicas.removeFromCorruptReplicasMap(blk);
    }
  }

  /**
   * For each block in the name-node verify whether it belongs to any file,
   * extra or low redundancy. Place it into the respective queue.
   */
  public void processMisReplicatedBlocks() {
    assert namesystem.hasWriteLock();
    stopReconstructionInitializer();
    neededReconstruction.clear();
    reconstructionQueuesInitializer = new Daemon() {

      @Override
      public void run() {
        try {
          processMisReplicatesAsync();
        } catch (InterruptedException ie) {
          LOG.info("Interrupted while processing reconstruction queues.");
        } catch (Exception e) {
          LOG.error("Error while processing reconstruction queues async", e);
        }
      }
    };
    reconstructionQueuesInitializer
        .setName("Reconstruction Queue Initializer");
    reconstructionQueuesInitializer.start();
  }

  /*
   * Stop the ongoing initialisation of reconstruction queues
   */
  public void stopReconstructionInitializer() {
    if (reconstructionQueuesInitializer != null) {
      reconstructionQueuesInitializer.interrupt();
      try {
        reconstructionQueuesInitializer.join();
      } catch (final InterruptedException e) {
        LOG.warn("Interrupted while waiting for "
            + "reconstructionQueueInitializer. Returning..");
        return;
      } finally {
        reconstructionQueuesInitializer = null;
      }
    }
  }

  /*
   * Since the BlocksMapGset does not throw the ConcurrentModificationException
   * and supports further iteration after modification to list, there is a
   * chance of missing the newly added block while iterating. Since every
   * addition to blocksMap will check for mis-replication, missing mis-replication
   * check for new blocks will not be a problem.
   */
  private void processMisReplicatesAsync() throws InterruptedException {
    long nrInvalid = 0, nrOverReplicated = 0;
    long nrUnderReplicated = 0, nrPostponed = 0, nrUnderConstruction = 0;
    long startTimeMisReplicatedScan = Time.monotonicNow();
    Iterator<BlockInfo> blocksItr = blocksMap.getBlocks().iterator();
    long totalBlocks = blocksMap.size();
    reconstructionQueuesInitProgress = 0;
    long totalProcessed = 0;
    long sleepDuration =
        Math.max(1, Math.min(numBlocksPerIteration/1000, 10000));

    while (namesystem.isRunning() && !Thread.currentThread().isInterrupted()) {
      int processed = 0;
      namesystem.writeLockInterruptibly();
      try {
        while (processed < numBlocksPerIteration && blocksItr.hasNext()) {
          BlockInfo block = blocksItr.next();
          MisReplicationResult res = processMisReplicatedBlock(block);
          switch (res) {
          case UNDER_REPLICATED:
            LOG.trace("under replicated block {}: {}", block, res);
            nrUnderReplicated++;
            break;
          case OVER_REPLICATED:
            LOG.trace("over replicated block {}: {}", block, res);
            nrOverReplicated++;
            break;
          case INVALID:
            LOG.trace("invalid block {}: {}", block, res);
            nrInvalid++;
            break;
          case POSTPONE:
            LOG.trace("postpone block {}: {}", block, res);
            nrPostponed++;
            postponeBlock(block);
            break;
          case UNDER_CONSTRUCTION:
            LOG.trace("under construction block {}: {}", block, res);
            nrUnderConstruction++;
            break;
          case OK:
            break;
          default:
            throw new AssertionError("Invalid enum value: " + res);
          }
          processed++;
        }
        totalProcessed += processed;
        // there is a possibility that if any of the blocks deleted/added during
        // initialisation, then progress might be different.
        if (totalBlocks > 0) { // here avoid metrics appear as NaN.
          reconstructionQueuesInitProgress = Math.min((float) totalProcessed
              / totalBlocks, 1.0f);
        }

        if (!blocksItr.hasNext()) {
          LOG.info("Total number of blocks            = {}", blocksMap.size());
          LOG.info("Number of invalid blocks          = {}", nrInvalid);
          LOG.info("Number of under-replicated blocks = {}", nrUnderReplicated);
          LOG.info("Number of  over-replicated blocks = {}{}", nrOverReplicated,
              ((nrPostponed > 0) ? (" (" + nrPostponed + " postponed)") : ""));
          LOG.info("Number of blocks being written    = {}",
                   nrUnderConstruction);
          NameNode.stateChangeLog
              .info("STATE* Replication Queue initialization "
                  + "scan for invalid, over- and under-replicated blocks "
                  + "completed in "
                  + (Time.monotonicNow() - startTimeMisReplicatedScan)
                  + " msec");
          break;
        }
      } finally {
        namesystem.writeUnlock("processMisReplicatesAsync");
        LOG.info("Reconstruction queues initialisation progress: {}, total number of blocks " +
            "processed: {}/{}", reconstructionQueuesInitProgress, totalProcessed, totalBlocks);
        // Make sure it is out of the write lock for sufficiently long time.
        Thread.sleep(sleepDuration);
      }
    }
    if (Thread.currentThread().isInterrupted()) {
      LOG.info("Interrupted while processing replication queues.");
    }
  }

  /**
   * Get the progress of the reconstruction queues initialisation
   * 
   * @return Returns values between 0 and 1 for the progress.
   */
  public float getReconstructionQueuesInitProgress() {
    return reconstructionQueuesInitProgress;
  }

  /**
   * Get the value of whether there are any non-EC blocks using StripedID.
   *
   * @return Returns the value of whether there are any non-EC blocks using StripedID.
   */
  public boolean hasNonEcBlockUsingStripedID(){
    return hasNonEcBlockUsingStripedID;
  }

  /**
   * Schedule replication work for a specified list of mis-replicated
   * blocks and return total number of blocks scheduled for replication.
   *
   * @param blocks A list of blocks for which replication work needs to
   *              be scheduled.
   * @return Total number of blocks for which replication work is scheduled.
   **/
  public int processMisReplicatedBlocks(List<BlockInfo> blocks) {
    int processed = 0;
    Iterator<BlockInfo> iter = blocks.iterator();

    try {
      while (isPopulatingReplQueues() && namesystem.isRunning()
              && !Thread.currentThread().isInterrupted()
              && iter.hasNext()) {
        int limit = processed + numBlocksPerIteration;
        namesystem.writeLockInterruptibly();
        try {
          while (iter.hasNext() && processed < limit) {
            BlockInfo blk = iter.next();
            MisReplicationResult r = processMisReplicatedBlock(blk);
            processed++;
            LOG.debug("BLOCK* processMisReplicatedBlocks: Re-scanned block {}, result is {}",
                blk, r);
          }
        } finally {
          namesystem.writeUnlock("processMisReplicatedBlocks");
        }
      }
    } catch (InterruptedException ex) {
      LOG.info("Caught InterruptedException while scheduling replication work" +
              " for mis-replicated blocks");
      Thread.currentThread().interrupt();
    }

    return processed;
  }

  /**
   * Process a single possibly misreplicated block. This adds it to the
   * appropriate queues if necessary, and returns a result code indicating
   * what happened with it.
   */
  private MisReplicationResult processMisReplicatedBlock(BlockInfo block) {
    if (block.isDeleted()) {
      // block does not belong to any file
      addToInvalidates(block);
      return MisReplicationResult.INVALID;
    }
    if (!block.isComplete()) {
      // Incomplete blocks are never considered mis-replicated --
      // they'll be reached when they are completed or recovered.
      return MisReplicationResult.UNDER_CONSTRUCTION;
    }
    // calculate current redundancy
    short expectedRedundancy = getExpectedRedundancyNum(block);
    NumberReplicas num = countNodes(block);
    final int numCurrentReplica = num.liveReplicas();
    // add to low redundancy queue if need to be
    if (isNeededReconstruction(block, num)) {
      if (neededReconstruction.add(block, numCurrentReplica,
          num.readOnlyReplicas(), num.outOfServiceReplicas(),
          expectedRedundancy)) {
        return MisReplicationResult.UNDER_REPLICATED;
      }
    }

    if (shouldProcessExtraRedundancy(num, expectedRedundancy)) {
      // extra redundancy block
      if (!processExtraRedundancyBlockWithoutPostpone(block, expectedRedundancy,
          null, null)) {
        return MisReplicationResult.POSTPONE;
      }
      return MisReplicationResult.OVER_REPLICATED;
    }
    
    return MisReplicationResult.OK;
  }
  
  /** Set replication for the blocks. */
  public void setReplication(
      final short oldRepl, final short newRepl, final BlockInfo b) {
    if (newRepl == oldRepl) {
      return;
    }

    // update neededReconstruction priority queues
    b.setReplication(newRepl);

    // Process the block only when active NN is out of safe mode.
    if (!isPopulatingReplQueues()) {
      return;
    }
    NumberReplicas num = countNodes(b);
    updateNeededReconstructions(b, 0, newRepl - oldRepl);
    if (shouldProcessExtraRedundancy(num, newRepl)) {
      processExtraRedundancyBlock(b, newRepl, null, null);
    }
  }

  /**
   * Process blocks with redundant replicas. If there are replicas in
   * stale storages, mark them in the postponedMisreplicatedBlocks.
   */
  private void processExtraRedundancyBlock(final BlockInfo block,
      final short replication, final DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {
    if (!processExtraRedundancyBlockWithoutPostpone(block, replication,
        addedNode, delNodeHint)) {
      postponeBlock(block);
    }
  }

  /**
   * Find how many of the containing nodes are "extra", if any.
   * If there are any extras, call chooseExcessRedundancies() to
   * mark them in the excessRedundancyMap.
   * @return true if all redundancy replicas are removed.
   */
  private boolean processExtraRedundancyBlockWithoutPostpone(final BlockInfo block,
      final short replication, final DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {
    assert namesystem.hasWriteLock();
    if (addedNode == delNodeHint) {
      delNodeHint = null;
    }
    Collection<DatanodeStorageInfo> nonExcess = new ArrayList<>();
    Collection<DatanodeDescriptor> corruptNodes = corruptReplicas
        .getNodes(block);
    boolean hasStaleStorage = false;
    Set<DatanodeStorageInfo> staleStorages = new HashSet<>();
    for (DatanodeStorageInfo storage : blocksMap.getStorages(block)) {
      if (storage.getState() != State.NORMAL) {
        continue;
      }
      final DatanodeDescriptor cur = storage.getDatanodeDescriptor();
      if (storage.areBlockContentsStale()) {
        hasStaleStorage = true;
        staleStorages.add(storage);
        continue;
      }
      if (!isExcess(cur, block)) {
        if (cur.isInService()) {
          // exclude corrupt replicas
          if (corruptNodes == null || !corruptNodes.contains(cur)) {
            nonExcess.add(storage);
          }
        }
      }
    }
    chooseExcessRedundancies(nonExcess, block, replication, addedNode,
        delNodeHint);
    if (hasStaleStorage) {
      LOG.trace("BLOCK* processExtraRedundancyBlockWithoutPostpone: Postponing {}"
              + " since storages {} does not yet have up-to-date information.",
          block, staleStorages);
      return false;
    }
    return true;
  }

  private void chooseExcessRedundancies(
      final Collection<DatanodeStorageInfo> nonExcess,
      BlockInfo storedBlock, short replication,
      DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {
    assert namesystem.hasWriteLock();
    // first form a rack to datanodes map and
    BlockCollection bc = getBlockCollection(storedBlock);
    if (storedBlock.isStriped()) {
      chooseExcessRedundancyStriped(bc, nonExcess, storedBlock, delNodeHint);
    } else {
      if (nonExcess.size() > replication) {
        final BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(
            bc.getStoragePolicyID());
        final List<StorageType> excessTypes = storagePolicy.chooseExcess(
            replication, DatanodeStorageInfo.toStorageTypes(nonExcess));
        chooseExcessRedundancyContiguous(nonExcess, storedBlock, replication,
            addedNode, delNodeHint, excessTypes);
      }
    }
  }

  /**
   * We want sufficient redundancy for the block, but we now have too many.
   * In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such that:
   *
   * srcNodes.size() - dstNodes.size() == replication
   *
   * We pick node that make sure that replicas are spread across racks and
   * also try hard to pick one with least free space.
   * The algorithm is first to pick a node with least free space from nodes
   * that are on a rack holding more than one replicas of the block.
   * So removing such a replica won't remove a rack. 
   * If no such a node is available,
   * then pick a node with least free space
   */
  private void chooseExcessRedundancyContiguous(
      final Collection<DatanodeStorageInfo> nonExcess, BlockInfo storedBlock,
      short replication, DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint, List<StorageType> excessTypes) {
    BlockPlacementPolicy replicator = placementPolicies.getPolicy(CONTIGUOUS);
    List<DatanodeStorageInfo> replicasToDelete = replicator
        .chooseReplicasToDelete(nonExcess, nonExcess, replication, excessTypes,
            addedNode, delNodeHint);
    for (DatanodeStorageInfo chosenReplica : replicasToDelete) {
      processChosenExcessRedundancy(nonExcess, chosenReplica, storedBlock);
    }
  }

  /**
   * We want block group has every internal block, but we have redundant
   * internal blocks (which have the same index).
   * In this method, we delete the redundant internal blocks until only one
   * left for each index.
   *
   * The block placement policy will make sure that the left internal blocks are
   * spread across racks and also try hard to pick one with least free space.
   */
  private void chooseExcessRedundancyStriped(BlockCollection bc,
      final Collection<DatanodeStorageInfo> nonExcess,
      BlockInfo storedBlock,
      DatanodeDescriptor delNodeHint) {
    assert storedBlock instanceof BlockInfoStriped;
    BlockInfoStriped sblk = (BlockInfoStriped) storedBlock;
    short groupSize = sblk.getTotalBlockNum();

    // find all duplicated indices
    BitSet found = new BitSet(groupSize); //indices found
    BitSet duplicated = new BitSet(groupSize); //indices found more than once
    HashMap<DatanodeStorageInfo, Integer> storage2index = new HashMap<>();
    boolean logEmptyExcessType = true;
    for (DatanodeStorageInfo storage : nonExcess) {
      int index = sblk.getStorageBlockIndex(storage);
      assert index >= 0;
      if (found.get(index)) {
        duplicated.set(index);
      }
      found.set(index);
      storage2index.put(storage, index);
    }

    if (duplicated.isEmpty()) {
      LOG.debug("Found no duplicated internal blocks for {}. Maybe it's " +
          "because there are stale storages.", storedBlock);
      return;
    }

    // use delHint only if delHint is duplicated
    final DatanodeStorageInfo delStorageHint =
        DatanodeStorageInfo.getDatanodeStorageInfo(nonExcess, delNodeHint);
    if (delStorageHint != null) {
      Integer index = storage2index.get(delStorageHint);
      if (index != null && duplicated.get(index)) {
        processChosenExcessRedundancy(nonExcess, delStorageHint, storedBlock);
        logEmptyExcessType = false;
      }
    }

    // cardinality of found indicates the expected number of internal blocks
    final int numOfTarget = found.cardinality();
    final BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(
        bc.getStoragePolicyID());
    final List<StorageType> excessTypes = storagePolicy.chooseExcess(
        (short) numOfTarget, DatanodeStorageInfo.toStorageTypes(nonExcess));
    if (excessTypes.isEmpty()) {
      if(logEmptyExcessType) {
        LOG.warn("excess types chosen for block {} among storages {} is empty",
                storedBlock, nonExcess);
      }
      return;
    }

    BlockPlacementPolicy placementPolicy = placementPolicies.getPolicy(STRIPED);
    // for each duplicated index, delete some replicas until only one left
    for (int targetIndex = duplicated.nextSetBit(0); targetIndex >= 0;
         targetIndex = duplicated.nextSetBit(targetIndex + 1)) {
      List<DatanodeStorageInfo> candidates = new ArrayList<>();
      for (DatanodeStorageInfo storage : nonExcess) {
        int index = storage2index.get(storage);
        if (index == targetIndex) {
          candidates.add(storage);
        }
      }
      if (candidates.size() > 1) {
        List<DatanodeStorageInfo> replicasToDelete = placementPolicy
            .chooseReplicasToDelete(nonExcess, candidates, (short) 1,
                excessTypes, null, null);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Choose redundant EC replicas to delete from blk_{} which is located in {}",
              sblk.getBlockId(), storage2index);
          LOG.debug("Storages with candidate blocks to be deleted: {}", candidates);
          LOG.debug("Storages with blocks to be deleted: {}", replicasToDelete);
        }
        Preconditions.checkArgument(candidates.containsAll(replicasToDelete),
            "The EC replicas to be deleted are not in the candidate list");
        for (DatanodeStorageInfo chosen : replicasToDelete) {
          processChosenExcessRedundancy(nonExcess, chosen, storedBlock);
          candidates.remove(chosen);
        }
      }
      duplicated.clear(targetIndex);
    }
  }

  private void processChosenExcessRedundancy(
      final Collection<DatanodeStorageInfo> nonExcess,
      final DatanodeStorageInfo chosen, BlockInfo storedBlock) {
    nonExcess.remove(chosen);
    excessRedundancyMap.add(chosen.getDatanodeDescriptor(), storedBlock);
    //
    // The 'excessblocks' tracks blocks until we get confirmation
    // that the datanode has deleted them; the only way we remove them
    // is when we get a "removeBlock" message.
    //
    // The 'invalidate' list is used to inform the datanode the block
    // should be deleted.  Items are removed from the invalidate list
    // upon giving instructions to the datanodes.
    //
    final Block blockToInvalidate = getBlockOnStorage(storedBlock, chosen);
    addToInvalidates(blockToInvalidate, chosen.getDatanodeDescriptor());
    blockLog.debug("BLOCK* chooseExcessRedundancies: ({}, {}) is added to invalidated blocks set",
        chosen, storedBlock);
  }

  private void removeStoredBlock(DatanodeStorageInfo storageInfo, Block block,
      DatanodeDescriptor node) {
    if (shouldPostponeBlocksFromFuture && isGenStampInFuture(block)) {
      queueReportedBlock(storageInfo, block, null,
          QUEUE_REASON_FUTURE_GENSTAMP);
      return;
    }
    removeStoredBlock(getStoredBlock(block), node);
  }

  /**
   * Modify (block{@literal -->}datanode) map. Possibly generate replication
   * tasks, if the removed block is still valid.
   */
  public void removeStoredBlock(BlockInfo storedBlock, DatanodeDescriptor node) {
    blockLog.debug("BLOCK* removeStoredBlock: {} from {}", storedBlock, node);
    assert (namesystem.hasWriteLock());
    {
      if (storedBlock == null || !blocksMap.removeNode(storedBlock, node)) {
        blockLog.debug("BLOCK* removeStoredBlock: {} has already been removed from node {}",
            storedBlock, node);
        return;
      }

      CachedBlock cblock = namesystem.getCacheManager().getCachedBlocks()
          .get(new CachedBlock(storedBlock.getBlockId(), (short) 0, false));
      if (cblock != null) {
        boolean removed = false;
        removed |= node.getPendingCached().remove(cblock);
        removed |= node.getCached().remove(cblock);
        removed |= node.getPendingUncached().remove(cblock);
        if (removed) {
          if (blockLog.isDebugEnabled()) {
            blockLog.debug("BLOCK* removeStoredBlock: {} removed from caching "
                + "related lists on node {}", storedBlock, node);
          }
        }
      }

      //
      // It's possible that the block was removed because of a datanode
      // failure. If the block is still valid, check if replication is
      // necessary. In that case, put block on a possibly-will-
      // be-replicated list.
      //
      if (!storedBlock.isDeleted()) {
        bmSafeMode.decrementSafeBlockCount(storedBlock);
        updateNeededReconstructions(storedBlock, -1, 0);
      }

      excessRedundancyMap.remove(node, storedBlock);
      corruptReplicas.removeFromCorruptReplicasMap(storedBlock, node);
    }
  }

  private void removeStaleReplicas(List<ReplicaUnderConstruction> staleReplicas,
      BlockInfo block) {
    for (ReplicaUnderConstruction r : staleReplicas) {
      removeStoredBlock(block,
          r.getExpectedStorageLocation().getDatanodeDescriptor());
      if (blockLog.isDebugEnabled()) {
        blockLog.debug("BLOCK* Removing stale replica {} of {}", r, Block.toString(r));
      }
    }
  }
  /**
   * Get all valid locations of the block & add the block to results
   * @return the length of the added block; 0 if the block is not added. If the
   * added block is a block group, return its approximate internal block size
   */
  private long addBlock(BlockInfo block, List<BlockWithLocations> results) {
    final List<DatanodeStorageInfo> locations = getValidLocations(block);
    if(locations.size() == 0) {
      return 0;
    } else {
      final String[] datanodeUuids = new String[locations.size()];
      final String[] storageIDs = new String[datanodeUuids.length];
      final StorageType[] storageTypes = new StorageType[datanodeUuids.length];
      for(int i = 0; i < locations.size(); i++) {
        final DatanodeStorageInfo s = locations.get(i);
        datanodeUuids[i] = s.getDatanodeDescriptor().getDatanodeUuid();
        storageIDs[i] = s.getStorageID();
        storageTypes[i] = s.getStorageType();
      }
      BlockWithLocations blkWithLocs = new BlockWithLocations(block,
          datanodeUuids, storageIDs, storageTypes);
      if(block.isStriped()) {
        BlockInfoStriped blockStriped = (BlockInfoStriped) block;
        byte[] indices = new byte[locations.size()];
        for (int i = 0; i < locations.size(); i++) {
          indices[i] =
              (byte) blockStriped.getStorageBlockIndex(locations.get(i));
        }
        results.add(new StripedBlockWithLocations(blkWithLocs, indices,
            blockStriped.getDataBlockNum(), blockStriped.getCellSize()));
        // approximate size
        return block.getNumBytes() / blockStriped.getDataBlockNum();
      }else{
        results.add(blkWithLocs);
        return block.getNumBytes();
      }
    }
  }

  /**
   * The given node is reporting that it received a certain block.
   */
  @VisibleForTesting
  public void addBlock(DatanodeStorageInfo storageInfo, Block block,
      String delHint) throws IOException {
    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    // Decrement number of blocks scheduled to this datanode.
    // for a retry request (of DatanodeProtocol#blockReceivedAndDeleted with 
    // RECEIVED_BLOCK), we currently also decrease the approximate number. 
    node.decrementBlocksScheduled(storageInfo.getStorageType());

    // get the deletion hint node
    DatanodeDescriptor delHintNode = null;
    if (delHint != null && delHint.length() != 0) {
      delHintNode = datanodeManager.getDatanode(delHint);
      if (delHintNode == null) {
        blockLog.warn("BLOCK* blockReceived: {} is expected to be removed " +
            "from an unrecorded node {}", block, delHint);
      }
    }

    //
    // Modify the blocks->datanode map and node's map.
    //
    BlockInfo storedBlock = getStoredBlock(block);
    if (storedBlock != null &&
        block.getGenerationStamp() == storedBlock.getGenerationStamp()) {
      if (pendingReconstruction.decrement(storedBlock, storageInfo)) {
        NameNode.getNameNodeMetrics().incSuccessfulReReplications();
      }
    }
    processAndHandleReportedBlock(storageInfo, block, ReplicaState.FINALIZED,
        delHintNode);
  }

  /**
   * Process a reported block.
   * @return true if the block is processed, or false if the block is queued
   * to be processed later.
   * @throws IOException
   */
  private boolean processAndHandleReportedBlock(
      DatanodeStorageInfo storageInfo, Block block,
      ReplicaState reportedState, DatanodeDescriptor delHintNode)
      throws IOException {
    // blockReceived reports a finalized block
    Collection<BlockInfoToAdd> toAdd = new LinkedList<>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    Collection<BlockToMarkCorrupt> toCorrupt =
        new LinkedList<BlockToMarkCorrupt>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<StatefulBlockInfo>();

    final DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();

    LOG.debug("Reported block {} on {} size {} replicaState = {}",
        block, node, block.getNumBytes(), reportedState);

    if (shouldPostponeBlocksFromFuture &&
        isGenStampInFuture(block)) {
      queueReportedBlock(storageInfo, block, reportedState,
          QUEUE_REASON_FUTURE_GENSTAMP);
      return false;
    }

    processReportedBlock(storageInfo, block, reportedState, toAdd, toInvalidate,
        toCorrupt, toUC);
    // the block is only in one of the to-do lists
    // if it is in none then data-node already has it
    assert toUC.size() + toAdd.size() + toInvalidate.size() + toCorrupt
        .size() <= 1 : "The block should be only in one of the lists.";

    for (StatefulBlockInfo b : toUC) {
      addStoredBlockUnderConstruction(b, storageInfo);
    }
    long numBlocksLogged = 0;
    for (BlockInfoToAdd b : toAdd) {
      addStoredBlock(b.stored, b.reported, storageInfo, delHintNode,
          numBlocksLogged < maxNumBlocksToLog);
      numBlocksLogged++;
    }
    if (numBlocksLogged > maxNumBlocksToLog) {
      blockLog.debug("BLOCK* addBlock: logged info for {} of {} reported.",
          maxNumBlocksToLog, numBlocksLogged);
    }
    for (Block b : toInvalidate) {
      blockLog.debug("BLOCK* addBlock: block {} on node {} size {} does not belong to any file",
          b, node, b.getNumBytes());
      addToInvalidates(b, node);
    }
    for (BlockToMarkCorrupt b : toCorrupt) {
      markBlockAsCorrupt(b, storageInfo, node);
    }
    return true;
  }

  /**
   * The given node is reporting incremental information about some blocks.
   * This includes blocks that are starting to be received, completed being
   * received, or deleted.
   * 
   * This method must be called with FSNamesystem lock held.
   */
  public void processIncrementalBlockReport(final DatanodeID nodeID,
      final StorageReceivedDeletedBlocks srdb) throws IOException {
    assert namesystem.hasWriteLock();
    final DatanodeDescriptor node = datanodeManager.getDatanode(nodeID);
    if (node == null || !node.isRegistered()) {
      blockLog.warn("BLOCK* processIncrementalBlockReport"
              + " is received from dead or unregistered node {}", nodeID);
      throw new IOException(
          "Got incremental block report from unregistered or dead node");
    }

    boolean successful = false;
    try {
      processIncrementalBlockReport(node, srdb);
      successful = true;
    } finally {
      if (!successful) {
        node.setForceRegistration(true);
      }
    }
  }

  private void processIncrementalBlockReport(final DatanodeDescriptor node,
      final StorageReceivedDeletedBlocks srdb) throws IOException {
    DatanodeStorageInfo storageInfo =
        node.getStorageInfo(srdb.getStorage().getStorageID());
    if (storageInfo == null) {
      // The DataNode is reporting an unknown storage. Usually the NN learns
      // about new storages from heartbeats but during NN restart we may
      // receive a block report or incremental report before the heartbeat.
      // We must handle this for protocol compatibility. This issue was
      // uncovered by HDFS-6094.
      storageInfo = node.updateStorage(srdb.getStorage());
    }

    int received = 0;
    int deleted = 0;
    int receiving = 0;

    for (ReceivedDeletedBlockInfo rdbi : srdb.getBlocks()) {
      switch (rdbi.getStatus()) {
      case DELETED_BLOCK:
        removeStoredBlock(storageInfo, rdbi.getBlock(), node);
        deleted++;
        break;
      case RECEIVED_BLOCK:
        addBlock(storageInfo, rdbi.getBlock(), rdbi.getDelHints());
        received++;
        break;
      case RECEIVING_BLOCK:
        receiving++;
        processAndHandleReportedBlock(storageInfo, rdbi.getBlock(),
                                      ReplicaState.RBW, null);
        break;
      default:
        String msg = 
          "Unknown block status code reported by " + node +
          ": " + rdbi;
        blockLog.warn(msg);
        assert false : msg; // if assertions are enabled, throw.
        break;
      }
      blockLog.debug("BLOCK* block {}: {} is received from {}",
          rdbi.getStatus(), rdbi.getBlock(), node);
    }
    if (blockLog.isDebugEnabled()) {
      blockLog.debug("*BLOCK* NameNode.processIncrementalBlockReport: from "
              + "{} receiving: {}, received: {}, deleted: {}", node, receiving,
          received, deleted);
    }
  }

  /**
   * Return the number of nodes hosting a given block, grouped
   * by the state of those replicas.
   * For a striped block, this includes nodes storing blocks belonging to the
   * striped block group. But note we exclude duplicated internal block replicas
   * for calculating {@link NumberReplicas#liveReplicas}. If the replica on a
   * decommissioning node is the same as the replica on a live node, the
   * internal block for this replica is live, not decommissioning.
   */
  public NumberReplicas countNodes(BlockInfo b) {
    return countNodes(b, false);
  }

  NumberReplicas countNodes(BlockInfo b, boolean inStartupSafeMode) {
    NumberReplicas numberReplicas = new NumberReplicas();
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(b);
    if (b.isStriped()) {
      countReplicasForStripedBlock(numberReplicas, (BlockInfoStriped) b,
          nodesCorrupt, inStartupSafeMode);
    } else {
      for (DatanodeStorageInfo storage : blocksMap.getStorages(b)) {
        checkReplicaOnStorage(numberReplicas, b, storage, nodesCorrupt,
            inStartupSafeMode);
      }
    }
    return numberReplicas;
  }

  private StoredReplicaState checkReplicaOnStorage(NumberReplicas counters,
      BlockInfo b, DatanodeStorageInfo storage,
      Collection<DatanodeDescriptor> nodesCorrupt, boolean inStartupSafeMode) {
    final StoredReplicaState s;
    if (storage.getState() == State.NORMAL) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      if (nodesCorrupt != null && nodesCorrupt.contains(node)) {
        s = StoredReplicaState.CORRUPT;
      } else if (inStartupSafeMode) {
        s = StoredReplicaState.LIVE;
        counters.add(s, 1);
        return s;
      } else if (node.isDecommissionInProgress()) {
        s = StoredReplicaState.DECOMMISSIONING;
      } else if (node.isDecommissioned()) {
        s = StoredReplicaState.DECOMMISSIONED;
      } else if (node.isMaintenance()) {
        if (node.isInMaintenance() || !node.isAlive()) {
          s = StoredReplicaState.MAINTENANCE_NOT_FOR_READ;
        } else {
          s = StoredReplicaState.MAINTENANCE_FOR_READ;
        }
      } else if (isExcess(node, b)) {
        s = StoredReplicaState.EXCESS;
      } else {
        s = StoredReplicaState.LIVE;
      }
      counters.add(s, 1);
      if (storage.areBlockContentsStale()) {
        counters.add(StoredReplicaState.STALESTORAGE, 1);
      }
    } else if (!inStartupSafeMode &&
        storage.getState() == State.READ_ONLY_SHARED) {
      s = StoredReplicaState.READONLY;
      counters.add(s, 1);
    } else {
      s = null;
    }
    return s;
  }

  /**
   * For a striped block, it is possible it contains full number of internal
   * blocks (i.e., 9 by default), but with duplicated replicas of the same
   * internal block. E.g., for the following list of internal blocks
   * b0, b0, b1, b2, b3, b4, b5, b6, b7
   * we have 9 internal blocks but we actually miss b8.
   * We should use this method to detect the above scenario and schedule
   * necessary reconstruction.
   */
  private void countReplicasForStripedBlock(NumberReplicas counters,
      BlockInfoStriped block, Collection<DatanodeDescriptor> nodesCorrupt,
      boolean inStartupSafeMode) {
    BitSet liveBitSet = new BitSet(block.getTotalBlockNum());
    BitSet decommissioningBitSet = new BitSet(block.getTotalBlockNum());
    for (StorageAndBlockIndex si : block.getStorageAndIndexInfos()) {
      StoredReplicaState state = checkReplicaOnStorage(counters, block,
          si.getStorage(), nodesCorrupt, inStartupSafeMode);
      countLiveAndDecommissioningReplicas(counters, state, liveBitSet,
          decommissioningBitSet, si.getBlockIndex());
    }
  }

  /**
   * Count distinct live and decommission internal blocks with blockIndex.
   * If A replica with INDEX is decommissioning, and B replica with INDEX
   * is live, the internal INDEX block is live.
   */
  private void countLiveAndDecommissioningReplicas(NumberReplicas counters,
      StoredReplicaState state, BitSet liveBitSet,
      BitSet decommissioningBitSet, byte blockIndex) {
    if (state == StoredReplicaState.LIVE) {
      if (!liveBitSet.get(blockIndex)) {
        liveBitSet.set(blockIndex);
        // Sub decommissioning because the index replica is live.
        if (decommissioningBitSet.get(blockIndex)) {
          counters.subtract(StoredReplicaState.DECOMMISSIONING, 1);
        }
      } else {
        counters.subtract(StoredReplicaState.LIVE, 1);
        counters.add(StoredReplicaState.REDUNDANT, 1);
      }
    } else if (state == StoredReplicaState.DECOMMISSIONING) {
      if (liveBitSet.get(blockIndex) || decommissioningBitSet.get(blockIndex)) {
        counters.subtract(StoredReplicaState.DECOMMISSIONING, 1);
      } else {
        decommissioningBitSet.set(blockIndex);
      }
    }
  }

  @VisibleForTesting
  int getExcessSize4Testing(String dnUuid) {
    return excessRedundancyMap.getSize4Testing(dnUuid);
  }

  public boolean isExcess(DatanodeDescriptor dn, BlockInfo blk) {
    return excessRedundancyMap.contains(dn, blk);
  }

  /** 
   * Simpler, faster form of {@link #countNodes} that only returns the number
   * of live nodes.  If in startup safemode (or its 30-sec extension period),
   * then it gains speed by ignoring issues of excess replicas or nodes
   * that are decommissioned or in process of becoming decommissioned.
   * If not in startup, then it calls {@link #countNodes} instead.
   *
   * @param b - the block being tested
   * @return count of live nodes for this block
   */
  int countLiveNodes(BlockInfo b) {
    final boolean inStartupSafeMode = namesystem.isInStartupSafeMode();
    return countNodes(b, inStartupSafeMode).liveReplicas();
  }
  
  /**
   * On putting the node in service, check if the node has excess replicas.
   * If there are any excess replicas, call processExtraRedundancyBlock().
   * Process extra redundancy blocks only when active NN is out of safe mode.
   */
  void processExtraRedundancyBlocksOnInService(
      final DatanodeDescriptor srcNode) {
    if (!isPopulatingReplQueues()) {
      return;
    }

    int numExtraRedundancy = 0;
    for (DatanodeStorageInfo datanodeStorageInfo : srcNode.getStorageInfos()) {
      // the namesystem lock is released between iterations. Make sure the
      // storage is not removed before continuing.
      if (srcNode.getStorageInfo(datanodeStorageInfo.getStorageID()) == null) {
        continue;
      }
      final Iterator<BlockInfo> it = datanodeStorageInfo.getBlockIterator();
      while(it.hasNext()) {
        final BlockInfo block = it.next();
        if (block.isDeleted()) {
          //Orphan block, will be handled eventually, skip
          continue;
        }
        int expectedReplication = this.getExpectedRedundancyNum(block);
        NumberReplicas num = countNodes(block);
        if (shouldProcessExtraRedundancy(num, expectedReplication)) {
          // extra redundancy block
          processExtraRedundancyBlock(block, (short) expectedReplication, null,
              null);
          numExtraRedundancy++;
        }
      }
      // When called by tests like TestDefaultBlockPlacementPolicy.
      // testPlacementWithLocalRackNodesDecommissioned, it is not protected by
      // lock, only when called by DatanodeManager.refreshNodes have writeLock
      if (namesystem.hasWriteLock()) {
        namesystem.writeUnlock("processExtraRedundancyBlocksOnInService");
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        namesystem.writeLock();
      }
    }
    LOG.info("Invalidated {} extra redundancy blocks on {} after "
             + "it is in service", numExtraRedundancy, srcNode);
  }

  /**
   * Returns whether a node can be safely decommissioned or in maintenance
   * based on its liveness. Dead nodes cannot always be safely decommissioned
   * or in maintenance.
   */
  boolean isNodeHealthyForDecommissionOrMaintenance(DatanodeDescriptor node) {
    if (!node.checkBlockReportReceived()) {
      LOG.info("Node {} hasn't sent its first block report.", node);
      return false;
    }

    if (node.isAlive()) {
      return true;
    }

    updateState();
    if (pendingReconstructionBlocksCount == 0 &&
        lowRedundancyBlocksCount == 0) {
      LOG.info("Node {} is dead and there are no low redundancy" +
          " blocks or blocks pending reconstruction. Safe to decommission or" +
          " put in maintenance.", node);
      return true;
    }

    LOG.warn("Node {} is dead " +
        "while in {}. Cannot be safely " +
        "decommissioned or be in maintenance since there is risk of reduced " +
        "data durability or data loss. Either restart the failed node or " +
        "force decommissioning or maintenance by removing, calling " +
        "refreshNodes, then re-adding to the excludes or host config files.",
        node, node.getAdminState());
    return false;
  }

  public int getActiveBlockCount() {
    return blocksMap.size();
  }

  public DatanodeStorageInfo[] getStorages(BlockInfo block) {
    final DatanodeStorageInfo[] storages = new DatanodeStorageInfo[block.numNodes()];
    int i = 0;
    for(DatanodeStorageInfo s : blocksMap.getStorages(block)) {
      storages[i++] = s;
    }
    return storages;
  }

  /** @return an iterator of the datanodes. */
  public Iterable<DatanodeStorageInfo> getStorages(final Block block) {
    return blocksMap.getStorages(block);
  }

  public int getTotalBlocks() {
    return blocksMap.size();
  }

  public void removeBlock(BlockInfo block) {
    assert namesystem.hasWriteLock();
    // No need to ACK blocks that are being removed entirely
    // from the namespace, since the removal of the associated
    // file already removes them from the block map below.
    block.setNumBytes(BlockCommand.NO_ACK);
    addToInvalidates(block);
    removeBlockFromMap(block);
    // Remove the block from pendingReconstruction and neededReconstruction
    PendingBlockInfo remove = pendingReconstruction.remove(block);
    if (remove != null) {
      DatanodeStorageInfo.decrementBlocksScheduled(remove.getTargets()
          .toArray(new DatanodeStorageInfo[remove.getTargets().size()]));
    }
    neededReconstruction.remove(block, LowRedundancyBlocks.LEVEL);
    postponedMisreplicatedBlocks.remove(block);
  }

  public BlockInfo getStoredBlock(Block block) {
    if (!BlockIdManager.isStripedBlockID(block.getBlockId())) {
      return blocksMap.getStoredBlock(block);
    }
    if (!hasNonEcBlockUsingStripedID) {
      return blocksMap.getStoredBlock(
          new Block(BlockIdManager.convertToStripedID(block.getBlockId())));
    }
    BlockInfo info = blocksMap.getStoredBlock(block);
    if (info != null) {
      return info;
    }
    return blocksMap.getStoredBlock(
        new Block(BlockIdManager.convertToStripedID(block.getBlockId())));
  }

  public void updateLastBlock(BlockInfo lastBlock, ExtendedBlock newBlock) {
    lastBlock.setNumBytes(newBlock.getNumBytes());
    List<ReplicaUnderConstruction> staleReplicas = lastBlock
        .setGenerationStampAndVerifyReplicas(newBlock.getGenerationStamp());
    removeStaleReplicas(staleReplicas, lastBlock);
  }

  /** updates a block in needed reconstruction queue. */
  private void updateNeededReconstructions(final BlockInfo block,
      final int curReplicasDelta, int expectedReplicasDelta) {
    namesystem.writeLock();
    try {
      if (!isPopulatingReplQueues() || !block.isComplete()) {
        return;
      }
      NumberReplicas repl = countNodes(block);
      int pendingNum = pendingReconstruction.getNumReplicas(block);
      int curExpectedReplicas = getExpectedRedundancyNum(block);
      if (!hasEnoughEffectiveReplicas(block, repl, pendingNum)) {
        neededReconstruction.update(block, repl.liveReplicas() + pendingNum,
            repl.readOnlyReplicas(), repl.outOfServiceReplicas(),
            curExpectedReplicas, curReplicasDelta, expectedReplicasDelta);
      } else {
        int oldReplicas = repl.liveReplicas() + pendingNum - curReplicasDelta;
        int oldExpectedReplicas = curExpectedReplicas-expectedReplicasDelta;
        neededReconstruction.remove(block, oldReplicas, repl.readOnlyReplicas(),
            repl.outOfServiceReplicas(), oldExpectedReplicas);
      }
    } finally {
      namesystem.writeUnlock("updateNeededReconstructions");
    }
  }

  /**
   * Check sufficient redundancy of the blocks in the collection. If any block
   * is needed reconstruction, insert it into the reconstruction queue.
   * Otherwise, if the block is more than the expected replication factor,
   * process it as an extra redundancy block.
   */
  public void checkRedundancy(BlockCollection bc) {
    for (BlockInfo block : bc.getBlocks()) {
      short expected = getExpectedRedundancyNum(block);
      final NumberReplicas n = countNodes(block);
      final int pending = pendingReconstruction.getNumReplicas(block);
      if (!hasEnoughEffectiveReplicas(block, n, pending)) {
        neededReconstruction.add(block, n.liveReplicas() + pending,
            n.readOnlyReplicas(), n.outOfServiceReplicas(), expected);
      } else if (shouldProcessExtraRedundancy(n, expected)) {
        processExtraRedundancyBlock(block, expected, null, null);
      }
    }
  }

  /**
   * Get blocks to invalidate for {@code nodeId}
   * in {@link #invalidateBlocks}.
   *
   * @return number of blocks scheduled for removal during this iteration.
   */
  private int invalidateWorkForOneNode(DatanodeInfo dn) {
    final List<Block> toInvalidate;
    
    namesystem.writeLock();
    try {
      // blocks should not be replicated or removed if safe mode is on
      if (namesystem.isInSafeMode()) {
        LOG.debug("In safemode, not computing reconstruction work");
        return 0;
      }
      try {
        DatanodeDescriptor dnDescriptor = datanodeManager.getDatanode(dn);
        if (dnDescriptor == null) {
          LOG.warn("DataNode {} cannot be found with UUID {}" +
              ", removing block invalidation work.", dn, dn.getDatanodeUuid());
          invalidateBlocks.remove(dn);
          return 0;
        }
        toInvalidate = invalidateBlocks.invalidateWork(dnDescriptor);
        
        if (toInvalidate == null) {
          return 0;
        }
      } catch(UnregisteredNodeException une) {
        return 0;
      }
    } finally {
      namesystem.writeUnlock("invalidateWorkForOneNode");
    }
    if (blockLog.isDebugEnabled()) {
      blockLog.debug("BLOCK* {}: ask {} to delete {}",
          getClass().getSimpleName(), dn, toInvalidate);
    }
    return toInvalidate.size();
  }

  @VisibleForTesting
  public boolean containsInvalidateBlock(final DatanodeInfo dn,
      final Block block) {
    return invalidateBlocks.contains(dn, block);
  }

  boolean isPlacementPolicySatisfied(BlockInfo storedBlock) {
    return getBlockPlacementStatus(storedBlock, null)
        .isPlacementPolicySatisfied();
  }

  BlockPlacementStatus getBlockPlacementStatus(BlockInfo storedBlock) {
    return getBlockPlacementStatus(storedBlock, null);
  }

  BlockPlacementStatus getBlockPlacementStatus(BlockInfo storedBlock,
      DatanodeStorageInfo[] additionalStorage) {
    List<DatanodeDescriptor> liveNodes = new ArrayList<>();
    if (additionalStorage != null) {
      // additionalNodes, are potential new targets for the block. If there are
      // any passed, include them when checking the placement policy to see if
      // the policy is met, when it may not have been met without these nodes.
      for (DatanodeStorageInfo s : additionalStorage) {
        liveNodes.add(getDatanodeDescriptorFromStorage(s));
      }
    }
    Collection<DatanodeDescriptor> corruptNodes = corruptReplicas
        .getNodes(storedBlock);
    for (DatanodeStorageInfo storage : blocksMap.getStorages(storedBlock)) {
      if (storage.getStorageType() == StorageType.PROVIDED
          && storage.getState() == State.NORMAL) {
        // assume the policy is satisfied for blocks on PROVIDED storage
        // as long as the storage is in normal state.
        return new BlockPlacementStatus() {
          @Override
          public boolean isPlacementPolicySatisfied() {
            return true;
          }

          @Override
          public String getErrorDescription() {
            return null;
          }

          @Override
          public int getAdditionalReplicasRequired() {
            return 0;
          }
        };
      }
      final DatanodeDescriptor cur = getDatanodeDescriptorFromStorage(storage);
      // Nodes under maintenance should be counted as valid replicas from
      // rack policy point of view.
      if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()
          && ((corruptNodes == null) || !corruptNodes.contains(cur))) {
        liveNodes.add(cur);
      }
    }
    DatanodeInfo[] locs = liveNodes.toArray(new DatanodeInfo[liveNodes.size()]);
    BlockType blockType = storedBlock.getBlockType();
    BlockPlacementPolicy placementPolicy = placementPolicies
        .getPolicy(blockType);
    int numReplicas = blockType == STRIPED ? ((BlockInfoStriped) storedBlock)
        .getRealTotalBlockNum() : storedBlock.getReplication();
    return placementPolicy.verifyBlockPlacement(locs, numReplicas);
  }

  boolean isNeededReconstructionForMaintenance(BlockInfo storedBlock,
      NumberReplicas numberReplicas) {
    return storedBlock.isComplete() && (numberReplicas.liveReplicas() <
        getMinMaintenanceStorageNum(storedBlock) ||
        !isPlacementPolicySatisfied(storedBlock));
  }

  boolean isNeededReconstruction(BlockInfo storedBlock,
      NumberReplicas numberReplicas) {
    return isNeededReconstruction(storedBlock, numberReplicas, 0);
  }

  /**
   * A block needs reconstruction if the number of redundancies is less than
   * expected or if it does not have enough racks.
   */
  boolean isNeededReconstruction(BlockInfo storedBlock,
      NumberReplicas numberReplicas, int pending) {
    return storedBlock.isComplete() &&
        !hasEnoughEffectiveReplicas(storedBlock, numberReplicas, pending);
  }

  // Exclude maintenance, but make sure it has minimal live replicas
  // to satisfy the maintenance requirement.
  public short getExpectedLiveRedundancyNum(BlockInfo block,
      NumberReplicas numberReplicas) {
    final short expectedRedundancy = getExpectedRedundancyNum(block);
    return (short)Math.max(expectedRedundancy -
        numberReplicas.maintenanceReplicas(),
        getMinMaintenanceStorageNum(block));
  }

  public short getExpectedRedundancyNum(BlockInfo block) {
    return block.isStriped() ?
        ((BlockInfoStriped) block).getRealTotalBlockNum() :
        block.getReplication();
  }

  public long getMissingBlocksCount() {
    // not locking
    return this.neededReconstruction.getCorruptBlockSize();
  }

  public long getMissingReplOneBlocksCount() {
    // not locking
    return this.neededReconstruction.getCorruptReplicationOneBlockSize();
  }

  public long getHighestPriorityReplicatedBlockCount(){
    return this.neededReconstruction.getHighestPriorityReplicatedBlockCount();
  }

  public long getHighestPriorityECBlockCount(){
    return this.neededReconstruction.getHighestPriorityECBlockCount();
  }

  public BlockInfo addBlockCollection(BlockInfo block,
      BlockCollection bc) {
    BlockInfo blockInfo = blocksMap.addBlockCollection(block, bc);
    blockIdManager.setGenerationStampIfGreater(block.getGenerationStamp());
    return blockInfo;
  }

  /**
   * Do some check when adding a block to blocksmap.
   * For HDFS-7994 to check whether then block is a NonEcBlockUsingStripedID.
   *
   */
  public BlockInfo addBlockCollectionWithCheck(
      BlockInfo block, BlockCollection bc) {
    if (!hasNonEcBlockUsingStripedID && !block.isStriped() &&
        BlockIdManager.isStripedBlockID(block.getBlockId())) {
      hasNonEcBlockUsingStripedID = true;
    }
    return addBlockCollection(block, bc);
  }

  BlockCollection getBlockCollection(BlockInfo b) {
    return namesystem.getBlockCollection(b.getBlockCollectionId());
  }

  public int numCorruptReplicas(Block block) {
    return corruptReplicas.numCorruptReplicas(block);
  }

  public void removeBlockFromMap(BlockInfo block) {
    for(DatanodeStorageInfo info : blocksMap.getStorages(block)) {
      excessRedundancyMap.remove(info.getDatanodeDescriptor(), block);
    }

    blocksMap.removeBlock(block);
    // If block is removed from blocksMap remove it from corruptReplicasMap
    corruptReplicas.removeFromCorruptReplicasMap(block);
  }

  public int getCapacity() {
    return blocksMap.getCapacity();
  }

  /**
   * Return an iterator over the set of blocks for which there are no replicas.
   */
  public Iterator<BlockInfo> getCorruptReplicaBlockIterator() {
    return neededReconstruction.iterator(
        LowRedundancyBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
  }

  /**
   * Get the replicas which are corrupt for a given block.
   */
  public Collection<DatanodeDescriptor> getCorruptReplicas(Block block) {
    return corruptReplicas.getNodes(block);
  }

 /**
  * Get reason for certain corrupted replicas for a given block and a given dn.
  */
 public String getCorruptReason(Block block, DatanodeDescriptor node) {
   return corruptReplicas.getCorruptReason(block, node);
 }

  /** @return the size of UnderReplicatedBlocks */
  public int numOfUnderReplicatedBlocks() {
    return neededReconstruction.size();
  }

  /**
   * Used as ad hoc to check the time stamp of the last full cycle of
   * {@link #redundancyThread}. This is used by the Junit tests to block until
   * {@link #lastRedundancyCycleTS} is updated.
   * @return the current {@link #lastRedundancyCycleTS}.
   */
  @VisibleForTesting
  public long getLastRedundancyMonitorTS() {
    return lastRedundancyCycleTS.get();
  }

  /**
   * Periodically deletes the marked block.
   */
  private class MarkedDeleteBlockScrubber implements Runnable {
    private Iterator<BlockInfo> toDeleteIterator = null;
    private boolean isSleep;
    private NameNodeMetrics metrics;

    private void remove(long time) {
      if (checkToDeleteIterator()) {
        namesystem.writeLock();
        try {
          while (toDeleteIterator.hasNext()) {
            removeBlock(toDeleteIterator.next());
            metrics.decrPendingDeleteBlocksCount();
            if (Time.monotonicNow() - time > deleteBlockLockTimeMs) {
              isSleep = true;
              break;
            }
          }
        } finally {
          namesystem.writeUnlock("markedDeleteBlockScrubberThread");
        }
      }
    }

    private boolean checkToDeleteIterator() {
      return toDeleteIterator != null && toDeleteIterator.hasNext();
    }

    @Override
    public void run() {
      LOG.info("Start MarkedDeleteBlockScrubber thread");
      while (namesystem.isRunning() &&
          !Thread.currentThread().isInterrupted()) {
        if (!markedDeleteQueue.isEmpty() || checkToDeleteIterator()) {
          try {
            metrics = NameNode.getNameNodeMetrics();
            metrics.setDeleteBlocksQueued(markedDeleteQueue.size());
            isSleep = false;
            long startTime = Time.monotonicNow();
            remove(startTime);
            while (!isSleep && !markedDeleteQueue.isEmpty() &&
                !Thread.currentThread().isInterrupted()) {
              List<BlockInfo> markedDeleteList = markedDeleteQueue.poll();
              if (markedDeleteList != null) {
                toDeleteIterator = markedDeleteList.listIterator();
              }
              remove(startTime);
            }
          } catch (Exception e){
            LOG.warn("MarkedDeleteBlockScrubber encountered an exception" +
                " during the block deletion process, " +
                " the deletion of the block will retry in {} millisecond.",
                deleteBlockUnlockIntervalTimeMs, e);
          }
        }
        if (isSleep) {
          LOG.debug("Clear markedDeleteQueue over {} millisecond to release the write lock",
              deleteBlockLockTimeMs);
        }
        try {
          Thread.sleep(deleteBlockUnlockIntervalTimeMs);
        } catch (InterruptedException e) {
          LOG.info("Stopping MarkedDeleteBlockScrubber.");
          break;
        }
      }
    }
  }

  /**
   * Periodically calls computeBlockRecoveryWork().
   */
  private class RedundancyMonitor implements Runnable {

    @Override
    public void run() {
      while (namesystem.isRunning()) {
        try {
          // Process recovery work only when active NN is out of safe mode.
          if (isPopulatingReplQueues()) {
            computeDatanodeWork();
            processPendingReconstructions();
            rescanPostponedMisreplicatedBlocks();
            lastRedundancyCycleTS.set(Time.monotonicNow());
          }
          TimeUnit.MILLISECONDS.sleep(redundancyRecheckIntervalMs);
        } catch (Throwable t) {
          if (!namesystem.isRunning()) {
            LOG.info("Stopping RedundancyMonitor.");
            if (!(t instanceof InterruptedException)) {
              LOG.info("RedundancyMonitor received an exception"
                  + " while shutting down.", t);
            }
            break;
          } else if (!checkNSRunning && t instanceof InterruptedException) {
            LOG.info("Stopping RedundancyMonitor for testing.");
            break;
          }
          LOG.error("RedundancyMonitor thread received Runtime exception. ",
              t);
          terminate(1, t);
        }
      }
    }
  }


  /**
   * Compute block replication and block invalidation work that can be scheduled
   * on data-nodes. The datanode will be informed of this work at the next
   * heartbeat.
   * 
   * @return number of blocks scheduled for replication or removal.
   */
  int computeDatanodeWork() {
    // Blocks should not be replicated or removed if in safe mode.
    // It's OK to check safe mode here w/o holding lock, in the worst
    // case extra replications will be scheduled, and these will get
    // fixed up later.
    if (namesystem.isInSafeMode()) {
      return 0;
    }

    final int numlive = heartbeatManager.getLiveDatanodeCount();
    final int blocksToProcess = numlive
        * this.blocksReplWorkMultiplier;
    final int nodesToProcess = (int) Math.ceil(numlive
        * this.blocksInvalidateWorkPct);

    int workFound = this.computeBlockReconstructionWork(blocksToProcess);

    // Update counters
    namesystem.writeLock();
    try {
      this.updateState();
      this.scheduledReplicationBlocksCount = workFound;
    } finally {
      namesystem.writeUnlock("computeDatanodeWork");
    }
    workFound += this.computeInvalidateWork(nodesToProcess);
    return workFound;
  }

  /**
   * Clear all queues that hold decisions previously made by
   * this NameNode.
   */
  public void clearQueues() {
    neededReconstruction.clear();
    pendingReconstruction.clear();
    excessRedundancyMap.clear();
    invalidateBlocks.clear();
    datanodeManager.clearPendingQueues();
    postponedMisreplicatedBlocks.clear();
  };

  public static LocatedBlock newLocatedBlock(
      ExtendedBlock b, DatanodeStorageInfo[] storages,
      long startOffset, boolean corrupt) {
    // startOffset is unknown
    return new LocatedBlock(
        b, DatanodeStorageInfo.toDatanodeInfos(storages),
        DatanodeStorageInfo.toStorageIDs(storages),
        DatanodeStorageInfo.toStorageTypes(storages),
        startOffset, corrupt,
        null);
  }

  public static LocatedStripedBlock newLocatedStripedBlock(
      ExtendedBlock b, DatanodeStorageInfo[] storages,
      byte[] indices, long startOffset, boolean corrupt) {
    // startOffset is unknown
    return new LocatedStripedBlock(
        b, DatanodeStorageInfo.toDatanodeInfos(storages),
        DatanodeStorageInfo.toStorageIDs(storages),
        DatanodeStorageInfo.toStorageTypes(storages),
        indices, startOffset, corrupt,
        null);
  }

  public static LocatedBlock newLocatedBlock(ExtendedBlock eb, BlockInfo info,
      DatanodeStorageInfo[] locs, long offset) throws IOException {
    final LocatedBlock lb;
    if (info.isStriped()) {
      lb = newLocatedStripedBlock(eb, locs,
          info.getUnderConstructionFeature().getBlockIndices(),
          offset, false);
    } else {
      lb = newLocatedBlock(eb, locs, offset, false);
    }
    return lb;
  }

  /**
   * A simple result enum for the result of
   * {@link BlockManager#processMisReplicatedBlock(BlockInfo)}.
   */
  enum MisReplicationResult {
    /** The block should be invalidated since it belongs to a deleted file. */
    INVALID,
    /** The block is currently under-replicated. */
    UNDER_REPLICATED,
    /** The block is currently over-replicated. */
    OVER_REPLICATED,
    /** A decision can't currently be made about this block. */
    POSTPONE,
    /** The block is under construction, so should be ignored. */
    UNDER_CONSTRUCTION,
    /** The block is properly replicated. */
    OK
  }

  public void shutdown() {
    stopReconstructionInitializer();
    blocksMap.close();
    MBeans.unregister(mxBeanName);
    mxBeanName = null;
  }
  
  public void clear() {
    blockIdManager.clear();
    clearQueues();
    blocksMap.clear();
  }

  public BlockReportLeaseManager getBlockReportLeaseManager() {
    return blockReportLeaseManager;
  }

  @Override // BlockStatsMXBean
  public Map<StorageType, StorageTypeStats> getStorageTypeStats() {
    return  datanodeManager.getDatanodeStatistics().getStorageTypeStats();
  }

  /**
   * Initialize replication queues.
   */
  public void initializeReplQueues() {
    LOG.info("initializing replication queues");
    processMisReplicatedBlocks();
    initializedReplQueues = true;
  }

  /**
   * Check if replication queues are to be populated
   * @return true when node is HAState.Active and not in the very first safemode
   */
  public boolean isPopulatingReplQueues() {
    if (!shouldPopulateReplQueues()) {
      return false;
    }
    return initializedReplQueues;
  }

  public void setInitializedReplQueues(boolean v) {
    this.initializedReplQueues = v;
  }

  public boolean shouldPopulateReplQueues() {
    HAContext haContext = namesystem.getHAContext();
    if (haContext == null || haContext.getState() == null)
      return false;
    return haContext.getState().shouldPopulateReplQueues();
  }

  boolean getShouldPostponeBlocksFromFuture() {
    return shouldPostponeBlocksFromFuture;
  }

  // async processing of an action, used for IBRs.
  public void enqueueBlockOp(final Runnable action) throws IOException {
    try {
      blockReportThread.enqueue(action);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  // sync batch processing for a full BR.
  public <T> T runBlockOp(final Callable<T> action)
      throws IOException {
    final FutureTask<T> future = new FutureTask<T>(action);
    enqueueBlockOp(future);
    try {
      return future.get();
    } catch (ExecutionException ee) {
      Throwable cause = ee.getCause();
      if (cause == null) {
        cause = ee;
      }
      if (!(cause instanceof IOException)) {
        cause = new IOException(cause);
      }
      throw (IOException)cause;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException(ie);
    }
  }

  /**
   * Notification of a successful block recovery.
   * @param block for which the recovery succeeded
   */
  public void successfulBlockRecovery(BlockInfo block) {
    pendingRecoveryBlocks.remove(block);
  }

  /**
   * Checks whether a recovery attempt has been made for the given block.
   * If so, checks whether that attempt has timed out.
   * @param b block for which recovery is being attempted
   * @return true if no recovery attempt has been made or
   *         the previous attempt timed out
   */
  public boolean addBlockRecoveryAttempt(BlockInfo b) {
    return pendingRecoveryBlocks.add(b);
  }

  @VisibleForTesting
  public void flushBlockOps() throws IOException {
    runBlockOp(new Callable<Void>(){
      @Override
      public Void call() {
        return null;
      }
    });
  }

  public int getBlockOpQueueLength() {
    return blockReportThread.queue.size();
  }

  private class BlockReportProcessingThread extends Thread {
    private long lastFull = 0;

    private final BlockingQueue<Runnable> queue;

    BlockReportProcessingThread(int size) {
      super("Block report processor");
      queue = new ArrayBlockingQueue<>(size);
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        processQueue();
      } catch (Throwable t) {
        LOG.error("Error while processing the block report, terminating the "
            + "process", t);
        ExitUtil.terminate(1,
            getName() + " encountered fatal exception: " + t);
      }
    }

    private void processQueue() {
      while (namesystem.isRunning()) {
        NameNodeMetrics metrics = NameNode.getNameNodeMetrics();
        try {
          Runnable action = queue.take();
          // batch as many operations in the write lock until the queue
          // runs dry, or the max lock hold is reached.
          int processed = 0;
          namesystem.writeLock();
          metrics.setBlockOpsQueued(queue.size() + 1);
          try {
            long start = Time.monotonicNow();
            do {
              processed++;
              action.run();
              if (Time.monotonicNow() - start > maxLockHoldTime) {
                break;
              }
              action = queue.poll();
            } while (action != null);
          } finally {
            namesystem.writeUnlock("processQueue");
            metrics.addBlockOpsBatched(processed - 1);
          }
        } catch (InterruptedException e) {
          // ignore unless thread was specifically interrupted.
          if (Thread.interrupted()) {
            break;
          }
        }
      }
      queue.clear();
    }

    void enqueue(Runnable action) throws InterruptedException {
      if (!queue.offer(action)) {
        if (!isAlive() && namesystem.isRunning()) {
          ExitUtil.terminate(1, getName()+" is not running");
        }
        long now = Time.monotonicNow();
        if (now - lastFull > 4000) {
          lastFull = now;
          LOG.info("Block report queue is full");
        }
        queue.put(action);
      }
    }
  }

  /**
   * @return redundancy thread.
   */
  @VisibleForTesting
  Daemon getRedundancyThread() {
    return redundancyThread;
  }

  public BlockIdManager getBlockIdManager() {
    return blockIdManager;
  }

  @VisibleForTesting
  public ConcurrentLinkedQueue<List<BlockInfo>> getMarkedDeleteQueue() {
    return markedDeleteQueue;
  }

  public void addBLocksToMarkedDeleteQueue(List<BlockInfo> blockInfos) {
    markedDeleteQueue.add(blockInfos);
    NameNode.getNameNodeMetrics().
        incrPendingDeleteBlocksCount(blockInfos.size());
  }

  public long nextGenerationStamp(boolean legacyBlock) throws IOException {
    return blockIdManager.nextGenerationStamp(legacyBlock);
  }

  public boolean isLegacyBlock(Block block) {
    return blockIdManager.isLegacyBlock(block);
  }

  public long nextBlockId(BlockType blockType) {
    return blockIdManager.nextBlockId(blockType);
  }

  boolean isGenStampInFuture(Block block) {
    return blockIdManager.isGenStampInFuture(block);
  }

  boolean isReplicaCorrupt(BlockInfo blk, DatanodeDescriptor d) {
    return corruptReplicas.isReplicaCorrupt(blk, d);
  }

  private int setBlockIndices(BlockInfo blk, byte[] blockIndices, int i,
                              DatanodeStorageInfo storage) {
    // TODO this can be more efficient
    if (blockIndices != null) {
      byte index = ((BlockInfoStriped)blk).getStorageBlockIndex(storage);
      assert index >= 0;
      blockIndices[i++] = index;
    }
    return i;
  }

  private static long getBlockRecoveryTimeout(long heartbeatIntervalSecs) {
    return TimeUnit.SECONDS.toMillis(heartbeatIntervalSecs *
        BLOCK_RECOVERY_TIMEOUT_MULTIPLIER);
  }

  @VisibleForTesting
  public void setBlockRecoveryTimeout(long blockRecoveryTimeout) {
    pendingRecoveryBlocks.setRecoveryTimeoutInterval(blockRecoveryTimeout);
  }

  @VisibleForTesting
  public ProvidedStorageMap getProvidedStorageMap() {
    return providedStorageMap;
  }

  /**
   * Create SPS manager instance. It manages the user invoked sps paths and does
   * the movement.
   *
   * @param conf
   *          configuration
   * @return true if the instance is successfully created, false otherwise.
   */
  private boolean createSPSManager(final Configuration conf) {
    return createSPSManager(conf, null);
  }

  /**
   * Create SPS manager instance. It manages the user invoked sps paths and does
   * the movement.
   *
   * @param conf
   *          configuration
   * @param spsMode
   *          satisfier mode
   * @return true if the instance is successfully created, false otherwise.
   */
  public boolean createSPSManager(final Configuration conf,
      final String spsMode) {
    // sps manager manages the user invoked sps paths and does the movement.
    // StoragePolicySatisfier(SPS) configs
    boolean storagePolicyEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY,
        DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_DEFAULT);
    String modeVal = spsMode;
    if (org.apache.commons.lang3.StringUtils.isBlank(modeVal)) {
      modeVal = conf.get(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
          DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT);
    }
    StoragePolicySatisfierMode mode = StoragePolicySatisfierMode
        .fromString(modeVal);
    if (!storagePolicyEnabled || mode == StoragePolicySatisfierMode.NONE) {
      LOG.info("Storage policy satisfier is disabled");
      return false;
    }
    spsManager = new StoragePolicySatisfyManager(conf, namesystem);
    return true;
  }

  /**
   * Nullify SPS manager as this feature is disabled fully.
   */
  public void disableSPS() {
    spsManager = null;
  }

  /**
   * @return sps manager.
   */
  public StoragePolicySatisfyManager getSPSManager() {
    return spsManager;
  }

  public void setExcludeSlowNodesEnabled(boolean enable) {
    placementPolicies.getPolicy(CONTIGUOUS).setExcludeSlowNodesEnabled(enable);
    placementPolicies.getPolicy(STRIPED).setExcludeSlowNodesEnabled(enable);
  }

  @VisibleForTesting
  public boolean getExcludeSlowNodesEnabled(BlockType blockType) {
    return placementPolicies.getPolicy(blockType).getExcludeSlowNodesEnabled();
  }

  public void setMinBlocksForWrite(int minBlocksForWrite) {
    ensurePositiveInt(minBlocksForWrite,
        DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY);
    placementPolicies.getPolicy(CONTIGUOUS).setMinBlocksForWrite(minBlocksForWrite);
    placementPolicies.getPolicy(STRIPED).setMinBlocksForWrite(minBlocksForWrite);
  }

  @VisibleForTesting
  public int getMinBlocksForWrite(BlockType blockType) {
    return placementPolicies.getPolicy(blockType).getMinBlocksForWrite();
  }
}
