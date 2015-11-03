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

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.ObjectName;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplicasMap.Reason;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo.AddBlockResult;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingDataNodeMessages.ReportedBlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
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
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;

import static org.apache.hadoop.hdfs.util.StripedBlockUtil.getInternalBlockLength;

import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps information related to the blocks stored in the Hadoop cluster.
 */
@InterfaceAudience.Private
public class BlockManager implements BlockStatsMXBean {

  public static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);
  public static final Logger blockLog = NameNode.blockStateChangeLog;

  private static final String QUEUE_REASON_CORRUPT_STATE =
    "it has the wrong state or generation stamp";

  private static final String QUEUE_REASON_FUTURE_GENSTAMP =
    "generation stamp is in the future";

  private final Namesystem namesystem;

  private final DatanodeManager datanodeManager;
  private final HeartbeatManager heartbeatManager;
  private final BlockTokenSecretManager blockTokenSecretManager;

  private final PendingDataNodeMessages pendingDNMessages =
    new PendingDataNodeMessages();

  private volatile long pendingReplicationBlocksCount = 0L;
  private volatile long corruptReplicaBlocksCount = 0L;
  private volatile long underReplicatedBlocksCount = 0L;
  private volatile long scheduledReplicationBlocksCount = 0L;

  /** flag indicating whether replication queues have been initialized */
  private boolean initializedReplQueues;

  private final AtomicLong excessBlocksCount = new AtomicLong(0L);
  private final AtomicLong postponedMisreplicatedBlocksCount = new AtomicLong(0L);
  private final long startupDelayBlockDeletionInMs;
  private final BlockReportLeaseManager blockReportLeaseManager;
  private ObjectName mxBeanName;

  /** Used by metrics */
  public long getPendingReplicationBlocksCount() {
    return pendingReplicationBlocksCount;
  }
  /** Used by metrics */
  public long getUnderReplicatedBlocksCount() {
    return underReplicatedBlocksCount;
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
    return excessBlocksCount.get();
  }
  /** Used by metrics */
  public long getPostponedMisreplicatedBlocksCount() {
    return postponedMisreplicatedBlocksCount.get();
  }
  /** Used by metrics */
  public int getPendingDataNodeMessageCount() {
    return pendingDNMessages.count();
  }

  /**replicationRecheckInterval is how often namenode checks for new replication work*/
  private final long replicationRecheckInterval;
  
  /**
   * Mapping: Block -> { BlockCollection, datanodes, self ref }
   * Updated only in response to client-sent information.
   */
  final BlocksMap blocksMap;

  /** Replication thread. */
  final Daemon replicationThread = new Daemon(new ReplicationMonitor());
  
  /** Store blocks -> datanodedescriptor(s) map of corrupt replicas */
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
  private final LightWeightHashSet<Block> postponedMisreplicatedBlocks =
      new LightWeightHashSet<>();

  /**
   * Maps a StorageID to the set of blocks that are "extra" for this
   * DataNode. We'll eventually remove these extras.
   */
  public final Map<String, LightWeightHashSet<BlockInfo>> excessReplicateMap =
    new HashMap<>();

  /**
   * Store set of Blocks that need to be replicated 1 or more times.
   * We also store pending replication-orders.
   */
  public final UnderReplicatedBlocks neededReplications = new UnderReplicatedBlocks();

  @VisibleForTesting
  final PendingReplicationBlocks pendingReplications;

  /** The maximum number of replicas allowed for a block */
  public final short maxReplication;
  /**
   * The maximum number of outgoing replication streams a given node should have
   * at one time considering all but the highest priority replications needed.
    */
  int maxReplicationStreams;
  /**
   * The maximum number of outgoing replication streams a given node should have
   * at one time.
   */
  int replicationStreamsHardLimit;
  /** Minimum copies needed or else write is disallowed */
  public final short minReplication;
  /** Default number of replicas */
  public final int defaultReplication;
  /** value returned by MAX_CORRUPT_FILES_RETURNED */
  final int maxCorruptFilesReturned;

  final float blocksInvalidateWorkPct;
  final int blocksReplWorkMultiplier;

  // whether or not to issue block encryption keys.
  final boolean encryptDataTransfer;
  
  // Max number of blocks to log info about during a block report.
  private final long maxNumBlocksToLog;

  /**
   * When running inside a Standby node, the node may receive block reports
   * from datanodes before receiving the corresponding namespace edits from
   * the active NameNode. Thus, it will postpone them for later processing,
   * instead of marking the blocks as corrupt.
   */
  private boolean shouldPostponeBlocksFromFuture = false;

  /**
   * Process replication queues asynchronously to allow namenode safemode exit
   * and failover to be faster. HDFS-5496
   */
  private Daemon replicationQueuesInitializer = null;
  /**
   * Number of blocks to process asychronously for replication queues
   * initialization once aquired the namesystem lock. Remaining blocks will be
   * processed again after aquiring lock again.
   */
  private int numBlocksPerIteration;
  /**
   * Progress of the Replication queues initialisation.
   */
  private double replicationQueuesInitProgress = 0.0;

  /** for block replicas placement */
  private BlockPlacementPolicies placementPolicies;
  private final BlockStoragePolicySuite storagePolicySuite;

  /** Check whether name system is running before terminating */
  private boolean checkNSRunning = true;

  /** Check whether there are any non-EC blocks using StripedID */
  private boolean hasNonEcBlockUsingStripedID = false;

  /** Keeps track of how many bytes are in Future Generation blocks. */
  private AtomicLong numberOfBytesInFutureBlocks;

  /** Reports if Name node was started with Rollback option. */
  private boolean inRollBack = false;

  public BlockManager(final Namesystem namesystem, final Configuration conf)
    throws IOException {
    this.namesystem = namesystem;
    datanodeManager = new DatanodeManager(this, namesystem, conf);
    heartbeatManager = datanodeManager.getHeartbeatManager();

    startupDelayBlockDeletionInMs = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY,
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_DEFAULT) * 1000L;
    invalidateBlocks = new InvalidateBlocks(
        datanodeManager.blockInvalidateLimit, startupDelayBlockDeletionInMs);

    // Compute the map capacity by allocating 2% of total memory
    blocksMap = new BlocksMap(
        LightWeightGSet.computeCapacity(2.0, "BlocksMap"));
    placementPolicies = new BlockPlacementPolicies(
      conf, datanodeManager.getFSClusterStats(),
      datanodeManager.getNetworkTopology(),
      datanodeManager.getHost2DatanodeMap());
    storagePolicySuite = BlockStoragePolicySuite.createDefaultSuite();
    pendingReplications = new PendingReplicationBlocks(conf.getInt(
      DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
      DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_DEFAULT) * 1000L);

    blockTokenSecretManager = createBlockTokenSecretManager(conf);

    this.maxCorruptFilesReturned = conf.getInt(
      DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED_KEY,
      DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED);
    this.defaultReplication = conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 
                                          DFSConfigKeys.DFS_REPLICATION_DEFAULT);

    final int maxR = conf.getInt(DFSConfigKeys.DFS_REPLICATION_MAX_KEY, 
                                 DFSConfigKeys.DFS_REPLICATION_MAX_DEFAULT);
    final int minR = conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
                                 DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
    if (minR <= 0)
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY
          + " = " + minR + " <= 0");
    if (maxR > Short.MAX_VALUE)
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_REPLICATION_MAX_KEY
          + " = " + maxR + " > " + Short.MAX_VALUE);
    if (minR > maxR)
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY
          + " = " + minR + " > "
          + DFSConfigKeys.DFS_REPLICATION_MAX_KEY
          + " = " + maxR);
    this.minReplication = (short)minR;
    this.maxReplication = (short)maxR;

    this.maxReplicationStreams =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT);
    this.replicationStreamsHardLimit =
        conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT);
    this.blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
    this.blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);

    this.replicationRecheckInterval = 
      conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 
                  DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000L;
    
    this.encryptDataTransfer =
        conf.getBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY,
            DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT);
    
    this.maxNumBlocksToLog =
        conf.getLong(DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_KEY,
            DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT);
    this.numBlocksPerIteration = conf.getInt(
        DFSConfigKeys.DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT,
        DFSConfigKeys.DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT_DEFAULT);
    this.blockReportLeaseManager = new BlockReportLeaseManager(conf);
    this.numberOfBytesInFutureBlocks = new AtomicLong();
    this.inRollBack = isInRollBackMode(NameNode.getStartupOption(conf));

    LOG.info("defaultReplication         = " + defaultReplication);
    LOG.info("maxReplication             = " + maxReplication);
    LOG.info("minReplication             = " + minReplication);
    LOG.info("maxReplicationStreams      = " + maxReplicationStreams);
    LOG.info("replicationRecheckInterval = " + replicationRecheckInterval);
    LOG.info("encryptDataTransfer        = " + encryptDataTransfer);
    LOG.info("maxNumBlocksToLog          = " + maxNumBlocksToLog);
  }

  private static BlockTokenSecretManager createBlockTokenSecretManager(
      final Configuration conf) throws IOException {
    final boolean isEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);
    LOG.info(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + "=" + isEnabled);

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
    LOG.info(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY
        + "=" + updateMin + " min(s), "
        + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY
        + "=" + lifetimeMin + " min(s), "
        + DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY
        + "=" + encryptionAlgorithm);
    
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    boolean isHaEnabled = HAUtil.isHAEnabled(conf, nsId);

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
          lifetimeMin * 60 * 1000L, nnIndex, nnIds.size(), null, encryptionAlgorithm);
    } else {
      return new BlockTokenSecretManager(updateMin*60*1000L,
          lifetimeMin*60*1000L, 0, 1, null, encryptionAlgorithm);
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
    if (isBlockTokenEnabled()) {
      blockTokenSecretManager.setBlockPoolId(blockPoolId);
    }
  }

  public BlockStoragePolicySuite getStoragePolicySuite() {
    return storagePolicySuite;
  }

  /** get the BlockTokenSecretManager */
  @VisibleForTesting
  public BlockTokenSecretManager getBlockTokenSecretManager() {
    return blockTokenSecretManager;
  }

  /** Allow silent termination of replication monitor for testing */
  @VisibleForTesting
  void enableRMTerminationForTesting() {
    checkNSRunning = false;
  }

  private boolean isBlockTokenEnabled() {
    return blockTokenSecretManager != null;
  }

  /** Should the access keys be updated? */
  boolean shouldUpdateBlockKey(final long updateTime) throws IOException {
    return isBlockTokenEnabled()? blockTokenSecretManager.updateKeys(updateTime)
        : false;
  }

  public void activate(Configuration conf) {
    pendingReplications.start();
    datanodeManager.activate(conf);
    this.replicationThread.setName("ReplicationMonitor");
    this.replicationThread.start();
    mxBeanName = MBeans.register("NameNode", "BlockStats", this);
  }

  public void close() {
    try {
      replicationThread.interrupt();
      replicationThread.join(3000);
    } catch (InterruptedException ie) {
    }
    datanodeManager.close();
    pendingReplications.stop();
    blocksMap.close();
  }

  /** @return the datanodeManager */
  public DatanodeManager getDatanodeManager() {
    return datanodeManager;
  }

  @VisibleForTesting
  public BlockPlacementPolicy getBlockPlacementPolicy() {
    return placementPolicies.getPolicy(false);
  }

  /** Dump meta data to out. */
  public void metaSave(PrintWriter out) {
    assert namesystem.hasWriteLock();
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    datanodeManager.fetchDatanodes(live, dead, false);
    out.println("Live Datanodes: " + live.size());
    out.println("Dead Datanodes: " + dead.size());
    //
    // Dump contents of neededReplication
    //
    synchronized (neededReplications) {
      out.println("Metasave: Blocks waiting for replication: " + 
                  neededReplications.size());
      for (Block block : neededReplications) {
        dumpBlockMeta(block, out);
      }
    }
    
    // Dump any postponed over-replicated blocks
    out.println("Mis-replicated blocks that have been postponed:");
    for (Block block : postponedMisreplicatedBlocks) {
      dumpBlockMeta(block, out);
    }

    // Dump blocks from pendingReplication
    pendingReplications.metaSave(out);

    // Dump blocks that are waiting to be deleted
    invalidateBlocks.dump(out);

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
    // source node returned is not used
    chooseSourceDatanodes(getStoredBlock(block), containingNodes,
        containingLiveReplicasNodes, numReplicas,
        new LinkedList<Short>(), UnderReplicatedBlocks.LEVEL);
    
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
              " l: " + numReplicas.liveReplicas() +
              " d: " + numReplicas.decommissionedAndDecommissioning() +
              " c: " + numReplicas.corruptReplicas() +
              " e: " + numReplicas.excessReplicas() + ") "); 

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
      }
      
      if (storage.areBlockContentsStale()) {
        state += " (block deletions maybe out of date)";
      }
      out.print(" " + node + state + " : ");
    }
    out.println("");
  }

  /** @return maxReplicationStreams */
  public int getMaxReplicationStreams() {
    return maxReplicationStreams;
  }

  public int getDefaultStorageNum(BlockInfo block) {
    if (block.isStriped()) {
      return ((BlockInfoStriped) block).getRealTotalBlockNum();
    } else {
      return defaultReplication;
    }
  }

  public short getMinStorageNum(BlockInfo block) {
    if (block.isStriped()) {
      return ((BlockInfoStriped) block).getRealDataBlockNum();
    } else {
      return minReplication;
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
  private static boolean commitBlock(final BlockInfo block,
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
    block.commitBlock(commitBlock);
    return true;
  }
  
  /**
   * Commit the last block of the file and mark it as complete if it has
   * meets the minimum replication requirement
   * 
   * @param bc block collection
   * @param commitBlock - contains client reported block length and generation
   * @return true if the last block is changed to committed state.
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  public boolean commitOrCompleteLastBlock(BlockCollection bc,
      Block commitBlock) throws IOException {
    if(commitBlock == null)
      return false; // not committing, this is a block allocation retry
    BlockInfo lastBlock = bc.getLastBlock();
    if(lastBlock == null)
      return false; // no blocks in file yet
    if(lastBlock.isComplete())
      return false; // already completed (e.g. by syncBlock)
    
    final boolean b = commitBlock(lastBlock, commitBlock);
    if (hasMinStorage(lastBlock)) {
      if (b && !bc.isStriped()) {
        addExpectedReplicasToPending(lastBlock);
      }
      completeBlock(lastBlock, false);
    }
    return b;
  }

  /**
   * If IBR is not sent from expected locations yet, add the datanodes to
   * pendingReplications in order to keep ReplicationMonitor from scheduling
   * the block.
   */
  private void addExpectedReplicasToPending(BlockInfo lastBlock) {
    DatanodeStorageInfo[] expectedStorages =
        lastBlock.getUnderConstructionFeature().getExpectedStorageLocations();
    if (expectedStorages.length - lastBlock.numNodes() > 0) {
      ArrayList<DatanodeDescriptor> pendingNodes =
          new ArrayList<DatanodeDescriptor>();
      for (DatanodeStorageInfo storage : expectedStorages) {
        DatanodeDescriptor dnd = storage.getDatanodeDescriptor();
        if (lastBlock.findStorageInfo(dnd) == null) {
          pendingNodes.add(dnd);
        }
      }
      pendingReplications.increment(lastBlock,
          pendingNodes.toArray(new DatanodeDescriptor[pendingNodes.size()]));
    }
  }

  /**
   * Convert a specified block of the file to a complete block.
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  private void completeBlock(BlockInfo curBlock, boolean force)
      throws IOException {
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

    curBlock.convertToCompleteBlock();
    // Since safe-mode only counts complete blocks, and we now have
    // one more complete block, we need to adjust the total up, and
    // also count it as safe, if we have at least the minimum replica
    // count. (We may not have the minimum replica count yet if this is
    // a "forced" completion when a file is getting closed by an
    // OP_CLOSE edit on the standby).
    namesystem.adjustSafeModeBlockTotals(0, 1);
    final int minStorage = curBlock.isStriped() ?
        ((BlockInfoStriped) curBlock).getRealDataBlockNum() : minReplication;
    namesystem.incrementSafeBlockCount(
        Math.min(numNodes, minStorage), curBlock);
  }

  /**
   * Force the given block in the given file to be marked as complete,
   * regardless of whether enough replicas are present. This is necessary
   * when tailing edit logs as a Standby.
   */
  public void forceCompleteBlock(final BlockInfo block) throws IOException {
    block.commitBlock(block);
    completeBlock(block, true);
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

    // Remove block from replication queue.
    NumberReplicas replicas = countNodes(lastBlock);
    neededReplications.remove(lastBlock, replicas.liveReplicas(),
        replicas.readOnlyReplicas(),
        replicas.decommissionedAndDecommissioning(), getReplication(lastBlock));
    pendingReplications.remove(lastBlock);

    // remove this block from the list of pending blocks to be deleted. 
    for (DatanodeStorageInfo storage : targets) {
      final Block b = getBlockOnStorage(lastBlock, storage);
      if (b != null) {
        invalidateBlocks.remove(storage.getDatanodeDescriptor(), b);
      }
    }
    
    // Adjust safe-mode totals, since under-construction blocks don't
    // count in safe-mode.
    namesystem.adjustSafeModeBlockTotals(
        // decrement safe if we had enough
        hasMinStorage(lastBlock, targets.length) ? -1 : 0,
        // always decrement total blocks
        -1);

    final long fileLength = bc.computeContentSummary(
        getStoragePolicySuite()).getLength();
    final long pos = fileLength - lastBlock.getNumBytes();
    return createLocatedBlock(lastBlock, pos,
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

  private List<LocatedBlock> createLocatedBlockList(final BlockInfo[] blocks,
      final long offset, final long length, final int nrBlocksToReturn,
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
      return Collections.emptyList();

    long endOff = offset + length;
    List<LocatedBlock> results = new ArrayList<>(blocks.length);
    do {
      results.add(createLocatedBlock(blocks[curBlk], curPos, mode));
      curPos += blocks[curBlk].getNumBytes();
      curBlk++;
    } while (curPos < endOff 
          && curBlk < blocks.length
          && results.size() < nrBlocksToReturn);
    return results;
  }

  private LocatedBlock createLocatedBlock(final BlockInfo[] blocks,
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
    
    return createLocatedBlock(blocks[curBlk], curPos, mode);
  }

  private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos,
    final AccessMode mode) throws IOException {
    final LocatedBlock lb = createLocatedBlock(blk, pos);
    if (mode != null) {
      setBlockToken(lb, mode);
    }
    return lb;
  }

  /** @return a LocatedBlock for the given block */
  private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos)
      throws IOException {
    if (!blk.isComplete()) {
      final BlockUnderConstructionFeature uc = blk.getUnderConstructionFeature();
      if (blk.isStriped()) {
        final DatanodeStorageInfo[] storages = uc.getExpectedStorageLocations();
        final ExtendedBlock eb = new ExtendedBlock(namesystem.getBlockPoolId(),
            blk);
        return newLocatedStripedBlock(eb, storages, uc.getBlockIndices(), pos,
            false);
      } else {
        final DatanodeStorageInfo[] storages = uc.getExpectedStorageLocations();
        final ExtendedBlock eb = new ExtendedBlock(namesystem.getBlockPoolId(),
            blk);
        return newLocatedBlock(eb, storages, pos, false);
      }
    }

    // get block locations
    final int numCorruptNodes = countNodes(blk).corruptReplicas();
    final int numCorruptReplicas = corruptReplicas.numCorruptReplicas(blk);
    if (numCorruptNodes != numCorruptReplicas) {
      LOG.warn("Inconsistent number of corrupt replicas for "
          + blk + " blockMap has " + numCorruptNodes
          + " but corrupt replicas map has " + numCorruptReplicas);
    }

    final int numNodes = blocksMap.numNodes(blk);
    final boolean isCorrupt = numCorruptNodes != 0 &&
        numCorruptNodes == numNodes;
    final int numMachines = isCorrupt ? numNodes: numNodes - numCorruptNodes;
    final DatanodeStorageInfo[] machines = new DatanodeStorageInfo[numMachines];
    final int[] blockIndices = blk.isStriped() ? new int[numMachines] : null;
    int j = 0, i = 0;
    if (numMachines > 0) {
      for(DatanodeStorageInfo storage : blocksMap.getStorages(blk)) {
        final DatanodeDescriptor d = storage.getDatanodeDescriptor();
        final boolean replicaCorrupt = corruptReplicas.isReplicaCorrupt(blk, d);
        if (isCorrupt || (!replicaCorrupt)) {
          machines[j++] = storage;
          // TODO this can be more efficient
          if (blockIndices != null) {
            int index = ((BlockInfoStriped) blk).getStorageBlockIndex(storage);
            assert index >= 0;
            blockIndices[i++] = index;
          }
        }
      }
    }
    assert j == machines.length :
      "isCorrupt: " + isCorrupt + 
      " numMachines: " + numMachines +
      " numNodes: " + numNodes +
      " numCorrupt: " + numCorruptNodes +
      " numCorruptRepls: " + numCorruptReplicas;
    final ExtendedBlock eb = new ExtendedBlock(namesystem.getBlockPoolId(), blk);
    return blockIndices == null ?
        newLocatedBlock(eb, machines, pos, isCorrupt) :
        newLocatedStripedBlock(eb, machines, blockIndices, pos, isCorrupt);
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
        LOG.debug("blocks = " + java.util.Arrays.asList(blocks));
      }
      final AccessMode mode = needBlockToken? BlockTokenIdentifier.AccessMode.READ: null;
      final List<LocatedBlock> locatedblocks = createLocatedBlockList(
          blocks, offset, length, Integer.MAX_VALUE, mode);

      final LocatedBlock lastlb;
      final boolean isComplete;
      if (!inSnapshot) {
        final BlockInfo last = blocks[blocks.length - 1];
        final long lastPos = last.isComplete()?
            fileSizeExcludeBlocksUnderConstruction - last.getNumBytes()
            : fileSizeExcludeBlocksUnderConstruction;
        lastlb = createLocatedBlock(last, lastPos, mode);
        isComplete = last.isComplete();
      } else {
        lastlb = createLocatedBlock(blocks,
            fileSizeExcludeBlocksUnderConstruction, mode);
        isComplete = true;
      }
      return new LocatedBlocks(fileSizeExcludeBlocksUnderConstruction,
          isFileUnderConstruction, locatedblocks, lastlb, isComplete, feInfo,
          ecPolicy);
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
        int[] indices = sb.getBlockIndices();
        Token<BlockTokenIdentifier>[] blockTokens = new Token[indices.length];
        ExtendedBlock internalBlock = new ExtendedBlock(b.getBlock());
        for (int i = 0; i < indices.length; i++) {
          internalBlock.setBlockId(b.getBlock().getBlockId() + indices[i]);
          blockTokens[i] = blockTokenSecretManager.generateToken(
              NameNode.getRemoteUser().getShortUserName(),
              internalBlock, EnumSet.of(mode));
        }
        sb.setBlockTokens(blockTokens);
      } else {
        b.setBlockToken(blockTokenSecretManager.generateToken(
            NameNode.getRemoteUser().getShortUserName(),
            b.getBlock(), EnumSet.of(mode)));
      }
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

    if (replication < minReplication || replication > maxReplication) {
      StringBuilder msg = new StringBuilder("Requested replication factor of ");

      msg.append(replication);

      if (replication > maxReplication) {
        msg.append(" exceeds maximum of ");
        msg.append(maxReplication);
      } else {
        msg.append(" is less than the required minimum of ");
        msg.append(minReplication);
      }

      msg.append(" for ").append(src);

      if (clientName != null) {
        msg.append(" from ").append(clientName);
      }

      throw new IOException(msg.toString());
    }
  }

  /**
   * Check if a block is replicated to at least the minimum replication.
   */
  public boolean isSufficientlyReplicated(BlockInfo b) {
    // Compare against the lesser of the minReplication and number of live DNs.
    final int replication =
        Math.min(minReplication, getDatanodeManager().getNumLiveDataNodes());
    return countNodes(b).liveReplicas() >= replication;
  }

  /**
   * return a list of blocks & their locations on <code>datanode</code> whose
   * total size is <code>size</code>
   * 
   * @param datanode on which blocks are located
   * @param size total size of blocks
   */
  public BlocksWithLocations getBlocks(DatanodeID datanode, long size
      ) throws IOException {
    namesystem.checkOperation(OperationCategory.READ);
    namesystem.readLock();
    try {
      namesystem.checkOperation(OperationCategory.READ);
      return getBlocksWithLocations(datanode, size);  
    } finally {
      namesystem.readUnlock();
    }
  }

  /** Get all blocks with location information from a datanode. */
  private BlocksWithLocations getBlocksWithLocations(final DatanodeID datanode,
      final long size) throws UnregisteredNodeException {
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
    Iterator<BlockInfo> iter = node.getBlockIterator();
    // starting from a random block
    int startBlock = ThreadLocalRandom.current().nextInt(numBlocks);
    // skip blocks
    for(int i=0; i<startBlock; i++) {
      iter.next();
    }
    List<BlockWithLocations> results = new ArrayList<BlockWithLocations>();
    long totalSize = 0;
    BlockInfo curBlock;
    while(totalSize<size && iter.hasNext()) {
      curBlock = iter.next();
      if(!curBlock.isComplete())  continue;
      totalSize += addBlock(curBlock, results);
    }
    if(totalSize<size) {
      iter = node.getBlockIterator(); // start from the beginning
      for(int i=0; i<startBlock&&totalSize<size; i++) {
        curBlock = iter.next();
        if(!curBlock.isComplete())  continue;
        totalSize += addBlock(curBlock, results);
      }
    }

    return new BlocksWithLocations(
        results.toArray(new BlockWithLocations[results.size()]));
  }

   
  /** Remove the blocks associated to the given datanode. */
  void removeBlocksAssociatedTo(final DatanodeDescriptor node) {
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
    namesystem.checkSafeMode();
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
    StringBuilder datanodes = new StringBuilder();
    for(DatanodeStorageInfo storage : blocksMap.getStorages(storedBlock,
        State.NORMAL)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      final Block b = getBlockOnStorage(storedBlock, storage);
      if (b != null) {
        invalidateBlocks.add(b, node, false);
        datanodes.append(node).append(" ");
      }
    }
    if (datanodes.length() != 0) {
      blockLog.debug("BLOCK* addToInvalidates: {} {}", storedBlock,
          datanodes.toString());
    }
  }

  private Block getBlockOnStorage(BlockInfo storedBlock,
      DatanodeStorageInfo storage) {
    return storedBlock.isStriped() ?
        ((BlockInfoStriped) storedBlock).getBlockOnStorage(storage) : storedBlock;
  }

  /**
   * Remove all block invalidation tasks under this datanode UUID;
   * used when a datanode registers with a new UUID and the old one
   * is wiped.
   */
  void removeFromInvalidates(final DatanodeInfo datanode) {
    if (!isPopulatingReplQueues()) {
      return;
    }
    invalidateBlocks.remove(datanode);
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
    
    markBlockAsCorrupt(new BlockToMarkCorrupt(reportedBlock, storedBlock,
            blk.getGenerationStamp(), reason, Reason.CORRUPTION_REPORTED),
        storageID == null ? null : node.getStorageInfo(storageID),
        node);
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
      blockLog.debug("BLOCK markBlockAsCorrupt: {} cannot be marked as" +
          " corrupt as it does not belong to any file", b);
      addToInvalidates(b.getCorrupted(), node);
      return;
    }
    short expectedReplicas =
        getExpectedReplicaNum(b.getStored());

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
        b.getReasonCode());

    NumberReplicas numberOfReplicas = countNodes(b.getStored());
    boolean hasEnoughLiveReplicas = numberOfReplicas.liveReplicas() >=
        expectedReplicas;

    boolean minReplicationSatisfied = hasMinStorage(b.getStored(),
        numberOfReplicas.liveReplicas());

    boolean hasMoreCorruptReplicas = minReplicationSatisfied &&
        (numberOfReplicas.liveReplicas() + numberOfReplicas.corruptReplicas()) >
        expectedReplicas;
    boolean corruptedDuringWrite = minReplicationSatisfied &&
        b.isCorruptedDuringWrite();
    // case 1: have enough number of live replicas
    // case 2: corrupted replicas + live replicas > Replication factor
    // case 3: Block is marked corrupt due to failure while writing. In this
    //         case genstamp will be different than that of valid block.
    // In all these cases we can delete the replica.
    // In case of 3, rbw block will be deleted and valid block can be replicated
    if (hasEnoughLiveReplicas || hasMoreCorruptReplicas
        || corruptedDuringWrite) {
      // the block is over-replicated so invalidate the replicas immediately
      invalidateBlock(b, node, numberOfReplicas);
    } else if (isPopulatingReplQueues()) {
      // add the block to neededReplication
      updateNeededReplications(b.getStored(), -1, 0);
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
    if (nr.replicasOnStaleNodes() > 0) {
      blockLog.debug("BLOCK* invalidateBlocks: postponing " +
          "invalidation of {} on {} because {} replica(s) are located on " +
          "nodes with potentially out-of-date block reports", b, dn,
          nr.replicasOnStaleNodes());
      postponeBlock(b.getCorrupted());
      return false;
    } else {
      // we already checked the number of replicas in the caller of this
      // function and know there are enough live replicas, so we can delete it.
      addToInvalidates(b.getCorrupted(), dn);
      removeStoredBlock(b.getStored(), node);
      blockLog.debug("BLOCK* invalidateBlocks: {} on {} listed for deletion.",
          b, dn);
      return true;
    }
  }


  public void setPostponeBlocksFromFuture(boolean postpone) {
    this.shouldPostponeBlocksFromFuture  = postpone;
  }


  private void postponeBlock(Block blk) {
    if (postponedMisreplicatedBlocks.add(blk)) {
      postponedMisreplicatedBlocksCount.incrementAndGet();
    }
  }
  
  
  void updateState() {
    pendingReplicationBlocksCount = pendingReplications.size();
    underReplicatedBlocksCount = neededReplications.size();
    corruptReplicaBlocksCount = corruptReplicas.size();
  }

  /** Return number of under-replicated but not missing blocks */
  public int getUnderReplicatedNotMissingBlocks() {
    return neededReplications.getUnderReplicatedBlockCount();
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
   * Scan blocks in {@link #neededReplications} and assign recovery
   * (replication or erasure coding) work to data-nodes they belong to.
   *
   * The number of process blocks equals either twice the number of live
   * data-nodes or the number of under-replicated blocks whichever is less.
   *
   * @return number of blocks scheduled for replication during this iteration.
   */
  int computeBlockRecoveryWork(int blocksToProcess) {
    List<List<BlockInfo>> blocksToReplicate = null;
    namesystem.writeLock();
    try {
      // Choose the blocks to be replicated
      blocksToReplicate = neededReplications
          .chooseUnderReplicatedBlocks(blocksToProcess);
    } finally {
      namesystem.writeUnlock();
    }
    return computeRecoveryWorkForBlocks(blocksToReplicate);
  }

  /**
   * Recover a set of blocks to full strength through replication or
   * erasure coding
   *
   * @param blocksToRecover blocks to be recovered, for each priority
   * @return the number of blocks scheduled for replication
   */
  @VisibleForTesting
  int computeRecoveryWorkForBlocks(List<List<BlockInfo>> blocksToRecover) {
    int scheduledWork = 0;
    List<BlockRecoveryWork> recovWork = new LinkedList<>();

    // Step 1: categorize at-risk blocks into replication and EC tasks
    namesystem.writeLock();
    try {
      synchronized (neededReplications) {
        for (int priority = 0; priority < blocksToRecover.size(); priority++) {
          for (BlockInfo block : blocksToRecover.get(priority)) {
            BlockRecoveryWork rw = scheduleRecovery(block, priority);
            if (rw != null) {
              recovWork.add(rw);
            }
          }
        }
      }
    } finally {
      namesystem.writeUnlock();
    }

    // Step 2: choose target nodes for each recovery task
    final Set<Node> excludedNodes = new HashSet<>();
    for(BlockRecoveryWork rw : recovWork){
      // Exclude all of the containing nodes from being targets.
      // This list includes decommissioning or corrupt nodes.
      excludedNodes.clear();
      for (DatanodeDescriptor dn : rw.getContainingNodes()) {
        excludedNodes.add(dn);
      }

      // choose replication targets: NOT HOLDING THE GLOBAL LOCK
      // It is costly to extract the filename for which chooseTargets is called,
      // so for now we pass in the block collection itself.
      final BlockPlacementPolicy placementPolicy =
          placementPolicies.getPolicy(rw.getBlock().isStriped());
      rw.chooseTargets(placementPolicy, storagePolicySuite, excludedNodes);
    }

    // Step 3: add tasks to the DN
    namesystem.writeLock();
    try {
      for(BlockRecoveryWork rw : recovWork){
        final DatanodeStorageInfo[] targets = rw.getTargets();
        if(targets == null || targets.length == 0){
          rw.resetTargets();
          continue;
        }

        synchronized (neededReplications) {
          if (validateRecoveryWork(rw)) {
            scheduledWork++;
          }
        }
      }
    } finally {
      namesystem.writeUnlock();
    }

    if (blockLog.isInfoEnabled()) {
      // log which blocks have been scheduled for replication
      for(BlockRecoveryWork rw : recovWork){
        DatanodeStorageInfo[] targets = rw.getTargets();
        if (targets != null && targets.length != 0) {
          StringBuilder targetList = new StringBuilder("datanode(s)");
          for (DatanodeStorageInfo target : targets) {
            targetList.append(' ');
            targetList.append(target.getDatanodeDescriptor());
          }
          blockLog.debug("BLOCK* ask {} to replicate {} to {}", rw.getSrcNodes(),
              rw.getBlock(), targetList);
        }
      }
    }
    if (blockLog.isDebugEnabled()) {
      blockLog.debug("BLOCK* neededReplications = {} pendingReplications = {}",
          neededReplications.size(), pendingReplications.size());
    }

    return scheduledWork;
  }

  boolean hasEnoughEffectiveReplicas(BlockInfo block,
      NumberReplicas numReplicas, int pendingReplicaNum, int required) {
    int numEffectiveReplicas = numReplicas.liveReplicas() + pendingReplicaNum;
    return (numEffectiveReplicas >= required) &&
        (pendingReplicaNum > 0 || isPlacementPolicySatisfied(block));
  }

  private BlockRecoveryWork scheduleRecovery(BlockInfo block, int priority) {
    // block should belong to a file
    BlockCollection bc = getBlockCollection(block);
    // abandoned block or block reopened for append
    if (bc == null
        || (bc.isUnderConstruction() && block.equals(bc.getLastBlock()))) {
      // remove from neededReplications
      neededReplications.remove(block, priority);
      return null;
    }

    short requiredReplication = getExpectedReplicaNum(block);

    // get a source data-node
    List<DatanodeDescriptor> containingNodes = new ArrayList<>();
    List<DatanodeStorageInfo> liveReplicaNodes = new ArrayList<>();
    NumberReplicas numReplicas = new NumberReplicas();
    List<Short> liveBlockIndices = new ArrayList<>();
    final DatanodeDescriptor[] srcNodes = chooseSourceDatanodes(block,
        containingNodes, liveReplicaNodes, numReplicas,
        liveBlockIndices, priority);
    if(srcNodes == null || srcNodes.length == 0) {
      // block can not be recovered from any node
      LOG.debug("Block " + block + " cannot be recovered " +
          "from any node");
      return null;
    }

    // liveReplicaNodes can include READ_ONLY_SHARED replicas which are
    // not included in the numReplicas.liveReplicas() count
    assert liveReplicaNodes.size() >= numReplicas.liveReplicas();

    int pendingNum = pendingReplications.getNumReplicas(block);
    if (hasEnoughEffectiveReplicas(block, numReplicas, pendingNum,
        requiredReplication)) {
      neededReplications.remove(block, priority);
      blockLog.debug("BLOCK* Removing {} from neededReplications as" +
          " it has enough replicas", block);
      return null;
    }

    final int additionalReplRequired;
    if (numReplicas.liveReplicas() < requiredReplication) {
      additionalReplRequired = requiredReplication - numReplicas.liveReplicas()
          - pendingNum;
    } else {
      additionalReplRequired = 1; // Needed on a new rack
    }

    if (block.isStriped()) {
      if (pendingNum > 0) {
        // Wait the previous recovery to finish.
        return null;
      }
      short[] indices = new short[liveBlockIndices.size()];
      for (int i = 0 ; i < liveBlockIndices.size(); i++) {
        indices[i] = liveBlockIndices.get(i);
      }
      return new ErasureCodingWork(block, bc, srcNodes,
          containingNodes, liveReplicaNodes, additionalReplRequired,
          priority, indices);
    } else {
      return new ReplicationWork(block, bc, srcNodes,
          containingNodes, liveReplicaNodes, additionalReplRequired,
          priority);
    }
  }

  private boolean validateRecoveryWork(BlockRecoveryWork rw) {
    BlockInfo block = rw.getBlock();
    int priority = rw.getPriority();
    // Recheck since global lock was released
    // block should belong to a file
    BlockCollection bc = getBlockCollection(block);
    // abandoned block or block reopened for append
    if (bc == null
        || (bc.isUnderConstruction() && block.equals(bc.getLastBlock()))) {
      neededReplications.remove(block, priority);
      rw.resetTargets();
      return false;
    }

    // do not schedule more if enough replicas is already pending
    final short requiredReplication = getExpectedReplicaNum(block);
    NumberReplicas numReplicas = countNodes(block);
    final int pendingNum = pendingReplications.getNumReplicas(block);
    if (hasEnoughEffectiveReplicas(block, numReplicas, pendingNum,
        requiredReplication)) {
      neededReplications.remove(block, priority);
      rw.resetTargets();
      blockLog.debug("BLOCK* Removing {} from neededReplications as" +
          " it has enough replicas", block);
      return false;
    }

    DatanodeStorageInfo[] targets = rw.getTargets();
    if ( (numReplicas.liveReplicas() >= requiredReplication) &&
        (!isPlacementPolicySatisfied(block)) ) {
      if (rw.getSrcNodes()[0].getNetworkLocation().equals(
          targets[0].getDatanodeDescriptor().getNetworkLocation())) {
        //No use continuing, unless a new rack in this case
        return false;
      }
    }

    // Add block to the to be recovered list
    if (block.isStriped()) {
      assert rw instanceof ErasureCodingWork;
      assert rw.getTargets().length > 0;
      assert pendingNum == 0: "Should wait the previous recovery to finish";
      String src = getBlockCollection(block).getName();
      ErasureCodingPolicy ecPolicy = null;
      try {
        ecPolicy = namesystem.getErasureCodingPolicyForPath(src);
      } catch (IOException e) {
        blockLog
            .warn("Failed to get EC policy for the file {} ", src);
      }
      if (ecPolicy == null) {
        blockLog.warn("No erasure coding policy found for the file {}. "
            + "So cannot proceed for recovery", src);
        // TODO: we may have to revisit later for what we can do better to
        // handle this case.
        return false;
      }
      rw.getTargets()[0].getDatanodeDescriptor().addBlockToBeErasureCoded(
          new ExtendedBlock(namesystem.getBlockPoolId(), block),
          rw.getSrcNodes(), rw.getTargets(),
          ((ErasureCodingWork) rw).getLiveBlockIndicies(), ecPolicy);
    } else {
      rw.getSrcNodes()[0].addBlockToBeReplicated(block, targets);
    }

    DatanodeStorageInfo.incrementBlocksScheduled(targets);

    // Move the block-replication into a "pending" state.
    // The reason we use 'pending' is so we can retry
    // replications that fail after an appropriate amount of time.
    pendingReplications.increment(block,
        DatanodeStorageInfo.toDatanodeDescriptors(targets));
    blockLog.debug("BLOCK* block {} is moved from neededReplications to "
        + "pendingReplications", block);

    int numEffectiveReplicas = numReplicas.liveReplicas() + pendingNum;
    // remove from neededReplications
    if(numEffectiveReplicas + targets.length >= requiredReplication) {
      neededReplications.remove(block, priority);
    }
    return true;
  }

  /** Choose target for WebHDFS redirection. */
  public DatanodeStorageInfo[] chooseTarget4WebHDFS(String src,
      DatanodeDescriptor clientnode, Set<Node> excludes, long blocksize) {
    return placementPolicies.getPolicy(false).chooseTarget(src, 1, clientnode,
        Collections.<DatanodeStorageInfo>emptyList(), false, excludes,
        blocksize, storagePolicySuite.getDefaultPolicy());
  }

  /** Choose target for getting additional datanodes for an existing pipeline. */
  public DatanodeStorageInfo[] chooseTarget4AdditionalDatanode(String src,
      int numAdditionalNodes,
      Node clientnode,
      List<DatanodeStorageInfo> chosen,
      Set<Node> excludes,
      long blocksize,
      byte storagePolicyID,
      boolean isStriped) {
    final BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(storagePolicyID);
    final BlockPlacementPolicy blockplacement = placementPolicies.getPolicy(isStriped);
    return blockplacement.chooseTarget(src, numAdditionalNodes, clientnode,
        chosen, true, excludes, blocksize, storagePolicy);
  }

  /**
   * Choose target datanodes for creating a new block.
   * 
   * @throws IOException
   *           if the number of targets < minimum replication.
   * @see BlockPlacementPolicy#chooseTarget(String, int, Node,
   *      Set, long, List, BlockStoragePolicy)
   */
  public DatanodeStorageInfo[] chooseTarget4NewBlock(final String src,
      final int numOfReplicas, final Node client,
      final Set<Node> excludedNodes,
      final long blocksize,
      final List<String> favoredNodes,
      final byte storagePolicyID,
      final boolean isStriped) throws IOException {
    List<DatanodeDescriptor> favoredDatanodeDescriptors = 
        getDatanodeDescriptors(favoredNodes);
    final BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(storagePolicyID);
    final BlockPlacementPolicy blockplacement = placementPolicies.getPolicy(isStriped);
    final DatanodeStorageInfo[] targets = blockplacement.chooseTarget(src,
        numOfReplicas, client, excludedNodes, blocksize, 
        favoredDatanodeDescriptors, storagePolicy);
    if (targets.length < minReplication) {
      throw new IOException("File " + src + " could only be replicated to "
          + targets.length + " nodes instead of minReplication (="
          + minReplication + ").  There are "
          + getDatanodeManager().getNetworkTopology().getNumOfLeaves()
          + " datanode(s) running and "
          + (excludedNodes == null? "no": excludedNodes.size())
          + " node(s) are excluded in this operation.");
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
   * Parse the data-nodes the block belongs to and choose a certain number
   * from them to be the recovery sources.
   *
   * We prefer nodes that are in DECOMMISSION_INPROGRESS state to other nodes
   * since the former do not have write traffic and hence are less busy.
   * We do not use already decommissioned nodes as a source.
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
   * @param priority integer representing replication priority of the given
   *                 block
   * @return the array of DatanodeDescriptor of the chosen nodes from which to
   *         recover the given block
   */
  @VisibleForTesting
  DatanodeDescriptor[] chooseSourceDatanodes(BlockInfo block,
      List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> nodesContainingLiveReplicas,
      NumberReplicas numReplicas,
      List<Short> liveBlockIndices, int priority) {
    containingNodes.clear();
    nodesContainingLiveReplicas.clear();
    List<DatanodeDescriptor> srcNodes = new ArrayList<>();
    int live = 0;
    int readonly = 0;
    int decommissioned = 0;
    int decommissioning = 0;
    int corrupt = 0;
    int excess = 0;
    liveBlockIndices.clear();
    final boolean isStriped = block.isStriped();

    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(block);
    for (DatanodeStorageInfo storage : blocksMap.getStorages(block)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      LightWeightHashSet<BlockInfo> excessBlocks =
        excessReplicateMap.get(node.getDatanodeUuid());
      int countableReplica = storage.getState() == State.NORMAL ? 1 : 0;
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node)))
        corrupt += countableReplica;
      else if (node.isDecommissionInProgress()) {
        decommissioning += countableReplica;
      } else if (node.isDecommissioned()) {
        decommissioned += countableReplica;
      } else if (excessBlocks != null && excessBlocks.contains(block)) {
        excess += countableReplica;
      } else {
        nodesContainingLiveReplicas.add(storage);
        live += countableReplica;
      }
      if (storage.getState() == State.READ_ONLY_SHARED) {
        readonly++;
      }
      containingNodes.add(node);
      // Check if this replica is corrupt
      // If so, do not select the node as src node
      if ((nodesCorrupt != null) && nodesCorrupt.contains(node))
        continue;
      if(priority != UnderReplicatedBlocks.QUEUE_HIGHEST_PRIORITY 
          && !node.isDecommissionInProgress() 
          && node.getNumberOfBlocksToBeReplicated() >= maxReplicationStreams)
      {
        continue; // already reached replication limit
      }
      if (node.getNumberOfBlocksToBeReplicated() >= replicationStreamsHardLimit)
      {
        continue;
      }
      // the block must not be scheduled for removal on srcNode
      if(excessBlocks != null && excessBlocks.contains(block))
        continue;
      // never use already decommissioned nodes
      if(node.isDecommissioned())
        continue;

      if(isStriped || srcNodes.isEmpty()) {
        srcNodes.add(node);
        if (isStriped) {
          liveBlockIndices.add((short) ((BlockInfoStriped) block).
              getStorageBlockIndex(storage));
        }
        continue;
      }
      // for replicated block, switch to a different node randomly
      // this to prevent from deterministically selecting the same node even
      // if the node failed to replicate the block on previous iterations
      if (!isStriped && ThreadLocalRandom.current().nextBoolean()) {
        srcNodes.set(0, node);
      }
    }
    if(numReplicas != null)
      numReplicas.set(live, readonly, decommissioned, decommissioning, corrupt,
          excess, 0);
    return srcNodes.toArray(new DatanodeDescriptor[srcNodes.size()]);
  }

  /**
   * If there were any replication requests that timed out, reap them
   * and put them back into the neededReplication queue
   */
  private void processPendingReplications() {
    BlockInfo[] timedOutItems = pendingReplications.getTimedOutBlocks();
    if (timedOutItems != null) {
      namesystem.writeLock();
      try {
        for (int i = 0; i < timedOutItems.length; i++) {
          /*
           * Use the blockinfo from the blocksmap to be certain we're working
           * with the most up-to-date block information (e.g. genstamp).
           */
          BlockInfo bi = blocksMap.getStoredBlock(timedOutItems[i]);
          if (bi == null) {
            continue;
          }
          NumberReplicas num = countNodes(timedOutItems[i]);
          if (isNeededReplication(bi, num.liveReplicas())) {
            neededReplications.add(bi, num.liveReplicas(), num.readOnlyReplicas(),
                num.decommissionedAndDecommissioning(), getReplication(bi));
          }
        }
      } finally {
        namesystem.writeUnlock();
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
   * The given storage is reporting all its blocks.
   * Update the (storage-->block list) and (block-->storage list) maps.
   *
   * @return true if all known storages of the given DN have finished reporting.
   * @throws IOException
   */
  public boolean processReport(final DatanodeID nodeID,
      final DatanodeStorage storage,
      final BlockListAsLongs newReport, BlockReportContext context,
      boolean lastStorageInRpc) throws IOException {
    namesystem.writeLock();
    final long startTime = Time.monotonicNow(); //after acquiring write lock
    final long endTime;
    DatanodeDescriptor node;
    Collection<Block> invalidatedBlocks = null;

    try {
      node = datanodeManager.getDatanode(nodeID);
      if (node == null || !node.isAlive()) {
        throw new IOException(
            "ProcessReport from dead or unregistered node: " + nodeID);
      }

      // To minimize startup time, we discard any second (or later) block reports
      // that we receive while still in startup phase.
      DatanodeStorageInfo storageInfo = node.getStorageInfo(storage.getStorageID());

      if (storageInfo == null) {
        // We handle this for backwards compatibility.
        storageInfo = node.updateStorage(storage);
      }
      if (namesystem.isInStartupSafeMode()
          && storageInfo.getBlockReportCount() > 0) {
        blockLog.info("BLOCK* processReport: "
            + "discarded non-initial block report from {}"
            + " because namenode still in startup phase", nodeID);
        blockReportLeaseManager.removeLease(node);
        return !node.hasStaleStorages();
      }
      if (context != null) {
        if (!blockReportLeaseManager.checkLease(node, startTime,
              context.getLeaseId())) {
          return false;
        }
      }

      if (storageInfo.getBlockReportCount() == 0) {
        // The first block report can be processed a lot more efficiently than
        // ordinary block reports.  This shortens restart times.
        LOG.info("Processing first storage report for " +
            storageInfo.getStorageID() + " from datanode " +
            nodeID.getDatanodeUuid());
        processFirstBlockReport(storageInfo, newReport);
      } else {
        invalidatedBlocks = processReport(storageInfo, newReport);
      }
      
      storageInfo.receivedBlockReport();
      if (context != null) {
        storageInfo.setLastBlockReportId(context.getReportId());
        if (lastStorageInRpc) {
          int rpcsSeen = node.updateBlockReportContext(context);
          if (rpcsSeen >= context.getTotalRpcs()) {
            long leaseId = blockReportLeaseManager.removeLease(node);
            BlockManagerFaultInjector.getInstance().
                removeBlockReportLease(node, leaseId);
            List<DatanodeStorageInfo> zombies = node.removeZombieStorages();
            if (zombies.isEmpty()) {
              LOG.debug("processReport 0x{}: no zombie storages found.",
                  Long.toHexString(context.getReportId()));
            } else {
              for (DatanodeStorageInfo zombie : zombies) {
                removeZombieReplicas(context, zombie);
              }
            }
            node.clearBlockReportContext();
          } else {
            LOG.debug("processReport 0x{}: {} more RPCs remaining in this " +
                    "report.", Long.toHexString(context.getReportId()),
                (context.getTotalRpcs() - rpcsSeen)
            );
          }
        }
      }
    } finally {
      endTime = Time.monotonicNow();
      namesystem.writeUnlock();
    }

    if (invalidatedBlocks != null) {
      for (Block b : invalidatedBlocks) {
        blockLog.info("BLOCK* processReport: {} on node {} size {} does not " +
            "belong to any file", b, node, b.getNumBytes());
      }
    }

    // Log the block report processing stats from Namenode perspective
    final NameNodeMetrics metrics = NameNode.getNameNodeMetrics();
    if (metrics != null) {
      metrics.addBlockReport((int) (endTime - startTime));
    }
    blockLog.info("BLOCK* processReport: from storage {} node {}, " +
            "blocks: {}, hasStaleStorage: {}, processing time: {} msecs", storage
            .getStorageID(), nodeID, newReport.getNumberOfBlocks(),
        node.hasStaleStorages(), (endTime - startTime));
    return !node.hasStaleStorages();
  }

  private void removeZombieReplicas(BlockReportContext context,
      DatanodeStorageInfo zombie) {
    LOG.warn("processReport 0x{}: removing zombie storage {}, which no " +
            "longer exists on the DataNode.",
        Long.toHexString(context.getReportId()), zombie.getStorageID());
    assert(namesystem.hasWriteLock());
    Iterator<BlockInfo> iter = zombie.getBlockIterator();
    int prevBlocks = zombie.numBlocks();
    while (iter.hasNext()) {
      BlockInfo block = iter.next();
      // We assume that a block can be on only one storage in a DataNode.
      // That's why we pass in the DatanodeDescriptor rather than the
      // DatanodeStorageInfo.
      // TODO: remove this assumption in case we want to put a block on
      // more than one storage on a datanode (and because it's a difficult
      // assumption to really enforce)
      removeStoredBlock(block, zombie.getDatanodeDescriptor());
      Block b = getBlockOnStorage(block, zombie);
      if (b != null) {
        invalidateBlocks.remove(zombie.getDatanodeDescriptor(), b);
      }
    }
    assert(zombie.numBlocks() == 0);
    LOG.warn("processReport 0x{}: removed {} replicas from storage {}, " +
            "which no longer exists on the DataNode.",
        Long.toHexString(context.getReportId()), prevBlocks,
        zombie.getStorageID());
  }

  /**
   * Rescan the list of blocks which were previously postponed.
   */
  void rescanPostponedMisreplicatedBlocks() {
    if (getPostponedMisreplicatedBlocksCount() == 0) {
      return;
    }
    long startTimeRescanPostponedMisReplicatedBlocks = Time.monotonicNow();
    long startPostponedMisReplicatedBlocksCount =
        getPostponedMisreplicatedBlocksCount();
    namesystem.writeLock();
    try {
      // blocksPerRescan is the configured number of blocks per rescan.
      // Randomly select blocksPerRescan consecutive blocks from the HashSet
      // when the number of blocks remaining is larger than blocksPerRescan.
      // The reason we don't always pick the first blocksPerRescan blocks is to
      // handle the case if for some reason some datanodes remain in
      // content stale state for a long time and only impact the first
      // blocksPerRescan blocks.
      int i = 0;
      long startIndex = 0;
      long blocksPerRescan =
          datanodeManager.getBlocksPerPostponedMisreplicatedBlocksRescan();
      long base = getPostponedMisreplicatedBlocksCount() - blocksPerRescan;
      if (base > 0) {
        startIndex = ThreadLocalRandom.current().nextLong() % (base+1);
        if (startIndex < 0) {
          startIndex += (base+1);
        }
      }
      Iterator<Block> it = postponedMisreplicatedBlocks.iterator();
      for (int tmp = 0; tmp < startIndex; tmp++) {
        it.next();
      }

      for (;it.hasNext(); i++) {
        Block b = it.next();
        if (i >= blocksPerRescan) {
          break;
        }

        BlockInfo bi = getStoredBlock(b);
        if (bi == null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("BLOCK* rescanPostponedMisreplicatedBlocks: " +
                "Postponed mis-replicated block " + b + " no longer found " +
                "in block map.");
          }
          it.remove();
          postponedMisreplicatedBlocksCount.decrementAndGet();
          continue;
        }
        MisReplicationResult res = processMisReplicatedBlock(bi);
        if (LOG.isDebugEnabled()) {
          LOG.debug("BLOCK* rescanPostponedMisreplicatedBlocks: " +
              "Re-scanned block " + b + ", result is " + res);
        }
        if (res != MisReplicationResult.POSTPONE) {
          it.remove();
          postponedMisreplicatedBlocksCount.decrementAndGet();
        }
      }
    } finally {
      namesystem.writeUnlock();
      long endPostponedMisReplicatedBlocksCount =
          getPostponedMisreplicatedBlocksCount();
      LOG.info("Rescan of postponedMisreplicatedBlocks completed in " +
          (Time.monotonicNow() - startTimeRescanPostponedMisReplicatedBlocks) +
          " msecs. " + endPostponedMisReplicatedBlocksCount +
          " blocks are left. " + (startPostponedMisReplicatedBlocksCount -
          endPostponedMisReplicatedBlocksCount) + " blocks are removed.");
    }
  }
  
  private Collection<Block> processReport(
      final DatanodeStorageInfo storageInfo,
      final BlockListAsLongs report) throws IOException {
    // Normal case:
    // Modify the (block-->datanode) map, according to the difference
    // between the old and new block report.
    //
    Collection<BlockInfoToAdd> toAdd = new LinkedList<>();
    Collection<BlockInfo> toRemove = new TreeSet<>();
    Collection<Block> toInvalidate = new LinkedList<>();
    Collection<BlockToMarkCorrupt> toCorrupt = new LinkedList<>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<>();
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
        blockLog.debug("BLOCK* markBlockReplicasAsCorrupt: mark block replica" +
            " {} on {} as corrupt because the dn is not in the new committed " +
            "storage list.", b, storage.getDatanodeDescriptor());
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
  private void processFirstBlockReport(
      final DatanodeStorageInfo storageInfo,
      final BlockListAsLongs report) throws IOException {
    if (report == null) return;
    assert (namesystem.hasWriteLock());
    assert (storageInfo.getBlockReportCount() == 0);

    for (BlockReportReplica iblk : report) {
      ReplicaState reportedState = iblk.getState();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Initial report of block " + iblk.getBlockName()
            + " on " + storageInfo.getDatanodeDescriptor() + " size " +
            iblk.getNumBytes() + " replicaState = " + reportedState);
      }
      if (shouldPostponeBlocksFromFuture &&
          namesystem.isGenStampInFuture(iblk)) {
        queueReportedBlock(storageInfo, iblk, reportedState,
            QUEUE_REASON_FUTURE_GENSTAMP);
        continue;
      }

      BlockInfo storedBlock = getStoredBlock(iblk);

      // If block does not belong to any file, we check if it violates
      // an integrity assumption of Name node
      if (storedBlock == null) {
        if (namesystem.isInStartupSafeMode()
            && !shouldPostponeBlocksFromFuture
            && !inRollBack
            && namesystem.isGenStampInFuture(iblk)) {
          numberOfBytesInFutureBlocks.addAndGet(iblk.getBytesOnDisk());
        }
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
        if (namesystem.isInSnapshot(storedBlock)) {
          int numOfReplicas = storedBlock.getUnderConstructionFeature()
              .getNumExpectedLocations();
          namesystem.incrementSafeBlockCount(numOfReplicas, storedBlock);
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
      BlockInfo storedBlock = processReportedBlock(storageInfo,
          iblk, iState, toAdd, toInvalidate, toCorrupt, toUC);

      // move block to the head of the list
      if (storedBlock != null &&
          (curIndex = storedBlock.findStorageInfo(storageInfo)) >= 0) {
        headIndex = storageInfo.moveBlockToHead(storedBlock, curIndex, headIndex);
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

    if(LOG.isDebugEnabled()) {
      LOG.debug("Reported block " + block
          + " on " + dn + " size " + block.getNumBytes()
          + " replicaState = " + reportedState);
    }
  
    if (shouldPostponeBlocksFromFuture &&
        namesystem.isGenStampInFuture(block)) {
      queueReportedBlock(storageInfo, block, reportedState,
          QUEUE_REASON_FUTURE_GENSTAMP);
      return null;
    }
    
    // find block by blockId
    BlockInfo storedBlock = getStoredBlock(block);
    if(storedBlock == null) {
      // If blocksMap does not contain reported block id,
      // the replica should be removed from the data-node.
      toInvalidate.add(new Block(block));
      return null;
    }
    BlockUCState ucState = storedBlock.getBlockUCState();
    
    // Block is on the NN
    if(LOG.isDebugEnabled()) {
      LOG.debug("In memory blockUCState = " + ucState);
    }

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
        // TODO: Pretty confident this should be s/storedBlock/block below,
        // since we should be postponing the info of the reported block, not
        // the stored block. See HDFS-6289 for more context.
        queueReportedBlock(storageInfo, storedBlock, reportedState,
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
      toAdd.add(new BlockInfoToAdd(storedBlock, block));
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
      LOG.debug("Queueing reported block " + block +
          " in state " + reportedState + 
          " from datanode " + storageInfo.getDatanodeDescriptor() +
          " for later processing because " + reason + ".");
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
    for (ReportedBlockInfo rbi : rbis) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing previouly queued message " + rbi);
      }
      if (rbi.getReportedState() == null) {
        // This is a DELETE_BLOCK request
        DatanodeStorageInfo storageInfo = rbi.getStorageInfo();
        removeStoredBlock(getStoredBlock(rbi.getBlock()),
            storageInfo.getDatanodeDescriptor());
      } else {
        processAndHandleReportedBlock(rbi.getStorageInfo(),
            rbi.getBlock(), rbi.getReportedState(), null);
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
      LOG.info("Processing " + count + " messages from DataNodes " +
          "that were previously queued during standby state");
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
        if (storedBlock.isStriped()) {
          assert BlockIdManager.isStripedBlockID(reported.getBlockId());
          assert storedBlock.getBlockId() ==
              BlockIdManager.convertToStripedID(reported.getBlockId());
          BlockInfoStriped stripedBlock = (BlockInfoStriped) storedBlock;
          int reportedBlkIdx = BlockIdManager.getBlockIndex(reported);
          wrongSize = reported.getNumBytes() != getInternalBlockLength(
              stripedBlock.getNumBytes(), stripedBlock.getCellSize(),
              stripedBlock.getDataBlockNum(), reportedBlkIdx);
        } else {
          wrongSize = storedBlock.getNumBytes() != reported.getNumBytes();
        }
        if (wrongSize) {
          return new BlockToMarkCorrupt(new Block(reported), storedBlock,
              "block is " + ucState + " and reported length " +
              reported.getNumBytes() + " does not match " +
              "length in block map " + storedBlock.getNumBytes(),
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
      if (!storedBlock.isComplete()) {
        return null; // not corrupt
      } else if (storedBlock.getGenerationStamp() != reported.getGenerationStamp()) {
        final long reportedGS = reported.getGenerationStamp();
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
          LOG.info("Received an RBW replica for " + storedBlock +
              " on " + dn + ": ignoring it, since it is " +
              "complete with the same genstamp");
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
      LOG.warn(msg);
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

    if (ucBlock.reportedState == ReplicaState.FINALIZED &&
        (block.findStorageInfo(storageInfo) < 0)) {
      addStoredBlock(block, ucBlock.reportedBlock, storageInfo, null, true);
    }
  } 

  /**
   * Faster version of {@link #addStoredBlock},
   * intended for use with initial block report at startup. If not in startup
   * safe mode, will call standard addStoredBlock(). Assumes this method is
   * called "immediately" so there is no need to refresh the storedBlock from
   * blocksMap. Doesn't handle underReplication/overReplication, or worry about
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
      completeBlock(storedBlock, false);
    } else if (storedBlock.isComplete() && result == AddBlockResult.ADDED) {
      // check whether safe replication is reached for the block
      // only complete blocks are counted towards that.
      // In the case that the block just became complete above, completeBlock()
      // handles the safe block count maintenance.
      namesystem.incrementSafeBlockCount(numCurrentReplica, storedBlock);
    }
  }

  /**
   * Modify (block-->datanode) map. Remove block from set of
   * needed replications if this takes care of the problem.
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
      blockLog.debug("BLOCK* addStoredBlock: {} on {} size {} but it does not" +
          " belong to any file", block, node, block.getNumBytes());

      // we could add this block to invalidate set of this datanode.
      // it will happen in next block report otherwise.
      return block;
    }
    BlockCollection bc = getBlockCollection(storedBlock);
    assert bc != null : "Block must belong to a file";

    // add block to the datanode
    AddBlockResult result = storageInfo.addBlock(storedBlock, reportedBlock);

    int curReplicaDelta;
    if (result == AddBlockResult.ADDED) {
      curReplicaDelta = 1;
      if (logEveryBlock) {
        logAddStoredBlock(storedBlock, node);
      }
    } else if (result == AddBlockResult.REPLACED) {
      curReplicaDelta = 0;
      blockLog.warn("BLOCK* addStoredBlock: block {} moved to storageType " +
          "{} on node {}", storedBlock, storageInfo.getStorageType(), node);
    } else {
      // if the same block is added again and the replica was corrupt
      // previously because of a wrong gen stamp, remove it from the
      // corrupt block list.
      corruptReplicas.removeFromCorruptReplicasMap(block, node,
          Reason.GENSTAMP_MISMATCH);
      curReplicaDelta = 0;
      blockLog.warn("BLOCK* addStoredBlock: Redundant addStoredBlock request"
              + " received for {} on node {} size {}", storedBlock, node,
          storedBlock.getNumBytes());
    }

    // Now check for completion of blocks and safe block count
    NumberReplicas num = countNodes(storedBlock);
    int numLiveReplicas = num.liveReplicas();
    int numCurrentReplica = numLiveReplicas
      + pendingReplications.getNumReplicas(storedBlock);

    if(storedBlock.getBlockUCState() == BlockUCState.COMMITTED &&
        hasMinStorage(storedBlock, numLiveReplicas)) {
      completeBlock(storedBlock, false);
    } else if (storedBlock.isComplete() && result == AddBlockResult.ADDED) {
      // check whether safe replication is reached for the block
      // only complete blocks are counted towards that
      // Is no-op if not in safe mode.
      // In the case that the block just became complete above, completeBlock()
      // handles the safe block count maintenance.
      namesystem.incrementSafeBlockCount(numCurrentReplica, storedBlock);
    }
    
    // if file is under construction, then done for now
    if (bc.isUnderConstruction()) {
      return storedBlock;
    }

    // do not try to handle over/under-replicated blocks during first safe mode
    if (!isPopulatingReplQueues()) {
      return storedBlock;
    }

    // handle underReplication/overReplication
    short fileReplication = getExpectedReplicaNum(storedBlock);
    if (!isNeededReplication(storedBlock, numCurrentReplica)) {
      neededReplications.remove(storedBlock, numCurrentReplica,
          num.readOnlyReplicas(),
          num.decommissionedAndDecommissioning(), fileReplication);
    } else {
      updateNeededReplications(storedBlock, curReplicaDelta, 0);
    }
    if (numCurrentReplica > fileReplication) {
      processOverReplicatedBlock(storedBlock, fileReplication, node, delNodeHint);
    }
    // If the file replication has reached desired value
    // we can remove any corrupt replicas the block may have
    int corruptReplicasCount = corruptReplicas.numCorruptReplicas(storedBlock);
    int numCorruptNodes = num.corruptReplicas();
    if (numCorruptNodes != corruptReplicasCount) {
      LOG.warn("Inconsistent number of corrupt replicas for " +
          storedBlock + ". blockMap has " + numCorruptNodes +
          " but corrupt replicas map has " + corruptReplicasCount);
    }
    if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication)) {
      invalidateCorruptReplicas(storedBlock, reportedBlock, num);
    }
    return storedBlock;
  }

  private void logAddStoredBlock(BlockInfo storedBlock,
      DatanodeDescriptor node) {
    if (!blockLog.isDebugEnabled()) {
      return;
    }

    StringBuilder sb = new StringBuilder(500);
    sb.append("BLOCK* addStoredBlock: blockMap updated: ")
      .append(node)
      .append(" is added to ");
    storedBlock.appendStringTo(sb);
    sb.append(" size " )
      .append(storedBlock.getNumBytes());
    blockLog.debug(sb.toString());
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
    for (DatanodeDescriptor node : nodesCopy) {
      try {
        if (!invalidateBlock(new BlockToMarkCorrupt(reported, blk, null,
            Reason.ANY), node, numberReplicas)) {
          removedFromBlocksMap = false;
        }
      } catch (IOException e) {
        blockLog.debug("invalidateCorruptReplicas error in deleting bad block"
            + " {} on {}", blk, node, e);
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
   * over or under replicated. Place it into the respective queue.
   */
  public void processMisReplicatedBlocks() {
    assert namesystem.hasWriteLock();
    stopReplicationInitializer();
    neededReplications.clear();
    replicationQueuesInitializer = new Daemon() {

      @Override
      public void run() {
        try {
          processMisReplicatesAsync();
        } catch (InterruptedException ie) {
          LOG.info("Interrupted while processing replication queues.");
        } catch (Exception e) {
          LOG.error("Error while processing replication queues async", e);
        }
      }
    };
    replicationQueuesInitializer.setName("Replication Queue Initializer");
    replicationQueuesInitializer.start();
  }

  /*
   * Stop the ongoing initialisation of replication queues
   */
  private void stopReplicationInitializer() {
    if (replicationQueuesInitializer != null) {
      replicationQueuesInitializer.interrupt();
      try {
        replicationQueuesInitializer.join();
      } catch (final InterruptedException e) {
        LOG.warn("Interrupted while waiting for replicationQueueInitializer. Returning..");
        return;
      } finally {
        replicationQueuesInitializer = null;
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
    replicationQueuesInitProgress = 0;
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
          if (LOG.isTraceEnabled()) {
            LOG.trace("block " + block + ": " + res);
          }
          switch (res) {
          case UNDER_REPLICATED:
            nrUnderReplicated++;
            break;
          case OVER_REPLICATED:
            nrOverReplicated++;
            break;
          case INVALID:
            nrInvalid++;
            break;
          case POSTPONE:
            nrPostponed++;
            postponeBlock(block);
            break;
          case UNDER_CONSTRUCTION:
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
        replicationQueuesInitProgress = Math.min((double) totalProcessed
            / totalBlocks, 1.0);

        if (!blocksItr.hasNext()) {
          LOG.info("Total number of blocks            = " + blocksMap.size());
          LOG.info("Number of invalid blocks          = " + nrInvalid);
          LOG.info("Number of under-replicated blocks = " + nrUnderReplicated);
          LOG.info("Number of  over-replicated blocks = " + nrOverReplicated
              + ((nrPostponed > 0) ? (" (" + nrPostponed + " postponed)") : ""));
          LOG.info("Number of blocks being written    = " + nrUnderConstruction);
          NameNode.stateChangeLog
              .info("STATE* Replication Queue initialization "
                  + "scan for invalid, over- and under-replicated blocks "
                  + "completed in "
                  + (Time.monotonicNow() - startTimeMisReplicatedScan)
                  + " msec");
          break;
        }
      } finally {
        namesystem.writeUnlock();
        // Make sure it is out of the write lock for sufficiently long time.
        Thread.sleep(sleepDuration);
      }
    }
    if (Thread.currentThread().isInterrupted()) {
      LOG.info("Interrupted while processing replication queues.");
    }
  }

  /**
   * Get the progress of the Replication queues initialisation
   * 
   * @return Returns values between 0 and 1 for the progress.
   */
  public double getReplicationQueuesInitProgress() {
    return replicationQueuesInitProgress;
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
    // calculate current replication
    short expectedReplication = getExpectedReplicaNum(block);
    NumberReplicas num = countNodes(block);
    int numCurrentReplica = num.liveReplicas();
    // add to under-replicated queue if need to be
    if (isNeededReplication(block, numCurrentReplica)) {
      if (neededReplications.add(block, numCurrentReplica, num.readOnlyReplicas(),
          num.decommissionedAndDecommissioning(), expectedReplication)) {
        return MisReplicationResult.UNDER_REPLICATED;
      }
    }

    if (numCurrentReplica > expectedReplication) {
      if (num.replicasOnStaleNodes() > 0) {
        // If any of the replicas of this block are on nodes that are
        // considered "stale", then these replicas may in fact have
        // already been deleted. So, we cannot safely act on the
        // over-replication until a later point in time, when
        // the "stale" nodes have block reported.
        return MisReplicationResult.POSTPONE;
      }
      
      // over-replicated block
      processOverReplicatedBlock(block, expectedReplication, null, null);
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

    // update needReplication priority queues
    b.setReplication(newRepl);
    updateNeededReplications(b, 0, newRepl - oldRepl);

    if (oldRepl > newRepl) {
      processOverReplicatedBlock(b, newRepl, null, null);
    }
  }

  /**
   * Find how many of the containing nodes are "extra", if any.
   * If there are any extras, call chooseExcessReplicates() to
   * mark them in the excessReplicateMap.
   */
  private void processOverReplicatedBlock(final BlockInfo block,
      final short replication, final DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {
    assert namesystem.hasWriteLock();
    if (addedNode == delNodeHint) {
      delNodeHint = null;
    }
    Collection<DatanodeStorageInfo> nonExcess = new ArrayList<>();
    Collection<DatanodeDescriptor> corruptNodes = corruptReplicas
        .getNodes(block);
    for(DatanodeStorageInfo storage : blocksMap.getStorages(block, State.NORMAL)) {
      final DatanodeDescriptor cur = storage.getDatanodeDescriptor();
      if (storage.areBlockContentsStale()) {
        LOG.info("BLOCK* processOverReplicatedBlock: " +
            "Postponing processing of over-replicated " +
            block + " since storage + " + storage
            + "datanode " + cur + " does not yet have up-to-date " +
            "block information.");
        postponeBlock(block);
        return;
      }
      LightWeightHashSet<BlockInfo> excessBlocks = excessReplicateMap.get(
          cur.getDatanodeUuid());
      if (excessBlocks == null || !excessBlocks.contains(block)) {
        if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
          // exclude corrupt replicas
          if (corruptNodes == null || !corruptNodes.contains(cur)) {
            nonExcess.add(storage);
          }
        }
      }
    }
    chooseExcessReplicates(nonExcess, block, replication, addedNode,
        delNodeHint);
  }

  private void chooseExcessReplicates(
      final Collection<DatanodeStorageInfo> nonExcess,
      BlockInfo storedBlock, short replication,
      DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {
    assert namesystem.hasWriteLock();
    // first form a rack to datanodes map and
    BlockCollection bc = getBlockCollection(storedBlock);
    if (storedBlock.isStriped()) {
      chooseExcessReplicasStriped(bc, nonExcess, storedBlock, delNodeHint);
    } else {
      final BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(
          bc.getStoragePolicyID());
      final List<StorageType> excessTypes = storagePolicy.chooseExcess(
          replication, DatanodeStorageInfo.toStorageTypes(nonExcess));
      chooseExcessReplicasContiguous(nonExcess, storedBlock, replication,
          addedNode, delNodeHint, excessTypes);
    }
  }

  /**
   * We want "replication" replicates for the block, but we now have too many.  
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
  private void chooseExcessReplicasContiguous(
      final Collection<DatanodeStorageInfo> nonExcess, BlockInfo storedBlock,
      short replication, DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint, List<StorageType> excessTypes) {
    BlockPlacementPolicy replicator = placementPolicies.getPolicy(false);
    List<DatanodeStorageInfo> replicasToDelete = replicator
        .chooseReplicasToDelete(nonExcess, replication, excessTypes,
            addedNode, delNodeHint);
    for (DatanodeStorageInfo choosenReplica : replicasToDelete) {
      processChosenExcessReplica(nonExcess, choosenReplica, storedBlock);
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
  private void chooseExcessReplicasStriped(BlockCollection bc,
      final Collection<DatanodeStorageInfo> nonExcess,
      BlockInfo storedBlock,
      DatanodeDescriptor delNodeHint) {
    assert storedBlock instanceof BlockInfoStriped;
    BlockInfoStriped sblk = (BlockInfoStriped) storedBlock;
    short groupSize = sblk.getTotalBlockNum();
    BlockPlacementPolicy placementPolicy = placementPolicies.getPolicy(true);

    // find all duplicated indices
    BitSet found = new BitSet(groupSize); //indices found
    BitSet duplicated = new BitSet(groupSize); //indices found more than once
    HashMap<DatanodeStorageInfo, Integer> storage2index = new HashMap<>();
    for (DatanodeStorageInfo storage : nonExcess) {
      int index = sblk.getStorageBlockIndex(storage);
      assert index >= 0;
      if (found.get(index)) {
        duplicated.set(index);
      }
      found.set(index);
      storage2index.put(storage, index);
    }
    // the number of target left replicas equals to the of number of the found
    // indices.
    int numOfTarget = found.cardinality();

    final BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(
        bc.getStoragePolicyID());
    final List<StorageType> excessTypes = storagePolicy.chooseExcess(
        (short)numOfTarget, DatanodeStorageInfo.toStorageTypes(nonExcess));

    // use delHint only if delHint is duplicated
    final DatanodeStorageInfo delStorageHint =
        DatanodeStorageInfo.getDatanodeStorageInfo(nonExcess, delNodeHint);
    if (delStorageHint != null) {
      Integer index = storage2index.get(delStorageHint);
      if (index != null && duplicated.get(index)) {
        processChosenExcessReplica(nonExcess, delStorageHint, storedBlock);
      }
    }

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
      Block internalBlock = new Block(storedBlock);
      internalBlock.setBlockId(storedBlock.getBlockId() + targetIndex);
      while (candidates.size() > 1) {
        List<DatanodeStorageInfo> replicasToDelete = placementPolicy
            .chooseReplicasToDelete(candidates, (short) 1, excessTypes, null,
                null);
        for (DatanodeStorageInfo chosen : replicasToDelete) {
          processChosenExcessReplica(nonExcess, chosen, storedBlock);
          candidates.remove(chosen);
        }
      }
      duplicated.clear(targetIndex);
    }
  }

  private void processChosenExcessReplica(
      final Collection<DatanodeStorageInfo> nonExcess,
      final DatanodeStorageInfo chosen, BlockInfo storedBlock) {
    nonExcess.remove(chosen);
    addToExcessReplicate(chosen.getDatanodeDescriptor(), storedBlock);
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
    blockLog.debug("BLOCK* chooseExcessReplicates: "
        + "({}, {}) is added to invalidated blocks set", chosen, storedBlock);
  }

  private void addToExcessReplicate(DatanodeInfo dn, BlockInfo storedBlock) {
    assert namesystem.hasWriteLock();
    LightWeightHashSet<BlockInfo> excessBlocks = excessReplicateMap.get(
        dn.getDatanodeUuid());
    if (excessBlocks == null) {
      excessBlocks = new LightWeightHashSet<>();
      excessReplicateMap.put(dn.getDatanodeUuid(), excessBlocks);
    }
    if (excessBlocks.add(storedBlock)) {
      excessBlocksCount.incrementAndGet();
      blockLog.debug("BLOCK* addToExcessReplicate: ({}, {}) is added to"
          + " excessReplicateMap", dn, storedBlock);
    }
  }

  private void removeStoredBlock(DatanodeStorageInfo storageInfo, Block block,
      DatanodeDescriptor node) {
    if (shouldPostponeBlocksFromFuture &&
        namesystem.isGenStampInFuture(block)) {
      queueReportedBlock(storageInfo, block, null,
          QUEUE_REASON_FUTURE_GENSTAMP);
      return;
    }
    removeStoredBlock(getStoredBlock(block), node);
  }

  /**
   * Modify (block-->datanode) map. Possibly generate replication tasks, if the
   * removed block is still valid.
   */
  public void removeStoredBlock(BlockInfo storedBlock, DatanodeDescriptor node) {
    blockLog.debug("BLOCK* removeStoredBlock: {} from {}", storedBlock, node);
    assert (namesystem.hasWriteLock());
    {
      if (storedBlock == null || !blocksMap.removeNode(storedBlock, node)) {
        blockLog.debug("BLOCK* removeStoredBlock: {} has already been" +
            " removed from node {}", storedBlock, node);
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
          blockLog.debug("BLOCK* removeStoredBlock: {} removed from caching "
              + "related lists on node {}", storedBlock, node);
        }
      }

      //
      // It's possible that the block was removed because of a datanode
      // failure. If the block is still valid, check if replication is
      // necessary. In that case, put block on a possibly-will-
      // be-replicated list.
      //
      BlockCollection bc = getBlockCollection(storedBlock);
      if (bc != null) {
        namesystem.decrementSafeBlockCount(storedBlock);
        updateNeededReplications(storedBlock, -1, 0);
      }

      //
      // We've removed a block from a node, so it's definitely no longer
      // in "excess" there.
      //
      LightWeightHashSet<BlockInfo> excessBlocks = excessReplicateMap.get(
          node.getDatanodeUuid());
      if (excessBlocks != null) {
        if (excessBlocks.remove(storedBlock)) {
          excessBlocksCount.decrementAndGet();
          blockLog.debug("BLOCK* removeStoredBlock: {} is removed from " +
              "excessBlocks", storedBlock);
          if (excessBlocks.size() == 0) {
            excessReplicateMap.remove(node.getDatanodeUuid());
          }
        }
      }

      // Remove the replica from corruptReplicas
      corruptReplicas.removeFromCorruptReplicasMap(storedBlock, node);
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
  void addBlock(DatanodeStorageInfo storageInfo, Block block, String delHint)
      throws IOException {
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
    if (storedBlock != null) {
      pendingReplications.decrement(storedBlock, node);
    }
    processAndHandleReportedBlock(storageInfo, block, ReplicaState.FINALIZED,
        delHintNode);
  }
  
  private void processAndHandleReportedBlock(
      DatanodeStorageInfo storageInfo, Block block,
      ReplicaState reportedState, DatanodeDescriptor delHintNode)
      throws IOException {
    // blockReceived reports a finalized block
    Collection<BlockInfoToAdd> toAdd = new LinkedList<>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    Collection<BlockToMarkCorrupt> toCorrupt = new LinkedList<BlockToMarkCorrupt>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<StatefulBlockInfo>();
    final DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();

    processReportedBlock(storageInfo, block, reportedState, toAdd, toInvalidate,
        toCorrupt, toUC);
    // the block is only in one of the to-do lists
    // if it is in none then data-node already has it
    assert toUC.size() + toAdd.size() + toInvalidate.size() + toCorrupt.size() <= 1
      : "The block should be only in one of the lists.";

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
      blockLog.debug("BLOCK* addBlock: block {} on node {} size {} does not " +
          "belong to any file", b, node, b.getNumBytes());
      addToInvalidates(b, node);
    }
    for (BlockToMarkCorrupt b : toCorrupt) {
      markBlockAsCorrupt(b, storageInfo, node);
    }
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
    int received = 0;
    int deleted = 0;
    int receiving = 0;
    final DatanodeDescriptor node = datanodeManager.getDatanode(nodeID);
    if (node == null || !node.isAlive()) {
      blockLog.warn("BLOCK* processIncrementalBlockReport"
              + " is received from dead or unregistered node {}", nodeID);
      throw new IOException(
          "Got incremental block report from unregistered or dead node");
    }

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
          "Unknown block status code reported by " + nodeID +
          ": " + rdbi;
        blockLog.warn(msg);
        assert false : msg; // if assertions are enabled, throw.
        break;
      }
      blockLog.debug("BLOCK* block {}: {} is received from {}",
          rdbi.getStatus(), rdbi.getBlock(), nodeID);
    }
    blockLog.debug("*BLOCK* NameNode.processIncrementalBlockReport: from "
            + "{} receiving: {}, received: {}, deleted: {}", nodeID, receiving,
        received, deleted);
  }

  /**
   * Return the number of nodes hosting a given block, grouped
   * by the state of those replicas.
   * For a striped block, this includes nodes storing blocks belonging to the
   * striped block group.
   */
  public NumberReplicas countNodes(Block b) {
    int decommissioned = 0;
    int decommissioning = 0;
    int live = 0;
    int readonly = 0;
    int corrupt = 0;
    int excess = 0;
    int stale = 0;
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(b);
    for(DatanodeStorageInfo storage : blocksMap.getStorages(b)) {
      if (storage.getState() == State.FAILED) {
        continue;
      } else if (storage.getState() == State.READ_ONLY_SHARED) {
        readonly++;
        continue;
      }
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) {
        corrupt++;
      } else if (node.isDecommissionInProgress()) {
        decommissioning++;
      } else if (node.isDecommissioned()) {
        decommissioned++;
      } else {
        LightWeightHashSet<BlockInfo> blocksExcess = excessReplicateMap.get(
            node.getDatanodeUuid());
        if (blocksExcess != null && blocksExcess.contains(b)) {
          excess++;
        } else {
          live++;
        }
      }
      if (storage.areBlockContentsStale()) {
        stale++;
      }
    }
    return new NumberReplicas(live, readonly, decommissioned, decommissioning,
        corrupt, excess, stale);
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
    if (!namesystem.isInStartupSafeMode()) {
      return countNodes(b).liveReplicas();
    }
    // else proceed with fast case
    int live = 0;
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(b);
    for(DatanodeStorageInfo storage : blocksMap.getStorages(b, State.NORMAL)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      if ((nodesCorrupt == null) || (!nodesCorrupt.contains(node)))
        live++;
    }
    return live;
  }
  
  /**
   * On stopping decommission, check if the node has excess replicas.
   * If there are any excess replicas, call processOverReplicatedBlock().
   * Process over replicated blocks only when active NN is out of safe mode.
   */
  void processOverReplicatedBlocksOnReCommission(
      final DatanodeDescriptor srcNode) {
    if (!isPopulatingReplQueues()) {
      return;
    }
    final Iterator<BlockInfo> it = srcNode.getBlockIterator();
    int numOverReplicated = 0;
    while(it.hasNext()) {
      final BlockInfo block = it.next();
      int expectedReplication = this.getReplication(block);
      NumberReplicas num = countNodes(block);
      int numCurrentReplica = num.liveReplicas();
      if (numCurrentReplica > expectedReplication) {
        // over-replicated block 
        processOverReplicatedBlock(block, (short) expectedReplication, null,
            null);
        numOverReplicated++;
      }
    }
    LOG.info("Invalidated " + numOverReplicated + " over-replicated blocks on " +
        srcNode + " during recommissioning");
  }

  /**
   * Returns whether a node can be safely decommissioned based on its 
   * liveness. Dead nodes cannot always be safely decommissioned.
   */
  boolean isNodeHealthyForDecommission(DatanodeDescriptor node) {
    if (!node.checkBlockReportReceived()) {
      LOG.info("Node {} hasn't sent its first block report.", node);
      return false;
    }

    if (node.isAlive()) {
      return true;
    }

    updateState();
    if (pendingReplicationBlocksCount == 0 &&
        underReplicatedBlocksCount == 0) {
      LOG.info("Node {} is dead and there are no under-replicated" +
          " blocks or blocks pending replication. Safe to decommission.",
          node);
      return true;
    }

    LOG.warn("Node {} is dead " +
        "while decommission is in progress. Cannot be safely " +
        "decommissioned since there is risk of reduced " +
        "data durability or data loss. Either restart the failed node or" +
        " force decommissioning by removing, calling refreshNodes, " +
        "then re-adding to the excludes files.", node);
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
    // Remove the block from pendingReplications and neededReplications
    pendingReplications.remove(block);
    neededReplications.remove(block, UnderReplicatedBlocks.LEVEL);
    if (postponedMisreplicatedBlocks.remove(block)) {
      postponedMisreplicatedBlocksCount.decrementAndGet();
    }
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

  /** updates a block in under replication queue */
  private void updateNeededReplications(final BlockInfo block,
      final int curReplicasDelta, int expectedReplicasDelta) {
    namesystem.writeLock();
    try {
      if (!isPopulatingReplQueues()) {
        return;
      }
      NumberReplicas repl = countNodes(block);
      int curExpectedReplicas = getReplication(block);
      if (isNeededReplication(block, repl.liveReplicas())) {
        neededReplications.update(block, repl.liveReplicas(), repl.readOnlyReplicas(),
            repl.decommissionedAndDecommissioning(), curExpectedReplicas,
            curReplicasDelta, expectedReplicasDelta);
      } else {
        int oldReplicas = repl.liveReplicas()-curReplicasDelta;
        int oldExpectedReplicas = curExpectedReplicas-expectedReplicasDelta;
        neededReplications.remove(block, oldReplicas, repl.readOnlyReplicas(),
            repl.decommissionedAndDecommissioning(), oldExpectedReplicas);
      }
    } finally {
      namesystem.writeUnlock();
    }
  }

  /**
   * Check replication of the blocks in the collection.
   * If any block is needed replication, insert it into the replication queue.
   * Otherwise, if the block is more than the expected replication factor,
   * process it as an over replicated block.
   */
  public void checkReplication(BlockCollection bc) {
    for (BlockInfo block : bc.getBlocks()) {
      short expected = getExpectedReplicaNum(block);
      final NumberReplicas n = countNodes(block);
      final int pending = pendingReplications.getNumReplicas(block);
      if (!hasEnoughEffectiveReplicas(block, n, pending, expected)) {
        neededReplications.add(block, n.liveReplicas() + pending,
            n.readOnlyReplicas(),
            n.decommissionedAndDecommissioning(), expected);
      } else if (n.liveReplicas() > expected) {
        processOverReplicatedBlock(block, expected, null, null);
      }
    }
  }

  /**
   * Check that the indicated blocks are present and
   * replicated.
   */
  public boolean checkBlocksProperlyReplicated(
      String src, BlockInfo[] blocks) {
    for (BlockInfo b: blocks) {
      if (!b.isComplete()) {
        final int numNodes = b.numNodes();
        final int min = getMinStorageNum(b);
        final BlockUCState state = b.getBlockUCState();
        LOG.info("BLOCK* " + b + " is not COMPLETE (ucState = " + state
            + ", replication# = " + numNodes + (numNodes < min ? " < " : " >= ")
            + " minimum = " + min + ") in file " + src);
        return false;
      }
    }
    return true;
  }

  /** 
   * @return 0 if the block is not found;
   *         otherwise, return the replication factor of the block.
   */
  private int getReplication(BlockInfo block) {
    return getExpectedReplicaNum(block);
  }

  /**
   * Get blocks to invalidate for <i>nodeId</i>
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
        LOG.debug("In safemode, not computing replication work");
        return 0;
      }
      try {
        DatanodeDescriptor dnDescriptor = datanodeManager.getDatanode(dn);
        if (dnDescriptor == null) {
          LOG.warn("DataNode " + dn + " cannot be found with UUID " +
              dn.getDatanodeUuid() + ", removing block invalidation work.");
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
      namesystem.writeUnlock();
    }
    blockLog.debug("BLOCK* {}: ask {} to delete {}", getClass().getSimpleName(),
        dn, toInvalidate);
    return toInvalidate.size();
  }

  @VisibleForTesting
  public boolean containsInvalidateBlock(final DatanodeInfo dn,
      final Block block) {
    return invalidateBlocks.contains(dn, block);
  }

  boolean isPlacementPolicySatisfied(BlockInfo storedBlock) {
    List<DatanodeDescriptor> liveNodes = new ArrayList<>();
    Collection<DatanodeDescriptor> corruptNodes = corruptReplicas
        .getNodes(storedBlock);
    for (DatanodeStorageInfo storage : blocksMap.getStorages(storedBlock)) {
      final DatanodeDescriptor cur = storage.getDatanodeDescriptor();
      if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()
          && ((corruptNodes == null) || !corruptNodes.contains(cur))) {
        liveNodes.add(cur);
      }
    }
    DatanodeInfo[] locs = liveNodes.toArray(new DatanodeInfo[liveNodes.size()]);
    BlockPlacementPolicy placementPolicy = placementPolicies
        .getPolicy(storedBlock.isStriped());
    int numReplicas = storedBlock.isStriped() ? ((BlockInfoStriped) storedBlock)
        .getRealDataBlockNum() : storedBlock.getReplication();
    return placementPolicy.verifyBlockPlacement(locs, numReplicas).isPlacementPolicySatisfied();
  }

  /**
   * A block needs replication if the number of replicas is less than expected
   * or if it does not have enough racks.
   */
  boolean isNeededReplication(BlockInfo storedBlock, int current) {
    int expected = getExpectedReplicaNum(storedBlock);
    return current < expected || !isPlacementPolicySatisfied(storedBlock);
  }

  public short getExpectedReplicaNum(BlockInfo block) {
    return block.isStriped() ?
        ((BlockInfoStriped) block).getRealTotalBlockNum() :
        block.getReplication();
  }

  public long getMissingBlocksCount() {
    // not locking
    return this.neededReplications.getCorruptBlockSize();
  }

  public long getMissingReplOneBlocksCount() {
    // not locking
    return this.neededReplications.getCorruptReplOneBlockSize();
  }

  public BlockInfo addBlockCollection(BlockInfo block,
      BlockCollection bc) {
    return blocksMap.addBlockCollection(block, bc);
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

  public BlockCollection getBlockCollection(BlockInfo b) {
    return namesystem.getBlockCollection(b.getBlockCollectionId());
  }

  public int numCorruptReplicas(Block block) {
    return corruptReplicas.numCorruptReplicas(block);
  }

  public void removeBlockFromMap(Block block) {
    removeFromExcessReplicateMap(block);
    blocksMap.removeBlock(block);
    // If block is removed from blocksMap remove it from corruptReplicasMap
    corruptReplicas.removeFromCorruptReplicasMap(block);
  }

  /**
   * If a block is removed from blocksMap, remove it from excessReplicateMap.
   */
  private void removeFromExcessReplicateMap(Block block) {
    for (DatanodeStorageInfo info : blocksMap.getStorages(block)) {
      String uuid = info.getDatanodeDescriptor().getDatanodeUuid();
      LightWeightHashSet<BlockInfo> excessReplicas =
          excessReplicateMap.get(uuid);
      if (excessReplicas != null) {
        if (excessReplicas.remove(block)) {
          excessBlocksCount.decrementAndGet();
          if (excessReplicas.isEmpty()) {
            excessReplicateMap.remove(uuid);
          }
        }
      }
    }
  }

  public int getCapacity() {
    return blocksMap.getCapacity();
  }

  /**
   * Return an iterator over the set of blocks for which there are no replicas.
   */
  public Iterator<BlockInfo> getCorruptReplicaBlockIterator() {
    return neededReplications.iterator(
        UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
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
    return neededReplications.size();
  }

  /**
   * Periodically calls computeBlockRecoveryWork().
   */
  private class ReplicationMonitor implements Runnable {

    @Override
    public void run() {
      while (namesystem.isRunning()) {
        try {
          // Process replication work only when active NN is out of safe mode.
          if (isPopulatingReplQueues()) {
            computeDatanodeWork();
            processPendingReplications();
            rescanPostponedMisreplicatedBlocks();
          }
          Thread.sleep(replicationRecheckInterval);
        } catch (Throwable t) {
          if (!namesystem.isRunning()) {
            LOG.info("Stopping ReplicationMonitor.");
            if (!(t instanceof InterruptedException)) {
              LOG.info("ReplicationMonitor received an exception"
                  + " while shutting down.", t);
            }
            break;
          } else if (!checkNSRunning && t instanceof InterruptedException) {
            LOG.info("Stopping ReplicationMonitor for testing.");
            break;
          }
          LOG.error("ReplicationMonitor thread received Runtime exception. ",
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

    int workFound = this.computeBlockRecoveryWork(blocksToProcess);

    // Update counters
    namesystem.writeLock();
    try {
      this.updateState();
      this.scheduledReplicationBlocksCount = workFound;
    } finally {
      namesystem.writeUnlock();
    }
    workFound += this.computeInvalidateWork(nodesToProcess);
    return workFound;
  }

  /**
   * Clear all queues that hold decisions previously made by
   * this NameNode.
   */
  public void clearQueues() {
    neededReplications.clear();
    pendingReplications.clear();
    excessReplicateMap.clear();
    invalidateBlocks.clear();
    datanodeManager.clearPendingQueues();
    postponedMisreplicatedBlocks.clear();
    postponedMisreplicatedBlocksCount.set(0);
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
      int[] indices, long startOffset, boolean corrupt) {
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
    stopReplicationInitializer();
    blocksMap.close();
    MBeans.unregister(mxBeanName);
    mxBeanName = null;
  }
  
  public void clear() {
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

  /**
   * Returns the number of bytes that reside in blocks with Generation Stamps
   * greater than generation stamp known to Namenode.
   *
   * @return Bytes in future
   */
  public long getBytesInFuture() {
    return numberOfBytesInFutureBlocks.get();
  }

  /**
   * Clears the bytes in future counter.
   */
  public void clearBytesInFuture() {
    numberOfBytesInFutureBlocks.set(0);
  }

  /**
   * Returns true if Namenode was started with a RollBack option.
   *
   * @param option - StartupOption
   * @return boolean
   */
  private boolean isInRollBackMode(HdfsServerConstants.StartupOption option) {
    if (option == HdfsServerConstants.StartupOption.ROLLBACK) {
      return true;
    }
    if ((option == HdfsServerConstants.StartupOption.ROLLINGUPGRADE) &&
        (option.getRollingUpgradeStartupOption() ==
            HdfsServerConstants.RollingUpgradeStartupOption.ROLLBACK)) {
      return true;
    }
    return false;
  }

}
