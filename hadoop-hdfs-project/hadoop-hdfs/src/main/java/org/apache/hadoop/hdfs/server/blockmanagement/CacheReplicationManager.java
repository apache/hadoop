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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.util.Time;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Analogue of the BlockManager class for cached replicas. Maintains the mapping
 * of cached blocks to datanodes via processing datanode cache reports. Based on
 * these reports and addition and removal of caching directives in the
 * CacheManager, the CacheReplicationManager will schedule caching and uncaching
 * work.
 * 
 * The CacheReplicationManager does not have a separate lock, so depends on
 * taking the namesystem lock as appropriate.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public class CacheReplicationManager extends ReportProcessor {

  private static final Log LOG =
      LogFactory.getLog(CacheReplicationManager.class);

  // Statistics
  private volatile long pendingCacheBlocksCount = 0L;
  private volatile long underCachedBlocksCount = 0L;
  private volatile long scheduledCacheBlocksCount = 0L;

  /** Used by metrics */
  public long getPendingCacheBlocksCount() {
    return pendingCacheBlocksCount;
  }
  /** Used by metrics */
  public long getUnderCachedBlocksCount() {
    return underCachedBlocksCount;
  }
  /** Used by metrics */
  public long getScheduledCacheBlocksCount() {
    return scheduledCacheBlocksCount;
  }
  /** Used by metrics */
  public long getPendingBlocksToUncacheCount() {
    return blocksToUncache.numBlocks();
  }

  private final Namesystem namesystem;
  private final BlockManager blockManager;
  private final DatanodeManager datanodeManager;
  private final boolean isCachingEnabled;

  /**
   * Mapping of blocks to datanodes where the block is cached
   */
  final BlocksMap cachedBlocksMap;
  /**
   * Blocks to be uncached
   */
  private final UncacheBlocks blocksToUncache;
  /**
   * Blocks that need to be cached
   */
  private final LightWeightHashSet<Block> neededCacheBlocks;
  /**
   * Blocks that are being cached
   */
  private final PendingReplicationBlocks pendingCacheBlocks;

  /**
   * Executor for the CacheReplicationMonitor thread
   */
  private ExecutorService monitor = null;

  private final Configuration conf;

  public CacheReplicationManager(final Namesystem namesystem,
      final BlockManager blockManager, final DatanodeManager datanodeManager,
      final FSClusterStats stats, final Configuration conf) throws IOException {
    super(conf);
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    this.datanodeManager = datanodeManager;
    this.conf = conf;
    isCachingEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_CACHING_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_CACHING_ENABLED_DEFAULT);
    if (isCachingEnabled) {
      cachedBlocksMap = new BlocksMap(BlockManager.DEFAULT_MAP_LOAD_FACTOR);
      blocksToUncache = new UncacheBlocks();
      pendingCacheBlocks = new PendingReplicationBlocks(1000 * conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
          DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_DEFAULT));
      neededCacheBlocks = new LightWeightHashSet<Block>();
    } else {
      cachedBlocksMap = null;
      blocksToUncache = null;
      pendingCacheBlocks = null;
      neededCacheBlocks = null;
    }
  }

  public void activate() {
    if (isCachingEnabled) {
      pendingCacheBlocks.start();
      this.monitor = Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat(CacheReplicationMonitor.class.toString())
          .build());
      monitor.submit(new CacheReplicationMonitor(namesystem, blockManager,
          datanodeManager, this, blocksToUncache, neededCacheBlocks,
          pendingCacheBlocks, conf));
      monitor.shutdown();
    }
  }

  public void close() {
    if (isCachingEnabled) {
      monitor.shutdownNow();
      try {
        monitor.awaitTermination(3000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
      }
      pendingCacheBlocks.stop();
      cachedBlocksMap.close();
    }
  }

  public void clearQueues() {
    if (isCachingEnabled) {
      blocksToUncache.clear();
      synchronized (neededCacheBlocks) {
        neededCacheBlocks.clear();
      }
      pendingCacheBlocks.clear();
    }
  }

  public boolean isCachingEnabled() {
    return isCachingEnabled;
  }

  /**
   * @return desired cache replication factor of the block
   */
  short getCacheReplication(Block block) {
    final BlockCollection bc = blockManager.blocksMap.getBlockCollection(block);
    return bc == null ? 0 : bc.getCacheReplication();
  }

  public void setCachedLocations(LocatedBlock block) {
    BlockInfo blockInfo = cachedBlocksMap.getStoredBlock(
        block.getBlock().getLocalBlock());
    for (int i=0; i<blockInfo.numNodes(); i++) {
      block.addCachedLoc(blockInfo.getDatanode(i));
    }
  }

  /**
   * Returns the number of cached replicas of a block
   */
  short getNumCached(Block block) {
    Iterator<DatanodeDescriptor> it = cachedBlocksMap.nodeIterator(block);
    short numCached = 0;
    while (it.hasNext()) {
      it.next();
      numCached++;
    }
    return numCached;
  }

  /**
   * The given datanode is reporting all of its cached blocks.
   * Update the cache state of blocks in the block map.
   */
  public void processCacheReport(final DatanodeID nodeID, final String poolId,
      final BlockListAsLongs newReport) throws IOException {
    if (!isCachingEnabled) {
      String error = "cacheReport received from datanode " + nodeID
          + " but caching is disabled on the namenode ("
          + DFSConfigKeys.DFS_NAMENODE_CACHING_ENABLED_KEY + ")";
      LOG.warn(error + ", ignoring");
      throw new IOException(error);
    }
    namesystem.writeLock();
    final long startTime = Time.now(); //after acquiring write lock
    final long endTime;
    try {
      final DatanodeDescriptor node = datanodeManager.getDatanode(nodeID);
      if (node == null || !node.isAlive) {
        throw new IOException(
            "processCacheReport from dead or unregistered node: " + nodeID);
      }

      // TODO: do an optimized initial cache report while in startup safemode
      if (namesystem.isInStartupSafeMode()) {
        blockLogInfo("#processCacheReport: "
            + "discarded cache report from " + nodeID
            + " because namenode still in startup phase");
        return;
      }

      processReport(node, newReport);

      // TODO: process postponed blocks reported while a standby
      //rescanPostponedMisreplicatedBlocks();
    } finally {
      endTime = Time.now();
      namesystem.writeUnlock();
    }

    // Log the block report processing stats from Namenode perspective
    final NameNodeMetrics metrics = NameNode.getNameNodeMetrics();
    if (metrics != null) {
      metrics.addCacheBlockReport((int) (endTime - startTime));
    }
    blockLogInfo("#processCacheReport: from "
        + nodeID + ", blocks: " + newReport.getNumberOfBlocks()
        + ", processing time: " + (endTime - startTime) + " msecs");
  }

  @Override // ReportProcessor
  void markBlockAsCorrupt(BlockToMarkCorrupt b, DatanodeInfo dn)
      throws IOException {
    throw new UnsupportedOperationException("Corrupt blocks should not be in"
        + " the cache report");
  }

  @Override // ReportProcessor
  void addToInvalidates(final Block b, final DatanodeInfo node) {
    blocksToUncache.add(b, node, true);
  }

  @Override // ReportProcessor
  void addStoredBlockUnderConstruction(
      BlockInfoUnderConstruction storedBlock, DatanodeDescriptor node,
      ReplicaState reportedState) {
    throw new UnsupportedOperationException("Under-construction blocks"
        + " should not be in the cache report");
  }

  @Override // ReportProcessor
  int moveBlockToHead(DatanodeDescriptor dn, BlockInfo storedBlock,
      int curIndex, int headIndex) {
    return dn.moveCachedBlockToHead(storedBlock, curIndex, headIndex);
  }

  @Override // ReportProcessor
  boolean addBlock(DatanodeDescriptor dn, BlockInfo block) {
    return dn.addCachedBlock(block);
  }

  @Override // ReportProcessor
  boolean removeBlock(DatanodeDescriptor dn, BlockInfo block) {
    return dn.removeCachedBlock(block);
  }

  /**
   * Similar to processReportedBlock. Simpler since it doesn't need to worry
   * about under construction and corrupt replicas.
   * 
   * @return Updated BlockInfo for the block if it should be kept, null if
   * it is to be invalidated.
   */
  @Override // ReportProcessor
  BlockInfo processReportedBlock(final DatanodeDescriptor dn,
      final Block block, final ReplicaState reportedState,
      final Collection<BlockInfo> toAdd,
      final Collection<Block> toInvalidate,
      Collection<BlockToMarkCorrupt> toCorrupt,
      Collection<StatefulBlockInfo> toUC) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Reported cached block " + block
          + " on " + dn + " size " + block.getNumBytes()
          + " replicaState = " + reportedState);
    }

    final boolean shouldPostponeBlocksFromFuture =
        blockManager.shouldPostponeBlocksFromFuture();
    if (shouldPostponeBlocksFromFuture &&
        namesystem.isGenStampInFuture(block)) {
      // TODO: queuing cache operations on the standby
      if (LOG.isTraceEnabled()) {
        LOG.trace("processReportedBlock: block " + block + " has a "
            + "genstamp from the future and namenode is in standby mode,"
            + " ignoring");
      }
      return null;
    }

    BlockInfo storedBlock = blockManager.blocksMap.getStoredBlock(block);
    if (storedBlock == null) {
      // If blocksMap does not contain reported block id,
      // the BlockManager will take care of invalidating it, and the datanode
      // will automatically uncache at that point.
      if (LOG.isTraceEnabled()) {
        LOG.trace("processReportedBlock: block " + block + " not found "
            + "in blocksMap, ignoring");
      }
      return null;
    }

    BlockUCState ucState = storedBlock.getBlockUCState();

    // Datanodes currently only will cache completed replicas.
    // Let's just invalidate anything that's not completed and the right
    // genstamp and number of bytes.
    if (!ucState.equals(BlockUCState.COMPLETE) ||
        block.getGenerationStamp() != storedBlock.getGenerationStamp() ||
        block.getNumBytes() != storedBlock.getNumBytes()) {
      if (shouldPostponeBlocksFromFuture) {
        // TODO: queuing cache operations on the standby
        if (LOG.isTraceEnabled()) {
          LOG.trace("processReportedBlock: block " + block + " has a "
              + "mismatching genstamp or length and namenode is in standby"
              + " mode, ignoring");
        }
        return null;
      } else {
        toInvalidate.add(block);
        if (LOG.isTraceEnabled()) {
          LOG.trace("processReportedBlock: block " + block + " scheduled"
              + " for uncaching because it is misreplicated"
              + " or under construction.");
        }
        return null;
      }
    }

    // It's a keeper

    // Could be present in blocksMap and not in cachedBlocksMap, add it
    BlockInfo cachedBlock = cachedBlocksMap.getStoredBlock(block);
    if (cachedBlock == null) {
      cachedBlock = new BlockInfo(block, 0);
      cachedBlocksMap.addBlockCollection(cachedBlock,
          storedBlock.getBlockCollection());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("In memory blockUCState = " + ucState);
    }

    // Ignore replicas that are already scheduled for removal
    if (blocksToUncache.contains(dn.getStorageID(), block)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("processReportedBlock: block " + block + " is already"
            + " scheduled to be uncached, not adding it to the cachedBlocksMap");
      }
      return cachedBlock;
    }

    // add replica if not already present in the cached block map
    if (reportedState == ReplicaState.FINALIZED
        && cachedBlock.findDatanode(dn) < 0) {
      toAdd.add(cachedBlock);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("processReportedBlock: block " + block + " scheduled"
          + " to be added to cachedBlocksMap");
    }
    return cachedBlock;
  }

  /**
   * Modify (cached block-->datanode) map with a newly cached block. Remove
   * block from set of needed cache replications if this takes care of the
   * problem.
   * 
   * @return the block that is stored in cachedBlockMap.
   */
  @Override // ReportProcessor
  Block addStoredBlock(final BlockInfo block, DatanodeDescriptor node,
      DatanodeDescriptor delNodeHint, boolean logEveryBlock) throws IOException {
    assert block != null && namesystem.hasWriteLock();
    BlockInfo cachedBlock = block;
    if (cachedBlock == null || cachedBlock.getBlockCollection() == null) {
      // If this block does not belong to anyfile, then we are done.
      blockLogInfo("#addStoredBlock: " + block + " on "
          + node + " size " + block.getNumBytes()
          + " but it does not belong to any file");
      // we could add this block to invalidate set of this datanode.
      // it will happen in next block report otherwise.
      return block;
    }

    BlockCollection bc = cachedBlock.getBlockCollection();

    // add block to the datanode
    boolean added = node.addCachedBlock(cachedBlock);

    int curReplicaDelta;
    if (added) {
      curReplicaDelta = 1;
      if (logEveryBlock) {
        logAddStoredBlock(cachedBlock, node);
      }
    } else {
      curReplicaDelta = 0;
      blockLogWarn("#addStoredBlock: "
          + "Redundant addCachedBlock request received for " + cachedBlock
          + " on " + node + " size " + cachedBlock.getNumBytes());
    }

    // Remove it from pending list if present
    pendingCacheBlocks.decrement(block, node);

    // Now check for completion of blocks and safe block count
    int numCachedReplicas = getNumCached(cachedBlock);
    int numEffectiveCachedReplica = numCachedReplicas
      + pendingCacheBlocks.getNumReplicas(cachedBlock);

    // if file is under construction, then done for now
    if (bc instanceof MutableBlockCollection) {
      return cachedBlock;
    }

    // do not try to handle over/under-replicated blocks during first safe mode
    if (!namesystem.isPopulatingReplQueues()) {
      return cachedBlock;
    }

    // Under-replicated
    short cacheReplication = bc.getCacheReplication();
    if (numEffectiveCachedReplica >= cacheReplication) {
      synchronized (neededCacheBlocks) {
        neededCacheBlocks.remove(cachedBlock);
      }
    } else {
      updateNeededCaching(cachedBlock, curReplicaDelta, 0);
    }

    // Over-replicated, we don't need this new replica
    if (numEffectiveCachedReplica > cacheReplication) {
      blocksToUncache.add(cachedBlock, node, true);
    }

    return cachedBlock;
  }

  /**
   * Modify (cached block-->datanode) map. Possibly generate replication tasks,
   * if the removed block is still valid.
   */
  @Override // ReportProcessor
  void removeStoredBlock(Block block, DatanodeDescriptor node) {
    blockLogDebug("#removeStoredBlock: " + block + " from " + node);
    assert (namesystem.hasWriteLock());
    {
      if (!cachedBlocksMap.removeNode(block, node)) {
        blockLogDebug("#removeStoredBlock: "
            + block + " has already been removed from node " + node);
        return;
      }

      // Prune the block from the map if it's the last cache replica
      if (cachedBlocksMap.getStoredBlock(block).numNodes() == 0) {
        cachedBlocksMap.removeBlock(block);
      }

      //
      // It's possible that the block was removed because of a datanode
      // failure. If the block is still valid, check if replication is
      // necessary. In that case, put block on a possibly-will-
      // be-replicated list.
      //
      BlockCollection bc = blockManager.blocksMap.getBlockCollection(block);
      if (bc != null) {
        updateNeededCaching(block, -1, 0);
      }
    }
  }

  /**
   * Reduce cache replication factor to the new replication by randomly
   * choosing replicas to invalidate.
   */
  private void processOverCachedBlock(final Block block,
      final short replication) {
    assert namesystem.hasWriteLock();
    List<DatanodeDescriptor> nodes = getSafeReplicas(cachedBlocksMap, block);
    List<DatanodeDescriptor> targets =
        CacheReplicationPolicy.chooseTargetsToUncache(nodes, replication);
    for (DatanodeDescriptor dn: targets) {
      blocksToUncache.add(block, dn, true);
    }
  }

  /** Set replication for the blocks. */
  public void setCacheReplication(final short oldRepl, final short newRepl,
      final String src, final Block... blocks) {
    if (!isCachingEnabled) {
      LOG.warn("Attempted to set cache replication for " + src + " but caching"
          + " is disabled (" + DFSConfigKeys.DFS_NAMENODE_CACHING_ENABLED_KEY
          + "), ignoring");
      return;
    }
    if (newRepl == oldRepl) {
      return;
    }

    // update needReplication priority queues
    for (Block b : blocks) {
      updateNeededCaching(b, 0, newRepl-oldRepl);
    }

    if (oldRepl > newRepl) {
      // old replication > the new one; need to remove copies
      LOG.info("Decreasing cache replication from " + oldRepl + " to " + newRepl
          + " for " + src);
      for (Block b : blocks) {
        processOverCachedBlock(b, newRepl);
      }
    } else { // replication factor is increased
      LOG.info("Increasing cache replication from " + oldRepl + " to " + newRepl
          + " for " + src);
    }
  }

  /** updates a block in under replicated queue */
  private void updateNeededCaching(final Block block,
      final int curReplicasDelta, int expectedReplicasDelta) {
    namesystem.writeLock();
    try {
      if (!namesystem.isPopulatingReplQueues()) {
        return;
      }
      final int numCached = getNumCached(block);
      final int curExpectedReplicas = getCacheReplication(block);
      if (numCached < curExpectedReplicas) {
        neededCacheBlocks.add(block);
      } else {
        synchronized (neededCacheBlocks) {
          neededCacheBlocks.remove(block);
        }
      }
    } finally {
      namesystem.writeUnlock();
    }
  }

  /**
   * Return the safe replicas (not corrupt or decomissioning/decommissioned) of
   * a block in a BlocksMap
   */
  List<DatanodeDescriptor> getSafeReplicas(BlocksMap map, Block block) {
    List<DatanodeDescriptor> nodes = new ArrayList<DatanodeDescriptor>(3);
    Collection<DatanodeDescriptor> corrupted =
        blockManager.corruptReplicas.getNodes(block);
    Iterator<DatanodeDescriptor> it = map.nodeIterator(block);
    while (it.hasNext()) {
      DatanodeDescriptor dn = it.next();
      // Don't count a decommissioned or decommissioning nodes
      if (dn.isDecommissioned() || dn.isDecommissionInProgress()) {
        continue;
      }
      // Don't count a corrupted node
      if (corrupted != null && corrupted.contains(dn)) {
        continue;
      }
      nodes.add(dn);
    }
    return nodes;
  }
}
