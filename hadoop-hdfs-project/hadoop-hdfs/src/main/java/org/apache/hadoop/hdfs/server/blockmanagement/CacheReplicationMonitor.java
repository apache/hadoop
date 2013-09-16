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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;

/**
 * Periodically computes new replication work. This consists of two tasks:
 * 
 * 1) Assigning blocks in the neededCacheBlocks to datanodes where they will be
 * cached. This moves them to the pendingCacheBlocks list.
 * 
 * 2) Placing caching tasks in pendingCacheBlocks that have timed out
 * back into neededCacheBlocks for reassignment.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
class CacheReplicationMonitor implements Runnable {

  private static final Log LOG =
      LogFactory.getLog(CacheReplicationMonitor.class);

  private static final Log blockLog = NameNode.blockStateChangeLog;

  private final Namesystem namesystem;
  private final BlockManager blockManager;
  private final DatanodeManager datanodeManager;
  private final CacheReplicationManager cacheReplManager;

  private final UncacheBlocks blocksToUncache;
  private final LightWeightHashSet<Block> neededCacheBlocks;
  private final PendingReplicationBlocks pendingCacheBlocks;

  /**
   * Re-check period for computing cache replication work
   */
  private final long cacheReplicationRecheckInterval;

  public CacheReplicationMonitor(Namesystem namesystem,
      BlockManager blockManager, DatanodeManager datanodeManager,
      CacheReplicationManager cacheReplManager,
      UncacheBlocks blocksToUncache,
      LightWeightHashSet<Block> neededCacheBlocks,
      PendingReplicationBlocks pendingCacheBlocks,
      Configuration conf) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    this.datanodeManager = datanodeManager;
    this.cacheReplManager = cacheReplManager;

    this.blocksToUncache = blocksToUncache;
    this.neededCacheBlocks = neededCacheBlocks;
    this.pendingCacheBlocks = pendingCacheBlocks;

    this.cacheReplicationRecheckInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000L;
  }

  @Override
  public void run() {
    LOG.info("CacheReplicationMonitor is starting");
    while (namesystem.isRunning()) {
      try {
        computeCachingWork();
        processPendingCachingWork();
        Thread.sleep(cacheReplicationRecheckInterval);
      } catch (Throwable t) {
        if (!namesystem.isRunning()) {
          LOG.info("Stopping CacheReplicationMonitor.");
          if (!(t instanceof InterruptedException)) {
            LOG.info("CacheReplicationMonitor received an exception"
                + " while shutting down.", t);
          }
          break;
        }
        LOG.fatal("ReplicationMonitor thread received Runtime exception. ", t);
        terminate(1, t);
      }
    }
  }

  /**
   * Assigns under-cached blocks to new datanodes.
   */
  private void computeCachingWork() {
    List<Block> blocksToCache = null;
    namesystem.writeLock();
    try {
      synchronized (neededCacheBlocks) {
        blocksToCache = neededCacheBlocks.pollAll();
      }
    } finally {
      namesystem.writeUnlock();
    }
    computeCachingWorkForBlocks(blocksToCache);
    computeUncacheWork();
  }

  private void computeCachingWorkForBlocks(List<Block> blocksToCache) {
    int requiredRepl, effectiveRepl, additionalRepl;
    List<DatanodeDescriptor> cachedNodes, storedNodes, targets;

    final HashMap<Block, List<DatanodeDescriptor>> work =
        new HashMap<Block, List<DatanodeDescriptor>>();
    namesystem.writeLock();
    try {
      synchronized (neededCacheBlocks) {
        for (Block block: blocksToCache) {
          // Required number of cached replicas
          requiredRepl = cacheReplManager.getCacheReplication(block);
          // Replicas that are safely cached
          cachedNodes = cacheReplManager.getSafeReplicas(
              cacheReplManager.cachedBlocksMap, block);
          // Replicas that are safely stored on disk
          storedNodes = cacheReplManager.getSafeReplicas(
              blockManager.blocksMap, block);
          // "effective" replication factor which includes pending
          // replication work
          effectiveRepl = cachedNodes.size()
              + pendingCacheBlocks.getNumReplicas(block);
          if (effectiveRepl >= requiredRepl) {
            neededCacheBlocks.remove(block);
            blockLog.info("BLOCK* Removing " + block
                + " from neededCacheBlocks as it has enough cached replicas");
              continue;
          }
          // Choose some replicas to cache if needed
          additionalRepl = requiredRepl - effectiveRepl;
          targets = new ArrayList<DatanodeDescriptor>(storedNodes.size());
          // Only target replicas that aren't already cached.
          for (DatanodeDescriptor dn: storedNodes) {
            if (!cachedNodes.contains(dn)) {
              targets.add(dn);
            }
          }
          if (targets.size() < additionalRepl) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Block " + block + " cannot be cached on additional"
                  + " nodes because there are no more available datanodes"
                  + " with the block on disk.");
            }
          }
          targets = CacheReplicationPolicy.chooseTargetsToCache(block, targets,
              additionalRepl);
          if (targets.size() < additionalRepl) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Block " + block + " cannot be cached on additional"
                  + " nodes because there is not sufficient cache space on"
                  + " available target datanodes.");
            }
          }
          // Continue if we couldn't get more cache targets
          if (targets.size() == 0) {
            continue;
          }

          // Update datanodes and blocks that were scheduled for caching
          work.put(block, targets);
          // Schedule caching on the targets
          for (DatanodeDescriptor target: targets) {
            target.addBlockToBeCached(block);
          }
          // Add block to the pending queue
          pendingCacheBlocks.increment(block,
              targets.toArray(new DatanodeDescriptor[] {}));
          if (blockLog.isDebugEnabled()) {
            blockLog.debug("BLOCK* block " + block
                + " is moved from neededCacheBlocks to pendingCacheBlocks");
          }
          // Remove from needed queue if it will be fully replicated
          if (effectiveRepl + targets.size() >= requiredRepl) {
            neededCacheBlocks.remove(block);
          }
        }
      }
    } finally {
      namesystem.writeUnlock();
    }

    if (blockLog.isInfoEnabled()) {
      // log which blocks have been scheduled for replication
      for (Entry<Block, List<DatanodeDescriptor>> item : work.entrySet()) {
        Block block = item.getKey();
        List<DatanodeDescriptor> nodes = item.getValue();
        StringBuilder targetList = new StringBuilder("datanode(s)");
        for (DatanodeDescriptor node: nodes) {
          targetList.append(' ');
          targetList.append(node);
        }
        blockLog.info("BLOCK* ask " + targetList + " to cache " + block);
      }
    }

    if (blockLog.isDebugEnabled()) {
        blockLog.debug(
          "BLOCK* neededCacheBlocks = " + neededCacheBlocks.size()
          + " pendingCacheBlocks = " + pendingCacheBlocks.size());
    }
  }

  /**
   * Reassign pending caching work that has timed out
   */
  private void processPendingCachingWork() {
    Block[] timedOutItems = pendingCacheBlocks.getTimedOutBlocks();
    if (timedOutItems != null) {
      namesystem.writeLock();
      try {
        for (int i = 0; i < timedOutItems.length; i++) {
          Block block = timedOutItems[i];
          final short numCached = cacheReplManager.getNumCached(block);
          final short cacheReplication =
              cacheReplManager.getCacheReplication(block);
          // Needs to be cached if under-replicated
          if (numCached < cacheReplication) {
            synchronized (neededCacheBlocks) {
              neededCacheBlocks.add(block);
            }
          }
        }
      } finally {
        namesystem.writeUnlock();
      }
    }
  }

  /**
   * Schedule blocks for uncaching at datanodes
   * @return total number of block for deletion
   */
  int computeUncacheWork() {
    final List<String> nodes = blocksToUncache.getStorageIDs();
    int blockCnt = 0;
    for (String node: nodes) {
      blockCnt += uncachingWorkForOneNode(node);
    }
    return blockCnt;
  }

  /**
   * Gets the list of blocks scheduled for uncaching at a datanode and
   * schedules them for uncaching.
   * 
   * @return number of blocks scheduled for removal
   */
  private int uncachingWorkForOneNode(String nodeId) {
    final List<Block> toInvalidate;
    final DatanodeDescriptor dn;

    namesystem.writeLock();
    try {
      // get blocks to invalidate for the nodeId
      assert nodeId != null;
      dn = datanodeManager.getDatanode(nodeId);
      if (dn == null) {
        blocksToUncache.remove(nodeId);
        return 0;
      }
      toInvalidate = blocksToUncache.invalidateWork(nodeId, dn);
      if (toInvalidate == null) {
        return 0;
      }
    } finally {
      namesystem.writeUnlock();
    }
    if (blockLog.isInfoEnabled()) {
      blockLog.info("BLOCK* " + getClass().getSimpleName()
          + ": ask " + dn + " to uncache " + toInvalidate);
    }
    return toInvalidate.size();
  }
}
