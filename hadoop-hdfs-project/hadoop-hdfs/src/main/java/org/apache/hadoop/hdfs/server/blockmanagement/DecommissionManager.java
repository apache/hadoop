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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.util.CyclicIteration;
import org.apache.hadoop.util.ChunkedArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Manages datanode decommissioning. A background monitor thread 
 * periodically checks the status of datanodes that are in-progress of 
 * decommissioning.
 * <p/>
 * A datanode can be decommissioned in a few situations:
 * <ul>
 * <li>If a DN is dead, it is decommissioned immediately.</li>
 * <li>If a DN is alive, it is decommissioned after all of its blocks 
 * are sufficiently replicated. Merely under-replicated blocks do not 
 * block decommissioning as long as they are above a replication 
 * threshold.</li>
 * </ul>
 * In the second case, the datanode transitions to a 
 * decommission-in-progress state and is tracked by the monitor thread. The 
 * monitor periodically scans through the list of insufficiently replicated
 * blocks on these datanodes to 
 * determine if they can be decommissioned. The monitor also prunes this list 
 * as blocks become replicated, so monitor scans will become more efficient 
 * over time.
 * <p/>
 * Decommission-in-progress nodes that become dead do not progress to 
 * decommissioned until they become live again. This prevents potential 
 * durability loss for singly-replicated blocks (see HDFS-6791).
 * <p/>
 * This class depends on the FSNamesystem lock for synchronization.
 */
@InterfaceAudience.Private
public class DecommissionManager {
  private static final Logger LOG = LoggerFactory.getLogger(DecommissionManager
      .class);

  private final Namesystem namesystem;
  private final BlockManager blockManager;
  private final HeartbeatManager hbManager;
  private final ScheduledExecutorService executor;

  /**
   * Map containing the decommission-in-progress datanodes that are being
   * tracked so they can be be marked as decommissioned.
   * <p/>
   * This holds a set of references to the under-replicated blocks on the DN at
   * the time the DN is added to the map, i.e. the blocks that are preventing
   * the node from being marked as decommissioned. During a monitor tick, this
   * list is pruned as blocks becomes replicated.
   * <p/>
   * Note also that the reference to the list of under-replicated blocks 
   * will be null on initial add
   * <p/>
   * However, this map can become out-of-date since it is not updated by block
   * reports or other events. Before being finally marking as decommissioned,
   * another check is done with the actual block map.
   */
  private final TreeMap<DatanodeDescriptor, AbstractList<BlockInfo>>
      decomNodeBlocks;

  /**
   * Tracking a node in decomNodeBlocks consumes additional memory. To limit
   * the impact on NN memory consumption, we limit the number of nodes in 
   * decomNodeBlocks. Additional nodes wait in pendingNodes.
   */
  private final Queue<DatanodeDescriptor> pendingNodes;

  private Monitor monitor = null;

  DecommissionManager(final Namesystem namesystem,
      final BlockManager blockManager, final HeartbeatManager hbManager) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    this.hbManager = hbManager;

    executor = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat("DecommissionMonitor-%d")
            .setDaemon(true).build());
    decomNodeBlocks = new TreeMap<>();
    pendingNodes = new LinkedList<>();
  }

  /**
   * Start the decommission monitor thread.
   * @param conf
   */
  void activate(Configuration conf) {
    final int intervalSecs =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT);
    checkArgument(intervalSecs >= 0, "Cannot set a negative " +
        "value for " + DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY);

    int blocksPerInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);

    final String deprecatedKey =
        "dfs.namenode.decommission.nodes.per.interval";
    final String strNodes = conf.get(deprecatedKey);
    if (strNodes != null) {
      LOG.warn("Deprecated configuration key {} will be ignored.",
          deprecatedKey);
      LOG.warn("Please update your configuration to use {} instead.", 
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY);
    }

    checkArgument(blocksPerInterval > 0,
        "Must set a positive value for "
        + DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY);

    final int maxConcurrentTrackedNodes = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES_DEFAULT);
    checkArgument(maxConcurrentTrackedNodes >= 0, "Cannot set a negative " +
        "value for "
        + DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES);

    monitor = new Monitor(blocksPerInterval, maxConcurrentTrackedNodes);
    executor.scheduleAtFixedRate(monitor, intervalSecs, intervalSecs,
        TimeUnit.SECONDS);

    LOG.debug("Activating DecommissionManager with interval {} seconds, " +
            "{} max blocks per interval, " +
            "{} max concurrently tracked nodes.", intervalSecs,
        blocksPerInterval, maxConcurrentTrackedNodes);
  }

  /**
   * Stop the decommission monitor thread, waiting briefly for it to terminate.
   */
  void close() {
    executor.shutdownNow();
    try {
      executor.awaitTermination(3000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {}
  }

  /**
   * Start decommissioning the specified datanode. 
   * @param node
   */
  @VisibleForTesting
  public void startDecommission(DatanodeDescriptor node) {
    if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
      // Update DN stats maintained by HeartbeatManager
      hbManager.startDecommission(node);
      // hbManager.startDecommission will set dead node to decommissioned.
      if (node.isDecommissionInProgress()) {
        for (DatanodeStorageInfo storage : node.getStorageInfos()) {
          LOG.info("Starting decommission of {} {} with {} blocks",
              node, storage, storage.numBlocks());
        }
        node.decommissioningStatus.setStartTime(monotonicNow());
        pendingNodes.add(node);
      }
    } else {
      LOG.trace("startDecommission: Node {} in {}, nothing to do." +
          node, node.getAdminState());
    }
  }

  /**
   * Stop decommissioning the specified datanode. 
   * @param node
   */
  @VisibleForTesting
  public void stopDecommission(DatanodeDescriptor node) {
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      // Update DN stats maintained by HeartbeatManager
      hbManager.stopDecommission(node);
      // extra redundancy blocks will be detected and processed when
      // the dead node comes back and send in its full block report.
      if (node.isAlive()) {
        blockManager.processExtraRedundancyBlocksOnReCommission(node);
      }
      // Remove from tracking in DecommissionManager
      pendingNodes.remove(node);
      decomNodeBlocks.remove(node);
    } else {
      LOG.trace("stopDecommission: Node {} in {}, nothing to do." +
          node, node.getAdminState());
    }
  }

  private void setDecommissioned(DatanodeDescriptor dn) {
    dn.setDecommissioned();
    LOG.info("Decommissioning complete for node {}", dn);
  }

  /**
   * Checks whether a block is sufficiently replicated/stored for
   * decommissioning. For replicated blocks or striped blocks, full-strength
   * replication or storage is not always necessary, hence "sufficient".
   * @return true if sufficient, else false.
   */
  private boolean isSufficient(BlockInfo block, BlockCollection bc,
      NumberReplicas numberReplicas) {
    final int numExpected = blockManager.getExpectedRedundancyNum(block);
    final int numLive = numberReplicas.liveReplicas();
    if (numLive >= numExpected
        && blockManager.isPlacementPolicySatisfied(block)) {
      // Block has enough replica, skip
      LOG.trace("Block {} does not need replication.", block);
      return true;
    }

    // Block is under-replicated
    LOG.trace("Block {} numExpected={}, numLive={}", block, numExpected, 
        numLive);
    if (numExpected > numLive) {
      if (bc.isUnderConstruction() && block.equals(bc.getLastBlock())) {
        // Can decom a UC block as long as there will still be minReplicas
        if (blockManager.hasMinStorage(block, numLive)) {
          LOG.trace("UC block {} sufficiently-replicated since numLive ({}) "
              + ">= minR ({})", block, numLive,
              blockManager.getMinStorageNum(block));
          return true;
        } else {
          LOG.trace("UC block {} insufficiently-replicated since numLive "
              + "({}) < minR ({})", block, numLive,
              blockManager.getMinStorageNum(block));
        }
      } else {
        // Can decom a non-UC as long as the default replication is met
        if (numLive >= blockManager.getDefaultStorageNum(block)) {
          return true;
        }
      }
    }
    return false;
  }

  private void logBlockReplicationInfo(BlockInfo block,
      BlockCollection bc,
      DatanodeDescriptor srcNode, NumberReplicas num,
      Iterable<DatanodeStorageInfo> storages) {
    if (!NameNode.blockStateChangeLog.isInfoEnabled()) {
      return;
    }

    int curReplicas = num.liveReplicas();
    int curExpectedRedundancy = blockManager.getExpectedRedundancyNum(block);
    StringBuilder nodeList = new StringBuilder();
    for (DatanodeStorageInfo storage : storages) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      nodeList.append(node);
      nodeList.append(" ");
    }
    NameNode.blockStateChangeLog.info(
        "Block: " + block + ", Expected Replicas: "
        + curExpectedRedundancy + ", live replicas: " + curReplicas
        + ", corrupt replicas: " + num.corruptReplicas()
        + ", decommissioned replicas: " + num.decommissioned()
        + ", decommissioning replicas: " + num.decommissioning()
        + ", excess replicas: " + num.excessReplicas()
        + ", Is Open File: " + bc.isUnderConstruction()
        + ", Datanodes having this block: " + nodeList + ", Current Datanode: "
        + srcNode + ", Is current datanode decommissioning: "
        + srcNode.isDecommissionInProgress());
  }

  @VisibleForTesting
  public int getNumPendingNodes() {
    return pendingNodes.size();
  }

  @VisibleForTesting
  public int getNumTrackedNodes() {
    return decomNodeBlocks.size();
  }

  @VisibleForTesting
  public int getNumNodesChecked() {
    return monitor.numNodesChecked;
  }

  /**
   * Checks to see if DNs have finished decommissioning.
   * <p/>
   * Since this is done while holding the namesystem lock, 
   * the amount of work per monitor tick is limited.
   */
  private class Monitor implements Runnable {
    /**
     * The maximum number of blocks to check per tick.
     */
    private final int numBlocksPerCheck;
    /**
     * The maximum number of nodes to track in decomNodeBlocks. A value of 0
     * means no limit.
     */
    private final int maxConcurrentTrackedNodes;
    /**
     * The number of blocks that have been checked on this tick.
     */
    private int numBlocksChecked = 0;
    /**
     * The number of nodes that have been checked on this tick. Used for 
     * statistics.
     */
    private int numNodesChecked = 0;
    /**
     * The last datanode in decomNodeBlocks that we've processed
     */
    private DatanodeDescriptor iterkey = new DatanodeDescriptor(new 
        DatanodeID("", "", "", 0, 0, 0, 0));

    Monitor(int numBlocksPerCheck, int maxConcurrentTrackedNodes) {
      this.numBlocksPerCheck = numBlocksPerCheck;
      this.maxConcurrentTrackedNodes = maxConcurrentTrackedNodes;
    }

    private boolean exceededNumBlocksPerCheck() {
      LOG.trace("Processed {} blocks so far this tick", numBlocksChecked);
      return numBlocksChecked >= numBlocksPerCheck;
    }

    @Override
    public void run() {
      if (!namesystem.isRunning()) {
        LOG.info("Namesystem is not running, skipping decommissioning checks"
            + ".");
        return;
      }
      // Reset the checked count at beginning of each iteration
      numBlocksChecked = 0;
      numNodesChecked = 0;
      // Check decom progress
      namesystem.writeLock();
      try {
        processPendingNodes();
        check();
      } finally {
        namesystem.writeUnlock();
      }
      if (numBlocksChecked + numNodesChecked > 0) {
        LOG.info("Checked {} blocks and {} nodes this tick", numBlocksChecked,
            numNodesChecked);
      }
    }

    /**
     * Pop datanodes off the pending list and into decomNodeBlocks, 
     * subject to the maxConcurrentTrackedNodes limit.
     */
    private void processPendingNodes() {
      while (!pendingNodes.isEmpty() &&
          (maxConcurrentTrackedNodes == 0 ||
           decomNodeBlocks.size() < maxConcurrentTrackedNodes)) {
        decomNodeBlocks.put(pendingNodes.poll(), null);
      }
    }

    private void check() {
      final Iterator<Map.Entry<DatanodeDescriptor, AbstractList<BlockInfo>>>
          it = new CyclicIteration<>(decomNodeBlocks, iterkey).iterator();
      final LinkedList<DatanodeDescriptor> toRemove = new LinkedList<>();

      while (it.hasNext() && !exceededNumBlocksPerCheck()) {
        numNodesChecked++;
        final Map.Entry<DatanodeDescriptor, AbstractList<BlockInfo>>
            entry = it.next();
        final DatanodeDescriptor dn = entry.getKey();
        AbstractList<BlockInfo> blocks = entry.getValue();
        boolean fullScan = false;
        if (blocks == null) {
          // This is a newly added datanode, run through its list to schedule 
          // under-replicated blocks for replication and collect the blocks 
          // that are insufficiently replicated for further tracking
          LOG.debug("Newly-added node {}, doing full scan to find " +
              "insufficiently-replicated blocks.", dn);
          blocks = handleInsufficientlyStored(dn);
          decomNodeBlocks.put(dn, blocks);
          fullScan = true;
        } else {
          // This is a known datanode, check if its # of insufficiently 
          // replicated blocks has dropped to zero and if it can be decommed
          LOG.debug("Processing decommission-in-progress node {}", dn);
          pruneReliableBlocks(dn, blocks);
        }
        if (blocks.size() == 0) {
          if (!fullScan) {
            // If we didn't just do a full scan, need to re-check with the 
            // full block map.
            //
            // We've replicated all the known insufficiently replicated 
            // blocks. Re-check with the full block map before finally 
            // marking the datanode as decommissioned 
            LOG.debug("Node {} has finished replicating current set of "
                + "blocks, checking with the full block map.", dn);
            blocks = handleInsufficientlyStored(dn);
            decomNodeBlocks.put(dn, blocks);
          }
          // If the full scan is clean AND the node liveness is okay, 
          // we can finally mark as decommissioned.
          final boolean isHealthy =
              blockManager.isNodeHealthyForDecommission(dn);
          if (blocks.size() == 0 && isHealthy) {
            setDecommissioned(dn);
            toRemove.add(dn);
            LOG.debug("Node {} is sufficiently replicated and healthy, "
                + "marked as decommissioned.", dn);
          } else {
            LOG.debug("Node {} {} healthy."
                + " It needs to replicate {} more blocks."
                + " Decommissioning is still in progress.",
                dn, isHealthy? "is": "isn't", blocks.size());
          }
        } else {
          LOG.debug("Node {} still has {} blocks to replicate "
                  + "before it is a candidate to finish decommissioning.",
              dn, blocks.size());
        }
        iterkey = dn;
      }
      // Remove the datanodes that are decommissioned
      for (DatanodeDescriptor dn : toRemove) {
        Preconditions.checkState(dn.isDecommissioned(),
            "Removing a node that is not yet decommissioned!");
        decomNodeBlocks.remove(dn);
      }
    }

    /**
     * Removes reliable blocks from the block list of a datanode.
     */
    private void pruneReliableBlocks(final DatanodeDescriptor datanode,
        AbstractList<BlockInfo> blocks) {
      processBlocksForDecomInternal(datanode, blocks.iterator(), null, true);
    }

    /**
     * Returns a list of blocks on a datanode that are insufficiently replicated
     * or require recovery, i.e. requiring recovery and should prevent
     * decommission.
     * <p/>
     * As part of this, it also schedules replication/recovery work.
     *
     * @return List of blocks requiring recovery
     */
    private AbstractList<BlockInfo> handleInsufficientlyStored(
        final DatanodeDescriptor datanode) {
      AbstractList<BlockInfo> insufficient = new ChunkedArrayList<>();
      processBlocksForDecomInternal(datanode, datanode.getBlockIterator(),
          insufficient, false);
      return insufficient;
    }

    /**
     * Used while checking if decommission-in-progress datanodes can be marked
     * as decommissioned. Combines shared logic of 
     * pruneReliableBlocks and handleInsufficientlyStored.
     *
     * @param datanode                    Datanode
     * @param it                          Iterator over the blocks on the
     *                                    datanode
     * @param insufficientList            Return parameter. If it's not null,
     *                                    will contain the insufficiently
     *                                    replicated-blocks from the list.
     * @param pruneReliableBlocks         whether to remove blocks reliable
     *                                    enough from the iterator
     */
    private void processBlocksForDecomInternal(
        final DatanodeDescriptor datanode,
        final Iterator<BlockInfo> it,
        final List<BlockInfo> insufficientList,
        boolean pruneReliableBlocks) {
      boolean firstReplicationLog = true;
      int lowRedundancyBlocks = 0;
      int decommissionOnlyReplicas = 0;
      int lowRedundancyInOpenFiles = 0;
      while (it.hasNext()) {
        numBlocksChecked++;
        final BlockInfo block = it.next();
        // Remove the block from the list if it's no longer in the block map,
        // e.g. the containing file has been deleted
        if (blockManager.blocksMap.getStoredBlock(block) == null) {
          LOG.trace("Removing unknown block {}", block);
          it.remove();
          continue;
        }

        long bcId = block.getBlockCollectionId();
        if (bcId == INodeId.INVALID_INODE_ID) {
          // Orphan block, will be invalidated eventually. Skip.
          continue;
        }

        final BlockCollection bc = blockManager.getBlockCollection(block);
        final NumberReplicas num = blockManager.countNodes(block);
        final int liveReplicas = num.liveReplicas();

        // Schedule low redundancy blocks for reconstruction if not already
        // pending
        if (blockManager.isNeededReconstruction(block, liveReplicas)) {
          if (!blockManager.neededReconstruction.contains(block) &&
              blockManager.pendingReconstruction.getNumReplicas(block) == 0 &&
              blockManager.isPopulatingReplQueues()) {
            // Process these blocks only when active NN is out of safe mode.
            blockManager.neededReconstruction.add(block,
                liveReplicas, num.readOnlyReplicas(),
                num.decommissionedAndDecommissioning(),
                blockManager.getExpectedRedundancyNum(block));
          }
        }

        // Even if the block is without sufficient redundancy,
        // it doesn't block decommission if has sufficient redundancy
        if (isSufficient(block, bc, num)) {
          if (pruneReliableBlocks) {
            it.remove();
          }
          continue;
        }

        // We've found a block without sufficient redundancy.
        if (insufficientList != null) {
          insufficientList.add(block);
        }
        // Log if this is our first time through
        if (firstReplicationLog) {
          logBlockReplicationInfo(block, bc, datanode, num,
              blockManager.blocksMap.getStorages(block));
          firstReplicationLog = false;
        }
        // Update various counts
        lowRedundancyBlocks++;
        if (bc.isUnderConstruction()) {
          lowRedundancyInOpenFiles++;
        }
        if ((liveReplicas == 0) && (num.decommissionedAndDecommissioning() > 0)) {
          decommissionOnlyReplicas++;
        }
      }

      datanode.decommissioningStatus.set(lowRedundancyBlocks,
          decommissionOnlyReplicas,
          lowRedundancyInOpenFiles);
    }
  }

  @VisibleForTesting
  void runMonitorForTest() throws ExecutionException, InterruptedException {
    executor.submit(monitor).get();
  }
}
