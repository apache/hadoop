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

import java.util.AbstractList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.util.CyclicIteration;
import org.apache.hadoop.util.ChunkedArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.util.Time.monotonicNow;

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
  private final TreeMap<DatanodeDescriptor, AbstractList<BlockInfoContiguous>>
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

    // By default, the new configuration key overrides the deprecated one.
    // No # node limit is set.
    int blocksPerInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);
    int nodesPerInterval = Integer.MAX_VALUE;

    // If the expected key isn't present and the deprecated one is, 
    // use the deprecated one into the new one. This overrides the 
    // default.
    //
    // Also print a deprecation warning.
    final String deprecatedKey =
        "dfs.namenode.decommission.nodes.per.interval";
    final String strNodes = conf.get(deprecatedKey);
    if (strNodes != null) {
      nodesPerInterval = Integer.parseInt(strNodes);
      blocksPerInterval = Integer.MAX_VALUE;
      LOG.warn("Using deprecated configuration key {} value of {}.",
          deprecatedKey, nodesPerInterval); 
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

    monitor = new Monitor(blocksPerInterval, 
        nodesPerInterval, maxConcurrentTrackedNodes);
    executor.scheduleAtFixedRate(monitor, intervalSecs, intervalSecs,
        TimeUnit.SECONDS);

    LOG.debug("Activating DecommissionManager with interval {} seconds, " +
            "{} max blocks per interval, {} max nodes per interval, " +
            "{} max concurrently tracked nodes.", intervalSecs,
        blocksPerInterval, nodesPerInterval, maxConcurrentTrackedNodes);
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
      // Over-replicated blocks will be detected and processed when
      // the dead node comes back and send in its full block report.
      if (node.isAlive) {
        blockManager.processOverReplicatedBlocksOnReCommission(node);
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
   * Checks whether a block is sufficiently replicated for decommissioning.
   * Full-strength replication is not always necessary, hence "sufficient".
   * @return true if sufficient, else false.
   */
  private boolean isSufficientlyReplicated(BlockInfoContiguous block, 
      BlockCollection bc,
      NumberReplicas numberReplicas) {
    final int numExpected = bc.getBlockReplication();
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
        if (numLive >= blockManager.minReplication) {
          LOG.trace("UC block {} sufficiently-replicated since numLive ({}) "
              + ">= minR ({})", block, numLive, blockManager.minReplication);
          return true;
        } else {
          LOG.trace("UC block {} insufficiently-replicated since numLive "
              + "({}) < minR ({})", block, numLive,
              blockManager.minReplication);
        }
      } else {
        // Can decom a non-UC as long as the default replication is met
        if (numLive >= blockManager.defaultReplication) {
          return true;
        }
      }
    }
    return false;
  }

  private static void logBlockReplicationInfo(Block block, BlockCollection bc,
      DatanodeDescriptor srcNode, NumberReplicas num,
      Iterable<DatanodeStorageInfo> storages) {
    int curReplicas = num.liveReplicas();
    int curExpectedReplicas = bc.getBlockReplication();
    StringBuilder nodeList = new StringBuilder();
    for (DatanodeStorageInfo storage : storages) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      nodeList.append(node);
      nodeList.append(" ");
    }
    LOG.info("Block: " + block + ", Expected Replicas: "
        + curExpectedReplicas + ", live replicas: " + curReplicas
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
     * The maximum number of nodes to check per tick.
     */
    private final int numNodesPerCheck;
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
     * The number of blocks checked after (re)holding lock.
     */
    private int numBlocksCheckedPerLock = 0;
    /**
     * The number of nodes that have been checked on this tick. Used for 
     * testing.
     */
    private int numNodesChecked = 0;
    /**
     * The last datanode in decomNodeBlocks that we've processed
     */
    private DatanodeDescriptor iterkey = new DatanodeDescriptor(new 
        DatanodeID("", "", "", 0, 0, 0, 0));

    Monitor(int numBlocksPerCheck, int numNodesPerCheck, int 
        maxConcurrentTrackedNodes) {
      this.numBlocksPerCheck = numBlocksPerCheck;
      this.numNodesPerCheck = numNodesPerCheck;
      this.maxConcurrentTrackedNodes = maxConcurrentTrackedNodes;
    }

    private boolean exceededNumBlocksPerCheck() {
      LOG.trace("Processed {} blocks so far this tick", numBlocksChecked);
      return numBlocksChecked >= numBlocksPerCheck;
    }

    @Deprecated
    private boolean exceededNumNodesPerCheck() {
      LOG.trace("Processed {} nodes so far this tick", numNodesChecked);
      return numNodesChecked >= numNodesPerCheck;
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
      numBlocksCheckedPerLock = 0;
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
      final Iterator<Map.Entry<DatanodeDescriptor, AbstractList<BlockInfoContiguous>>>
          it = new CyclicIteration<>(decomNodeBlocks, iterkey).iterator();
      final LinkedList<DatanodeDescriptor> toRemove = new LinkedList<>();

      while (it.hasNext() && !exceededNumBlocksPerCheck()
          && !exceededNumNodesPerCheck() && namesystem.isRunning()) {
        numNodesChecked++;
        final Map.Entry<DatanodeDescriptor, AbstractList<BlockInfoContiguous>>
            entry = it.next();
        final DatanodeDescriptor dn = entry.getKey();
        AbstractList<BlockInfoContiguous> blocks = entry.getValue();
        boolean fullScan = false;
        if (blocks == null) {
          // This is a newly added datanode, run through its list to schedule 
          // under-replicated blocks for replication and collect the blocks 
          // that are insufficiently replicated for further tracking
          LOG.debug("Newly-added node {}, doing full scan to find " +
              "insufficiently-replicated blocks.", dn);
          blocks = handleInsufficientlyReplicated(dn);
          decomNodeBlocks.put(dn, blocks);
          fullScan = true;
        } else {
          // This is a known datanode, check if its # of insufficiently 
          // replicated blocks has dropped to zero and if it can be decommed
          LOG.debug("Processing decommission-in-progress node {}", dn);
          pruneSufficientlyReplicated(dn, blocks);
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
            blocks = handleInsufficientlyReplicated(dn);
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
            if (LOG.isDebugEnabled()) {
              StringBuilder b = new StringBuilder("Node {} ");
              if (isHealthy) {
                b.append("is ");
              } else {
                b.append("isn't ");
              }
              b.append("healthy and still needs to replicate {} more blocks," +
                  " decommissioning is still in progress.");
              LOG.debug(b.toString(), dn, blocks.size());
            }
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
     * Removes sufficiently replicated blocks from the block list of a 
     * datanode.
     */
    private void pruneSufficientlyReplicated(final DatanodeDescriptor datanode,
        AbstractList<BlockInfoContiguous> blocks) {
      processBlocksForDecomInternal(datanode, blocks.iterator(), null, true);
    }

    /**
     * Returns a list of blocks on a datanode that are insufficiently 
     * replicated, i.e. are under-replicated enough to prevent decommission.
     * <p/>
     * As part of this, it also schedules replication work for 
     * any under-replicated blocks.
     *
     * @param datanode
     * @return List of insufficiently replicated blocks 
     */
    private AbstractList<BlockInfoContiguous> handleInsufficientlyReplicated(
        final DatanodeDescriptor datanode) {
      AbstractList<BlockInfoContiguous> insufficient = new ChunkedArrayList<>();
      processBlocksForDecomInternal(datanode, datanode.getBlockIterator(),
          insufficient, false);
      return insufficient;
    }

    /**
     * Used while checking if decommission-in-progress datanodes can be marked
     * as decommissioned. Combines shared logic of 
     * pruneSufficientlyReplicated and handleInsufficientlyReplicated.
     *
     * @param datanode                    Datanode
     * @param it                          Iterator over the blocks on the
     *                                    datanode
     * @param insufficientlyReplicated    Return parameter. If it's not null,
     *                                    will contain the insufficiently
     *                                    replicated-blocks from the list.
     * @param pruneSufficientlyReplicated whether to remove sufficiently
     *                                    replicated blocks from the iterator
     * @return true if there are under-replicated blocks in the provided block
     * iterator, else false.
     */
    private void processBlocksForDecomInternal(
        final DatanodeDescriptor datanode,
        final Iterator<BlockInfoContiguous> it,
        final List<BlockInfoContiguous> insufficientlyReplicated,
        boolean pruneSufficientlyReplicated) {
      boolean firstReplicationLog = true;
      int underReplicatedBlocks = 0;
      int decommissionOnlyReplicas = 0;
      int underReplicatedInOpenFiles = 0;
      while (it.hasNext()) {
        if (insufficientlyReplicated == null
            && numBlocksCheckedPerLock >= numBlocksPerCheck) {
          // During fullscan insufficientlyReplicated will NOT be null, iterator
          // will be DN's iterator. So should not yield lock, otherwise
          // ConcurrentModificationException could occur.
          // Once the fullscan done, iterator will be a copy. So can yield the
          // lock.
          // Yielding is required in case of block number is greater than the
          // configured per-iteration-limit.
          namesystem.writeUnlock();
          try {
            LOG.debug("Yielded lock during decommission check");
            Thread.sleep(0, 500);
          } catch (InterruptedException ignored) {
            return;
          }
          //reset
          numBlocksCheckedPerLock = 0;
          namesystem.writeLock();
        }
        numBlocksChecked++;
        numBlocksCheckedPerLock++;
        final BlockInfoContiguous block = it.next();
        // Remove the block from the list if it's no longer in the block map,
        // e.g. the containing file has been deleted
        if (blockManager.blocksMap.getStoredBlock(block) == null) {
          LOG.trace("Removing unknown block {}", block);
          it.remove();
          continue;
        }
        BlockCollection bc = blockManager.blocksMap.getBlockCollection(block);
        if (bc == null) {
          // Orphan block, will be invalidated eventually. Skip.
          continue;
        }

        final NumberReplicas num = blockManager.countNodes(block);
        final int liveReplicas = num.liveReplicas();
        final int curReplicas = liveReplicas;

        // Schedule under-replicated blocks for replication if not already
        // pending
        if (blockManager.isNeededReplication(block, bc.getBlockReplication(),
            liveReplicas)) {
          if (!blockManager.neededReplications.contains(block) &&
              blockManager.pendingReplications.getNumReplicas(block) == 0 &&
              namesystem.isPopulatingReplQueues()) {
            // Process these blocks only when active NN is out of safe mode.
            blockManager.neededReplications.add(block,
                curReplicas,
                num.decommissionedAndDecommissioning(),
                bc.getBlockReplication());
          }
        }

        // Even if the block is under-replicated, 
        // it doesn't block decommission if it's sufficiently replicated 
        if (isSufficientlyReplicated(block, bc, num)) {
          if (pruneSufficientlyReplicated) {
            it.remove();
          }
          continue;
        }

        // We've found an insufficiently replicated block.
        if (insufficientlyReplicated != null) {
          insufficientlyReplicated.add(block);
        }
        // Log if this is our first time through
        if (firstReplicationLog) {
          logBlockReplicationInfo(block, bc, datanode, num,
              blockManager.blocksMap.getStorages(block));
          firstReplicationLog = false;
        }
        // Update various counts
        underReplicatedBlocks++;
        if (bc.isUnderConstruction()) {
          underReplicatedInOpenFiles++;
        }
        if ((curReplicas == 0) && (num.decommissionedAndDecommissioning() > 0)) {
          decommissionOnlyReplicas++;
        }
      }

      datanode.decommissioningStatus.set(underReplicatedBlocks,
          decommissionOnlyReplicas,
          underReplicatedInOpenFiles);
    }
  }

  @VisibleForTesting
  void runMonitor() throws ExecutionException, InterruptedException {
    Future f = executor.submit(monitor);
    f.get();
  }
}
