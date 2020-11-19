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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.util.CyclicIteration;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.util.ChunkedArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractList;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Iterator;

/**
 * Checks to see if datanodes have finished DECOMMISSION_INPROGRESS or
 * ENTERING_MAINTENANCE state.
 * <p>
 * Since this is done while holding the namesystem lock,
 * the amount of work per monitor tick is limited.
 */

public class DatanodeAdminDefaultMonitor extends DatanodeAdminMonitorBase
    implements DatanodeAdminMonitorInterface {

  /**
   * Map containing the DECOMMISSION_INPROGRESS or ENTERING_MAINTENANCE
   * datanodes that are being tracked so they can be be marked as
   * DECOMMISSIONED or IN_MAINTENANCE. Even after the node is marked as
   * IN_MAINTENANCE, the node remains in the map until
   * maintenance expires checked during a monitor tick.
   * <p/>
   * This holds a set of references to the under-replicated blocks on the DN
   * at the time the DN is added to the map, i.e. the blocks that are
   * preventing the node from being marked as decommissioned. During a monitor
   * tick, this list is pruned as blocks becomes replicated.
   * <p/>
   * Note also that the reference to the list of under-replicated blocks
   * will be null on initial add
   * <p/>
   * However, this map can become out-of-date since it is not updated by block
   * reports or other events. Before being finally marking as decommissioned,
   * another check is done with the actual block map.
   */
  private final TreeMap<DatanodeDescriptor, AbstractList<BlockInfo>>
      outOfServiceNodeBlocks;

  /**
   * The maximum number of blocks to check per tick.
   */
  private int numBlocksPerCheck;

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
   * statistics.
   */
  private int numNodesChecked = 0;
  /**
   * The last datanode in outOfServiceNodeBlocks that we've processed.
   */
  private DatanodeDescriptor iterkey = new DatanodeDescriptor(
      new DatanodeID("", "", "", 0, 0, 0, 0));

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminDefaultMonitor.class);

  DatanodeAdminDefaultMonitor() {
    this.outOfServiceNodeBlocks = new TreeMap<>();
  }

  @Override
  protected void processConf() {
    numBlocksPerCheck = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);
    if (numBlocksPerCheck <= 0) {
      LOG.error("{} must be greater than zero. Defaulting to {}",
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);
      numBlocksPerCheck =
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT;
    }
    LOG.info("Initialized the Default Decommission and Maintenance monitor");
  }

  private boolean exceededNumBlocksPerCheck() {
    LOG.trace("Processed {} blocks so far this tick", numBlocksChecked);
    return numBlocksChecked >= numBlocksPerCheck;
  }

  @Override
  public void stopTrackingNode(DatanodeDescriptor dn) {
    pendingNodes.remove(dn);
    outOfServiceNodeBlocks.remove(dn);
  }

  @Override
  public int getTrackedNodeCount() {
    return outOfServiceNodeBlocks.size();
  }

  @Override
  public int getNumNodesChecked() {
    return numNodesChecked;
  }

  @Override
  public void run() {
    LOG.debug("DatanodeAdminMonitor is running.");
    if (!namesystem.isRunning()) {
      LOG.info("Namesystem is not running, skipping " +
          "decommissioning/maintenance checks.");
      return;
    }
    // Reset the checked count at beginning of each iteration
    numBlocksChecked = 0;
    numBlocksCheckedPerLock = 0;
    numNodesChecked = 0;
    // Check decommission or maintenance progress.
    namesystem.writeLock();
    try {
      processPendingNodes();
      check();
    } catch (Exception e) {
      LOG.warn("DatanodeAdminMonitor caught exception when processing node.",
          e);
    } finally {
      namesystem.writeUnlock();
    }
    if (numBlocksChecked + numNodesChecked > 0) {
      LOG.info("Checked {} blocks and {} nodes this tick. {} nodes are now " +
              "in maintenance or transitioning state. {} nodes pending.",
          numBlocksChecked, numNodesChecked, outOfServiceNodeBlocks.size(),
          pendingNodes.size());
    }
  }

  /**
   * Pop datanodes off the pending list and into decomNodeBlocks,
   * subject to the maxConcurrentTrackedNodes limit.
   */
  private void processPendingNodes() {
    while (!pendingNodes.isEmpty() &&
        (maxConcurrentTrackedNodes == 0 ||
            outOfServiceNodeBlocks.size() < maxConcurrentTrackedNodes)) {
      outOfServiceNodeBlocks.put(pendingNodes.poll(), null);
    }
  }

  private void check() {
    final Iterator<Map.Entry<DatanodeDescriptor, AbstractList<BlockInfo>>>
        it = new CyclicIteration<>(outOfServiceNodeBlocks,
        iterkey).iterator();
    final List<DatanodeDescriptor> toRemove = new ArrayList<>();

    while (it.hasNext() && !exceededNumBlocksPerCheck() && namesystem
        .isRunning()) {
      numNodesChecked++;
      final Map.Entry<DatanodeDescriptor, AbstractList<BlockInfo>>
          entry = it.next();
      final DatanodeDescriptor dn = entry.getKey();
      try {
        AbstractList<BlockInfo> blocks = entry.getValue();
        boolean fullScan = false;
        if (dn.isMaintenance() && dn.maintenanceExpired()) {
          // If maintenance expires, stop tracking it.
          dnAdmin.stopMaintenance(dn);
          toRemove.add(dn);
          continue;
        }
        if (dn.isInMaintenance()) {
          // The dn is IN_MAINTENANCE and the maintenance hasn't expired yet.
          continue;
        }
        if (blocks == null) {
          // This is a newly added datanode, run through its list to schedule
          // under-replicated blocks for replication and collect the blocks
          // that are insufficiently replicated for further tracking
          LOG.debug("Newly-added node {}, doing full scan to find " +
              "insufficiently-replicated blocks.", dn);
          blocks = handleInsufficientlyStored(dn);
          outOfServiceNodeBlocks.put(dn, blocks);
          fullScan = true;
        } else {
          // This is a known datanode, check if its # of insufficiently
          // replicated blocks has dropped to zero and if it can move
          // to the next state.
          LOG.debug("Processing {} node {}", dn.getAdminState(), dn);
          pruneReliableBlocks(dn, blocks);
        }
        if (blocks.size() == 0) {
          if (!fullScan) {
            // If we didn't just do a full scan, need to re-check with the
            // full block map.
            //
            // We've replicated all the known insufficiently replicated
            // blocks. Re-check with the full block map before finally
            // marking the datanode as DECOMMISSIONED or IN_MAINTENANCE.
            LOG.debug("Node {} has finished replicating current set of "
                + "blocks, checking with the full block map.", dn);
            blocks = handleInsufficientlyStored(dn);
            outOfServiceNodeBlocks.put(dn, blocks);
          }
          // If the full scan is clean AND the node liveness is okay,
          // we can finally mark as DECOMMISSIONED or IN_MAINTENANCE.
          final boolean isHealthy =
              blockManager.isNodeHealthyForDecommissionOrMaintenance(dn);
          if (blocks.size() == 0 && isHealthy) {
            if (dn.isDecommissionInProgress()) {
              dnAdmin.setDecommissioned(dn);
              toRemove.add(dn);
            } else if (dn.isEnteringMaintenance()) {
              // IN_MAINTENANCE node remains in the outOfServiceNodeBlocks to
              // to track maintenance expiration.
              dnAdmin.setInMaintenance(dn);
            } else {
              Preconditions.checkState(false,
                  "Node %s is in an invalid state! "
                      + "Invalid state: %s %s blocks are on this dn.",
                  dn, dn.getAdminState(), blocks.size());
            }
            LOG.debug("Node {} is sufficiently replicated and healthy, "
                + "marked as {}.", dn, dn.getAdminState());
          } else {
            LOG.info("Node {} {} healthy."
                    + " It needs to replicate {} more blocks."
                    + " {} is still in progress.", dn,
                isHealthy ? "is": "isn't", blocks.size(), dn.getAdminState());
          }
        } else {
          LOG.info("Node {} still has {} blocks to replicate "
                  + "before it is a candidate to finish {}.",
              dn, blocks.size(), dn.getAdminState());
        }
      } catch (Exception e) {
        // Log and postpone to process node when meet exception since it is in
        // an invalid state.
        LOG.warn("DatanodeAdminMonitor caught exception when processing node "
            + "{}.", dn, e);
        pendingNodes.add(dn);
        toRemove.add(dn);
      } finally {
        iterkey = dn;
      }
    }
    // Remove the datanodes that are DECOMMISSIONED or in service after
    // maintenance expiration.
    for (DatanodeDescriptor dn : toRemove) {
      Preconditions.checkState(dn.isDecommissioned() || dn.isInService(),
          "Removing node %s that is not yet decommissioned or in service!",
          dn);
      outOfServiceNodeBlocks.remove(dn);
    }
  }

  /**
   * Removes reliable blocks from the block list of a datanode.
   */
  private void pruneReliableBlocks(final DatanodeDescriptor datanode,
                                   AbstractList<BlockInfo> blocks) {
    processBlocksInternal(datanode, blocks.iterator(), null, true);
  }

  /**
   * Returns a list of blocks on a datanode that are insufficiently
   * replicated or require recovery, i.e. requiring recovery and
   * should prevent decommission or maintenance.
   * <p/>
   * As part of this, it also schedules replication/recovery work.
   *
   * @return List of blocks requiring recovery
   */
  private AbstractList<BlockInfo> handleInsufficientlyStored(
      final DatanodeDescriptor datanode) {
    AbstractList<BlockInfo> insufficient = new ChunkedArrayList<>();
    processBlocksInternal(datanode, datanode.getBlockIterator(),
        insufficient, false);
    return insufficient;
  }

  /**
   * Used while checking if DECOMMISSION_INPROGRESS datanodes can be
   * marked as DECOMMISSIONED or ENTERING_MAINTENANCE datanodes can be
   * marked as IN_MAINTENANCE. Combines shared logic of pruneReliableBlocks
   * and handleInsufficientlyStored.
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
  private void processBlocksInternal(
      final DatanodeDescriptor datanode,
      final Iterator<BlockInfo> it,
      final List<BlockInfo> insufficientList,
      boolean pruneReliableBlocks) {
    boolean firstReplicationLog = true;
    // Low redundancy in UC Blocks only
    int lowRedundancyBlocksInOpenFiles = 0;
    LightWeightHashSet<Long> lowRedundancyOpenFiles =
        new LightWeightLinkedSet<>();
    // All low redundancy blocks. Includes lowRedundancyOpenFiles.
    int lowRedundancyBlocks = 0;
    // All maintenance and decommission replicas.
    int outOfServiceOnlyReplicas = 0;
    while (it.hasNext()) {
      if (insufficientList == null
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
          LOG.debug("Yielded lock during decommission/maintenance check");
          Thread.sleep(0, 500);
        } catch (InterruptedException ignored) {
          return;
        }
        // reset
        numBlocksCheckedPerLock = 0;
        namesystem.writeLock();
      }
      numBlocksChecked++;
      numBlocksCheckedPerLock++;
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

      // Schedule low redundancy blocks for reconstruction
      // if not already pending.
      boolean isDecommission = datanode.isDecommissionInProgress();
      boolean isMaintenance = datanode.isEnteringMaintenance();
      boolean neededReconstruction = isDecommission ?
          blockManager.isNeededReconstruction(block, num) :
          blockManager.isNeededReconstructionForMaintenance(block, num);
      if (neededReconstruction) {
        if (!blockManager.neededReconstruction.contains(block) &&
            blockManager.pendingReconstruction.getNumReplicas(block) == 0 &&
            blockManager.isPopulatingReplQueues()) {
          // Process these blocks only when active NN is out of safe mode.
          blockManager.neededReconstruction.add(block,
              liveReplicas, num.readOnlyReplicas(),
              num.outOfServiceReplicas(),
              blockManager.getExpectedRedundancyNum(block));
        }
      }

      // Even if the block is without sufficient redundancy,
      // it might not block decommission/maintenance if it
      // has sufficient redundancy.
      if (dnAdmin.isSufficient(block, bc, num, isDecommission, isMaintenance)) {
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
        dnAdmin.logBlockReplicationInfo(block, bc, datanode, num,
            blockManager.blocksMap.getStorages(block));
        firstReplicationLog = false;
      }
      // Update various counts
      lowRedundancyBlocks++;
      if (bc.isUnderConstruction()) {
        INode ucFile = namesystem.getFSDirectory().getInode(bc.getId());
        if (!(ucFile instanceof INodeFile) ||
            !ucFile.asFile().isUnderConstruction()) {
          LOG.warn("File {} is not under construction. Skipping add to " +
              "low redundancy open files!", ucFile.getLocalName());
        } else {
          lowRedundancyBlocksInOpenFiles++;
          lowRedundancyOpenFiles.add(ucFile.getId());
        }
      }
      if ((liveReplicas == 0) && (num.outOfServiceReplicas() > 0)) {
        outOfServiceOnlyReplicas++;
      }
    }

    datanode.getLeavingServiceStatus().set(lowRedundancyBlocksInOpenFiles,
        lowRedundancyOpenFiles, lowRedundancyBlocks,
        outOfServiceOnlyReplicas);
  }
}
