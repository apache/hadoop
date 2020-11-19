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

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * This class implements the logic to track decommissioning and entering
 * maintenance nodes, ensure all their blocks are adequately replicated
 * before they are moved to the decommissioned or maintenance state.
 *
 * This monitor avoids flooding the replication queue with all pending blocks
 * and instead feeds them to the queue as the prior set complete replication.
 *
 * HDFS-14854 contains details about the overall design of this class.
 *
 */
public class DatanodeAdminBackoffMonitor extends DatanodeAdminMonitorBase
    implements DatanodeAdminMonitorInterface  {
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
  private HashMap<DatanodeDescriptor, HashMap<BlockInfo, Integer>>
      outOfServiceNodeBlocks = new HashMap<>();

  /**
   * Any nodes where decommission or maintenance has been cancelled are added
   * to this queue for later processing.
   */
  private final Queue<DatanodeDescriptor> cancelledNodes = new ArrayDeque<>();

  /**
   * The numbe of blocks to process when moving blocks to pendingReplication
   * before releasing and reclaiming the namenode lock.
   */
  private int blocksPerLock;

  /**
   * The number of blocks that have been checked on this tick.
   */
  private int numBlocksChecked = 0;
  /**
   * The maximum number of blocks to hold in PendingRep at any time.
   */
  private int pendingRepLimit;

  /**
   * The list of blocks which have been placed onto the replication queue
   * and are waiting to be sufficiently replicated.
   */
  private final Map<DatanodeDescriptor, List<BlockInfo>>
      pendingRep = new HashMap<>();

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminBackoffMonitor.class);

  DatanodeAdminBackoffMonitor() {
  }


  @Override
  protected void processConf() {
    this.pendingRepLimit = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT,
        DFSConfigKeys.
            DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT_DEFAULT);
    if (this.pendingRepLimit < 1) {
      LOG.error("{} is set to an invalid value, it must be greater than "+
              "zero. Defaulting to {}",
          DFSConfigKeys.
              DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT,
          DFSConfigKeys.
              DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT_DEFAULT
      );
      this.pendingRepLimit = DFSConfigKeys.
          DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT_DEFAULT;
    }
    this.blocksPerLock = conf.getInt(
        DFSConfigKeys.
            DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK,
        DFSConfigKeys.
            DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK_DEFAULT
    );
    if (blocksPerLock <= 0) {
      LOG.error("{} is set to an invalid value, it must be greater than "+
              "zero. Defaulting to {}",
          DFSConfigKeys.
              DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK,
          DFSConfigKeys.
              DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK_DEFAULT);
      blocksPerLock =
          DFSConfigKeys.
              DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK_DEFAULT;
    }
    LOG.info("Initialized the Backoff Decommission and Maintenance Monitor");
  }

  /**
   * Queue a node to be removed from tracking. This method must be called
   * under the namenode write lock.
   * @param dn The datanode to stop tracking for decommission.
   */
  @Override
  public void stopTrackingNode(DatanodeDescriptor dn) {
    pendingNodes.remove(dn);
    cancelledNodes.add(dn);
  }

  @Override
  public int getTrackedNodeCount() {
    return outOfServiceNodeBlocks.size();
  }

  @Override
  public int getNumNodesChecked() {
    // We always check all nodes on each tick
    return outOfServiceNodeBlocks.size();
  }

  @Override
  public void run() {
    LOG.debug("DatanodeAdminMonitorV2 is running.");
    if (!namesystem.isRunning()) {
      LOG.info("Namesystem is not running, skipping " +
          "decommissioning/maintenance checks.");
      return;
    }
    // Reset the checked count at beginning of each iteration
    numBlocksChecked = 0;
    // Check decommission or maintenance progress.
    try {
      namesystem.writeLock();
      try {
        /**
         * Other threads can modify the pendingNode list and the cancelled
         * node list, so we must process them under the NN write lock to
         * prevent any concurrent modifications.
         *
         * Always process the cancelled list before the pending list, as
         * it is possible for a node to be cancelled, and then quickly added
         * back again. If we process these the other way around, the added
         * node will be removed from tracking by the pending cancel.
         */
        processCancelledNodes();
        processPendingNodes();
      } finally {
        namesystem.writeUnlock();
      }
      // After processing the above, various parts of the check() method will
      // take and drop the read / write lock as needed. Aside from the
      // cancelled and pending lists, nothing outside of the monitor thread
      // modifies anything inside this class, so many things can be done
      // without any lock.
      check();
    } catch (Exception e) {
      LOG.warn("DatanodeAdminMonitor caught exception when processing node.",
          e);
    }
    if (numBlocksChecked + outOfServiceNodeBlocks.size() > 0) {
      LOG.info("Checked {} blocks this tick. {} nodes are now " +
          "in maintenance or transitioning state. {} nodes pending. {} " +
          "nodes waiting to be cancelled.",
          numBlocksChecked, outOfServiceNodeBlocks.size(), pendingNodes.size(),
          cancelledNodes.size());
    }
  }

  /**
   * Move any pending nodes into outOfServiceNodeBlocks to initiate the
   * decommission or maintenance mode process.
   *
   * This method must be executed under the namenode write lock to prevent
   * the pendingNodes list from being modified externally.
   */
  private void processPendingNodes() {
    while (!pendingNodes.isEmpty() &&
        (maxConcurrentTrackedNodes == 0 ||
            outOfServiceNodeBlocks.size() < maxConcurrentTrackedNodes)) {
      outOfServiceNodeBlocks.put(pendingNodes.poll(), null);
    }
  }

  /**
   * Process any nodes which have had their decommission or maintenance mode
   * cancelled by an administrator.
   *
   * This method must be executed under the
   * write lock to prevent the cancelledNodes list being modified externally.
   */
  private void processCancelledNodes() {
    while(!cancelledNodes.isEmpty()) {
      DatanodeDescriptor dn = cancelledNodes.poll();
      outOfServiceNodeBlocks.remove(dn);
      pendingRep.remove(dn);
    }
  }

  /**
   * This method performs each of the steps to track a node from
   * decommissioning or entering maintenance to the end state.
   *
   * First, any newly added nodes are scanned.
   *
   * Then any expired maintenance nodes are handled.
   *
   * Next the pendingRep map is scanned and all blocks which are now
   * sufficiently replicated are removed
   *
   * Then new blocks are moved to pendingRep
   *
   * Finally we check if any nodes have completed the replication process and
   * if so move them to their final states.
   *
   * This methods which this method calls will take and release the namenode
   * read and write lock several times.
   *
   */
  private void check() {
    final List<DatanodeDescriptor> toRemove = new ArrayList<>();

    if (outOfServiceNodeBlocks.size() == 0) {
      // No nodes currently being tracked so simply return
      return;
    }

    // Check if there are any pending nodes to process, ie those where the
    // storage has not been scanned yet. For all which are pending, scan
    // the storage and load the under-replicated block list into
    // outOfServiceNodeBlocks. As this does not modify any external structures
    // it can be done under the namenode *read* lock, and the lock can be
    // dropped between each storage on each node.
    //
    // TODO - This is an expensive call, depending on how many nodes are
    //        to be processed, but it requires only the read lock and it will
    //        be dropped and re-taken frequently. We may want to throttle this
    //        to process only a few nodes per iteration.
    outOfServiceNodeBlocks.keySet()
        .stream()
        .filter(n -> outOfServiceNodeBlocks.get(n) == null)
        .forEach(n -> scanDatanodeStorage(n, true));

    processMaintenanceNodes();
    // First check the pending replication list and remove any blocks
    // which are now replicated OK. This list is constrained in size so this
    // call should not be overly expensive.
    processPendingReplication();

    // Now move a limited number of blocks to pending
    moveBlocksToPending();

    // Check if any nodes have reached zero blocks and also update the stats
    // exposed via JMX for all nodes still being processed.
    checkForCompletedNodes(toRemove);

    // Finally move the nodes to their final state if they are ready.
    processCompletedNodes(toRemove);
  }

  /**
   * Checks for any nodes which are in maintenance and if maintenance has
   * expired, the node will be moved back to in_service (or dead) as required.
   */
  private void processMaintenanceNodes() {
    // Check for any maintenance state nodes which need to be expired
    namesystem.writeLock();
    try {
      for (DatanodeDescriptor dn : outOfServiceNodeBlocks.keySet()) {
        if (dn.isMaintenance() && dn.maintenanceExpired()) {
          // If maintenance expires, stop tracking it. This can be an
          // expensive call, as it may need to invalidate blocks. Therefore
          // we can yield and retake the write lock after each node
          //
          // The call to stopMaintenance makes a call to stopTrackingNode()
          // which added the node to the cancelled list. Therefore expired
          // maintenance nodes do not need to be added to the toRemove list.
          dnAdmin.stopMaintenance(dn);
          namesystem.writeUnlock();
          namesystem.writeLock();
        }
      }
    } finally {
      namesystem.writeUnlock();
    }
  }

  /**
   * Loop over all nodes in the passed toRemove list and move the node to
   * the required end state. This will also remove any entries from
   * outOfServiceNodeBlocks and pendingRep for the node if required.
   *
   * @param toRemove The list of nodes to process for completion.
   */
  private void processCompletedNodes(List<DatanodeDescriptor> toRemove) {
    if (toRemove.size() == 0) {
      // If there are no nodes to process simply return and avoid
      // taking the write lock at all.
      return;
    }
    namesystem.writeLock();
    try {
      for (DatanodeDescriptor dn : toRemove) {
        final boolean isHealthy =
            blockManager.isNodeHealthyForDecommissionOrMaintenance(dn);
        if (isHealthy) {
          if (dn.isDecommissionInProgress()) {
            dnAdmin.setDecommissioned(dn);
            outOfServiceNodeBlocks.remove(dn);
            pendingRep.remove(dn);
          } else if (dn.isEnteringMaintenance()) {
            // IN_MAINTENANCE node remains in the outOfServiceNodeBlocks to
            // to track maintenance expiration.
            dnAdmin.setInMaintenance(dn);
            pendingRep.remove(dn);
          } else if (dn.isInService()) {
            // Decom / maint was cancelled and the node is yet to be processed
            // from cancelledNodes
            LOG.info("Node {} completed decommission and maintenance " +
                "but has been moved back to in service", dn);
            pendingRep.remove(dn);
            outOfServiceNodeBlocks.remove(dn);
            continue;
          } else {
            // Should not happen
            LOG.error("Node {} is in an unexpected state {} and has been "+
                    "removed from tracking for decommission or maintenance",
                dn, dn.getAdminState());
            pendingRep.remove(dn);
            outOfServiceNodeBlocks.remove(dn);
            continue;
          }
          LOG.info("Node {} is sufficiently replicated and healthy, "
              + "marked as {}.", dn, dn.getAdminState());
        } else {
          LOG.info("Node {} isn't healthy."
                  + " It needs to replicate {} more blocks."
                  + " {} is still in progress.", dn,
              getPendingCountForNode(dn), dn.getAdminState());
        }
      }
    } finally {
      namesystem.writeUnlock();
    }
  }

  /**
   * Loop over all nodes and check for any which have zero unprocessed or
   * pending blocks. If the node has zero blocks pending, the storage is
   * rescanned to ensure no transient blocks were missed on the first pass.
   *
   * If, after rescan the number of blocks pending replication is zero, the
   * node is added to the passed removeList which will later be processed to
   * complete the decommission or entering maintenance process.
   *
   * @param removeList Nodes which have zero pending blocks are added to this
   *                   list.
   */
  private void checkForCompletedNodes(List<DatanodeDescriptor> removeList) {
    for (DatanodeDescriptor dn : outOfServiceNodeBlocks.keySet()) {
      // If the node is already in maintenance, we don't need to perform
      // any further checks on it.
      if (dn.isInMaintenance()) {
        LOG.debug("Node {} is currently in maintenance", dn);
        continue;
      } else if (!dn.isInService()) {
        // A node could be inService if decom or maint has been cancelled, but
        // the cancelled list is yet to be processed. We don't need to check
        // inService nodes here
        int outstandingBlocks = getPendingCountForNode(dn);
        if (outstandingBlocks == 0) {
          scanDatanodeStorage(dn, false);
          outstandingBlocks = getPendingCountForNode(dn);
        }
        LOG.info("Node {} has {} blocks yet to process", dn, outstandingBlocks);
        if (outstandingBlocks == 0) {
          removeList.add(dn);
        }
      }
    }
  }

  /**
   * Returns the number of block pending for the given node by adding those
   * blocks in pendingRep and outOfServiceNodeBlocks.
   *
   * @param dn The datanode to return the count for
   * @return The total block count, or zero if none are pending
   */
  private int getPendingCountForNode(DatanodeDescriptor dn) {
    int count = 0;
    HashMap<BlockInfo, Integer> blocks = outOfServiceNodeBlocks.get(dn);
    if (blocks != null) {
      count += blocks.size();
    }
    List<BlockInfo> pendingBlocks = pendingRep.get(dn);
    if (pendingBlocks != null) {
      count += pendingBlocks.size();
    }
    return count;
  }

  /**
   * Iterate across all nodes in outOfServiceNodeBlocks which have blocks yet
   * to be processed.
   *
   * The block is removed from outOfServiceNodeBlocks and if it needs
   * replication it is added to the pendingRep map and also to the
   * BlockManager replication queue.
   *
   * Any block that does not need replication is discarded.
   *
   * The method will return when there are the pendingRep map has
   * pendingRepLimit blocks or there are no further blocks to process.
   */
  private void moveBlocksToPending() {
    int blocksProcessed = 0;
    int pendingCount = getPendingCount();
    int yetToBeProcessed = getYetToBeProcessedCount();

    if (pendingCount == 0 && yetToBeProcessed == 0) {
      // There are no blocks to process so just return
      LOG.debug("There are no pending or blocks yet to be processed");
      return;
    }

    namesystem.writeLock();
    try {
      long repQueueSize = blockManager.getLowRedundancyBlocksCount();

      LOG.info("There are {} blocks pending replication and the limit is "+
          "{}. A further {} blocks are waiting to be processed. "+
          "The replication queue currently has {} blocks",
          pendingCount, pendingRepLimit, yetToBeProcessed, repQueueSize);

      if (pendingCount >= pendingRepLimit) {
        // Only add more blocks to the replication queue if we don't already
        // have too many pending
        return;
      }

      // Create a "Block Iterator" for each node decommissioning or entering
      // maintenance. These iterators will be used "round robined" to add blocks
      // to the replication queue and PendingRep
      HashMap<DatanodeDescriptor, Iterator<BlockInfo>>
          iterators = new HashMap<>();
      for (Map.Entry<DatanodeDescriptor, HashMap<BlockInfo, Integer>> e
          : outOfServiceNodeBlocks.entrySet()) {
        iterators.put(e.getKey(), e.getValue().keySet().iterator());
      }

      // Now loop until we fill the pendingRep map with pendingRepLimit blocks
      // or run out of blocks to add.
      Iterator<DatanodeDescriptor> nodeIter =
          Iterables.cycle(iterators.keySet()).iterator();
      while (nodeIter.hasNext()) {
        // Cycle through each node with blocks which still need processed
        DatanodeDescriptor dn = nodeIter.next();
        Iterator<BlockInfo> blockIt = iterators.get(dn);
        while (blockIt.hasNext()) {
          // Process the blocks for the node until we find one that needs
          // replication
          if (blocksProcessed >= blocksPerLock) {
            blocksProcessed = 0;
            namesystem.writeUnlock();
            namesystem.writeLock();
          }
          blocksProcessed++;
          if (nextBlockAddedToPending(blockIt, dn)) {
            // Exit the inner "block" loop so an iterator for the next datanode
            // is used for the next block.
            pendingCount++;
            break;
          }
        }
        if (!blockIt.hasNext()) {
          // remove the iterator as there are no blocks left in it
          nodeIter.remove();
        }
        if (pendingCount >= pendingRepLimit) {
          // We have scheduled the limit of blocks for replication, so do
          // not add any more
          break;
        }
      }
    } finally {
      namesystem.writeUnlock();
    }
    LOG.debug("{} blocks are now pending replication", pendingCount);
  }

  /**
   * Takes and removes the next block from the given iterator and checks if it
   * needs additional replicas. If it does, it will be scheduled for
   * reconstruction and added to the pendingRep map.
   * @param it The iterator to take the next block from
   * @param dn The datanodeDescriptor the iterator applies to
   * @return True if the block needs replication, otherwise false
   */
  private boolean nextBlockAddedToPending(Iterator<BlockInfo> it,
      DatanodeDescriptor dn) {
    BlockInfo block = it.next();
    it.remove();
    numBlocksChecked++;
    if (!isBlockReplicatedOk(dn, block, true, null)) {
      pendingRep.computeIfAbsent(dn, k -> new LinkedList<>()).add(block);
      return true;
    }
    return false;
  }

  private int getPendingCount() {
    if (pendingRep.size() == 0) {
      return 0;
    }
    return pendingRep.values()
        .stream()
        .map(a -> a.size())
        .reduce(0, (a, b) -> a + b);
  }

  private int getYetToBeProcessedCount() {
    if (outOfServiceNodeBlocks.size() == 0) {
      return 0;
    }
    return outOfServiceNodeBlocks.values()
        .stream()
        .map(a -> a.size())
        .reduce(0, (a, b) -> a + b);
  }

  /**
   * Scan all the blocks held on a datanodes. For a node being decommissioned
   * we assume that the majority of blocks on the node will need to have new
   * replicas made, and therefore we do not check if they are under replicated
   * here and instead add them to the list of blocks to track.
   *
   * For a node being moved into maintenance, we assume most blocks will be
   * replicated OK and hence we do check their under-replicated status here,
   * hopefully reducing the number of blocks to track.
   *
   * On a re-scan (initalScan = false) we assume the node has been processed
   * already, and hence there should be few under-replicated blocks, so we
   * check the under-replicated status before adding the blocks to the
   * tracking list.
   *
   * This means that for a node being decomission there should be a large
   * number of blocks to process later but for maintenance, a smaller number.
   *
   * As this method does not schedule any blocks for reconstuction, this
   * scan can be performed under the namenode readlock, and the lock is
   * dropped and reaquired for each storage on the DN.
   *
   * @param dn - The datanode to process
   * @param initialScan - True is this is the first time scanning the node
   *                    or false if it is a rescan.
   */
  private void scanDatanodeStorage(DatanodeDescriptor dn,
                                   Boolean initialScan) {
    HashMap<BlockInfo, Integer> blockList = outOfServiceNodeBlocks.get(dn);
    if (blockList == null) {
      blockList = new HashMap<>();
      outOfServiceNodeBlocks.put(dn, blockList);
    }

    DatanodeStorageInfo[] storage;
    namesystem.readLock();
    try {
      storage = dn.getStorageInfos();
    } finally {
      namesystem.readUnlock();
    }

    for (DatanodeStorageInfo s : storage) {
      namesystem.readLock();
      try {
        // As the lock is dropped and re-taken between each storage, we need
        // to check the storage is still present before processing it, as it
        // may have been removed.
        if (dn.getStorageInfo(s.getStorageID()) == null) {
          continue;
        }
        Iterator<BlockInfo> it = s.getBlockIterator();
        while (it.hasNext()) {
          BlockInfo b = it.next();
          if (!initialScan || dn.isEnteringMaintenance()) {
            // this is a rescan, so most blocks should be replicated now,
            // or this node is going into maintenance. On a healthy
            // cluster using racks or upgrade domain, a node should be
            // able to go into maintenance without replicating many blocks
            // so we will check them immediately.
            if (!isBlockReplicatedOk(dn, b, false, null)) {
              blockList.put(b, null);
            }
          } else {
            blockList.put(b, null);
          }
          numBlocksChecked++;
        }
      } finally {
        namesystem.readUnlock();
      }
    }
  }

  /**
   * Process the list of pendingReplication Blocks. These are the blocks
   * which have been moved from outOfServiceNodeBlocks, confirmed to be
   * under-replicated and were added to the blockManager replication
   * queue.
   *
   * Any blocks which have been confirmed to be replicated sufficiently are
   * removed from the list.
   *
   * The datanode stats are also updated in this method, updating the total
   * pending block count, the number of blocks in PendingRep which are in
   * open files and the number of blocks in PendingRep which are only on
   * out of service nodes.
   *
   * As this method makes changes to the replication queue, it acquires the
   * namenode write lock while it runs.
   */
  private void processPendingReplication() {
    namesystem.writeLock();
    try {
      for (Iterator<Map.Entry<DatanodeDescriptor, List<BlockInfo>>>
           entIt = pendingRep.entrySet().iterator(); entIt.hasNext();) {
        Map.Entry<DatanodeDescriptor, List<BlockInfo>> entry = entIt.next();
        DatanodeDescriptor dn = entry.getKey();
        List<BlockInfo> blocks = entry.getValue();
        if (blocks == null) {
          // should not be able to happen
          entIt.remove();
          continue;
        }
        Iterator<BlockInfo> blockIt =  blocks.iterator();
        BlockStats suspectBlocks = new BlockStats();
        while(blockIt.hasNext()) {
          BlockInfo b = blockIt.next();
          if (isBlockReplicatedOk(dn, b, true, suspectBlocks)) {
            blockIt.remove();
          }
          numBlocksChecked++;
        }
        if (blocks.size() == 0) {
          entIt.remove();
        }
        // Update metrics for this datanode.
        dn.getLeavingServiceStatus().set(
            suspectBlocks.getOpenFileCount(),
            suspectBlocks.getOpenFiles(),
            getPendingCountForNode(dn),
            suspectBlocks.getOutOfServiceBlockCount());
      }
    } finally {
      namesystem.writeUnlock();
    }
  }

  /**
   * Checks if a block is sufficiently replicated and optionally schedules
   * it for reconstruction if it is not.
   *
   * If a BlockStats object is passed, this method will also update it if the
   * block is part of an open file or only on outOfService nodes.
   *
   * @param datanode The datanode the block belongs to
   * @param block The block to check
   * @param scheduleReconStruction Whether to add the block to the replication
   *                               queue if it is not sufficiently replicated.
   *                               Passing true will add it to the replication
   *                               queue, and false will not.
   * @param suspectBlocks If non-null check if the block is part of an open
   *                      file or only on out of service nodes and update the
   *                      passed object accordingly.
   * @return
   */
  private boolean isBlockReplicatedOk(DatanodeDescriptor datanode,
      BlockInfo block, Boolean scheduleReconStruction,
      BlockStats suspectBlocks) {
    if (blockManager.blocksMap.getStoredBlock(block) == null) {
      LOG.trace("Removing unknown block {}", block);
      return true;
    }

    long bcId = block.getBlockCollectionId();
    if (bcId == INodeId.INVALID_INODE_ID) {
      // Orphan block, will be invalidated eventually. Skip.
      return false;
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
    if (neededReconstruction && scheduleReconStruction) {
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

    if (suspectBlocks != null) {
      // Only if we pass a BlockStats object should we do these
      // checks, as they should only be checked when processing PendingRep.
      if (bc.isUnderConstruction()) {
        INode ucFile = namesystem.getFSDirectory().getInode(bc.getId());
        if (!(ucFile instanceof INodeFile) ||
            !ucFile.asFile().isUnderConstruction()) {
          LOG.warn("File {} is not under construction. Skipping add to " +
              "low redundancy open files!", ucFile.getLocalName());
        } else {
          suspectBlocks.addOpenFile(ucFile.getId());
        }
      }
      if ((liveReplicas == 0) && (num.outOfServiceReplicas() > 0)) {
        suspectBlocks.incrementOutOfServiceBlocks();
      }
    }

    // Even if the block is without sufficient redundancy,
    // it might not block decommission/maintenance if it
    // has sufficient redundancy.
    if (dnAdmin.isSufficient(block, bc, num, isDecommission, isMaintenance)) {
      return true;
    }
    return false;
  }

  static class BlockStats {
    private LightWeightHashSet<Long> openFiles =
        new LightWeightLinkedSet<>();
    private int openFileBlockCount = 0;
    private int outOfServiceBlockCount = 0;

    public void addOpenFile(long id) {
      // Several blocks can be part of the same file so track how
      // many adds we get, as the same file could be added several times
      // for different blocks.
      openFileBlockCount++;
      openFiles.add(id);
    }

    public void incrementOutOfServiceBlocks() {
      outOfServiceBlockCount++;
    }

    public LightWeightHashSet<Long> getOpenFiles() {
      return openFiles;
    }

    public int getOpenFileCount() {
      return openFileBlockCount;
    }

    public int getOutOfServiceBlockCount() {
      return outOfServiceBlockCount;
    }
  }
}