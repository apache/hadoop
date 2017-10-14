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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped.StorageAndBlockIndex;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.hdfs.server.protocol.BlocksStorageMoveAttemptFinished;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Setting storagePolicy on a file after the file write will only update the new
 * storage policy type in Namespace, but physical block storage movement will
 * not happen until user runs "Mover Tool" explicitly for such files. The
 * StoragePolicySatisfier Daemon thread implemented for addressing the case
 * where users may want to physically move the blocks by HDFS itself instead of
 * running mover tool explicitly. Just calling client API to
 * satisfyStoragePolicy on a file/dir will automatically trigger to move its
 * physical storage locations as expected in asynchronous manner. Here Namenode
 * will pick the file blocks which are expecting to change its storages, then it
 * will build the mapping of source block location and expected storage type and
 * location to move. After that this class will also prepare commands to send to
 * Datanode for processing the physical block movements.
 */
@InterfaceAudience.Private
public class StoragePolicySatisfier implements Runnable {
  public static final Logger LOG =
      LoggerFactory.getLogger(StoragePolicySatisfier.class);
  private Daemon storagePolicySatisfierThread;
  private final Namesystem namesystem;
  private final BlockManager blockManager;
  private final BlockStorageMovementNeeded storageMovementNeeded;
  private final BlockStorageMovementAttemptedItems storageMovementsMonitor;
  private volatile boolean isRunning = false;
  private int spsWorkMultiplier;
  private long blockCount = 0L;
  /**
   * Represents the collective analysis status for all blocks.
   */
  private static class BlocksMovingAnalysis {

    enum Status {
      // Represents that, the analysis skipped due to some conditions. A such
      // condition is if block collection is in incomplete state.
      ANALYSIS_SKIPPED_FOR_RETRY,
      // Represents that few or all blocks found respective target to do
      // the storage movement.
      BLOCKS_TARGETS_PAIRED,
      // Represents that none of the blocks found respective target to do
      // the storage movement.
      NO_BLOCKS_TARGETS_PAIRED,
      // Represents that, none of the blocks found for block storage movements.
      BLOCKS_ALREADY_SATISFIED,
      // Represents that, the analysis skipped due to some conditions.
      // Example conditions are if no blocks really exists in block collection
      // or
      // if analysis is not required on ec files with unsuitable storage
      // policies
      BLOCKS_TARGET_PAIRING_SKIPPED,
      // Represents that, All the reported blocks are satisfied the policy but
      // some of the blocks are low redundant.
      FEW_LOW_REDUNDANCY_BLOCKS
    }

    private Status status = null;
    private List<Block> assignedBlocks = null;

    BlocksMovingAnalysis(Status status, List<Block> blockMovingInfo) {
      this.status = status;
      this.assignedBlocks = blockMovingInfo;
    }
  }

  public StoragePolicySatisfier(final Namesystem namesystem,
      final BlockManager blkManager, Configuration conf) {
    this.namesystem = namesystem;
    this.storageMovementNeeded = new BlockStorageMovementNeeded(namesystem,
        this, conf.getInt(
            DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_KEY,
            DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_DEFAULT));
    this.blockManager = blkManager;
    this.storageMovementsMonitor = new BlockStorageMovementAttemptedItems(
        conf.getLong(
            DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
            DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_DEFAULT),
        conf.getLong(
            DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY,
            DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_DEFAULT),
        storageMovementNeeded);
    this.spsWorkMultiplier = DFSUtil.getSPSWorkMultiplier(conf);
  }

  /**
   * Start storage policy satisfier demon thread. Also start block storage
   * movements monitor for retry the attempts if needed.
   */
  public synchronized void start(boolean reconfigStart) {
    isRunning = true;
    if (checkIfMoverRunning()) {
      isRunning = false;
      LOG.error(
          "Stopping StoragePolicySatisfier thread " + "as Mover ID file "
              + HdfsServerConstants.MOVER_ID_PATH.toString()
              + " been opened. Maybe a Mover instance is running!");
      return;
    }
    if (reconfigStart) {
      LOG.info("Starting StoragePolicySatisfier, as admin requested to "
          + "start it.");
    } else {
      LOG.info("Starting StoragePolicySatisfier.");
    }

    // Ensure that all the previously submitted block movements(if any) have to
    // be stopped in all datanodes.
    addDropSPSWorkCommandsToAllDNs();
    storageMovementNeeded.init();
    storagePolicySatisfierThread = new Daemon(this);
    storagePolicySatisfierThread.setName("StoragePolicySatisfier");
    storagePolicySatisfierThread.start();
    this.storageMovementsMonitor.start();
  }

  /**
   * Disables storage policy satisfier by stopping its services.
   *
   * @param forceStop
   *          true represents that it should stop SPS service by clearing all
   *          pending SPS work
   */
  public synchronized void disable(boolean forceStop) {
    isRunning = false;

    if (storagePolicySatisfierThread == null) {
      return;
    }

    storageMovementNeeded.close();

    storagePolicySatisfierThread.interrupt();
    this.storageMovementsMonitor.stop();
    if (forceStop) {
      storageMovementNeeded.clearQueuesWithNotification();
      addDropSPSWorkCommandsToAllDNs();
    } else {
      LOG.info("Stopping StoragePolicySatisfier.");
    }
  }

  /**
   * Timed wait to stop storage policy satisfier daemon threads.
   */
  public synchronized void stopGracefully() {
    if (isRunning) {
      disable(true);
    }
    this.storageMovementsMonitor.stopGracefully();

    if (storagePolicySatisfierThread == null) {
      return;
    }
    try {
      storagePolicySatisfierThread.join(3000);
    } catch (InterruptedException ie) {
    }
  }

  /**
   * Check whether StoragePolicySatisfier is running.
   * @return true if running
   */
  public boolean isRunning() {
    return isRunning;
  }

  // Return true if a Mover instance is running
  private boolean checkIfMoverRunning() {
    String moverId = HdfsServerConstants.MOVER_ID_PATH.toString();
    return namesystem.isFileOpenedForWrite(moverId);
  }

  /**
   * Adding drop commands to all datanodes to stop performing the satisfier
   * block movements, if any.
   */
  private void addDropSPSWorkCommandsToAllDNs() {
    this.blockManager.getDatanodeManager().addDropSPSWorkCommandsToAllDNs();
  }

  @Override
  public void run() {
    while (namesystem.isRunning() && isRunning) {
      try {
        if (!namesystem.isInSafeMode()) {
          ItemInfo itemInfo = storageMovementNeeded.get();
          if (itemInfo != null) {
            long trackId = itemInfo.getTrackId();
            BlockCollection blockCollection;
            BlocksMovingAnalysis status = null;
            try {
              namesystem.readLock();
              blockCollection = namesystem.getBlockCollection(trackId);
              // Check blockCollectionId existence.
              if (blockCollection == null) {
                // File doesn't exists (maybe got deleted), remove trackId from
                // the queue
                storageMovementNeeded.removeItemTrackInfo(itemInfo);
              } else {
                status =
                    analyseBlocksStorageMovementsAndAssignToDN(
                        blockCollection);
              }
            } finally {
              namesystem.readUnlock();
            }
            if (blockCollection != null) {
              switch (status.status) {
              // Just add to monitor, so it will be retried after timeout
              case ANALYSIS_SKIPPED_FOR_RETRY:
                // Just add to monitor, so it will be tracked for report and
                // be removed on storage movement attempt finished report.
              case BLOCKS_TARGETS_PAIRED:
                this.storageMovementsMonitor.add(new AttemptedItemInfo(
                    itemInfo.getStartId(), itemInfo.getTrackId(),
                    monotonicNow(), status.assignedBlocks));
                break;
              case NO_BLOCKS_TARGETS_PAIRED:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding trackID " + trackId
                      + " back to retry queue as none of the blocks"
                      + " found its eligible targets.");
                }
                this.storageMovementNeeded.add(itemInfo);
                break;
              case FEW_LOW_REDUNDANCY_BLOCKS:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding trackID " + trackId
                      + " back to retry queue as some of the blocks"
                      + " are low redundant.");
                }
                this.storageMovementNeeded.add(itemInfo);
                break;
              // Just clean Xattrs
              case BLOCKS_TARGET_PAIRING_SKIPPED:
              case BLOCKS_ALREADY_SATISFIED:
              default:
                LOG.info("Block analysis skipped or blocks already satisfied"
                    + " with storages. So, Cleaning up the Xattrs.");
                storageMovementNeeded.removeItemTrackInfo(itemInfo);
                break;
              }
            }
          }
        }
        int numLiveDn = namesystem.getFSDirectory().getBlockManager()
            .getDatanodeManager().getNumLiveDataNodes();
        if (storageMovementNeeded.size() == 0
            || blockCount > (numLiveDn * spsWorkMultiplier)) {
          Thread.sleep(3000);
          blockCount = 0L;
        }
      } catch (Throwable t) {
        handleException(t);
      }
    }
  }

  private void handleException(Throwable t) {
    // double check to avoid entering into synchronized block.
    if (isRunning) {
      synchronized (this) {
        if (isRunning) {
          isRunning = false;
          // Stopping monitor thread and clearing queues as well
          this.clearQueues();
          this.storageMovementsMonitor.stopGracefully();
          if (!namesystem.isRunning()) {
            LOG.info("Stopping StoragePolicySatisfier.");
            if (!(t instanceof InterruptedException)) {
              LOG.info("StoragePolicySatisfier received an exception"
                  + " while shutting down.", t);
            }
            return;
          }
        }
      }
    }
    LOG.error("StoragePolicySatisfier thread received runtime exception. "
        + "Stopping Storage policy satisfier work", t);
    return;
  }

  private BlocksMovingAnalysis analyseBlocksStorageMovementsAndAssignToDN(
      BlockCollection blockCollection) {
    BlocksMovingAnalysis.Status status =
        BlocksMovingAnalysis.Status.BLOCKS_ALREADY_SATISFIED;
    byte existingStoragePolicyID = blockCollection.getStoragePolicyID();
    BlockStoragePolicy existingStoragePolicy =
        blockManager.getStoragePolicy(existingStoragePolicyID);
    if (!blockCollection.getLastBlock().isComplete()) {
      // Postpone, currently file is under construction
      // So, should we add back? or leave it to user
      LOG.info("BlockCollectionID: {} file is under construction. So, postpone"
          + " this to the next retry iteration", blockCollection.getId());
      return new BlocksMovingAnalysis(
          BlocksMovingAnalysis.Status.ANALYSIS_SKIPPED_FOR_RETRY,
          new ArrayList<>());
    }

    BlockInfo[] blocks = blockCollection.getBlocks();
    if (blocks.length == 0) {
      LOG.info("BlockCollectionID: {} file is not having any blocks."
          + " So, skipping the analysis.", blockCollection.getId());
      return new BlocksMovingAnalysis(
          BlocksMovingAnalysis.Status.BLOCKS_TARGET_PAIRING_SKIPPED,
          new ArrayList<>());
    }
    List<BlockMovingInfo> blockMovingInfos = new ArrayList<BlockMovingInfo>();

    for (int i = 0; i < blocks.length; i++) {
      BlockInfo blockInfo = blocks[i];
      List<StorageType> expectedStorageTypes;
      if (blockInfo.isStriped()) {
        if (ErasureCodingPolicyManager
            .checkStoragePolicySuitableForECStripedMode(
                existingStoragePolicyID)) {
          expectedStorageTypes = existingStoragePolicy
              .chooseStorageTypes((short) blockInfo.getCapacity());
        } else {
          // Currently we support only limited policies (HOT, COLD, ALLSSD)
          // for EC striped mode files. SPS will ignore to move the blocks if
          // the storage policy is not in EC Striped mode supported policies
          LOG.warn("The storage policy " + existingStoragePolicy.getName()
              + " is not suitable for Striped EC files. "
              + "So, ignoring to move the blocks");
          return new BlocksMovingAnalysis(
              BlocksMovingAnalysis.Status.BLOCKS_TARGET_PAIRING_SKIPPED,
              new ArrayList<>());
        }
      } else {
        expectedStorageTypes = existingStoragePolicy
            .chooseStorageTypes(blockInfo.getReplication());
      }

      DatanodeStorageInfo[] storages = blockManager.getStorages(blockInfo);
      StorageType[] storageTypes = new StorageType[storages.length];
      for (int j = 0; j < storages.length; j++) {
        DatanodeStorageInfo datanodeStorageInfo = storages[j];
        StorageType storageType = datanodeStorageInfo.getStorageType();
        storageTypes[j] = storageType;
      }
      List<StorageType> existing =
          new LinkedList<StorageType>(Arrays.asList(storageTypes));
      if (!DFSUtil.removeOverlapBetweenStorageTypes(expectedStorageTypes,
          existing, true)) {
        boolean blocksPaired = computeBlockMovingInfos(blockMovingInfos,
            blockInfo, expectedStorageTypes, existing, storages);
        if (blocksPaired) {
          status = BlocksMovingAnalysis.Status.BLOCKS_TARGETS_PAIRED;
        } else {
          // none of the blocks found its eligible targets for satisfying the
          // storage policy.
          status = BlocksMovingAnalysis.Status.NO_BLOCKS_TARGETS_PAIRED;
        }
      } else {
        if (blockManager.hasLowRedundancyBlocks(blockCollection)) {
          status = BlocksMovingAnalysis.Status.FEW_LOW_REDUNDANCY_BLOCKS;
        }
      }
    }

    List<Block> assignedBlockIds = new ArrayList<Block>();
    for (BlockMovingInfo blkMovingInfo : blockMovingInfos) {
      // Check for at least one block storage movement has been chosen
      if (blkMovingInfo.getTarget() != null) {
        // assign block storage movement task to the target node
        ((DatanodeDescriptor) blkMovingInfo.getTarget())
            .addBlocksToMoveStorage(blkMovingInfo);
        LOG.debug("BlockMovingInfo: {}", blkMovingInfo);
        assignedBlockIds.add(blkMovingInfo.getBlock());
        blockCount++;
      }
    }
    return new BlocksMovingAnalysis(status, assignedBlockIds);
  }

  /**
   * Compute the list of block moving information corresponding to the given
   * blockId. This will check that each block location of the given block is
   * satisfying the expected storage policy. If block location is not satisfied
   * the policy then find out the target node with the expected storage type to
   * satisfy the storage policy.
   *
   * @param blockMovingInfos
   *          - list of block source and target node pair
   * @param blockInfo
   *          - block details
   * @param expectedStorageTypes
   *          - list of expected storage type to satisfy the storage policy
   * @param existing
   *          - list to get existing storage types
   * @param storages
   *          - available storages
   * @return false if some of the block locations failed to find target node to
   *         satisfy the storage policy, true otherwise
   */
  private boolean computeBlockMovingInfos(
      List<BlockMovingInfo> blockMovingInfos, BlockInfo blockInfo,
      List<StorageType> expectedStorageTypes, List<StorageType> existing,
      DatanodeStorageInfo[] storages) {
    boolean foundMatchingTargetNodesForBlock = true;
    if (!DFSUtil.removeOverlapBetweenStorageTypes(expectedStorageTypes,
        existing, true)) {
      List<StorageTypeNodePair> sourceWithStorageMap =
          new ArrayList<StorageTypeNodePair>();
      List<DatanodeStorageInfo> existingBlockStorages =
          new ArrayList<DatanodeStorageInfo>(Arrays.asList(storages));
      // if expected type exists in source node already, local movement would be
      // possible, so lets find such sources first.
      Iterator<DatanodeStorageInfo> iterator = existingBlockStorages.iterator();
      while (iterator.hasNext()) {
        DatanodeStorageInfo datanodeStorageInfo = iterator.next();
        if (checkSourceAndTargetTypeExists(
            datanodeStorageInfo.getDatanodeDescriptor(), existing,
            expectedStorageTypes)) {
          sourceWithStorageMap
              .add(new StorageTypeNodePair(datanodeStorageInfo.getStorageType(),
                  datanodeStorageInfo.getDatanodeDescriptor()));
          iterator.remove();
          existing.remove(datanodeStorageInfo.getStorageType());
        }
      }

      // Let's find sources for existing types left.
      for (StorageType existingType : existing) {
        iterator = existingBlockStorages.iterator();
        while (iterator.hasNext()) {
          DatanodeStorageInfo datanodeStorageInfo = iterator.next();
          StorageType storageType = datanodeStorageInfo.getStorageType();
          if (storageType == existingType) {
            iterator.remove();
            sourceWithStorageMap.add(new StorageTypeNodePair(storageType,
                datanodeStorageInfo.getDatanodeDescriptor()));
            break;
          }
        }
      }

      StorageTypeNodeMap locsForExpectedStorageTypes =
          findTargetsForExpectedStorageTypes(expectedStorageTypes);

      foundMatchingTargetNodesForBlock |= findSourceAndTargetToMove(
          blockMovingInfos, blockInfo, sourceWithStorageMap,
          expectedStorageTypes, locsForExpectedStorageTypes);
    }
    return foundMatchingTargetNodesForBlock;
  }

  /**
   * Find the good target node for each source node for which block storages was
   * misplaced.
   *
   * @param blockMovingInfos
   *          - list of block source and target node pair
   * @param blockInfo
   *          - Block
   * @param sourceWithStorageList
   *          - Source Datanode with storages list
   * @param expected
   *          - Expecting storages to move
   * @param locsForExpectedStorageTypes
   *          - Available DNs for expected storage types
   * @return false if some of the block locations failed to find target node to
   *         satisfy the storage policy
   */
  private boolean findSourceAndTargetToMove(
      List<BlockMovingInfo> blockMovingInfos, BlockInfo blockInfo,
      List<StorageTypeNodePair> sourceWithStorageList,
      List<StorageType> expected,
      StorageTypeNodeMap locsForExpectedStorageTypes) {
    boolean foundMatchingTargetNodesForBlock = true;
    List<DatanodeDescriptor> excludeNodes = new ArrayList<>();

    // Looping over all the source node locations and choose the target
    // storage within same node if possible. This is done separately to
    // avoid choosing a target which already has this block.
    for (int i = 0; i < sourceWithStorageList.size(); i++) {
      StorageTypeNodePair existingTypeNodePair = sourceWithStorageList.get(i);

      // Check whether the block replica is already placed in the expected
      // storage type in this source datanode.
      if (!expected.contains(existingTypeNodePair.storageType)) {
        StorageTypeNodePair chosenTarget = chooseTargetTypeInSameNode(
            blockInfo, existingTypeNodePair.dn, expected);
        if (chosenTarget != null) {
          if (blockInfo.isStriped()) {
            buildStripedBlockMovingInfos(blockInfo, existingTypeNodePair.dn,
                existingTypeNodePair.storageType, chosenTarget.dn,
                chosenTarget.storageType, blockMovingInfos);
          } else {
            buildContinuousBlockMovingInfos(blockInfo, existingTypeNodePair.dn,
                existingTypeNodePair.storageType, chosenTarget.dn,
                chosenTarget.storageType, blockMovingInfos);
          }
          expected.remove(chosenTarget.storageType);
          // TODO: We can increment scheduled block count for this node?
        }
      }
      // To avoid choosing this excludeNodes as targets later
      excludeNodes.add(existingTypeNodePair.dn);
    }

    // Looping over all the source node locations. Choose a remote target
    // storage node if it was not found out within same node.
    for (int i = 0; i < sourceWithStorageList.size(); i++) {
      StorageTypeNodePair existingTypeNodePair = sourceWithStorageList.get(i);
      StorageTypeNodePair chosenTarget = null;
      // Chosen the target storage within same datanode. So just skipping this
      // source node.
      if (checkIfAlreadyChosen(blockMovingInfos, existingTypeNodePair.dn)) {
        continue;
      }
      if (chosenTarget == null && blockManager.getDatanodeManager()
          .getNetworkTopology().isNodeGroupAware()) {
        chosenTarget = chooseTarget(blockInfo, existingTypeNodePair.dn,
            expected, Matcher.SAME_NODE_GROUP, locsForExpectedStorageTypes,
            excludeNodes);
      }

      // Then, match nodes on the same rack
      if (chosenTarget == null) {
        chosenTarget =
            chooseTarget(blockInfo, existingTypeNodePair.dn, expected,
                Matcher.SAME_RACK, locsForExpectedStorageTypes, excludeNodes);
      }

      if (chosenTarget == null) {
        chosenTarget =
            chooseTarget(blockInfo, existingTypeNodePair.dn, expected,
                Matcher.ANY_OTHER, locsForExpectedStorageTypes, excludeNodes);
      }
      if (null != chosenTarget) {
        if (blockInfo.isStriped()) {
          buildStripedBlockMovingInfos(blockInfo, existingTypeNodePair.dn,
              existingTypeNodePair.storageType, chosenTarget.dn,
              chosenTarget.storageType, blockMovingInfos);
        } else {
          buildContinuousBlockMovingInfos(blockInfo, existingTypeNodePair.dn,
              existingTypeNodePair.storageType, chosenTarget.dn,
              chosenTarget.storageType, blockMovingInfos);
        }

        expected.remove(chosenTarget.storageType);
        excludeNodes.add(chosenTarget.dn);
        // TODO: We can increment scheduled block count for this node?
      } else {
        LOG.warn(
            "Failed to choose target datanode for the required"
                + " storage types {}, block:{}, existing storage type:{}",
            expected, blockInfo, existingTypeNodePair.storageType);
      }
    }

    if (expected.size() > 0) {
      foundMatchingTargetNodesForBlock = false;
    }

    return foundMatchingTargetNodesForBlock;
  }

  private boolean checkIfAlreadyChosen(List<BlockMovingInfo> blockMovingInfos,
      DatanodeDescriptor dn) {
    for (BlockMovingInfo blockMovingInfo : blockMovingInfos) {
      if (blockMovingInfo.getSource().equals(dn)) {
        return true;
      }
    }
    return false;
  }

  private void buildContinuousBlockMovingInfos(BlockInfo blockInfo,
      DatanodeInfo sourceNode, StorageType sourceStorageType,
      DatanodeInfo targetNode, StorageType targetStorageType,
      List<BlockMovingInfo> blkMovingInfos) {
    Block blk = new Block(blockInfo.getBlockId(), blockInfo.getNumBytes(),
        blockInfo.getGenerationStamp());
    BlockMovingInfo blkMovingInfo = new BlockMovingInfo(blk, sourceNode,
        targetNode, sourceStorageType, targetStorageType);
    blkMovingInfos.add(blkMovingInfo);
  }

  private void buildStripedBlockMovingInfos(BlockInfo blockInfo,
      DatanodeInfo sourceNode, StorageType sourceStorageType,
      DatanodeInfo targetNode, StorageType targetStorageType,
      List<BlockMovingInfo> blkMovingInfos) {
    // For a striped block, it needs to construct internal block at the given
    // index of a block group. Here it is iterating over all the block indices
    // and construct internal blocks which can be then considered for block
    // movement.
    BlockInfoStriped sBlockInfo = (BlockInfoStriped) blockInfo;
    for (StorageAndBlockIndex si : sBlockInfo.getStorageAndIndexInfos()) {
      if (si.getBlockIndex() >= 0) {
        DatanodeDescriptor dn = si.getStorage().getDatanodeDescriptor();
        if (sourceNode.equals(dn)) {
          // construct internal block
          long blockId = blockInfo.getBlockId() + si.getBlockIndex();
          long numBytes = StripedBlockUtil.getInternalBlockLength(
              sBlockInfo.getNumBytes(), sBlockInfo.getCellSize(),
              sBlockInfo.getDataBlockNum(), si.getBlockIndex());
          Block blk = new Block(blockId, numBytes,
              blockInfo.getGenerationStamp());
          BlockMovingInfo blkMovingInfo = new BlockMovingInfo(blk, sourceNode,
              targetNode, sourceStorageType, targetStorageType);
          blkMovingInfos.add(blkMovingInfo);
        }
      }
    }
  }

  /**
   * Choose the target storage within same datanode if possible.
   *
   * @param block
   *          - block info
   * @param source
   *          - source datanode
   * @param targetTypes
   *          - list of target storage types
   */
  private StorageTypeNodePair chooseTargetTypeInSameNode(Block block,
      DatanodeDescriptor source, List<StorageType> targetTypes) {
    for (StorageType t : targetTypes) {
      DatanodeStorageInfo chooseStorage4Block =
          source.chooseStorage4Block(t, block.getNumBytes());
      if (chooseStorage4Block != null) {
        return new StorageTypeNodePair(t, source);
      }
    }
    return null;
  }

  private StorageTypeNodePair chooseTarget(Block block,
      DatanodeDescriptor source, List<StorageType> targetTypes, Matcher matcher,
      StorageTypeNodeMap locsForExpectedStorageTypes,
      List<DatanodeDescriptor> excludeNodes) {
    for (StorageType t : targetTypes) {
      List<DatanodeDescriptor> nodesWithStorages =
          locsForExpectedStorageTypes.getNodesWithStorages(t);
      if (nodesWithStorages == null || nodesWithStorages.isEmpty()) {
        continue; // no target nodes with the required storage type.
      }
      Collections.shuffle(nodesWithStorages);
      for (DatanodeDescriptor target : nodesWithStorages) {
        if (!excludeNodes.contains(target) && matcher.match(
            blockManager.getDatanodeManager().getNetworkTopology(), source,
            target)) {
          if (null != target.chooseStorage4Block(t, block.getNumBytes())) {
            return new StorageTypeNodePair(t, target);
          }
        }
      }
    }
    return null;
  }

  private static class StorageTypeNodePair {
    private StorageType storageType = null;
    private DatanodeDescriptor dn = null;

    StorageTypeNodePair(StorageType storageType, DatanodeDescriptor dn) {
      this.storageType = storageType;
      this.dn = dn;
    }
  }

  private StorageTypeNodeMap findTargetsForExpectedStorageTypes(
      List<StorageType> expected) {
    StorageTypeNodeMap targetMap = new StorageTypeNodeMap();
    List<DatanodeDescriptor> reports = blockManager.getDatanodeManager()
        .getDatanodeListForReport(DatanodeReportType.LIVE);
    for (DatanodeDescriptor dn : reports) {
      StorageReport[] storageReports = dn.getStorageReports();
      for (StorageReport storageReport : storageReports) {
        StorageType t = storageReport.getStorage().getStorageType();
        if (expected.contains(t)) {
          final long maxRemaining = getMaxRemaining(dn.getStorageReports(), t);
          if (maxRemaining > 0L) {
            targetMap.add(t, dn);
          }
        }
      }
    }
    return targetMap;
  }

  private static long getMaxRemaining(StorageReport[] storageReports,
      StorageType t) {
    long max = 0L;
    for (StorageReport r : storageReports) {
      if (r.getStorage().getStorageType() == t) {
        if (r.getRemaining() > max) {
          max = r.getRemaining();
        }
      }
    }
    return max;
  }

  private boolean checkSourceAndTargetTypeExists(DatanodeDescriptor dn,
      List<StorageType> existing, List<StorageType> expectedStorageTypes) {
    DatanodeStorageInfo[] allDNStorageInfos = dn.getStorageInfos();
    boolean isExpectedTypeAvailable = false;
    boolean isExistingTypeAvailable = false;
    for (DatanodeStorageInfo dnInfo : allDNStorageInfos) {
      StorageType storageType = dnInfo.getStorageType();
      if (existing.contains(storageType)) {
        isExistingTypeAvailable = true;
      }
      if (expectedStorageTypes.contains(storageType)) {
        isExpectedTypeAvailable = true;
      }
    }
    return isExistingTypeAvailable && isExpectedTypeAvailable;
  }

  private static class StorageTypeNodeMap {
    private final EnumMap<StorageType, List<DatanodeDescriptor>> typeNodeMap =
        new EnumMap<StorageType, List<DatanodeDescriptor>>(StorageType.class);

    private void add(StorageType t, DatanodeDescriptor dn) {
      List<DatanodeDescriptor> nodesWithStorages = getNodesWithStorages(t);
      LinkedList<DatanodeDescriptor> value = null;
      if (nodesWithStorages == null) {
        value = new LinkedList<DatanodeDescriptor>();
        value.add(dn);
        typeNodeMap.put(t, value);
      } else {
        nodesWithStorages.add(dn);
      }
    }

    /**
     * @param type
     *          - Storage type
     * @return datanodes which has the given storage type
     */
    private List<DatanodeDescriptor> getNodesWithStorages(StorageType type) {
      return typeNodeMap.get(type);
    }
  }

  /**
   * Receives set of storage movement attempt finished blocks report.
   *
   * @param moveAttemptFinishedBlks
   *          set of storage movement attempt finished blocks.
   */
  void handleStorageMovementAttemptFinishedBlks(
      BlocksStorageMoveAttemptFinished moveAttemptFinishedBlks) {
    if (moveAttemptFinishedBlks.getBlocks().length <= 0) {
      return;
    }
    storageMovementsMonitor
        .addReportedMovedBlocks(moveAttemptFinishedBlks.getBlocks());
  }

  @VisibleForTesting
  BlockStorageMovementAttemptedItems getAttemptedItemsMonitor() {
    return storageMovementsMonitor;
  }

  /**
   * Clear the queues from to be storage movement needed lists and items tracked
   * in storage movement monitor.
   */
  public void clearQueues() {
    LOG.warn("Clearing all the queues from StoragePolicySatisfier. So, "
        + "user requests on satisfying block storages would be discarded.");
    storageMovementNeeded.clearAll();
  }

  /**
   * Set file inode in queue for which storage movement needed for its blocks.
   *
   * @param inodeId
   *          - file inode/blockcollection id.
   */
  public void satisfyStoragePolicy(Long inodeId) {
    //For file startId and trackId is same
    storageMovementNeeded.add(new ItemInfo(inodeId, inodeId));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added track info for inode {} to block "
          + "storageMovementNeeded queue", inodeId);
    }
  }

  public void addInodeToPendingDirQueue(long id) {
    storageMovementNeeded.addToPendingDirQueue(id);
  }

  /**
   * Clear queues for given track id.
   */
  public void clearQueue(long trackId) {
    storageMovementNeeded.clearQueue(trackId);
  }

  /**
   * ItemInfo is a file info object for which need to satisfy the
   * policy.
   */
  public static class ItemInfo {
    private long startId;
    private long trackId;

    public ItemInfo(long startId, long trackId) {
      this.startId = startId;
      this.trackId = trackId;
    }

    /**
     * Return the start inode id of the current track Id.
     */
    public long getStartId() {
      return startId;
    }

    /**
     * Return the File inode Id for which needs to satisfy the policy.
     */
    public long getTrackId() {
      return trackId;
    }

    /**
     * Returns true if the tracking path is a directory, false otherwise.
     */
    public boolean isDir() {
      return (startId != trackId);
    }
  }

  /**
   * This class contains information of an attempted blocks and its last
   * attempted or reported time stamp. This is used by
   * {@link BlockStorageMovementAttemptedItems#storageMovementAttemptedItems}.
   */
  final static class AttemptedItemInfo extends ItemInfo {
    private long lastAttemptedOrReportedTime;
    private final List<Block> blocks;

    /**
     * AttemptedItemInfo constructor.
     *
     * @param rootId
     *          rootId for trackId
     * @param trackId
     *          trackId for file.
     * @param lastAttemptedOrReportedTime
     *          last attempted or reported time
     */
    AttemptedItemInfo(long rootId, long trackId,
        long lastAttemptedOrReportedTime,
        List<Block> blocks) {
      super(rootId, trackId);
      this.lastAttemptedOrReportedTime = lastAttemptedOrReportedTime;
      this.blocks = blocks;
    }

    /**
     * @return last attempted or reported time stamp.
     */
    long getLastAttemptedOrReportedTime() {
      return lastAttemptedOrReportedTime;
    }

    /**
     * Update lastAttemptedOrReportedTime, so that the expiration time will be
     * postponed to future.
     */
    void touchLastReportedTimeStamp() {
      this.lastAttemptedOrReportedTime = monotonicNow();
    }

    List<Block> getBlocks() {
      return this.blocks;
    }

  }
}
