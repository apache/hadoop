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

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY;

import java.io.IOException;
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
import org.apache.hadoop.hdfs.server.protocol.BlocksStorageMovementResult;
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

  /**
   * Represents the collective analysis status for all blocks.
   */
  private enum BlocksMovingAnalysisStatus {
    // Represents that, the analysis skipped due to some conditions. A such
    // condition is if block collection is in incomplete state.
    ANALYSIS_SKIPPED_FOR_RETRY,
    // Represents that, all block storage movement needed blocks found its
    // targets.
    ALL_BLOCKS_TARGETS_PAIRED,
    // Represents that, only fewer or none of the block storage movement needed
    // block found its eligible targets.
    FEW_BLOCKS_TARGETS_PAIRED,
    // Represents that, none of the blocks found for block storage movements.
    BLOCKS_ALREADY_SATISFIED,
    // Represents that, the analysis skipped due to some conditions.
    // Example conditions are if no blocks really exists in block collection or
    // if analysis is not required on ec files with unsuitable storage policies
    BLOCKS_TARGET_PAIRING_SKIPPED,
    // Represents that, All the reported blocks are satisfied the policy but
    // some of the blocks are low redundant.
    FEW_LOW_REDUNDANCY_BLOCKS
  }

  public StoragePolicySatisfier(final Namesystem namesystem,
      final BlockStorageMovementNeeded storageMovementNeeded,
      final BlockManager blkManager, Configuration conf) {
    this.namesystem = namesystem;
    this.storageMovementNeeded = storageMovementNeeded;
    this.blockManager = blkManager;
    this.storageMovementsMonitor = new BlockStorageMovementAttemptedItems(
        conf.getLong(
            DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
            DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_DEFAULT),
        conf.getLong(
            DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY,
            DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_DEFAULT),
        storageMovementNeeded,
        this);
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
          + "activate it.");
    } else {
      LOG.info("Starting StoragePolicySatisfier.");
    }

    // Ensure that all the previously submitted block movements(if any) have to
    // be stopped in all datanodes.
    addDropSPSWorkCommandsToAllDNs();

    storagePolicySatisfierThread = new Daemon(this);
    storagePolicySatisfierThread.setName("StoragePolicySatisfier");
    storagePolicySatisfierThread.start();
    this.storageMovementsMonitor.start();
  }

  /**
   * Deactivates storage policy satisfier by stopping its services.
   *
   * @param reconfig
   *          true represents deactivating SPS service as requested by admin,
   *          false otherwise
   */
  public synchronized void deactivate(boolean reconfig) {
    isRunning = false;
    if (storagePolicySatisfierThread == null) {
      return;
    }

    storagePolicySatisfierThread.interrupt();
    this.storageMovementsMonitor.deactivate();
    if (reconfig) {
      LOG.info("Stopping StoragePolicySatisfier, as admin requested to "
          + "deactivate it.");
      this.clearQueuesWithNotification();
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
      deactivate(true);
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
          Long blockCollectionID = storageMovementNeeded.get();
          if (blockCollectionID != null) {
            BlockCollection blockCollection =
                namesystem.getBlockCollection(blockCollectionID);
            // Check blockCollectionId existence.
            if (blockCollection != null) {
              BlocksMovingAnalysisStatus status =
                  analyseBlocksStorageMovementsAndAssignToDN(blockCollection);
              switch (status) {
              // Just add to monitor, so it will be retried after timeout
              case ANALYSIS_SKIPPED_FOR_RETRY:
                // Just add to monitor, so it will be tracked for result and
                // be removed on successful storage movement result.
              case ALL_BLOCKS_TARGETS_PAIRED:
                this.storageMovementsMonitor.add(blockCollectionID, true);
                break;
              // Add to monitor with allBlcoksAttemptedToSatisfy flag false, so
              // that it will be tracked and still it will be consider for retry
              // as analysis was not found targets for storage movement blocks.
              case FEW_BLOCKS_TARGETS_PAIRED:
                this.storageMovementsMonitor.add(blockCollectionID, false);
                break;
              case FEW_LOW_REDUNDANCY_BLOCKS:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding trackID " + blockCollectionID
                      + " back to retry queue as some of the blocks"
                      + " are low redundant.");
                }
                this.storageMovementNeeded.add(blockCollectionID);
                break;
              // Just clean Xattrs
              case BLOCKS_TARGET_PAIRING_SKIPPED:
              case BLOCKS_ALREADY_SATISFIED:
              default:
                LOG.info("Block analysis skipped or blocks already satisfied"
                    + " with storages. So, Cleaning up the Xattrs.");
                postBlkStorageMovementCleanup(blockCollectionID);
                break;
              }
            }
          }
        }
        // TODO: We can think to make this as configurable later, how frequently
        // we want to check block movements.
        Thread.sleep(3000);
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

  private BlocksMovingAnalysisStatus analyseBlocksStorageMovementsAndAssignToDN(
      BlockCollection blockCollection) {
    BlocksMovingAnalysisStatus status =
        BlocksMovingAnalysisStatus.BLOCKS_ALREADY_SATISFIED;
    byte existingStoragePolicyID = blockCollection.getStoragePolicyID();
    BlockStoragePolicy existingStoragePolicy =
        blockManager.getStoragePolicy(existingStoragePolicyID);
    if (!blockCollection.getLastBlock().isComplete()) {
      // Postpone, currently file is under construction
      // So, should we add back? or leave it to user
      LOG.info("BlockCollectionID: {} file is under construction. So, postpone"
          + " this to the next retry iteration", blockCollection.getId());
      return BlocksMovingAnalysisStatus.ANALYSIS_SKIPPED_FOR_RETRY;
    }

    // First datanode will be chosen as the co-ordinator node for storage
    // movements. Later this can be optimized if needed.
    DatanodeDescriptor coordinatorNode = null;
    BlockInfo[] blocks = blockCollection.getBlocks();
    if (blocks.length == 0) {
      LOG.info("BlockCollectionID: {} file is not having any blocks."
          + " So, skipping the analysis.", blockCollection.getId());
      return BlocksMovingAnalysisStatus.BLOCKS_TARGET_PAIRING_SKIPPED;
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
          return BlocksMovingAnalysisStatus.BLOCKS_TARGET_PAIRING_SKIPPED;
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
        boolean computeStatus = computeBlockMovingInfos(blockMovingInfos,
            blockInfo, expectedStorageTypes, existing, storages);
        if (computeStatus
            && status != BlocksMovingAnalysisStatus.FEW_BLOCKS_TARGETS_PAIRED
            && !blockManager.hasLowRedundancyBlocks(blockCollection)) {
          status = BlocksMovingAnalysisStatus.ALL_BLOCKS_TARGETS_PAIRED;
        } else {
          status = BlocksMovingAnalysisStatus.FEW_BLOCKS_TARGETS_PAIRED;
        }
      } else {
        if (blockManager.hasLowRedundancyBlocks(blockCollection)) {
          status = BlocksMovingAnalysisStatus.FEW_LOW_REDUNDANCY_BLOCKS;
        }
      }
    }

    assignBlockMovingInfosToCoordinatorDn(blockCollection.getId(),
        blockMovingInfos, coordinatorNode);
    return status;
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

  private void assignBlockMovingInfosToCoordinatorDn(long blockCollectionID,
      List<BlockMovingInfo> blockMovingInfos,
      DatanodeDescriptor coordinatorNode) {

    if (blockMovingInfos.size() < 1) {
      // TODO: Major: handle this case. I think we need retry cases to
      // be implemented. Idea is, if some files are not getting storage movement
      // chances, then we can just retry limited number of times and exit.
      return;
    }

    // For now, first datanode will be chosen as the co-ordinator. Later
    // this can be optimized if needed.
    coordinatorNode = (DatanodeDescriptor) blockMovingInfos.get(0)
        .getSources()[0];

    boolean needBlockStorageMovement = false;
    for (BlockMovingInfo blkMovingInfo : blockMovingInfos) {
      // Check for atleast one block storage movement has been chosen
      if (blkMovingInfo.getTargets().length > 0){
        needBlockStorageMovement = true;
        break;
      }
    }
    if (!needBlockStorageMovement) {
      // Simply return as there is no targets selected for scheduling the block
      // movement.
      return;
    }

    // 'BlockCollectionId' is used as the tracking ID. All the blocks under this
    // blockCollectionID will be added to this datanode.
    coordinatorNode.addBlocksToMoveStorage(blockCollectionID, blockMovingInfos);
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
    List<DatanodeInfo> sourceNodes = new ArrayList<>();
    List<StorageType> sourceStorageTypes = new ArrayList<>();
    List<DatanodeInfo> targetNodes = new ArrayList<>();
    List<StorageType> targetStorageTypes = new ArrayList<>();
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
          sourceNodes.add(existingTypeNodePair.dn);
          sourceStorageTypes.add(existingTypeNodePair.storageType);
          targetNodes.add(chosenTarget.dn);
          targetStorageTypes.add(chosenTarget.storageType);
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
      if (sourceNodes.contains(existingTypeNodePair.dn)) {
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
        sourceNodes.add(existingTypeNodePair.dn);
        sourceStorageTypes.add(existingTypeNodePair.storageType);
        targetNodes.add(chosenTarget.dn);
        targetStorageTypes.add(chosenTarget.storageType);
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

    blockMovingInfos.addAll(getBlockMovingInfos(blockInfo, sourceNodes,
        sourceStorageTypes, targetNodes, targetStorageTypes));
    return foundMatchingTargetNodesForBlock;
  }

  private List<BlockMovingInfo> getBlockMovingInfos(BlockInfo blockInfo,
      List<DatanodeInfo> sourceNodes, List<StorageType> sourceStorageTypes,
      List<DatanodeInfo> targetNodes, List<StorageType> targetStorageTypes) {
    List<BlockMovingInfo> blkMovingInfos = new ArrayList<>();
    // No source-target node pair exists.
    if (sourceNodes.size() <= 0) {
      return blkMovingInfos;
    }

    if (blockInfo.isStriped()) {
      buildStripedBlockMovingInfos(blockInfo, sourceNodes, sourceStorageTypes,
          targetNodes, targetStorageTypes, blkMovingInfos);
    } else {
      buildContinuousBlockMovingInfos(blockInfo, sourceNodes,
          sourceStorageTypes, targetNodes, targetStorageTypes, blkMovingInfos);
    }
    return blkMovingInfos;
  }

  private void buildContinuousBlockMovingInfos(BlockInfo blockInfo,
      List<DatanodeInfo> sourceNodes, List<StorageType> sourceStorageTypes,
      List<DatanodeInfo> targetNodes, List<StorageType> targetStorageTypes,
      List<BlockMovingInfo> blkMovingInfos) {
    Block blk = new Block(blockInfo.getBlockId(), blockInfo.getNumBytes(),
        blockInfo.getGenerationStamp());
    BlockMovingInfo blkMovingInfo = new BlockMovingInfo(blk,
        sourceNodes.toArray(new DatanodeInfo[sourceNodes.size()]),
        targetNodes.toArray(new DatanodeInfo[targetNodes.size()]),
        sourceStorageTypes.toArray(new StorageType[sourceStorageTypes.size()]),
        targetStorageTypes.toArray(new StorageType[targetStorageTypes.size()]));
    blkMovingInfos.add(blkMovingInfo);
  }

  private void buildStripedBlockMovingInfos(BlockInfo blockInfo,
      List<DatanodeInfo> sourceNodes, List<StorageType> sourceStorageTypes,
      List<DatanodeInfo> targetNodes, List<StorageType> targetStorageTypes,
      List<BlockMovingInfo> blkMovingInfos) {
    // For a striped block, it needs to construct internal block at the given
    // index of a block group. Here it is iterating over all the block indices
    // and construct internal blocks which can be then considered for block
    // movement.
    BlockInfoStriped sBlockInfo = (BlockInfoStriped) blockInfo;
    for (StorageAndBlockIndex si : sBlockInfo.getStorageAndIndexInfos()) {
      if (si.getBlockIndex() >= 0) {
        DatanodeDescriptor dn = si.getStorage().getDatanodeDescriptor();
        DatanodeInfo[] srcNode = new DatanodeInfo[1];
        StorageType[] srcStorageType = new StorageType[1];
        DatanodeInfo[] targetNode = new DatanodeInfo[1];
        StorageType[] targetStorageType = new StorageType[1];
        for (int i = 0; i < sourceNodes.size(); i++) {
          DatanodeInfo node = sourceNodes.get(i);
          if (node.equals(dn)) {
            srcNode[0] = node;
            srcStorageType[0] = sourceStorageTypes.get(i);
            targetNode[0] = targetNodes.get(i);
            targetStorageType[0] = targetStorageTypes.get(i);

            // construct internal block
            long blockId = blockInfo.getBlockId() + si.getBlockIndex();
            long numBytes = StripedBlockUtil.getInternalBlockLength(
                sBlockInfo.getNumBytes(), sBlockInfo.getCellSize(),
                sBlockInfo.getDataBlockNum(), si.getBlockIndex());
            Block blk = new Block(blockId, numBytes,
                blockInfo.getGenerationStamp());
            BlockMovingInfo blkMovingInfo = new BlockMovingInfo(blk, srcNode,
                targetNode, srcStorageType, targetStorageType);
            blkMovingInfos.add(blkMovingInfo);
            break; // found matching source-target nodes
          }
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
   * Receives the movement results of collection of blocks associated to a
   * trackId.
   *
   * @param blksMovementResults
   *          movement status of the set of blocks associated to a trackId.
   */
  void handleBlocksStorageMovementResults(
      BlocksStorageMovementResult[] blksMovementResults) {
    if (blksMovementResults.length <= 0) {
      return;
    }
    storageMovementsMonitor.addResults(blksMovementResults);
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
   * Clean all the movements in storageMovementNeeded and notify
   * to clean up required resources.
   * @throws IOException
   */
  private void clearQueuesWithNotification() {
    Long id;
    while ((id = storageMovementNeeded.get()) != null) {
      try {
        postBlkStorageMovementCleanup(id);
      } catch (IOException ie) {
        LOG.warn("Failed to remove SPS "
            + "xattr for collection id " + id, ie);
      }
    }
  }

  /**
   * When block movement has been finished successfully, some additional
   * operations should be notified, for example, SPS xattr should be
   * removed.
   * @param trackId track id i.e., block collection id.
   * @throws IOException
   */
  public void postBlkStorageMovementCleanup(long trackId)
      throws IOException {
    this.namesystem.removeXattr(trackId, XATTR_SATISFY_STORAGE_POLICY);
  }
}
