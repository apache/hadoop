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
package org.apache.hadoop.hdfs.server.namenode.sps;

import static org.apache.hadoop.util.Time.monotonicNow;

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
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfyPathStatus;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.hdfs.server.protocol.BlocksStorageMoveAttemptFinished;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
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
 * where users may want to physically move the blocks by a dedidated daemon (can
 * run inside Namenode or stand alone) instead of running mover tool explicitly.
 * Just calling client API to satisfyStoragePolicy on a file/dir will
 * automatically trigger to move its physical storage locations as expected in
 * asynchronous manner. Here SPS will pick the file blocks which are expecting
 * to change its storages, then it will build the mapping of source block
 * location and expected storage type and location to move. After that this
 * class will also prepare requests to send to Datanode for processing the
 * physical block movements.
 */
@InterfaceAudience.Private
public class StoragePolicySatisfier implements SPSService, Runnable {
  public static final Logger LOG =
      LoggerFactory.getLogger(StoragePolicySatisfier.class);
  private Daemon storagePolicySatisfierThread;
  private BlockStorageMovementNeeded storageMovementNeeded;
  private BlockStorageMovementAttemptedItems storageMovementsMonitor;
  private volatile boolean isRunning = false;
  private int spsWorkMultiplier;
  private long blockCount = 0L;
  private int blockMovementMaxRetry;
  private Context ctxt;
  private BlockMoveTaskHandler blockMoveTaskHandler;
  private Configuration conf;

  public StoragePolicySatisfier(Configuration conf) {
    this.conf = conf;
  }
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
      FEW_LOW_REDUNDANCY_BLOCKS,
      // Represents that, movement failures due to unexpected errors.
      BLOCKS_FAILED_TO_MOVE
    }

    private Status status = null;
    private List<Block> assignedBlocks = null;

    BlocksMovingAnalysis(Status status, List<Block> blockMovingInfo) {
      this.status = status;
      this.assignedBlocks = blockMovingInfo;
    }
  }

  public void init(final Context context, final FileIdCollector fileIDCollector,
      final BlockMoveTaskHandler blockMovementTaskHandler) {
    this.ctxt = context;
    this.storageMovementNeeded =
        new BlockStorageMovementNeeded(context, fileIDCollector);
    this.storageMovementsMonitor =
        new BlockStorageMovementAttemptedItems(this,
        storageMovementNeeded);
    this.blockMoveTaskHandler = blockMovementTaskHandler;
    this.spsWorkMultiplier = DFSUtil.getSPSWorkMultiplier(getConf());
    this.blockMovementMaxRetry = getConf().getInt(
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MAX_RETRY_ATTEMPTS_KEY,
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MAX_RETRY_ATTEMPTS_DEFAULT);
  }

  /**
   * Start storage policy satisfier demon thread. Also start block storage
   * movements monitor for retry the attempts if needed.
   */
  @Override
  public synchronized void start(boolean reconfigStart) {
    isRunning = true;
    if (ctxt.isMoverRunning()) {
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
    storagePolicySatisfierThread = new Daemon(this);
    storagePolicySatisfierThread.setName("StoragePolicySatisfier");
    storagePolicySatisfierThread.start();
    this.storageMovementsMonitor.start();
    this.storageMovementNeeded.activate();
  }

  @Override
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

  @Override
  public synchronized void stopGracefully() {
    if (isRunning) {
      disable(true);
    }

    if (this.storageMovementsMonitor != null) {
      this.storageMovementsMonitor.stopGracefully();
    }

    if (storagePolicySatisfierThread == null) {
      return;
    }
    try {
      storagePolicySatisfierThread.join(3000);
    } catch (InterruptedException ie) {
    }
  }

  @Override
  public boolean isRunning() {
    return isRunning;
  }

  /**
   * Adding drop commands to all datanodes to stop performing the satisfier
   * block movements, if any.
   */
  private void addDropSPSWorkCommandsToAllDNs() {
    ctxt.addDropPreviousSPSWorkAtDNs();
  }

  @Override
  public void run() {
    while (ctxt.isRunning()) {
      try {
        if (!ctxt.isInSafeMode()) {
          ItemInfo itemInfo = storageMovementNeeded.get();
          if (itemInfo != null) {
            if(itemInfo.getRetryCount() >= blockMovementMaxRetry){
              LOG.info("Failed to satisfy the policy after "
                  + blockMovementMaxRetry + " retries. Removing inode "
                  + itemInfo.getFileId() + " from the queue");
              storageMovementNeeded.removeItemTrackInfo(itemInfo, false);
              continue;
            }
            long trackId = itemInfo.getFileId();
            BlocksMovingAnalysis status = null;
            DatanodeStorageReport[] liveDnReports;
            BlockStoragePolicy existingStoragePolicy;
            // TODO: presently, context internally acquire the lock
            // and returns the result. Need to discuss to move the lock outside?
            boolean hasLowRedundancyBlocks = ctxt
                .hasLowRedundancyBlocks(trackId);
            HdfsFileStatus fileStatus = ctxt.getFileInfo(trackId);
            // Check path existence.
            if (fileStatus == null || fileStatus.isDir()) {
              // File doesn't exists (maybe got deleted) or its a directory,
              // just remove trackId from the queue
              storageMovementNeeded.removeItemTrackInfo(itemInfo, true);
            } else {
              liveDnReports = ctxt.getLiveDatanodeStorageReport();
              byte existingStoragePolicyID = fileStatus.getStoragePolicy();
              existingStoragePolicy = ctxt
                  .getStoragePolicy(existingStoragePolicyID);

              HdfsLocatedFileStatus file = (HdfsLocatedFileStatus) fileStatus;
              status = analyseBlocksStorageMovementsAndAssignToDN(file,
                  hasLowRedundancyBlocks, existingStoragePolicy, liveDnReports);
              switch (status.status) {
              // Just add to monitor, so it will be retried after timeout
              case ANALYSIS_SKIPPED_FOR_RETRY:
                // Just add to monitor, so it will be tracked for report and
                // be removed on storage movement attempt finished report.
              case BLOCKS_TARGETS_PAIRED:
                this.storageMovementsMonitor.add(new AttemptedItemInfo(itemInfo
                    .getStartId(), itemInfo.getFileId(), monotonicNow(),
                    status.assignedBlocks, itemInfo.getRetryCount()));
                break;
              case NO_BLOCKS_TARGETS_PAIRED:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding trackID " + trackId
                      + " back to retry queue as none of the blocks"
                      + " found its eligible targets.");
                }
                itemInfo.increRetryCount();
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
              case BLOCKS_FAILED_TO_MOVE:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding trackID " + trackId
                      + " back to retry queue as some of the blocks"
                      + " movement failed.");
                }
                this.storageMovementNeeded.add(itemInfo);
                break;
              // Just clean Xattrs
              case BLOCKS_TARGET_PAIRING_SKIPPED:
              case BLOCKS_ALREADY_SATISFIED:
              default:
                LOG.info("Block analysis skipped or blocks already satisfied"
                    + " with storages. So, Cleaning up the Xattrs.");
                storageMovementNeeded.removeItemTrackInfo(itemInfo, true);
                break;
              }
            }
          }
        }
        int numLiveDn = ctxt.getNumLiveDataNodes();
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
          if (!(t instanceof InterruptedException)) {
            LOG.info("StoragePolicySatisfier received an exception"
                + " while shutting down.", t);
          }
          LOG.info("Stopping StoragePolicySatisfier.");
        }
      }
    }
    LOG.error("StoragePolicySatisfier thread received runtime exception. "
        + "Stopping Storage policy satisfier work", t);
    return;
  }

  private BlocksMovingAnalysis analyseBlocksStorageMovementsAndAssignToDN(
      HdfsLocatedFileStatus fileInfo, boolean hasLowRedundancyBlocks,
      BlockStoragePolicy existingStoragePolicy,
      DatanodeStorageReport[] liveDns) {
    BlocksMovingAnalysis.Status status =
        BlocksMovingAnalysis.Status.BLOCKS_ALREADY_SATISFIED;
    final ErasureCodingPolicy ecPolicy = fileInfo.getErasureCodingPolicy();
    final LocatedBlocks locatedBlocks = fileInfo.getLocatedBlocks();
    final boolean lastBlkComplete = locatedBlocks.isLastBlockComplete();
    if (!lastBlkComplete) {
      // Postpone, currently file is under construction
      // So, should we add back? or leave it to user
      LOG.info("BlockCollectionID: {} file is under construction. So, postpone"
          + " this to the next retry iteration", fileInfo.getFileId());
      return new BlocksMovingAnalysis(
          BlocksMovingAnalysis.Status.ANALYSIS_SKIPPED_FOR_RETRY,
          new ArrayList<>());
    }

    List<LocatedBlock> blocks = locatedBlocks.getLocatedBlocks();
    if (blocks.size() == 0) {
      LOG.info("BlockCollectionID: {} file is not having any blocks."
          + " So, skipping the analysis.", fileInfo.getFileId());
      return new BlocksMovingAnalysis(
          BlocksMovingAnalysis.Status.BLOCKS_TARGET_PAIRING_SKIPPED,
          new ArrayList<>());
    }
    List<BlockMovingInfo> blockMovingInfos = new ArrayList<BlockMovingInfo>();

    for (int i = 0; i < blocks.size(); i++) {
      LocatedBlock blockInfo = blocks.get(i);
      List<StorageType> expectedStorageTypes;
      if (blockInfo.isStriped()) {
        if (ErasureCodingPolicyManager
            .checkStoragePolicySuitableForECStripedMode(
                existingStoragePolicy.getId())) {
          expectedStorageTypes = existingStoragePolicy
              .chooseStorageTypes((short) blockInfo.getLocations().length);
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
            .chooseStorageTypes(fileInfo.getReplication());
      }

      List<StorageType> existing = new LinkedList<StorageType>(
          Arrays.asList(blockInfo.getStorageTypes()));
      if (!DFSUtil.removeOverlapBetweenStorageTypes(expectedStorageTypes,
          existing, true)) {
        boolean blocksPaired = computeBlockMovingInfos(blockMovingInfos,
            blockInfo, expectedStorageTypes, existing, blockInfo.getLocations(),
            liveDns, ecPolicy);
        if (blocksPaired) {
          status = BlocksMovingAnalysis.Status.BLOCKS_TARGETS_PAIRED;
        } else {
          // none of the blocks found its eligible targets for satisfying the
          // storage policy.
          status = BlocksMovingAnalysis.Status.NO_BLOCKS_TARGETS_PAIRED;
        }
      } else {
        if (hasLowRedundancyBlocks) {
          status = BlocksMovingAnalysis.Status.FEW_LOW_REDUNDANCY_BLOCKS;
        }
      }
    }

    List<Block> assignedBlockIds = new ArrayList<Block>();
    for (BlockMovingInfo blkMovingInfo : blockMovingInfos) {
      // Check for at least one block storage movement has been chosen
      try {
        blockMoveTaskHandler.submitMoveTask(blkMovingInfo,
            storageMovementsMonitor);
        LOG.debug("BlockMovingInfo: {}", blkMovingInfo);
        assignedBlockIds.add(blkMovingInfo.getBlock());
        blockCount++;
      } catch (IOException e) {
        LOG.warn("Exception while scheduling movement task", e);
        // failed to move the block.
        status = BlocksMovingAnalysis.Status.BLOCKS_FAILED_TO_MOVE;
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
      List<BlockMovingInfo> blockMovingInfos, LocatedBlock blockInfo,
      List<StorageType> expectedStorageTypes, List<StorageType> existing,
      DatanodeInfo[] storages, DatanodeStorageReport[] liveDns,
      ErasureCodingPolicy ecPolicy) {
    boolean foundMatchingTargetNodesForBlock = true;
    if (!DFSUtil.removeOverlapBetweenStorageTypes(expectedStorageTypes,
        existing, true)) {
      List<StorageTypeNodePair> sourceWithStorageMap =
          new ArrayList<StorageTypeNodePair>();
      List<DatanodeInfo> existingBlockStorages = new ArrayList<DatanodeInfo>(
          Arrays.asList(storages));
      // if expected type exists in source node already, local movement would be
      // possible, so lets find such sources first.
      Iterator<DatanodeInfo> iterator = existingBlockStorages.iterator();
      while (iterator.hasNext()) {
        DatanodeInfoWithStorage dnInfo = (DatanodeInfoWithStorage) iterator
            .next();
        if (checkSourceAndTargetTypeExists(dnInfo, existing,
            expectedStorageTypes, liveDns)) {
          sourceWithStorageMap
              .add(new StorageTypeNodePair(dnInfo.getStorageType(), dnInfo));
          iterator.remove();
          existing.remove(dnInfo.getStorageType());
        }
      }

      // Let's find sources for existing types left.
      for (StorageType existingType : existing) {
        iterator = existingBlockStorages.iterator();
        while (iterator.hasNext()) {
          DatanodeInfoWithStorage dnStorageInfo =
              (DatanodeInfoWithStorage) iterator.next();
          StorageType storageType = dnStorageInfo.getStorageType();
          if (storageType == existingType) {
            iterator.remove();
            sourceWithStorageMap.add(new StorageTypeNodePair(storageType,
                dnStorageInfo));
            break;
          }
        }
      }

      StorageTypeNodeMap locsForExpectedStorageTypes =
          findTargetsForExpectedStorageTypes(expectedStorageTypes, liveDns);

      foundMatchingTargetNodesForBlock |= findSourceAndTargetToMove(
          blockMovingInfos, blockInfo, sourceWithStorageMap,
          expectedStorageTypes, locsForExpectedStorageTypes,
          ecPolicy);
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
      List<BlockMovingInfo> blockMovingInfos, LocatedBlock blockInfo,
      List<StorageTypeNodePair> sourceWithStorageList,
      List<StorageType> expected,
      StorageTypeNodeMap locsForExpectedStorageTypes,
      ErasureCodingPolicy ecPolicy) {
    boolean foundMatchingTargetNodesForBlock = true;
    List<DatanodeInfo> excludeNodes = new ArrayList<>();

    // Looping over all the source node locations and choose the target
    // storage within same node if possible. This is done separately to
    // avoid choosing a target which already has this block.
    for (int i = 0; i < sourceWithStorageList.size(); i++) {
      StorageTypeNodePair existingTypeNodePair = sourceWithStorageList.get(i);

      // Check whether the block replica is already placed in the expected
      // storage type in this source datanode.
      if (!expected.contains(existingTypeNodePair.storageType)) {
        StorageTypeNodePair chosenTarget = chooseTargetTypeInSameNode(blockInfo,
            existingTypeNodePair.dn, expected);
        if (chosenTarget != null) {
          if (blockInfo.isStriped()) {
            buildStripedBlockMovingInfos(blockInfo, existingTypeNodePair.dn,
                existingTypeNodePair.storageType, chosenTarget.dn,
                chosenTarget.storageType, blockMovingInfos,
                ecPolicy);
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
      if (chosenTarget == null && ctxt
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
              chosenTarget.storageType, blockMovingInfos, ecPolicy);
        } else {
          buildContinuousBlockMovingInfos(blockInfo, existingTypeNodePair.dn,
              existingTypeNodePair.storageType, chosenTarget.dn,
              chosenTarget.storageType, blockMovingInfos);
        }

        expected.remove(chosenTarget.storageType);
        excludeNodes.add(chosenTarget.dn);
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
      DatanodeInfo dn) {
    for (BlockMovingInfo blockMovingInfo : blockMovingInfos) {
      if (blockMovingInfo.getSource().equals(dn)) {
        return true;
      }
    }
    return false;
  }

  private void buildContinuousBlockMovingInfos(LocatedBlock blockInfo,
      DatanodeInfo sourceNode, StorageType sourceStorageType,
      DatanodeInfo targetNode, StorageType targetStorageType,
      List<BlockMovingInfo> blkMovingInfos) {
    Block blk = ExtendedBlock.getLocalBlock(blockInfo.getBlock());
    BlockMovingInfo blkMovingInfo = new BlockMovingInfo(blk, sourceNode,
        targetNode, sourceStorageType, targetStorageType);
    blkMovingInfos.add(blkMovingInfo);
  }

  private void buildStripedBlockMovingInfos(LocatedBlock blockInfo,
      DatanodeInfo sourceNode, StorageType sourceStorageType,
      DatanodeInfo targetNode, StorageType targetStorageType,
      List<BlockMovingInfo> blkMovingInfos, ErasureCodingPolicy ecPolicy) {
    // For a striped block, it needs to construct internal block at the given
    // index of a block group. Here it is iterating over all the block indices
    // and construct internal blocks which can be then considered for block
    // movement.
    LocatedStripedBlock sBlockInfo = (LocatedStripedBlock) blockInfo;
    byte[] indices = sBlockInfo.getBlockIndices();
    DatanodeInfo[] locations = sBlockInfo.getLocations();
    for (int i = 0; i < indices.length; i++) {
      byte blkIndex = indices[i];
      if (blkIndex >= 0) {
        // pick block movement only for the given source node.
        if (sourceNode.equals(locations[i])) {
          // construct internal block
          ExtendedBlock extBlock = sBlockInfo.getBlock();
          long numBytes = StripedBlockUtil.getInternalBlockLength(
              extBlock.getNumBytes(), ecPolicy, blkIndex);
          Block blk = new Block(ExtendedBlock.getLocalBlock(extBlock));
          long blkId = blk.getBlockId() + blkIndex;
          blk.setBlockId(blkId);
          blk.setNumBytes(numBytes);
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
  private StorageTypeNodePair chooseTargetTypeInSameNode(LocatedBlock blockInfo,
      DatanodeInfo source, List<StorageType> targetTypes) {
    for (StorageType t : targetTypes) {
      boolean goodTargetDn = ctxt.verifyTargetDatanodeHasSpaceForScheduling(
          source, t, blockInfo.getBlockSize());
      if (goodTargetDn) {
        return new StorageTypeNodePair(t, source);
      }
    }
    return null;
  }

  private StorageTypeNodePair chooseTarget(LocatedBlock block,
      DatanodeInfo source, List<StorageType> targetTypes, Matcher matcher,
      StorageTypeNodeMap locsForExpectedStorageTypes,
      List<DatanodeInfo> excludeNodes) {
    for (StorageType t : targetTypes) {
      List<DatanodeInfo> nodesWithStorages = locsForExpectedStorageTypes
          .getNodesWithStorages(t);
      if (nodesWithStorages == null || nodesWithStorages.isEmpty()) {
        continue; // no target nodes with the required storage type.
      }
      Collections.shuffle(nodesWithStorages);
      for (DatanodeInfo target : nodesWithStorages) {
        if (!excludeNodes.contains(target)
            && matcher.match(ctxt.getNetworkTopology(), source, target)) {
          boolean goodTargetDn = ctxt.verifyTargetDatanodeHasSpaceForScheduling(
              target, t, block.getBlockSize());
          if (goodTargetDn) {
            return new StorageTypeNodePair(t, target);
          }
        }
      }
    }
    return null;
  }

  private static class StorageTypeNodePair {
    private StorageType storageType = null;
    private DatanodeInfo dn = null;

    StorageTypeNodePair(StorageType storageType, DatanodeInfo dn) {
      this.storageType = storageType;
      this.dn = dn;
    }
  }

  private StorageTypeNodeMap findTargetsForExpectedStorageTypes(
      List<StorageType> expected, DatanodeStorageReport[] liveDns) {
    StorageTypeNodeMap targetMap = new StorageTypeNodeMap();
    for (DatanodeStorageReport dn : liveDns) {
      StorageReport[] storageReports = dn.getStorageReports();
      for (StorageReport storageReport : storageReports) {
        StorageType t = storageReport.getStorage().getStorageType();
        if (expected.contains(t)) {
          final long maxRemaining = getMaxRemaining(dn.getStorageReports(), t);
          if (maxRemaining > 0L) {
            targetMap.add(t, dn.getDatanodeInfo());
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

  private boolean checkSourceAndTargetTypeExists(DatanodeInfo dn,
      List<StorageType> existing, List<StorageType> expectedStorageTypes,
      DatanodeStorageReport[] liveDns) {
    boolean isExpectedTypeAvailable = false;
    boolean isExistingTypeAvailable = false;
    for (DatanodeStorageReport liveDn : liveDns) {
      if (dn.equals(liveDn.getDatanodeInfo())) {
        StorageReport[] storageReports = liveDn.getStorageReports();
        for (StorageReport eachStorage : storageReports) {
          StorageType storageType = eachStorage.getStorage().getStorageType();
          if (existing.contains(storageType)) {
            isExistingTypeAvailable = true;
          }
          if (expectedStorageTypes.contains(storageType)) {
            isExpectedTypeAvailable = true;
          }
          if (isExistingTypeAvailable && isExpectedTypeAvailable) {
            return true;
          }
        }
      }
    }
    return isExistingTypeAvailable && isExpectedTypeAvailable;
  }

  private static class StorageTypeNodeMap {
    private final EnumMap<StorageType, List<DatanodeInfo>> typeNodeMap =
        new EnumMap<StorageType, List<DatanodeInfo>>(StorageType.class);

    private void add(StorageType t, DatanodeInfo dn) {
      List<DatanodeInfo> nodesWithStorages = getNodesWithStorages(t);
      LinkedList<DatanodeInfo> value = null;
      if (nodesWithStorages == null) {
        value = new LinkedList<DatanodeInfo>();
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
    private List<DatanodeInfo> getNodesWithStorages(StorageType type) {
      return typeNodeMap.get(type);
    }
  }

  /**
   * Receives set of storage movement attempt finished blocks report.
   *
   * @param moveAttemptFinishedBlks
   *          set of storage movement attempt finished blocks.
   */
  public void handleStorageMovementAttemptFinishedBlks(
      BlocksStorageMoveAttemptFinished moveAttemptFinishedBlks) {
    if (moveAttemptFinishedBlks.getBlocks().length <= 0) {
      return;
    }
    storageMovementsMonitor
        .notifyMovementTriedBlocks(moveAttemptFinishedBlks.getBlocks());
  }

  @VisibleForTesting
  BlockMovementListener getAttemptedItemsMonitor() {
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

  /**
   * Clear queues for given track id.
   */
  public void clearQueue(long trackId) {
    storageMovementNeeded.clearQueue(trackId);
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
        List<Block> blocks, int retryCount) {
      super(rootId, trackId, retryCount);
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

  public StoragePolicySatisfyPathStatus checkStoragePolicySatisfyPathStatus(
      String path) throws IOException {
    return storageMovementNeeded.getStatus(ctxt.getFileID(path));
  }

  @Override
  public void addFileIdToProcess(ItemInfo trackInfo) {
    storageMovementNeeded.add(trackInfo);
  }

  @Override
  public void addAllFileIdsToProcess(long startId, List<ItemInfo> itemInfoList,
      boolean scanCompleted) {
    getStorageMovementQueue().addAll(startId, itemInfoList, scanCompleted);
  }

  @Override
  public int processingQueueSize() {
    return storageMovementNeeded.size();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @VisibleForTesting
  public BlockStorageMovementNeeded getStorageMovementQueue() {
    return storageMovementNeeded;
  }
}
