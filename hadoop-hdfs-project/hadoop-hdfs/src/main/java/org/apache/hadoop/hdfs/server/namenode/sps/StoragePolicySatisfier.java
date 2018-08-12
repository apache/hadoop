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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Setting storagePolicy on a file after the file write will only update the new
 * storage policy type in Namespace, but physical block storage movement will
 * not happen until user runs "Mover Tool" explicitly for such files. The
 * StoragePolicySatisfier Daemon thread implemented for addressing the case
 * where users may want to physically move the blocks by a dedicated daemon (can
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
  private final Configuration conf;
  private DatanodeCacheManager dnCacheMgr;

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
    private Map<Block, Set<StorageTypeNodePair>> assignedBlocks = null;

    BlocksMovingAnalysis(Status status,
        Map<Block, Set<StorageTypeNodePair>> assignedBlocks) {
      this.status = status;
      this.assignedBlocks = assignedBlocks;
    }
  }

  public void init(final Context context) {
    this.ctxt = context;
    this.storageMovementNeeded = new BlockStorageMovementNeeded(context);
    this.storageMovementsMonitor = new BlockStorageMovementAttemptedItems(
        this, storageMovementNeeded, context);
    this.spsWorkMultiplier = getSPSWorkMultiplier(getConf());
    this.blockMovementMaxRetry = getConf().getInt(
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MAX_RETRY_ATTEMPTS_KEY,
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MAX_RETRY_ATTEMPTS_DEFAULT);
  }

  /**
   * Start storage policy satisfier demon thread. Also start block storage
   * movements monitor for retry the attempts if needed.
   */
  @Override
  public synchronized void start(StoragePolicySatisfierMode serviceMode) {
    if (serviceMode == StoragePolicySatisfierMode.NONE) {
      LOG.error("Can't start StoragePolicySatisfier for the given mode:{}",
          serviceMode);
      return;
    }
    LOG.info("Starting {} StoragePolicySatisfier.",
        StringUtils.toLowerCase(serviceMode.toString()));
    isRunning = true;
    storagePolicySatisfierThread = new Daemon(this);
    storagePolicySatisfierThread.setName("StoragePolicySatisfier");
    storagePolicySatisfierThread.start();
    this.storageMovementsMonitor.start();
    this.storageMovementNeeded.activate();
    dnCacheMgr = new DatanodeCacheManager(conf);
  }

  @Override
  public synchronized void stop(boolean forceStop) {
    isRunning = false;
    if (storagePolicySatisfierThread == null) {
      return;
    }

    storageMovementNeeded.close();

    storagePolicySatisfierThread.interrupt();
    this.storageMovementsMonitor.stop();
    if (forceStop) {
      storageMovementNeeded.clearQueuesWithNotification();
    } else {
      LOG.info("Stopping StoragePolicySatisfier.");
    }
  }

  @Override
  public synchronized void stopGracefully() {
    if (isRunning) {
      stop(false);
    }

    if (this.storageMovementsMonitor != null) {
      this.storageMovementsMonitor.stopGracefully();
    }

    if (storagePolicySatisfierThread != null) {
      try {
        storagePolicySatisfierThread.join(3000);
      } catch (InterruptedException ie) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Interrupted Exception while waiting to join sps thread,"
              + " ignoring it", ie);
        }
      }
    }
  }

  @Override
  public boolean isRunning() {
    return isRunning;
  }

  @Override
  public void run() {
    while (isRunning) {
      // Check if dependent service is running
      if (!ctxt.isRunning()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Upstream service is down, skipping the sps work.");
        }
        continue;
      }
      ItemInfo itemInfo = null;
      try {
        boolean retryItem = false;
        if (!ctxt.isInSafeMode()) {
          itemInfo = storageMovementNeeded.get();
          if (itemInfo != null) {
            if(itemInfo.getRetryCount() >= blockMovementMaxRetry){
              LOG.info("Failed to satisfy the policy after "
                  + blockMovementMaxRetry + " retries. Removing inode "
                  + itemInfo.getFile() + " from the queue");
              storageMovementNeeded.removeItemTrackInfo(itemInfo, false);
              continue;
            }
            long trackId = itemInfo.getFile();
            BlocksMovingAnalysis status = null;
            BlockStoragePolicy existingStoragePolicy;
            // TODO: presently, context internally acquire the lock
            // and returns the result. Need to discuss to move the lock outside?
            HdfsFileStatus fileStatus = ctxt.getFileInfo(trackId);
            // Check path existence.
            if (fileStatus == null || fileStatus.isDir()) {
              // File doesn't exists (maybe got deleted) or its a directory,
              // just remove trackId from the queue
              storageMovementNeeded.removeItemTrackInfo(itemInfo, true);
            } else {
              byte existingStoragePolicyID = fileStatus.getStoragePolicy();
              existingStoragePolicy = ctxt
                  .getStoragePolicy(existingStoragePolicyID);

              HdfsLocatedFileStatus file = (HdfsLocatedFileStatus) fileStatus;
              status = analyseBlocksStorageMovementsAndAssignToDN(file,
                  existingStoragePolicy);
              switch (status.status) {
              // Just add to monitor, so it will be retried after timeout
              case ANALYSIS_SKIPPED_FOR_RETRY:
                // Just add to monitor, so it will be tracked for report and
                // be removed on storage movement attempt finished report.
              case BLOCKS_TARGETS_PAIRED:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Block analysis status:{} for the file id:{}."
                      + " Adding to attempt monitor queue for the storage "
                      + "movement attempt finished report",
                      status.status, fileStatus.getFileId());
                }
                this.storageMovementsMonitor.add(itemInfo.getStartPath(),
                    itemInfo.getFile(), monotonicNow(), status.assignedBlocks,
                    itemInfo.getRetryCount());
                break;
              case NO_BLOCKS_TARGETS_PAIRED:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding trackID:{} for the file id:{} back to"
                      + " retry queue as none of the blocks found its eligible"
                      + " targets.", trackId, fileStatus.getFileId());
                }
                retryItem = true;
                break;
              case FEW_LOW_REDUNDANCY_BLOCKS:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding trackID:{} for the file id:{} back to "
                      + "retry queue as some of the blocks are low redundant.",
                      trackId, fileStatus.getFileId());
                }
                retryItem = true;
                break;
              case BLOCKS_FAILED_TO_MOVE:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding trackID:{} for the file id:{} back to "
                      + "retry queue as some of the blocks movement failed.",
                      trackId, fileStatus.getFileId());
                }
                retryItem = true;
                break;
              // Just clean Xattrs
              case BLOCKS_TARGET_PAIRING_SKIPPED:
              case BLOCKS_ALREADY_SATISFIED:
              default:
                LOG.info("Block analysis status:{} for the file id:{}."
                    + " So, Cleaning up the Xattrs.", status.status,
                    fileStatus.getFileId());
                storageMovementNeeded.removeItemTrackInfo(itemInfo, true);
                break;
              }
            }
          }
        } else {
          LOG.info("Namenode is in safemode. It will retry again.");
          Thread.sleep(3000);
        }
        int numLiveDn = ctxt.getNumLiveDataNodes();
        if (storageMovementNeeded.size() == 0
            || blockCount > (numLiveDn * spsWorkMultiplier)) {
          Thread.sleep(3000);
          blockCount = 0L;
        }
        if (retryItem) {
          this.storageMovementNeeded.add(itemInfo);
        }
      } catch (IOException e) {
        LOG.error("Exception during StoragePolicySatisfier execution - "
            + "will continue next cycle", e);
        // Since it could not finish this item in previous iteration due to IOE,
        // just try again.
        this.storageMovementNeeded.add(itemInfo);
      } catch (Throwable t) {
        synchronized (this) {
          if (isRunning) {
            isRunning = false;
            if (t instanceof InterruptedException) {
              LOG.info("Stopping StoragePolicySatisfier.", t);
            } else {
              LOG.error("StoragePolicySatisfier thread received "
                  + "runtime exception.", t);
            }
            // Stopping monitor thread and clearing queues as well
            this.clearQueues();
            this.storageMovementsMonitor.stopGracefully();
          }
        }
      }
    }
  }

  private BlocksMovingAnalysis analyseBlocksStorageMovementsAndAssignToDN(
      HdfsLocatedFileStatus fileInfo,
      BlockStoragePolicy existingStoragePolicy) throws IOException {
    BlocksMovingAnalysis.Status status =
        BlocksMovingAnalysis.Status.BLOCKS_ALREADY_SATISFIED;
    final ErasureCodingPolicy ecPolicy = fileInfo.getErasureCodingPolicy();
    final LocatedBlocks locatedBlocks = fileInfo.getLocatedBlocks();
    final boolean lastBlkComplete = locatedBlocks.isLastBlockComplete();
    if (!lastBlkComplete) {
      // Postpone, currently file is under construction
      LOG.info("File: {} is under construction. So, postpone"
          + " this to the next retry iteration", fileInfo.getFileId());
      return new BlocksMovingAnalysis(
          BlocksMovingAnalysis.Status.ANALYSIS_SKIPPED_FOR_RETRY,
          new HashMap<>());
    }

    List<LocatedBlock> blocks = locatedBlocks.getLocatedBlocks();
    if (blocks.size() == 0) {
      LOG.info("File: {} is not having any blocks."
          + " So, skipping the analysis.", fileInfo.getFileId());
      return new BlocksMovingAnalysis(
          BlocksMovingAnalysis.Status.BLOCKS_TARGET_PAIRING_SKIPPED,
          new HashMap<>());
    }
    List<BlockMovingInfo> blockMovingInfos = new ArrayList<BlockMovingInfo>();
    boolean hasLowRedundancyBlocks = false;
    int replication = fileInfo.getReplication();
    DatanodeMap liveDns = dnCacheMgr.getLiveDatanodeStorageReport(ctxt);
    for (int i = 0; i < blocks.size(); i++) {
      LocatedBlock blockInfo = blocks.get(i);

      // Block is considered as low redundancy when the block locations array
      // length is less than expected replication factor. If any of the block is
      // low redundant, then hasLowRedundancyBlocks will be marked as true.
      hasLowRedundancyBlocks |= isLowRedundancyBlock(blockInfo, replication,
          ecPolicy);

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
              new HashMap<>());
        }
      } else {
        expectedStorageTypes = existingStoragePolicy
            .chooseStorageTypes(fileInfo.getReplication());
      }

      List<StorageType> existing = new LinkedList<StorageType>(
          Arrays.asList(blockInfo.getStorageTypes()));
      if (!removeOverlapBetweenStorageTypes(expectedStorageTypes,
          existing, true)) {
        boolean blocksPaired = computeBlockMovingInfos(blockMovingInfos,
            blockInfo, expectedStorageTypes, existing, blockInfo.getLocations(),
            liveDns, ecPolicy);
        if (blocksPaired) {
          status = BlocksMovingAnalysis.Status.BLOCKS_TARGETS_PAIRED;
        } else if (status !=
            BlocksMovingAnalysis.Status.BLOCKS_TARGETS_PAIRED) {
          // Check if the previous block was successfully paired. Here the
          // status will set to NO_BLOCKS_TARGETS_PAIRED only when none of the
          // blocks of a file found its eligible targets to satisfy the storage
          // policy.
          status = BlocksMovingAnalysis.Status.NO_BLOCKS_TARGETS_PAIRED;
        }
      }
    }

    // If there is no block paired and few blocks are low redundant, so marking
    // the status as FEW_LOW_REDUNDANCY_BLOCKS.
    if (hasLowRedundancyBlocks
        && status != BlocksMovingAnalysis.Status.BLOCKS_TARGETS_PAIRED) {
      status = BlocksMovingAnalysis.Status.FEW_LOW_REDUNDANCY_BLOCKS;
    }
    Map<Block, Set<StorageTypeNodePair>> assignedBlocks = new HashMap<>();
    for (BlockMovingInfo blkMovingInfo : blockMovingInfos) {
      // Check for at least one block storage movement has been chosen
      try {
        ctxt.submitMoveTask(blkMovingInfo);
        LOG.debug("BlockMovingInfo: {}", blkMovingInfo);
        StorageTypeNodePair nodeStorage = new StorageTypeNodePair(
            blkMovingInfo.getTargetStorageType(), blkMovingInfo.getTarget());
        Set<StorageTypeNodePair> nodesWithStorage = assignedBlocks
            .get(blkMovingInfo.getBlock());
        if (nodesWithStorage == null) {
          nodesWithStorage = new HashSet<>();
          assignedBlocks.put(blkMovingInfo.getBlock(), nodesWithStorage);
        }
        nodesWithStorage.add(nodeStorage);
        blockCount++;
      } catch (IOException e) {
        LOG.warn("Exception while scheduling movement task", e);
        // failed to move the block.
        status = BlocksMovingAnalysis.Status.BLOCKS_FAILED_TO_MOVE;
      }
    }
    return new BlocksMovingAnalysis(status, assignedBlocks);
  }

  /**
   * The given block is considered as low redundancy when the block locations
   * length is less than expected replication factor. For EC blocks, redundancy
   * is the summation of data + parity blocks.
   *
   * @param blockInfo
   *          block
   * @param replication
   *          replication factor of the given file block
   * @param ecPolicy
   *          erasure coding policy of the given file block
   * @return true if the given block is low redundant.
   */
  private boolean isLowRedundancyBlock(LocatedBlock blockInfo, int replication,
      ErasureCodingPolicy ecPolicy) {
    boolean hasLowRedundancyBlock = false;
    if (blockInfo.isStriped()) {
      // For EC blocks, redundancy is the summation of data + parity blocks.
      replication = ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits();
    }
    // block is considered as low redundancy when the block locations length is
    // less than expected replication factor.
    hasLowRedundancyBlock = blockInfo.getLocations().length < replication ? true
        : false;
    return hasLowRedundancyBlock;
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
   * @param liveDns
   *          - live datanodes which can be used as targets
   * @param ecPolicy
   *          - ec policy of sps invoked file
   * @return false if some of the block locations failed to find target node to
   *         satisfy the storage policy, true otherwise
   */
  private boolean computeBlockMovingInfos(
      List<BlockMovingInfo> blockMovingInfos, LocatedBlock blockInfo,
      List<StorageType> expectedStorageTypes, List<StorageType> existing,
      DatanodeInfo[] storages, DatanodeMap liveDns,
      ErasureCodingPolicy ecPolicy) {
    boolean foundMatchingTargetNodesForBlock = true;
    if (!removeOverlapBetweenStorageTypes(expectedStorageTypes,
        existing, true)) {
      List<StorageTypeNodePair> sourceWithStorageMap =
          new ArrayList<StorageTypeNodePair>();
      List<DatanodeInfo> existingBlockStorages = new ArrayList<DatanodeInfo>(
          Arrays.asList(storages));

      // Add existing storages into exclude nodes to avoid choosing this as
      // remote target later.
      List<DatanodeInfo> excludeNodes = new ArrayList<>(existingBlockStorages);

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

      EnumMap<StorageType, List<DatanodeWithStorage.StorageDetails>> targetDns =
          findTargetsForExpectedStorageTypes(expectedStorageTypes, liveDns);

      foundMatchingTargetNodesForBlock |= findSourceAndTargetToMove(
          blockMovingInfos, blockInfo, sourceWithStorageMap,
          expectedStorageTypes, targetDns,
          ecPolicy, excludeNodes);
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
   * @param expectedTypes
   *          - Expecting storages to move
   * @param targetDns
   *          - Available DNs for expected storage types
   * @param ecPolicy
   *          - erasure coding policy of sps invoked file
   * @param excludeNodes
   *          - existing source nodes, which has replica copy
   * @return false if some of the block locations failed to find target node to
   *         satisfy the storage policy
   */
  private boolean findSourceAndTargetToMove(
      List<BlockMovingInfo> blockMovingInfos, LocatedBlock blockInfo,
      List<StorageTypeNodePair> sourceWithStorageList,
      List<StorageType> expectedTypes,
      EnumMap<StorageType, List<DatanodeWithStorage.StorageDetails>> targetDns,
      ErasureCodingPolicy ecPolicy, List<DatanodeInfo> excludeNodes) {
    boolean foundMatchingTargetNodesForBlock = true;

    // Looping over all the source node locations and choose the target
    // storage within same node if possible. This is done separately to
    // avoid choosing a target which already has this block.
    for (int i = 0; i < sourceWithStorageList.size(); i++) {
      StorageTypeNodePair existingTypeNodePair = sourceWithStorageList.get(i);

      // Check whether the block replica is already placed in the expected
      // storage type in this source datanode.
      if (!expectedTypes.contains(existingTypeNodePair.storageType)) {
        StorageTypeNodePair chosenTarget = chooseTargetTypeInSameNode(blockInfo,
            existingTypeNodePair.dn, targetDns, expectedTypes);
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
          expectedTypes.remove(chosenTarget.storageType);
        }
      }
    }
    // If all the sources and targets are paired within same node, then simply
    // return.
    if (expectedTypes.size() <= 0) {
      return foundMatchingTargetNodesForBlock;
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
      if (chosenTarget == null && dnCacheMgr.getCluster().isNodeGroupAware()) {
        chosenTarget = chooseTarget(blockInfo, existingTypeNodePair.dn,
            expectedTypes, Matcher.SAME_NODE_GROUP, targetDns,
            excludeNodes);
      }

      // Then, match nodes on the same rack
      if (chosenTarget == null) {
        chosenTarget =
            chooseTarget(blockInfo, existingTypeNodePair.dn, expectedTypes,
                Matcher.SAME_RACK, targetDns, excludeNodes);
      }

      if (chosenTarget == null) {
        chosenTarget =
            chooseTarget(blockInfo, existingTypeNodePair.dn, expectedTypes,
                Matcher.ANY_OTHER, targetDns, excludeNodes);
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

        expectedTypes.remove(chosenTarget.storageType);
        excludeNodes.add(chosenTarget.dn);
      } else {
        LOG.warn(
            "Failed to choose target datanode for the required"
                + " storage types {}, block:{}, existing storage type:{}",
            expectedTypes, blockInfo, existingTypeNodePair.storageType);
      }
    }

    if (expectedTypes.size() > 0) {
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
   * @param blockInfo
   *          - block info
   * @param source
   *          - source datanode
   * @param targetDns
   *          - set of target datanodes with its respective storage type
   * @param targetTypes
   *          - list of target storage types
   */
  private StorageTypeNodePair chooseTargetTypeInSameNode(LocatedBlock blockInfo,
      DatanodeInfo source,
      EnumMap<StorageType, List<DatanodeWithStorage.StorageDetails>> targetDns,
      List<StorageType> targetTypes) {
    for (StorageType t : targetTypes) {
      List<DatanodeWithStorage.StorageDetails> targetNodeStorages =
          targetDns.get(t);
      if (targetNodeStorages == null) {
        continue;
      }
      for (DatanodeWithStorage.StorageDetails targetNode : targetNodeStorages) {
        if (targetNode.getDatanodeInfo().equals(source)) {
          // Good target with enough space to write the given block size.
          if (targetNode.hasSpaceForScheduling(blockInfo.getBlockSize())) {
            targetNode.incScheduledSize(blockInfo.getBlockSize());
            return new StorageTypeNodePair(t, source);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Datanode:{} storage type:{} doesn't have sufficient "
                + "space:{} to move the target block size:{}",
                source, t, targetNode, blockInfo.getBlockSize());
          }
        }
      }
    }
    return null;
  }

  private StorageTypeNodePair chooseTarget(LocatedBlock block,
      DatanodeInfo source, List<StorageType> targetTypes, Matcher matcher,
      EnumMap<StorageType, List<DatanodeWithStorage.StorageDetails>>
      locsForExpectedStorageTypes, List<DatanodeInfo> excludeNodes) {
    for (StorageType t : targetTypes) {
      List<DatanodeWithStorage.StorageDetails> nodesWithStorages =
          locsForExpectedStorageTypes.get(t);
      if (nodesWithStorages == null || nodesWithStorages.isEmpty()) {
        continue; // no target nodes with the required storage type.
      }
      Collections.shuffle(nodesWithStorages);
      for (DatanodeWithStorage.StorageDetails targetNode : nodesWithStorages) {
        DatanodeInfo target = targetNode.getDatanodeInfo();
        if (!excludeNodes.contains(target)
            && matcher.match(dnCacheMgr.getCluster(), source, target)) {
          // Good target with enough space to write the given block size.
          if (targetNode.hasSpaceForScheduling(block.getBlockSize())) {
            targetNode.incScheduledSize(block.getBlockSize());
            return new StorageTypeNodePair(t, target);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Datanode:{} storage type:{} doesn't have sufficient "
                + "space:{} to move the target block size:{}",
                target, t, targetNode, block.getBlockSize());
          }
        }
      }
    }
    return null;
  }

  /**
   * Keeps datanode with its respective storage type.
   */
  static final class StorageTypeNodePair {
    private final StorageType storageType;
    private final DatanodeInfo dn;

    StorageTypeNodePair(StorageType storageType, DatanodeInfo dn) {
      this.storageType = storageType;
      this.dn = dn;
    }

    public DatanodeInfo getDatanodeInfo() {
      return dn;
    }

    public StorageType getStorageType() {
      return storageType;
    }

    @Override
    public String toString() {
      return new StringBuilder().append("StorageTypeNodePair(\n  ")
          .append("DatanodeInfo: ").append(dn).append(", StorageType: ")
          .append(storageType).toString();
    }
  }

  private EnumMap<StorageType, List<DatanodeWithStorage.StorageDetails>>
      findTargetsForExpectedStorageTypes(List<StorageType> expected,
        DatanodeMap liveDns) {
    EnumMap<StorageType, List<DatanodeWithStorage.StorageDetails>> targetsMap =
        new EnumMap<StorageType, List<DatanodeWithStorage.StorageDetails>>(
        StorageType.class);

    for (StorageType storageType : expected) {
      List<DatanodeWithStorage> nodes = liveDns.getTarget(storageType);
      if (nodes == null) {
        return targetsMap;
      }
      List<DatanodeWithStorage.StorageDetails> listNodes = targetsMap
          .get(storageType);
      if (listNodes == null) {
        listNodes = new ArrayList<>();
        targetsMap.put(storageType, listNodes);
      }

      for (DatanodeWithStorage n : nodes) {
        final DatanodeWithStorage.StorageDetails node = getMaxRemaining(n,
            storageType);
        if (node != null) {
          listNodes.add(node);
        }
      }
    }
    return targetsMap;
  }

  private static DatanodeWithStorage.StorageDetails getMaxRemaining(
      DatanodeWithStorage node, StorageType storageType) {
    long max = 0L;
    DatanodeWithStorage.StorageDetails nodeInfo = null;
    List<DatanodeWithStorage.StorageDetails> storages = node
        .getNodesWithStorages(storageType);
    for (DatanodeWithStorage.StorageDetails n : storages) {
      if (n.availableSizeToMove() > max) {
        max = n.availableSizeToMove();
        nodeInfo = n;
      }
    }
    return nodeInfo;
  }

  private boolean checkSourceAndTargetTypeExists(DatanodeInfo dn,
      List<StorageType> existingStorageTypes,
      List<StorageType> expectedStorageTypes, DatanodeMap liveDns) {
    boolean isExpectedTypeAvailable = false;
    boolean isExistingTypeAvailable = false;
    for (DatanodeWithStorage liveDn : liveDns.getTargets()) {
      if (dn.equals(liveDn.datanode)) {
        for (StorageType eachType : liveDn.getStorageTypes()) {
          if (existingStorageTypes.contains(eachType)) {
            isExistingTypeAvailable = true;
          }
          if (expectedStorageTypes.contains(eachType)) {
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

  /**
   * Maintains storage type map with the available datanodes in the cluster.
   */
  public static class DatanodeMap {
    private final EnumMap<StorageType, List<DatanodeWithStorage>> targetsMap =
        new EnumMap<StorageType, List<DatanodeWithStorage>>(StorageType.class);

    private List<DatanodeWithStorage> targets = new ArrayList<>();

    /**
     * Build datanode map with the available storage types.
     *
     * @param node
     *          datanode
     * @param storageTypes
     *          list of available storage types in the given datanode
     * @param maxSize2Move
     *          available space which can be used for scheduling block move
     */
    void addTarget(DatanodeInfo node, List<StorageType> storageTypes,
        List<Long> maxSize2Move) {
      DatanodeWithStorage nodeStorage = new DatanodeWithStorage(node);
      targets.add(nodeStorage);
      for (int i = 0; i < storageTypes.size(); i++) {
        StorageType type = storageTypes.get(i);
        List<DatanodeWithStorage> nodeStorages = targetsMap.get(type);
        nodeStorage.addStorageType(type, maxSize2Move.get(i));
        if (nodeStorages == null) {
          nodeStorages = new LinkedList<>();
          targetsMap.put(type, nodeStorages);
        }
        nodeStorages.add(nodeStorage);
      }
    }

    List<DatanodeWithStorage> getTarget(StorageType storageType) {
      return targetsMap.get(storageType);
    }

    public List<DatanodeWithStorage> getTargets() {
      return targets;
    }

    void reset() {
      targetsMap.clear();
    }
  }

  /**
   * Keeps datanode with its respective set of supported storage types. It holds
   * the available space in each volumes and will be used while pairing the
   * target datanodes.
   */
  public static final class DatanodeWithStorage {
    private final EnumMap<StorageType, List<StorageDetails>> storageMap =
        new EnumMap<StorageType, List<StorageDetails>>(StorageType.class);
    private final DatanodeInfo datanode;

    private DatanodeWithStorage(DatanodeInfo datanode) {
      this.datanode = datanode;
    }

    public DatanodeInfo getDatanodeInfo() {
      return datanode;
    }

    Set<StorageType> getStorageTypes() {
      return storageMap.keySet();
    }

    private void addStorageType(StorageType t, long maxSize2Move) {
      List<StorageDetails> nodesWithStorages = getNodesWithStorages(t);
      if (nodesWithStorages == null) {
        nodesWithStorages = new LinkedList<StorageDetails>();
        storageMap.put(t, nodesWithStorages);
      }
      nodesWithStorages.add(new StorageDetails(maxSize2Move));
    }

    /**
     * Returns datanode storages which has the given storage type.
     *
     * @param type
     *          - storage type
     * @return datanodes for the given storage type
     */
    private List<StorageDetails> getNodesWithStorages(StorageType type) {
      return storageMap.get(type);
    }

    @Override
    public String toString() {
      return new StringBuilder().append("DatanodeWithStorageInfo(\n  ")
          .append("Datanode: ").append(datanode).append(" StorageTypeNodeMap: ")
          .append(storageMap).append(")").toString();
    }

    /** Storage details in a datanode storage type. */
    final class StorageDetails {
      private final long maxSize2Move;
      private long scheduledSize = 0L;

      private StorageDetails(long maxSize2Move) {
        this.maxSize2Move = maxSize2Move;
      }

      private DatanodeInfo getDatanodeInfo() {
        return DatanodeWithStorage.this.datanode;
      }

      /**
       * Checks whether this datanode storage has sufficient space to occupy the
       * given block size.
       */
      private synchronized boolean hasSpaceForScheduling(long size) {
        return availableSizeToMove() > size;
      }

      /**
       * @return the total number of bytes that need to be moved.
       */
      private synchronized long availableSizeToMove() {
        return maxSize2Move - scheduledSize;
      }

      /** Increment scheduled size. */
      private synchronized void incScheduledSize(long size) {
        scheduledSize += size;
      }

      @Override
      public String toString() {
        return new StringBuilder().append("StorageDetails(\n  ")
            .append("maxSize2Move: ").append(maxSize2Move)
            .append(" scheduledSize: ").append(scheduledSize).append(")")
            .toString();
      }
    }
  }

  /**
   * Receives storage movement attempt finished block report.
   *
   * @param dnInfo
   *          reported datanode
   * @param storageType
   *          - storage type
   * @param block
   *          movement attempt finished block.
   */
  @Override
  public void notifyStorageMovementAttemptFinishedBlk(DatanodeInfo dnInfo,
      StorageType storageType, Block block) {
    storageMovementsMonitor.notifyReportedBlock(dnInfo, storageType, block);
  }

  @VisibleForTesting
  public BlockStorageMovementAttemptedItems getAttemptedItemsMonitor() {
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
   * This class contains information of an attempted blocks and its last
   * attempted or reported time stamp. This is used by
   * {@link BlockStorageMovementAttemptedItems#storageMovementAttemptedItems}.
   */
  final static class AttemptedItemInfo extends ItemInfo {
    private long lastAttemptedOrReportedTime;
    private final Set<Block> blocks;

    /**
     * AttemptedItemInfo constructor.
     *
     * @param rootId
     *          rootId for trackId
     * @param trackId
     *          trackId for file.
     * @param lastAttemptedOrReportedTime
     *          last attempted or reported time
     * @param blocks
     *          scheduled blocks
     * @param retryCount
     *          file retry count
     */
    AttemptedItemInfo(long rootId, long trackId,
        long lastAttemptedOrReportedTime,
        Set<Block> blocks, int retryCount) {
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

    Set<Block> getBlocks() {
      return this.blocks;
    }
  }

  @Override
  public void addFileToProcess(ItemInfo trackInfo, boolean scanCompleted) {
    storageMovementNeeded.add(trackInfo, scanCompleted);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added track info for inode {} to block "
          + "storageMovementNeeded queue", trackInfo.getFile());
    }
  }

  @Override
  public void addAllFilesToProcess(long startPath, List<ItemInfo> itemInfoList,
      boolean scanCompleted) {
    getStorageMovementQueue().addAll(startPath, itemInfoList, scanCompleted);
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

  @Override
  public void markScanCompletedForPath(long inodeId) {
    getStorageMovementQueue().markScanCompletedForDir(inodeId);
  }

  /**
   * Join main SPS thread.
   */
  public void join() throws InterruptedException {
    storagePolicySatisfierThread.join();
  }

  /**
   * Remove the overlap between the expected types and the existing types.
   *
   * @param expected
   *          - Expected storage types list.
   * @param existing
   *          - Existing storage types list.
   * @param ignoreNonMovable
   *          ignore non-movable storage types by removing them from both
   *          expected and existing storage type list to prevent non-movable
   *          storage from being moved.
   * @returns if the existing types or the expected types is empty after
   *          removing the overlap.
   */
  private static boolean removeOverlapBetweenStorageTypes(
      List<StorageType> expected,
      List<StorageType> existing, boolean ignoreNonMovable) {
    for (Iterator<StorageType> i = existing.iterator(); i.hasNext();) {
      final StorageType t = i.next();
      if (expected.remove(t)) {
        i.remove();
      }
    }
    if (ignoreNonMovable) {
      removeNonMovable(existing);
      removeNonMovable(expected);
    }
    return expected.isEmpty() || existing.isEmpty();
  }

  private static void removeNonMovable(List<StorageType> types) {
    for (Iterator<StorageType> i = types.iterator(); i.hasNext();) {
      final StorageType t = i.next();
      if (!t.isMovable()) {
        i.remove();
      }
    }
  }

  /**
   * Get DFS_SPS_WORK_MULTIPLIER_PER_ITERATION from
   * configuration.
   *
   * @param conf Configuration
   * @return Value of DFS_SPS_WORK_MULTIPLIER_PER_ITERATION
   */
  private static int getSPSWorkMultiplier(Configuration conf) {
    int spsWorkMultiplier = conf
        .getInt(
            DFSConfigKeys.DFS_SPS_WORK_MULTIPLIER_PER_ITERATION,
            DFSConfigKeys.DFS_SPS_WORK_MULTIPLIER_PER_ITERATION_DEFAULT);
    Preconditions.checkArgument(
        (spsWorkMultiplier > 0),
        DFSConfigKeys.DFS_SPS_WORK_MULTIPLIER_PER_ITERATION +
        " = '" + spsWorkMultiplier + "' is invalid. " +
        "It should be a positive, non-zero integer value.");
    return spsWorkMultiplier;
  }
}
