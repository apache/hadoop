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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.hdfs.server.protocol.BlocksStorageMovementResult;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
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

  public StoragePolicySatisfier(final Namesystem namesystem,
      final BlockStorageMovementNeeded storageMovementNeeded,
      final BlockManager blkManager) {
    this.namesystem = namesystem;
    this.storageMovementNeeded = storageMovementNeeded;
    this.blockManager = blkManager;
    // TODO: below selfRetryTimeout and checkTimeout can be configurable later
    // Now, the default values of selfRetryTimeout and checkTimeout are 30mins
    // and 5mins respectively
    this.storageMovementsMonitor = new BlockStorageMovementAttemptedItems(
        5 * 60 * 1000, 30 * 60 * 1000, storageMovementNeeded);
  }

  /**
   * Start storage policy satisfier demon thread. Also start block storage
   * movements monitor for retry the attempts if needed.
   */
  public void start() {
    storagePolicySatisfierThread = new Daemon(this);
    storagePolicySatisfierThread.setName("StoragePolicySatisfier");
    storagePolicySatisfierThread.start();
    this.storageMovementsMonitor.start();
  }

  /**
   * Stop storage policy satisfier demon thread.
   */
  public void stop() {
    if (storagePolicySatisfierThread == null) {
      return;
    }
    storagePolicySatisfierThread.interrupt();
    try {
      storagePolicySatisfierThread.join(3000);
    } catch (InterruptedException ie) {
    }
    this.storageMovementsMonitor.stop();
  }

  @Override
  public void run() {
    while (namesystem.isRunning()) {
      try {
        Long blockCollectionID = storageMovementNeeded.get();
        if (blockCollectionID != null) {
          computeAndAssignStorageMismatchedBlocksToDNs(blockCollectionID);
          this.storageMovementsMonitor.add(blockCollectionID);
        }
        // TODO: We can think to make this as configurable later, how frequently
        // we want to check block movements.
        Thread.sleep(3000);
      } catch (Throwable t) {
        if (!namesystem.isRunning()) {
          LOG.info("Stopping StoragePolicySatisfier.");
          if (!(t instanceof InterruptedException)) {
            LOG.info("StoragePolicySatisfier received an exception"
                + " while shutting down.", t);
          }
          break;
        }
        LOG.error("StoragePolicySatisfier thread received runtime exception. "
            + "Stopping Storage policy satisfier work", t);
        // TODO: Just break for now. Once we implement dynamic start/stop
        // option, we can add conditions here when to break/terminate.
        break;
      }
    }
  }

  private void computeAndAssignStorageMismatchedBlocksToDNs(
      long blockCollectionID) {
    BlockCollection blockCollection =
        namesystem.getBlockCollection(blockCollectionID);
    if (blockCollection == null) {
      return;
    }
    byte existingStoragePolicyID = blockCollection.getStoragePolicyID();
    BlockStoragePolicy existingStoragePolicy =
        blockManager.getStoragePolicy(existingStoragePolicyID);
    if (!blockCollection.getLastBlock().isComplete()) {
      // Postpone, currently file is under construction
      // So, should we add back? or leave it to user
      return;
    }

    // First datanode will be chosen as the co-ordinator node for storage
    // movements. Later this can be optimized if needed.
    DatanodeDescriptor coordinatorNode = null;
    BlockInfo[] blocks = blockCollection.getBlocks();
    List<BlockMovingInfo> blockMovingInfos = new ArrayList<BlockMovingInfo>();
    for (int i = 0; i < blocks.length; i++) {
      BlockInfo blockInfo = blocks[i];
      List<StorageType> expectedStorageTypes =
          existingStoragePolicy.chooseStorageTypes(blockInfo.getReplication());
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
        List<StorageTypeNodePair> sourceWithStorageMap =
            new ArrayList<StorageTypeNodePair>();
        List<DatanodeStorageInfo> existingBlockStorages =
            new ArrayList<DatanodeStorageInfo>(Arrays.asList(storages));
        for (StorageType existingType : existing) {
          Iterator<DatanodeStorageInfo> iterator =
              existingBlockStorages.iterator();
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

        BlockMovingInfo blockMovingInfo =
            findSourceAndTargetToMove(blockInfo, existing, sourceWithStorageMap,
                expectedStorageTypes, locsForExpectedStorageTypes);
        if (coordinatorNode == null) {
          // For now, first datanode will be chosen as the co-ordinator. Later
          // this can be optimized if needed.
          coordinatorNode =
              (DatanodeDescriptor) blockMovingInfo.getSources()[0];
        }
        blockMovingInfos.add(blockMovingInfo);
      }
    }

    addBlockMovingInfosToCoordinatorDn(blockCollectionID, blockMovingInfos,
        coordinatorNode);
  }

  private void addBlockMovingInfosToCoordinatorDn(long blockCollectionID,
      List<BlockMovingInfo> blockMovingInfos,
      DatanodeDescriptor coordinatorNode) {

    if (blockMovingInfos.size() < 1) {
      // TODO: Major: handle this case. I think we need retry cases to
      // be implemented. Idea is, if some files are not getting storage movement
      // chances, then we can just retry limited number of times and exit.
      return;
    }

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
   * @param blockInfo
   *          - Block
   * @param existing
   *          - Existing storage types of block
   * @param sourceWithStorageList
   *          - Source Datanode with storages list
   * @param expected
   *          - Expecting storages to move
   * @param locsForExpectedStorageTypes
   *          - Available DNs for expected storage types
   * @return list of block source and target node pair
   */
  private BlockMovingInfo findSourceAndTargetToMove(BlockInfo blockInfo,
      List<StorageType> existing,
      List<StorageTypeNodePair> sourceWithStorageList,
      List<StorageType> expected,
      StorageTypeNodeMap locsForExpectedStorageTypes) {
    List<DatanodeInfo> sourceNodes = new ArrayList<>();
    List<StorageType> sourceStorageTypes = new ArrayList<>();
    List<DatanodeInfo> targetNodes = new ArrayList<>();
    List<StorageType> targetStorageTypes = new ArrayList<>();
    List<DatanodeDescriptor> chosenNodes = new ArrayList<>();
    for (int i = 0; i < sourceWithStorageList.size(); i++) {
      StorageTypeNodePair existingTypeNodePair = sourceWithStorageList.get(i);
      StorageTypeNodePair chosenTarget = chooseTargetTypeInSameNode(
          existingTypeNodePair.dn, expected);

      if (chosenTarget == null && blockManager.getDatanodeManager()
          .getNetworkTopology().isNodeGroupAware()) {
        chosenTarget = chooseTarget(blockInfo, existingTypeNodePair.dn,
            expected, Matcher.SAME_NODE_GROUP, locsForExpectedStorageTypes,
            chosenNodes);
      }

      // Then, match nodes on the same rack
      if (chosenTarget == null) {
        chosenTarget =
            chooseTarget(blockInfo, existingTypeNodePair.dn, expected,
                Matcher.SAME_RACK, locsForExpectedStorageTypes, chosenNodes);
      }

      if (chosenTarget == null) {
        chosenTarget =
            chooseTarget(blockInfo, existingTypeNodePair.dn, expected,
                Matcher.ANY_OTHER, locsForExpectedStorageTypes, chosenNodes);
      }
      if (null != chosenTarget) {
        sourceNodes.add(existingTypeNodePair.dn);
        sourceStorageTypes.add(existingTypeNodePair.storageType);
        targetNodes.add(chosenTarget.dn);
        targetStorageTypes.add(chosenTarget.storageType);
        chosenNodes.add(chosenTarget.dn);
        // TODO: We can increment scheduled block count for this node?
      } else {
        LOG.warn(
            "Failed to choose target datanode for the required"
                + " storage types {}, block:{}, existing storage type:{}",
            expected, blockInfo, existingTypeNodePair.storageType);
        sourceNodes.add(existingTypeNodePair.dn);
        sourceStorageTypes.add(existingTypeNodePair.storageType);
        // Imp: Not setting the target details, empty targets. Later, this is
        // used as an indicator for retrying this block movement.
      }
    }
    BlockMovingInfo blkMovingInfo = new BlockMovingInfo(blockInfo,
        sourceNodes.toArray(new DatanodeInfo[sourceNodes.size()]),
        targetNodes.toArray(new DatanodeInfo[targetNodes.size()]),
        sourceStorageTypes.toArray(new StorageType[sourceStorageTypes.size()]),
        targetStorageTypes.toArray(new StorageType[targetStorageTypes.size()]));
    return blkMovingInfo;
  }

  /**
   * Choose the target storage within same datanode if possible.
   *
   * @param source source datanode
   * @param targetTypes list of target storage types
   */
  private StorageTypeNodePair chooseTargetTypeInSameNode(
      DatanodeDescriptor source, List<StorageType> targetTypes) {
    for (StorageType t : targetTypes) {
      DatanodeStorageInfo chooseStorage4Block =
          source.chooseStorage4Block(t, 0);
      if (chooseStorage4Block != null) {
        return new StorageTypeNodePair(t, source);
      }
    }
    return null;
  }

  private StorageTypeNodePair chooseTarget(Block block,
      DatanodeDescriptor source, List<StorageType> targetTypes, Matcher matcher,
      StorageTypeNodeMap locsForExpectedStorageTypes,
      List<DatanodeDescriptor> chosenNodes) {
    for (StorageType t : targetTypes) {
      List<DatanodeDescriptor> nodesWithStorages =
          locsForExpectedStorageTypes.getNodesWithStorages(t);
      if (nodesWithStorages == null || nodesWithStorages.isEmpty()) {
        continue; // no target nodes with the required storage type.
      }
      Collections.shuffle(nodesWithStorages);
      for (DatanodeDescriptor target : nodesWithStorages) {
        if (!chosenNodes.contains(target) && matcher.match(
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
    public StorageType storageType = null;
    public DatanodeDescriptor dn = null;

    public StorageTypeNodePair(StorageType storageType, DatanodeDescriptor dn) {
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
}
