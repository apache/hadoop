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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState.COMPLETE;

/**
 * Represents the under construction feature of a Block.
 * This is usually the last block of a file opened for write or append.
 */
public class BlockUnderConstructionFeature {
  private BlockUCState blockUCState;
  private static final ReplicaUnderConstruction[] NO_REPLICAS =
      new ReplicaUnderConstruction[0];

  /**
   * Block replicas as assigned when the block was allocated.
   */
  private ReplicaUnderConstruction[] replicas = NO_REPLICAS;

  /**
   * Index of the primary data node doing the recovery. Useful for log
   * messages.
   */
  private int primaryNodeIndex = -1;

  /**
   * The new generation stamp, which this block will have
   * after the recovery succeeds. Also used as a recovery id to identify
   * the right recovery if any of the abandoned recoveries re-appear.
   */
  private long blockRecoveryId = 0;

  /**
   * The block source to use in the event of copy-on-write truncate.
   */
  private BlockInfo truncateBlock;

  public BlockUnderConstructionFeature(Block blk,
      BlockUCState state, DatanodeStorageInfo[] targets, BlockType blockType) {
    assert getBlockUCState() != COMPLETE :
        "BlockUnderConstructionFeature cannot be in COMPLETE state";
    this.blockUCState = state;
    setExpectedLocations(blk, targets, blockType);
  }

  /** Set expected locations */
  public void setExpectedLocations(Block block, DatanodeStorageInfo[] targets,
      BlockType blockType) {
    if (targets == null) {
      return;
    }
    int numLocations = 0;
    for (DatanodeStorageInfo target : targets) {
      if (target != null) {
        numLocations++;
      }
    }

    this.replicas = new ReplicaUnderConstruction[numLocations];
    int offset = 0;
    for(int i = 0; i < targets.length; i++) {
      if (targets[i] != null) {
        // when creating a new striped block we simply sequentially assign block
        // index to each storage
        Block replicaBlock = blockType == BlockType.STRIPED ?
            new Block(block.getBlockId() + i, 0, block.getGenerationStamp()) :
            block;
        replicas[offset++] = new ReplicaUnderConstruction(replicaBlock,
            targets[i], ReplicaState.RBW);
      }
    }
  }

  /**
   * Create array of expected replica locations
   * (as has been assigned by chooseTargets()).
   */
  public DatanodeStorageInfo[] getExpectedStorageLocations() {
    int numLocations = getNumExpectedLocations();
    DatanodeStorageInfo[] storages = new DatanodeStorageInfo[numLocations];
    for (int i = 0; i < numLocations; i++) {
      storages[i] = replicas[i].getExpectedStorageLocation();
    }
    return storages;
  }

  /**
   * Note that this iterator doesn't guarantee thread-safe. It depends on
   * external mechanisms such as the FSNamesystem lock for protection.
   */
  public Iterator<DatanodeStorageInfo> getExpectedStorageLocationsIterator() {
    return new Iterator<DatanodeStorageInfo>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index <  replicas.length;
      }

      @Override
      public DatanodeStorageInfo next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return replicas[index++].getExpectedStorageLocation();
      }
    };
  }

  /**
   * @return the index array indicating the block index in each storage. Used
   * only by striped blocks.
   */
  public byte[] getBlockIndices() {
    int numLocations = getNumExpectedLocations();
    byte[] indices = new byte[numLocations];
    for (int i = 0; i < numLocations; i++) {
      indices[i] = BlockIdManager.getBlockIndex(replicas[i]);
    }
    return indices;
  }

  public byte[] getBlockIndicesForSpecifiedStorages(List<Integer> storageIdx) {
    byte[] indices = new byte[storageIdx.size()];
    for (int i = 0; i < indices.length; i++) {
      indices[i] = BlockIdManager.getBlockIndex(replicas[storageIdx.get(i)]);
    }
    return indices;
  }

  public int getNumExpectedLocations() {
    return replicas.length;
  }

  /**
   * when committing a striped block whose size is less than a stripe, we need
   * to decrease the scheduled block size of the DataNodes that do not store
   * any internal block.
   */
  void updateStorageScheduledSize(BlockInfoStriped storedBlock) {
    assert storedBlock.getUnderConstructionFeature() == this;
    if (replicas.length == 0) {
      return;
    }
    final int dataBlockNum = storedBlock.getDataBlockNum();
    final int realDataBlockNum = storedBlock.getRealDataBlockNum();
    if (realDataBlockNum < dataBlockNum) {
      for (ReplicaUnderConstruction replica : replicas) {
        int index = BlockIdManager.getBlockIndex(replica);
        if (index >= realDataBlockNum && index < dataBlockNum) {
          final DatanodeStorageInfo storage =
              replica.getExpectedStorageLocation();
          storage.getDatanodeDescriptor()
              .decrementBlocksScheduled(storage.getStorageType());
        }
      }
    }
  }

  /**
   * Return the state of the block under construction.
   * @see BlockUCState
   */
  public BlockUCState getBlockUCState() {
    return blockUCState;
  }

  void setBlockUCState(BlockUCState s) {
    blockUCState = s;
  }

  public long getBlockRecoveryId() {
    return blockRecoveryId;
  }

  /** Get recover block */
  public BlockInfo getTruncateBlock() {
    return truncateBlock;
  }

  public void setTruncateBlock(BlockInfo recoveryBlock) {
    this.truncateBlock = recoveryBlock;
  }

  /**
   * Set {@link #blockUCState} to {@link BlockUCState#COMMITTED}.
   */
  void commit() {
    blockUCState = BlockUCState.COMMITTED;
  }

  List<ReplicaUnderConstruction> getStaleReplicas(long genStamp) {
    List<ReplicaUnderConstruction> staleReplicas = new ArrayList<>();
    // Remove replicas with wrong gen stamp. The replica list is unchanged.
    for (ReplicaUnderConstruction r : replicas) {
      if (genStamp != r.getGenerationStamp()) {
        staleReplicas.add(r);
      }
    }
    return staleReplicas;
  }

  /**
   * Initialize lease recovery for this block.
   * Find the first alive data-node starting from the previous primary and
   * make it primary.
   * @param blockInfo Block to be recovered
   * @param recoveryId Recovery ID (new gen stamp)
   * @param startRecovery Issue recovery command to datanode if true.
   */
  public void initializeBlockRecovery(BlockInfo blockInfo, long recoveryId,
      boolean startRecovery) {
    setBlockUCState(BlockUCState.UNDER_RECOVERY);
    blockRecoveryId = recoveryId;
    if (!startRecovery) {
      return;
    }
    if (replicas.length == 0) {
      NameNode.blockStateChangeLog.warn("BLOCK*" +
          " BlockUnderConstructionFeature.initializeBlockRecovery:" +
          " No blocks found, lease removed.");
      // sets primary node index and return.
      primaryNodeIndex = -1;
      return;
    }
    boolean allLiveReplicasTriedAsPrimary = true;
    for (ReplicaUnderConstruction replica : replicas) {
      // Check if all replicas have been tried or not.
      if (replica.isAlive()) {
        allLiveReplicasTriedAsPrimary = allLiveReplicasTriedAsPrimary
            && replica.getChosenAsPrimary();
      }
    }
    if (allLiveReplicasTriedAsPrimary) {
      // Just set all the replicas to be chosen whether they are alive or not.
      for (ReplicaUnderConstruction replica : replicas) {
        replica.setChosenAsPrimary(false);
      }
    }
    long mostRecentLastUpdate = 0;
    ReplicaUnderConstruction primary = null;
    primaryNodeIndex = -1;
    for (int i = 0; i < replicas.length; i++) {
      // Skip alive replicas which have been chosen for recovery.
      if (!(replicas[i].isAlive() && !replicas[i].getChosenAsPrimary())) {
        continue;
      }
      final ReplicaUnderConstruction ruc = replicas[i];
      final long lastUpdate = ruc.getExpectedStorageLocation()
          .getDatanodeDescriptor().getLastUpdateMonotonic();
      if (lastUpdate > mostRecentLastUpdate) {
        primaryNodeIndex = i;
        primary = ruc;
        mostRecentLastUpdate = lastUpdate;
      }
    }
    if (primary != null) {
      primary.getExpectedStorageLocation().getDatanodeDescriptor()
          .addBlockToBeRecovered(blockInfo);
      primary.setChosenAsPrimary(true);
      NameNode.blockStateChangeLog.debug(
          "BLOCK* {} recovery started, primary={}", this, primary);
    }
  }

  /** Add the reported replica if it is not already in the replica list. */
  void addReplicaIfNotPresent(DatanodeStorageInfo storage,
      Block reportedBlock, ReplicaState rState) {
    if (replicas.length == 0) {
      replicas = new ReplicaUnderConstruction[1];
      replicas[0] = new ReplicaUnderConstruction(reportedBlock, storage,
          rState);
    } else {
      for (int i = 0; i < replicas.length; i++) {
        DatanodeStorageInfo expected =
            replicas[i].getExpectedStorageLocation();
        if (expected == storage) {
          replicas[i].setGenerationStamp(reportedBlock.getGenerationStamp());
          return;
        } else if (expected != null && expected.getDatanodeDescriptor() ==
            storage.getDatanodeDescriptor()) {
          // The Datanode reported that the block is on a different storage
          // than the one chosen by BlockPlacementPolicy. This can occur as
          // we allow Datanodes to choose the target storage. Update our
          // state by removing the stale entry and adding a new one.
          replicas[i] = new ReplicaUnderConstruction(reportedBlock, storage,
              rState);
          return;
        }
      }
      ReplicaUnderConstruction[] newReplicas =
          new ReplicaUnderConstruction[replicas.length + 1];
      System.arraycopy(replicas, 0, newReplicas, 0, replicas.length);
      newReplicas[newReplicas.length - 1] = new ReplicaUnderConstruction(
          reportedBlock, storage, rState);
      replicas = newReplicas;
    }
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(100);
    appendUCParts(b);
    return b.toString();
  }

  private void appendUCParts(StringBuilder sb) {
    sb.append("{UCState=").append(blockUCState)
      .append(", truncateBlock=").append(truncateBlock)
      .append(", primaryNodeIndex=").append(primaryNodeIndex)
      .append(", replicas=[");
    int i = 0;
    for (ReplicaUnderConstruction r : replicas) {
      r.appendStringTo(sb);
      if (++i < replicas.length) {
        sb.append(", ");
      }
    }
    sb.append("]}");
  }
  
  public void appendUCPartsConcise(StringBuilder sb) {
    sb.append("replicas=");
    int i = 0;
    for (ReplicaUnderConstruction r : replicas) {
      sb.append(r.getExpectedStorageLocation().getDatanodeDescriptor());
      if (++i < replicas.length) {
        sb.append(", ");
      }
    }
  }
}
