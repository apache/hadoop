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
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;

import java.io.IOException;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState.COMPLETE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION;

/**
 * Represents a striped block that is currently being constructed.
 * This is usually the last block of a file opened for write or append.
 */
public class BlockInfoUnderConstructionStriped extends BlockInfoStriped
    implements BlockInfoUnderConstruction{
  private BlockUCState blockUCState;

  /**
   * Block replicas as assigned when the block was allocated.
   */
  private ReplicaUnderConstruction[] replicas;

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
   * Constructor with null storage targets.
   */
  public BlockInfoUnderConstructionStriped(Block blk, ErasureCodingPolicy ecPolicy) {
    this(blk, ecPolicy, UNDER_CONSTRUCTION, null);
  }

  /**
   * Create a striped block that is currently being constructed.
   */
  public BlockInfoUnderConstructionStriped(Block blk, ErasureCodingPolicy ecPolicy,
      BlockUCState state, DatanodeStorageInfo[] targets) {
    super(blk, ecPolicy);
    assert getBlockUCState() != COMPLETE :
      "BlockInfoUnderConstructionStriped cannot be in COMPLETE state";
    this.blockUCState = state;
    setExpectedLocations(targets);
  }

  @Override
  public BlockInfoStriped convertToCompleteBlock() throws IOException {
    assert getBlockUCState() != COMPLETE :
      "Trying to convert a COMPLETE block";
    return new BlockInfoStriped(this);
  }

  /** Set expected locations */
  @Override
  public void setExpectedLocations(DatanodeStorageInfo[] targets) {
    int numLocations = targets == null ? 0 : targets.length;
    this.replicas = new ReplicaUnderConstruction[numLocations];
    for(int i = 0; i < numLocations; i++) {
      // when creating a new block we simply sequentially assign block index to
      // each storage
      Block blk = new Block(this.getBlockId() + i, 0, this.getGenerationStamp());
      replicas[i] = new ReplicaUnderConstruction(blk, targets[i],
          ReplicaState.RBW);
    }
  }

  /**
   * Create array of expected replica locations
   * (as has been assigned by chooseTargets()).
   */
  @Override
  public DatanodeStorageInfo[] getExpectedStorageLocations() {
    int numLocations = getNumExpectedLocations();
    DatanodeStorageInfo[] storages = new DatanodeStorageInfo[numLocations];
    for (int i = 0; i < numLocations; i++) {
      storages[i] = replicas[i].getExpectedStorageLocation();
    }
    return storages;
  }

  /** @return the index array indicating the block index in each storage */
  public int[] getBlockIndices() {
    int numLocations = getNumExpectedLocations();
    int[] indices = new int[numLocations];
    for (int i = 0; i < numLocations; i++) {
      indices[i] = BlockIdManager.getBlockIndex(replicas[i]);
    }
    return indices;
  }

  @Override
  public int getNumExpectedLocations() {
    return replicas == null ? 0 : replicas.length;
  }

  /**
   * Return the state of the block under construction.
   * @see BlockUCState
   */
  @Override // BlockInfo
  public BlockUCState getBlockUCState() {
    return blockUCState;
  }

  void setBlockUCState(BlockUCState s) {
    blockUCState = s;
  }

  @Override
  public long getBlockRecoveryId() {
    return blockRecoveryId;
  }

  @Override
  public Block getTruncateBlock() {
    return null;
  }

  @Override
  public Block toBlock(){
    return this;
  }

  @Override
  public void setGenerationStampAndVerifyReplicas(long genStamp) {
    // Set the generation stamp for the block.
    setGenerationStamp(genStamp);
    if (replicas == null)
      return;

    // Remove the replicas with wrong gen stamp.
    // The replica list is unchanged.
    for (ReplicaUnderConstruction r : replicas) {
      if (genStamp != r.getGenerationStamp()) {
        r.getExpectedStorageLocation().removeBlock(this);
        NameNode.blockStateChangeLog.info("BLOCK* Removing stale replica "
            + "from location: {}", r.getExpectedStorageLocation());
      }
    }
  }

  @Override
  public void commitBlock(Block block) throws IOException {
    if (getBlockId() != block.getBlockId()) {
      throw new IOException("Trying to commit inconsistent block: id = "
          + block.getBlockId() + ", expected id = " + getBlockId());
    }
    blockUCState = BlockUCState.COMMITTED;
    this.set(getBlockId(), block.getNumBytes(), block.getGenerationStamp());
    // Sort out invalid replicas.
    setGenerationStampAndVerifyReplicas(block.getGenerationStamp());
  }

  @Override
  public void initializeBlockRecovery(long recoveryId) {
    setBlockUCState(BlockUCState.UNDER_RECOVERY);
    blockRecoveryId = recoveryId;
    if (replicas == null || replicas.length == 0) {
      NameNode.blockStateChangeLog.warn("BLOCK*" +
          " BlockInfoUnderConstructionStriped.initLeaseRecovery:" +
          " No blocks found, lease removed.");
      // sets primary node index and return.
      primaryNodeIndex = -1;
      return;
    }
    boolean allLiveReplicasTriedAsPrimary = true;
    for (ReplicaUnderConstruction replica : replicas) {
      // Check if all replicas have been tried or not.
      if (replica.isAlive()) {
        allLiveReplicasTriedAsPrimary = (allLiveReplicasTriedAsPrimary &&
            replica.getChosenAsPrimary());
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
    for(int i = 0; i < replicas.length; i++) {
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
          .addBlockToBeRecovered(this);
      primary.setChosenAsPrimary(true);
      NameNode.blockStateChangeLog.info(
          "BLOCK* {} recovery started, primary={}", this, primary);
    }
  }

  @Override
  public void addReplicaIfNotPresent(DatanodeStorageInfo storage,
      Block reportedBlock, ReplicaState rState) {
    if (replicas == null) {
      replicas = new ReplicaUnderConstruction[1];
      replicas[0] = new ReplicaUnderConstruction(reportedBlock, storage, rState);
    } else {
      for (int i = 0; i < replicas.length; i++) {
        DatanodeStorageInfo expected = replicas[i].getExpectedStorageLocation();
        if (expected == storage) {
          replicas[i].setBlockId(reportedBlock.getBlockId());
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
    appendStringTo(b);
    return b.toString();
  }

  @Override
  public void appendStringTo(StringBuilder sb) {
    super.appendStringTo(sb);
    appendUCParts(sb);
  }

  private void appendUCParts(StringBuilder sb) {
    sb.append("{UCState=").append(blockUCState).
        append(", primaryNodeIndex=").append(primaryNodeIndex).
        append(", replicas=[");
    if (replicas != null) {
      int i = 0;
      for (ReplicaUnderConstruction r : replicas) {
        r.appendStringTo(sb);
        if (++i < replicas.length) {
          sb.append(", ");
        }
      }
    }
    sb.append("]}");
  }
}
