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

import java.io.IOException;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState.COMPLETE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION;

/**
 * Represents a striped block that is currently being constructed.
 * This is usually the last block of a file opened for write or append.
 */
public class BlockInfoStripedUnderConstruction extends BlockInfoStriped {
  private BlockUCState blockUCState;

  /**
   * Block replicas as assigned when the block was allocated.
   */
  private ReplicaUnderConstruction[] replicas;

  /**
   * The new generation stamp, which this block will have
   * after the recovery succeeds. Also used as a recovery id to identify
   * the right recovery if any of the abandoned recoveries re-appear.
   */
  private long blockRecoveryId = 0;

  /**
   * Constructor with null storage targets.
   */
  public BlockInfoStripedUnderConstruction(Block blk, short dataBlockNum,
      short parityBlockNum) {
    this(blk, dataBlockNum, parityBlockNum, UNDER_CONSTRUCTION, null);
  }

  /**
   * Create a striped block that is currently being constructed.
   */
  public BlockInfoStripedUnderConstruction(Block blk, short dataBlockNum,
      short parityBlockNum, BlockUCState state, DatanodeStorageInfo[] targets) {
    super(blk, dataBlockNum, parityBlockNum);
    assert getBlockUCState() != COMPLETE :
      "BlockInfoStripedUnderConstruction cannot be in COMPLETE state";
    this.blockUCState = state;
    setExpectedLocations(targets);
  }

  /**
   * Convert an under construction striped block to a complete striped block.
   *
   * @return BlockInfoStriped - a complete block.
   * @throws IOException if the state of the block
   * (the generation stamp and the length) has not been committed by
   * the client or it does not have at least a minimal number of replicas
   * reported from data-nodes.
   */
  BlockInfoStriped convertToCompleteBlock() throws IOException {
    assert getBlockUCState() != COMPLETE :
      "Trying to convert a COMPLETE block";
    return new BlockInfoStriped(this);
  }

  /** Set expected locations */
  public void setExpectedLocations(DatanodeStorageInfo[] targets) {
    int numLocations = targets == null ? 0 : targets.length;
    this.replicas = new ReplicaUnderConstruction[numLocations];
    for(int i = 0; i < numLocations; i++) {
      // when creating a new block we simply sequentially assign block index to
      // each storage
      Block blk = new Block(this.getBlockId() + i, this.getGenerationStamp(), 0);
      replicas[i] = new ReplicaUnderConstruction(blk, targets[i],
          ReplicaState.RBW);
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

  /** @return the index array indicating the block index in each storage */
  public int[] getBlockIndices() {
    int numLocations = getNumExpectedLocations();
    int[] indices = new int[numLocations];
    for (int i = 0; i < numLocations; i++) {
      indices[i] = BlockIdManager.getBlockIndex(replicas[i]);
    }
    return indices;
  }

  /** Get the number of expected locations */
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

  /** Get block recovery ID */
  public long getBlockRecoveryId() {
    return blockRecoveryId;
  }

  /**
   * Process the recorded replicas. When about to commit or finish the
   * pipeline recovery sort out bad replicas.
   * @param genStamp  The final generation stamp for the block.
   */
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

  /**
   * Commit block's length and generation stamp as reported by the client.
   * Set block state to {@link BlockUCState#COMMITTED}.
   * @param block - contains client reported block length and generation
   */
  void commitBlock(Block block) throws IOException {
    if (getBlockId() != block.getBlockId()) {
      throw new IOException("Trying to commit inconsistent block: id = "
          + block.getBlockId() + ", expected id = " + getBlockId());
    }
    blockUCState = BlockUCState.COMMITTED;
    this.set(getBlockId(), block.getNumBytes(), block.getGenerationStamp());
    // Sort out invalid replicas.
    setGenerationStampAndVerifyReplicas(block.getGenerationStamp());
  }

  /**
   * Initialize lease recovery for this striped block.
   */
  public void initializeBlockRecovery(long recoveryId) {
    setBlockUCState(BlockUCState.UNDER_RECOVERY);
    blockRecoveryId = recoveryId;
    if (replicas == null || replicas.length == 0) {
      NameNode.blockStateChangeLog.warn("BLOCK*" +
          " BlockInfoUnderConstruction.initLeaseRecovery:" +
          " No blocks found, lease removed.");
    }
    // TODO we need to implement different recovery logic here
  }

  void addReplicaIfNotPresent(DatanodeStorageInfo storage, Block reportedBlock,
      ReplicaState rState) {
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
    sb.append("{UCState=").append(blockUCState).append(", replicas=[");
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
