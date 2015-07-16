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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * Represents a block that is currently being constructed.<br>
 * This is usually the last block of a file opened for write or append.
 */
public class BlockInfoUnderConstructionContiguous extends BlockInfoContiguous
    implements BlockInfoUnderConstruction{
  /** Block state. See {@link BlockUCState} */
  private BlockUCState blockUCState;

  /**
   * Block replicas as assigned when the block was allocated.
   * This defines the pipeline order.
   */
  private List<ReplicaUnderConstruction> replicas;

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
  private Block truncateBlock;

  /**
   * Create block and set its state to
   * {@link BlockUCState#UNDER_CONSTRUCTION}.
   */
  public BlockInfoUnderConstructionContiguous(Block blk, short replication) {
    this(blk, replication, BlockUCState.UNDER_CONSTRUCTION, null);
  }

  /**
   * Create a block that is currently being constructed.
   */
  public BlockInfoUnderConstructionContiguous(Block blk, short replication,
      BlockUCState state, DatanodeStorageInfo[] targets) {
    super(blk, replication);
    assert getBlockUCState() != BlockUCState.COMPLETE :
      "BlockInfoUnderConstructionContiguous cannot be in COMPLETE state";
    this.blockUCState = state;
    setExpectedLocations(targets);
  }

  @Override
  public BlockInfoContiguous convertToCompleteBlock() throws IOException {
    assert getBlockUCState() != BlockUCState.COMPLETE :
      "Trying to convert a COMPLETE block";
    return new BlockInfoContiguous(this);
  }

  @Override
  public void setExpectedLocations(DatanodeStorageInfo[] targets) {
    int numLocations = targets == null ? 0 : targets.length;
    this.replicas = new ArrayList<>(numLocations);
    for(int i = 0; i < numLocations; i++) {
      replicas.add(new ReplicaUnderConstruction(this, targets[i],
          ReplicaState.RBW));
    }
  }

  @Override
  public DatanodeStorageInfo[] getExpectedStorageLocations() {
    int numLocations = replicas == null ? 0 : replicas.size();
    DatanodeStorageInfo[] storages = new DatanodeStorageInfo[numLocations];
    for (int i = 0; i < numLocations; i++) {
      storages[i] = replicas.get(i).getExpectedStorageLocation();
    }
    return storages;
  }

  @Override
  public int getNumExpectedLocations() {
    return replicas == null ? 0 : replicas.size();
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
    return truncateBlock;
  }

  @Override
  public Block toBlock(){
    return this;
  }

  public void setTruncateBlock(Block recoveryBlock) {
    this.truncateBlock = recoveryBlock;
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
    if(getBlockId() != block.getBlockId())
      throw new IOException("Trying to commit inconsistent block: id = "
          + block.getBlockId() + ", expected id = " + getBlockId());
    blockUCState = BlockUCState.COMMITTED;
    this.set(getBlockId(), block.getNumBytes(), block.getGenerationStamp());
    // Sort out invalid replicas.
    setGenerationStampAndVerifyReplicas(block.getGenerationStamp());
  }

  @Override
  public void initializeBlockRecovery(long recoveryId) {
    setBlockUCState(BlockUCState.UNDER_RECOVERY);
    blockRecoveryId = recoveryId;
    if (replicas.size() == 0) {
      NameNode.blockStateChangeLog.warn("BLOCK*"
        + " BlockInfoUnderConstructionContiguous.initLeaseRecovery:"
        + " No blocks found, lease removed.");
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
    for(int i = 0; i < replicas.size(); i++) {
      // Skip alive replicas which have been chosen for recovery.
      if (!(replicas.get(i).isAlive() && !replicas.get(i).getChosenAsPrimary())) {
        continue;
      }
      final ReplicaUnderConstruction ruc = replicas.get(i);
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
      Block block, ReplicaState rState) {
    Iterator<ReplicaUnderConstruction> it = replicas.iterator();
    while (it.hasNext()) {
      ReplicaUnderConstruction r = it.next();
      DatanodeStorageInfo expectedLocation = r.getExpectedStorageLocation();
      if(expectedLocation == storage) {
        // Record the gen stamp from the report
        r.setGenerationStamp(block.getGenerationStamp());
        return;
      } else if (expectedLocation != null &&
                 expectedLocation.getDatanodeDescriptor() ==
                     storage.getDatanodeDescriptor()) {

        // The Datanode reported that the block is on a different storage
        // than the one chosen by BlockPlacementPolicy. This can occur as
        // we allow Datanodes to choose the target storage. Update our
        // state by removing the stale entry and adding a new one.
        it.remove();
        break;
      }
    }
    replicas.add(new ReplicaUnderConstruction(block, storage, rState));
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
    sb.append("{UCState=").append(blockUCState)
      .append(", truncateBlock=" + truncateBlock)
      .append(", primaryNodeIndex=").append(primaryNodeIndex)
      .append(", replicas=[");
    if (replicas != null) {
      Iterator<ReplicaUnderConstruction> iter = replicas.iterator();
      if (iter.hasNext()) {
        iter.next().appendStringTo(sb);
        while (iter.hasNext()) {
          sb.append(", ");
          iter.next().appendStringTo(sb);
        }
      }
    }
    sb.append("]}");
  }
}
