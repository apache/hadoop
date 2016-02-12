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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.LightWeightGSet;

import static org.apache.hadoop.hdfs.server.namenode.INodeId.INVALID_INODE_ID;

/**
 * For a given block (or an erasure coding block group), BlockInfo class
 * maintains 1) the {@link BlockCollection} it is part of, and 2) datanodes
 * where the replicas of the block, or blocks belonging to the erasure coding
 * block group, are stored.
 */
@InterfaceAudience.Private
public abstract class BlockInfo extends Block
    implements LightWeightGSet.LinkedElement {

  public static final BlockInfo[] EMPTY_ARRAY = {};

  /**
   * Replication factor.
   */
  private short replication;

  /**
   * Block collection ID.
   */
  private long bcId;

  /** For implementing {@link LightWeightGSet.LinkedElement} interface. */
  private LightWeightGSet.LinkedElement nextLinkedElement;


  // Storages this block is replicated on
  protected DatanodeStorageInfo[] storages;

  private BlockUnderConstructionFeature uc;

  /**
   * Construct an entry for blocksmap
   * @param size the block's replication factor, or the total number of blocks
   *             in the block group
   */
  public BlockInfo(short size) {
    this.storages = new DatanodeStorageInfo[size];
    this.bcId = INVALID_INODE_ID;
    this.replication = isStriped() ? 0 : size;
  }

  public BlockInfo(Block blk, short size) {
    super(blk);
    this.storages = new DatanodeStorageInfo[size];
    this.bcId = INVALID_INODE_ID;
    this.replication = isStriped() ? 0 : size;
  }

  public short getReplication() {
    return replication;
  }

  public void setReplication(short repl) {
    this.replication = repl;
  }

  public long getBlockCollectionId() {
    return bcId;
  }

  public void setBlockCollectionId(long id) {
    this.bcId = id;
  }

  public void delete() {
    setBlockCollectionId(INVALID_INODE_ID);
  }

  public boolean isDeleted() {
    return bcId == INVALID_INODE_ID;
  }

  public Iterator<DatanodeStorageInfo> getStorageInfos() {
    return new Iterator<DatanodeStorageInfo>() {

      private int index = 0;

      @Override
      public boolean hasNext() {
        while (index < storages.length && storages[index] == null) {
          index++;
        }
        return index < storages.length;
      }

      @Override
      public DatanodeStorageInfo next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return storages[index++];
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Sorry. can't remove.");
      }
    };
  }

  public DatanodeDescriptor getDatanode(int index) {
    DatanodeStorageInfo storage = getStorageInfo(index);
    return storage == null ? null : storage.getDatanodeDescriptor();
  }

  DatanodeStorageInfo getStorageInfo(int index) {
    assert this.storages != null : "BlockInfo is not initialized";
    return storages[index];
  }

  void setStorageInfo(int index, DatanodeStorageInfo storage) {
    assert this.storages != null : "BlockInfo is not initialized";
    this.storages[index] = storage;
  }

  public int getCapacity() {
    assert this.storages != null : "BlockInfo is not initialized";
    return storages.length;
  }

  /**
   * Count the number of data-nodes the block currently belongs to (i.e., NN
   * has received block reports from the DN).
   */
  public abstract int numNodes();

  /**
   * Add a {@link DatanodeStorageInfo} location for a block
   * @param storage The storage to add
   * @param reportedBlock The block reported from the datanode. This is only
   *                      used by erasure coded blocks, this block's id contains
   *                      information indicating the index of the block in the
   *                      corresponding block group.
   */
  abstract boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock);

  /**
   * Remove {@link DatanodeStorageInfo} location for a block
   */
  abstract boolean removeStorage(DatanodeStorageInfo storage);

  public abstract boolean isStriped();

  /** @return true if there is no datanode storage associated with the block */
  abstract boolean hasNoStorage();

  /**
   * Find specified DatanodeStorageInfo.
   * @return DatanodeStorageInfo or null if not found.
   */
  DatanodeStorageInfo findStorageInfo(DatanodeDescriptor dn) {
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if(cur != null && cur.getDatanodeDescriptor() == dn) {
        return cur;
      }
    }
    return null;
  }

  /**
   * Find specified DatanodeStorageInfo.
   * @return index or -1 if not found.
   */
  int findStorageInfo(DatanodeStorageInfo storageInfo) {
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if (cur == storageInfo) {
        return idx;
      }
    }
    return -1;
  }

  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }

  @Override
  public LightWeightGSet.LinkedElement getNext() {
    return nextLinkedElement;
  }

  @Override
  public void setNext(LightWeightGSet.LinkedElement next) {
    this.nextLinkedElement = next;
  }

  /* UnderConstruction Feature related */

  public BlockUnderConstructionFeature getUnderConstructionFeature() {
    return uc;
  }

  public BlockUCState getBlockUCState() {
    return uc == null ? BlockUCState.COMPLETE : uc.getBlockUCState();
  }

  /**
   * Is this block complete?
   *
   * @return true if the state of the block is {@link BlockUCState#COMPLETE}
   */
  public boolean isComplete() {
    return getBlockUCState().equals(BlockUCState.COMPLETE);
  }

  public final boolean isCompleteOrCommitted() {
    final BlockUCState state = getBlockUCState();
    return state.equals(BlockUCState.COMPLETE) ||
        state.equals(BlockUCState.COMMITTED);
  }

  /**
   * Add/Update the under construction feature.
   */
  public void convertToBlockUnderConstruction(BlockUCState s,
      DatanodeStorageInfo[] targets) {
    if (isComplete()) {
      uc = new BlockUnderConstructionFeature(this, s, targets,
          this.isStriped());
    } else {
      // the block is already under construction
      uc.setBlockUCState(s);
      uc.setExpectedLocations(this, targets, this.isStriped());
    }
  }

  /**
   * Convert an under construction block to complete.
   */
  void convertToCompleteBlock() {
    assert getBlockUCState() != BlockUCState.COMPLETE :
        "Trying to convert a COMPLETE block";
    uc = null;
  }

  /**
   * Process the recorded replicas. When about to commit or finish the
   * pipeline recovery sort out bad replicas.
   * @param genStamp  The final generation stamp for the block.
   */
  public void setGenerationStampAndVerifyReplicas(long genStamp) {
    Preconditions.checkState(uc != null && !isComplete());
    // Set the generation stamp for the block.
    setGenerationStamp(genStamp);

    // Remove the replicas with wrong gen stamp
    List<ReplicaUnderConstruction> staleReplicas = uc.getStaleReplicas(genStamp);
    for (ReplicaUnderConstruction r : staleReplicas) {
      r.getExpectedStorageLocation().removeBlock(this);
      NameNode.blockStateChangeLog.debug("BLOCK* Removing stale replica {}"
          + " of {}", r, Block.toString(r));
    }
  }

  /**
   * Commit block's length and generation stamp as reported by the client.
   * Set block state to {@link BlockUCState#COMMITTED}.
   * @param block - contains client reported block length and generation
   * @throws IOException if block ids are inconsistent.
   */
  void commitBlock(Block block) throws IOException {
    if (getBlockId() != block.getBlockId()) {
      throw new IOException("Trying to commit inconsistent block: id = "
          + block.getBlockId() + ", expected id = " + getBlockId());
    }
    Preconditions.checkState(!isComplete());
    uc.commit();
    this.setNumBytes(block.getNumBytes());
    // Sort out invalid replicas.
    setGenerationStampAndVerifyReplicas(block.getGenerationStamp());
  }
}
