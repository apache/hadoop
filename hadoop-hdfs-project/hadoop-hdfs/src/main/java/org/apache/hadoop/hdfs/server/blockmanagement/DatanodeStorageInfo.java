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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;

/**
 * A Datanode has one or more storages. A storage in the Datanode is represented
 * by this class.
 */
public class DatanodeStorageInfo {
  public static final DatanodeStorageInfo[] EMPTY_ARRAY = {};

  public static DatanodeInfo[] toDatanodeInfos(DatanodeStorageInfo[] storages) {
    return toDatanodeInfos(Arrays.asList(storages));
  }
  static DatanodeInfo[] toDatanodeInfos(List<DatanodeStorageInfo> storages) {
    final DatanodeInfo[] datanodes = new DatanodeInfo[storages.size()];
    for(int i = 0; i < storages.size(); i++) {
      datanodes[i] = storages.get(i).getDatanodeDescriptor();
    }
    return datanodes;
  }

  public static String[] toStorageIDs(DatanodeStorageInfo[] storages) {
    String[] storageIDs = new String[storages.length];
    for(int i = 0; i < storageIDs.length; i++) {
      storageIDs[i] = storages[i].getStorageID();
    }
    return storageIDs;
  }

  public static StorageType[] toStorageTypes(DatanodeStorageInfo[] storages) {
    StorageType[] storageTypes = new StorageType[storages.length];
    for(int i = 0; i < storageTypes.length; i++) {
      storageTypes[i] = storages[i].getStorageType();
    }
    return storageTypes;
  }

  /**
   * Iterates over the list of blocks belonging to the data-node.
   */
  static class BlockIterator implements Iterator<BlockInfo> {
    private BlockInfo current;
    private DatanodeStorageInfo node;

    BlockIterator(BlockInfo head, DatanodeStorageInfo dn) {
      this.current = head;
      this.node = dn;
    }

    public boolean hasNext() {
      return current != null;
    }

    public BlockInfo next() {
      BlockInfo res = current;
      current = current.getNext(current.findStorageInfo(node));
      return res;
    }

    public void remove() {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  private final DatanodeDescriptor dn;
  private final String storageID;
  private final StorageType storageType;
  private State state;

  private long capacity;
  private long dfsUsed;
  private long remaining;

  private volatile BlockInfo blockList = null;
  private int numBlocks = 0;

  /** The number of block reports received */
  private int blockReportCount = 0;

  /**
   * Set to false on any NN failover, and reset to true
   * whenever a block report is received.
   */
  private boolean heartbeatedSinceFailover = false;

  /**
   * At startup or at failover, the storages in the cluster may have pending
   * block deletions from a previous incarnation of the NameNode. The block
   * contents are considered as stale until a block report is received. When a
   * storage is considered as stale, the replicas on it are also considered as
   * stale. If any block has at least one stale replica, then no invalidations
   * will be processed for this block. See HDFS-1972.
   */
  private boolean blockContentsStale = true;

  /* Variables for maintaining number of blocks scheduled to be written to
   * this storage. This count is approximate and might be slightly bigger
   * in case of errors (e.g. datanode does not report if an error occurs
   * while writing the block).
   */
  private int currApproxBlocksScheduled = 0;
  private int prevApproxBlocksScheduled = 0;
  private long lastBlocksScheduledRollTime = 0;
  private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600*1000; //10min

  public DatanodeStorageInfo(DatanodeDescriptor dn, DatanodeStorage s) {
    this.dn = dn;
    this.storageID = s.getStorageID();
    this.storageType = s.getStorageType();
    this.state = s.getState();
  }

  int getBlockReportCount() {
    return blockReportCount;
  }

  void setBlockReportCount(int blockReportCount) {
    this.blockReportCount = blockReportCount;
  }

  boolean areBlockContentsStale() {
    return blockContentsStale;
  }

  void markStaleAfterFailover() {
    heartbeatedSinceFailover = false;
    blockContentsStale = true;
  }

  void receivedHeartbeat(final long lastUpdate) {
    heartbeatedSinceFailover = true;
    rollBlocksScheduled(lastUpdate);
  }

  void receivedBlockReport() {
    if (heartbeatedSinceFailover) {
      blockContentsStale = false;
    }
    blockReportCount++;
  }

  void setUtilization(long capacity, long dfsUsed, long remaining) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
  }
  
  public void setState(State s) {
    this.state = s;
    
    // TODO: if goes to failed state cleanup the block list
  }
  
  public State getState() {
    return this.state;
  }
  
  public String getStorageID() {
    return storageID;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  public long getCapacity() {
    return capacity;
  }

  public long getDfsUsed() {
    return dfsUsed;
  }

  public long getRemaining() {
    return remaining;
  }

  public boolean addBlock(BlockInfo b) {
    if(!b.addStorage(this))
      return false;
    // add to the head of the data-node list
    blockList = b.listInsert(blockList, this);
    numBlocks++;
    return true;
  }

  public boolean removeBlock(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    if (b.removeStorage(this)) {
      numBlocks--;
      return true;
    } else {
      return false;
    }
  }

  public int numBlocks() {
    return numBlocks;
  }
  
  Iterator<BlockInfo> getBlockIterator() {
    return new BlockIterator(this.blockList, this);
  }

  public void updateState(StorageReport r) {
    capacity = r.getCapacity();
    dfsUsed = r.getDfsUsed();
    remaining = r.getRemaining();
  }

  public DatanodeDescriptor getDatanodeDescriptor() {
    return dn;
  }

  /**
   * @return Approximate number of blocks currently scheduled to be written
   *         to this storage.
   */
  int getBlocksScheduled() {
    return currApproxBlocksScheduled + prevApproxBlocksScheduled;
  }

  /** Increment the number of blocks scheduled for each given storage */ 
  public static void incrementBlocksScheduled(DatanodeStorageInfo... storages) {
    for (DatanodeStorageInfo s : storages) {
      s.incrementBlocksScheduled();
    }
  }

  /** Increment the number of blocks scheduled. */
  private void incrementBlocksScheduled() {
    currApproxBlocksScheduled++;
  }
  
  /** Decrement the number of blocks scheduled. */
  void decrementBlocksScheduled() {
    if (prevApproxBlocksScheduled > 0) {
      prevApproxBlocksScheduled--;
    } else if (currApproxBlocksScheduled > 0) {
      currApproxBlocksScheduled--;
    } 
    // its ok if both counters are zero.
  }
  
  /** Adjusts curr and prev number of blocks scheduled every few minutes. */
  private void rollBlocksScheduled(long now) {
    if (now - lastBlocksScheduledRollTime > BLOCKS_SCHEDULED_ROLL_INTERVAL) {
      prevApproxBlocksScheduled = currApproxBlocksScheduled;
      currApproxBlocksScheduled = 0;
      lastBlocksScheduledRollTime = now;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || !(obj instanceof DatanodeStorageInfo)) {
      return false;
    }
    final DatanodeStorageInfo that = (DatanodeStorageInfo)obj;
    return this.storageID.equals(that.storageID);
  }

  @Override
  public int hashCode() {
    return storageID.hashCode();
  }

  @Override
  public String toString() {
    return "[" + storageType + "]" + storageID + ":" + state;
  }
}
