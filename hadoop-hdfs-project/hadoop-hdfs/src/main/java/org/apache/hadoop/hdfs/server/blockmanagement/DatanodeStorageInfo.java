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
  static DatanodeInfo[] toDatanodeInfos(DatanodeStorageInfo[] storages) {
    return toDatanodeInfos(Arrays.asList(storages));
  }
  static DatanodeInfo[] toDatanodeInfos(List<DatanodeStorageInfo> storages) {
    final DatanodeInfo[] datanodes = new DatanodeInfo[storages.size()];
    for(int i = 0; i < storages.size(); i++) {
      datanodes[i] = storages.get(i).getDatanodeDescriptor();
    }
    return datanodes;
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
  
  public DatanodeStorageInfo(DatanodeDescriptor dn, DatanodeStorage s) {
    this.dn = dn;
    this.storageID = s.getStorageID();
    this.storageType = s.getStorageType();
    this.state = s.getState();
  }
  
  public void setUtilization(long capacity, long dfsUsed, long remaining) {
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
    return true;
  }

  public boolean removeBlock(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    return b.removeStorage(this);
  }

  public int numBlocks() {
    return blockList == null ? 0 : blockList.listCount(this);
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
  
  @Override
  public String toString() {
    return "[" + storageType + "]" + storageID + ":" + state;
  }
}
