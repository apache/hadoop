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
package org.apache.hadoop.dfs;

import java.util.*;

import org.apache.hadoop.dfs.BlocksMap.BlockInfo;

/**************************************************
 * DatanodeDescriptor tracks stats on a given DataNode,
 * such as available storage capacity, last update time, etc.,
 * and maintains a set of blocks stored on the datanode. 
 *
 * This data structure is a data structure that is internal
 * to the namenode. It is *not* sent over-the-wire to the Client
 * or the Datnodes. Neither is it stored persistently in the
 * fsImage.

 **************************************************/
public class DatanodeDescriptor extends DatanodeInfo {

  private volatile BlockInfo blockList = null;
  // isAlive == heartbeats.contains(this)
  // This is an optimization, because contains takes O(n) time on Arraylist
  protected boolean isAlive = false;

  //
  // List of blocks to be replicated by this datanode
  // Also, a list of datanodes per block to indicate the target
  // datanode of this replication.
  //
  List<Block> replicateBlocks;
  List<DatanodeDescriptor[]> replicateTargetSets;
  List<Block> invalidateBlocks;
  
  /** Default constructor */
  public DatanodeDescriptor() {
    super();
    initWorkLists();
  }
  
  /** DatanodeDescriptor constructor
   * @param nodeID id of the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID) {
    this(nodeID, 0L, 0L, 0L, 0);
  }

  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            String networkLocation) {
    this(nodeID, networkLocation, null);
  }
  
  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   * @param hostName it could be different from host specified for DatanodeID
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            String networkLocation,
                            String hostName) {
    this(nodeID, networkLocation, hostName, 0L, 0L, 0L, 0);
  }
  
  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param capacity capacity of the data node
   * @param dfsUsed space used by the data node
   * @param remaining remaing capacity of the data node
   * @param xceiverCount # of data transfers at the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            long capacity,
                            long dfsUsed,
                            long remaining,
                            int xceiverCount) {
    super(nodeID);
    updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
    initWorkLists();
  }

  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   * @param capacity capacity of the data node, including space used by non-dfs
   * @param dfsUsed the used space by dfs datanode
   * @param remaining remaing capacity of the data node
   * @param xceiverCount # of data transfers at the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID,
                            String networkLocation,
                            String hostName,
                            long capacity,
                            long dfsUsed,
                            long remaining,
                            int xceiverCount) {
    super(nodeID, networkLocation, hostName);
    updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
    initWorkLists();
  }

  /*
   * initialize list of blocks that store work for the datanodes
   */
  private void initWorkLists() {
    replicateBlocks = new ArrayList<Block>();
    replicateTargetSets = new ArrayList<DatanodeDescriptor[]>();
    invalidateBlocks = new ArrayList<Block>();
  }

  /**
   * Add data-node to the block.
   * Add block to the head of the list of blocks belonging to the data-node.
   */
  boolean addBlock(BlockInfo b) {
    if(!b.addNode(this))
      return false;
    // add to the head of the data-node list
    blockList = b.listInsert(blockList, this);
    return true;
  }
  
  /**
   * Remove block from the list of blocks belonging to the data-node.
   * Remove data-node from the block.
   */
  boolean removeBlock(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    return b.removeNode(this);
  }

  /**
   * Move block to the head of the list of blocks belonging to the data-node.
   */
  void moveBlockToHead(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    blockList = b.listInsert(blockList, this);
  }

  void resetBlocks() {
    this.capacity = 0;
    this.remaining = 0;
    this.xceiverCount = 0;
    this.blockList = null;
  }

  int numBlocks() {
    return blockList == null ? 0 : blockList.listCount(this);
  }

  /**
   */
  void updateHeartbeat(long capacity, long dfsUsed, long remaining,
      int xceiverCount) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.lastUpdate = System.currentTimeMillis();
    this.xceiverCount = xceiverCount;
  }

  /**
   * Iterates over the list of blocks belonging to the data-node.
   */
  static private class BlockIterator implements Iterator<Block> {
    private BlockInfo current;
    private DatanodeDescriptor node;
      
    BlockIterator(BlockInfo head, DatanodeDescriptor dn) {
      this.current = head;
      this.node = dn;
    }

    public boolean hasNext() {
      return current != null;
    }

    public BlockInfo next() {
      BlockInfo res = current;
      current = current.getNext(current.findDatanode(node));
      return res;
    }

    public void remove()  {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  Iterator<Block> getBlockIterator() {
    return new BlockIterator(this.blockList, this);
  }
  
  /*
   * Store block replication work.
   */
  void addBlocksToBeReplicated(Block[] blocklist, 
                               DatanodeDescriptor[][] targets) {
    assert(blocklist != null && targets != null);
    assert(blocklist.length > 0 && targets.length > 0);
    synchronized (replicateBlocks) {
      assert(blocklist.length == targets.length);
      for (int i = 0; i < blocklist.length; i++) {
        replicateBlocks.add(blocklist[i]);
        replicateTargetSets.add(targets[i]);
      }
    }
  }

  /*
   * Store block invalidation work.
   */
  void addBlocksToBeInvalidated(Block[] blocklist) {
    assert(blocklist != null && blocklist.length > 0);
    synchronized (invalidateBlocks) {
      for (int i = 0; i < blocklist.length; i++) {
        invalidateBlocks.add(blocklist[i]);
      }
    }
  }

  /*
   * The number of work items that are pending to be replicated
   */
  int getNumberOfBlocksToBeReplicated() {
    synchronized (replicateBlocks) {
      return replicateBlocks.size();
    }
  }

  /*
   * The number of block invalidation items that are pending to 
   * be sent to the datanode
   */
  int getNumberOfBlocksToBeInvalidated() {
    synchronized (invalidateBlocks) {
      return invalidateBlocks.size();
    }
  }

  /**
   * Remove the specified number of target sets
   */
  void getReplicationSets(int maxNumTransfers, Object[] xferResults) {
    assert(xferResults.length == 2);
    assert(xferResults[0] == null && xferResults[1] == null);

    synchronized (replicateBlocks) {
      assert(replicateBlocks.size() == replicateTargetSets.size());

      if (maxNumTransfers <= 0 || replicateBlocks.size() == 0) {
        return;
      }
      int numTransfers = 0;
      int numBlocks = 0;
      int i;
      for (i = 0; i < replicateTargetSets.size() && 
             numTransfers < maxNumTransfers; i++) {
        numTransfers += replicateTargetSets.get(i).length;
      }
      numBlocks = i;
      Block[] blocklist = new Block[numBlocks];
      DatanodeDescriptor targets[][] = new DatanodeDescriptor[numBlocks][];

      for (i = 0; i < numBlocks; i++) {
        blocklist[i] = replicateBlocks.get(0);
        targets[i] = replicateTargetSets.get(0);
        replicateBlocks.remove(0);
        replicateTargetSets.remove(0);
      }
      xferResults[0] = blocklist;
      xferResults[1] = targets;
      assert(blocklist.length > 0 && targets.length > 0);
    }
  }

  /**
   * Remove the specified number of blocks to be invalidated
   */
  void getInvalidateBlocks(int maxblocks, Object[] xferResults) {
    assert(xferResults[0] == null);

    synchronized (invalidateBlocks) {
      if (maxblocks <= 0 || invalidateBlocks.size() == 0) {
        return;
      }
      int outnum = Math.min(maxblocks, invalidateBlocks.size());
      Block[] blocklist = new Block[outnum];
      for (int i = 0; i < outnum; i++) {
        blocklist[i] = invalidateBlocks.get(0);
        invalidateBlocks.remove(0);
      }
      assert(blocklist.length > 0);
      xferResults[0] = blocklist;
    }
  }

  void reportDiff(BlocksMap blocksMap,
                  BlockListAsLongs newReport,
                  Collection<Block> toAdd,
                  Collection<Block> toRemove) {
    // place a deilimiter in the list which separates blocks 
    // that have been reported from those that have not
    BlockInfo delimiter = new BlockInfo(new Block(), 1);
    boolean added = this.addBlock(delimiter);
    assert added : "Delimiting block cannot be present in the node";
    if(newReport == null)
      newReport = new BlockListAsLongs( new long[0]);
    // scan the report and collect newly reported blocks
    // Note we are taking special precaution to limit tmp blocks allocated
    // as part this block report - which why block list is stored as longs
    Block iblk = new Block(); // a fixed new'ed block to be reused with index i
    for (int i = 0; i < newReport.getNumberOfBlocks(); ++i) {
      iblk.set(newReport.getBlockId(i), newReport.getBlockLen(i));
      BlockInfo storedBlock = blocksMap.getStoredBlock(iblk);
      if(storedBlock == null) { // Brand new block
        toAdd.add(new Block(iblk));
        continue;
      }
      if(storedBlock.findDatanode(this) < 0) {// Known block, but not on the DN
        toAdd.add(storedBlock);
        continue;
      }
      // move block to the head of the list
      this.moveBlockToHead(storedBlock);
    }
    // collect blocks that have not been reported
    // all of them are next to the delimiter
    Iterator<Block> it = new BlockIterator(delimiter.getNext(0), this);
    while(it.hasNext())
      toRemove.add(it.next());
    this.removeBlock(delimiter);
  }
}
