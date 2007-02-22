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

import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

/**************************************************
 * DatanodeDescriptor tracks stats on a given DataNode,
 * such as available storage capacity, last update time, etc.,
 * and maintains a set of blocks stored on the datanode. 
 *
 * This data structure is a data structure that is internal
 * to the namenode. It is *not* sent over-the-wire to the Client
 * or the Datnodes. Neither is it stored persistently in the
 * fsImage.

 * @author Mike Cafarella
 * @author Konstantin Shvachko
 **************************************************/
public class DatanodeDescriptor extends DatanodeInfo {

  private volatile SortedMap<Block, Block> blocks = new TreeMap<Block, Block>();
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
  public DatanodeDescriptor( DatanodeID nodeID ) {
    this( nodeID, 0L, 0L, 0 );
  }

  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   */
  public DatanodeDescriptor( DatanodeID nodeID, 
                             String networkLocation ) {
    this( nodeID, networkLocation, null );
  }
  
  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   * @param hostName it could be different from host specified for DatanodeID
   */
  public DatanodeDescriptor( DatanodeID nodeID, 
                             String networkLocation,
                             String hostName ) {
    this( nodeID, networkLocation, hostName, 0L, 0L, 0 );
  }
  
  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param capacity capacity of the data node
   * @param remaining remaing capacity of the data node
   * @param xceiverCount # of data transfers at the data node
   */
  public DatanodeDescriptor( DatanodeID nodeID, 
                      long capacity, 
                      long remaining,
                      int xceiverCount ) {
    super( nodeID );
    updateHeartbeat(capacity, remaining, xceiverCount);
    initWorkLists();
  }

  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   * @param capacity capacity of the data node
   * @param remaining remaing capacity of the data node
   * @param xceiverCount # of data transfers at the data node
   */
  public DatanodeDescriptor( DatanodeID nodeID,
                              String networkLocation,
                              String hostName,
                              long capacity, 
                              long remaining,
                              int xceiverCount ) {
    super( nodeID, networkLocation, hostName );
    updateHeartbeat( capacity, remaining, xceiverCount);
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
   */
  void addBlock(Block b) {
      blocks.put(b, b);
  }
  
  void removeBlock(Block b) {
      blocks.remove(b);
  }

  void resetBlocks() {
    this.capacity = 0;
    this.remaining = 0;
    this.xceiverCount = 0;
    this.blocks.clear();
  }

  int numBlocks() {
    return blocks.size();
  }
  
  /**
   */
  void updateHeartbeat(long capacity, long remaining, int xceiverCount) {
    this.capacity = capacity;
    this.remaining = remaining;
    this.lastUpdate = System.currentTimeMillis();
    this.xceiverCount = xceiverCount;
  }
  
  Block[] getBlocks() {
    return (Block[]) blocks.keySet().toArray(new Block[blocks.size()]);
  }

  Iterator<Block> getBlockIterator() {
    return blocks.keySet().iterator();
  }
  
  Block getBlock(long blockId) {
    return blocks.get( new Block(blockId, 0) );
  }
  
  Block getBlock(Block b) {
    return blocks.get(b);
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
   * The number of block invalidattion items that are pending to 
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
}
