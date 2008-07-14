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

import java.io.DataInput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableUtils;

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
  /** Block and targets pair */
  public static class BlockTargetPair {
    public final Block block;
    public final DatanodeDescriptor[] targets;    

    BlockTargetPair(Block block, DatanodeDescriptor[] targets) {
      this.block = block;
      this.targets = targets;
    }
  }

  /** A BlockTargetPair queue. */
  private static class BlockQueue {
    private final Queue<BlockTargetPair> blockq = new LinkedList<BlockTargetPair>();

    /** Size of the queue */
    synchronized int size() {return blockq.size();}

    /** Enqueue */
    synchronized boolean offer(Block block, DatanodeDescriptor[] targets) { 
      return blockq.offer(new BlockTargetPair(block, targets));
    }

    /** Dequeue */
    synchronized List<BlockTargetPair> poll(int numTargets) {
      if (numTargets <= 0 || blockq.isEmpty()) {
        return null;
      }
      else {
        List<BlockTargetPair> results = new ArrayList<BlockTargetPair>();
        for(; !blockq.isEmpty() && numTargets > 0; ) {
          numTargets -= blockq.peek().targets.length; 
          if (numTargets >= 0) {
            results.add(blockq.poll());
          }
        }
        return results;
      }
    }
  }

  private volatile BlockInfo blockList = null;
  // isAlive == heartbeats.contains(this)
  // This is an optimization, because contains takes O(n) time on Arraylist
  protected boolean isAlive = false;

  /** A queue of blocks to be replicated by this datanode */
  private BlockQueue replicateBlocks = new BlockQueue();
  /** A queue of blocks to be recovered by this datanode */
  private BlockQueue recoverBlocks = new BlockQueue();
  /** A set of blocks to be invalidated by this datanode */
  private Set<Block> invalidateBlocks = new TreeSet<Block>();

  boolean processedBlockReport = false;
  
  /** Default constructor */
  public DatanodeDescriptor() {}
  
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
    this.dfsUsed = 0;
    this.xceiverCount = 0;
    this.blockList = null;
  }

  public int numBlocks() {
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
  
  /**
   * Store block replication work.
   */
  void addBlockToBeReplicated(Block block, DatanodeDescriptor[] targets) {
    assert(block != null && targets != null && targets.length > 0);
    replicateBlocks.offer(block, targets);
  }

  /**
   * Store block recovery work.
   */
  void addBlockToBeRecovered(Block block, DatanodeDescriptor[] targets) {
    assert(block != null && targets != null && targets.length > 0);
    recoverBlocks.offer(block, targets);
  }

  /**
   * Store block invalidation work.
   */
  void addBlocksToBeInvalidated(List<Block> blocklist) {
    assert(blocklist != null && blocklist.size() > 0);
    synchronized (invalidateBlocks) {
      for(Block blk : blocklist) {
        invalidateBlocks.add(blk);
      }
    }
  }

  /**
   * The number of work items that are pending to be replicated
   */
  int getNumberOfBlocksToBeReplicated() {
    return replicateBlocks.size();
  }

  /**
   * The number of block invalidation items that are pending to 
   * be sent to the datanode
   */
  int getNumberOfBlocksToBeInvalidated() {
    synchronized (invalidateBlocks) {
      return invalidateBlocks.size();
    }
  }
  
  /**
   * Set the bit signifying that the first block report from this datanode has been 
   * processed
   */
  void setBlockReportProcessed(boolean val) {
    processedBlockReport = val;
  }
  
  /**
   * Have we processed any block report from this datanode yet
   */
  boolean getBlockReportProcessed() {
    return processedBlockReport;
  }

  BlockCommand getReplicationCommand(int maxTransfers) {
    List<BlockTargetPair> blocktargetlist = replicateBlocks.poll(maxTransfers);
    return blocktargetlist == null? null:
        new BlockCommand(DatanodeProtocol.DNA_TRANSFER, blocktargetlist);
  }

  BlockCommand getLeaseRecoveryCommand(int maxTransfers) {
    List<BlockTargetPair> blocktargetlist = recoverBlocks.poll(maxTransfers);
    return blocktargetlist == null? null:
        new BlockCommand(DatanodeProtocol.DNA_RECOVERBLOCK, blocktargetlist);
  }

  /**
   * Remove the specified number of blocks to be invalidated
   */
  BlockCommand getInvalidateBlocks(int maxblocks) {
    Block[] deleteList = getBlockArray(invalidateBlocks, maxblocks); 
    return deleteList == null? 
        null: new BlockCommand(DatanodeProtocol.DNA_INVALIDATE, deleteList);
  }

  static private Block[] getBlockArray(Collection<Block> blocks, int max) {
    Block[] blockarray = null;
    synchronized(blocks) {
      int n = blocks.size();
      if (max > 0 && n > 0) {
        if (max < n) {
          n = max;
        }
        blockarray = blocks.toArray(new Block[n]);
        blocks.clear();
        assert(blockarray.length > 0);
      }
    }
    return blockarray;
  }

  void reportDiff(BlocksMap blocksMap,
                  BlockListAsLongs newReport,
                  Collection<Block> toAdd,
                  Collection<Block> toRemove,
                  Collection<Block> toInvalidate) {
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
      iblk.set(newReport.getBlockId(i), newReport.getBlockLen(i), 
               newReport.getBlockGenStamp(i));
      BlockInfo storedBlock = blocksMap.getStoredBlock(iblk);
      if(storedBlock == null) {
        // If block is not in blocksMap it does not belong to any file
        toInvalidate.add(new Block(iblk));
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

  /** Serialization for FSEditLog */
  void readFieldsFromFSEditLog(DataInput in) throws IOException {
    this.name = UTF8.readString(in);
    this.storageID = UTF8.readString(in);
    this.infoPort = in.readShort() & 0x0000ffff;

    this.capacity = in.readLong();
    this.dfsUsed = in.readLong();
    this.remaining = in.readLong();
    this.lastUpdate = in.readLong();
    this.xceiverCount = in.readInt();
    this.location = Text.readString(in);
    this.hostName = Text.readString(in);
    setAdminState(WritableUtils.readEnum(in, AdminStates.class));
  }
}
