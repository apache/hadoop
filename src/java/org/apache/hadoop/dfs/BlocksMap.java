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

/**
 * This class maintains the map from a block to its metadata.
 * block's metadata currently includes INode it belongs to and
 * the datanodes that store the block.
 */
class BlocksMap {
        
  /**
   * Internal class for block metadata.
   */
  static class BlockInfo {
    private INodeFile          inode;
      
    /** nodes could contain some null entries at the end, so 
     *  nodes.legth >= number of datanodes. 
     *  if nodes != null then nodes[0] != null.
     */
    private DatanodeDescriptor[]           nodes;
    private Block                          block; //block that was inserted.   
  }
      
  private static class NodeIterator implements Iterator<DatanodeDescriptor> {
    NodeIterator(DatanodeDescriptor[] nodes) {
      arr = nodes;
    }
    private DatanodeDescriptor[] arr;
    private int nextIdx = 0;
      
    public boolean hasNext() {
      return arr != null && nextIdx < arr.length && arr[nextIdx] != null;
    }
      
    public DatanodeDescriptor next() {
      return arr[nextIdx++];
    }
      
    public void remove()  {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }
      
  private Map<Block, BlockInfo> map = new HashMap<Block, BlockInfo>();
      
  /** add BlockInfo if mapping does not exist. */
  private BlockInfo checkBlockInfo(Block b) {
    BlockInfo info = map.get(b);
    if (info == null) {
      info = new BlockInfo();
      info.block = b;
      map.put(b, info);
    }
    return info;
  }
      
  public INodeFile getINode(Block b) {
    BlockInfo info = map.get(b);
    return (info != null) ? info.inode : null;
  }
          
  public void addINode(Block b, INodeFile iNode) {
    BlockInfo info = checkBlockInfo(b);
    info.inode = iNode;
  }
    
  public void removeINode(Block b) {
    BlockInfo info = map.get(b);
    if (info != null) {
      info.inode = null;
      if (info.nodes == null) {
        map.remove(b);
      }
    }
  }
      
  /** Returns the block object it it exists in the map. */
  public Block getStoredBlock(Block b) {
    BlockInfo info = map.get(b);
    return (info != null) ? info.block : null;
  }
    
  /** Returned Iterator does not support. */
  public Iterator<DatanodeDescriptor> nodeIterator(Block b) {
    BlockInfo info = map.get(b);
    return new NodeIterator((info != null) ? info.nodes : null);
  }
    
  /** counts number of containing nodes. Better than using iterator. */
  public int numNodes(Block b) {
    int count = 0;
    BlockInfo info = map.get(b);
    if (info != null && info.nodes != null) {
      count = info.nodes.length;
      while (info.nodes[ count-1 ] == null) {// mostly false
        count--;
      }
    }
    return count;
  }
      
  /** returns true if the node does not already exists and is added.
   * false if the node already exists.*/
  public boolean addNode(Block b, 
                         DatanodeDescriptor node,
                         int replicationHint) {
    BlockInfo info = checkBlockInfo(b);
    if (info.nodes == null) {
      info.nodes = new DatanodeDescriptor[ replicationHint ];
    }
      
    DatanodeDescriptor[] arr = info.nodes;
    for(int i=0; i < arr.length; i++) {
      if (arr[i] == null) {
        arr[i] = node;
        return true;
      }
      if (arr[i] == node) {
        return false;
      }
    }

    /* Not enough space left. Create a new array. Should normally 
     * happen only when replication is manually increased by the user. */
    info.nodes = new DatanodeDescriptor[ arr.length + 1 ];
    for(int i=0; i < arr.length; i++) {
      info.nodes[i] = arr[i];
    }
    info.nodes[ arr.length ] = node;
    return true;
  }
    
  public boolean removeNode(Block b, DatanodeDescriptor node) {
    BlockInfo info = map.get(b);
    if (info == null || info.nodes == null) {
      return false;
    }

    boolean removed = false;
    // swap lastNode and node's location. set lastNode to null.
    DatanodeDescriptor[] arr = info.nodes;
    int lastNode = -1;
    for(int i=arr.length-1; i >= 0; i--) {
      if (lastNode < 0 && arr[i] != null) {
        lastNode = i;
      }
      if (arr[i] == node) {
        arr[i] = arr[ lastNode ];
        arr[ lastNode ] = null;
        removed = true;
        break;
      }
    }
        
    /*
     * if ((lastNode + 1) < arr.length/4) {
     *    we could trim the array.
     * } 
     */
    if (arr[0] == null) { // no datanodes left.
      info.nodes = null;
      if (info.inode == null) {
        map.remove(b);
      }
    }
    return removed;
  }

  public int size() {
    return map.size();
  }
}
