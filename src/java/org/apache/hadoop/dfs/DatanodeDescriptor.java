/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

/**************************************************
 * DatanodeDescriptor tracks stats on a given DataNode,
 * such as available storage capacity, last update time, etc.,
 * and maintains a set of blocks stored on the datanode. 
 *
 * @author Mike Cafarella
 * @author Konstantin Shvachko
 **************************************************/
class DatanodeDescriptor extends DatanodeInfo implements Comparable {

  private volatile TreeSet blocks;

  DatanodeDescriptor( DatanodeID nodeID ) {
    this( nodeID.getName(), nodeID.getStorageID(), 0, 0);
  }
  
  /**
   * Create DatanodeDescriptor.
   */
  DatanodeDescriptor( DatanodeID nodeID, 
                            long capacity, 
                            long remaining) {
    this( nodeID.getName(), nodeID.getStorageID(), capacity, remaining );
  }

  /**
   * @param name hostname:portNumber as String object.
   */
  DatanodeDescriptor( String name, 
                            String storageID, 
                            long capacity, 
                            long remaining) {
    super( name, storageID );
    this.blocks = new TreeSet();
    updateHeartbeat(capacity, remaining);
  }

  /**
   */
  void updateBlocks(Block newBlocks[]) {
    blocks.clear();
    for (int i = 0; i < newBlocks.length; i++) {
      blocks.add(newBlocks[i]);
    }
  }

  /**
   */
  void addBlock(Block b) {
    blocks.add(b);
  }

  /**
   */
  void updateHeartbeat(long capacity, long remaining) {
    this.capacity = capacity;
    this.remaining = remaining;
    this.lastUpdate = System.currentTimeMillis();
  }
  
  /**
   * Verify whether the node is dead.
   * 
   * A data node is considered dead if its last heartbeat was received
   * EXPIRE_INTERVAL msecs ago.
   */
  boolean isDead() {
    return getLastUpdate() < 
              System.currentTimeMillis() - FSConstants.EXPIRE_INTERVAL;
  }

  Block[] getBlocks() {
    return (Block[]) blocks.toArray(new Block[blocks.size()]);
  }

  Iterator getBlockIterator() {
    return blocks.iterator();
  }

  /** Comparable.
   * Basis of compare is the String name (host:portNumber) only.
   * @param o
   * @return as specified by Comparable.
   */
  public int compareTo(Object o) {
    DatanodeDescriptor d = (DatanodeDescriptor) o;
    return name.compareTo(d.getName());
  }
}
