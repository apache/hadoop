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

/**************************************************
 * DatanodeDescriptor tracks stats on a given DataNode,
 * such as available storage capacity, last update time, etc.,
 * and maintains a set of blocks stored on the datanode. 
 *
 * @author Mike Cafarella
 * @author Konstantin Shvachko
 **************************************************/
class DatanodeDescriptor extends DatanodeInfo {

  private volatile Collection<Block> blocks = new TreeSet<Block>();
  // isAlive == heartbeats.contains(this)
  // This is an optimization, because contains takes O(n) time on Arraylist
  protected boolean isAlive = false;

  DatanodeDescriptor() {
    super();
  }
  
  DatanodeDescriptor( DatanodeID nodeID ) {
    this( nodeID, 0L, 0L, 0 );
  }
  
  /**
   * Create DatanodeDescriptor.
   */
  DatanodeDescriptor( DatanodeID nodeID, 
                      long capacity, 
                      long remaining,
                      int xceiverCount ) {
    super( nodeID );
    updateHeartbeat(capacity, remaining, xceiverCount);
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
    return (Block[]) blocks.toArray(new Block[blocks.size()]);
  }

  Iterator<Block> getBlockIterator() {
    return blocks.iterator();
  }
}
