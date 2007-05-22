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

import org.apache.hadoop.io.UTF8;
import java.io.*;
import java.util.*;

/**
 * Information about the file while it is being written to.
 * Note that at that time the file is not visible to the outside.
 * 
 * This class contains a <code>Collection</code> of blocks that has
 * been written into the file so far, and file replication. 
 * 
 * @author shv
 */
class FileUnderConstruction {
  private short blockReplication; // file replication
  private long blockSize;
  private Collection<Block> blocks;
  private UTF8 clientName;         // lease holder
  private UTF8 clientMachine;
  private DatanodeDescriptor clientNode; // if client is a cluster node too.
    
  FileUnderConstruction(short replication,
                        long blockSize,
                        UTF8 clientName,
                        UTF8 clientMachine,
                        DatanodeDescriptor clientNode) throws IOException {
    this.blockReplication = replication;
    this.blockSize = blockSize;
    this.blocks = new ArrayList<Block>();
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
  }
    
  public short getReplication() {
    return this.blockReplication;
  }
    
  public long getBlockSize() {
    return blockSize;
  }
    
  public Collection<Block> getBlocks() {
    return blocks;
  }
    
  public UTF8 getClientName() {
    return clientName;
  }
    
  public UTF8 getClientMachine() {
    return clientMachine;
  }

  public DatanodeDescriptor getClientNode() {
    return clientNode;
  }

  /**
   * Return the penultimate allocated block for this file.
   */
  public Block getPenultimateBlock() {
    if (blocks.size() <= 1) {
      return null;
    }
    return ((ArrayList<Block>)blocks).get(blocks.size() - 2);
  }
  
  long computeFileLength() {
    long total = 0;
    for (Block blk : blocks) {
      total += blk.getNumBytes();
    }
    return total;
  }
}
