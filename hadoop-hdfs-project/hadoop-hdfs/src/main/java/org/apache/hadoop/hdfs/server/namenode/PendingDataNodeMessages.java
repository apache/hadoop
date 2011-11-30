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

import java.util.PriorityQueue;

import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;

public class PendingDataNodeMessages {
  
  PriorityQueue<DataNodeMessage> queue = new PriorityQueue<DataNodeMessage>();
  
  enum MessageType {
    BLOCK_RECEIVED_DELETE,
    BLOCK_REPORT,
    COMMIT_BLOCK_SYNCHRONIZATION
  }
  
  static abstract class DataNodeMessage 
     implements Comparable<DataNodeMessage> {
    
    final MessageType type;
    private final long targetGs;
    
    DataNodeMessage(MessageType type, long targetGenStamp) {
      this.type = type;
      this.targetGs = targetGenStamp;
    }
    
    protected MessageType getType() {
      return type;
    }
    
    protected long getTargetGs() {
      return targetGs;
    }
    
    public int compareTo(DataNodeMessage other) {
      if (targetGs == other.targetGs) {
        return 0;
      } else if (targetGs < other.targetGs) {
        return -1;
      }
      return 1;
    }
  }
  
  static class BlockReceivedDeleteMessage extends DataNodeMessage {
    final DatanodeRegistration nodeReg;
    final String poolId;
    final ReceivedDeletedBlockInfo[] receivedAndDeletedBlocks;
    
    BlockReceivedDeleteMessage(DatanodeRegistration nodeReg, String poolId,
      ReceivedDeletedBlockInfo[] receivedAndDeletedBlocks, long targetGs) {
      super(MessageType.BLOCK_RECEIVED_DELETE, targetGs);
      this.nodeReg = nodeReg;
      this.poolId = poolId;
      this.receivedAndDeletedBlocks = receivedAndDeletedBlocks;
    }
    
    DatanodeRegistration getNodeReg() {
      return nodeReg;
    }
    
    String getPoolId() {
      return poolId;
    }
    
    ReceivedDeletedBlockInfo[] getReceivedAndDeletedBlocks() {
      return receivedAndDeletedBlocks;
    }
    
    public String toString() {
      return "BlockReceivedDeletedMessage with " +
        receivedAndDeletedBlocks.length + " blocks";
    }
  }
  
  static class CommitBlockSynchronizationMessage extends DataNodeMessage {

    private final ExtendedBlock block;
    private final long newgenerationstamp;
    private final long newlength;
    private final boolean closeFile;
    private final boolean deleteblock;
    private final DatanodeID[] newtargets;

    CommitBlockSynchronizationMessage(ExtendedBlock block,
        long newgenerationstamp, long newlength, boolean closeFile,
        boolean deleteblock, DatanodeID[] newtargets, long targetGenStamp) {
      super(MessageType.COMMIT_BLOCK_SYNCHRONIZATION, targetGenStamp);
      this.block = block;
      this.newgenerationstamp = newgenerationstamp;
      this.newlength = newlength;
      this.closeFile = closeFile;
      this.deleteblock = deleteblock;
      this.newtargets = newtargets;
    }

    ExtendedBlock getBlock() {
      return block;
    }

    long getNewgenerationstamp() {
      return newgenerationstamp;
    }

    long getNewlength() {
      return newlength;
    }

    boolean isCloseFile() {
      return closeFile;
    }

    boolean isDeleteblock() {
      return deleteblock;
    }

    DatanodeID[] getNewtargets() {
      return newtargets;
    }
    
    public String toString() {
      return "CommitBlockSynchronizationMessage for " + block;
    }
  }
  
  static class BlockReportMessage extends DataNodeMessage {

    private final DatanodeRegistration nodeReg;
    private final String poolId;
    private final BlockListAsLongs blockList;

    BlockReportMessage(DatanodeRegistration nodeReg, String poolId,
        BlockListAsLongs blist, long targetGenStamp) {
      super(MessageType.BLOCK_REPORT, targetGenStamp);
      this.nodeReg = nodeReg;
      this.poolId = poolId;
      this.blockList = blist;
    }

    DatanodeRegistration getNodeReg() {
      return nodeReg;
    }

    String getPoolId() {
      return poolId;
    }

    BlockListAsLongs getBlockList() {
      return blockList;
    }

    public String toString() {
      return "BlockReport from " + nodeReg + " with " + blockList.getNumberOfBlocks() + " blocks";
    }
  }

  synchronized void queueMessage(DataNodeMessage msg) {
    queue.add(msg);
  }
  
  /**
   * Returns a message if contains a message less or equal to the given gs,
   * otherwise returns null.
   * 
   * @param gs
   */
  synchronized DataNodeMessage take(long gs) {
    DataNodeMessage m = queue.peek();
    if (m != null && m.getTargetGs() < gs) {
      return queue.remove();
    } else {
      return null;
    }
  }
  
  synchronized boolean isEmpty() {
    return queue.isEmpty();
  }
}
