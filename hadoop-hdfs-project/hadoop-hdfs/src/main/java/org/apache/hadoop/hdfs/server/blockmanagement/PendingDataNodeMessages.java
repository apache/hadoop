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

import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

/**
 * In the Standby Node, we can receive messages about blocks
 * before they are actually available in the namespace, or while
 * they have an outdated state in the namespace. In those cases,
 * we queue those block-related messages in this structure.
 * */  
class PendingDataNodeMessages {
  
  final Map<Block, Queue<ReportedBlockInfo>> queueByBlockId =
    Maps.newHashMap();
  private int count = 0;
  
    
  static class ReportedBlockInfo {
    private final Block block;
    private final DatanodeStorageInfo storageInfo;
    private final ReplicaState reportedState;

    ReportedBlockInfo(DatanodeStorageInfo storageInfo, Block block,
        ReplicaState reportedState) {
      this.storageInfo = storageInfo;
      this.block = block;
      this.reportedState = reportedState;
    }

    Block getBlock() {
      return block;
    }

    ReplicaState getReportedState() {
      return reportedState;
    }
    
    DatanodeStorageInfo getStorageInfo() {
      return storageInfo;
    }

    @Override
    public String toString() {
      return "ReportedBlockInfo [block=" + block + ", dn="
          + storageInfo.getDatanodeDescriptor()
          + ", reportedState=" + reportedState + "]";
    }
  }
  
  /**
   * Remove all pending DN messages which reference the given DN.
   * @param dn the datanode whose messages we should remove.
   */
  void removeAllMessagesForDatanode(DatanodeDescriptor dn) {
    for (Map.Entry<Block, Queue<ReportedBlockInfo>> entry :
        queueByBlockId.entrySet()) {
      Queue<ReportedBlockInfo> newQueue = Lists.newLinkedList();
      Queue<ReportedBlockInfo> oldQueue = entry.getValue();
      while (!oldQueue.isEmpty()) {
        ReportedBlockInfo rbi = oldQueue.remove();
        if (!rbi.getStorageInfo().getDatanodeDescriptor().equals(dn)) {
          newQueue.add(rbi);
        } else {
          count--;
        }
      }
      queueByBlockId.put(entry.getKey(), newQueue);
    }
  }
  
  void enqueueReportedBlock(DatanodeStorageInfo storageInfo, Block block,
      ReplicaState reportedState) {
    if (BlockIdManager.isStripedBlockID(block.getBlockId())) {
      Block blkId = new Block(BlockIdManager.convertToStripedID(block
          .getBlockId()));
      getBlockQueue(blkId).add(
          new ReportedBlockInfo(storageInfo, new Block(block), reportedState));
    } else {
      block = new Block(block);
      getBlockQueue(block).add(
          new ReportedBlockInfo(storageInfo, block, reportedState));
    }
    count++;
  }
  
  /**
   * @return any messages that were previously queued for the given block,
   * or null if no messages were queued.
   */
  Queue<ReportedBlockInfo> takeBlockQueue(Block block) {
    Queue<ReportedBlockInfo> queue = queueByBlockId.remove(block);
    if (queue != null) {
      count -= queue.size();
    }
    return queue;
  }


  private Queue<ReportedBlockInfo> getBlockQueue(Block block) {
    Queue<ReportedBlockInfo> queue = queueByBlockId.get(block);
    if (queue == null) {
      queue = Lists.newLinkedList();
      queueByBlockId.put(block, queue);
    }
    return queue;
  }
  
  int count() {
    return count ;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Block, Queue<ReportedBlockInfo>> entry :
      queueByBlockId.entrySet()) {
      sb.append("Block " + entry.getKey() + ":\n");
      for (ReportedBlockInfo rbi : entry.getValue()) {
        sb.append("  ").append(rbi).append("\n");
      }
    }
    return sb.toString();
  }

  Iterable<ReportedBlockInfo> takeAll() {
    List<ReportedBlockInfo> rbis = Lists.newArrayListWithCapacity(
        count);
    for (Queue<ReportedBlockInfo> q : queueByBlockId.values()) {
      rbis.addAll(q);
    }
    queueByBlockId.clear();
    count = 0;
    return rbis;
  }
}