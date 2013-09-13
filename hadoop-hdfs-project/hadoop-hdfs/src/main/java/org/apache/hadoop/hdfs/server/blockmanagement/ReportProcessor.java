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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportIterator;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import com.google.common.base.Preconditions;

/**
 * Handles common operations of processing a block report from a datanode,
 * generating a diff of updates to the BlocksMap, and then feeding the diff
 * to the subclass-implemented hooks.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public abstract class ReportProcessor {

  static final Log blockLog = NameNode.blockStateChangeLog;
  private final String className = getClass().getSimpleName();
  // Max number of blocks to log info about during a block report.
  final long maxNumBlocksToLog;

  void blockLogDebug(String message) {
    if (blockLog.isDebugEnabled()) {
      blockLog.info("BLOCK* " + className + message);
    }
  }

  void blockLogInfo(String message) {
    if (blockLog.isInfoEnabled()) {
      blockLog.info("BLOCK* " + className + message);
    }
  }

  void blockLogWarn(String message) {
    blockLog.warn("BLOCK* " + className + message);
  }

  void logAddStoredBlock(BlockInfo storedBlock, DatanodeDescriptor node) {
    if (!blockLog.isInfoEnabled()) {
      return;
    }
    StringBuilder sb = new StringBuilder(500);
    sb.append("BLOCK* " + className + "#addStoredBlock: blockMap updated: ")
      .append(node)
      .append(" is added to ");
    storedBlock.appendStringTo(sb);
    sb.append(" size " )
      .append(storedBlock.getNumBytes());
    blockLog.info(sb);
  }

  public ReportProcessor(Configuration conf) {
    this.maxNumBlocksToLog = conf.getLong(
        DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_KEY,
        DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT);
  }

  /**
   * Processes a block report from a datanode, updating the block to
   * datanode mapping, adding new blocks and removing invalid ones.
   * Also computes and queues new replication and invalidation work.
   * @param node Datanode sending the block report
   * @param report as list of longs
   * @throws IOException
   */
  final void processReport(final DatanodeDescriptor node,
      final BlockListAsLongs report) throws IOException {
    // Normal case:
    // Modify the (block-->datanode) map, according to the difference
    // between the old and new block report.
    //
    Collection<BlockInfo> toAdd = new LinkedList<BlockInfo>();
    Collection<Block> toRemove = new LinkedList<Block>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    Collection<BlockToMarkCorrupt> toCorrupt = new LinkedList<BlockToMarkCorrupt>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<StatefulBlockInfo>();
    reportDiff(node, report, toAdd, toRemove, toInvalidate, toCorrupt, toUC);

    // Process the blocks on each queue
    for (StatefulBlockInfo b : toUC) {
      addStoredBlockUnderConstruction(b.storedBlock, node, b.reportedState);
    }
    for (Block b : toRemove) {
      removeStoredBlock(b, node);
    }
    int numBlocksLogged = 0;
    for (BlockInfo b : toAdd) {
      addStoredBlock(b, node, null, numBlocksLogged < maxNumBlocksToLog);
      numBlocksLogged++;
    }

    if (numBlocksLogged > maxNumBlocksToLog) {
      blockLogInfo("#processReport: logged"
          + " info for " + maxNumBlocksToLog
          + " of " + numBlocksLogged + " reported.");
    }
    for (Block b : toInvalidate) {
      blockLogInfo("#processReport: "
          + b + " on " + node + " size " + b.getNumBytes()
          + " does not belong to any file");
      addToInvalidates(b, node);
    }
    for (BlockToMarkCorrupt b : toCorrupt) {
      markBlockAsCorrupt(b, node);
    }
  }

  /**
   * Compute the difference between the current state of the datanode in the
   * BlocksMap and the new reported state, categorizing changes into
   * different groups (e.g. new blocks to be added, blocks that were removed,
   * blocks that should be invalidated, etc.).
   */
  private void reportDiff(DatanodeDescriptor dn,
      BlockListAsLongs newReport,
      Collection<BlockInfo> toAdd,              // add to DatanodeDescriptor
      Collection<Block> toRemove,           // remove from DatanodeDescriptor
      Collection<Block> toInvalidate,       // should be removed from DN
      Collection<BlockToMarkCorrupt> toCorrupt, // add to corrupt replicas list
      Collection<StatefulBlockInfo> toUC) { // add to under-construction list
    // place a delimiter in the list which separates blocks
    // that have been reported from those that have not
    BlockInfo delimiter = new BlockInfo(new Block(), 1);
    boolean added = addBlock(dn, delimiter);
    assert added : "Delimiting block cannot be present in the node";
    int headIndex = 0; //currently the delimiter is in the head of the list
    int curIndex;

    if (newReport == null) {
      newReport = new BlockListAsLongs();
    }
    // scan the report and process newly reported blocks
    BlockReportIterator itBR = newReport.getBlockReportIterator();
    while (itBR.hasNext()) {
      Block iblk = itBR.next();
      ReplicaState iState = itBR.getCurrentReplicaState();
      BlockInfo storedBlock = processReportedBlock(dn, iblk, iState,
                                  toAdd, toInvalidate, toCorrupt, toUC);
      // move block to the head of the list
      if (storedBlock != null && (curIndex = storedBlock.findDatanode(dn)) >= 0) {
        headIndex = moveBlockToHead(dn, storedBlock, curIndex, headIndex);
      }
    }
    // collect blocks that have not been reported
    // all of them are next to the delimiter
    Iterator<? extends Block> it = new DatanodeDescriptor.BlockIterator(
        delimiter.getNext(0), dn);
    while (it.hasNext()) {
      toRemove.add(it.next());
    }
    removeBlock(dn, delimiter);
  }

  // Operations on the blocks on a datanode

  abstract int moveBlockToHead(DatanodeDescriptor dn, BlockInfo storedBlock,
      int curIndex, int headIndex);

  abstract boolean addBlock(DatanodeDescriptor dn, BlockInfo block);

  abstract boolean removeBlock(DatanodeDescriptor dn, BlockInfo block);

  // Cache report processing

  abstract BlockInfo processReportedBlock(DatanodeDescriptor dn, Block iblk,
      ReplicaState iState, Collection<BlockInfo> toAdd,
      Collection<Block> toInvalidate, Collection<BlockToMarkCorrupt> toCorrupt,
      Collection<StatefulBlockInfo> toUC);

  // Hooks for processing the cache report diff

  abstract Block addStoredBlock(final BlockInfo block,
      DatanodeDescriptor node, DatanodeDescriptor delNodeHint,
      boolean logEveryBlock) throws IOException;

  abstract void removeStoredBlock(Block block, DatanodeDescriptor node);

  abstract void markBlockAsCorrupt(BlockToMarkCorrupt b, DatanodeInfo dn)
      throws IOException;

  abstract void addToInvalidates(final Block b, final DatanodeInfo node);

  abstract void addStoredBlockUnderConstruction(
      BlockInfoUnderConstruction storedBlock, DatanodeDescriptor node,
      ReplicaState reportedState) throws IOException;

  /**
   * BlockToMarkCorrupt is used to build the "toCorrupt" list, which is a
   * list of blocks that should be considered corrupt due to a block report.
   */
  static class BlockToMarkCorrupt {
    /** The corrupted block in a datanode. */
    final BlockInfo corrupted;
    /** The corresponding block stored in the BlockManager. */
    final BlockInfo stored;
    /** The reason to mark corrupt. */
    final String reason;

    BlockToMarkCorrupt(BlockInfo corrupted, BlockInfo stored, String reason) {
      Preconditions.checkNotNull(corrupted, "corrupted is null");
      Preconditions.checkNotNull(stored, "stored is null");

      this.corrupted = corrupted;
      this.stored = stored;
      this.reason = reason;
    }

    BlockToMarkCorrupt(BlockInfo stored, String reason) {
      this(stored, stored, reason);
    }

    BlockToMarkCorrupt(BlockInfo stored, long gs, String reason) {
      this(new BlockInfo(stored), stored, reason);
      //the corrupted block in datanode has a different generation stamp
      corrupted.setGenerationStamp(gs);
    }

    @Override
    public String toString() {
      return corrupted + "("
          + (corrupted == stored? "same as stored": "stored=" + stored) + ")";
    }
  }

  /**
   * StatefulBlockInfo is used to build the "toUC" list, which is a list of
   * updates to the information about under-construction blocks.
   * Besides the block in question, it provides the ReplicaState
   * reported by the datanode in the block report.
   */
  static class StatefulBlockInfo {
    final BlockInfoUnderConstruction storedBlock;
    final ReplicaState reportedState;

    StatefulBlockInfo(BlockInfoUnderConstruction storedBlock,
        ReplicaState reportedState) {
      this.storedBlock = storedBlock;
      this.reportedState = reportedState;
    }
  }

}
