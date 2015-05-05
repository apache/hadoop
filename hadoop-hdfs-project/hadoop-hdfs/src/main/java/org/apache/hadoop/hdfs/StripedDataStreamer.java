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

package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.NUM_DATA_BLOCKS;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.NUM_PARITY_BLOCKS;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hdfs.DFSStripedOutputStream.Coordinator;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

/****************************************************************************
 * The StripedDataStreamer class is used by {@link DFSStripedOutputStream}.
 * There are two kinds of StripedDataStreamer, leading streamer and ordinary
 * stream. Leading streamer requests a block group from NameNode, unwraps
 * it to located blocks and transfers each located block to its corresponding
 * ordinary streamer via a blocking queue.
 *
 ****************************************************************************/
public class StripedDataStreamer extends DataStreamer {
  private final Coordinator coordinator;
  private final int index;
  private volatile boolean isFailed;

  StripedDataStreamer(HdfsFileStatus stat,
                      DFSClient dfsClient, String src,
                      Progressable progress, DataChecksum checksum,
                      AtomicReference<CachingStrategy> cachingStrategy,
                      ByteArrayManager byteArrayManage, String[] favoredNodes,
                      short index, Coordinator coordinator) {
    super(stat, null, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, favoredNodes);
    this.index = index;
    this.coordinator = coordinator;
  }

  int getIndex() {
    return index;
  }

  void setIsFailed(boolean isFailed) {
    this.isFailed = isFailed;
  }

  boolean isFailed() {
    return isFailed;
  }

  public boolean isLeadingStreamer () {
    return index == 0;
  }

  private boolean isParityStreamer() {
    return index >= NUM_DATA_BLOCKS;
  }

  @Override
  protected void endBlock() {
    if (!isParityStreamer()) {
      coordinator.putEndBlock(index, block);
    }
    super.endBlock();
  }

  @Override
  protected LocatedBlock locateFollowingBlock(DatanodeInfo[] excludedNodes)
      throws IOException {
    if (isLeadingStreamer()) {
      if (coordinator.shouldLocateFollowingBlock()) {
        // set numByte for the previous block group
        long bytes = 0;
        for (int i = 0; i < NUM_DATA_BLOCKS; i++) {
          final ExtendedBlock b = coordinator.getEndBlock(i);
          bytes += b == null ? 0 : b.getNumBytes();
        }
        block.setNumBytes(bytes);
      }

      final LocatedStripedBlock lsb
          = (LocatedStripedBlock)super.locateFollowingBlock(excludedNodes);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Obtained block group " + lsb);
      }
      LocatedBlock[] blocks = StripedBlockUtil.parseStripedBlockGroup(lsb,
          BLOCK_STRIPED_CELL_SIZE, NUM_DATA_BLOCKS, NUM_PARITY_BLOCKS);

      assert blocks.length == (NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS) :
          "Fail to get block group from namenode: blockGroupSize: " +
              (NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS) + ", blocks.length: " +
              blocks.length;
      for (int i = 0; i < blocks.length; i++) {
        coordinator.putStripedBlock(i, blocks[i]);
      }
    }

    return coordinator.getStripedBlock(index);
  }

  @Override
  public String toString() {
    return "#" + index + ": isFailed? " + Boolean.toString(isFailed).charAt(0)
        + ", " + super.toString();
  }
}
