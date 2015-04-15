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

import java.util.List;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/****************************************************************************
 * The StripedDataStreamer class is used by {@link DFSStripedOutputStream}.
 * There are two kinds of StripedDataStreamer, leading streamer and ordinary
 * stream. Leading streamer requests a block group from NameNode, unwraps
 * it to located blocks and transfers each located block to its corresponding
 * ordinary streamer via a blocking queue.
 *
 ****************************************************************************/
public class StripedDataStreamer extends DataStreamer {
  private final short index;
  private final  List<BlockingQueue<LocatedBlock>> stripedBlocks;
  private static short blockGroupSize = HdfsConstants.NUM_DATA_BLOCKS
      + HdfsConstants.NUM_PARITY_BLOCKS;
  private boolean hasCommittedBlock = false;

  StripedDataStreamer(HdfsFileStatus stat, ExtendedBlock block,
                      DFSClient dfsClient, String src,
                      Progressable progress, DataChecksum checksum,
                      AtomicReference<CachingStrategy> cachingStrategy,
                      ByteArrayManager byteArrayManage, short index,
                      List<BlockingQueue<LocatedBlock>> stripedBlocks) {
    super(stat,block, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage);
    this.index = index;
    this.stripedBlocks = stripedBlocks;
  }

  /**
   * Construct a data streamer for appending to the last partial block
   * @param lastBlock last block of the file to be appended
   * @param stat status of the file to be appended
   * @throws IOException if error occurs
   */
  StripedDataStreamer(LocatedBlock lastBlock, HdfsFileStatus stat,
                      DFSClient dfsClient, String src,
                      Progressable progress, DataChecksum checksum,
                      AtomicReference<CachingStrategy> cachingStrategy,
                      ByteArrayManager byteArrayManage, short index,
                      List<BlockingQueue<LocatedBlock>> stripedBlocks)
      throws IOException {
    super(lastBlock, stat, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage);
    this.index = index;
    this.stripedBlocks = stripedBlocks;
  }

  public boolean isLeadingStreamer () {
    return index == 0;
  }

  private boolean isParityStreamer() {
    return index >= HdfsConstants.NUM_DATA_BLOCKS;
  }

  @Override
  protected void endBlock() {
    if (!isLeadingStreamer() && !isParityStreamer()) {
      //before retrieving a new block, transfer the finished block to
      //leading streamer
      LocatedBlock finishedBlock = new LocatedBlock(
          new ExtendedBlock(block.getBlockPoolId(), block.getBlockId(),
                       block.getNumBytes(),block.getGenerationStamp()), null);
      try{
        boolean offSuccess = stripedBlocks.get(0).offer(finishedBlock, 30,
            TimeUnit.SECONDS);
      }catch (InterruptedException ie) {
      //TODO: Handle InterruptedException (HDFS-7786)
      }
    }
    super.endBlock();
  }

  /**
   * This function is called after the streamer is closed.
   */
  void countTailingBlockGroupBytes () throws IOException {
    if (isLeadingStreamer()) {
      //when committing a block group, leading streamer has to adjust
      // {@link block} including the size of block group
      for (int i = 1; i < HdfsConstants.NUM_DATA_BLOCKS; i++) {
        try {
          LocatedBlock finishedLocatedBlock = stripedBlocks.get(0).poll(30,
              TimeUnit.SECONDS);
          if (finishedLocatedBlock == null) {
            throw new IOException("Fail to get finished LocatedBlock " +
                "from streamer, i=" + i);
          }
          ExtendedBlock finishedBlock = finishedLocatedBlock.getBlock();
          long bytes = finishedBlock == null ? 0 : finishedBlock.getNumBytes();
          if (block != null) {
            block.setNumBytes(block.getNumBytes() + bytes);
          }
        } catch (InterruptedException ie) {
          DFSClient.LOG.info("InterruptedException received when " +
              "putting a block to stripeBlocks, ie = " + ie);
        }
      }
    }
  }

  @Override
  protected LocatedBlock locateFollowingBlock(DatanodeInfo[] excludedNodes)
      throws IOException {
    LocatedBlock lb = null;
    if (isLeadingStreamer()) {
      if(hasCommittedBlock) {
        /**
         * when committing a block group, leading streamer has to adjust
         * {@link block} to include the size of block group
         */
        for (int i = 1; i < HdfsConstants.NUM_DATA_BLOCKS; i++) {
          try {
            LocatedBlock finishedLocatedBlock = stripedBlocks.get(0).poll(30,
                TimeUnit.SECONDS);
            if (finishedLocatedBlock == null) {
              throw new IOException("Fail to get finished LocatedBlock " +
                  "from streamer, i=" + i);
            }
            ExtendedBlock finishedBlock = finishedLocatedBlock.getBlock();
            long bytes = finishedBlock == null ? 0 : finishedBlock.getNumBytes();
            if(block != null) {
              block.setNumBytes(block.getNumBytes() + bytes);
            }
          } catch (InterruptedException ie) {
            DFSClient.LOG.info("InterruptedException received when putting" +
                " a block to stripeBlocks, ie = " + ie);
          }
        }
      }

      lb = super.locateFollowingBlock(excludedNodes);
      hasCommittedBlock = true;
      assert lb instanceof LocatedStripedBlock;
      DFSClient.LOG.debug("Leading streamer obtained bg " + lb);
      LocatedBlock[] blocks = StripedBlockUtil.
          parseStripedBlockGroup((LocatedStripedBlock) lb,
              HdfsConstants.BLOCK_STRIPED_CELL_SIZE, HdfsConstants.NUM_DATA_BLOCKS,
              HdfsConstants.NUM_PARITY_BLOCKS
          );
      assert blocks.length == blockGroupSize :
          "Fail to get block group from namenode: blockGroupSize: " +
              blockGroupSize + ", blocks.length: " + blocks.length;
      lb = blocks[0];
      for (int i = 1; i < blocks.length; i++) {
        try {
          boolean offSuccess = stripedBlocks.get(i).offer(blocks[i],
              90, TimeUnit.SECONDS);
          if(!offSuccess){
            String msg = "Fail to put block to stripeBlocks. i = " + i;
            DFSClient.LOG.info(msg);
            throw new IOException(msg);
          } else {
            DFSClient.LOG.info("Allocate a new block to a streamer. i = " + i
                + ", block: " + blocks[i]);
          }
        } catch (InterruptedException ie) {
          DFSClient.LOG.info("InterruptedException received when putting" +
              " a block to stripeBlocks, ie = " + ie);
        }
      }
    } else {
      try {
        //wait 90 seconds to get a block from the queue
        lb = stripedBlocks.get(index).poll(90, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        DFSClient.LOG.info("InterruptedException received when retrieving " +
            "a block from stripeBlocks, ie = " + ie);
      }
    }
    return lb;
  }
}
