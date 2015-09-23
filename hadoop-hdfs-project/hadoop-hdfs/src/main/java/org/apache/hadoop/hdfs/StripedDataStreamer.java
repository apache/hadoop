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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdfs.DFSStripedOutputStream.Coordinator;
import org.apache.hadoop.hdfs.DFSStripedOutputStream.MultipleBlockingQueue;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class extends {@link DataStreamer} to support writing striped blocks
 * to datanodes.
 * A {@link DFSStripedOutputStream} has multiple {@link StripedDataStreamer}s.
 * Whenever the streamers need to talk the namenode, only the fastest streamer
 * sends an rpc call to the namenode and then populates the result for the
 * other streamers.
 */
public class StripedDataStreamer extends DataStreamer {
  /**
   * This class is designed for multiple threads to share a
   * {@link MultipleBlockingQueue}. Initially, the queue is empty. The earliest
   * thread calling poll populates entries to the queue and the other threads
   * will wait for it. Once the entries are populated, all the threads can poll
   * their entries.
   *
   * @param <T> the queue entry type.
   */
  static abstract class ConcurrentPoll<T> {
    final MultipleBlockingQueue<T> queue;

    ConcurrentPoll(MultipleBlockingQueue<T> queue) {
      this.queue = queue;
    }

    T poll(final int i) throws IOException {
      for(;;) {
        synchronized(queue) {
          final T polled = queue.poll(i);
          if (polled != null) { // already populated; return polled item.
            return polled;
          }
          if (isReady2Populate()) {
            try {
              populate();
              return queue.poll(i);
            } catch(IOException ioe) {
              LOG.warn("Failed to populate, " + this, ioe);
              throw ioe;
            }
          }
        }

        // sleep and then retry.
        sleep(100, "poll");
      }
    }

    boolean isReady2Populate() {
      return queue.isEmpty();
    }

    abstract void populate() throws IOException;
  }

  private static void sleep(long ms, String op) throws InterruptedIOException {
    try {
      Thread.sleep(ms);
    } catch(InterruptedException ie) {
      throw DFSUtil.toInterruptedIOException(
          "Sleep interrupted during " + op, ie);
    }
  }

  private final Coordinator coordinator;
  private final int index;
  private volatile boolean failed;
  private final ECSchema schema;
  private final int cellSize;

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
    this.schema = stat.getErasureCodingPolicy().getSchema();
    this.cellSize = stat.getErasureCodingPolicy().getCellSize();
  }

  int getIndex() {
    return index;
  }

  void setFailed(boolean failed) {
    this.failed = failed;
  }

  boolean isFailed() {
    return failed;
  }

  private boolean isParityStreamer() {
    return index >= schema.getNumDataUnits();
  }

  @Override
  protected void endBlock() {
    if (!isParityStreamer()) {
      coordinator.offerEndBlock(index, block);
    }
    super.endBlock();
  }

  @Override
  int getNumBlockWriteRetry() {
    return 0;
  }

  @Override
  LocatedBlock locateFollowingBlock(final DatanodeInfo[] excludedNodes)
      throws IOException {
    return new ConcurrentPoll<LocatedBlock>(coordinator.getFollowingBlocks()) {
      @Override
      boolean isReady2Populate() {
        return super.isReady2Populate()
            && (block == null || coordinator.hasAllEndBlocks());
      }

      @Override
      void populate() throws IOException {
        getLastException().check(false);

        if (block != null) {
          // set numByte for the previous block group
          long bytes = 0;
          for (int i = 0; i < schema.getNumDataUnits(); i++) {
            final ExtendedBlock b = coordinator.takeEndBlock(i);
            StripedBlockUtil.checkBlocks(index, block, i, b);
            bytes += b.getNumBytes();
          }
          block.setNumBytes(bytes);
          block.setBlockId(block.getBlockId() - index);
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("locateFollowingBlock: index=" + index + ", block=" + block);
        }

        final LocatedBlock lb = StripedDataStreamer.super.locateFollowingBlock(
            excludedNodes);
        if (lb.getLocations().length < schema.getNumDataUnits()) {
          throw new IOException(
              "Failed to get datablocks number of nodes from namenode: blockGroupSize= "
                  + (schema.getNumDataUnits() + schema.getNumParityUnits())
                  + ", blocks.length= " + lb.getLocations().length);
        }
        final LocatedBlock[] blocks =
            StripedBlockUtil.parseStripedBlockGroup((LocatedStripedBlock) lb,
                cellSize, schema.getNumDataUnits(), schema.getNumParityUnits());

        for (int i = 0; i < blocks.length; i++) {
          StripedDataStreamer si = coordinator.getStripedDataStreamer(i);
          if (si.isFailed()) {
            continue; // skipping failed data streamer
          }
          if (blocks[i] == null) {
            // Set exception and close streamer as there is no block locations
            // found for the parity block.
            LOG.warn("Failed to get block location for parity block, index="
                + i);
            si.getLastException().set(
                new IOException("Failed to get following block, i=" + i));
            si.setFailed(true);
            si.endBlock();
            si.close(true);
          } else {
            queue.offer(i, blocks[i]);
          }
        }
      }
    }.poll(index);
  }

  @VisibleForTesting
  LocatedBlock peekFollowingBlock() {
    return coordinator.getFollowingBlocks().peek(index);
  }

  @Override
  LocatedBlock updateBlockForPipeline() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("updateBlockForPipeline(), " + this);
    }
    return new ConcurrentPoll<LocatedBlock>(coordinator.getNewBlocks()) {
      @Override
      void populate() throws IOException {
        final ExtendedBlock bg = coordinator.getBlockGroup();
        final LocatedBlock updated = callUpdateBlockForPipeline(bg);
        final long newGS = updated.getBlock().getGenerationStamp();
        final LocatedBlock[] updatedBlks = StripedBlockUtil
            .parseStripedBlockGroup((LocatedStripedBlock) updated, cellSize,
                schema.getNumDataUnits(), schema.getNumParityUnits());
        for (int i = 0; i < schema.getNumDataUnits()
            + schema.getNumParityUnits(); i++) {
          StripedDataStreamer si = coordinator.getStripedDataStreamer(i);
          if (si.isFailed()) {
            continue; // skipping failed data streamer
          }
          final ExtendedBlock bi = si.getBlock();
          if (bi != null) {
            final LocatedBlock lb = new LocatedBlock(newBlock(bi, newGS),
                null, null, null, -1, updated.isCorrupt(), null);
            lb.setBlockToken(updatedBlks[i].getBlockToken());
            queue.offer(i, lb);
          } else {
            final MultipleBlockingQueue<LocatedBlock> followingBlocks
                = coordinator.getFollowingBlocks();
            synchronized(followingBlocks) {
              final LocatedBlock lb = followingBlocks.peek(i);
              if (lb != null) {
                lb.getBlock().setGenerationStamp(newGS);
                si.getErrorState().reset();
                continue;
              }
            }

            //streamer i just have polled the block, sleep and retry.
            sleep(100, "updateBlockForPipeline, " + this);
            i--;
          }
        }
      }
    }.poll(index);
  }

  @Override
  ExtendedBlock updatePipeline(final long newGS) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("updatePipeline(newGS=" + newGS + "), " + this);
    }
    return new ConcurrentPoll<ExtendedBlock>(coordinator.getUpdateBlocks()) {
      @Override
      void populate() throws IOException {
        final MultipleBlockingQueue<LocatedBlock> followingBlocks
            = coordinator.getFollowingBlocks();
        final ExtendedBlock bg = coordinator.getBlockGroup();
        final ExtendedBlock newBG = newBlock(bg, newGS);

        final int n = schema.getNumDataUnits() + schema.getNumParityUnits();
        final DatanodeInfo[] newNodes = new DatanodeInfo[n];
        final String[] newStorageIDs = new String[n];
        for (int i = 0; i < n; i++) {
          final StripedDataStreamer si = coordinator.getStripedDataStreamer(i);
          DatanodeInfo[] nodes = si.getNodes();
          String[] storageIDs = si.getStorageIDs();
          if (nodes == null || storageIDs == null) {
            synchronized(followingBlocks) {
              final LocatedBlock lb = followingBlocks.peek(i);
              if (lb != null) {
                nodes = lb.getLocations();
                storageIDs = lb.getStorageIDs();
              }
            }
          }
          if (nodes != null && storageIDs != null) {
            newNodes[i] = nodes[0];
            newStorageIDs[i] = storageIDs[0];
          } else {
            //streamer i just have polled the block, sleep and retry.
            sleep(100, "updatePipeline, " + this);
            i--;
          }
        }
        final ExtendedBlock updated = callUpdatePipeline(bg, newBG, newNodes,
            newStorageIDs);

        for (int i = 0; i < n; i++) {
          final StripedDataStreamer si = coordinator.getStripedDataStreamer(i);
          final ExtendedBlock bi = si.getBlock();
          if (bi != null) {
            queue.offer(i, newBlock(bi, updated.getGenerationStamp()));
          } else if (!si.isFailed()) {
            synchronized(followingBlocks) {
              final LocatedBlock lb = followingBlocks.peek(i);
              if (lb != null) {
                lb.getBlock().setGenerationStamp(newGS);
                si.getErrorState().reset();
                continue;
              }
            }

            //streamer i just have polled the block, sleep and retry.
            sleep(100, "updatePipeline, " + this);
            i--;
          }
        }
      }
    }.poll(index);
  }

  @Override
  public String toString() {
    return "#" + index + ": " + (failed? "failed, ": "") + super.toString();
  }
}
