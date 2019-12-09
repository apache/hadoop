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
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSStripedOutputStream.Coordinator;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
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
@InterfaceAudience.Private
public class StripedDataStreamer extends DataStreamer {
  private final Coordinator coordinator;
  private final int index;

  StripedDataStreamer(HdfsFileStatus stat,
                      DFSClient dfsClient, String src,
                      Progressable progress, DataChecksum checksum,
                      AtomicReference<CachingStrategy> cachingStrategy,
                      ByteArrayManager byteArrayManage, String[] favoredNodes,
                      short index, Coordinator coordinator,
                      final EnumSet<AddBlockFlag> flags) {
    super(stat, null, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, favoredNodes, flags);
    this.index = index;
    this.coordinator = coordinator;
  }

  int getIndex() {
    return index;
  }

  boolean isHealthy() {
    return !streamerClosed() && !getErrorState().hasInternalError();
  }

  @Override
  protected void endBlock() {
    coordinator.offerEndBlock(index, block.getCurrentBlock());
    super.endBlock();
  }

  /**
   * The upper level DFSStripedOutputStream will allocate the new block group.
   * All the striped data streamer only needs to fetch from the queue, which
   * should be already be ready.
   */
  private LocatedBlock getFollowingBlock() throws IOException {
    if (!this.isHealthy()) {
      // No internal block for this streamer, maybe no enough healthy DN.
      // Throw the exception which has been set by the StripedOutputStream.
      this.getLastException().check(false);
    }
    return coordinator.getFollowingBlocks().poll(index);
  }

  @Override
  protected LocatedBlock nextBlockOutputStream() throws IOException {
    boolean success;
    LocatedBlock lb = getFollowingBlock();
    block.setCurrentBlock(lb.getBlock());
    block.setNumBytes(0);
    bytesSent = 0;
    accessToken = lb.getBlockToken();

    DatanodeInfo[] nodes = lb.getLocations();
    StorageType[] storageTypes = lb.getStorageTypes();
    String[] storageIDs = lb.getStorageIDs();

    // Connect to the DataNode. If fail the internal error state will be set.
    success = createBlockOutputStream(nodes, storageTypes, storageIDs, 0L,
        false);

    if (!success) {
      block.setCurrentBlock(null);
      final DatanodeInfo badNode = nodes[getErrorState().getBadNodeIndex()];
      LOG.warn("Excluding datanode " + badNode);
      excludedNodes.put(badNode, badNode);
      throw new IOException("Unable to create new block." + this);
    }
    return lb;
  }

  @VisibleForTesting
  LocatedBlock peekFollowingBlock() {
    return coordinator.getFollowingBlocks().peek(index);
  }

  @Override
  protected void setupPipelineInternal(DatanodeInfo[] nodes,
      StorageType[] nodeStorageTypes, String[] nodeStorageIDs)
      throws IOException {
    boolean success = false;
    while (!success && !streamerClosed() && dfsClient.clientRunning) {
      if (!handleRestartingDatanode()) {
        return;
      }
      if (!handleBadDatanode()) {
        // for striped streamer if it is datanode error then close the stream
        // and return. no need to replace datanode
        return;
      }

      // get a new generation stamp and an access token
      final LocatedBlock lb = coordinator.getNewBlocks().take(index);
      long newGS = lb.getBlock().getGenerationStamp();
      setAccessToken(lb.getBlockToken());

      // set up the pipeline again with the remaining nodes. when a striped
      // data streamer comes here, it must be in external error state.
      assert getErrorState().hasExternalError();
      success = createBlockOutputStream(nodes, nodeStorageTypes,
          nodeStorageIDs, newGS, true);

      failPacket4Testing();
      getErrorState().checkRestartingNodeDeadline(nodes);

      // notify coordinator the result of createBlockOutputStream
      synchronized (coordinator) {
        if (!streamerClosed()) {
          coordinator.updateStreamer(this, success);
          coordinator.notify();
        } else {
          success = false;
        }
      }

      if (success) {
        // wait for results of other streamers
        success = coordinator.takeStreamerUpdateResult(index);
        if (success) {
          // if all succeeded, update its block using the new GS
          updateBlockGS(newGS);
        } else {
          // otherwise close the block stream and restart the recovery process
          closeStream();
        }
      } else {
        // if fail, close the stream. The internal error state and last
        // exception have already been set in createBlockOutputStream
        // TODO: wait for restarting DataNodes during RollingUpgrade
        closeStream();
        setStreamerAsClosed();
      }
    } // while
  }

  void setExternalError() {
    getErrorState().setExternalError();
    synchronized (dataQueue) {
      dataQueue.notifyAll();
    }
  }

  @Override
  public String toString() {
    return "#" + index + ": " + (!isHealthy() ? "failed, ": "") + super.toString();
  }
}
