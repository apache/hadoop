/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 * This class maintains the map of the commitIndexes to be watched for
 * successful replication in the datanodes in a given pipeline. It also releases
 * the buffers associated with the user data back to {@Link BufferPool} once
 * minimum replication criteria is achieved during an ozone key write.
 */
package org.apache.hadoop.hdds.scm.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * This class executes watchForCommit on ratis pipeline and releases
 * buffers once data successfully gets replicated.
 */
public class CommitWatcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(CommitWatcher.class);

  // A reference to the pool of buffers holding the data
  private BufferPool bufferPool;

  // The map should maintain the keys (logIndexes) in order so that while
  // removing we always end up updating incremented data flushed length.
  // Also, corresponding to the logIndex, the corresponding list of buffers will
  // be released from the buffer pool.
  private ConcurrentSkipListMap<Long, List<ByteBuffer>>
      commitIndex2flushedDataMap;

  // future Map to hold up all putBlock futures
  private ConcurrentHashMap<Long,
      CompletableFuture<ContainerProtos.ContainerCommandResponseProto>>
      futureMap;

  private XceiverClientSpi xceiverClient;

  private final long watchTimeout;

  // total data which has been successfully flushed and acknowledged
  // by all servers
  private long totalAckDataLength;

  public CommitWatcher(BufferPool bufferPool, XceiverClientSpi xceiverClient,
      long watchTimeout) {
    this.bufferPool = bufferPool;
    this.xceiverClient = xceiverClient;
    this.watchTimeout = watchTimeout;
    commitIndex2flushedDataMap = new ConcurrentSkipListMap<>();
    totalAckDataLength = 0;
    futureMap = new ConcurrentHashMap<>();
  }

  /**
   * just update the totalAckDataLength. In case of failure,
   * we will read the data starting from totalAckDataLength.
   */
  private long releaseBuffers(List<Long> indexes) {
    Preconditions.checkArgument(!commitIndex2flushedDataMap.isEmpty());
    for (long index : indexes) {
      Preconditions.checkState(commitIndex2flushedDataMap.containsKey(index));
      List<ByteBuffer> buffers = commitIndex2flushedDataMap.remove(index);
      long length = buffers.stream().mapToLong(value -> {
        int pos = value.position();
        return pos;
      }).sum();
      totalAckDataLength += length;
      // clear the future object from the future Map
      Preconditions.checkNotNull(futureMap.remove(totalAckDataLength));
      for (ByteBuffer byteBuffer : buffers) {
        bufferPool.releaseBuffer(byteBuffer);
      }
    }
    return totalAckDataLength;
  }

  public void updateCommitInfoMap(long index, List<ByteBuffer> byteBufferList) {
    commitIndex2flushedDataMap
        .put(index, byteBufferList);
  }

  int getCommitInfoMapSize() {
    return commitIndex2flushedDataMap.size();
  }

  /**
   * Calls watch for commit for the first index in commitIndex2flushedDataMap to
   * the Ratis client.
   * @return reply reply from raft client
   * @throws IOException in case watchForCommit fails
   */
  public XceiverClientReply watchOnFirstIndex() throws IOException {
    if (!commitIndex2flushedDataMap.isEmpty()) {
      // wait for the  first commit index in the commitIndex2flushedDataMap
      // to get committed to all or majority of nodes in case timeout
      // happens.
      long index =
          commitIndex2flushedDataMap.keySet().stream().mapToLong(v -> v).min()
              .getAsLong();
      LOG.debug("waiting for first index " + index + " to catch up");
      return watchForCommit(index);
    } else {
      return null;
    }
  }

  /**
   * Calls watch for commit for the first index in commitIndex2flushedDataMap to
   * the Ratis client.
   * @return reply reply from raft client
   * @throws IOException in case watchForCommit fails
   */
  public XceiverClientReply watchOnLastIndex()
      throws IOException {
    if (!commitIndex2flushedDataMap.isEmpty()) {
      // wait for the  commit index in the commitIndex2flushedDataMap
      // to get committed to all or majority of nodes in case timeout
      // happens.
      long index =
          commitIndex2flushedDataMap.keySet().stream().mapToLong(v -> v).max()
              .getAsLong();
      LOG.debug("waiting for last flush Index " + index + " to catch up");
      return watchForCommit(index);
    } else {
      return null;
    }
  }


  private void adjustBuffers(long commitIndex) {
    List<Long> keyList = commitIndex2flushedDataMap.keySet().stream()
        .filter(p -> p <= commitIndex).collect(Collectors.toList());
    if (keyList.isEmpty()) {
      return;
    } else {
      releaseBuffers(keyList);
    }
  }

  // It may happen that once the exception is encountered , we still might
  // have successfully flushed up to a certain index. Make sure the buffers
  // only contain data which have not been sufficiently replicated
  void releaseBuffersOnException() {
    adjustBuffers(xceiverClient.getReplicatedMinCommitIndex());
  }


  /**
   * calls watchForCommit API of the Ratis Client. For Standalone client,
   * it is a no op.
   * @param commitIndex log index to watch for
   * @return minimum commit index replicated to all nodes
   * @throws IOException IOException in case watch gets timed out
   */
  public XceiverClientReply watchForCommit(long commitIndex)
      throws IOException {
    Preconditions.checkState(!commitIndex2flushedDataMap.isEmpty());
    long index;
    try {
      XceiverClientReply reply =
          xceiverClient.watchForCommit(commitIndex, watchTimeout);
      if (reply == null) {
        index = 0;
      } else {
        index = reply.getLogIndex();
      }
      adjustBuffers(index);
      return reply;
    } catch (TimeoutException | InterruptedException | ExecutionException e) {
      LOG.warn("watchForCommit failed for index " + commitIndex, e);
      IOException ioException = new IOException(
          "Unexpected Storage Container Exception: " + e.toString(), e);
      releaseBuffersOnException();
      throw ioException;
    }
  }

  @VisibleForTesting
  public ConcurrentSkipListMap<Long,
      List<ByteBuffer>> getCommitIndex2flushedDataMap() {
    return commitIndex2flushedDataMap;
  }

  public ConcurrentHashMap<Long,
      CompletableFuture<ContainerProtos.
          ContainerCommandResponseProto>> getFutureMap() {
    return futureMap;
  }

  public long getTotalAckDataLength() {
    return totalAckDataLength;
  }

  public void cleanup() {
    if (commitIndex2flushedDataMap != null) {
      commitIndex2flushedDataMap.clear();
    }
    if (futureMap != null) {
      futureMap.clear();
    }
    commitIndex2flushedDataMap = null;
  }
}
