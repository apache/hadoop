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

package org.apache.hadoop.hdds.scm.storage;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientAsyncReply;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.client.BlockID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls
    .putBlockAsync;
import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls
    .writeChunkAsync;

/**
 * An {@link OutputStream} used by the REST service in combination with the
 * SCMClient to write the value of a key to a sequence
 * of container chunks.  Writes are buffered locally and periodically written to
 * the container as a new chunk.  In order to preserve the semantics that
 * replacement of a pre-existing key is atomic, each instance of the stream has
 * an internal unique identifier.  This unique identifier and a monotonically
 * increasing chunk index form a composite key that is used as the chunk name.
 * After all data is written, a putKey call creates or updates the corresponding
 * container key, and this call includes the full list of chunks that make up
 * the key data.  The list of chunks is updated all at once.  Therefore, a
 * concurrent reader never can see an intermediate state in which different
 * chunks of data from different versions of the key data are interleaved.
 * This class encapsulates all state management for buffering and writing
 * through to the container.
 */
public class ChunkOutputStream extends OutputStream {
  public static final Logger LOG =
      LoggerFactory.getLogger(ChunkOutputStream.class);

  private BlockID blockID;
  private final String key;
  private final String traceID;
  private final BlockData.Builder containerBlockData;
  private XceiverClientManager xceiverClientManager;
  private XceiverClientSpi xceiverClient;
  private final String streamId;
  private int chunkIndex;
  private int chunkSize;
  private final long streamBufferFlushSize;
  private final long streamBufferMaxSize;
  private final long watchTimeout;
  private ByteBuffer buffer;
  // The IOException will be set by response handling thread in case there is an
  // exception received in the response. If the exception is set, the next
  // request will fail upfront.
  private IOException ioException;
  private ExecutorService responseExecutor;

  // position of the buffer where the last flush was attempted
  private int lastFlushPos;

  // position of the buffer till which the flush was successfully
  // acknowledged by all nodes in pipeline
  private int lastSuccessfulFlushIndex;

  // list to hold up all putBlock futures
  private List<CompletableFuture<ContainerProtos.ContainerCommandResponseProto>>
      futureList;
  // list maintaining commit indexes for putBlocks
  private List<Long> commitIndexList;

  /**
   * Creates a new ChunkOutputStream.
   *
   * @param blockID              block ID
   * @param key                  chunk key
   * @param xceiverClientManager client manager that controls client
   * @param xceiverClient        client to perform container calls
   * @param traceID              container protocol call args
   * @param chunkSize            chunk size
   */
  public ChunkOutputStream(BlockID blockID, String key,
      XceiverClientManager xceiverClientManager, XceiverClientSpi xceiverClient,
      String traceID, int chunkSize, long streamBufferFlushSize,
      long streamBufferMaxSize, long watchTimeout, ByteBuffer buffer) {
    this.blockID = blockID;
    this.key = key;
    this.traceID = traceID;
    this.chunkSize = chunkSize;
    KeyValue keyValue =
        KeyValue.newBuilder().setKey("TYPE").setValue("KEY").build();
    this.containerBlockData =
        BlockData.newBuilder().setBlockID(blockID.getDatanodeBlockIDProtobuf())
            .addMetadata(keyValue);
    this.xceiverClientManager = xceiverClientManager;
    this.xceiverClient = xceiverClient;
    this.streamId = UUID.randomUUID().toString();
    this.chunkIndex = 0;
    this.streamBufferFlushSize = streamBufferFlushSize;
    this.streamBufferMaxSize = streamBufferMaxSize;
    this.watchTimeout = watchTimeout;
    this.buffer = buffer;
    this.ioException = null;

    // A single thread executor handle the responses of async requests
    responseExecutor = Executors.newSingleThreadExecutor();
    commitIndexList = new ArrayList<>();
    lastSuccessfulFlushIndex = 0;
    futureList = new ArrayList<>();
    lastFlushPos = 0;
  }

  public BlockID getBlockID() {
    return blockID;
  }

  public int getLastSuccessfulFlushIndex() {
    return lastSuccessfulFlushIndex;
  }


  @Override
  public void write(int b) throws IOException {
    checkOpen();
    byte[] buf = new byte[1];
    buf[0] = (byte) b;
    write(buf, 0, 1);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return;
    }
    checkOpen();
    while (len > 0) {
      int writeLen;
      writeLen = Math.min(chunkSize - buffer.position() % chunkSize, len);
      buffer.put(b, off, writeLen);
      if (buffer.position() % chunkSize == 0) {
        int pos = buffer.position() - chunkSize;
        int limit = buffer.position();
        writeChunk(pos, limit);
      }
      off += writeLen;
      len -= writeLen;
      if (buffer.position() >= streamBufferFlushSize
          && buffer.position() % streamBufferFlushSize == 0) {

        lastFlushPos = buffer.position();
        futureList.add(handlePartialFlush());
      }
      if (buffer.position() >= streamBufferMaxSize
          && buffer.position() % streamBufferMaxSize == 0) {
        handleFullBuffer();
      }
    }
  }

  /**
   * Will be called on the retryPath in case closedContainerException/
   * TimeoutException.
   * @param len length of data to write
   * @throws IOException if error occured
   */

  // In this case, the data is already cached in the buffer.
  public void writeOnRetry(int len) throws IOException {
    if (len == 0) {
      return;
    }
    int off = 0;
    checkOpen();
    while (len > 0) {
      int writeLen;
      writeLen = Math.min(chunkSize, len);
      if (writeLen == chunkSize) {
        int pos = off;
        int limit = pos + chunkSize;
        writeChunk(pos, limit);
      }
      off += writeLen;
      len -= writeLen;
      if (off % streamBufferFlushSize == 0) {
        lastFlushPos = off;
        futureList.add(handlePartialFlush());
      }
      if (off % streamBufferMaxSize == 0) {
        handleFullBuffer();
      }
    }
  }

  private void handleResponse(
      ContainerProtos.ContainerCommandResponseProto response,
      XceiverClientAsyncReply asyncReply) {
    validateResponse(response);
    discardBuffer(asyncReply);
  }

  private void discardBuffer(XceiverClientAsyncReply asyncReply) {
    if (!commitIndexList.isEmpty()) {
      long index = commitIndexList.get(0);
      if (checkIfBufferDiscardRequired(asyncReply, index)) {
        updateFlushIndex();
      }
    }
  }

  /**
   * just update the lastSuccessfulFlushIndex. Since we have allocated
   * the buffer more than the streamBufferMaxSize, we can keep on writing
   * to the buffer. In case of failure, we will read the data starting from
   * lastSuccessfulFlushIndex.
   */
  private void updateFlushIndex() {
    lastSuccessfulFlushIndex += streamBufferFlushSize;
    LOG.debug("Discarding buffer till pos " + lastSuccessfulFlushIndex);
    if (!commitIndexList.isEmpty()) {
      commitIndexList.remove(0);
      futureList.remove(0);
    }

  }
  /**
   * Check if the last commitIndex stored at the beginning of the
   * commitIndexList is less than equal to current commitInfo indexes.
   * If its true, the buffer has been successfully flushed till the
   * last position where flush happened.
   */
  private boolean checkIfBufferDiscardRequired(
      XceiverClientAsyncReply asyncReply, long commitIndex) {
    if (asyncReply.getCommitInfos() != null) {
      for (XceiverClientAsyncReply.CommitInfo info : asyncReply
          .getCommitInfos()) {
        if (info.getCommitIndex() < commitIndex) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * This is a blocking call.It will wait for the flush till the commit index
   * at the head of the commitIndexList gets replicated to all or majority.
   * @throws IOException
   */
  private void handleFullBuffer() throws IOException {
    if (!commitIndexList.isEmpty()) {
      watchForCommit(commitIndexList.get(0));
    }
  }

  /**
   * calls watchForCommit API of the Ratis Client. For Standalone client,
   * it is a no op.
   * @param commitIndex log index to watch for
   * @throws IOException IOException in case watch gets timed out
   */
  private void watchForCommit(long commitIndex) throws IOException {
    checkOpen();
    Preconditions.checkState(!commitIndexList.isEmpty());
    try {
      xceiverClient.watchForCommit(commitIndex, watchTimeout);
    } catch (TimeoutException | InterruptedException | ExecutionException e) {
      LOG.warn("watchForCommit failed for index " + commitIndex, e);
      throw new IOException(
          "Unexpected Storage Container Exception: " + e.toString(), e);
    }
  }

  private CompletableFuture<ContainerProtos.
      ContainerCommandResponseProto> handlePartialFlush()
      throws IOException {
    String requestId =
        traceID + ContainerProtos.Type.PutBlock + chunkIndex + blockID;
    try {
      XceiverClientAsyncReply asyncReply =
          putBlockAsync(xceiverClient, containerBlockData.build(), requestId);
      CompletableFuture<ContainerProtos.ContainerCommandResponseProto> future =
          asyncReply.getResponse();

      return future.thenApplyAsync(e -> {
        handleResponse(e, asyncReply);
        // if the ioException is not set, putBlock is successful
        if (ioException == null) {
          LOG.debug(
              "Adding index " + asyncReply.getLogIndex() + " commitList size "
                  + commitIndexList.size());
          BlockID responseBlockID = BlockID.getFromProtobuf(
              e.getPutBlock().getCommittedBlockLength().getBlockID());
          Preconditions.checkState(blockID.getContainerBlockID()
              .equals(responseBlockID.getContainerBlockID()));
          // updates the bcsId of the block
          blockID = responseBlockID;
          long index = asyncReply.getLogIndex();
          // for standalone protocol, logIndex will always be 0.
          if (index != 0) {
            commitIndexList.add(index);
          } else {
            updateFlushIndex();
          }
        }
        return e;
      }, responseExecutor);
    } catch (IOException | InterruptedException | ExecutionException e) {
      throw new IOException(
          "Unexpected Storage Container Exception: " + e.toString(), e);
    }
  }

  @Override
  public void flush() throws IOException {
    if (xceiverClientManager != null && xceiverClient != null
        && buffer != null) {
      checkOpen();
      if (buffer.position() > 0 && lastSuccessfulFlushIndex != buffer
          .position()) {
        try {

          // flush the last chunk data residing on the buffer
          if (buffer.position() % chunkSize > 0) {
            int pos = buffer.position() - (buffer.position() % chunkSize);
            writeChunk(pos, buffer.position());
          }
          if (lastFlushPos != buffer.position()) {
            lastFlushPos = buffer.position();
            handlePartialFlush();
          }
          CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(
              futureList.toArray(new CompletableFuture[futureList.size()]));
          combinedFuture.get();
          // just check again if the exception is hit while waiting for the
          // futures to ensure flush has indeed succeeded
          checkOpen();
        } catch (InterruptedException | ExecutionException e) {
          throw new IOException(
              "Unexpected Storage Container Exception: " + e.toString(), e);
        }
      }
    }
  }

  private void writeChunk(int pos, int limit) throws IOException {
    // Please note : We are not flipping the slice when we write since
    // the slices are pointing the buffer start and end as needed for
    // the chunk write. Also please note, Duplicate does not create a
    // copy of data, it only creates metadata that points to the data
    // stream.
    ByteBuffer chunk = buffer.duplicate();
    chunk.position(pos);
    chunk.limit(limit);
    writeChunkToContainer(chunk);
  }

  @Override
  public void close() throws IOException {
    if (xceiverClientManager != null && xceiverClient != null
        && buffer != null) {
      try {
        if (buffer.position() > lastFlushPos) {
          int pos = buffer.position() - (buffer.position() % chunkSize);
          writeChunk(pos, buffer.position());
          futureList.add(handlePartialFlush());
        }
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(
            futureList.toArray(new CompletableFuture[futureList.size()]));

        // wait for all the transactions to complete
        combinedFuture.get();

        // irrespective of whether the commitIndexList is empty or not,
        // ensure there is no exception set(For Standalone Protocol)
        checkOpen();
        if (!commitIndexList.isEmpty()) {
          // wait for the last commit index in the commitIndexList to get
          // committed to all or majority of nodes in case timeout happens.
          long lastIndex = commitIndexList.get(commitIndexList.size() - 1);
          LOG.debug(
              "waiting for last flush Index " + lastIndex + " to catch up");
          watchForCommit(lastIndex);
          updateFlushIndex();
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(
            "Unexpected Storage Container Exception: " + e.toString(), e);
      } finally {
        cleanup();
      }
    }
    // clear the buffer
    buffer.clear();
  }

  private void validateResponse(
      ContainerProtos.ContainerCommandResponseProto responseProto) {
    try {
      ContainerProtocolCalls.validateContainerResponse(responseProto);
    } catch (StorageContainerException sce) {
      ioException = new IOException(
          "Unexpected Storage Container Exception: " + sce.toString(), sce);
    }
  }

  public void cleanup() {
    if (xceiverClientManager != null) {
      xceiverClientManager.releaseClient(xceiverClient);
    }
    xceiverClientManager = null;
    xceiverClient = null;
    if (futureList != null) {
      futureList.clear();
    }
    futureList = null;
    commitIndexList = null;
    responseExecutor.shutdown();
  }

  /**
   * Checks if the stream is open or exception has occured.
   * If not, throws an exception.
   *
   * @throws IOException if stream is closed
   */
  private void checkOpen() throws IOException {
    if (xceiverClient == null) {
      throw new IOException("ChunkOutputStream has been closed.");
    } else if (ioException != null) {
      throw ioException;
    }
  }

  /**
   * Writes buffered data as a new chunk to the container and saves chunk
   * information to be used later in putKey call.
   *
   * @throws IOException if there is an I/O error while performing the call
   */
  private void writeChunkToContainer(ByteBuffer chunk) throws IOException {
    int effectiveChunkSize = chunk.remaining();
    ByteString data = ByteString.copyFrom(chunk);
    ChunkInfo chunkInfo = ChunkInfo.newBuilder().setChunkName(
        DigestUtils.md5Hex(key) + "_stream_" + streamId + "_chunk_"
            + ++chunkIndex).setOffset(0).setLen(effectiveChunkSize).build();
    // generate a unique requestId
    String requestId =
        traceID + ContainerProtos.Type.WriteChunk + chunkIndex + chunkInfo
            .getChunkName();
    try {
      XceiverClientAsyncReply asyncReply =
          writeChunkAsync(xceiverClient, chunkInfo, blockID, data, requestId);
      CompletableFuture<ContainerProtos.ContainerCommandResponseProto> future =
          asyncReply.getResponse();
      future.thenApplyAsync(e -> {
        handleResponse(e, asyncReply);
        return e;
      }, responseExecutor);
    } catch (IOException | InterruptedException | ExecutionException e) {
      throw new IOException(
          "Unexpected Storage Container Exception: " + e.toString(), e);
    }
    LOG.debug(
        "writing chunk " + chunkInfo.getChunkName() + " blockID " + blockID
            + " length " + chunk.remaining());
    containerBlockData.addChunks(chunkInfo);
  }
}
