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
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
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
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;

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
public class BlockOutputStream extends OutputStream {
  public static final Logger LOG =
      LoggerFactory.getLogger(BlockOutputStream.class);

  private BlockID blockID;
  private final String key;
  private final String traceID;
  private final BlockData.Builder containerBlockData;
  private XceiverClientManager xceiverClientManager;
  private XceiverClientSpi xceiverClient;
  private final Checksum checksum;
  private final String streamId;
  private int chunkIndex;
  private int chunkSize;
  private final long streamBufferFlushSize;
  private final long streamBufferMaxSize;
  private final long watchTimeout;
  private List<ByteBuffer> bufferList;
  // The IOException will be set by response handling thread in case there is an
  // exception received in the response. If the exception is set, the next
  // request will fail upfront.
  private IOException ioException;
  private ExecutorService responseExecutor;

  // the effective length of data flushed so far
  private long totalDataFlushedLength;

  // effective data write attempted so far for the block
  private long writtenDataLength;

  // total data which has been successfully flushed and acknowledged
  // by all servers
  private long totalAckDataLength;

  // list to hold up all putBlock futures
  private List<CompletableFuture<ContainerProtos.ContainerCommandResponseProto>>
      futureList;
  // map containing mapping for putBlock logIndex to to flushedDataLength Map.
  private ConcurrentHashMap<Long, Long> commitIndex2flushedDataMap;

  private int currentBufferIndex;

  /**
   * Creates a new BlockOutputStream.
   *
   * @param blockID              block ID
   * @param key                  chunk key
   * @param xceiverClientManager client manager that controls client
   * @param xceiverClient        client to perform container calls
   * @param traceID              container protocol call args
   * @param chunkSize            chunk size
   * @param bufferList           list of byte buffers
   * @param streamBufferFlushSize flush size
   * @param streamBufferMaxSize   max size of the currentBuffer
   * @param watchTimeout          watch timeout
   * @param checksum              checksum
   */
  public BlockOutputStream(BlockID blockID, String key,
      XceiverClientManager xceiverClientManager, XceiverClientSpi xceiverClient,
      String traceID, int chunkSize, long streamBufferFlushSize,
      long streamBufferMaxSize, long watchTimeout,
      List<ByteBuffer> bufferList, Checksum checksum) {
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
    this.bufferList = bufferList;
    this.checksum = checksum;

    // A single thread executor handle the responses of async requests
    responseExecutor = Executors.newSingleThreadExecutor();
    commitIndex2flushedDataMap = new ConcurrentHashMap<>();
    totalAckDataLength = 0;
    futureList = new ArrayList<>();
    totalDataFlushedLength = 0;
    currentBufferIndex = 0;
    writtenDataLength = 0;
  }

  public BlockID getBlockID() {
    return blockID;
  }

  public long getTotalSuccessfulFlushedData() {
    return totalAckDataLength;
  }

  public long getWrittenDataLength() {
    return writtenDataLength;
  }

  private long computeBufferData() {
    int dataLength =
        bufferList.stream().mapToInt(Buffer::position).sum();
    Preconditions.checkState(dataLength <= streamBufferMaxSize);
    return dataLength;
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
    while (len > 0) {
      checkOpen();
      int writeLen;
      allocateBuffer();
      ByteBuffer currentBuffer = getCurrentBuffer();
      writeLen =
          Math.min(chunkSize - currentBuffer.position() % chunkSize, len);
      currentBuffer.put(b, off, writeLen);
      if (currentBuffer.position() % chunkSize == 0) {
        int pos = currentBuffer.position() - chunkSize;
        int limit = currentBuffer.position();
        writeChunk(pos, limit);
      }
      off += writeLen;
      len -= writeLen;
      writtenDataLength += writeLen;
      if (currentBuffer.position() == streamBufferFlushSize) {
        totalDataFlushedLength += streamBufferFlushSize;
        handlePartialFlush();
      }
      long bufferedData = computeBufferData();
      // Data in the bufferList can not exceed streamBufferMaxSize
      if (bufferedData == streamBufferMaxSize) {
        handleFullBuffer();
      }
    }
  }

  private ByteBuffer getCurrentBuffer() {
    ByteBuffer buffer = bufferList.get(currentBufferIndex);
    if (!buffer.hasRemaining()) {
      currentBufferIndex =
          currentBufferIndex < getMaxNumBuffers() - 1 ? ++currentBufferIndex :
              0;
    }
    return bufferList.get(currentBufferIndex);
  }

  private int getMaxNumBuffers() {
    return (int)(streamBufferMaxSize/streamBufferFlushSize);
  }

  private void allocateBuffer() {
    for (int i = bufferList.size(); i < getMaxNumBuffers(); i++) {
      bufferList.add(ByteBuffer.allocate((int)streamBufferFlushSize));
    }
  }

  /**
   * Will be called on the retryPath in case closedContainerException/
   * TimeoutException.
   * @param len length of data to write
   * @throws IOException if error occurred
   */

  // In this case, the data is already cached in the currentBuffer.
  public void writeOnRetry(long len) throws IOException {
    if (len == 0) {
      return;
    }
    int off = 0;
    int pos = off;
    while (len > 0) {
      long writeLen;
      writeLen = Math.min(chunkSize, len);
      if (writeLen == chunkSize) {
        int limit = pos + chunkSize;
        writeChunk(pos, limit);
      }
      off += writeLen;
      len -= writeLen;
      writtenDataLength += writeLen;
      if (off % streamBufferFlushSize == 0) {
        // reset the position to zero as now we wll readng thhe next buffer in
        // the list
        pos = 0;
        totalDataFlushedLength += streamBufferFlushSize;
        handlePartialFlush();
      }
      if (computeBufferData() % streamBufferMaxSize == 0) {
        handleFullBuffer();
      }
    }
  }

  /**
   * just update the totalAckDataLength. Since we have allocated
   * the currentBuffer more than the streamBufferMaxSize, we can keep on writing
   * to the currentBuffer. In case of failure, we will read the data starting
   * from totalAckDataLength.
   */
  private void updateFlushIndex(long index) {
    if (!commitIndex2flushedDataMap.isEmpty()) {
      Preconditions.checkState(commitIndex2flushedDataMap.containsKey(index));
      totalAckDataLength = commitIndex2flushedDataMap.remove(index);
      LOG.debug("Total data successfully replicated: " + totalAckDataLength);
      futureList.remove(0);
      // Flush has been committed to required servers successful.
      // just swap the bufferList head and tail after clearing.
      ByteBuffer currentBuffer = bufferList.remove(0);
      currentBuffer.clear();
      if (currentBufferIndex != 0) {
        currentBufferIndex--;
      }
      bufferList.add(currentBuffer);
    }
  }

  /**
   * This is a blocking call. It will wait for the flush till the commit index
   * at the head of the commitIndex2flushedDataMap gets replicated to all or
   * majority.
   * @throws IOException
   */
  private void handleFullBuffer() throws IOException {
    try {
      checkOpen();
      if (!futureList.isEmpty()) {
        waitOnFlushFutures();
      }
    } catch (InterruptedException | ExecutionException e) {
      adjustBuffersOnException();
      throw new IOException(
          "Unexpected Storage Container Exception: " + e.toString(), e);
    }
    if (!commitIndex2flushedDataMap.isEmpty()) {
      watchForCommit(
          commitIndex2flushedDataMap.keySet().stream().mapToLong(v -> v)
              .min().getAsLong());
    }
  }

  private void adjustBuffers(long commitIndex) {
    commitIndex2flushedDataMap.keySet().stream().forEach(index -> {
      if (index <= commitIndex) {
        updateFlushIndex(index);
      } else {
        return;
      }
    });
  }

  // It may happen that once the exception is encountered , we still might
  // have successfully flushed up to a certain index. Make sure the buffers
  // only contain data which have not been sufficiently replicated
  private void adjustBuffersOnException() {
    adjustBuffers(xceiverClient.getReplicatedMinCommitIndex());
  }

  /**
   * calls watchForCommit API of the Ratis Client. For Standalone client,
   * it is a no op.
   * @param commitIndex log index to watch for
   * @return minimum commit index replicated to all nodes
   * @throws IOException IOException in case watch gets timed out
   */
  private void watchForCommit(long commitIndex) throws IOException {
    checkOpen();
    Preconditions.checkState(!commitIndex2flushedDataMap.isEmpty());
    try {
      long index =
          xceiverClient.watchForCommit(commitIndex, watchTimeout);
      adjustBuffers(index);
    } catch (TimeoutException | InterruptedException | ExecutionException e) {
      LOG.warn("watchForCommit failed for index " + commitIndex, e);
      adjustBuffersOnException();
      throw new IOException(
          "Unexpected Storage Container Exception: " + e.toString(), e);
    }
  }

  private CompletableFuture<ContainerProtos.
      ContainerCommandResponseProto> handlePartialFlush()
      throws IOException {
    checkOpen();
    long flushPos = totalDataFlushedLength;
    String requestId =
        traceID + ContainerProtos.Type.PutBlock + chunkIndex + blockID;
    CompletableFuture<ContainerProtos.
        ContainerCommandResponseProto> flushFuture;
    try {
      XceiverClientAsyncReply asyncReply =
          putBlockAsync(xceiverClient, containerBlockData.build(), requestId);
      CompletableFuture<ContainerProtos.ContainerCommandResponseProto> future =
          asyncReply.getResponse();
      flushFuture = future.thenApplyAsync(e -> {
        try {
          validateResponse(e);
        } catch (IOException sce) {
          future.completeExceptionally(sce);
          return e;
        }
        // if the ioException is not set, putBlock is successful
        if (ioException == null) {
          LOG.debug(
              "Adding index " + asyncReply.getLogIndex() + " commitMap size "
                  + commitIndex2flushedDataMap.size());
          BlockID responseBlockID = BlockID.getFromProtobuf(
              e.getPutBlock().getCommittedBlockLength().getBlockID());
          Preconditions.checkState(blockID.getContainerBlockID()
              .equals(responseBlockID.getContainerBlockID()));
          // updates the bcsId of the block
          blockID = responseBlockID;
          // for standalone protocol, logIndex will always be 0.
          commitIndex2flushedDataMap.put(asyncReply.getLogIndex(), flushPos);
        }
        return e;
      }, responseExecutor).exceptionally(e -> {
        LOG.debug(
            "putBlock failed for blockID " + blockID + " with exception " + e
                .getLocalizedMessage());
        CompletionException ce =  new CompletionException(e);
        setIoException(ce);
        throw ce;
      });
    } catch (IOException | InterruptedException | ExecutionException e) {
      throw new IOException(
          "Unexpected Storage Container Exception: " + e.toString(), e);
    }
    futureList.add(flushFuture);
    return flushFuture;
  }

  @Override
  public void flush() throws IOException {
    if (xceiverClientManager != null && xceiverClient != null
        && bufferList != null) {
      checkOpen();
      int bufferSize = bufferList.size();
      if (bufferSize > 0) {
        try {
          // flush the last chunk data residing on the currentBuffer
          if (totalDataFlushedLength < writtenDataLength) {
            ByteBuffer currentBuffer = getCurrentBuffer();
            int pos = currentBuffer.position() - (currentBuffer.position()
                % chunkSize);
            int limit = currentBuffer.position() - pos;
            writeChunk(pos, currentBuffer.position());
            totalDataFlushedLength += limit;
            handlePartialFlush();
          }
          waitOnFlushFutures();
          // just check again if the exception is hit while waiting for the
          // futures to ensure flush has indeed succeeded
          checkOpen();
        } catch (InterruptedException | ExecutionException e) {
          adjustBuffersOnException();
          throw new IOException(
              "Unexpected Storage Container Exception: " + e.toString(), e);
        }
      }
    }
  }

  private void writeChunk(int pos, int limit) throws IOException {
    // Please note : We are not flipping the slice when we write since
    // the slices are pointing the currentBuffer start and end as needed for
    // the chunk write. Also please note, Duplicate does not create a
    // copy of data, it only creates metadata that points to the data
    // stream.
    ByteBuffer chunk = bufferList.get(currentBufferIndex).duplicate();
    chunk.position(pos);
    chunk.limit(limit);
    writeChunkToContainer(chunk);
  }

  @Override
  public void close() throws IOException {
    if (xceiverClientManager != null && xceiverClient != null
        && bufferList != null) {
      int bufferSize = bufferList.size();
      if (bufferSize > 0) {
        try {
          // flush the last chunk data residing on the currentBuffer
          if (totalDataFlushedLength < writtenDataLength) {
            ByteBuffer currentBuffer = getCurrentBuffer();
            int pos = currentBuffer.position() - (currentBuffer.position()
                % chunkSize);
            int limit = currentBuffer.position() - pos;
            writeChunk(pos, currentBuffer.position());
            totalDataFlushedLength += limit;
            handlePartialFlush();
          }
          waitOnFlushFutures();
          // irrespective of whether the commitIndex2flushedDataMap is empty
          // or not, ensure there is no exception set
          checkOpen();
          if (!commitIndex2flushedDataMap.isEmpty()) {
            // wait for the last commit index in the commitIndex2flushedDataMap
            // to get committed to all or majority of nodes in case timeout
            // happens.
            long lastIndex =
                commitIndex2flushedDataMap.keySet().stream()
                    .mapToLong(v -> v).max().getAsLong();
            LOG.debug(
                "waiting for last flush Index " + lastIndex + " to catch up");
            watchForCommit(lastIndex);
          }
        } catch (InterruptedException | ExecutionException e) {
          adjustBuffersOnException();
          throw new IOException(
              "Unexpected Storage Container Exception: " + e.toString(), e);
        } finally {
          cleanup();
        }
      }
      // clear the currentBuffer
      bufferList.stream().forEach(ByteBuffer::clear);
    }
  }

  private void waitOnFlushFutures()
      throws InterruptedException, ExecutionException {
    CompletableFuture<Void> combinedFuture = CompletableFuture
        .allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
    // wait for all the transactions to complete
    combinedFuture.get();
  }

  private void validateResponse(
      ContainerProtos.ContainerCommandResponseProto responseProto)
      throws IOException {
    try {
      // if the ioException is already set, it means a prev request has failed
      // just throw the exception. The current operation will fail with the
      // original error
      if (ioException != null) {
        throw ioException;
      }
      ContainerProtocolCalls.validateContainerResponse(responseProto);
    } catch (StorageContainerException sce) {
      LOG.error("Unexpected Storage Container Exception: ", sce);
      setIoException(sce);
      throw sce;
    }
  }

  private void setIoException(Exception e) {
    if (ioException != null) {
      ioException =  new IOException(
          "Unexpected Storage Container Exception: " + e.toString(), e);
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
    if (commitIndex2flushedDataMap != null) {
      commitIndex2flushedDataMap.clear();
    }
    commitIndex2flushedDataMap = null;
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
      throw new IOException("BlockOutputStream has been closed.");
    } else if (ioException != null) {
      adjustBuffersOnException();
      throw ioException;
    }
  }

  /**
   * Writes buffered data as a new chunk to the container and saves chunk
   * information to be used later in putKey call.
   *
   * @throws IOException if there is an I/O error while performing the call
   * @throws OzoneChecksumException if there is an error while computing
   * checksum
   */
  private void writeChunkToContainer(ByteBuffer chunk) throws IOException {
    int effectiveChunkSize = chunk.remaining();
    ByteString data = ByteString.copyFrom(chunk);
    ChecksumData checksumData = checksum.computeChecksum(data);
    ChunkInfo chunkInfo = ChunkInfo.newBuilder()
        .setChunkName(DigestUtils.md5Hex(key) + "_stream_" + streamId +
            "_chunk_" + ++chunkIndex)
        .setOffset(0)
        .setLen(effectiveChunkSize)
        .setChecksumData(checksumData.getProtoBufMessage())
        .build();
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
        try {
          validateResponse(e);
        } catch (IOException sce) {
          future.completeExceptionally(sce);
        }
        return e;
      }, responseExecutor).exceptionally(e -> {
        LOG.debug(
            "writing chunk failed " + chunkInfo.getChunkName() + " blockID "
                + blockID + " with exception " + e.getLocalizedMessage());
        CompletionException ce = new CompletionException(e);
        setIoException(ce);
        throw ce;
      });
    } catch (IOException | InterruptedException | ExecutionException e) {
      throw new IOException(
          "Unexpected Storage Container Exception: " + e.toString(), e);
    }
    LOG.debug(
        "writing chunk " + chunkInfo.getChunkName() + " blockID " + blockID
            + " length " + effectiveChunkSize);
    containerBlockData.addChunks(chunkInfo);
  }
}
