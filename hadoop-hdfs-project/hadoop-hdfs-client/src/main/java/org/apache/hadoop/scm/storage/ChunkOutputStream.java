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

package org.apache.hadoop.scm.storage;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.KeyData;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.KeyValue;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .Result.SUCCESS;
import static org.apache.hadoop.scm.storage.ContainerProtocolCalls.putKey;
import static org.apache.hadoop.scm.storage.ContainerProtocolCalls.writeChunk;


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

  private final String containerKey;
  private final String key;
  private final String traceID;
  private final KeyData.Builder containerKeyData;
  private final String streamId;
  private XceiverClientManager xceiverClientManager;
  private XceiverClientSpi xceiverClient;
  private ByteBuffer buffer;
  private int chunkSize;
  private int streamBufferSize;

  /**
   * Creates a new ChunkOutputStream.
   *
   * @param containerKey container key
   * @param key chunk key
   * @param xceiverClientManager client manager that controls client
   * @param xceiverClient client to perform container calls
   * @param traceID container protocol call args
   * @param chunkSize chunk size
   * @param maxBufferSize -- Controls the maximum amount of memory that we need
   * to allocate data buffering.
   */
  public ChunkOutputStream(String containerKey, String key,
      XceiverClientManager xceiverClientManager, XceiverClientSpi xceiverClient,
      String traceID, int chunkSize, int maxBufferSize) {
    this.containerKey = containerKey;
    this.key = key;
    this.traceID = traceID;
    this.chunkSize = chunkSize;
    this.streamBufferSize = maxBufferSize;

    KeyValue keyValue = KeyValue.newBuilder()
        .setKey("TYPE").setValue("KEY").build();
    this.containerKeyData = KeyData.newBuilder()
        .setContainerName(xceiverClient.getPipeline().getContainerName())
        .setName(containerKey)
        .addMetadata(keyValue);
    this.xceiverClientManager = xceiverClientManager;
    this.xceiverClient = xceiverClient;
    this.buffer = ByteBuffer.allocate(maxBufferSize);
    this.streamId = UUID.randomUUID().toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void write(int b) throws IOException {
    checkOpen();
    byte[] c = new byte[1];
    c[0] = (byte) b;
    write(c, 0, 1);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return;
    }
    checkOpen();
    int rollbackPosition = buffer.position();
    int rollbackLimit = buffer.limit();
    try {
      List<ImmutablePair<CompletableFuture<ContainerProtos
              .ContainerCommandResponseProto>, ChunkInfo>>
          writeFutures = writeInParallel(b, off, len);
      // This is a rendezvous point for this function call, all chunk I/O
      // for this block must complete before we can declare this call as
      // complete.

      // Wait until all the futures complete or throws an exception if any of
      // the calls ended with an exception this call will throw.
      // if futures is null, it means that we wrote the data to the buffer and
      // returned.
      if (writeFutures != null) {
        CompletableFuture.allOf(writeFutures.toArray(new
            CompletableFuture[writeFutures.size()])).join();

        // Wrote this data, we will clear this buffer now.
        buffer.clear();
      }
    } catch (InterruptedException | ExecutionException e) {
      buffer.position(rollbackPosition);
      buffer.limit(rollbackLimit);
      throw new IOException("Unexpected error in write. ", e);
    }
  }

  /**
   * Write a given block into many small chunks in parallel.
   *
   * @param b
   * @param off
   * @param len
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public List<ImmutablePair<CompletableFuture<ContainerProtos
          .ContainerCommandResponseProto>, ChunkInfo>>
      writeInParallel(byte[] b, int off, int len)
      throws IOException, ExecutionException, InterruptedException {

    Preconditions.checkArgument(len <= streamBufferSize,
        "A chunk write cannot be " + "larger than max buffer size limit.");
    long newBlockCount = len / chunkSize;
    buffer.put(b, off, len);
    List<ImmutablePair<CompletableFuture<ContainerProtos
            .ContainerCommandResponseProto>, ChunkInfo>>
        writeFutures = new LinkedList<>();

    // We if must have at least a chunkSize of data ready to write, if so we
    // will go ahead and start writing that data.
    if (buffer.position() >= chunkSize) {
      // Allocate new byte slices which will point to each chunk of data
      // that we want to write. Divide the byte buffer into individual chunks
      // each of length equals to chunkSize max where each chunk will be
      // assigned a chunkId where, for each chunk the async write requests will
      // be made and wait for all of them to return before the write call
      // returns.
      for (int chunkId = 0; chunkId < newBlockCount; chunkId++) {
        // Please note : We are not flipping the slice when we write since
        // the slices are pointing the buffer start and end as needed for
        // the chunk write. Also please note, Duplicate does not create a
        // copy of data, it only creates metadata that points to the data
        // stream.
        ByteBuffer chunk = buffer.duplicate();
        Preconditions.checkState((chunkId * chunkSize) < buffer.limit(),
            "Chunk offset cannot be beyond the limits of the buffer.");
        chunk.position(chunkId * chunkSize);
        // Min handles the case where the last block might be lesser than
        // chunk Size.
        chunk.limit(chunk.position() +
            Math.min(chunkSize, chunk.remaining() - (chunkId * chunkSize)));

        // Schedule all the writes, this is a non-block call which returns
        // futures. We collect these futures and wait for all  of them to
        // complete in the next line.
        writeFutures.add(writeChunkToContainer(chunk, 0, chunkSize));
      }
      return writeFutures;
    }
    // Nothing to do , return null.
    return null;
  }

  @Override
  public synchronized void flush() throws IOException {
    checkOpen();
    if (buffer.position() > 0) {
      int rollbackPosition = buffer.position();
      int rollbackLimit = buffer.limit();
      ByteBuffer chunk = buffer.duplicate();
      try {

        ImmutablePair<CompletableFuture<ContainerProtos
            .ContainerCommandResponseProto>, ChunkInfo>
            result = writeChunkToContainer(chunk, 0, chunkSize);
        updateChunkInfo(result);
        buffer.clear();
      } catch (ExecutionException | InterruptedException e) {
        buffer.position(rollbackPosition);
        buffer.limit(rollbackLimit);
        throw new IOException("Failure in flush", e);
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (xceiverClientManager != null && xceiverClient != null &&
        buffer != null) {
      try {
        if (buffer.position() > 0) {
          // This flip is needed since this is the real buffer to which we
          // are writing and position will have moved each time we did a put.
          buffer.flip();

          // Call get immediately to make this call Synchronous.

          ImmutablePair<CompletableFuture<ContainerProtos
              .ContainerCommandResponseProto>, ChunkInfo>
              result = writeChunkToContainer(buffer, 0, buffer.limit());
          updateChunkInfo(result);
          buffer.clear();
        }
        putKey(xceiverClient, containerKeyData.build(), traceID);
      } catch (IOException | InterruptedException | ExecutionException e) {
        throw new IOException(
            "Unexpected Storage Container Exception: " + e.toString(), e);
      } finally {
        xceiverClientManager.releaseClient(xceiverClient);
        xceiverClientManager = null;
        xceiverClient = null;
        buffer = null;
      }
    }

  }

  private void updateChunkInfo(
      ImmutablePair<
          CompletableFuture<ContainerProtos.ContainerCommandResponseProto>,
          ChunkInfo
          > result) throws InterruptedException, ExecutionException {
    // Wait for this call to complete.
    ContainerProtos.ContainerCommandResponseProto response =
        result.getLeft().get();

    // If the write call to the chunk is successful, we need to add that
    // chunk information to the containerKeyData.
    // TODO: Clean up the garbage in case of failure.
    if(response.getResult() == SUCCESS) {
      ChunkInfo chunk = result.getRight();
      containerKeyData.addChunks(chunk);
    }
  }

  /**
   * Checks if the stream is open.  If not, throws an exception.
   *
   * @throws IOException if stream is closed
   */
  private synchronized void checkOpen() throws IOException {
    if (xceiverClient == null) {
      throw new IOException("ChunkOutputStream has been closed.");
    }
  }

  /**
   * Writes buffered data as a new chunk to the container and saves chunk
   * information to be used later in putKey call.
   *
   * @param data -- Data to write.
   * @param offset - offset to the data buffer
   * @param len - Length in bytes
   * @return Returns a Immutable pair -- A future object that will contian
   * the result of the operation, and the chunkInfo that we wrote.
   *
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private ImmutablePair<
      CompletableFuture<ContainerProtos.ContainerCommandResponseProto>,
      ChunkInfo>
      writeChunkToContainer(ByteBuffer data, int offset, int len)
      throws IOException, ExecutionException, InterruptedException {


    ByteString dataString = ByteString.copyFrom(data);
    ChunkInfo chunk = ChunkInfo.newBuilder().setChunkName(
            DigestUtils.md5Hex(key) + "_stream_"
                + streamId + "_chunk_" + Time.monotonicNowNanos())
        .setOffset(0)
        .setLen(len)
        .build();
    CompletableFuture<ContainerProtos.ContainerCommandResponseProto> response =
        writeChunk(xceiverClient, chunk, key, dataString, traceID);
    return new ImmutablePair(response, chunk);
  }
}
