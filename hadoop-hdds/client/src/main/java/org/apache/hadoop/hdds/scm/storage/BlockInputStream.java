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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ReadChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    ContainerCommandRequestProto;
import org.apache.hadoop.hdds.client.BlockID;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * An {@link InputStream} used by the REST service in combination with the
 * SCMClient to read the value of a key from a sequence
 * of container chunks.  All bytes of the key value are stored in container
 * chunks.  Each chunk may contain multiple underlying {@link ByteBuffer}
 * instances.  This class encapsulates all state management for iterating
 * through the sequence of chunks and the sequence of buffers within each chunk.
 */
public class BlockInputStream extends InputStream implements Seekable {

  private static final int EOF = -1;

  private final BlockID blockID;
  private final String traceID;
  private XceiverClientManager xceiverClientManager;
  private XceiverClientSpi xceiverClient;
  private List<ChunkInfo> chunks;
  // ChunkIndex points to the index current chunk in the buffers or the the
  // index of chunk which will be read next into the buffers in
  // readChunkFromContainer().
  private int chunkIndex;
  // ChunkIndexOfCurrentBuffer points to the index of chunk read into the
  // buffers or index of the last chunk in the buffers. It is updated only
  // when a new chunk is read from container into the buffers.
  private int chunkIndexOfCurrentBuffer;
  private long[] chunkOffset;
  private List<ByteBuffer> buffers;
  private int bufferIndex;
  private long bufferPosition;
  private boolean verifyChecksum;

  /**
   * Creates a new BlockInputStream.
   *
   * @param blockID block ID of the chunk
   * @param xceiverClientManager client manager that controls client
   * @param xceiverClient client to perform container calls
   * @param chunks list of chunks to read
   * @param traceID container protocol call traceID
   * @param verifyChecksum verify checksum
   * @param initialPosition the initial position of the stream pointer. This
   *                        position is seeked now if the up-stream was seeked
   *                        before this was created.
   */
  public BlockInputStream(
      BlockID blockID, XceiverClientManager xceiverClientManager,
      XceiverClientSpi xceiverClient, List<ChunkInfo> chunks, String traceID,
      boolean verifyChecksum, long initialPosition) throws IOException {
    this.blockID = blockID;
    this.traceID = traceID;
    this.xceiverClientManager = xceiverClientManager;
    this.xceiverClient = xceiverClient;
    this.chunks = chunks;
    this.chunkIndex = 0;
    this.chunkIndexOfCurrentBuffer = -1;
    // chunkOffset[i] stores offset at which chunk i stores data in
    // BlockInputStream
    this.chunkOffset = new long[this.chunks.size()];
    initializeChunkOffset();
    this.buffers = null;
    this.bufferIndex = 0;
    this.bufferPosition = -1;
    this.verifyChecksum = verifyChecksum;
    if (initialPosition > 0) {
      // The stream was seeked to a position before the stream was
      // initialized. So seeking to the position now.
      seek(initialPosition);
    }
  }

  private void initializeChunkOffset() {
    long tempOffset = 0;
    for (int i = 0; i < chunks.size(); i++) {
      chunkOffset[i] = tempOffset;
      tempOffset += chunks.get(i).getLen();
    }
  }

  @Override
  public synchronized int read()
      throws IOException {
    checkOpen();
    int available = prepareRead(1);
    int dataout = EOF;

    if (available == EOF) {
      Preconditions
          .checkState(buffers == null); //should have released by now, see below
    } else {
      dataout = Byte.toUnsignedInt(buffers.get(bufferIndex).get());
    }

    if (blockStreamEOF()) {
      // consumer might use getPos to determine EOF,
      // so release buffers when serving the last byte of data
      releaseBuffers();
    }

    return dataout;
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    // According to the JavaDocs for InputStream, it is recommended that
    // subclasses provide an override of bulk read if possible for performance
    // reasons.  In addition to performance, we need to do it for correctness
    // reasons.  The Ozone REST service uses PipedInputStream and
    // PipedOutputStream to relay HTTP response data between a Jersey thread and
    // a Netty thread.  It turns out that PipedInputStream/PipedOutputStream
    // have a subtle dependency (bug?) on the wrapped stream providing separate
    // implementations of single-byte read and bulk read.  Without this, get key
    // responses might close the connection before writing all of the bytes
    // advertised in the Content-Length.
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return 0;
    }
    checkOpen();
    int total = 0;
    while (len > 0) {
      int available = prepareRead(len);
      if (available == EOF) {
        Preconditions
            .checkState(buffers == null); //should have been released by now
        return total != 0 ? total : EOF;
      }
      buffers.get(bufferIndex).get(b, off + total, available);
      len -= available;
      total += available;
    }

    if (blockStreamEOF()) {
      // smart consumers determine EOF by calling getPos()
      // so we release buffers when serving the final bytes of data
      releaseBuffers();
    }

    return total;
  }

  /**
   * Determines if all data in the stream has been consumed.
   *
   * @return true if EOF, false if more data is available
   */
  protected boolean blockStreamEOF() {
    if (buffersHaveData() || chunksRemaining()) {
      return false;
    } else {
      // if there are any chunks, we better be at the last chunk for EOF
      Preconditions.checkState(((chunks == null) || chunks.isEmpty() ||
              chunkIndex == (chunks.size() - 1)),
          "EOF detected, but not at the last chunk");
      return true;
    }
  }

  private void releaseBuffers() {
    //ashes to ashes, dust to dust
    buffers = null;
    bufferIndex = 0;
  }

  @Override
  public synchronized void close() {
    if (xceiverClientManager != null && xceiverClient != null) {
      xceiverClientManager.releaseClient(xceiverClient, false);
      xceiverClientManager = null;
      xceiverClient = null;
    }
  }

  /**
   * Checks if the stream is open.  If not, throws an exception.
   *
   * @throws IOException if stream is closed
   */
  private synchronized void checkOpen() throws IOException {
    if (xceiverClient == null) {
      throw new IOException("BlockInputStream has been closed.");
    }
  }

  /**
   * Prepares to read by advancing through chunks and buffers as needed until it
   * finds data to return or encounters EOF.
   *
   * @param len desired length of data to read
   * @return length of data available to read, possibly less than desired length
   */
  private synchronized int prepareRead(int len) throws IOException {
    for (;;) {
      if (!buffersAllocated()) {
        // The current chunk at chunkIndex has not been read from the
        // container. Read the chunk and put the data into buffers.
        readChunkFromContainer();
      }
      if (buffersHaveData()) {
        // Data is available from buffers
        ByteBuffer bb = buffers.get(bufferIndex);
        return len > bb.remaining() ? bb.remaining() : len;
      } else if (chunksRemaining()) {
        // There are additional chunks available.
        // Read the next chunk in the block.
        chunkIndex += 1;
        readChunkFromContainer();
      } else {
        // All available input has been consumed.
        return EOF;
      }
    }
  }

  private boolean buffersAllocated() {
    if (buffers == null || buffers.isEmpty()) {
      return false;
    }
    return true;
  }

  private boolean buffersHaveData() {
    boolean hasData = false;

    if (buffersAllocated()) {
      while (bufferIndex < (buffers.size())) {
        if (buffers.get(bufferIndex).hasRemaining()) {
          // current buffer has data
          hasData = true;
          break;
        } else {
          if (buffersRemaining()) {
            // move to next available buffer
            ++bufferIndex;
            Preconditions.checkState(bufferIndex < buffers.size());
          } else {
            // no more buffers remaining
            break;
          }
        }
      }
    }

    return hasData;
  }

  private boolean buffersRemaining() {
    return (bufferIndex < (buffers.size() - 1));
  }

  private boolean chunksRemaining() {
    if ((chunks == null) || chunks.isEmpty()) {
      return false;
    }
    // Check if more chunks are remaining in the stream after chunkIndex
    if (chunkIndex < (chunks.size() - 1)) {
      return true;
    }
    // ChunkIndex is the last chunk in the stream. Check if this chunk has
    // been read from container or not. Return true if chunkIndex has not
    // been read yet and false otherwise.
    return chunkIndexOfCurrentBuffer != chunkIndex;
  }

  /**
   * Attempts to read the chunk at the specified offset in the chunk list.  If
   * successful, then the data of the read chunk is saved so that its bytes can
   * be returned from subsequent read calls.
   *
   * @throws IOException if there is an I/O error while performing the call
   */
  private synchronized void readChunkFromContainer() throws IOException {
    // Read the chunk at chunkIndex
    final ChunkInfo chunkInfo = chunks.get(chunkIndex);
    ByteString byteString;
    byteString = readChunk(chunkInfo);
    buffers = byteString.asReadOnlyByteBufferList();
    bufferIndex = 0;
    chunkIndexOfCurrentBuffer = chunkIndex;

    // The bufferIndex and position might need to be adjusted if seek() was
    // called on the stream before. This needs to be done so that the buffer
    // position can be advanced to the 'seeked' position.
    adjustBufferIndex();
  }

  /**
   * Send RPC call to get the chunk from the container.
   */
  @VisibleForTesting
  protected ByteString readChunk(final ChunkInfo chunkInfo)
      throws IOException {
    ReadChunkResponseProto readChunkResponse;
    try {
      List<CheckedBiFunction> validators =
          ContainerProtocolCalls.getValidatorList();
      validators.add(validator);
      readChunkResponse = ContainerProtocolCalls
          .readChunk(xceiverClient, chunkInfo, blockID, traceID, validators);
    } catch (IOException e) {
      if (e instanceof StorageContainerException) {
        throw e;
      }
      throw new IOException("Unexpected OzoneException: " + e.toString(), e);
    }
    return readChunkResponse.getData();
  }

  @VisibleForTesting
  protected List<DatanodeDetails> getDatanodeList() {
    return xceiverClient.getPipeline().getNodes();
  }

  private CheckedBiFunction<ContainerCommandRequestProto,
      ContainerCommandResponseProto, IOException> validator =
          (request, response) -> {
            ReadChunkResponseProto readChunkResponse = response.getReadChunk();
            final ChunkInfo chunkInfo = readChunkResponse.getChunkData();
            ByteString byteString = readChunkResponse.getData();
            if (byteString.size() != chunkInfo.getLen()) {
              // Bytes read from chunk should be equal to chunk size.
              throw new OzoneChecksumException(String
                  .format("Inconsistent read for chunk=%s len=%d bytesRead=%d",
                      chunkInfo.getChunkName(), chunkInfo.getLen(),
                      byteString.size()));
            }
            ChecksumData checksumData =
                ChecksumData.getFromProtoBuf(chunkInfo.getChecksumData());
            if (verifyChecksum) {
              Checksum.verifyChecksum(byteString, checksumData);
            }
          };

  @Override
  public synchronized void seek(long pos) throws IOException {
    if (pos < 0 || (chunks.size() == 0 && pos > 0)
        || pos >= chunkOffset[chunks.size() - 1] + chunks.get(chunks.size() - 1)
        .getLen()) {
      throw new EOFException("EOF encountered pos: " + pos + " container key: "
          + blockID.getLocalID());
    }

    if (pos < chunkOffset[chunkIndex]) {
      chunkIndex = Arrays.binarySearch(chunkOffset, 0, chunkIndex, pos);
    } else if (pos >= chunkOffset[chunkIndex] + chunks.get(chunkIndex)
        .getLen()) {
      chunkIndex =
          Arrays.binarySearch(chunkOffset, chunkIndex + 1, chunks.size(), pos);
    }
    if (chunkIndex < 0) {
      // Binary search returns -insertionPoint - 1  if element is not present
      // in the array. insertionPoint is the point at which element would be
      // inserted in the sorted array. We need to adjust the chunkIndex
      // accordingly so that chunkIndex = insertionPoint - 1
      chunkIndex = -chunkIndex -2;
    }

    // The bufferPosition should be adjusted to account for the chunk offset
    // of the chunk the the pos actually points to.
    bufferPosition = pos - chunkOffset[chunkIndex];

    // Check if current buffers correspond to the chunk index being seeked
    // and if the buffers have any data.
    if (chunkIndex == chunkIndexOfCurrentBuffer && buffersAllocated()) {
      // Position the buffer to the seeked position.
      adjustBufferIndex();
    } else {
      // Release the current buffers. The next readChunkFromContainer will
      // read the required chunk and position the buffer to the seeked
      // position.
      releaseBuffers();
    }
  }

  private void adjustBufferIndex() {
    if (bufferPosition == -1) {
      // The stream has not been seeked to a position. No need to adjust the
      // buffer Index and position.
      return;
    }
    // The bufferPosition is w.r.t the buffers for current chunk.
    // Adjust the bufferIndex and position to the seeked position.
    long tempOffest = 0;
    for (int i = 0; i < buffers.size(); i++) {
      if (bufferPosition - tempOffest >= buffers.get(i).capacity()) {
        tempOffest += buffers.get(i).capacity();
      } else {
        bufferIndex = i;
        break;
      }
    }
    buffers.get(bufferIndex).position((int) (bufferPosition - tempOffest));
    // Reset the bufferPosition as the seek() operation has been completed.
    bufferPosition = -1;
  }

  @Override
  public synchronized long getPos() throws IOException {
    // position = chunkOffset of current chunk (at chunkIndex) + position of
    // the buffer corresponding to the chunk.
    long bufferPos = 0;

    if (bufferPosition >= 0) {
      // seek has been called but the buffers were empty. Hence, the buffer
      // position will be advanced after the buffers are filled.
      // We return the chunkOffset + bufferPosition here as that will be the
      // position of the buffer pointer after reading the chunk file.
      bufferPos = bufferPosition;

    } else if (blockStreamEOF()) {
      // all data consumed, buffers have been released.
      // get position from the chunk offset and chunk length of last chunk
      bufferPos = chunks.get(chunkIndex).getLen();

    } else if (buffersAllocated()) {
      // get position from available buffers of current chunk
      bufferPos = buffers.get(bufferIndex).position();

    }

    return chunkOffset[chunkIndex] + bufferPos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  public BlockID getBlockID() {
    return blockID;
  }

  @VisibleForTesting
  protected int getChunkIndex() {
    return chunkIndex;
  }
}
