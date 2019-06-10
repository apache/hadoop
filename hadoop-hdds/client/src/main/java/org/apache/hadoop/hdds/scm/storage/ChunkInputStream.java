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
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * An {@link InputStream} called from BlockInputStream to read a chunk from the
 * container. Each chunk may contain multiple underlying {@link ByteBuffer}
 * instances.
 */
public class ChunkInputStream extends InputStream implements Seekable {

  private ChunkInfo chunkInfo;
  private final long length;
  private final BlockID blockID;
  private XceiverClientSpi xceiverClient;
  private boolean verifyChecksum;
  private boolean allocated = false;

  // Buffer to store the chunk data read from the DN container
  private List<ByteBuffer> buffers;

  // Index of the buffers corresponding to the current position of the buffers
  private int bufferIndex;

  // The offset of the current data residing in the buffers w.r.t the start
  // of chunk data
  private long bufferOffset;

  // The number of bytes of chunk data residing in the buffers currently
  private long bufferLength;

  // Position of the ChunkInputStream is maintained by this variable (if a
  // seek is performed. This position is w.r.t to the chunk only and not the
  // block or key. This variable is set only if either the buffers are not
  // yet allocated or the if the allocated buffers do not cover the seeked
  // position. Once the chunk is read, this variable is reset.
  private long chunkPosition = -1;

  private static final int EOF = -1;

  ChunkInputStream(ChunkInfo chunkInfo, BlockID blockId, 
          XceiverClientSpi xceiverClient, boolean verifyChecksum) {
    this.chunkInfo = chunkInfo;
    this.length = chunkInfo.getLen();
    this.blockID = blockId;
    this.xceiverClient = xceiverClient;
    this.verifyChecksum = verifyChecksum;
  }

  public synchronized long getRemaining() throws IOException {
    return length - getPos();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized int read() throws IOException {
    checkOpen();
    int available = prepareRead(1);
    int dataout = EOF;

    if (available == EOF) {
      // There is no more data in the chunk stream. The buffers should have
      // been released by now
      Preconditions.checkState(buffers == null);
    } else {
      dataout = Byte.toUnsignedInt(buffers.get(bufferIndex).get());
    }

    if (chunkStreamEOF()) {
      // consumer might use getPos to determine EOF,
      // so release buffers when serving the last byte of data
      releaseBuffers();
    }

    return dataout;
  }

  /**
   * {@inheritDoc}
   */
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
        // There is no more data in the chunk stream. The buffers should have
        // been released by now
        Preconditions.checkState(buffers == null);
        return total != 0 ? total : EOF;
      }
      buffers.get(bufferIndex).get(b, off + total, available);
      len -= available;
      total += available;
    }

    if (chunkStreamEOF()) {
      // smart consumers determine EOF by calling getPos()
      // so we release buffers when serving the final bytes of data
      releaseBuffers();
    }

    return total;
  }

  /**
   * Seeks the ChunkInputStream to the specified position. This is done by
   * updating the chunkPosition to the seeked position in case the buffers
   * are not allocated or buffers do not contain the data corresponding to
   * the seeked position (determined by buffersHavePosition()). Otherwise,
   * the buffers position is updated to the seeked position.
   */
  @Override
  public synchronized void seek(long pos) throws IOException {
    if (pos < 0 || pos >= length) {
      if (pos == 0) {
        // It is possible for length and pos to be zero in which case
        // seek should return instead of throwing exception
        return;
      }
      throw new EOFException("EOF encountered at pos: " + pos + " for chunk: "
          + chunkInfo.getChunkName());
    }

    if (buffersHavePosition(pos)) {
      // The bufferPosition is w.r.t the current chunk.
      // Adjust the bufferIndex and position to the seeked position.
      adjustBufferPosition(pos - bufferOffset);
    } else {
      chunkPosition = pos;
    }
  }

  @Override
  public synchronized long getPos() throws IOException {
    if (chunkPosition >= 0) {
      return chunkPosition;
    }
    if (chunkStreamEOF()) {
      return length;
    }
    if (buffersHaveData()) {
      return bufferOffset + buffers.get(bufferIndex).position();
    }
    if (buffersAllocated()) {
      return bufferOffset + bufferLength;
    }
    return 0;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized void close() {
    if (xceiverClient != null) {
      xceiverClient = null;
    }
  }

  /**
   * Checks if the stream is open.  If not, throw an exception.
   *
   * @throws IOException if stream is closed
   */
  protected synchronized void checkOpen() throws IOException {
    if (xceiverClient == null) {
      throw new IOException("BlockInputStream has been closed.");
    }
  }

  /**
   * Prepares to read by advancing through buffers or allocating new buffers,
   * as needed until it finds data to return, or encounters EOF.
   * @param len desired lenght of data to read
   * @return length of data available to read, possibly less than desired length
   */
  private synchronized int prepareRead(int len) throws IOException {
    for (;;) {
      if (chunkPosition >= 0) {
        if (buffersHavePosition(chunkPosition)) {
          // The current buffers have the seeked position. Adjust the buffer
          // index and position to point to the chunkPosition.
          adjustBufferPosition(chunkPosition - bufferOffset);
        } else {
          // Read a required chunk data to fill the buffers with seeked
          // position data
          readChunkFromContainer(len);
        }
      }
      if (buffersHaveData()) {
        // Data is available from buffers
        ByteBuffer bb = buffers.get(bufferIndex);
        return len > bb.remaining() ? bb.remaining() : len;
      } else  if (dataRemainingInChunk()) {
        // There is more data in the chunk stream which has not
        // been read into the buffers yet.
        readChunkFromContainer(len);
      } else {
        // All available input from this chunk stream has been consumed.
        return EOF;
      }
    }
  }

  /**
   * Reads full or partial Chunk from DN Container based on the current
   * position of the ChunkInputStream, the number of bytes of data to read
   * and the checksum boundaries.
   * If successful, then the read data in saved in the buffers so that
   * subsequent read calls can utilize it.
   * @param len number of bytes of data to be read
   * @throws IOException if there is an I/O error while performing the call
   * to Datanode
   */
  private synchronized void readChunkFromContainer(int len) throws IOException {

    // index of first byte to be read from the chunk
    long startByteIndex;
    if (chunkPosition >= 0) {
      // If seek operation was called to advance the buffer position, the
      // chunk should be read from that position onwards.
      startByteIndex = chunkPosition;
    } else {
      // Start reading the chunk from the last chunkPosition onwards.
      startByteIndex = bufferOffset + bufferLength;
    }

    if (verifyChecksum) {
      // Update the bufferOffset and bufferLength as per the checksum
      // boundary requirement.
      computeChecksumBoundaries(startByteIndex, len);
    } else {
      // Read from the startByteIndex
      bufferOffset = startByteIndex;
      bufferLength = len;
    }

    // Adjust the chunkInfo so that only the required bytes are read from
    // the chunk.
    final ChunkInfo adjustedChunkInfo = ChunkInfo.newBuilder(chunkInfo)
        .setOffset(bufferOffset)
        .setLen(bufferLength)
        .build();

    ByteString byteString = readChunk(adjustedChunkInfo);

    buffers = byteString.asReadOnlyByteBufferList();
    bufferIndex = 0;
    allocated = true;

    // If the stream was seeked to position before, then the buffer
    // position should be adjusted as the reads happen at checksum boundaries.
    // The buffers position might need to be adjusted for the following
    // scenarios:
    //    1. Stream was seeked to a position before the chunk was read
    //    2. Chunk was read from index < the current position to account for
    //    checksum boundaries.
    adjustBufferPosition(startByteIndex - bufferOffset);
  }

  /**
   * Send RPC call to get the chunk from the container.
   */
  @VisibleForTesting
  protected ByteString readChunk(ChunkInfo readChunkInfo) throws IOException {
    ReadChunkResponseProto readChunkResponse;

    try {
      List<CheckedBiFunction> validators =
          ContainerProtocolCalls.getValidatorList();
      validators.add(validator);

      readChunkResponse = ContainerProtocolCalls.readChunk(xceiverClient,
          readChunkInfo, blockID, validators);

    } catch (IOException e) {
      if (e instanceof StorageContainerException) {
        throw e;
      }
      throw new IOException("Unexpected OzoneException: " + e.toString(), e);
    }

    return readChunkResponse.getData();
  }

  private CheckedBiFunction<ContainerCommandRequestProto,
      ContainerCommandResponseProto, IOException> validator =
          (request, response) -> {
            final ChunkInfo reqChunkInfo =
                request.getReadChunk().getChunkData();

            ReadChunkResponseProto readChunkResponse = response.getReadChunk();
            ByteString byteString = readChunkResponse.getData();

            if (byteString.size() != reqChunkInfo.getLen()) {
              // Bytes read from chunk should be equal to chunk size.
              throw new OzoneChecksumException(String
                  .format("Inconsistent read for chunk=%s len=%d bytesRead=%d",
                      reqChunkInfo.getChunkName(), reqChunkInfo.getLen(),
                      byteString.size()));
            }

            if (verifyChecksum) {
              ChecksumData checksumData = ChecksumData.getFromProtoBuf(
                  chunkInfo.getChecksumData());

              // ChecksumData stores checksum for each 'numBytesPerChecksum'
              // number of bytes in a list. Compute the index of the first
              // checksum to match with the read data

              int checkumStartIndex = (int) (reqChunkInfo.getOffset() /
                  checksumData.getBytesPerChecksum());
              Checksum.verifyChecksum(
                  byteString, checksumData, checkumStartIndex);
            }
          };

  /**
   * Return the offset and length of bytes that need to be read from the
   * chunk file to cover the checksum boundaries covering the actual start and
   * end of the chunk index to be read.
   * For example, lets say the client is reading from index 120 to 450 in the
   * chunk. And let's say checksum is stored for every 100 bytes in the chunk
   * i.e. the first checksum is for bytes from index 0 to 99, the next for
   * bytes from index 100 to 199 and so on. To verify bytes from 120 to 450,
   * we would need to read from bytes 100 to 499 so that checksum
   * verification can be done.
   *
   * @param startByteIndex the first byte index to be read by client
   * @param dataLen number of bytes to be read from the chunk
   */
  private void computeChecksumBoundaries(long startByteIndex, int dataLen) {

    int bytesPerChecksum = chunkInfo.getChecksumData().getBytesPerChecksum();
    // index of the last byte to be read from chunk, inclusively.
    final long endByteIndex = startByteIndex + dataLen - 1;

    bufferOffset =  (startByteIndex / bytesPerChecksum)
        * bytesPerChecksum; // inclusive
    final long endIndex = ((endByteIndex / bytesPerChecksum) + 1)
        * bytesPerChecksum; // exclusive
    bufferLength = Math.min(endIndex, length) - bufferOffset;
  }

  /**
   * Adjust the buffers position to account for seeked position and/ or checksum
   * boundary reads.
   * @param bufferPosition the position to which the buffers must be advanced
   */
  private void adjustBufferPosition(long bufferPosition) {
    // The bufferPosition is w.r.t the current chunk.
    // Adjust the bufferIndex and position to the seeked chunkPosition.
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

    // Reset the chunkPosition as chunk stream has been initialized i.e. the
    // buffers have been allocated.
    resetPosition();
  }

  /**
   * Check if the buffers have been allocated data and false otherwise.
   */
  private boolean buffersAllocated() {
    return buffers != null && !buffers.isEmpty();
  }

  /**
   * Check if the buffers have any data remaining between the current
   * position and the limit.
   */
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

  /**
   * Check if curernt buffers have the data corresponding to the input position.
   */
  private boolean buffersHavePosition(long pos) {
    // Check if buffers have been allocated
    if (buffersAllocated()) {
      // Check if the current buffers cover the input position
      return pos >= bufferOffset &&
          pos < bufferOffset + bufferLength;
    }
    return false;
  }

  /**
   * Check if there is more data in the chunk which has not yet been read
   * into the buffers.
   */
  private boolean dataRemainingInChunk() {
    long bufferPos;
    if (chunkPosition >= 0) {
      bufferPos = chunkPosition;
    } else {
      bufferPos = bufferOffset + bufferLength;
    }

    return bufferPos < length;
  }

  /**
   * Check if end of chunkStream has been reached.
   */
  private boolean chunkStreamEOF() {
    if (!allocated) {
      // Chunk data has not been read yet
      return false;
    }

    if (buffersHaveData() || dataRemainingInChunk()) {
      return false;
    } else {
      Preconditions.checkState(bufferOffset + bufferLength == length,
          "EOF detected, but not at the last byte of the chunk");
      return true;
    }
  }

  /**
   * If EOF is reached, release the buffers.
   */
  private void releaseBuffers() {
    buffers = null;
    bufferIndex = 0;
  }

  /**
   * Reset the chunkPosition once the buffers are allocated.
   */
  void resetPosition() {
    this.chunkPosition = -1;
  }

  String getChunkName() {
    return chunkInfo.getChunkName();
  }

  protected long getLength() {
    return length;
  }

  @VisibleForTesting
  protected long getChunkPosition() {
    return chunkPosition;
  }
}
