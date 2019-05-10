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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ReadChunkResponseProto;
import org.apache.hadoop.hdds.client.BlockID;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
  private int chunkIndex;
  private long[] chunkOffset;
  private List<ByteBuffer> buffers;
  private int bufferIndex;
  private int chunkIndexOfCurrentBuffer;
  private long bufferPosition;
  private final boolean verifyChecksum;

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
    this.chunkIndex = -1;
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
  private boolean blockStreamEOF() {
    if ((buffersAllocated() && buffersHaveData()) || chunksRemaining()) {
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
    chunkIndexOfCurrentBuffer = -1;
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

    return hasData;
  }

  private boolean buffersRemaining() {
    return (bufferIndex < (buffers.size() - 1));
  }

  private boolean chunksRemaining() {
    if ((chunks == null) || chunks.isEmpty()) {
      return false;
    }
    return (chunkIndex < (chunks.size() - 1));
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
    XceiverClientReply reply;
    ReadChunkResponseProto readChunkResponse = null;
    final ChunkInfo chunkInfo = chunks.get(chunkIndex);
    List<DatanodeDetails> excludeDns = null;
    ByteString byteString;
    List<DatanodeDetails> dnList = xceiverClient.getPipeline().getNodes();
    while (true) {
      try {
        reply = ContainerProtocolCalls
            .readChunk(xceiverClient, chunkInfo, blockID, traceID, excludeDns);
        ContainerProtos.ContainerCommandResponseProto response;
        response = reply.getResponse().get();
        ContainerProtocolCalls.validateContainerResponse(response);
        readChunkResponse = response.getReadChunk();
      } catch (IOException e) {
        if (e instanceof StorageContainerException) {
          throw e;
        }
        throw new IOException("Unexpected OzoneException: " + e.toString(), e);
      } catch (ExecutionException | InterruptedException e) {
        throw new IOException(
            "Failed to execute ReadChunk command for chunk  " + chunkInfo
                .getChunkName(), e);
      }
      byteString = readChunkResponse.getData();
      try {
        if (byteString.size() != chunkInfo.getLen()) {
          // Bytes read from chunk should be equal to chunk size.
          throw new IOException(String
              .format("Inconsistent read for chunk=%s len=%d bytesRead=%d",
                  chunkInfo.getChunkName(), chunkInfo.getLen(),
                  byteString.size()));
        }
        ChecksumData checksumData =
            ChecksumData.getFromProtoBuf(chunkInfo.getChecksumData());
        if (verifyChecksum) {
          Checksum.verifyChecksum(byteString, checksumData);
        }
        break;
      } catch (IOException ioe) {
        // we will end up in this situation only if the checksum mismatch
        // happens or the length of the chunk mismatches.
        // In this case, read should be retried on a different replica.
        // TODO: Inform SCM of a possible corrupt container replica here
        if (excludeDns == null) {
          excludeDns = new ArrayList<>();
        }
        excludeDns.addAll(reply.getDatanodes());
        if (excludeDns.size() == dnList.size()) {
          throw ioe;
        }
      }
    }

    buffers = byteString.asReadOnlyByteBufferList();
    bufferIndex = 0;
    chunkIndexOfCurrentBuffer = chunkIndex;

    // The bufferIndex and position might need to be adjusted if seek() was
    // called on the stream before. This needs to be done so that the buffer
    // position can be advanced to the 'seeked' position.
    adjustBufferIndex();
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    if (pos < 0 || (chunks.size() == 0 && pos > 0)
        || pos >= chunkOffset[chunks.size() - 1] + chunks.get(chunks.size() - 1)
        .getLen()) {
      throw new EOFException("EOF encountered pos: " + pos + " container key: "
          + blockID.getLocalID());
    }
    if (chunkIndex == -1) {
      chunkIndex = Arrays.binarySearch(chunkOffset, pos);
    } else if (pos < chunkOffset[chunkIndex]) {
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
    if (bufferPosition >= 0) {
      // seek has been called but the buffers were empty. Hence, the buffer
      // position will be advanced after the buffers are filled.
      // We return the chunkOffset + bufferPosition here as that will be the
      // position of the buffer pointer after reading the chunk file.
      return chunkOffset[chunkIndex] + bufferPosition;
    }

    if (chunkIndex == -1) {
      // no data consumed yet, a new stream
      return 0;
    }

    if (blockStreamEOF()) {
      // all data consumed, buffers have been released.
      // get position from the chunk offset and chunk length of last chunk
      return chunkOffset[chunkIndex] + chunks.get(chunkIndex).getLen();
    }

    // get position from available buffers of current chunk
    return chunkOffset[chunkIndex] + buffers.get(bufferIndex).position();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  public BlockID getBlockID() {
    return blockID;
  }
}
