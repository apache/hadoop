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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.google.protobuf.ByteString;

import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.XceiverClientManager;

/**
 * An {@link InputStream} used by the REST service in combination with the
 * SCMClient to read the value of a key from a sequence
 * of container chunks.  All bytes of the key value are stored in container
 * chunks.  Each chunk may contain multiple underlying {@link ByteBuffer}
 * instances.  This class encapsulates all state management for iterating
 * through the sequence of chunks and the sequence of buffers within each chunk.
 */
public class ChunkInputStream extends InputStream implements Seekable {

  private static final int EOF = -1;

  private final String key;
  private final String traceID;
  private XceiverClientManager xceiverClientManager;
  private XceiverClientSpi xceiverClient;
  private List<ChunkInfo> chunks;
  private int chunkIndex;
  private long[] chunkOffset;
  private List<ByteBuffer> buffers;
  private int bufferIndex;

  /**
   * Creates a new ChunkInputStream.
   *
   * @param key chunk key
   * @param xceiverClientManager client manager that controls client
   * @param xceiverClient client to perform container calls
   * @param chunks list of chunks to read
   * @param traceID container protocol call traceID
   */
  public ChunkInputStream(String key, XceiverClientManager xceiverClientManager,
      XceiverClientSpi xceiverClient, List<ChunkInfo> chunks, String traceID) {
    this.key = key;
    this.traceID = traceID;
    this.xceiverClientManager = xceiverClientManager;
    this.xceiverClient = xceiverClient;
    this.chunks = chunks;
    this.chunkIndex = -1;
    // chunkOffset[i] stores offset at which chunk i stores data in
    // ChunkInputStream
    this.chunkOffset = new long[this.chunks.size()];
    initializeChunkOffset();
    this.buffers = null;
    this.bufferIndex = 0;
  }

  private void initializeChunkOffset() {
    int tempOffset = 0;
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
    return available == EOF ? EOF :
        Byte.toUnsignedInt(buffers.get(bufferIndex).get());
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
    int available = prepareRead(len);
    if (available == EOF) {
      return EOF;
    }
    buffers.get(bufferIndex).get(b, off, available);
    return available;
  }

  @Override
  public synchronized void close() {
    if (xceiverClientManager != null && xceiverClient != null) {
      xceiverClientManager.releaseClient(xceiverClient);
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
      throw new IOException("ChunkInputStream has been closed.");
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
      if (chunks == null || chunks.isEmpty()) {
        // This must be an empty key.
        return EOF;
      } else if (buffers == null) {
        // The first read triggers fetching the first chunk.
        readChunkFromContainer();
      } else if (!buffers.isEmpty() &&
          buffers.get(bufferIndex).hasRemaining()) {
        // Data is available from the current buffer.
        ByteBuffer bb = buffers.get(bufferIndex);
        return len > bb.remaining() ? bb.remaining() : len;
      } else if (!buffers.isEmpty() &&
          !buffers.get(bufferIndex).hasRemaining() &&
          bufferIndex < buffers.size() - 1) {
        // There are additional buffers available.
        ++bufferIndex;
      } else if (chunkIndex < chunks.size() - 1) {
        // There are additional chunks available.
        readChunkFromContainer();
      } else {
        // All available input has been consumed.
        return EOF;
      }
    }
  }

  /**
   * Attempts to read the chunk at the specified offset in the chunk list.  If
   * successful, then the data of the read chunk is saved so that its bytes can
   * be returned from subsequent read calls.
   *
   * @throws IOException if there is an I/O error while performing the call
   */
  private synchronized void readChunkFromContainer() throws IOException {
    // On every chunk read chunkIndex should be increased so as to read the
    // next chunk
    chunkIndex += 1;
    final ReadChunkResponseProto readChunkResponse;
    try {
      readChunkResponse = ContainerProtocolCalls.readChunk(xceiverClient,
          chunks.get(chunkIndex), key, traceID);
    } catch (IOException e) {
      throw new IOException("Unexpected OzoneException: " + e.toString(), e);
    }
    ByteString byteString = readChunkResponse.getData();
    buffers = byteString.asReadOnlyByteBufferList();
    bufferIndex = 0;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    if (pos < 0 || (chunks.size() == 0 && pos > 0)
        || pos >= chunkOffset[chunks.size() - 1] + chunks.get(chunks.size() - 1)
        .getLen()) {
      throw new EOFException(
          "EOF encountered pos: " + pos + " container key: " + key);
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
    // adjust chunkIndex so that readChunkFromContainer reads the correct chunk
    chunkIndex -= 1;
    readChunkFromContainer();
    adjustBufferIndex(pos);
  }

  private void adjustBufferIndex(long pos) {
    long tempOffest = chunkOffset[chunkIndex];
    for (int i = 0; i < buffers.size(); i++) {
      if (pos - tempOffest >= buffers.get(i).capacity()) {
        tempOffest += buffers.get(i).capacity();
      } else {
        bufferIndex = i;
        break;
      }
    }
    buffers.get(bufferIndex).position((int) (pos - tempOffest));
  }

  @Override
  public synchronized long getPos() throws IOException {
    return chunkIndex == -1 ? 0 :
        chunkOffset[chunkIndex] + buffers.get(bufferIndex).position();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }
}
