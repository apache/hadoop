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
package org.apache.hadoop.ozone.client.io;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Maintaining a list of BlockInputStream. Read based on offset.
 */
public class KeyInputStream extends InputStream implements Seekable {

  private static final Logger LOG =
      LoggerFactory.getLogger(KeyInputStream.class);

  private static final int EOF = -1;

  private final ArrayList<ChunkInputStreamEntry> streamEntries;
  // streamOffset[i] stores the offset at which blockInputStream i stores
  // data in the key
  private long[] streamOffset = null;
  private int currentStreamIndex;
  private long length = 0;
  private boolean closed = false;
  private String key;

  public KeyInputStream() {
    streamEntries = new ArrayList<>();
    currentStreamIndex = 0;
  }

  @VisibleForTesting
  public synchronized int getCurrentStreamIndex() {
    return currentStreamIndex;
  }

  @VisibleForTesting
  public long getRemainingOfIndex(int index) throws IOException {
    return streamEntries.get(index).getRemaining();
  }

  /**
   * Append another stream to the end of the list.
   *
   * @param stream       the stream instance.
   * @param streamLength the max number of bytes that should be written to this
   *                     stream.
   */
  public synchronized void addStream(BlockInputStream stream,
      long streamLength) {
    streamEntries.add(new ChunkInputStreamEntry(stream, streamLength));
  }


  @Override
  public synchronized int read() throws IOException {
    byte[] buf = new byte[1];
    if (read(buf, 0, 1) == EOF) {
      return EOF;
    }
    return Byte.toUnsignedInt(buf[0]);
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    checkNotClosed();
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return 0;
    }
    int totalReadLen = 0;
    while (len > 0) {
      // if we are at the last block and have read the entire block, return
      if (streamEntries.size() == 0 ||
              (streamEntries.size() - 1 <= currentStreamIndex &&
                      streamEntries.get(currentStreamIndex)
                              .getRemaining() == 0)) {
        return totalReadLen == 0 ? EOF : totalReadLen;
      }
      ChunkInputStreamEntry current = streamEntries.get(currentStreamIndex);
      int numBytesToRead = Math.min(len, (int)current.getRemaining());
      int numBytesRead = current.read(b, off, numBytesToRead);
      if (numBytesRead != numBytesToRead) {
        // This implies that there is either data loss or corruption in the
        // chunk entries. Even EOF in the current stream would be covered in
        // this case.
        throw new IOException(String.format(
            "Inconsistent read for blockID=%s length=%d numBytesRead=%d",
            current.blockInputStream.getBlockID(), current.length,
            numBytesRead));
      }
      totalReadLen += numBytesRead;
      off += numBytesRead;
      len -= numBytesRead;
      if (current.getRemaining() <= 0 &&
          ((currentStreamIndex + 1) < streamEntries.size())) {
        currentStreamIndex += 1;
      }
    }
    return totalReadLen;
  }

  @Override
  public void seek(long pos) throws IOException {
    checkNotClosed();
    if (pos < 0 || pos >= length) {
      if (pos == 0) {
        // It is possible for length and pos to be zero in which case
        // seek should return instead of throwing exception
        return;
      }
      throw new EOFException(
          "EOF encountered at pos: " + pos + " for key: " + key);
    }
    Preconditions.assertTrue(currentStreamIndex >= 0);
    if (currentStreamIndex >= streamEntries.size()) {
      currentStreamIndex = Arrays.binarySearch(streamOffset, pos);
    } else if (pos < streamOffset[currentStreamIndex]) {
      currentStreamIndex =
          Arrays.binarySearch(streamOffset, 0, currentStreamIndex, pos);
    } else if (pos >= streamOffset[currentStreamIndex] + streamEntries
        .get(currentStreamIndex).length) {
      currentStreamIndex = Arrays
          .binarySearch(streamOffset, currentStreamIndex + 1,
              streamEntries.size(), pos);
    }
    if (currentStreamIndex < 0) {
      // Binary search returns -insertionPoint - 1  if element is not present
      // in the array. insertionPoint is the point at which element would be
      // inserted in the sorted array. We need to adjust the currentStreamIndex
      // accordingly so that currentStreamIndex = insertionPoint - 1
      currentStreamIndex = -currentStreamIndex - 2;
    }
    // seek to the proper offset in the BlockInputStream
    streamEntries.get(currentStreamIndex)
        .seek(pos - streamOffset[currentStreamIndex]);
  }

  @Override
  public long getPos() throws IOException {
    return length == 0 ? 0 :
        streamOffset[currentStreamIndex] + streamEntries.get(currentStreamIndex)
            .getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public int available() throws IOException {
    checkNotClosed();
    long remaining = length - getPos();
    return remaining <= Integer.MAX_VALUE ? (int) remaining : Integer.MAX_VALUE;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    for (int i = 0; i < streamEntries.size(); i++) {
      streamEntries.get(i).close();
    }
  }

  /**
   * Encapsulates BlockInputStream.
   */
  public static class ChunkInputStreamEntry extends InputStream
      implements Seekable {

    private final BlockInputStream blockInputStream;
    private final long length;

    public ChunkInputStreamEntry(BlockInputStream blockInputStream,
        long length) {
      this.blockInputStream = blockInputStream;
      this.length = length;
    }

    synchronized long getRemaining() throws IOException {
      return length - getPos();
    }

    @Override
    public synchronized int read(byte[] b, int off, int len)
        throws IOException {
      int readLen = blockInputStream.read(b, off, len);
      return readLen;
    }

    @Override
    public synchronized int read() throws IOException {
      int data = blockInputStream.read();
      return data;
    }

    @Override
    public synchronized void close() throws IOException {
      blockInputStream.close();
    }

    @Override
    public void seek(long pos) throws IOException {
      blockInputStream.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
      return blockInputStream.getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }
  }

  public static LengthInputStream getFromOmKeyInfo(
      OmKeyInfo keyInfo,
      XceiverClientManager xceiverClientManager,
      StorageContainerLocationProtocol
          storageContainerLocationClient,
      String requestId, boolean verifyChecksum) throws IOException {
    long length = 0;
    long containerKey;
    KeyInputStream groupInputStream = new KeyInputStream();
    groupInputStream.key = keyInfo.getKeyName();
    List<OmKeyLocationInfo> keyLocationInfos =
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
    groupInputStream.streamOffset = new long[keyLocationInfos.size()];
    for (int i = 0; i < keyLocationInfos.size(); i++) {
      OmKeyLocationInfo omKeyLocationInfo = keyLocationInfos.get(i);
      BlockID blockID = omKeyLocationInfo.getBlockID();
      long containerID = blockID.getContainerID();
      Pipeline pipeline = omKeyLocationInfo.getPipeline();

      // irrespective of the container state, we will always read via Standalone
      // protocol.
      if (pipeline.getType() != HddsProtos.ReplicationType.STAND_ALONE) {
        pipeline = Pipeline.newBuilder(pipeline)
            .setType(HddsProtos.ReplicationType.STAND_ALONE).build();
      }
      XceiverClientSpi xceiverClient = xceiverClientManager
          .acquireClient(pipeline);
      boolean success = false;
      containerKey = omKeyLocationInfo.getLocalID();
      try {
        LOG.debug("get key accessing {} {}",
            containerID, containerKey);
        groupInputStream.streamOffset[i] = length;
        ContainerProtos.DatanodeBlockID datanodeBlockID = blockID
            .getDatanodeBlockIDProtobuf();
        if (omKeyLocationInfo.getToken() != null) {
          UserGroupInformation.getCurrentUser().
              addToken(omKeyLocationInfo.getToken());
        }
        ContainerProtos.GetBlockResponseProto response = ContainerProtocolCalls
            .getBlock(xceiverClient, datanodeBlockID, requestId);
        List<ContainerProtos.ChunkInfo> chunks =
            response.getBlockData().getChunksList();
        for (ContainerProtos.ChunkInfo chunk : chunks) {
          length += chunk.getLen();
        }
        success = true;
        BlockInputStream inputStream = new BlockInputStream(
            omKeyLocationInfo.getBlockID(), xceiverClientManager, xceiverClient,
            chunks, requestId, verifyChecksum);
        groupInputStream.addStream(inputStream,
            omKeyLocationInfo.getLength());
      } finally {
        if (!success) {
          xceiverClientManager.releaseClient(xceiverClient, false);
        }
      }
    }
    groupInputStream.length = length;
    return new LengthInputStream(groupInputStream, length);
  }

  /**
   * Verify that the input stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(
          ": " + FSExceptionMessages.STREAM_IS_CLOSED + " Key: " + key);
    }
  }
}
