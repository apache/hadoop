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
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfo;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.storage.ChunkInputStream;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Maintaining a list of ChunkInputStream. Read based on offset.
 */
public class ChunkGroupInputStream extends InputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChunkGroupInputStream.class);

  private static final int EOF = -1;

  private final ArrayList<ChunkInputStreamEntry> streamEntries;
  private int currentStreamIndex;

  public ChunkGroupInputStream() {
    streamEntries = new ArrayList<>();
    currentStreamIndex = 0;
  }

  @VisibleForTesting
  public synchronized int getCurrentStreamIndex() {
    return currentStreamIndex;
  }

  @VisibleForTesting
  public long getRemainingOfIndex(int index) {
    return streamEntries.get(index).getRemaining();
  }

  /**
   * Append another stream to the end of the list.
   *
   * @param stream the stream instance.
   * @param length the max number of bytes that should be written to this
   *               stream.
   */
  public synchronized void addStream(InputStream stream, long length) {
    streamEntries.add(new ChunkInputStreamEntry(stream, length));
  }


  @Override
  public synchronized int read() throws IOException {
    if (streamEntries.size() <= currentStreamIndex) {
      throw new IndexOutOfBoundsException();
    }
    ChunkInputStreamEntry entry = streamEntries.get(currentStreamIndex);
    int data = entry.read();
    return data;
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
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
      if (streamEntries.size() <= currentStreamIndex) {
        return totalReadLen == 0 ? EOF : totalReadLen;
      }
      ChunkInputStreamEntry current = streamEntries.get(currentStreamIndex);
      int readLen = Math.min(len, (int)current.getRemaining());
      int actualLen = current.read(b, off, readLen);
      // this means the underlying stream has nothing at all, return
      if (actualLen == EOF) {
        return totalReadLen > 0? totalReadLen : EOF;
      }
      totalReadLen += actualLen;
      // this means there is no more data to read beyond this point, return
      if (actualLen != readLen) {
        return totalReadLen;
      }
      off += readLen;
      len -= readLen;
      if (current.getRemaining() <= 0) {
        currentStreamIndex += 1;
      }
    }
    return totalReadLen;
  }

  private static class ChunkInputStreamEntry extends InputStream {

    private final InputStream inputStream;
    private final long length;
    private long currentPosition;


    ChunkInputStreamEntry(InputStream chunkInputStream, long length) {
      this.inputStream = chunkInputStream;
      this.length = length;
      this.currentPosition = 0;
    }

    synchronized long getRemaining() {
      return length - currentPosition;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len)
        throws IOException {
      int readLen = inputStream.read(b, off, len);
      currentPosition += readLen;
      return readLen;
    }

    @Override
    public synchronized int read() throws IOException {
      int data = inputStream.read();
      currentPosition += 1;
      return data;
    }

    @Override
    public synchronized void close() throws IOException {
      inputStream.close();
    }
  }

  public static LengthInputStream getFromKsmKeyInfo(KsmKeyInfo keyInfo,
      XceiverClientManager xceiverClientManager,
      StorageContainerLocationProtocolClientSideTranslatorPB
          storageContainerLocationClient, String requestId)
      throws IOException {
    long length = 0;
    String containerKey;
    ChunkGroupInputStream groupInputStream = new ChunkGroupInputStream();
    for (KsmKeyLocationInfo ksmKeyLocationInfo :
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly()) {
      String containerName = ksmKeyLocationInfo.getContainerName();
      Pipeline pipeline =
          storageContainerLocationClient.getContainer(containerName);
      XceiverClientSpi xceiverClient =
          xceiverClientManager.acquireClient(pipeline);
      boolean success = false;
      containerKey = ksmKeyLocationInfo.getBlockID();
      try {
        LOG.debug("get key accessing {} {}",
            xceiverClient.getPipeline().getContainerName(), containerKey);
        ContainerProtos.KeyData containerKeyData = OzoneContainerTranslation
            .containerKeyDataForRead(
                xceiverClient.getPipeline().getContainerName(), containerKey);
        ContainerProtos.GetKeyResponseProto response = ContainerProtocolCalls
            .getKey(xceiverClient, containerKeyData, requestId);
        List<ContainerProtos.ChunkInfo> chunks =
            response.getKeyData().getChunksList();
        for (ContainerProtos.ChunkInfo chunk : chunks) {
          length += chunk.getLen();
        }
        success = true;
        ChunkInputStream inputStream = new ChunkInputStream(
            containerKey, xceiverClientManager, xceiverClient,
            chunks, requestId);
        groupInputStream.addStream(inputStream,
            ksmKeyLocationInfo.getLength());
      } finally {
        if (!success) {
          xceiverClientManager.releaseClient(xceiverClient);
        }
      }
    }
    return new LengthInputStream(groupInputStream, length);
  }
}
