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
package org.apache.hadoop.ozone.web.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Result;
import org.apache.hadoop.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ksm.helpers.KsmKeyLocationInfo;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.storage.ChunkOutputStream;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Maintaining a list of ChunkInputStream. Write based on offset.
 *
 * Note that this may write to multiple containers in one write call. In case
 * that first container succeeded but later ones failed, the succeeded writes
 * are not rolled back.
 *
 * TODO : currently not support multi-thread access.
 */
public class ChunkGroupOutputStream extends OutputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChunkGroupOutputStream.class);

  // array list's get(index) is O(1)
  private final ArrayList<ChunkOutputStreamEntry> streamEntries;
  private int currentStreamIndex;
  private long totalSize;
  private long byteOffset;

  //This has to be removed once HDFS-11888 is resolved.
  //local cache which will have list of created container names.
  private static Set<String> containersCreated = new HashSet<>();

  public ChunkGroupOutputStream() {
    this.streamEntries = new ArrayList<>();
    this.currentStreamIndex = 0;
    this.totalSize = 0;
    this.byteOffset = 0;
  }

  @VisibleForTesting
  public long getByteOffset() {
    return byteOffset;
  }

  /**
   * Append another stream to the end of the list. Note that the streams are not
   * actually created to this point, only enough meta data about the stream is
   * stored. When something is to be actually written to the stream, the stream
   * will be created (if not already).
   *
   * @param containerKey the key to store in the container
   * @param key the ozone key
   * @param xceiverClientManager xceiver manager instance
   * @param xceiverClient xceiver manager instance
   * @param requestID the request id
   * @param chunkSize the chunk size for this key chunks
   * @param length the total length of this key
   */
  public synchronized void addStream(String containerKey, String key,
      XceiverClientManager xceiverClientManager, XceiverClientSpi xceiverClient,
      String requestID, int chunkSize, long length) {
    streamEntries.add(new ChunkOutputStreamEntry(containerKey, key,
        xceiverClientManager, xceiverClient, requestID, chunkSize, length));
    totalSize += length;
  }

  @VisibleForTesting
  public synchronized void addStream(OutputStream outputStream, long length) {
    streamEntries.add(new ChunkOutputStreamEntry(outputStream, length));
    totalSize += length;
  }

  @Override
  public synchronized void write(int b) throws IOException {
    if (streamEntries.size() <= currentStreamIndex) {
      throw new IndexOutOfBoundsException();
    }
    ChunkOutputStreamEntry entry = streamEntries.get(currentStreamIndex);
    entry.write(b);
    if (entry.getRemaining() <= 0) {
      currentStreamIndex += 1;
    }
    byteOffset += 1;
  }

  /**
   * Try to write the bytes sequence b[off:off+len) to streams.
   *
   * NOTE: Throws exception if the data could not fit into the remaining space.
   * In which case nothing will be written.
   * TODO:May need to revisit this behaviour.
   *
   * @param b byte data
   * @param off starting offset
   * @param len length to write
   * @throws IOException
   */
  @Override
  public synchronized void write(byte[] b, int off, int len)
      throws IOException {
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
    if (streamEntries.size() <= currentStreamIndex) {
      throw new IOException("Write out of stream range! stream index:" +
          currentStreamIndex);
    }
    if (totalSize - byteOffset < len) {
      throw new IOException("Can not write " + len + " bytes with only " +
          (totalSize - byteOffset) + " byte space");
    }
    while (len > 0) {
      // in theory, this condition should never violate due the check above
      // still do a sanity check.
      Preconditions.checkArgument(currentStreamIndex < streamEntries.size());
      ChunkOutputStreamEntry current = streamEntries.get(currentStreamIndex);
      int writeLen = Math.min(len, (int)current.getRemaining());
      current.write(b, off, writeLen);
      if (current.getRemaining() <= 0) {
        currentStreamIndex += 1;
      }
      len -= writeLen;
      off += writeLen;
      byteOffset += writeLen;
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    for (int i = 0; i <= currentStreamIndex; i++) {
      streamEntries.get(i).flush();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    for (ChunkOutputStreamEntry entry : streamEntries) {
      entry.close();
    }
  }

  private static class ChunkOutputStreamEntry extends OutputStream {
    private OutputStream outputStream;
    private final String containerKey;
    private final String key;
    private final XceiverClientManager xceiverClientManager;
    private final XceiverClientSpi xceiverClient;
    private final String requestId;
    private final int chunkSize;
    // total number of bytes that should be written to this stream
    private final long length;
    // the current position of this stream 0 <= currentPosition < length
    private long currentPosition;

    ChunkOutputStreamEntry(String containerKey, String key,
        XceiverClientManager xceiverClientManager,
        XceiverClientSpi xceiverClient, String requestId, int chunkSize,
        long length) {
      this.outputStream = null;
      this.containerKey = containerKey;
      this.key = key;
      this.xceiverClientManager = xceiverClientManager;
      this.xceiverClient = xceiverClient;
      this.requestId = requestId;
      this.chunkSize = chunkSize;

      this.length = length;
      this.currentPosition = 0;
    }

    /**
     * For testing purpose, taking a some random created stream instance.
     * @param  outputStream a existing writable output stream
     * @param  length the length of data to write to the stream
     */
    ChunkOutputStreamEntry(OutputStream outputStream, long length) {
      this.outputStream = outputStream;
      this.containerKey = null;
      this.key = null;
      this.xceiverClientManager = null;
      this.xceiverClient = null;
      this.requestId = null;
      this.chunkSize = -1;

      this.length = length;
      this.currentPosition = 0;
    }

    long getLength() {
      return length;
    }

    long getRemaining() {
      return length - currentPosition;
    }

    private synchronized void checkStream() {
      if (this.outputStream == null) {
        this.outputStream = new ChunkOutputStream(containerKey,
            key, xceiverClientManager, xceiverClient,
            requestId, chunkSize);
      }
    }

    @Override
    public void write(int b) throws IOException {
      checkStream();
      outputStream.write(b);
      this.currentPosition += 1;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      checkStream();
      outputStream.write(b, off, len);
      this.currentPosition += len;
    }

    @Override
    public void flush() throws IOException {
      if (this.outputStream != null) {
        this.outputStream.flush();
      }
    }

    @Override
    public void close() throws IOException {
      if (this.outputStream != null) {
        this.outputStream.close();
      }
    }
  }

  public static ChunkGroupOutputStream getFromKsmKeyInfo(
      KsmKeyInfo keyInfo, XceiverClientManager xceiverClientManager,
      StorageContainerLocationProtocolClientSideTranslatorPB
          storageContainerLocationClient,
      int chunkSize, String requestId) throws IOException {
    // TODO: the following createContainer and key writes may fail, in which
    // case we should revert the above allocateKey to KSM.
    // check index as sanity check
    int index = 0;
    String containerKey;
    ChunkGroupOutputStream groupOutputStream = new ChunkGroupOutputStream();
    for (KsmKeyLocationInfo subKeyInfo : keyInfo.getKeyLocationList()) {
      containerKey = subKeyInfo.getBlockID();

      Preconditions.checkArgument(index++ == subKeyInfo.getIndex());
      String containerName = subKeyInfo.getContainerName();
      Pipeline pipeline =
          storageContainerLocationClient.getContainer(containerName);
      XceiverClientSpi xceiverClient =
          xceiverClientManager.acquireClient(pipeline);
      // create container if needed
      // TODO : should be subKeyInfo.getShouldCreateContainer(), but for now
      //The following change has to reverted once HDFS-11888 is fixed.
      if(!containersCreated.contains(containerName)) {
        synchronized (containerName.intern()) {
          //checking again, there is a chance that some other thread has
          // created it.
          if (!containersCreated.contains(containerName)) {
            LOG.debug("Need to create container {}.", containerName);
            try {
              ContainerProtocolCalls.createContainer(xceiverClient, requestId);
            } catch (StorageContainerException ex) {
              if (ex.getResult().equals(Result.CONTAINER_EXISTS)) {
                //container already exist.
                LOG.debug("Container {} already exists.", containerName);
              } else {
                LOG.error("Container creation failed for {}.",
                    containerName, ex);
                throw ex;
              }
            }
            containersCreated.add(containerName);
          }
        }
      }

      groupOutputStream.addStream(containerKey, keyInfo.getKeyName(),
          xceiverClientManager, xceiverClient, requestId, chunkSize,
          subKeyInfo.getLength());
    }
    return groupOutputStream;
  }
}
