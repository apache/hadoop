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
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ObjectStageChangeRequestProto;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.hdds.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.ChunkOutputStream;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ListIterator;

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

  public static final Logger LOG =
      LoggerFactory.getLogger(ChunkGroupOutputStream.class);

  // array list's get(index) is O(1)
  private final ArrayList<ChunkOutputStreamEntry> streamEntries;
  private int currentStreamIndex;
  private long byteOffset;
  private final OzoneManagerProtocolClientSideTranslatorPB omClient;
  private final
      StorageContainerLocationProtocolClientSideTranslatorPB scmClient;
  private final OmKeyArgs keyArgs;
  private final long openID;
  private final XceiverClientManager xceiverClientManager;
  private final int chunkSize;
  private final String requestID;
  private boolean closed;
  private final RetryPolicy retryPolicy;
  /**
   * A constructor for testing purpose only.
   */
  @VisibleForTesting
  public ChunkGroupOutputStream() {
    streamEntries = new ArrayList<>();
    omClient = null;
    scmClient = null;
    keyArgs = null;
    openID = -1;
    xceiverClientManager = null;
    chunkSize = 0;
    requestID = null;
    closed = false;
    retryPolicy = null;
  }

  /**
   * For testing purpose only. Not building output stream from blocks, but
   * taking from externally.
   *
   * @param outputStream
   * @param length
   */
  @VisibleForTesting
  public void addStream(OutputStream outputStream, long length) {
    streamEntries.add(new ChunkOutputStreamEntry(outputStream, length));
  }

  @VisibleForTesting
  public List<ChunkOutputStreamEntry> getStreamEntries() {
    return streamEntries;
  }

  public List<OmKeyLocationInfo> getLocationInfoList() {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    for (ChunkOutputStreamEntry streamEntry : streamEntries) {
      OmKeyLocationInfo info =
          new OmKeyLocationInfo.Builder().setBlockID(streamEntry.blockID)
              .setShouldCreateContainer(false)
              .setLength(streamEntry.currentPosition).setOffset(0).build();
      locationInfoList.add(info);
    }
    return locationInfoList;
  }

  public ChunkGroupOutputStream(
      OpenKeySession handler, XceiverClientManager xceiverClientManager,
      StorageContainerLocationProtocolClientSideTranslatorPB scmClient,
      OzoneManagerProtocolClientSideTranslatorPB omClient,
      int chunkSize, String requestId, ReplicationFactor factor,
      ReplicationType type, RetryPolicy retryPolicy) throws IOException {
    this.streamEntries = new ArrayList<>();
    this.currentStreamIndex = 0;
    this.byteOffset = 0;
    this.omClient = omClient;
    this.scmClient = scmClient;
    OmKeyInfo info = handler.getKeyInfo();
    this.keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(info.getVolumeName())
        .setBucketName(info.getBucketName())
        .setKeyName(info.getKeyName())
        .setType(type)
        .setFactor(factor)
        .setDataSize(info.getDataSize()).build();
    this.openID = handler.getId();
    this.xceiverClientManager = xceiverClientManager;
    this.chunkSize = chunkSize;
    this.requestID = requestId;
    this.retryPolicy = retryPolicy;
    LOG.debug("Expecting open key with one block, but got" +
        info.getKeyLocationVersions().size());
  }

  /**
   * When a key is opened, it is possible that there are some blocks already
   * allocated to it for this open session. In this case, to make use of these
   * blocks, we need to add these blocks to stream entries. But, a key's version
   * also includes blocks from previous versions, we need to avoid adding these
   * old blocks to stream entries, because these old blocks should not be picked
   * for write. To do this, the following method checks that, only those
   * blocks created in this particular open version are added to stream entries.
   *
   * @param version the set of blocks that are pre-allocated.
   * @param openVersion the version corresponding to the pre-allocation.
   * @throws IOException
   */
  public void addPreallocateBlocks(OmKeyLocationInfoGroup version,
      long openVersion) throws IOException {
    // server may return any number of blocks, (0 to any)
    // only the blocks allocated in this open session (block createVersion
    // equals to open session version)
    for (OmKeyLocationInfo subKeyInfo : version.getLocationList()) {
      if (subKeyInfo.getCreateVersion() == openVersion) {
        checkKeyLocationInfo(subKeyInfo);
      }
    }
  }

  private void checkKeyLocationInfo(OmKeyLocationInfo subKeyInfo)
      throws IOException {
    ContainerWithPipeline containerWithPipeline = scmClient
        .getContainerWithPipeline(subKeyInfo.getContainerID());
    ContainerInfo container = containerWithPipeline.getContainerInfo();

    XceiverClientSpi xceiverClient =
        xceiverClientManager.acquireClient(containerWithPipeline.getPipeline(),
            container.getContainerID());
    // create container if needed
    if (subKeyInfo.getShouldCreateContainer()) {
      try {
        ContainerProtocolCalls.createContainer(xceiverClient,
            container.getContainerID(), requestID);
        scmClient.notifyObjectStageChange(
            ObjectStageChangeRequestProto.Type.container,
            subKeyInfo.getContainerID(),
            ObjectStageChangeRequestProto.Op.create,
            ObjectStageChangeRequestProto.Stage.complete);
      } catch (StorageContainerException ex) {
        if (ex.getResult().equals(Result.CONTAINER_EXISTS)) {
          //container already exist, this should never happen
          LOG.debug("Container {} already exists.",
              container.getContainerID());
        } else {
          LOG.error("Container creation failed for {}.",
              container.getContainerID(), ex);
          throw ex;
        }
      }
    }
    streamEntries.add(new ChunkOutputStreamEntry(subKeyInfo.getBlockID(),
        keyArgs.getKeyName(), xceiverClientManager, xceiverClient, requestID,
        chunkSize, subKeyInfo.getLength()));
  }

  @VisibleForTesting
  public long getByteOffset() {
    return byteOffset;
  }


  @Override
  public void write(int b) throws IOException {
    byte[] buf = new byte[1];
    buf[0] = (byte) b;
    write(buf, 0, 1);
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
  public void write(byte[] b, int off, int len)
      throws IOException {
    checkNotClosed();
    handleWrite(b, off, len);
  }

  private void handleWrite(byte[] b, int off, int len) throws IOException {
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
    int succeededAllocates = 0;
    while (len > 0) {
      if (streamEntries.size() <= currentStreamIndex) {
        Preconditions.checkNotNull(omClient);
        // allocate a new block, if a exception happens, log an error and
        // throw exception to the caller directly, and the write fails.
        try {
          allocateNewBlock(currentStreamIndex);
          succeededAllocates += 1;
        } catch (IOException ioe) {
          LOG.error("Try to allocate more blocks for write failed, already " +
              "allocated " + succeededAllocates + " blocks for this write.");
          throw ioe;
        }
      }
      // in theory, this condition should never violate due the check above
      // still do a sanity check.
      Preconditions.checkArgument(currentStreamIndex < streamEntries.size());
      ChunkOutputStreamEntry current = streamEntries.get(currentStreamIndex);
      int writeLen = Math.min(len, (int) current.getRemaining());
      try {
        current.write(b, off, writeLen);
      } catch (IOException ioe) {
        if (checkIfContainerIsClosed(ioe)) {
          handleCloseContainerException(current, currentStreamIndex);
          continue;
        } else {
          throw ioe;
        }
      }
      if (current.getRemaining() <= 0) {
        // since the current block is already written close the stream.
        handleFlushOrClose(true);
        currentStreamIndex += 1;
      }
      len -= writeLen;
      off += writeLen;
      byteOffset += writeLen;
    }
  }

  private long getCommittedBlockLength(ChunkOutputStreamEntry streamEntry)
      throws IOException {
    long blockLength;
    ContainerProtos.GetCommittedBlockLengthResponseProto responseProto;
    RetryPolicy.RetryAction action;
    int numRetries = 0;
    while (true) {
      try {
        responseProto = ContainerProtocolCalls
            .getCommittedBlockLength(streamEntry.xceiverClient,
                streamEntry.blockID, requestID);
        blockLength = responseProto.getBlockLength();
        return blockLength;
      } catch (StorageContainerException sce) {
        try {
          action = retryPolicy.shouldRetry(sce, numRetries, 0, true);
        } catch (Exception e) {
          throw e instanceof IOException ? (IOException) e : new IOException(e);
        }
        if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
          if (action.reason != null) {
            LOG.error(
                "GetCommittedBlockLength request failed. " + action.reason,
                sce);
          }
          throw sce;
        }

        // Throw the exception if the thread is interrupted
        if (Thread.currentThread().isInterrupted()) {
          LOG.warn("Interrupted while trying for connection");
          throw sce;
        }
        Preconditions.checkArgument(
            action.action == RetryPolicy.RetryAction.RetryDecision.RETRY);
        try {
          Thread.sleep(action.delayMillis);
        } catch (InterruptedException e) {
          throw (IOException) new InterruptedIOException(
              "Interrupted: action=" + action + ", retry policy=" + retryPolicy)
              .initCause(e);
        }
        numRetries++;
        LOG.trace("Retrying GetCommittedBlockLength request. Already tried "
            + numRetries + " time(s); retry policy is " + retryPolicy);
        continue;
      }
    }
  }

  /**
   * Discards the subsequent pre allocated blocks and removes the streamEntries
   * from the streamEntries list for the container which is closed.
   * @param containerID id of the closed container
   */
  private void discardPreallocatedBlocks(long containerID) {
    // currentStreamIndex < streamEntries.size() signifies that, there are still
    // pre allocated blocks available.
    if (currentStreamIndex < streamEntries.size()) {
      ListIterator<ChunkOutputStreamEntry> streamEntryIterator =
          streamEntries.listIterator(currentStreamIndex);
      while (streamEntryIterator.hasNext()) {
        if (streamEntryIterator.next().blockID.getContainerID()
            == containerID) {
          streamEntryIterator.remove();
        }
      }
    }
  }

  /**
   * It might be possible that the blocks pre allocated might never get written
   * while the stream gets closed normally. In such cases, it would be a good
   * idea to trim down the locationInfoList by removing the unused blocks if any
   * so as only the used block info gets updated on OzoneManager during close.
   */
  private void removeEmptyBlocks() {
    if (currentStreamIndex < streamEntries.size()) {
      ListIterator<ChunkOutputStreamEntry> streamEntryIterator =
          streamEntries.listIterator(currentStreamIndex);
      while (streamEntryIterator.hasNext()) {
        if (streamEntryIterator.next().currentPosition == 0) {
          streamEntryIterator.remove();
        }
      }
    }
  }
  /**
   * It performs following actions :
   * a. Updates the committed length at datanode for the current stream in
   *    datanode.
   * b. Reads the data from the underlying buffer and writes it the next stream.
   *
   * @param streamEntry StreamEntry
   * @param streamIndex Index of the entry
   * @throws IOException Throws IOexception if Write fails
   */
  private void handleCloseContainerException(ChunkOutputStreamEntry streamEntry,
      int streamIndex) throws IOException {
    long committedLength = 0;
    ByteBuffer buffer = streamEntry.getBuffer();
    if (buffer == null) {
      // the buffer here will be null only when closeContainerException is
      // hit while calling putKey during close on chunkOutputStream.
      // Since closeContainer auto commit pending keys, no need to do
      // anything here.
      return;
    }

    // In case where not a single chunk of data has been written to the Datanode
    // yet. This block does not yet exist on the datanode but cached on the
    // outputStream buffer. No need to call GetCommittedBlockLength here
    // for this block associated with the stream here.
    if (streamEntry.currentPosition >= chunkSize
        || streamEntry.currentPosition != buffer.position()) {
      committedLength = getCommittedBlockLength(streamEntry);
      // update the length of the current stream
      streamEntry.currentPosition = committedLength;
    }

    if (buffer.position() > 0) {
      // If the data is still cached in the underlying stream, we need to
      // allocate new block and write this data in the datanode. The cached
      // data in the buffer does not exceed chunkSize.
      Preconditions.checkState(buffer.position() < chunkSize);
      currentStreamIndex += 1;
      // readjust the byteOffset value to the length actually been written.
      byteOffset -= buffer.position();
      handleWrite(buffer.array(), 0, buffer.position());
    }

    // just clean up the current stream. Since the container is already closed,
    // it will be auto committed. No need to call close again here.
    streamEntry.cleanup();
    // This case will arise when while writing the first chunk itself fails.
    // In such case, the current block associated with the stream has no data
    // written. Remove it from the current stream list.
    if (committedLength == 0) {
      streamEntries.remove(streamIndex);
      Preconditions.checkArgument(currentStreamIndex != 0);
      currentStreamIndex -= 1;
    }
    // discard subsequent pre allocated blocks from the streamEntries list
    // from the closed container
    discardPreallocatedBlocks(streamEntry.blockID.getContainerID());
  }

  private boolean checkIfContainerIsClosed(IOException ioe) {
    return Optional.of(ioe.getCause())
        .filter(e -> e instanceof StorageContainerException)
        .map(e -> (StorageContainerException) e)
        .filter(sce -> sce.getResult() == Result.CLOSED_CONTAINER_IO)
        .isPresent();
  }

  private long getKeyLength() {
    return streamEntries.parallelStream().mapToLong(e -> e.currentPosition)
        .sum();
  }

  /**
   * Contact OM to get a new block. Set the new block with the index (e.g.
   * first block has index = 0, second has index = 1 etc.)
   *
   * The returned block is made to new ChunkOutputStreamEntry to write.
   *
   * @param index the index of the block.
   * @throws IOException
   */
  private void allocateNewBlock(int index) throws IOException {
    OmKeyLocationInfo subKeyInfo = omClient.allocateBlock(keyArgs, openID);
    checkKeyLocationInfo(subKeyInfo);
  }

  @Override
  public void flush() throws IOException {
    checkNotClosed();
    handleFlushOrClose(false);
  }

  /**
   * Close or Flush the latest outputStream.
   * @param close Flag which decides whether to call close or flush on the
   *              outputStream.
   * @throws IOException In case, flush or close fails with exception.
   */
  private void handleFlushOrClose(boolean close) throws IOException {
    if (streamEntries.size() == 0) {
      return;
    }
    int size = streamEntries.size();
    int streamIndex =
        currentStreamIndex >= size ? size - 1 : currentStreamIndex;
    ChunkOutputStreamEntry entry = streamEntries.get(streamIndex);
    if (entry != null) {
      try {
        if (close) {
          entry.close();
        } else {
          entry.flush();
        }
      } catch (IOException ioe) {
        if (checkIfContainerIsClosed(ioe)) {
          // This call will allocate a new streamEntry and write the Data.
          // Close needs to be retried on the newly allocated streamEntry as
          // as well.
          handleCloseContainerException(entry, streamIndex);
          handleFlushOrClose(close);
        } else {
          throw ioe;
        }
      }
    }
  }

  /**
   * Commit the key to OM, this will add the blocks as the new key blocks.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    handleFlushOrClose(true);
    if (keyArgs != null) {
      // in test, this could be null
      removeEmptyBlocks();
      Preconditions.checkState(byteOffset == getKeyLength());
      keyArgs.setDataSize(byteOffset);
      keyArgs.setLocationInfoList(getLocationInfoList());
      omClient.commitKey(keyArgs, openID);
    } else {
      LOG.warn("Closing ChunkGroupOutputStream, but key args is null");
    }
  }

  /**
   * Builder class of ChunkGroupOutputStream.
   */
  public static class Builder {
    private OpenKeySession openHandler;
    private XceiverClientManager xceiverManager;
    private StorageContainerLocationProtocolClientSideTranslatorPB scmClient;
    private OzoneManagerProtocolClientSideTranslatorPB omClient;
    private int chunkSize;
    private String requestID;
    private ReplicationType type;
    private ReplicationFactor factor;
    private RetryPolicy retryPolicy;

    public Builder setHandler(OpenKeySession handler) {
      this.openHandler = handler;
      return this;
    }

    public Builder setXceiverClientManager(XceiverClientManager manager) {
      this.xceiverManager = manager;
      return this;
    }

    public Builder setScmClient(
        StorageContainerLocationProtocolClientSideTranslatorPB client) {
      this.scmClient = client;
      return this;
    }

    public Builder setOmClient(
        OzoneManagerProtocolClientSideTranslatorPB client) {
      this.omClient = client;
      return this;
    }

    public Builder setChunkSize(int size) {
      this.chunkSize = size;
      return this;
    }

    public Builder setRequestID(String id) {
      this.requestID = id;
      return this;
    }

    public Builder setType(ReplicationType replicationType) {
      this.type = replicationType;
      return this;
    }

    public Builder setFactor(ReplicationFactor replicationFactor) {
      this.factor = replicationFactor;
      return this;
    }

    public ChunkGroupOutputStream build() throws IOException {
      return new ChunkGroupOutputStream(openHandler, xceiverManager, scmClient,
          omClient, chunkSize, requestID, factor, type, retryPolicy);
    }

    public Builder setRetryPolicy(RetryPolicy rPolicy) {
      this.retryPolicy = rPolicy;
      return this;
    }

  }

  private static class ChunkOutputStreamEntry extends OutputStream {
    private OutputStream outputStream;
    private final BlockID blockID;
    private final String key;
    private final XceiverClientManager xceiverClientManager;
    private final XceiverClientSpi xceiverClient;
    private final String requestId;
    private final int chunkSize;
    // total number of bytes that should be written to this stream
    private final long length;
    // the current position of this stream 0 <= currentPosition < length
    private long currentPosition;

    ChunkOutputStreamEntry(BlockID blockID, String key,
        XceiverClientManager xceiverClientManager,
        XceiverClientSpi xceiverClient, String requestId, int chunkSize,
        long length) {
      this.outputStream = null;
      this.blockID = blockID;
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
      this.blockID = null;
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

    private void checkStream() {
      if (this.outputStream == null) {
        this.outputStream = new ChunkOutputStream(blockID,
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

    ByteBuffer getBuffer() throws IOException {
      if (this.outputStream instanceof ChunkOutputStream) {
        ChunkOutputStream out = (ChunkOutputStream) this.outputStream;
        return out.getBuffer();
      }
      throw new IOException("Invalid Output Stream for Key: " + key);
    }

    public void cleanup() {
      checkStream();
      if (this.outputStream instanceof ChunkOutputStream) {
        ChunkOutputStream out = (ChunkOutputStream) this.outputStream;
        out.cleanup();
      }
    }

  }

  /**
   * Verify that the output stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(
          ": " + FSExceptionMessages.STREAM_IS_CLOSED + " Key: " + keyArgs
              .getKeyName());
    }
  }
}
