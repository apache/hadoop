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
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.hdds.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.ratis.protocol.RaftRetryFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ListIterator;
import java.util.concurrent.TimeoutException;

/**
 * Maintaining a list of ChunkInputStream. Write based on offset.
 *
 * Note that this may write to multiple containers in one write call. In case
 * that first container succeeded but later ones failed, the succeeded writes
 * are not rolled back.
 *
 * TODO : currently not support multi-thread access.
 */
public class KeyOutputStream extends OutputStream {

  public static final Logger LOG =
      LoggerFactory.getLogger(KeyOutputStream.class);

  // array list's get(index) is O(1)
  private final ArrayList<BlockOutputStreamEntry> streamEntries;
  private int currentStreamIndex;
  private final OzoneManagerProtocolClientSideTranslatorPB omClient;
  private final
      StorageContainerLocationProtocolClientSideTranslatorPB scmClient;
  private final OmKeyArgs keyArgs;
  private final long openID;
  private final XceiverClientManager xceiverClientManager;
  private final int chunkSize;
  private final String requestID;
  private boolean closed;
  private final long streamBufferFlushSize;
  private final long streamBufferMaxSize;
  private final long watchTimeout;
  private final long blockSize;
  private final Checksum checksum;
  private List<ByteBuffer> bufferList;
  private OmMultipartCommitUploadPartInfo commitUploadPartInfo;
  /**
   * A constructor for testing purpose only.
   */
  @VisibleForTesting
  public KeyOutputStream() {
    streamEntries = new ArrayList<>();
    omClient = null;
    scmClient = null;
    keyArgs = null;
    openID = -1;
    xceiverClientManager = null;
    chunkSize = 0;
    requestID = null;
    closed = false;
    streamBufferFlushSize = 0;
    streamBufferMaxSize = 0;
    bufferList = new ArrayList<>(1);
    ByteBuffer buffer = ByteBuffer.allocate(1);
    bufferList.add(buffer);
    watchTimeout = 0;
    blockSize = 0;
    this.checksum = new Checksum();
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
    streamEntries.add(
        new BlockOutputStreamEntry(outputStream, length, checksum));
  }

  @VisibleForTesting
  public List<BlockOutputStreamEntry> getStreamEntries() {
    return streamEntries;
  }
  @VisibleForTesting
  public XceiverClientManager getXceiverClientManager() {
    return xceiverClientManager;
  }

  public List<OmKeyLocationInfo> getLocationInfoList() throws IOException {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    for (BlockOutputStreamEntry streamEntry : streamEntries) {
      OmKeyLocationInfo info =
          new OmKeyLocationInfo.Builder().setBlockID(streamEntry.blockID)
              .setLength(streamEntry.currentPosition).setOffset(0)
              .build();
      LOG.debug("block written " + streamEntry.blockID + ", length "
          + streamEntry.currentPosition + " bcsID " + streamEntry.blockID
          .getBlockCommitSequenceId());
      locationInfoList.add(info);
    }
    return locationInfoList;
  }

  public KeyOutputStream(OpenKeySession handler,
      XceiverClientManager xceiverClientManager,
      StorageContainerLocationProtocolClientSideTranslatorPB scmClient,
      OzoneManagerProtocolClientSideTranslatorPB omClient, int chunkSize,
      String requestId, ReplicationFactor factor, ReplicationType type,
      long bufferFlushSize, long bufferMaxSize, long size, long watchTimeout,
      Checksum checksum, String uploadID, int partNumber, boolean isMultipart) {
    this.streamEntries = new ArrayList<>();
    this.currentStreamIndex = 0;
    this.omClient = omClient;
    this.scmClient = scmClient;
    OmKeyInfo info = handler.getKeyInfo();
    this.keyArgs = new OmKeyArgs.Builder().setVolumeName(info.getVolumeName())
        .setBucketName(info.getBucketName()).setKeyName(info.getKeyName())
        .setType(type).setFactor(factor).setDataSize(info.getDataSize())
        .setIsMultipartKey(isMultipart).setMultipartUploadID(
            uploadID).setMultipartUploadPartNumber(partNumber)
        .build();
    this.openID = handler.getId();
    this.xceiverClientManager = xceiverClientManager;
    this.chunkSize = chunkSize;
    this.requestID = requestId;
    this.streamBufferFlushSize = bufferFlushSize;
    this.streamBufferMaxSize = bufferMaxSize;
    this.blockSize = size;
    this.watchTimeout = watchTimeout;
    this.checksum = checksum;

    Preconditions.checkState(chunkSize > 0);
    Preconditions.checkState(streamBufferFlushSize > 0);
    Preconditions.checkState(streamBufferMaxSize > 0);
    Preconditions.checkState(blockSize > 0);
    Preconditions.checkState(streamBufferFlushSize % chunkSize == 0);
    Preconditions.checkState(streamBufferMaxSize % streamBufferFlushSize == 0);
    Preconditions.checkState(blockSize % streamBufferMaxSize == 0);
    this.bufferList = new ArrayList<>();
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
        addKeyLocationInfo(subKeyInfo);
      }
    }
  }

  private void addKeyLocationInfo(OmKeyLocationInfo subKeyInfo)
      throws IOException {
    ContainerWithPipeline containerWithPipeline = scmClient
        .getContainerWithPipeline(subKeyInfo.getContainerID());
    XceiverClientSpi xceiverClient =
        xceiverClientManager.acquireClient(containerWithPipeline.getPipeline());
    streamEntries.add(new BlockOutputStreamEntry(subKeyInfo.getBlockID(),
        keyArgs.getKeyName(), xceiverClientManager, xceiverClient, requestID,
        chunkSize, subKeyInfo.getLength(), streamBufferFlushSize,
        streamBufferMaxSize, watchTimeout, bufferList, checksum));
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
    handleWrite(b, off, len, false);
  }

  private long computeBufferData() {
    return bufferList.stream().mapToInt(value -> value.position())
        .sum();
  }

  private void handleWrite(byte[] b, int off, long len, boolean retry)
      throws IOException {
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
          LOG.error("Try to allocate more blocks for write failed, already "
              + "allocated " + succeededAllocates + " blocks for this write.");
          throw ioe;
        }
      }
      // in theory, this condition should never violate due the check above
      // still do a sanity check.
      Preconditions.checkArgument(currentStreamIndex < streamEntries.size());
      BlockOutputStreamEntry current = streamEntries.get(currentStreamIndex);

      // length(len) will be in int range if the call is happening through
      // write API of chunkOutputStream. Length can be in long range if it comes
      // via Exception path.
      int writeLen = Math.min((int)len, (int) current.getRemaining());
      long currentPos = current.getWrittenDataLength();
      try {
        if (retry) {
          current.writeOnRetry(len);
        } else {
          current.write(b, off, writeLen);
        }
      } catch (IOException ioe) {
        if (checkIfContainerIsClosed(ioe) || checkIfTimeoutException(ioe)) {
          // for the current iteration, totalDataWritten - currentPos gives the
          // amount of data already written to the buffer
          writeLen = (int) (current.getWrittenDataLength() - currentPos);
          LOG.debug("writeLen {}, total len {}", writeLen, len);
          handleException(current, currentStreamIndex);
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
      ListIterator<BlockOutputStreamEntry> streamEntryIterator =
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
      ListIterator<BlockOutputStreamEntry> streamEntryIterator =
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
   * @throws IOException Throws IOException if Write fails
   */
  private void handleException(BlockOutputStreamEntry streamEntry,
      int streamIndex) throws IOException {
    long totalSuccessfulFlushedData =
        streamEntry.getTotalSuccessfulFlushedData();
    //set the correct length for the current stream
    streamEntry.currentPosition = totalSuccessfulFlushedData;
    long bufferedDataLen = computeBufferData();
    // just clean up the current stream.
    streamEntry.cleanup();
    if (bufferedDataLen > 0) {
      // If the data is still cached in the underlying stream, we need to
      // allocate new block and write this data in the datanode.
      currentStreamIndex += 1;
      handleWrite(null, 0, bufferedDataLen, true);
    }
    if (totalSuccessfulFlushedData == 0) {
      streamEntries.remove(streamIndex);
      currentStreamIndex -= 1;
    }
    // discard subsequent pre allocated blocks from the streamEntries list
    // from the closed container
    discardPreallocatedBlocks(streamEntry.blockID.getContainerID());
  }

  private boolean checkIfContainerIsClosed(IOException ioe) {
    if (ioe.getCause() != null) {
      return checkIfContainerNotOpenOrRaftRetryFailureException(ioe) || Optional
          .of(ioe.getCause())
          .filter(e -> e instanceof StorageContainerException)
          .map(e -> (StorageContainerException) e)
          .filter(sce -> sce.getResult() == Result.CLOSED_CONTAINER_IO)
          .isPresent();
    }
    return false;
  }

  private boolean checkIfContainerNotOpenOrRaftRetryFailureException(
      IOException ioe) {
    Throwable t = ioe.getCause();
    while (t != null) {
      if (t instanceof ContainerNotOpenException
          || t instanceof RaftRetryFailureException) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  private boolean checkIfTimeoutException(IOException ioe) {
    if (ioe.getCause() != null) {
      return Optional.of(ioe.getCause())
          .filter(e -> e instanceof TimeoutException).isPresent();
    } else {
      return false;
    }
  }

  private long getKeyLength() {
    return streamEntries.stream().mapToLong(e -> e.currentPosition)
        .sum();
  }

  /**
   * Contact OM to get a new block. Set the new block with the index (e.g.
   * first block has index = 0, second has index = 1 etc.)
   *
   * The returned block is made to new BlockOutputStreamEntry to write.
   *
   * @param index the index of the block.
   * @throws IOException
   */
  private void allocateNewBlock(int index) throws IOException {
    OmKeyLocationInfo subKeyInfo = omClient.allocateBlock(keyArgs, openID);
    addKeyLocationInfo(subKeyInfo);
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
    BlockOutputStreamEntry entry = streamEntries.get(streamIndex);
    if (entry != null) {
      try {
        if (close) {
          entry.close();
        } else {
          entry.flush();
        }
      } catch (IOException ioe) {
        if (checkIfContainerIsClosed(ioe) || checkIfTimeoutException(ioe)) {
          // This call will allocate a new streamEntry and write the Data.
          // Close needs to be retried on the newly allocated streamEntry as
          // as well.
          handleException(entry, streamIndex);
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
    try {
      handleFlushOrClose(true);
      if (keyArgs != null) {
        // in test, this could be null
        removeEmptyBlocks();
        keyArgs.setDataSize(getKeyLength());
        keyArgs.setLocationInfoList(getLocationInfoList());
        // When the key is multipart upload part file upload, we should not
        // commit the key, as this is not an actual key, this is a just a
        // partial key of a large file.
        if (keyArgs.getIsMultipartKey()) {
          commitUploadPartInfo = omClient.commitMultipartUploadPart(keyArgs,
              openID);
        } else {
          omClient.commitKey(keyArgs, openID);
        }
      } else {
        LOG.warn("Closing KeyOutputStream, but key args is null");
      }
    } catch (IOException ioe) {
      throw ioe;
    } finally {
      if (bufferList != null) {
        bufferList.stream().forEach(e -> e.clear());
      }
      bufferList = null;
    }
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    return commitUploadPartInfo;
  }

  /**
   * Builder class of KeyOutputStream.
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
    private long streamBufferFlushSize;
    private long streamBufferMaxSize;
    private long blockSize;
    private long watchTimeout;
    private Checksum checksum;
    private String multipartUploadID;
    private int multipartNumber;
    private boolean isMultipartKey;


    public Builder setMultipartUploadID(String uploadID) {
      this.multipartUploadID = uploadID;
      return this;
    }

    public Builder setMultipartNumber(int partNumber) {
      this.multipartNumber = partNumber;
      return this;
    }

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

    public Builder setStreamBufferFlushSize(long size) {
      this.streamBufferFlushSize = size;
      return this;
    }

    public Builder setStreamBufferMaxSize(long size) {
      this.streamBufferMaxSize = size;
      return this;
    }

    public Builder setBlockSize(long size) {
      this.blockSize = size;
      return this;
    }

    public Builder setWatchTimeout(long timeout) {
      this.watchTimeout = timeout;
      return this;
    }

    public Builder setChecksum(Checksum checksumObj){
      this.checksum = checksumObj;
      return this;
    }

    public Builder setIsMultipartKey(boolean isMultipart) {
      this.isMultipartKey = isMultipart;
      return this;
    }

    public KeyOutputStream build() throws IOException {
      return new KeyOutputStream(openHandler, xceiverManager, scmClient,
          omClient, chunkSize, requestID, factor, type, streamBufferFlushSize,
          streamBufferMaxSize, blockSize, watchTimeout, checksum,
          multipartUploadID, multipartNumber, isMultipartKey);
    }
  }

  private static class BlockOutputStreamEntry extends OutputStream {
    private OutputStream outputStream;
    private BlockID blockID;
    private final String key;
    private final XceiverClientManager xceiverClientManager;
    private final XceiverClientSpi xceiverClient;
    private final Checksum checksum;
    private final String requestId;
    private final int chunkSize;
    // total number of bytes that should be written to this stream
    private final long length;
    // the current position of this stream 0 <= currentPosition < length
    private long currentPosition;

    private final long streamBufferFlushSize;
    private final long streamBufferMaxSize;
    private final long watchTimeout;
    private List<ByteBuffer> bufferList;

    BlockOutputStreamEntry(BlockID blockID, String key,
        XceiverClientManager xceiverClientManager,
        XceiverClientSpi xceiverClient, String requestId, int chunkSize,
        long length, long streamBufferFlushSize, long streamBufferMaxSize,
        long watchTimeout, List<ByteBuffer> bufferList, Checksum checksum) {
      this.outputStream = null;
      this.blockID = blockID;
      this.key = key;
      this.xceiverClientManager = xceiverClientManager;
      this.xceiverClient = xceiverClient;
      this.requestId = requestId;
      this.chunkSize = chunkSize;

      this.length = length;
      this.currentPosition = 0;
      this.streamBufferFlushSize = streamBufferFlushSize;
      this.streamBufferMaxSize = streamBufferMaxSize;
      this.watchTimeout = watchTimeout;
      this.checksum = checksum;
      this.bufferList = bufferList;
    }

    /**
     * For testing purpose, taking a some random created stream instance.
     * @param  outputStream a existing writable output stream
     * @param  length the length of data to write to the stream
     */
    BlockOutputStreamEntry(OutputStream outputStream, long length,
        Checksum checksum) {
      this.outputStream = outputStream;
      this.blockID = null;
      this.key = null;
      this.xceiverClientManager = null;
      this.xceiverClient = null;
      this.requestId = null;
      this.chunkSize = -1;

      this.length = length;
      this.currentPosition = 0;
      streamBufferFlushSize = 0;
      streamBufferMaxSize = 0;
      bufferList = null;
      watchTimeout = 0;
      this.checksum = checksum;
    }

    long getLength() {
      return length;
    }

    long getRemaining() {
      return length - currentPosition;
    }

    private void checkStream() {
      if (this.outputStream == null) {
        this.outputStream =
            new BlockOutputStream(blockID, key, xceiverClientManager,
                xceiverClient, requestId, chunkSize, streamBufferFlushSize,
                streamBufferMaxSize, watchTimeout, bufferList, checksum);
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
        // after closing the chunkOutPutStream, blockId would have been
        // reconstructed with updated bcsId
        if (this.outputStream instanceof BlockOutputStream) {
          this.blockID = ((BlockOutputStream) outputStream).getBlockID();
        }
      }
    }

    long getTotalSuccessfulFlushedData() throws IOException {
      if (this.outputStream instanceof BlockOutputStream) {
        BlockOutputStream out = (BlockOutputStream) this.outputStream;
        blockID = out.getBlockID();
        return out.getTotalSuccessfulFlushedData();
      } else if (outputStream == null) {
        // For a pre allocated block for which no write has been initiated,
        // the OutputStream will be null here.
        // In such cases, the default blockCommitSequenceId will be 0
        return 0;
      }
      throw new IOException("Invalid Output Stream for Key: " + key);
    }

    long getWrittenDataLength() throws IOException {
      if (this.outputStream instanceof BlockOutputStream) {
        BlockOutputStream out = (BlockOutputStream) this.outputStream;
        return out.getWrittenDataLength();
      } else if (outputStream == null) {
        // For a pre allocated block for which no write has been initiated,
        // the OutputStream will be null here.
        // In such cases, the default blockCommitSequenceId will be 0
        return 0;
      }
      throw new IOException("Invalid Output Stream for Key: " + key);
    }

    void cleanup() {
      checkStream();
      if (this.outputStream instanceof BlockOutputStream) {
        BlockOutputStream out = (BlockOutputStream) this.outputStream;
        out.cleanup();
      }
    }

    void writeOnRetry(long len) throws IOException {
      checkStream();
      if (this.outputStream instanceof BlockOutputStream) {
        BlockOutputStream out = (BlockOutputStream) this.outputStream;
        out.writeOnRetry(len);
        this.currentPosition += len;
      } else {
        throw new IOException("Invalid Output Stream for Key: " + key);
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
