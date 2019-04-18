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
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ChecksumType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.AlreadyClosedException;
import org.apache.ratis.protocol.GroupMismatchException;
import org.apache.ratis.protocol.RaftRetryFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.ListIterator;
import java.util.concurrent.TimeoutException;

/**
 * Maintaining a list of BlockInputStream. Write based on offset.
 *
 * Note that this may write to multiple containers in one write call. In case
 * that first container succeeded but later ones failed, the succeeded writes
 * are not rolled back.
 *
 * TODO : currently not support multi-thread access.
 */
public class KeyOutputStream extends OutputStream {

  /**
   * Defines stream action while calling handleFlushOrClose.
   */
  enum StreamAction {
    FLUSH, CLOSE, FULL
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(KeyOutputStream.class);

  // array list's get(index) is O(1)
  private final ArrayList<BlockOutputStreamEntry> streamEntries;
  private int currentStreamIndex;
  private final OzoneManagerProtocol omClient;
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
  private final int bytesPerChecksum;
  private final ChecksumType checksumType;
  private final BufferPool bufferPool;
  private OmMultipartCommitUploadPartInfo commitUploadPartInfo;
  private FileEncryptionInfo feInfo;
  private ExcludeList excludeList;
  private final RetryPolicy retryPolicy;
  private int retryCount;
  private long offset;
  /**
   * A constructor for testing purpose only.
   */
  @VisibleForTesting
  @SuppressWarnings("parameternumber")
  public KeyOutputStream() {
    streamEntries = new ArrayList<>();
    omClient = null;
    keyArgs = null;
    openID = -1;
    xceiverClientManager = null;
    chunkSize = 0;
    requestID = null;
    closed = false;
    streamBufferFlushSize = 0;
    streamBufferMaxSize = 0;
    bufferPool = new BufferPool(chunkSize, 1);
    watchTimeout = 0;
    blockSize = 0;
    this.checksumType = ChecksumType.valueOf(
        OzoneConfigKeys.OZONE_CLIENT_CHECKSUM_TYPE_DEFAULT);
    this.bytesPerChecksum = OzoneConfigKeys
        .OZONE_CLIENT_BYTES_PER_CHECKSUM_DEFAULT_BYTES; // Default is 1MB
    this.retryPolicy = RetryPolicies.TRY_ONCE_THEN_FAIL;
    retryCount = 0;
    offset = 0;
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
          new OmKeyLocationInfo.Builder().setBlockID(streamEntry.getBlockID())
              .setLength(streamEntry.getCurrentPosition()).setOffset(0)
              .setToken(streamEntry.getToken())
              .setPipeline(streamEntry.getPipeline())
              .build();
      LOG.debug("block written " + streamEntry.getBlockID() + ", length "
          + streamEntry.getCurrentPosition() + " bcsID "
          + streamEntry.getBlockID().getBlockCommitSequenceId());
      locationInfoList.add(info);
    }
    return locationInfoList;
  }

  @VisibleForTesting
  public int getRetryCount() {
    return retryCount;
  }

  @SuppressWarnings("parameternumber")
  public KeyOutputStream(OpenKeySession handler,
      XceiverClientManager xceiverClientManager,
      OzoneManagerProtocol omClient, int chunkSize,
      String requestId, ReplicationFactor factor, ReplicationType type,
      long bufferFlushSize, long bufferMaxSize, long size, long watchTimeout,
      ChecksumType checksumType, int bytesPerChecksum,
      String uploadID, int partNumber, boolean isMultipart, int maxRetryCount) {
    this.streamEntries = new ArrayList<>();
    this.currentStreamIndex = 0;
    this.omClient = omClient;
    OmKeyInfo info = handler.getKeyInfo();
    // Retrieve the file encryption key info, null if file is not in
    // encrypted bucket.
    this.feInfo = info.getFileEncryptionInfo();
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
    this.bytesPerChecksum = bytesPerChecksum;
    this.checksumType = checksumType;

    Preconditions.checkState(chunkSize > 0);
    Preconditions.checkState(streamBufferFlushSize > 0);
    Preconditions.checkState(streamBufferMaxSize > 0);
    Preconditions.checkState(blockSize > 0);
    Preconditions.checkState(streamBufferFlushSize % chunkSize == 0);
    Preconditions.checkState(streamBufferMaxSize % streamBufferFlushSize == 0);
    Preconditions.checkState(blockSize % streamBufferMaxSize == 0);
    this.bufferPool =
        new BufferPool(chunkSize, (int)streamBufferMaxSize / chunkSize);
    this.excludeList = new ExcludeList();
    this.retryPolicy = OzoneClientUtils.createRetryPolicy(maxRetryCount);
    this.retryCount = 0;
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
    Preconditions.checkNotNull(subKeyInfo.getPipeline());
    UserGroupInformation.getCurrentUser().addToken(subKeyInfo.getToken());
    BlockOutputStreamEntry.Builder builder =
        new BlockOutputStreamEntry.Builder()
            .setBlockID(subKeyInfo.getBlockID())
            .setKey(keyArgs.getKeyName())
            .setXceiverClientManager(xceiverClientManager)
            .setPipeline(subKeyInfo.getPipeline())
            .setRequestId(requestID)
            .setChunkSize(chunkSize)
            .setLength(subKeyInfo.getLength())
            .setStreamBufferFlushSize(streamBufferFlushSize)
            .setStreamBufferMaxSize(streamBufferMaxSize)
            .setWatchTimeout(watchTimeout)
            .setbufferPool(bufferPool)
            .setChecksumType(checksumType)
            .setBytesPerChecksum(bytesPerChecksum)
            .setToken(subKeyInfo.getToken());
    streamEntries.add(builder.build());
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
    return bufferPool.computeBufferData();
  }

  private void handleWrite(byte[] b, int off, long len, boolean retry)
      throws IOException {
    int succeededAllocates = 0;
    while (len > 0) {
      try {
        if (streamEntries.size() <= currentStreamIndex) {
          Preconditions.checkNotNull(omClient);
          // allocate a new block, if a exception happens, log an error and
          // throw exception to the caller directly, and the write fails.
          try {
            allocateNewBlock(currentStreamIndex);
            succeededAllocates += 1;
          } catch (IOException ioe) {
            LOG.error("Try to allocate more blocks for write failed, already "
                + "allocated " + succeededAllocates
                + " blocks for this write.");
            throw ioe;
          }
        }
        // in theory, this condition should never violate due the check above
        // still do a sanity check.
        Preconditions.checkArgument(currentStreamIndex < streamEntries.size());
        BlockOutputStreamEntry current = streamEntries.get(currentStreamIndex);

        // length(len) will be in int range if the call is happening through
        // write API of blockOutputStream. Length can be in long range if it
        // comes via Exception path.
        int writeLen = Math.min((int) len, (int) current.getRemaining());
        long currentPos = current.getWrittenDataLength();
        try {
          if (retry) {
            current.writeOnRetry(len);
          } else {
            current.write(b, off, writeLen);
            offset += writeLen;
          }
        } catch (IOException ioe) {
          // for the current iteration, totalDataWritten - currentPos gives the
          // amount of data already written to the buffer

          // In the retryPath, the total data to be written will always be equal
          // to or less than the max length of the buffer allocated.
          // The len specified here is the combined sum of the data length of
          // the buffers
          Preconditions.checkState(!retry || len <= streamBufferMaxSize);
          int dataWritten = (int) (current.getWrittenDataLength() - currentPos);
          writeLen = retry ? (int) len : dataWritten;
          // In retry path, the data written is already accounted in offset.
          if (!retry) {
            offset += writeLen;
          }
          LOG.debug("writeLen {}, total len {}", writeLen, len);
          handleException(current, currentStreamIndex, ioe);
        }
        if (current.getRemaining() <= 0) {
          // since the current block is already written close the stream.
          handleFlushOrClose(StreamAction.FULL);
        }
        len -= writeLen;
        off += writeLen;
      } catch (Exception e) {
        markStreamClosed();
        throw e;
      }
    }
  }

  /**
   * Discards the subsequent pre allocated blocks and removes the streamEntries
   * from the streamEntries list for the container which is closed.
   * @param containerID id of the closed container
   * @param pipelineId id of the associated pipeline
   * @param streamIndex index of the stream
   */
  private void discardPreallocatedBlocks(long containerID,
      PipelineID pipelineId, int streamIndex) {
    // streamIndex < streamEntries.size() signifies that, there are still
    // pre allocated blocks available.

    // This will be called only to discard the next subsequent unused blocks
    // in the streamEntryList.
    if (streamIndex < streamEntries.size()) {
      ListIterator<BlockOutputStreamEntry> streamEntryIterator =
          streamEntries.listIterator(streamIndex);
      while (streamEntryIterator.hasNext()) {
        BlockOutputStreamEntry streamEntry = streamEntryIterator.next();
        Preconditions.checkArgument(streamEntry.getCurrentPosition() == 0);
        if (((pipelineId != null && streamEntry.getPipeline().getId()
            .equals(pipelineId)) || (containerID != -1
            && streamEntry.getBlockID().getContainerID() == containerID))) {
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
        if (streamEntryIterator.next().getCurrentPosition() == 0) {
          streamEntryIterator.remove();
        }
      }
    }
  }

  private void cleanup() {
    if (excludeList != null) {
      excludeList.clear();
      excludeList = null;
    }
    if (bufferPool != null) {
      bufferPool.clearBufferPool();
    }

    if (streamEntries != null) {
      streamEntries.clear();
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
   * @param exception actual exception that occurred
   * @throws IOException Throws IOException if Write fails
   */
  private void handleException(BlockOutputStreamEntry streamEntry,
      int streamIndex, IOException exception) throws IOException {
    Throwable t = checkForException(exception);
    boolean retryFailure = checkForRetryFailure(t);
    boolean closedContainerException = false;
    if (!retryFailure) {
      closedContainerException = checkIfContainerIsClosed(t);
    }
    PipelineID pipelineId = null;
    long totalSuccessfulFlushedData = streamEntry.getTotalAckDataLength();
    //set the correct length for the current stream
    streamEntry.setCurrentPosition(totalSuccessfulFlushedData);
    long bufferedDataLen = computeBufferData();
    LOG.warn("Encountered exception {}. The last committed block length is {}, "
            + "uncommitted data length is {} retry count {}", exception,
        totalSuccessfulFlushedData, bufferedDataLen, retryCount);
    Preconditions.checkArgument(bufferedDataLen <= streamBufferMaxSize);
    Preconditions.checkArgument(offset - getKeyLength() == bufferedDataLen);
    long containerId = streamEntry.getBlockID().getContainerID();
    Collection<DatanodeDetails> failedServers = streamEntry.getFailedServers();
    Preconditions.checkNotNull(failedServers);
    if (!failedServers.isEmpty()) {
      excludeList.addDatanodes(failedServers);
    }
    if (closedContainerException) {
      excludeList.addConatinerId(ContainerID.valueof(containerId));
    } else if (retryFailure || t instanceof TimeoutException
        || t instanceof GroupMismatchException) {
      pipelineId = streamEntry.getPipeline().getId();
      excludeList.addPipeline(pipelineId);
    }
    // just clean up the current stream.
    streamEntry.cleanup(retryFailure);

    // discard all sunsequent blocks the containers and pipelines which
    // are in the exclude list so that, the very next retry should never
    // write data on the  closed container/pipeline
    if (closedContainerException) {
      // discard subsequent pre allocated blocks from the streamEntries list
      // from the closed container
      discardPreallocatedBlocks(streamEntry.getBlockID().getContainerID(), null,
          streamIndex + 1);
    } else {
      // In case there is timeoutException or Watch for commit happening over
      // majority or the client connection failure to the leader in the
      // pipeline, just discard all the preallocated blocks on this pipeline.
      // Next block allocation will happen with excluding this specific pipeline
      // This will ensure if 2 way commit happens , it cannot span over multiple
      // blocks
      discardPreallocatedBlocks(-1, pipelineId, streamIndex + 1);
    }
    if (bufferedDataLen > 0) {
      // If the data is still cached in the underlying stream, we need to
      // allocate new block and write this data in the datanode.
      currentStreamIndex += 1;
      handleRetry(exception, bufferedDataLen);
      // reset the retryCount after handling the exception
      retryCount = 0;
    }
    if (totalSuccessfulFlushedData == 0) {
      streamEntries.remove(streamIndex);
      currentStreamIndex -= 1;
    }
  }

  private void markStreamClosed() {
    cleanup();
    closed = true;
  }

  private void handleRetry(IOException exception, long len) throws IOException {
    RetryPolicy.RetryAction action;
    try {
      action = retryPolicy
          .shouldRetry(exception, retryCount, 0, true);
    } catch (Exception e) {
      throw e instanceof IOException ? (IOException) e : new IOException(e);
    }
    if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
      String msg = "";
      if (action.reason != null) {
        msg = "Retry request failed. " + action.reason;
        LOG.error(msg, exception);
      }
      throw new IOException(msg, exception);
    }

    // Throw the exception if the thread is interrupted
    if (Thread.currentThread().isInterrupted()) {
      LOG.warn("Interrupted while trying for retry");
      throw exception;
    }
    Preconditions.checkArgument(
        action.action == RetryPolicy.RetryAction.RetryDecision.RETRY);
    if (action.delayMillis > 0) {
      try {
        Thread.sleep(action.delayMillis);
      } catch (InterruptedException e) {
        throw (IOException) new InterruptedIOException(
            "Interrupted: action=" + action + ", retry policy=" + retryPolicy)
            .initCause(e);
      }
    }
    retryCount++;
    LOG.trace("Retrying Write request. Already tried "
        + retryCount + " time(s); retry policy is " + retryPolicy);
    handleWrite(null, 0, len, true);
  }
  /**
   * Checks if the provided exception signifies retry failure in ratis client.
   * In case of retry failure, ratis client throws RaftRetryFailureException
   * and all succeeding operations are failed with AlreadyClosedException.
   */
  private boolean checkForRetryFailure(Throwable t) {
    return t instanceof RaftRetryFailureException
        || t instanceof AlreadyClosedException;
  }

  private boolean checkIfContainerIsClosed(Throwable t) {
    return t instanceof ContainerNotOpenException;
  }

  public Throwable checkForException(IOException ioe) throws IOException {
    Throwable t = ioe.getCause();
    while (t != null) {
      for (Class<? extends Exception> cls : OzoneClientUtils
          .getExceptionList()) {
        if (cls.isInstance(t)) {
          return t;
        }
      }
      t = t.getCause();
    }
    throw ioe;
  }

  private long getKeyLength() {
    return streamEntries.stream().mapToLong(e -> e.getCurrentPosition())
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
    OmKeyLocationInfo subKeyInfo =
        omClient.allocateBlock(keyArgs, openID, excludeList);
    addKeyLocationInfo(subKeyInfo);
  }

  @Override
  public void flush() throws IOException {
    checkNotClosed();
    handleFlushOrClose(StreamAction.FLUSH);
  }

  /**
   * Close or Flush the latest outputStream depending upon the action.
   * This function gets called when while write is going on, the current stream
   * gets full or explicit flush or close request is made by client. when the
   * stream gets full and we try to close the stream , we might end up hitting
   * an exception in the exception handling path, we write the data residing in
   * in the buffer pool to a new Block. In cases, as such, when the data gets
   * written to new stream , it will be at max half full. In such cases, we
   * should just write the data and not close the stream as the block won't be
   * completely full.
   * @param op Flag which decides whether to call close or flush on the
   *              outputStream.
   * @throws IOException In case, flush or close fails with exception.
   */
  private void handleFlushOrClose(StreamAction op) throws IOException {
    if (streamEntries.size() == 0) {
      return;
    }
    while (true) {
      try {
        int size = streamEntries.size();
        int streamIndex =
            currentStreamIndex >= size ? size - 1 : currentStreamIndex;
        BlockOutputStreamEntry entry = streamEntries.get(streamIndex);
        if (entry != null) {
          try {
            Collection<DatanodeDetails> failedServers =
                entry.getFailedServers();
            // failed servers can be null in case there is no data written in
            // the stream
            if (failedServers != null && !failedServers.isEmpty()) {
              excludeList.addDatanodes(failedServers);
            }
            switch (op) {
            case CLOSE:
              entry.close();
              break;
            case FULL:
              if (entry.getRemaining() == 0) {
                entry.close();
                currentStreamIndex++;
              }
              break;
            case FLUSH:
              entry.flush();
              break;
            default:
              throw new IOException("Invalid Operation");
            }
          } catch (IOException ioe) {
            handleException(entry, streamIndex, ioe);
            continue;
          }
        }
        break;
      } catch (Exception e) {
        markStreamClosed();
        throw e;
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
      handleFlushOrClose(StreamAction.CLOSE);
      if (keyArgs != null) {
        // in test, this could be null
        removeEmptyBlocks();
        long length = getKeyLength();
        Preconditions.checkArgument(offset == length);
        keyArgs.setDataSize(length);
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
      cleanup();
    }
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    return commitUploadPartInfo;
  }

  public FileEncryptionInfo getFileEncryptionInfo() {
    return feInfo;
  }

  @VisibleForTesting
  public ExcludeList getExcludeList() {
    return excludeList;
  }

  /**
   * Builder class of KeyOutputStream.
   */
  public static class Builder {
    private OpenKeySession openHandler;
    private XceiverClientManager xceiverManager;
    private OzoneManagerProtocol omClient;
    private int chunkSize;
    private String requestID;
    private ReplicationType type;
    private ReplicationFactor factor;
    private long streamBufferFlushSize;
    private long streamBufferMaxSize;
    private long blockSize;
    private long watchTimeout;
    private ChecksumType checksumType;
    private int bytesPerChecksum;
    private String multipartUploadID;
    private int multipartNumber;
    private boolean isMultipartKey;
    private int maxRetryCount;


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

    public Builder setOmClient(
        OzoneManagerProtocol client) {
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

    public Builder setChecksumType(ChecksumType cType){
      this.checksumType = cType;
      return this;
    }

    public Builder setBytesPerChecksum(int bytes){
      this.bytesPerChecksum = bytes;
      return this;
    }

    public Builder setIsMultipartKey(boolean isMultipart) {
      this.isMultipartKey = isMultipart;
      return this;
    }

    public Builder setMaxRetryCount(int maxCount) {
      this.maxRetryCount = maxCount;
      return this;
    }

    public KeyOutputStream build() throws IOException {
      return new KeyOutputStream(openHandler, xceiverManager,
          omClient, chunkSize, requestID, factor, type, streamBufferFlushSize,
          streamBufferMaxSize, blockSize, watchTimeout, checksumType,
          bytesPerChecksum, multipartUploadID, multipartNumber, isMultipartKey,
          maxRetryCount);
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
