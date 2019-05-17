
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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * This class manages the stream entries list and handles block allocation
 * from OzoneManager.
 */
public class BlockOutputStreamEntryPool {

  public static final Logger LOG =
      LoggerFactory.getLogger(BlockOutputStreamEntryPool.class);

  private final List<BlockOutputStreamEntry> streamEntries;
  private int currentStreamIndex;
  private final OzoneManagerProtocol omClient;
  private final OmKeyArgs keyArgs;
  private final XceiverClientManager xceiverClientManager;
  private final int chunkSize;
  private final String requestID;
  private final long streamBufferFlushSize;
  private final long streamBufferMaxSize;
  private final long watchTimeout;
  private final long blockSize;
  private final int bytesPerChecksum;
  private final ContainerProtos.ChecksumType checksumType;
  private final BufferPool bufferPool;
  private OmMultipartCommitUploadPartInfo commitUploadPartInfo;
  private final long openID;
  private ExcludeList excludeList;

  @SuppressWarnings("parameternumber")
  public BlockOutputStreamEntryPool(OzoneManagerProtocol omClient,
      int chunkSize, String requestId, HddsProtos.ReplicationFactor factor,
      HddsProtos.ReplicationType type, long bufferFlushSize, long bufferMaxSize,
      long size, long watchTimeout, ContainerProtos.ChecksumType checksumType,
      int bytesPerChecksum, String uploadID, int partNumber,
      boolean isMultipart, OmKeyInfo info,
      XceiverClientManager xceiverClientManager, long openID) {
    streamEntries = new ArrayList<>();
    currentStreamIndex = 0;
    this.omClient = omClient;
    this.keyArgs = new OmKeyArgs.Builder().setVolumeName(info.getVolumeName())
        .setBucketName(info.getBucketName()).setKeyName(info.getKeyName())
        .setType(type).setFactor(factor).setDataSize(info.getDataSize())
        .setIsMultipartKey(isMultipart).setMultipartUploadID(uploadID)
        .setMultipartUploadPartNumber(partNumber).build();
    this.xceiverClientManager = xceiverClientManager;
    this.chunkSize = chunkSize;
    this.requestID = requestId;
    this.streamBufferFlushSize = bufferFlushSize;
    this.streamBufferMaxSize = bufferMaxSize;
    this.blockSize = size;
    this.watchTimeout = watchTimeout;
    this.bytesPerChecksum = bytesPerChecksum;
    this.checksumType = checksumType;
    this.openID = openID;
    this.excludeList = new ExcludeList();

    Preconditions.checkState(chunkSize > 0);
    Preconditions.checkState(streamBufferFlushSize > 0);
    Preconditions.checkState(streamBufferMaxSize > 0);
    Preconditions.checkState(blockSize > 0);
    Preconditions.checkState(streamBufferFlushSize % chunkSize == 0);
    Preconditions.checkState(streamBufferMaxSize % streamBufferFlushSize == 0);
    Preconditions.checkState(blockSize % streamBufferMaxSize == 0);
    this.bufferPool =
        new BufferPool(chunkSize, (int) streamBufferMaxSize / chunkSize);
  }

  public BlockOutputStreamEntryPool() {
    streamEntries = new ArrayList<>();
    omClient = null;
    keyArgs = null;
    xceiverClientManager = null;
    chunkSize = 0;
    requestID = null;
    streamBufferFlushSize = 0;
    streamBufferMaxSize = 0;
    bufferPool = new BufferPool(chunkSize, 1);
    watchTimeout = 0;
    blockSize = 0;
    this.checksumType = ContainerProtos.ChecksumType.valueOf(
        OzoneConfigKeys.OZONE_CLIENT_CHECKSUM_TYPE_DEFAULT);
    this.bytesPerChecksum = OzoneConfigKeys
        .OZONE_CLIENT_BYTES_PER_CHECKSUM_DEFAULT_BYTES; // Default is 1MB
    currentStreamIndex = 0;
    openID = -1;
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

  public List<OmKeyLocationInfo> getLocationInfoList()  {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    for (BlockOutputStreamEntry streamEntry : streamEntries) {
      long length = streamEntry.getCurrentPosition();

      // Commit only those blocks to OzoneManager which are not empty
      if (length != 0) {
        OmKeyLocationInfo info =
            new OmKeyLocationInfo.Builder().setBlockID(streamEntry.getBlockID())
                .setLength(streamEntry.getCurrentPosition()).setOffset(0)
                .setToken(streamEntry.getToken())
                .setPipeline(streamEntry.getPipeline()).build();
        locationInfoList.add(info);
      }
      LOG.debug(
          "block written " + streamEntry.getBlockID() + ", length " + length
              + " bcsID " + streamEntry.getBlockID()
              .getBlockCommitSequenceId());
    }
    return locationInfoList;
  }

  /**
   * Discards the subsequent pre allocated blocks and removes the streamEntries
   * from the streamEntries list for the container which is closed.
   * @param containerID id of the closed container
   * @param pipelineId id of the associated pipeline
   */
  void discardPreallocatedBlocks(long containerID, PipelineID pipelineId) {
    // currentStreamIndex < streamEntries.size() signifies that, there are still
    // pre allocated blocks available.

    // This will be called only to discard the next subsequent unused blocks
    // in the streamEntryList.
    if (currentStreamIndex + 1 < streamEntries.size()) {
      ListIterator<BlockOutputStreamEntry> streamEntryIterator =
          streamEntries.listIterator(currentStreamIndex + 1);
      while (streamEntryIterator.hasNext()) {
        BlockOutputStreamEntry streamEntry = streamEntryIterator.next();
        Preconditions.checkArgument(streamEntry.getCurrentPosition() == 0);
        if ((pipelineId != null && streamEntry.getPipeline().getId()
            .equals(pipelineId)) || (containerID != -1
            && streamEntry.getBlockID().getContainerID() == containerID)) {
          streamEntryIterator.remove();
        }
      }
    }
  }

  List<BlockOutputStreamEntry> getStreamEntries() {
    return streamEntries;
  }

  XceiverClientManager getXceiverClientManager() {
    return xceiverClientManager;
  }

  String getKeyName() {
    return keyArgs.getKeyName();
  }

  long getKeyLength() {
    return streamEntries.stream().mapToLong(e -> e.getCurrentPosition()).sum();
  }
  /**
   * Contact OM to get a new block. Set the new block with the index (e.g.
   * first block has index = 0, second has index = 1 etc.)
   *
   * The returned block is made to new BlockOutputStreamEntry to write.
   *
   * @throws IOException
   */
  private void allocateNewBlock() throws IOException {
    OmKeyLocationInfo subKeyInfo =
        omClient.allocateBlock(keyArgs, openID, excludeList);
    addKeyLocationInfo(subKeyInfo);
  }


  void commitKey(long offset) throws IOException {
    if (keyArgs != null) {
      // in test, this could be null
      long length = getKeyLength();
      Preconditions.checkArgument(offset == length);
      keyArgs.setDataSize(length);
      keyArgs.setLocationInfoList(getLocationInfoList());
      // When the key is multipart upload part file upload, we should not
      // commit the key, as this is not an actual key, this is a just a
      // partial key of a large file.
      if (keyArgs.getIsMultipartKey()) {
        commitUploadPartInfo =
            omClient.commitMultipartUploadPart(keyArgs, openID);
      } else {
        omClient.commitKey(keyArgs, openID);
      }
    } else {
      LOG.warn("Closing KeyOutputStream, but key args is null");
    }
  }

  public BlockOutputStreamEntry getCurrentStreamEntry() {
    if (streamEntries.isEmpty() || streamEntries.size() <= currentStreamIndex) {
      return null;
    } else {
      return streamEntries.get(currentStreamIndex);
    }
  }

  BlockOutputStreamEntry allocateBlockIfNeeded() throws IOException {
    BlockOutputStreamEntry streamEntry = getCurrentStreamEntry();
    if (streamEntry != null && streamEntry.isClosed()) {
      // a stream entry gets closed either by :
      // a. If the stream gets full
      // b. it has encountered an exception
      currentStreamIndex++;
    }
    if (streamEntries.size() <= currentStreamIndex) {
      Preconditions.checkNotNull(omClient);
      // allocate a new block, if a exception happens, log an error and
      // throw exception to the caller directly, and the write fails.
      int succeededAllocates = 0;
      try {
        allocateNewBlock();
        succeededAllocates += 1;
      } catch (IOException ioe) {
        LOG.error("Try to allocate more blocks for write failed, already "
            + "allocated {} blocks for this write.", succeededAllocates, ioe);
        throw ioe;
      }
    }
    // in theory, this condition should never violate due the check above
    // still do a sanity check.
    Preconditions.checkArgument(currentStreamIndex < streamEntries.size());
    BlockOutputStreamEntry current = streamEntries.get(currentStreamIndex);
    return current;
  }

  long computeBufferData() {
    return bufferPool.computeBufferData();
  }

  void cleanup() {
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

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    return commitUploadPartInfo;
  }

  public ExcludeList getExcludeList() {
    return excludeList;
  }

  public long getStreamBufferMaxSize() {
    return streamBufferMaxSize;
  }

  boolean isEmpty() {
    return streamEntries.isEmpty();
  }
}
