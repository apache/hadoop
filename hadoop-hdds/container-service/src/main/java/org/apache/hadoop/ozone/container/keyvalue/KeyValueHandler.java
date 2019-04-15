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

package org.apache.hadoop.ozone.container.keyvalue;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .GetSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .PutSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.scm.ByteStringHelper;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis
    .DispatcherContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis
    .DispatcherContext.WriteChunkStage;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume
    .RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.keyvalue.helpers.SmallFileUtils;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.statemachine.background
    .BlockDeletingService;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_DATANODE_VOLUME_CHOOSING_POLICY;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.*;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for KeyValue Container type.
 */
public class KeyValueHandler extends Handler {

  private static final Logger LOG = LoggerFactory.getLogger(
      KeyValueHandler.class);

  private final ContainerType containerType;
  private final BlockManager blockManager;
  private final ChunkManager chunkManager;
  private final BlockDeletingService blockDeletingService;
  private final VolumeChoosingPolicy volumeChoosingPolicy;
  private final long maxContainerSize;

  // A lock that is held during container creation.
  private final AutoCloseableLock containerCreationLock;
  private final boolean doSyncWrite;

  public KeyValueHandler(Configuration config, StateContext context,
      ContainerSet contSet, VolumeSet volSet, ContainerMetrics metrics) {
    super(config, context, contSet, volSet, metrics);
    containerType = ContainerType.KeyValueContainer;
    blockManager = new BlockManagerImpl(config);
    doSyncWrite =
        conf.getBoolean(OzoneConfigKeys.DFS_CONTAINER_CHUNK_WRITE_SYNC_KEY,
            OzoneConfigKeys.DFS_CONTAINER_CHUNK_WRITE_SYNC_DEFAULT);
    chunkManager = new ChunkManagerImpl(doSyncWrite);
    long svcInterval = config
        .getTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
            OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    long serviceTimeout = config
        .getTimeDuration(OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
            OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT,
            TimeUnit.MILLISECONDS);
    this.blockDeletingService =
        new BlockDeletingService(containerSet, svcInterval, serviceTimeout,
            TimeUnit.MILLISECONDS, config);
    blockDeletingService.start();
    volumeChoosingPolicy = ReflectionUtils.newInstance(conf.getClass(
        HDDS_DATANODE_VOLUME_CHOOSING_POLICY, RoundRobinVolumeChoosingPolicy
            .class, VolumeChoosingPolicy.class), conf);
    maxContainerSize = (long)config.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    // this handler lock is used for synchronizing createContainer Requests,
    // so using a fair lock here.
    containerCreationLock = new AutoCloseableLock(new ReentrantLock(true));
    boolean isUnsafeByteOperationsEnabled = conf.getBoolean(
        OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED,
        OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED_DEFAULT);
    ByteStringHelper.init(isUnsafeByteOperationsEnabled);
  }

  @VisibleForTesting
  public VolumeChoosingPolicy getVolumeChoosingPolicyForTesting() {
    return volumeChoosingPolicy;
  }

  @Override
  public ContainerCommandResponseProto handle(
      ContainerCommandRequestProto request, Container container,
      DispatcherContext dispatcherContext) {

    Type cmdType = request.getCmdType();
    KeyValueContainer kvContainer = (KeyValueContainer) container;
    switch(cmdType) {
    case CreateContainer:
      return handleCreateContainer(request, kvContainer);
    case ReadContainer:
      return handleReadContainer(request, kvContainer);
    case UpdateContainer:
      return handleUpdateContainer(request, kvContainer);
    case DeleteContainer:
      return handleDeleteContainer(request, kvContainer);
    case ListContainer:
      return handleUnsupportedOp(request);
    case CloseContainer:
      return handleCloseContainer(request, kvContainer);
    case PutBlock:
      return handlePutBlock(request, kvContainer, dispatcherContext);
    case GetBlock:
      return handleGetBlock(request, kvContainer);
    case DeleteBlock:
      return handleDeleteBlock(request, kvContainer);
    case ListBlock:
      return handleUnsupportedOp(request);
    case ReadChunk:
      return handleReadChunk(request, kvContainer, dispatcherContext);
    case DeleteChunk:
      return handleDeleteChunk(request, kvContainer);
    case WriteChunk:
      return handleWriteChunk(request, kvContainer, dispatcherContext);
    case ListChunk:
      return handleUnsupportedOp(request);
    case CompactChunk:
      return handleUnsupportedOp(request);
    case PutSmallFile:
      return handlePutSmallFile(request, kvContainer, dispatcherContext);
    case GetSmallFile:
      return handleGetSmallFile(request, kvContainer);
    case GetCommittedBlockLength:
      return handleGetCommittedBlockLength(request, kvContainer);
    default:
      return null;
    }
  }

  @VisibleForTesting
  public ChunkManager getChunkManager() {
    return this.chunkManager;
  }

  @VisibleForTesting
  public BlockManager getBlockManager() {
    return this.blockManager;
  }

  /**
   * Handles Create Container Request. If successful, adds the container to
   * ContainerSet and sends an ICR to the SCM.
   */
  ContainerCommandResponseProto handleCreateContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {
    if (!request.hasCreateContainer()) {
      LOG.debug("Malformed Create Container request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }
    // Create Container request should be passed a null container as the
    // container would be created here.
    Preconditions.checkArgument(kvContainer == null);

    long containerID = request.getContainerID();

    KeyValueContainerData newContainerData = new KeyValueContainerData(
        containerID, maxContainerSize, request.getPipelineID(),
        getDatanodeDetails().getUuidString());
    // TODO: Add support to add metadataList to ContainerData. Add metadata
    // to container during creation.
    KeyValueContainer newContainer = new KeyValueContainer(
        newContainerData, conf);

    boolean created = false;
    try (AutoCloseableLock l = containerCreationLock.acquire()) {
      if (containerSet.getContainer(containerID) == null) {
        newContainer.create(volumeSet, volumeChoosingPolicy, scmID);
        created = containerSet.addContainer(newContainer);
      } else {
        // The create container request for an already existing container can
        // arrive in case the ContainerStateMachine reapplies the transaction
        // on datanode restart. Just log a warning msg here.
        LOG.debug("Container already exists." +
            "container Id " + containerID);
      }
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    }

    if (created) {
      try {
        sendICR(newContainer);
      } catch (StorageContainerException ex) {
        return ContainerUtils.logAndReturnError(LOG, ex, request);
      }
    }
    return ContainerUtils.getSuccessResponse(request);
  }

  public void populateContainerPathFields(KeyValueContainer container,
      long maxSize) throws IOException {
    volumeSet.readLock();
    try {
      HddsVolume containerVolume = volumeChoosingPolicy.chooseVolume(volumeSet
          .getVolumesList(), maxSize);
      String hddsVolumeDir = containerVolume.getHddsRootDir().toString();
      container.populatePathFields(scmID, containerVolume, hddsVolumeDir);
    } finally {
      volumeSet.readUnlock();
    }
  }

  /**
   * Handles Read Container Request. Returns the ContainerData as response.
   */
  ContainerCommandResponseProto handleReadContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {
    if (!request.hasReadContainer()) {
      LOG.debug("Malformed Read Container request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    // The container can become unhealthy after the lock is released.
    // The operation will likely fail/timeout in that happens.
    try {
      checkContainerIsHealthy(kvContainer);
    } catch (StorageContainerException sce) {
      return ContainerUtils.logAndReturnError(LOG, sce, request);
    }

    KeyValueContainerData containerData = kvContainer.getContainerData();
    return KeyValueContainerUtil.getReadContainerResponse(
        request, containerData);
  }


  /**
   * Handles Update Container Request. If successful, the container metadata
   * is updated.
   */
  ContainerCommandResponseProto handleUpdateContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasUpdateContainer()) {
      LOG.debug("Malformed Update Container request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    boolean forceUpdate = request.getUpdateContainer().getForceUpdate();
    List<KeyValue> keyValueList =
        request.getUpdateContainer().getMetadataList();
    Map<String, String> metadata = new HashMap<>();
    for (KeyValue keyValue : keyValueList) {
      metadata.put(keyValue.getKey(), keyValue.getValue());
    }

    try {
      if (!metadata.isEmpty()) {
        kvContainer.update(metadata, forceUpdate);
      }
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    }
    return ContainerUtils.getSuccessResponse(request);
  }

  /**
   * Handles Delete Container Request.
   * Open containers cannot be deleted.
   * Holds writeLock on ContainerSet till the container is removed from
   * containerMap. On disk deletion of container files will happen
   * asynchronously without the lock.
   */
  ContainerCommandResponseProto handleDeleteContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasDeleteContainer()) {
      LOG.debug("Malformed Delete container request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    boolean forceDelete = request.getDeleteContainer().getForceDelete();
    try {
      deleteInternal(kvContainer, forceDelete);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    }
    return ContainerUtils.getSuccessResponse(request);
  }

  /**
   * Handles Close Container Request. An open container is closed.
   * Close Container call is idempotent.
   */
  ContainerCommandResponseProto handleCloseContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasCloseContainer()) {
      LOG.debug("Malformed Update Container request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }
    try {
      markContainerForClose(kvContainer);
      closeContainer(kvContainer);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Close Container failed", ex,
              IO_EXCEPTION), request);
    }

    return ContainerUtils.getSuccessResponse(request);
  }

  /**
   * Handle Put Block operation. Calls BlockManager to process the request.
   */
  ContainerCommandResponseProto handlePutBlock(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer,
      DispatcherContext dispatcherContext) {

    long blockLength;
    if (!request.hasPutBlock()) {
      LOG.debug("Malformed Put Key request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    BlockData blockData;
    try {
      checkContainerOpen(kvContainer);

      blockData = BlockData.getFromProtoBuf(
          request.getPutBlock().getBlockData());
      Preconditions.checkNotNull(blockData);
      long bcsId =
          dispatcherContext == null ? 0 : dispatcherContext.getLogIndex();
      blockData.setBlockCommitSequenceId(bcsId);
      long numBytes = blockData.getProtoBufMessage().toByteArray().length;
      blockManager.putBlock(kvContainer, blockData);
      metrics.incContainerBytesStats(Type.PutBlock, numBytes);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Put Key failed", ex, IO_EXCEPTION),
          request);
    }

    return BlockUtils.putBlockResponseSuccess(request, blockData);
  }

  /**
   * Handle Get Block operation. Calls BlockManager to process the request.
   */
  ContainerCommandResponseProto handleGetBlock(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasGetBlock()) {
      LOG.debug("Malformed Get Key request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    // The container can become unhealthy after the lock is released.
    // The operation will likely fail/timeout in that happens.
    try {
      checkContainerIsHealthy(kvContainer);
    } catch (StorageContainerException sce) {
      return ContainerUtils.logAndReturnError(LOG, sce, request);
    }

    BlockData responseData;
    try {
      BlockID blockID = BlockID.getFromProtobuf(
          request.getGetBlock().getBlockID());
      responseData = blockManager.getBlock(kvContainer, blockID);
      long numBytes = responseData.getProtoBufMessage().toByteArray().length;
      metrics.incContainerBytesStats(Type.GetBlock, numBytes);

    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Get Key failed", ex, IO_EXCEPTION),
          request);
    }

    return BlockUtils.getBlockDataResponse(request, responseData);
  }

  /**
   * Handles GetCommittedBlockLength operation.
   * Calls BlockManager to process the request.
   */
  ContainerCommandResponseProto handleGetCommittedBlockLength(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {
    if (!request.hasGetCommittedBlockLength()) {
      LOG.debug("Malformed Get Key request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    // The container can become unhealthy after the lock is released.
    // The operation will likely fail/timeout in that happens.
    try {
      checkContainerIsHealthy(kvContainer);
    } catch (StorageContainerException sce) {
      return ContainerUtils.logAndReturnError(LOG, sce, request);
    }

    long blockLength;
    try {
      BlockID blockID = BlockID
          .getFromProtobuf(request.getGetCommittedBlockLength().getBlockID());
      blockLength = blockManager.getCommittedBlockLength(kvContainer, blockID);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("GetCommittedBlockLength failed", ex,
              IO_EXCEPTION), request);
    }

    return BlockUtils.getBlockLengthResponse(request, blockLength);
  }

  /**
   * Handle Delete Block operation. Calls BlockManager to process the request.
   */
  ContainerCommandResponseProto handleDeleteBlock(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasDeleteBlock()) {
      LOG.debug("Malformed Delete Key request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    try {
      checkContainerOpen(kvContainer);

      BlockID blockID = BlockID.getFromProtobuf(
          request.getDeleteBlock().getBlockID());

      blockManager.deleteBlock(kvContainer, blockID);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Delete Key failed", ex, IO_EXCEPTION),
          request);
    }

    return BlockUtils.getBlockResponseSuccess(request);
  }

  /**
   * Handle Read Chunk operation. Calls ChunkManager to process the request.
   */
  ContainerCommandResponseProto handleReadChunk(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer,
      DispatcherContext dispatcherContext) {

    if (!request.hasReadChunk()) {
      LOG.debug("Malformed Read Chunk request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    // The container can become unhealthy after the lock is released.
    // The operation will likely fail/timeout in that happens.
    try {
      checkContainerIsHealthy(kvContainer);
    } catch (StorageContainerException sce) {
      return ContainerUtils.logAndReturnError(LOG, sce, request);
    }

    ChunkInfo chunkInfo;
    byte[] data;
    try {
      BlockID blockID = BlockID.getFromProtobuf(
          request.getReadChunk().getBlockID());
      chunkInfo = ChunkInfo.getFromProtoBuf(request.getReadChunk()
          .getChunkData());
      Preconditions.checkNotNull(chunkInfo);

      if (dispatcherContext == null) {
        dispatcherContext = new DispatcherContext.Builder().build();
      }

      data = chunkManager
          .readChunk(kvContainer, blockID, chunkInfo, dispatcherContext);
      metrics.incContainerBytesStats(Type.ReadChunk, data.length);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Read Chunk failed", ex, IO_EXCEPTION),
          request);
    }

    return ChunkUtils.getReadChunkResponse(request, data, chunkInfo);
  }

  /**
   * Throw an exception if the container is unhealthy.
   *
   * @throws StorageContainerException if the container is unhealthy.
   * @param kvContainer
   */
  @VisibleForTesting
  void checkContainerIsHealthy(KeyValueContainer kvContainer)
      throws StorageContainerException {
    kvContainer.readLock();
    try {
      if (kvContainer.getContainerData().getState() == State.UNHEALTHY) {
        throw new StorageContainerException(
            "The container replica is unhealthy.",
            CONTAINER_UNHEALTHY);
      }
    } finally {
      kvContainer.readUnlock();
    }
  }

  /**
   * Handle Delete Chunk operation. Calls ChunkManager to process the request.
   */
  ContainerCommandResponseProto handleDeleteChunk(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasDeleteChunk()) {
      LOG.debug("Malformed Delete Chunk request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    // The container can become unhealthy after the lock is released.
    // The operation will likely fail/timeout in that happens.
    try {
      checkContainerIsHealthy(kvContainer);
    } catch (StorageContainerException sce) {
      return ContainerUtils.logAndReturnError(LOG, sce, request);
    }

    try {
      checkContainerOpen(kvContainer);

      BlockID blockID = BlockID.getFromProtobuf(
          request.getDeleteChunk().getBlockID());
      ContainerProtos.ChunkInfo chunkInfoProto = request.getDeleteChunk()
          .getChunkData();
      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(chunkInfoProto);
      Preconditions.checkNotNull(chunkInfo);

      chunkManager.deleteChunk(kvContainer, blockID, chunkInfo);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Delete Chunk failed", ex,
              IO_EXCEPTION), request);
    }

    return ChunkUtils.getChunkResponseSuccess(request);
  }

  /**
   * Handle Write Chunk operation. Calls ChunkManager to process the request.
   */
  ContainerCommandResponseProto handleWriteChunk(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer,
      DispatcherContext dispatcherContext) {

    if (!request.hasWriteChunk()) {
      LOG.debug("Malformed Write Chunk request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    try {
      checkContainerOpen(kvContainer);

      BlockID blockID = BlockID.getFromProtobuf(
          request.getWriteChunk().getBlockID());
      ContainerProtos.ChunkInfo chunkInfoProto =
          request.getWriteChunk().getChunkData();
      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(chunkInfoProto);
      Preconditions.checkNotNull(chunkInfo);

      ByteBuffer data = null;
      if (dispatcherContext == null) {
        dispatcherContext = new DispatcherContext.Builder().build();
      }
      WriteChunkStage stage = dispatcherContext.getStage();
      if (stage == WriteChunkStage.WRITE_DATA ||
          stage == WriteChunkStage.COMBINED) {
        data = request.getWriteChunk().getData().asReadOnlyByteBuffer();
      }

      chunkManager
          .writeChunk(kvContainer, blockID, chunkInfo, data, dispatcherContext);

      // We should increment stats after writeChunk
      if (stage == WriteChunkStage.WRITE_DATA||
          stage == WriteChunkStage.COMBINED) {
        metrics.incContainerBytesStats(Type.WriteChunk, request.getWriteChunk()
            .getChunkData().getLen());
      }
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Write Chunk failed", ex, IO_EXCEPTION),
          request);
    }

    return ChunkUtils.getChunkResponseSuccess(request);
  }

  /**
   * Handle Put Small File operation. Writes the chunk and associated key
   * using a single RPC. Calls BlockManager and ChunkManager to process the
   * request.
   */
  ContainerCommandResponseProto handlePutSmallFile(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer,
      DispatcherContext dispatcherContext) {

    if (!request.hasPutSmallFile()) {
      LOG.debug("Malformed Put Small File request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }
    PutSmallFileRequestProto putSmallFileReq =
        request.getPutSmallFile();
    BlockData blockData;

    try {
      checkContainerOpen(kvContainer);

      BlockID blockID = BlockID.getFromProtobuf(putSmallFileReq.getBlock()
          .getBlockData().getBlockID());
      blockData = BlockData.getFromProtoBuf(
          putSmallFileReq.getBlock().getBlockData());
      Preconditions.checkNotNull(blockData);

      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(
          putSmallFileReq.getChunkInfo());
      Preconditions.checkNotNull(chunkInfo);
      ByteBuffer data = putSmallFileReq.getData().asReadOnlyByteBuffer();
      if (dispatcherContext == null) {
        dispatcherContext = new DispatcherContext.Builder().build();
      }

      // chunks will be committed as a part of handling putSmallFile
      // here. There is no need to maintain this info in openContainerBlockMap.
      chunkManager
          .writeChunk(kvContainer, blockID, chunkInfo, data, dispatcherContext);

      List<ContainerProtos.ChunkInfo> chunks = new LinkedList<>();
      chunks.add(chunkInfo.getProtoBufMessage());
      blockData.setChunks(chunks);
      blockData.setBlockCommitSequenceId(dispatcherContext.getLogIndex());

      blockManager.putBlock(kvContainer, blockData);
      metrics.incContainerBytesStats(Type.PutSmallFile, data.capacity());

    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Read Chunk failed", ex,
              PUT_SMALL_FILE_ERROR), request);
    }

    return SmallFileUtils.getPutFileResponseSuccess(request, blockData);
  }

  /**
   * Handle Get Small File operation. Gets a data stream using a key. This
   * helps in reducing the RPC overhead for small files. Calls BlockManager and
   * ChunkManager to process the request.
   */
  ContainerCommandResponseProto handleGetSmallFile(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasGetSmallFile()) {
      LOG.debug("Malformed Get Small File request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    // The container can become unhealthy after the lock is released.
    // The operation will likely fail/timeout in that happens.
    try {
      checkContainerIsHealthy(kvContainer);
    } catch (StorageContainerException sce) {
      return ContainerUtils.logAndReturnError(LOG, sce, request);
    }

    GetSmallFileRequestProto getSmallFileReq = request.getGetSmallFile();

    try {
      BlockID blockID = BlockID.getFromProtobuf(getSmallFileReq.getBlock()
          .getBlockID());
      BlockData responseData = blockManager.getBlock(kvContainer, blockID);

      ContainerProtos.ChunkInfo chunkInfo = null;
      ByteString dataBuf = ByteString.EMPTY;
      DispatcherContext dispatcherContext =
          new DispatcherContext.Builder().build();
      for (ContainerProtos.ChunkInfo chunk : responseData.getChunks()) {
        // if the block is committed, all chunks must have been committed.
        // Tmp chunk files won't exist here.
        byte[] data = chunkManager.readChunk(kvContainer, blockID,
            ChunkInfo.getFromProtoBuf(chunk), dispatcherContext);
        ByteString current = ByteString.copyFrom(data);
        dataBuf = dataBuf.concat(current);
        chunkInfo = chunk;
      }
      metrics.incContainerBytesStats(Type.GetSmallFile, dataBuf.size());
      return SmallFileUtils.getGetSmallFileResponseSuccess(request, dataBuf
          .toByteArray(), ChunkInfo.getFromProtoBuf(chunkInfo));
    } catch (StorageContainerException e) {
      return ContainerUtils.logAndReturnError(LOG, e, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Write Chunk failed", ex,
              GET_SMALL_FILE_ERROR), request);
    }
  }

  /**
   * Handle unsupported operation.
   */
  ContainerCommandResponseProto handleUnsupportedOp(
      ContainerCommandRequestProto request) {
    // TODO : remove all unsupported operations or handle them.
    return ContainerUtils.unsupportedRequest(request);
  }

  /**
   * Check if container is open. Throw exception otherwise.
   * @param kvContainer
   * @throws StorageContainerException
   */
  private void checkContainerOpen(KeyValueContainer kvContainer)
      throws StorageContainerException {

    final State containerState = kvContainer.getContainerState();

    /*
     * In a closing state, follower will receive transactions from leader.
     * Once the leader is put to closing state, it will reject further requests
     * from clients. Only the transactions which happened before the container
     * in the leader goes to closing state, will arrive here even the container
     * might already be in closing state here.
     */
    if (containerState == State.OPEN || containerState == State.CLOSING) {
      return;
    }

    final ContainerProtos.Result result;
    switch (containerState) {
    case QUASI_CLOSED:
      result = CLOSED_CONTAINER_IO;
      break;
    case CLOSED:
      result = CLOSED_CONTAINER_IO;
      break;
    case UNHEALTHY:
      result = CONTAINER_UNHEALTHY;
      break;
    case INVALID:
      result = INVALID_CONTAINER_STATE;
      break;
    default:
      result = CONTAINER_INTERNAL_ERROR;
    }
    String msg = "Requested operation not allowed as ContainerState is " +
        containerState;
    throw new StorageContainerException(msg, result);
  }

  public Container importContainer(long containerID, long maxSize,
      String originPipelineId,
      String originNodeId,
      FileInputStream rawContainerStream,
      TarContainerPacker packer)
      throws IOException {

    KeyValueContainerData containerData =
        new KeyValueContainerData(containerID,
            maxSize, originPipelineId, originNodeId);

    KeyValueContainer container = new KeyValueContainer(containerData,
        conf);

    populateContainerPathFields(container, maxSize);
    container.importContainerData(rawContainerStream, packer);
    sendICR(container);
    return container;

  }

  @Override
  public void markContainerForClose(Container container)
      throws IOException {
    State currentState = container.getContainerState();
    // Move the container to CLOSING state only if it's OPEN
    if (currentState == State.OPEN) {
      container.markContainerForClose();
      sendICR(container);
    }
  }

  @Override
  public void quasiCloseContainer(Container container)
      throws IOException {
    final State state = container.getContainerState();
    // Quasi close call is idempotent.
    if (state == State.QUASI_CLOSED) {
      return;
    }
    // The container has to be in CLOSING state.
    if (state != State.CLOSING) {
      ContainerProtos.Result error = state == State.INVALID ?
          INVALID_CONTAINER_STATE : CONTAINER_INTERNAL_ERROR;
      throw new StorageContainerException("Cannot quasi close container #" +
          container.getContainerData().getContainerID() + " while in " +
          state + " state.", error);
    }
    container.quasiClose();
    sendICR(container);
  }

  @Override
  public void closeContainer(Container container)
      throws IOException {
    final State state = container.getContainerState();
    // Close call is idempotent.
    if (state == State.CLOSED) {
      return;
    }
    // The container has to be either in CLOSING or in QUASI_CLOSED state.
    if (state != State.CLOSING && state != State.QUASI_CLOSED) {
      ContainerProtos.Result error = state == State.INVALID ?
          INVALID_CONTAINER_STATE : CONTAINER_INTERNAL_ERROR;
      throw new StorageContainerException("Cannot close container #" +
          container.getContainerData().getContainerID() + " while in " +
          state + " state.", error);
    }
    container.close();
    sendICR(container);
  }

  @Override
  public void deleteContainer(Container container, boolean force)
      throws IOException {
    deleteInternal(container, force);
  }

  private void deleteInternal(Container container, boolean force)
      throws StorageContainerException {
    container.writeLock();
    try {
    // If force is false, we check container state.
      if (!force) {
        // Check if container is open
        if (container.getContainerData().isOpen()) {
          throw new StorageContainerException(
              "Deletion of Open Container is not allowed.",
              DELETE_ON_OPEN_CONTAINER);
        }
      }
      long containerId = container.getContainerData().getContainerID();
      containerSet.removeContainer(containerId);
    } finally {
      container.writeUnlock();
    }
    // Avoid holding write locks for disk operations
    container.delete();
  }
}