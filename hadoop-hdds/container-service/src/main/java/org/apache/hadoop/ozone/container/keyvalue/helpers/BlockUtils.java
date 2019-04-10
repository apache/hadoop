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

package org.apache.hadoop.ozone.container.keyvalue.helpers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    PutBlockResponseProto;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.utils.MetadataStore;

import java.io.IOException;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.NO_SUCH_BLOCK;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.UNABLE_TO_READ_METADATA_DB;

/**
 * Utils functions to help block functions.
 */
public final class BlockUtils {

  /** Never constructed. **/
  private BlockUtils() {

  }
  /**
   * Get a DB handler for a given container.
   * If the handler doesn't exist in cache yet, first create one and
   * add into cache. This function is called with containerManager
   * ReadLock held.
   *
   * @param containerData containerData.
   * @param conf configuration.
   * @return MetadataStore handle.
   * @throws StorageContainerException
   */
  public static MetadataStore getDB(KeyValueContainerData containerData,
                                    Configuration conf) throws
      StorageContainerException {
    Preconditions.checkNotNull(containerData);
    ContainerCache cache = ContainerCache.getInstance(conf);
    Preconditions.checkNotNull(cache);
    Preconditions.checkNotNull(containerData.getDbFile());
    try {
      return cache.getDB(containerData.getContainerID(), containerData
          .getContainerDBType(), containerData.getDbFile().getAbsolutePath(),
          conf);
    } catch (IOException ex) {
      String message = String.format("Error opening DB. Container:%s " +
          "ContainerPath:%s", containerData.getContainerID(), containerData
          .getDbFile().getPath());
      throw new StorageContainerException(message, UNABLE_TO_READ_METADATA_DB);
    }
  }
  /**
   * Remove a DB handler from cache.
   *
   * @param container - Container data.
   * @param conf - Configuration.
   */
  public static void removeDB(KeyValueContainerData container, Configuration
      conf) {
    Preconditions.checkNotNull(container);
    ContainerCache cache = ContainerCache.getInstance(conf);
    Preconditions.checkNotNull(cache);
    cache.removeDB(container.getDbFile().getAbsolutePath());
  }

  /**
   * Shutdown all DB Handles.
   *
   * @param cache - Cache for DB Handles.
   */
  @SuppressWarnings("unchecked")
  public static void shutdownCache(ContainerCache cache)  {
    cache.shutdownCache();
  }

  /**
   * Parses the {@link BlockData} from a bytes array.
   *
   * @param bytes Block data in bytes.
   * @return Block data.
   * @throws IOException if the bytes array is malformed or invalid.
   */
  public static BlockData getBlockData(byte[] bytes) throws IOException {
    try {
      ContainerProtos.BlockData blockData = ContainerProtos.BlockData.parseFrom(
          bytes);
      BlockData data = BlockData.getFromProtoBuf(blockData);
      return data;
    } catch (IOException e) {
      throw new StorageContainerException("Failed to parse block data from " +
          "the bytes array.", NO_SUCH_BLOCK);
    }
  }

  /**
   * Returns putBlock response success.
   * @param msg - Request.
   * @return Response.
   */
  public static ContainerCommandResponseProto putBlockResponseSuccess(
      ContainerCommandRequestProto msg, BlockData blockData) {
    ContainerProtos.BlockData blockDataProto = blockData.getProtoBufMessage();
    GetCommittedBlockLengthResponseProto.Builder
        committedBlockLengthResponseBuilder =
        getCommittedBlockLengthResponseBuilder(blockData.getSize(),
            blockDataProto.getBlockID());
    PutBlockResponseProto.Builder putKeyResponse =
        PutBlockResponseProto.newBuilder();
    putKeyResponse
        .setCommittedBlockLength(committedBlockLengthResponseBuilder);
    ContainerProtos.ContainerCommandResponseProto.Builder builder =
        ContainerUtils.getSuccessResponseBuilder(msg);
    builder.setPutBlock(putKeyResponse);
    return builder.build();
  }
  /**
   * Returns successful blockResponse.
   * @param msg - Request.
   * @return Response.
   */
  public static ContainerCommandResponseProto getBlockResponseSuccess(
      ContainerCommandRequestProto msg) {
    return ContainerUtils.getSuccessResponse(msg);
  }


  public static ContainerCommandResponseProto getBlockDataResponse(
      ContainerCommandRequestProto msg, BlockData data) {
    GetBlockResponseProto.Builder getBlock = ContainerProtos
        .GetBlockResponseProto
        .newBuilder();
    getBlock.setBlockData(data.getProtoBufMessage());
    ContainerProtos.ContainerCommandResponseProto.Builder builder =
        ContainerUtils.getSuccessResponseBuilder(msg);
    builder.setGetBlock(getBlock);
    return  builder.build();
  }

  /**
   * Returns successful getCommittedBlockLength Response.
   * @param msg - Request.
   * @return Response.
   */
  public static ContainerCommandResponseProto getBlockLengthResponse(
          ContainerCommandRequestProto msg, long blockLength) {
    GetCommittedBlockLengthResponseProto.Builder
        committedBlockLengthResponseBuilder =
        getCommittedBlockLengthResponseBuilder(blockLength,
            msg.getGetCommittedBlockLength().getBlockID());
    ContainerProtos.ContainerCommandResponseProto.Builder builder =
        ContainerUtils.getSuccessResponseBuilder(msg);
    builder.setGetCommittedBlockLength(committedBlockLengthResponseBuilder);
    return builder.build();
  }

  public static GetCommittedBlockLengthResponseProto.Builder
          getCommittedBlockLengthResponseBuilder(long blockLength,
      ContainerProtos.DatanodeBlockID blockID) {
    ContainerProtos.GetCommittedBlockLengthResponseProto.Builder
        getCommittedBlockLengthResponseBuilder = ContainerProtos.
        GetCommittedBlockLengthResponseProto.newBuilder();
    getCommittedBlockLengthResponseBuilder.setBlockLength(blockLength);
    getCommittedBlockLengthResponseBuilder.setBlockID(blockID);
    return getCommittedBlockLengthResponseBuilder;
  }
}