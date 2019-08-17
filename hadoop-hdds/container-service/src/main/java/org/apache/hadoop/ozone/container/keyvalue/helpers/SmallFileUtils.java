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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;

/**
 * File Utils are helper routines used by putSmallFile and getSmallFile
 * RPCs.
 */
public final class SmallFileUtils {
  /**
   * Never Constructed.
   */
  private SmallFileUtils() {
  }

  /**
   * Gets a response for the putSmallFile RPC.
   * @param msg - ContainerCommandRequestProto
   * @return - ContainerCommandResponseProto
   */
  public static ContainerCommandResponseProto getPutFileResponseSuccess(
      ContainerCommandRequestProto msg, BlockData blockData) {
    ContainerProtos.PutSmallFileResponseProto.Builder getResponse =
        ContainerProtos.PutSmallFileResponseProto.newBuilder();
    ContainerProtos.BlockData blockDataProto = blockData.getProtoBufMessage();
    ContainerProtos.GetCommittedBlockLengthResponseProto.Builder
        committedBlockLengthResponseBuilder = BlockUtils
        .getCommittedBlockLengthResponseBuilder(blockDataProto.getSize(),
            blockDataProto.getBlockID());
    getResponse.setCommittedBlockLength(committedBlockLengthResponseBuilder);
    ContainerCommandResponseProto.Builder builder =
        ContainerUtils.getSuccessResponseBuilder(msg);
    builder.setCmdType(ContainerProtos.Type.PutSmallFile);
    builder.setPutSmallFile(getResponse);
    return  builder.build();
  }

  /**
   * Gets a response to the read small file call.
   * @param msg - Msg
   * @param data  - Data
   * @param info  - Info
   * @return    Response.
   */
  public static ContainerCommandResponseProto getGetSmallFileResponseSuccess(
      ContainerCommandRequestProto msg, byte[] data, ChunkInfo info) {
    Preconditions.checkNotNull(msg);

    ContainerProtos.ReadChunkResponseProto.Builder readChunkresponse =
        ContainerProtos.ReadChunkResponseProto.newBuilder();
    readChunkresponse.setChunkData(info.getProtoBufMessage());
    readChunkresponse.setData(ByteString.copyFrom(data));
    readChunkresponse.setBlockID(msg.getGetSmallFile().getBlock().getBlockID());

    ContainerProtos.GetSmallFileResponseProto.Builder getSmallFile =
        ContainerProtos.GetSmallFileResponseProto.newBuilder();
    getSmallFile.setData(readChunkresponse.build());
    ContainerCommandResponseProto.Builder builder =
        ContainerUtils.getSuccessResponseBuilder(msg);
    builder.setCmdType(ContainerProtos.Type.GetSmallFile);
    builder.setGetSmallFile(getSmallFile);
    return builder.build();
  }

}
