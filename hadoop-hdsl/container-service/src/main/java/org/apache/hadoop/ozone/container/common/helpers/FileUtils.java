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
package org.apache.hadoop.ozone.container.common.helpers;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos;

/**
 * File Utils are helper routines used by putSmallFile and getSmallFile
 * RPCs.
 */
public final class FileUtils {
  /**
   * Never Constructed.
   */
  private FileUtils() {
  }

  /**
   * Gets a response for the putSmallFile RPC.
   * @param msg - ContainerCommandRequestProto
   * @return - ContainerCommandResponseProto
   */
  public static ContainerProtos.ContainerCommandResponseProto
      getPutFileResponse(ContainerProtos.ContainerCommandRequestProto msg) {
    ContainerProtos.PutSmallFileResponseProto.Builder getResponse =
        ContainerProtos.PutSmallFileResponseProto.newBuilder();
    ContainerProtos.ContainerCommandResponseProto.Builder builder =
        ContainerUtils.getContainerResponse(msg, ContainerProtos.Result
            .SUCCESS, "");
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
  public static ContainerProtos.ContainerCommandResponseProto
      getGetSmallFileResponse(ContainerProtos.ContainerCommandRequestProto msg,
      byte[] data, ChunkInfo info) {
    Preconditions.checkNotNull(msg);

    ContainerProtos.ReadChunkResponseProto.Builder readChunkresponse =
        ContainerProtos.ReadChunkResponseProto.newBuilder();
    readChunkresponse.setChunkData(info.getProtoBufMessage());
    readChunkresponse.setData(ByteString.copyFrom(data));
    readChunkresponse.setPipeline(msg.getGetSmallFile().getKey().getPipeline());

    ContainerProtos.GetSmallFileResponseProto.Builder getSmallFile =
        ContainerProtos.GetSmallFileResponseProto.newBuilder();
    getSmallFile.setData(readChunkresponse.build());
    ContainerProtos.ContainerCommandResponseProto.Builder builder =
        ContainerUtils.getContainerResponse(msg, ContainerProtos.Result
            .SUCCESS, "");
    builder.setCmdType(ContainerProtos.Type.GetSmallFile);
    builder.setGetSmallFile(getSmallFile);
    return builder.build();
  }

}
