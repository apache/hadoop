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

package org.apache.hadoop.ozone.container.helpers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;

/**
 * A set of helper functions to create proper responses.
 */
public final class ContainerUtils {

  /**
   * Returns a CreateContainer Response. This call is used by create and delete
   * containers which have null success responses.
   *
   * @param msg Request
   * @return Response.
   */
  public static ContainerProtos.ContainerCommandResponseProto
      getContainerResponse(ContainerProtos.ContainerCommandRequestProto msg) {
    ContainerProtos.ContainerCommandResponseProto.Builder builder =
        getContainerResponse(msg, ContainerProtos.Result.SUCCESS, "");
    return builder.build();
  }

  /**
   * Returns a ReadContainer Response.
   *
   * @param msg Request
   * @return Response.
   */
  public static ContainerProtos.ContainerCommandResponseProto
      getReadContainerResponse(ContainerProtos.ContainerCommandRequestProto msg,
                           ContainerData containerData) {
    Preconditions.checkNotNull(containerData);

    ContainerProtos.ReadContainerResponseProto.Builder response =
        ContainerProtos.ReadContainerResponseProto.newBuilder();
    response.setContainerData(containerData.getProtoBufMessage());

    ContainerProtos.ContainerCommandResponseProto.Builder builder =
        getContainerResponse(msg, ContainerProtos.Result.SUCCESS, "");
    builder.setReadContainer(response);
    return builder.build();
  }

  /**
   * We found a command type but no associated payload for the command. Hence
   * return malformed Command as response.
   *
   * @param msg - Protobuf message.
   * @return ContainerCommandResponseProto - MALFORMED_REQUEST.
   */
  public static ContainerProtos.ContainerCommandResponseProto.Builder
      getContainerResponse(ContainerProtos.ContainerCommandRequestProto msg,
                       ContainerProtos.Result result, String message) {
    return
        ContainerProtos.ContainerCommandResponseProto.newBuilder()
            .setCmdType(msg.getCmdType())
            .setTraceID(msg.getTraceID())
            .setResult(result)
            .setMessage(message);
  }

  /**
   * We found a command type but no associated payload for the command. Hence
   * return malformed Command as response.
   *
   * @param msg - Protobuf message.
   * @return ContainerCommandResponseProto - MALFORMED_REQUEST.
   */
  public static ContainerProtos.ContainerCommandResponseProto
      malformedRequest(ContainerProtos.ContainerCommandRequestProto msg) {
    return getContainerResponse(msg, ContainerProtos.Result.MALFORMED_REQUEST,
        "Cmd type does not match the payload.").build();
  }

  /**
   * We found a command type that is not supported yet.
   *
   * @param msg - Protobuf message.
   * @return ContainerCommandResponseProto - MALFORMED_REQUEST.
   */
  public static ContainerProtos.ContainerCommandResponseProto
      unsupportedRequest(ContainerProtos.ContainerCommandRequestProto msg) {
    return getContainerResponse(msg, ContainerProtos.Result.UNSUPPORTED_REQUEST,
        "Server does not support this command yet.").build();
  }

  private ContainerUtils() {
    //never constructed.
  }
}
