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

package org.apache.hadoop.ozone.container;

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.container.helpers.Pipeline;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.UUID;

/**
 * Helpers for container tests.
 */
public class ContainerTestHelper {

  /**
   * Create a pipeline with single node replica.
   *
   * @return Pipeline with single node in it.
   * @throws IOException
   */
  public static Pipeline createSingleNodePipeline() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    DatanodeID datanodeID = new DatanodeID(socket.getInetAddress()
        .getHostAddress(), socket.getInetAddress().getHostName(),
        UUID.randomUUID().toString(), port, port, port, port);
    datanodeID.setContainerPort(port);
    Pipeline pipeline = new Pipeline(datanodeID.getDatanodeUuid());
    pipeline.addMember(datanodeID);
    socket.close();
    return pipeline;
  }

  /**
   * Returns a create container command for test purposes. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCreateContainerRequest() throws
      IOException {
    ContainerProtos.CreateContainerRequestProto.Builder createRequest =
        ContainerProtos.CreateContainerRequestProto
            .newBuilder();
    ContainerProtos.ContainerData.Builder containerData = ContainerProtos
        .ContainerData.newBuilder();
    containerData.setName("testContainer");
    createRequest.setPipeline(
        ContainerTestHelper.createSingleNodePipeline().getProtobufMessage());
    createRequest.setContainerData(containerData.build());

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setCreateContainer(createRequest);
    return request.build();
  }

  /**
   * Returns a create container response for test purposes. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandResponseProto
  getCreateContainerResponse(ContainerCommandRequestProto request) throws
      IOException {
    ContainerProtos.CreateContainerResponseProto.Builder createResponse =
        ContainerProtos.CreateContainerResponseProto.newBuilder();

    ContainerCommandResponseProto.Builder response =
        ContainerCommandResponseProto.newBuilder();
    response.setCmdType(ContainerProtos.Type.CreateContainer);
    response.setTraceID(request.getTraceID());
    response.setCreateContainer(createResponse.build());
    return response.build();
  }


}
