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

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.Type;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Ozone Container dispatcher takes a call from the netty server and routes it
 * to the right handler function.
 */
public class Dispatcher implements ContainerDispatcher {
  static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);

  private final ContainerManager containerManager;

  /**
   * Constructs an OzoneContainer that receives calls from
   * XceiverServerHandler.
   *
   * @param containerManager - A class that manages containers.
   */
  public Dispatcher(ContainerManager containerManager) {
    Preconditions.checkNotNull(containerManager);
    this.containerManager = containerManager;
  }

  @Override
  public ContainerCommandResponseProto dispatch(
      ContainerCommandRequestProto msg) throws IOException {
    Preconditions.checkNotNull(msg);
    Type cmdType = msg.getCmdType();
    if ((cmdType == Type.CreateContainer) ||
        (cmdType == Type.DeleteContainer) ||
        (cmdType == Type.ReadContainer) ||
        (cmdType == Type.ListContainer) ||
        (cmdType == Type.UpdateContainer)) {

      return containerProcessHandler(msg);
    }


    return ContainerUtils.unsupportedRequest(msg);
  }

  /**
   * Handles the all Container related functionality.
   *
   * @param msg - command
   * @return - response
   * @throws IOException
   */
  private ContainerCommandResponseProto containerProcessHandler(
      ContainerCommandRequestProto msg) throws IOException {
    try {
      ContainerData cData = ContainerData.getFromProtBuf(
          msg.getCreateContainer().getContainerData());

      Pipeline pipeline = Pipeline.getFromProtoBuf(
          msg.getCreateContainer().getPipeline());
      Preconditions.checkNotNull(pipeline);

      switch (msg.getCmdType()) {
      case CreateContainer:
        return handleCreateContainer(msg, cData, pipeline);

      case DeleteContainer:
        return handleDeleteContainer(msg, cData, pipeline);

      case ListContainer:
        return ContainerUtils.unsupportedRequest(msg);

      case UpdateContainer:
        return ContainerUtils.unsupportedRequest(msg);

      case ReadContainer:
        return handleReadContainer(msg, cData);

      default:
        return ContainerUtils.unsupportedRequest(msg);
      }
    } catch (IOException ex) {
      LOG.warn("Container operation failed. " +
              "Container: {} Operation: {}  trace ID: {} Error: {}",
          msg.getCreateContainer().getContainerData().getName(),
          msg.getCmdType().name(),
          msg.getTraceID(),
          ex.toString());

      // TODO : Replace with finer error codes.
      return ContainerUtils.getContainerResponse(msg,
          ContainerProtos.Result.CONTAINER_INTERNAL_ERROR,
          ex.toString()).build();
    }
  }

  /**
   * Calls into container logic and returns appropriate response.
   *
   * @param msg   - Request
   * @param cData - Container Data object
   * @return ContainerCommandResponseProto
   * @throws IOException
   */
  private ContainerCommandResponseProto handleReadContainer(
      ContainerCommandRequestProto msg, ContainerData cData)
      throws IOException {

    if (!msg.hasReadContainer()) {
      LOG.debug("Malformed read container request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }
    ContainerData container = this.containerManager.readContainer(
        cData.getContainerName());
    return ContainerUtils.getReadContainerResponse(msg, container);
  }

  /**
   * Calls into container logic and returns appropriate response.
   *
   * @param msg      - Request
   * @param cData    - ContainerData
   * @param pipeline - Pipeline is the machines where this container lives.
   * @return Response.
   * @throws IOException
   */
  private ContainerCommandResponseProto handleDeleteContainer(
      ContainerCommandRequestProto msg, ContainerData cData,
      Pipeline pipeline) throws IOException {
    if (!msg.hasDeleteContainer()) {
      LOG.debug("Malformed delete container request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }
    this.containerManager.deleteContainer(pipeline,
        cData.getContainerName());
    return ContainerUtils.getContainerResponse(msg);
  }

  /**
   * Calls into container logic and returns appropriate response.
   *
   * @param msg      - Request
   * @param cData    - ContainerData
   * @param pipeline - Pipeline is the machines where this container lives.
   * @return Response.
   * @throws IOException
   */
  private ContainerCommandResponseProto handleCreateContainer(
      ContainerCommandRequestProto msg, ContainerData cData,
      Pipeline pipeline) throws IOException {
    if (!msg.hasCreateContainer()) {
      LOG.debug("Malformed create container request. trace ID: {}",
          msg.getTraceID());
      return ContainerUtils.malformedRequest(msg);
    }
    this.containerManager.createContainer(pipeline, cData);
    return ContainerUtils.getContainerResponse(msg);
  }
}
