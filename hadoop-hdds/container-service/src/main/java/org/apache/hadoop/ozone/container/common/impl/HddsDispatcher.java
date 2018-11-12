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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerDataProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.container.common.helpers.InvalidContainerStateException;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * Ozone Container dispatcher takes a call from the netty server and routes it
 * to the right handler function.
 */
public class HddsDispatcher implements ContainerDispatcher {

  static final Logger LOG = LoggerFactory.getLogger(HddsDispatcher.class);

  private final Map<ContainerType, Handler> handlers;
  private final Configuration conf;
  private final ContainerSet containerSet;
  private final VolumeSet volumeSet;
  private final StateContext context;
  private final float containerCloseThreshold;
  private String scmID;
  private ContainerMetrics metrics;

  /**
   * Constructs an OzoneContainer that receives calls from
   * XceiverServerHandler.
   */
  public HddsDispatcher(Configuration config, ContainerSet contSet,
      VolumeSet volumes, StateContext context) {
    this.conf = config;
    this.containerSet = contSet;
    this.volumeSet = volumes;
    this.context = context;
    this.handlers = Maps.newHashMap();
    this.metrics = ContainerMetrics.create(conf);
    for (ContainerType containerType : ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(
              containerType, conf, containerSet, volumeSet, metrics));
    }
    this.containerCloseThreshold = conf.getFloat(
        HddsConfigKeys.HDDS_CONTAINER_CLOSE_THRESHOLD,
        HddsConfigKeys.HDDS_CONTAINER_CLOSE_THRESHOLD_DEFAULT);

  }

  @Override
  public void init() {
  }

  @Override
  public void shutdown() {
    // Shutdown the volumes
    volumeSet.shutdown();
  }

  /**
   * Returns true for exceptions which can be ignored for marking the container
   * unhealthy.
   * @param result ContainerCommandResponse error code.
   * @return true if exception can be ignored, false otherwise.
   */
  private boolean canIgnoreException(Result result) {
    switch (result) {
    case SUCCESS:
    case CONTAINER_UNHEALTHY:
    case CLOSED_CONTAINER_IO:
    case DELETE_ON_OPEN_CONTAINER:
    case ERROR_CONTAINER_NOT_EMPTY:
      return true;
    default:
      return false;
    }
  }

  @Override
  public ContainerCommandResponseProto dispatch(
      ContainerCommandRequestProto msg) {
    LOG.trace("Command {}, trace ID: {} ", msg.getCmdType().toString(),
        msg.getTraceID());
    Preconditions.checkNotNull(msg);

    Container container = null;
    ContainerType containerType = null;
    ContainerCommandResponseProto responseProto = null;
    long startTime = System.nanoTime();
    ContainerProtos.Type cmdType = msg.getCmdType();
    long containerID = msg.getContainerID();
    metrics.incContainerOpsMetrics(cmdType);

    if (cmdType != ContainerProtos.Type.CreateContainer) {
      container = getContainer(containerID);

      if (container == null && (cmdType == ContainerProtos.Type.WriteChunk
          || cmdType == ContainerProtos.Type.PutSmallFile)) {
        // If container does not exist, create one for WriteChunk and
        // PutSmallFile request
        createContainer(msg);
        container = getContainer(containerID);
      }

      // if container not found return error
      if (container == null) {
        StorageContainerException sce = new StorageContainerException(
            "ContainerID " + containerID + " does not exist",
            ContainerProtos.Result.CONTAINER_NOT_FOUND);
        return ContainerUtils.logAndReturnError(LOG, sce, msg);
      }
      containerType = getContainerType(container);
    } else {
      if (!msg.hasCreateContainer()) {
        return ContainerUtils.malformedRequest(msg);
      }
      containerType = msg.getCreateContainer().getContainerType();
    }
    // Small performance optimization. We check if the operation is of type
    // write before trying to send CloseContainerAction.
    if (!HddsUtils.isReadOnly(msg)) {
      sendCloseContainerActionIfNeeded(container);
    }
    Handler handler = getHandler(containerType);
    if (handler == null) {
      StorageContainerException ex = new StorageContainerException("Invalid " +
          "ContainerType " + containerType,
          ContainerProtos.Result.CONTAINER_INTERNAL_ERROR);
      return ContainerUtils.logAndReturnError(LOG, ex, msg);
    }
    responseProto = handler.handle(msg, container);
    if (responseProto != null) {
      metrics.incContainerOpsLatencies(cmdType, System.nanoTime() - startTime);

      // If the request is of Write Type and the container operation
      // is unsuccessful, it implies the applyTransaction on the container
      // failed. All subsequent transactions on the container should fail and
      // hence replica will be marked unhealthy here. In this case, a close
      // container action will be sent to SCM to close the container.

      // ApplyTransaction called on closed Container will fail with Closed
      // container exception. In such cases, ignore the exception here
      // If the container is already marked unhealthy, no need to change the
      // state here.

      Result result = responseProto.getResult();
      if (!HddsUtils.isReadOnly(msg) && !canIgnoreException(result)) {
        // If the container is open/closing and the container operation
        // has failed, it should be first marked unhealthy and the initiate the
        // close container action. This also implies this is the first
        // transaction which has failed, so the container is marked unhealthy
        // right here.
        // Once container is marked unhealthy, all the subsequent write
        // transactions will fail with UNHEALTHY_CONTAINER exception.

        // For container to be moved to unhealthy state here, the container can
        // only be in open or closing state.
        State containerState = container.getContainerData().getState();
        Preconditions.checkState(
            containerState == State.OPEN || containerState == State.CLOSING);
        container.getContainerData()
            .setState(ContainerDataProto.State.UNHEALTHY);
        sendCloseContainerActionIfNeeded(container);
      }
      return responseProto;
    } else {
      return ContainerUtils.unsupportedRequest(msg);
    }
  }

  /**
   * Create a container using the input container request.
   * @param containerRequest - the container request which requires container
   *                         to be created.
   */
  private void createContainer(ContainerCommandRequestProto containerRequest) {
    ContainerProtos.CreateContainerRequestProto.Builder createRequest =
        ContainerProtos.CreateContainerRequestProto.newBuilder();
    ContainerType containerType =
        ContainerProtos.ContainerType.KeyValueContainer;
    createRequest.setContainerType(containerType);

    ContainerCommandRequestProto.Builder requestBuilder =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CreateContainer)
            .setContainerID(containerRequest.getContainerID())
            .setCreateContainer(createRequest.build())
            .setDatanodeUuid(containerRequest.getDatanodeUuid())
            .setTraceID(containerRequest.getTraceID());

    // TODO: Assuming the container type to be KeyValueContainer for now.
    // We need to get container type from the containerRequest.
    Handler handler = getHandler(containerType);
    handler.handle(requestBuilder.build(), null);
  }

  /**
   * This will be called as a part of creating the log entry during
   * startTransaction in Ratis on the leader node. In such cases, if the
   * container is not in open state for writing we should just fail.
   * Leader will propagate the exception to client.
   * @param msg  container command proto
   * @throws StorageContainerException In case container state is open for write
   *         requests and in invalid state for read requests.
   */
  @Override
  public void validateContainerCommand(
      ContainerCommandRequestProto msg) throws StorageContainerException {
    ContainerType containerType = msg.getCreateContainer().getContainerType();
    Handler handler = getHandler(containerType);
    if (handler == null) {
      StorageContainerException ex = new StorageContainerException(
          "Invalid " + "ContainerType " + containerType,
          ContainerProtos.Result.CONTAINER_INTERNAL_ERROR);
      throw ex;
    }
    ContainerProtos.Type cmdType = msg.getCmdType();
    long containerID = msg.getContainerID();
    Container container;
    container = getContainer(containerID);
    if (container != null) {
      State containerState = container.getContainerState();
      if (!HddsUtils.isReadOnly(msg) && containerState != State.OPEN) {
        switch (cmdType) {
        case CreateContainer:
          // Create Container is idempotent. There is nothing to validate.
          break;
        case CloseContainer:
          // If the container is unhealthy, closeContainer will be rejected
          // while execution. Nothing to validate here.
          break;
        default:
          // if the container is not open, no updates can happen. Just throw
          // an exception
          throw new ContainerNotOpenException(
              "Container " + containerID + " in " + containerState + " state");
        }
      } else if (HddsUtils.isReadOnly(msg) && containerState == State.INVALID) {
        throw new InvalidContainerStateException(
            "Container " + containerID + " in " + containerState + " state");
      }
    }
  }

  /**
   * If the container usage reaches the close threshold or the container is
   * marked unhealthy we send Close ContainerAction to SCM.
   * @param container current state of container
   */
  private void sendCloseContainerActionIfNeeded(Container container) {
    // We have to find a more efficient way to close a container.
    boolean isSpaceFull = isContainerFull(container);
    boolean shouldClose = isSpaceFull || isContainerUnhealthy(container);
    if (shouldClose) {
      ContainerData containerData = container.getContainerData();
      ContainerAction.Reason reason =
          isSpaceFull ? ContainerAction.Reason.CONTAINER_FULL :
              ContainerAction.Reason.CONTAINER_UNHEALTHY;
      ContainerAction action = ContainerAction.newBuilder()
          .setContainerID(containerData.getContainerID())
          .setAction(ContainerAction.Action.CLOSE).setReason(reason).build();
      context.addContainerActionIfAbsent(action);
    }
  }

  private boolean isContainerFull(Container container) {
    boolean isOpen = Optional.ofNullable(container)
        .map(cont -> cont.getContainerState() == ContainerDataProto.State.OPEN)
        .orElse(Boolean.FALSE);
    if (isOpen) {
      ContainerData containerData = container.getContainerData();
      double containerUsedPercentage =
          1.0f * containerData.getBytesUsed() / containerData.getMaxSize();
      return containerUsedPercentage >= containerCloseThreshold;
    } else {
      return false;
    }
  }

  private boolean isContainerUnhealthy(Container container) {
    return Optional.ofNullable(container).map(
        cont -> (cont.getContainerState() ==
            ContainerDataProto.State.UNHEALTHY))
        .orElse(Boolean.FALSE);
  }

  @Override
  public Handler getHandler(ContainerProtos.ContainerType containerType) {
    return handlers.get(containerType);
  }

  @Override
  public void setScmId(String scmId) {
    Preconditions.checkNotNull(scmId, "scmId Cannot be null");
    if (this.scmID == null) {
      this.scmID = scmId;
      for (Map.Entry<ContainerType, Handler> handlerMap : handlers.entrySet()) {
        handlerMap.getValue().setScmID(scmID);
      }
    }
  }

  public Container getContainer(long containerID) {
    return containerSet.getContainer(containerID);
  }

  private ContainerType getContainerType(Container container) {
    return container.getContainerType();
  }

  @VisibleForTesting
  public void setMetricsForTesting(ContainerMetrics containerMetrics) {
    this.metrics = containerMetrics;
  }
}
