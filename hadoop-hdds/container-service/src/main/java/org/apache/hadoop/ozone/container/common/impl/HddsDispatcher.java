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
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerAction;
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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerLifeCycleState;
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
    try {
      long containerID = msg.getContainerID();

      metrics.incContainerOpsMetrics(cmdType);
      if (cmdType != ContainerProtos.Type.CreateContainer) {
        container = getContainer(containerID);
        containerType = getContainerType(container);
      } else {
        if (!msg.hasCreateContainer()) {
          return ContainerUtils.malformedRequest(msg);
        }
        containerType = msg.getCreateContainer().getContainerType();
      }
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, msg);
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
      if (!HddsUtils.isReadOnly(msg)
          && responseProto.getResult() != ContainerProtos.Result.SUCCESS) {
        // If the container is open and the container operation has failed,
        // it should be first marked unhealthy and the initiate the close
        // container action. This also implies this is the first transaction
        // which has failed, so the container is marked unhealthy right here.
        // Once container is marked unhealthy, all the subsequent write
        // transactions will fail with UNHEALTHY_CONTAINER exception.
        if (container.getContainerState() == ContainerLifeCycleState.OPEN) {
          container.getContainerData()
              .setState(ContainerLifeCycleState.UNHEALTHY);
          sendCloseContainerActionIfNeeded(container);
        }
      }
      return responseProto;
    } else {
      return ContainerUtils.unsupportedRequest(msg);
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
        .map(cont -> cont.getContainerState() == ContainerLifeCycleState.OPEN)
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
        cont -> (cont.getContainerState() == ContainerLifeCycleState.UNHEALTHY))
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

  @VisibleForTesting
  public Container getContainer(long containerID)
      throws StorageContainerException {
    Container container = containerSet.getContainer(containerID);
    if (container == null) {
      throw new StorageContainerException(
          "ContainerID " + containerID + " does not exist",
          ContainerProtos.Result.CONTAINER_NOT_FOUND);
    }
    return container;
  }

  private ContainerType getContainerType(Container container) {
    return container.getContainerType();
  }

  @VisibleForTesting
  public void setMetricsForTesting(ContainerMetrics containerMetrics) {
    this.metrics = containerMetrics;
  }
}
