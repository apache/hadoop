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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerType;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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
  private String scmID;
  private ContainerMetrics metrics;

  /**
   * Constructs an OzoneContainer that receives calls from
   * XceiverServerHandler.
   */
  public HddsDispatcher(Configuration config, ContainerSet contSet,
      VolumeSet volumes) {
    this.conf = config;
    this.containerSet = contSet;
    this.volumeSet = volumes;
    this.handlers = Maps.newHashMap();
    this.metrics = ContainerMetrics.create(conf);
    for (ContainerType containerType : ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(
              containerType, conf, containerSet, volumeSet, metrics));
    }

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
      return responseProto;
    } else {
      return ContainerUtils.unsupportedRequest(msg);
    }
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
