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

package org.apache.hadoop.ozone.container.common.interfaces;


import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerType;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;

/**
 * Dispatcher sends ContainerCommandRequests to Handler. Each Container Type
 * should have an implementation for Handler.
 */
@SuppressWarnings("visibilitymodifier")
public abstract class Handler {

  protected final Configuration conf;
  protected final ContainerSet containerSet;
  protected final VolumeSet volumeSet;
  protected String scmID;
  protected final ContainerMetrics metrics;

  private final StateContext context;
  private final DatanodeDetails datanodeDetails;

  protected Handler(Configuration config, StateContext context,
      ContainerSet contSet, VolumeSet volumeSet,
      ContainerMetrics containerMetrics) {
    this.conf = config;
    this.context = context;
    this.containerSet = contSet;
    this.volumeSet = volumeSet;
    this.metrics = containerMetrics;
    this.datanodeDetails = context.getParent().getDatanodeDetails();
  }

  public static Handler getHandlerForContainerType(
      final ContainerType containerType, final Configuration config,
      final StateContext context, final ContainerSet contSet,
      final VolumeSet volumeSet, final ContainerMetrics metrics) {
    switch (containerType) {
    case KeyValueContainer:
      return new KeyValueHandler(config, context, contSet, volumeSet, metrics);
    default:
      throw new IllegalArgumentException("Handler for ContainerType: " +
        containerType + "doesn't exist.");
    }
  }

  /**
   * Returns the Id of this datanode.
   * @return datanode Id
   */
  protected DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }
  /**
   * This should be called whenever there is state change. It will trigger
   * an ICR to SCM.
   *
   * @param container Container for which ICR has to be sent
   */
  protected void sendICR(final Container container)
      throws StorageContainerException {
    IncrementalContainerReportProto icr = IncrementalContainerReportProto
        .newBuilder()
        .addReport(container.getContainerReport())
        .build();
    context.addReport(icr);
    context.getParent().triggerHeartbeat();
  }

  public abstract ContainerCommandResponseProto handle(
      ContainerCommandRequestProto msg, Container container,
      DispatcherContext dispatcherContext);

  /**
   * Import container data from a raw input stream.
   */
  public abstract Container importContainer(
      long containerID,
      long maxSize,
      String originPipelineId,
      String originNodeId,
      FileInputStream rawContainerStream,
      TarContainerPacker packer)
      throws IOException;

  /**
   * Marks the container for closing. Moves the container to CLOSING state.
   *
   * @param container container to update
   * @throws IOException in case of exception
   */
  public abstract void markContainerForClose(Container container)
      throws IOException;

  /**
   * Moves the Container to QUASI_CLOSED state.
   *
   * @param container container to be quasi closed
   * @throws IOException
   */
  public abstract void quasiCloseContainer(Container container)
      throws IOException;

  /**
   * Moves the Container to CLOSED state.
   *
   * @param container container to be closed
   * @throws IOException
   */
  public abstract void closeContainer(Container container)
      throws IOException;

  /**
   * Deletes the given container.
   *
   * @param container container to be deleted
   * @param force if this is set to true, we delete container without checking
   * state of the container.
   * @throws IOException
   */
  public abstract void deleteContainer(Container container, boolean force)
      throws IOException;

  public void setScmID(String scmId) {
    this.scmID = scmId;
  }

}
