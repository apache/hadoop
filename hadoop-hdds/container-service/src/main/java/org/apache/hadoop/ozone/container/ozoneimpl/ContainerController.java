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
package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdds.protocol.datanode.proto
    .ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Control plane for container management in datanode.
 */
public class ContainerController {

  private final ContainerSet containerSet;
  private final Map<ContainerType, Handler> handlers;

  public ContainerController(final ContainerSet containerSet,
      final Map<ContainerType, Handler> handlers) {
    this.containerSet = containerSet;
    this.handlers = handlers;
  }

  /**
   * Returns the Container given a container id.
   *
   * @param containerId ID of the container
   * @return Container
   */
  public Container getContainer(final long containerId) {
    return containerSet.getContainer(containerId);
  }

  /**
   * Marks the container for closing. Moves the container to CLOSING state.
   *
   * @param containerId Id of the container to update
   * @throws IOException in case of exception
   */
  public void markContainerForClose(final long containerId)
      throws IOException {
    Container container = containerSet.getContainer(containerId);

    if (container.getContainerState() == State.OPEN) {
      getHandler(container).markContainerForClose(container);
    }
  }

  /**
   * Returns the container report.
   *
   * @return ContainerReportsProto
   * @throws IOException in case of exception
   */
  public ContainerReportsProto getContainerReport()
      throws IOException {
    return containerSet.getContainerReport();
  }

  /**
   * Quasi closes a container given its id.
   *
   * @param containerId Id of the container to quasi close
   * @throws IOException in case of exception
   */
  public void quasiCloseContainer(final long containerId) throws IOException {
    final Container container = containerSet.getContainer(containerId);
    getHandler(container).quasiCloseContainer(container);
  }

  /**
   * Closes a container given its Id.
   *
   * @param containerId Id of the container to close
   * @throws IOException in case of exception
   */
  public void closeContainer(final long containerId) throws IOException {
    final Container container = containerSet.getContainer(containerId);
    getHandler(container).closeContainer(container);
  }

  public Container importContainer(final ContainerType type,
      final long containerId, final long maxSize, final String originPipelineId,
      final String originNodeId, final FileInputStream rawContainerStream,
      final TarContainerPacker packer)
      throws IOException {
    return handlers.get(type).importContainer(containerId, maxSize,
        originPipelineId, originNodeId, rawContainerStream, packer);
  }

  /**
   * Deletes a container given its Id.
   * @param containerId Id of the container to be deleted
   * @param force if this is set to true, we delete container without checking
   * state of the container.
   * @throws IOException
   */
  public void deleteContainer(final long containerId, boolean force)
      throws IOException {
    final Container container = containerSet.getContainer(containerId);
    getHandler(container).deleteContainer(container, force);
  }

  /**
   * Given a container, returns its handler instance.
   *
   * @param container Container
   * @return handler of the container
   */
  private Handler getHandler(final Container container) {
    return handlers.get(container.getContainerType());
  }
}
