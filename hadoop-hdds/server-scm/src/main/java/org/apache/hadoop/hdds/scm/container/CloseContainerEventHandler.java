/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND;

/**
 * In case of a node failure, volume failure, volume out of spapce, node
 * out of space etc, CLOSE_CONTAINER will be triggered.
 * CloseContainerEventHandler is the handler for CLOSE_CONTAINER.
 * When a close container event is fired, a close command for the container
 * should be sent to all the datanodes in the pipeline and containerStateManager
 * needs to update the container state to Closing.
 */
public class CloseContainerEventHandler implements EventHandler<ContainerID> {

  public static final Logger LOG =
      LoggerFactory.getLogger(CloseContainerEventHandler.class);

  private final PipelineManager pipelineManager;
  private final ContainerManager containerManager;

  public CloseContainerEventHandler(final PipelineManager pipelineManager,
      final ContainerManager containerManager) {
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;
  }

  @Override
  public void onMessage(ContainerID containerID, EventPublisher publisher) {
    LOG.info("Close container Event triggered for container : {}", containerID);
    try {
      // If the container is in OPEN state, FINALIZE it.
      if (containerManager.getContainer(containerID).getState()
          == LifeCycleState.OPEN) {
        containerManager.updateContainerState(
            containerID, LifeCycleEvent.FINALIZE);
      }

      // ContainerInfo has to read again after the above state change.
      final ContainerInfo container = containerManager
          .getContainer(containerID);
      // Send close command to datanodes, if the container is in CLOSING state
      if (container.getState() == LifeCycleState.CLOSING) {

        final CloseContainerCommand closeContainerCommand =
            new CloseContainerCommand(
                containerID.getId(), container.getPipelineID());

        getNodes(container).forEach(node -> publisher.fireEvent(
            DATANODE_COMMAND,
            new CommandForDatanode<>(node.getUuid(), closeContainerCommand)));
      } else {
        LOG.warn("Cannot close container {}, which is in {} state.",
            containerID, container.getState());
      }

    } catch (IOException ex) {
      LOG.error("Failed to close the container {}.", containerID, ex);
    }
  }

  /**
   * Returns the list of Datanodes where this container lives.
   *
   * @param container ContainerInfo
   * @return list of DatanodeDetails
   * @throws ContainerNotFoundException
   */
  private List<DatanodeDetails> getNodes(final ContainerInfo container)
      throws ContainerNotFoundException {
    try {
      return pipelineManager.getPipeline(container.getPipelineID()).getNodes();
    } catch (PipelineNotFoundException ex) {
      // Use container replica if the pipeline is not available.
      return containerManager.getContainerReplicas(container.containerID())
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());
    }
  }

}
