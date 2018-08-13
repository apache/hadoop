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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
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


  private final Mapping containerManager;

  public CloseContainerEventHandler(Mapping containerManager) {
    this.containerManager = containerManager;
  }

  @Override
  public void onMessage(ContainerID containerID, EventPublisher publisher) {

    LOG.info("Close container Event triggered for container : {}",
        containerID.getId());
    ContainerWithPipeline containerWithPipeline = null;
    ContainerInfo info;
    try {
      containerWithPipeline =
          containerManager.getContainerWithPipeline(containerID.getId());
      info = containerWithPipeline.getContainerInfo();
      if (info == null) {
        LOG.error("Failed to update the container state. Container with id : {} "
            + "does not exist", containerID.getId());
        return;
      }
    } catch (IOException e) {
      LOG.error("Failed to update the container state. Container with id : {} "
          + "does not exist", containerID.getId(), e);
      return;
    }

    if (info.getState() == HddsProtos.LifeCycleState.OPEN) {
      for (DatanodeDetails datanode :
          containerWithPipeline.getPipeline().getMachines()) {
        CommandForDatanode closeContainerCommand = new CommandForDatanode<>(
            datanode.getUuid(),
            new CloseContainerCommand(containerID.getId(),
                info.getReplicationType(), info.getPipelineID()));
        publisher.fireEvent(DATANODE_COMMAND, closeContainerCommand);
      }
      try {
        // Finalize event will make sure the state of the container transitions
        // from OPEN to CLOSING in containerStateManager.
        containerManager.updateContainerState(containerID.getId(),
            HddsProtos.LifeCycleEvent.FINALIZE);
      } catch (IOException ex) {
        LOG.error("Failed to update the container state to FINALIZE for"
            + "container : {}" + containerID, ex);
      }
    } else if (info.getState() == HddsProtos.LifeCycleState.ALLOCATED) {
      try {
        // Create event will make sure the state of the container transitions
        // from OPEN to CREATING in containerStateManager, this will move
        // the container out of active allocation path.
        containerManager.updateContainerState(containerID.getId(),
            HddsProtos.LifeCycleEvent.CREATE);
      } catch (IOException ex) {
        LOG.error("Failed to update the container state to CREATE for"
            + "container:{}" + containerID, ex);
      }
    } else {
      LOG.info("container with id : {} is in {} state and need not be closed.",
          containerID.getId(), info.getState());
    }

  }
}
