/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto
    .ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.CloseContainerCommandProto;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine
    .SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handler for close container command received from SCM.
 */
public class CloseContainerCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CloseContainerCommandHandler.class);

  private int invocationCount;
  private long totalTime;

  /**
   * Constructs a ContainerReport handler.
   */
  public CloseContainerCommandHandler() {
  }

  /**
   * Handles a given SCM command.
   *
   * @param command           - SCM Command
   * @param ozoneContainer         - Ozone Container.
   * @param context           - Current Context.
   * @param connectionManager - The SCMs that we are talking to.
   */
  @Override
  public void handle(SCMCommand command, OzoneContainer ozoneContainer,
      StateContext context, SCMConnectionManager connectionManager) {
    LOG.debug("Processing Close Container command.");
    invocationCount++;
    final long startTime = Time.monotonicNow();
    final DatanodeDetails datanodeDetails = context.getParent()
        .getDatanodeDetails();
    final CloseContainerCommandProto closeCommand =
        ((CloseContainerCommand)command).getProto();
    final ContainerController controller = ozoneContainer.getController();
    final long containerId = closeCommand.getContainerID();
    try {
      final Container container = controller.getContainer(containerId);

      if (container == null) {
        LOG.error("Container #{} does not exist in datanode. "
            + "Container close failed.", containerId);
        return;
      }

      if (container.getContainerState() ==
          ContainerProtos.ContainerDataProto.State.CLOSED) {
        // Closing a container is an idempotent operation.
        return;
      }

      // Move the container to CLOSING state
      controller.markContainerForClose(containerId);

      // If the container is part of open pipeline, close it via write channel
      if (ozoneContainer.getWriteChannel()
          .isExist(closeCommand.getPipelineID())) {
        if (closeCommand.getForce()) {
          LOG.warn("Cannot force close a container when the container is" +
              " part of an active pipeline.");
          return;
        }
        ContainerCommandRequestProto request =
            getContainerCommandRequestProto(datanodeDetails,
                closeCommand.getContainerID());
        ozoneContainer.getWriteChannel().submitRequest(
            request, closeCommand.getPipelineID());
        return;
      }
      // If we reach here, there is no active pipeline for this container.
      if (!closeCommand.getForce()) {
        // QUASI_CLOSE the container.
        controller.quasiCloseContainer(containerId);
      } else {
        // SCM told us to force close the container.
        controller.closeContainer(containerId);
      }
    } catch (NotLeaderException e) {
      LOG.debug("Follower cannot close container #{}.", containerId);
    } catch (IOException e) {
      LOG.error("Can't close container #{}", containerId, e);
    } finally {
      long endTime = Time.monotonicNow();
      totalTime += endTime - startTime;
    }
  }

  private ContainerCommandRequestProto getContainerCommandRequestProto(
      final DatanodeDetails datanodeDetails, final long containerId) {
    final ContainerCommandRequestProto.Builder command =
        ContainerCommandRequestProto.newBuilder();
    command.setCmdType(ContainerProtos.Type.CloseContainer);
    command.setTraceID(TracingUtil.exportCurrentSpan());
    command.setContainerID(containerId);
    command.setCloseContainer(
        ContainerProtos.CloseContainerRequestProto.getDefaultInstance());
    command.setDatanodeUuid(datanodeDetails.getUuidString());
    return command.build();
  }

  /**
   * Returns the command type that this command handler handles.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.closeContainerCommand;
  }

  /**
   * Returns number of times this handler has been invoked.
   *
   * @return int
   */
  @Override
  public int getInvocationCount() {
    return invocationCount;
  }

  /**
   * Returns the average time this function takes to run.
   *
   * @return long
   */
  @Override
  public long getAverageRunTime() {
    if (invocationCount > 0) {
      return totalTime / invocationCount;
    }
    return 0;
  }
}
