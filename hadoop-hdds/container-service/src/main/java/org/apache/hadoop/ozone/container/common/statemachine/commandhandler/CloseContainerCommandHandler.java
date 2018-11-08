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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.CloseContainerCommandProto;
import org.apache.hadoop.ozone.container.common.statemachine
    .SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Handler for close container command received from SCM.
 */
public class CloseContainerCommandHandler implements CommandHandler {
  static final Logger LOG =
      LoggerFactory.getLogger(CloseContainerCommandHandler.class);
  private int invocationCount;
  private long totalTime;
  private boolean cmdExecuted;

  /**
   * Constructs a ContainerReport handler.
   */
  public CloseContainerCommandHandler() {
  }

  /**
   * Handles a given SCM command.
   *
   * @param command           - SCM Command
   * @param container         - Ozone Container.
   * @param context           - Current Context.
   * @param connectionManager - The SCMs that we are talking to.
   */
  @Override
  public void handle(SCMCommand command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {
    LOG.debug("Processing Close Container command.");
    invocationCount++;
    long startTime = Time.monotonicNow();
    // TODO: define this as INVALID_CONTAINER_ID in HddsConsts.java (TBA)
    long containerID = -1;
    try {
      CloseContainerCommandProto closeContainerProto =
          CloseContainerCommandProto.parseFrom(command.getProtoBufMessage());
      containerID = closeContainerProto.getContainerID();
      // CloseContainer operation is idempotent, if the container is already
      // closed, then do nothing.
      if (!container.getContainerSet().getContainer(containerID)
          .getContainerData().isClosed()) {
        LOG.debug("Closing container {}.", containerID);
        HddsProtos.PipelineID pipelineID = closeContainerProto.getPipelineID();
        HddsProtos.ReplicationType replicationType =
            closeContainerProto.getReplicationType();

        ContainerProtos.ContainerCommandRequestProto.Builder request =
            ContainerProtos.ContainerCommandRequestProto.newBuilder();
        request.setCmdType(ContainerProtos.Type.CloseContainer);
        request.setContainerID(containerID);
        request.setCloseContainer(
            ContainerProtos.CloseContainerRequestProto.getDefaultInstance());
        request.setTraceID(UUID.randomUUID().toString());
        request.setDatanodeUuid(
            context.getParent().getDatanodeDetails().getUuidString());
        // submit the close container request for the XceiverServer to handle
        container.submitContainerRequest(
            request.build(), replicationType, pipelineID);
        // Since the container is closed, we trigger an ICR
        IncrementalContainerReportProto icr = IncrementalContainerReportProto
            .newBuilder()
            .addReport(container.getContainerSet()
                .getContainer(containerID).getContainerReport())
            .build();
        context.addReport(icr);
        context.getParent().triggerHeartbeat();
      }
    } catch (Exception e) {
      if (e instanceof NotLeaderException) {
        // If the particular datanode is not the Ratis leader, the close
        // container command will not be executed by the follower but will be
        // executed by Ratis stateMachine transactions via leader to follower.
        // There can also be case where the datanode is in candidate state.
        // In these situations, NotLeaderException is thrown.
        LOG.info("Follower cannot close the container {}.", containerID);
      } else {
        LOG.error("Can't close container " + containerID, e);
      }
    } finally {
      long endTime = Time.monotonicNow();
      totalTime += endTime - startTime;
    }
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
