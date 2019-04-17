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

import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.ozone.container.common.statemachine
    .SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handler to process the DeleteContainerCommand from SCM.
 */
public class DeleteContainerCommandHandler implements CommandHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeleteContainerCommandHandler.class);

  private int invocationCount;
  private long totalTime;

  @Override
  public void handle(final SCMCommand command,
                     final OzoneContainer ozoneContainer,
                     final StateContext context,
                     final SCMConnectionManager connectionManager) {
    final long startTime = Time.monotonicNow();
    invocationCount++;
    try {
      final DeleteContainerCommand deleteContainerCommand =
          (DeleteContainerCommand) command;
      final ContainerController controller = ozoneContainer.getController();
      controller.deleteContainer(deleteContainerCommand.getContainerID(),
          deleteContainerCommand.isForce());
      updateCommandStatus(context, command,
          (cmdStatus) -> cmdStatus.setStatus(true), LOG);
    } catch (IOException e) {
      updateCommandStatus(context, command,
          (cmdStatus) -> cmdStatus.setStatus(false), LOG);
      LOG.error("Exception occurred while deleting the container.", e);
    } finally {
      totalTime += Time.monotonicNow() - startTime;
    }

  }

  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.deleteContainerCommand;
  }

  @Override
  public int getInvocationCount() {
    return this.invocationCount;
  }

  @Override
  public long getAverageRunTime() {
    return invocationCount == 0 ? 0 : totalTime / invocationCount;
  }
}
