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
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;

import java.util.function.Consumer;

/**
 * Generic interface for handlers.
 */
public interface CommandHandler {

  /**
   * Handles a given SCM command.
   * @param command - SCM Command
   * @param container - Ozone Container.
   * @param context - Current Context.
   * @param connectionManager - The SCMs that we are talking to.
   */
  void handle(SCMCommand command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager);

  /**
   * Returns the command type that this command handler handles.
   * @return Type
   */
  SCMCommandProto.Type getCommandType();

  /**
   * Returns number of times this handler has been invoked.
   * @return int
   */
  int getInvocationCount();

  /**
   * Returns the average time this function takes to run.
   * @return  long
   */
  long getAverageRunTime();

  /**
   * Default implementation for updating command status.
   */
  default void updateCommandStatus(StateContext context, SCMCommand command,
      Consumer<CommandStatus> cmdStatusUpdater, Logger log) {
    if (!context.updateCommandStatus(command.getId(), cmdStatusUpdater)) {
      log.debug("{} with Id:{} not found.", command.getType(),
          command.getId());
    }
  }
}
