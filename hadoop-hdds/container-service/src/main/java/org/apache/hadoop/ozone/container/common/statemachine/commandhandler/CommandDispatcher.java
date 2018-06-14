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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Dispatches command to the correct handler.
 */
public final class CommandDispatcher {
  static final Logger LOG =
      LoggerFactory.getLogger(CommandDispatcher.class);
  private final StateContext context;
  private final Map<Type, CommandHandler> handlerMap;
  private final OzoneContainer container;
  private final SCMConnectionManager connectionManager;

  /**
   * Constructs a command Dispatcher.
   * @param context - Context.
   */
  /**
   * Constructs a command dispatcher.
   *
   * @param container - Ozone Container
   * @param context - Context
   * @param handlers - Set of handlers.
   */
  private CommandDispatcher(OzoneContainer container, SCMConnectionManager
      connectionManager, StateContext context,
      CommandHandler... handlers) {
    Preconditions.checkNotNull(context);
    Preconditions.checkNotNull(handlers);
    Preconditions.checkArgument(handlers.length > 0);
    Preconditions.checkNotNull(container);
    Preconditions.checkNotNull(connectionManager);
    this.context = context;
    this.container = container;
    this.connectionManager = connectionManager;
    handlerMap = new HashMap<>();
    for (CommandHandler h : handlers) {
      if(handlerMap.containsKey(h.getCommandType())){
        LOG.error("Duplicate handler for the same command. Exiting. Handle " +
            "key : { }", h.getCommandType().getDescriptorForType().getName());
        throw new IllegalArgumentException("Duplicate handler for the same " +
            "command.");
      }
      handlerMap.put(h.getCommandType(), h);
    }
  }

  public CommandHandler getCloseContainerHandler() {
    return handlerMap.get(Type.closeContainerCommand);
  }

  /**
   * Dispatch the command to the correct handler.
   *
   * @param command - SCM Command.
   */
  public void handle(SCMCommand command) {
    Preconditions.checkNotNull(command);
    CommandHandler handler = handlerMap.get(command.getType());
    if (handler != null) {
      handler.handle(command, container, context, connectionManager);
    } else {
      LOG.error("Unknown SCM Command queued. There is no handler for this " +
          "command. Command: {}", command.getType().getDescriptorForType()
          .getName());
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Helper class to construct command dispatcher.
   */
  public static class Builder {
    private final List<CommandHandler> handlerList;
    private OzoneContainer container;
    private StateContext context;
    private SCMConnectionManager connectionManager;

    public Builder() {
      handlerList = new LinkedList<>();
    }

    /**
     * Adds a handler.
     *
     * @param handler - handler
     * @return Builder
     */
    public Builder addHandler(CommandHandler handler) {
      Preconditions.checkNotNull(handler);
      handlerList.add(handler);
      return this;
    }

    /**
     * Add the OzoneContainer.
     *
     * @param ozoneContainer - ozone container.
     * @return Builder
     */
    public Builder setContainer(OzoneContainer ozoneContainer) {
      Preconditions.checkNotNull(ozoneContainer);
      this.container = ozoneContainer;
      return this;
    }

    /**
     * Set the Connection Manager.
     *
     * @param scmConnectionManager
     * @return this
     */
    public Builder setConnectionManager(SCMConnectionManager
        scmConnectionManager) {
      Preconditions.checkNotNull(scmConnectionManager);
      this.connectionManager = scmConnectionManager;
      return this;
    }

    /**
     * Sets the Context.
     *
     * @param stateContext - StateContext
     * @return this
     */
    public Builder setContext(StateContext stateContext) {
      Preconditions.checkNotNull(stateContext);
      this.context = stateContext;
      return this;
    }

    /**
     * Builds a command Dispatcher.
     * @return Command Dispatcher.
     */
    public CommandDispatcher build() {
      Preconditions.checkNotNull(this.connectionManager, "Missing connection" +
          " manager.");
      Preconditions.checkNotNull(this.container, "Missing container.");
      Preconditions.checkNotNull(this.context, "Missing context.");
      Preconditions.checkArgument(this.handlerList.size() > 0);
      return new CommandDispatcher(this.container, this.connectionManager,
          this.context, handlerList.toArray(
              new CommandHandler[handlerList.size()]));
    }
  }
}
