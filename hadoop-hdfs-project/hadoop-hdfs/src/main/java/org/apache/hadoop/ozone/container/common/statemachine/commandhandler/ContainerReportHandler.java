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

import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Container Report handler.
 */
public class ContainerReportHandler implements CommandHandler {
  static final Logger LOG =
      LoggerFactory.getLogger(ContainerReportHandler.class);
  private int invocationCount;
  private long totalTime;

  /**
   * Constructs a ContainerReport handler.
   */
  public ContainerReportHandler() {
  }

  /**
   * Handles a given SCM command.
   *
   * @param command - SCM Command
   * @param container - Ozone Container.
   * @param context - Current Context.
   * @param connectionManager - The SCMs that we are talking to.
   */
  @Override
  public void handle(SCMCommand command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {
    LOG.debug("Processing Container Report.");
    invocationCount++;
    long startTime = Time.monotonicNow();
    try {
      ContainerReportsRequestProto contianerReport =
          container.getContainerReport();

      // TODO : We send this report to all SCMs.Check if it is enough only to
      // send to the leader once we have RAFT enabled SCMs.
      for (EndpointStateMachine endPoint : connectionManager.getValues()) {
        endPoint.getEndPoint().sendContainerReport(contianerReport);
      }
    } catch (IOException ex) {
      LOG.error("Unable to process the Container Report command.", ex);
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
  public Type getCommandType() {
    return Type.sendContainerReport;
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
