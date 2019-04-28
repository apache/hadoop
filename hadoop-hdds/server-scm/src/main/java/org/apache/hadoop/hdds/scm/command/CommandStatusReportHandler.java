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

package org.apache.hadoop.hdds.scm.command;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .CommandStatusReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.IdentifiableEventPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Handles CommandStatusReports from datanode.
 */
public class CommandStatusReportHandler implements
    EventHandler<CommandStatusReportFromDatanode> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(CommandStatusReportHandler.class);

  @Override
  public void onMessage(CommandStatusReportFromDatanode report,
      EventPublisher publisher) {
    Preconditions.checkNotNull(report);
    List<CommandStatus> cmdStatusList = report.getReport().getCmdStatusList();
    Preconditions.checkNotNull(cmdStatusList);
    LOGGER.trace("Processing command status report for dn: {}", report
        .getDatanodeDetails());

    // Route command status to its watchers.
    cmdStatusList.forEach(cmdStatus -> {
      LOGGER.trace("Emitting command status for id:{} type: {}", cmdStatus
          .getCmdId(), cmdStatus.getType());
      if (cmdStatus.getType() == SCMCommandProto.Type.deleteBlocksCommand) {
        if (cmdStatus.getStatus() == CommandStatus.Status.EXECUTED) {
          publisher.fireEvent(SCMEvents.DELETE_BLOCK_STATUS,
              new DeleteBlockStatus(cmdStatus));
        }
      } else {
        LOGGER.debug("CommandStatus of type:{} not handled in " +
            "CommandStatusReportHandler.", cmdStatus.getType());
      }
    });
  }

  /**
   * Wrapper event for CommandStatus.
   */
  public static class CommandStatusEvent implements IdentifiableEventPayload {
    private CommandStatus cmdStatus;

    CommandStatusEvent(CommandStatus cmdStatus) {
      this.cmdStatus = cmdStatus;
    }

    public CommandStatus getCmdStatus() {
      return cmdStatus;
    }

    @Override
    public String toString() {
      return "CommandStatusEvent:" + cmdStatus.toString();
    }

    @Override
    public long getId() {
      return cmdStatus.getCmdId();
    }
  }

  /**
   * Wrapper event for DeleteBlock Command.
   */
  public static class DeleteBlockStatus extends CommandStatusEvent {
    public DeleteBlockStatus(CommandStatus cmdStatus) {
      super(cmdStatus);
    }
  }

}
