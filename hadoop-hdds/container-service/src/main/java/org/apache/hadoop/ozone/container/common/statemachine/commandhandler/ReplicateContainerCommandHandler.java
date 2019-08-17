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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.container.replication.ReplicationTask;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command handler to copy containers from sources.
 */
public class ReplicateContainerCommandHandler implements CommandHandler {

  static final Logger LOG =
      LoggerFactory.getLogger(ReplicateContainerCommandHandler.class);

  private int invocationCount;

  private long totalTime;

  private Configuration conf;

  private ReplicationSupervisor supervisor;

  public ReplicateContainerCommandHandler(
      Configuration conf,
      ReplicationSupervisor supervisor) {
    this.conf = conf;
    this.supervisor = supervisor;
  }

  @Override
  public void handle(SCMCommand command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {

    final ReplicateContainerCommand replicateCommand =
        (ReplicateContainerCommand) command;
    final List<DatanodeDetails> sourceDatanodes =
        replicateCommand.getSourceDatanodes();
    final long containerID = replicateCommand.getContainerID();

    Preconditions.checkArgument(sourceDatanodes.size() > 0,
        String.format("Replication command is received for container %d "
            + "but the size of source datanodes was 0.", containerID));

    supervisor.addTask(new ReplicationTask(containerID, sourceDatanodes));
  }

  @Override
  public SCMCommandProto.Type getCommandType() {
    return Type.replicateContainerCommand;
  }

  @Override
  public int getInvocationCount() {
    return this.invocationCount;
  }

  @Override
  public long getAverageRunTime() {
    if (invocationCount > 0) {
      return totalTime / invocationCount;
    }
    return 0;
  }
}
