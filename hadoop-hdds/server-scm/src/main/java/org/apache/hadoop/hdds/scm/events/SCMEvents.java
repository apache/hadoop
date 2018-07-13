/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.scm.events;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .CommandStatusReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .NodeReportFromDatanode;

import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.TypedEvent;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;

/**
 * Class that acts as the namespace for all SCM Events.
 */
public final class SCMEvents {

  /**
   * NodeReports are  sent out by Datanodes. This report is received by
   * SCMDatanodeHeartbeatDispatcher and NodeReport Event is generated.
   */
  public static final TypedEvent<NodeReportFromDatanode> NODE_REPORT =
      new TypedEvent<>(NodeReportFromDatanode.class, "Node_Report");
  /**
   * ContainerReports are send out by Datanodes. This report is received by
   * SCMDatanodeHeartbeatDispatcher and Container_Report Event
   * isTestSCMDatanodeHeartbeatDispatcher generated.
   */
  public static final TypedEvent<ContainerReportFromDatanode> CONTAINER_REPORT =
      new TypedEvent<>(ContainerReportFromDatanode.class, "Container_Report");

  /**
   * A Command status report will be sent by datanodes. This repoort is received
   * by SCMDatanodeHeartbeatDispatcher and CommandReport event is generated.
   */
  public static final TypedEvent<CommandStatusReportFromDatanode>
      CMD_STATUS_REPORT =
      new TypedEvent<>(CommandStatusReportFromDatanode.class,
          "Cmd_Status_Report");

  /**
   * When ever a command for the Datanode needs to be issued by any component
   * inside SCM, a Datanode_Command event is generated. NodeManager listens to
   * these events and dispatches them to Datanode for further processing.
   */
  public static final Event<CommandForDatanode> DATANODE_COMMAND =
      new TypedEvent<>(CommandForDatanode.class, "Datanode_Command");

  /**
   * A Close Container Event can be triggered under many condition. Some of them
   * are: 1. A Container is full, then we stop writing further information to
   * that container. DN's let SCM know that current state and sends a
   * informational message that allows SCM to close the container.
   * <p>
   * 2. If a pipeline is open; for example Ratis; if a single node fails, we
   * will proactively close these containers.
   * <p>
   * Once a command is dispatched to DN, we will also listen to updates from the
   * datanode which lets us know that this command completed or timed out.
   */
  public static final TypedEvent<ContainerID> CLOSE_CONTAINER =
      new TypedEvent<>(ContainerID.class, "Close_Container");

  /**
   * This event will be triggered whenever a new datanode is registered with
   * SCM.
   */
  public static final TypedEvent<DatanodeDetails> NEW_NODE =
      new TypedEvent<>(DatanodeDetails.class, "New_Node");

  /**
   * This event will be triggered whenever a datanode is moved from healthy to
   * stale state.
   */
  public static final TypedEvent<DatanodeDetails> STALE_NODE =
      new TypedEvent<>(DatanodeDetails.class, "Stale_Node");

  /**
   * This event will be triggered whenever a datanode is moved from stale to
   * dead state.
   */
  public static final TypedEvent<DatanodeDetails> DEAD_NODE =
      new TypedEvent<>(DatanodeDetails.class, "Dead_Node");

  /**
   * Private Ctor. Never Constructed.
   */
  private SCMEvents() {
  }

}
