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
import org.apache.hadoop.hdds.scm.block.PendingDeleteStatusList;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler
    .ReplicationStatus;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
        .PipelineReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .PipelineActionsFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .ContainerActionsFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .CommandStatusReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .NodeReportFromDatanode;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager
    .DeleteContainerCommandCompleted;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager
    .ReplicationCompleted;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationRequest;

import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
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
   * Event generated on DataNode registration.
   */
  public static final TypedEvent<NodeRegistrationContainerReport>
      NODE_REGISTRATION_CONT_REPORT = new TypedEvent<>(
      NodeRegistrationContainerReport.class,
      "Node_Registration_Container_Report");

  /**
   * ContainerReports are send out by Datanodes. This report is received by
   * SCMDatanodeHeartbeatDispatcher and Container_Report Event is generated.
   */
  public static final TypedEvent<ContainerReportFromDatanode> CONTAINER_REPORT =
      new TypedEvent<>(ContainerReportFromDatanode.class, "Container_Report");

  /**
   * IncrementalContainerReports are send out by Datanodes.
   * This report is received by SCMDatanodeHeartbeatDispatcher and
   * Incremental_Container_Report Event is generated.
   */
  public static final TypedEvent<IncrementalContainerReportFromDatanode>
      INCREMENTAL_CONTAINER_REPORT = new TypedEvent<>(
      IncrementalContainerReportFromDatanode.class,
      "Incremental_Container_Report");

  /**
   * ContainerActions are sent by Datanode. This event is received by
   * SCMDatanodeHeartbeatDispatcher and CONTAINER_ACTIONS event is generated.
   */
  public static final TypedEvent<ContainerActionsFromDatanode>
      CONTAINER_ACTIONS = new TypedEvent<>(ContainerActionsFromDatanode.class,
      "Container_Actions");

  /**
   * PipelineReports are send out by Datanodes. This report is received by
   * SCMDatanodeHeartbeatDispatcher and Pipeline_Report Event is generated.
   */
  public static final TypedEvent<PipelineReportFromDatanode> PIPELINE_REPORT =
          new TypedEvent<>(PipelineReportFromDatanode.class, "Pipeline_Report");

  /**
   * PipelineReport processed by pipeline report handler. This event is
   * received by HealthyPipelineSafeModeRule.
   */
  public static final TypedEvent<PipelineReportFromDatanode>
      PROCESSED_PIPELINE_REPORT = new TypedEvent<>(
          PipelineReportFromDatanode.class, "Processed_Pipeline_Report");

  /**
   * PipelineActions are sent by Datanode. This event is received by
   * SCMDatanodeHeartbeatDispatcher and PIPELINE_ACTIONS event is generated.
   */
  public static final TypedEvent<PipelineActionsFromDatanode>
      PIPELINE_ACTIONS = new TypedEvent<>(PipelineActionsFromDatanode.class,
      "Pipeline_Actions");

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

  public static final TypedEvent<CommandForDatanode>
      RETRIABLE_DATANODE_COMMAND =
      new TypedEvent<>(CommandForDatanode.class, "Retriable_Datanode_Command");

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
   * This event will be triggered whenever a datanode is moved from non-healthy
   * state to healthy state.
   */
  public static final TypedEvent<DatanodeDetails> NON_HEALTHY_TO_HEALTHY_NODE =
      new TypedEvent<>(DatanodeDetails.class, "NON_HEALTHY_TO_HEALTHY_NODE");

  /**
   * This event will be triggered by CommandStatusReportHandler whenever a
   * status for Replication SCMCommand is received.
   */
  public static final Event<ReplicationStatus> REPLICATION_STATUS = new
      TypedEvent<>(ReplicationStatus.class, "Replicate_Command_Status");
  /**
   * This event will be triggered by CommandStatusReportHandler whenever a
   * status for DeleteBlock SCMCommand is received.
   */
  public static final TypedEvent<CommandStatusReportHandler.DeleteBlockStatus>
      DELETE_BLOCK_STATUS =
      new TypedEvent<>(CommandStatusReportHandler.DeleteBlockStatus.class,
          "Delete_Block_Status");

  /**
   * This event will be triggered while processing container reports from DN
   * when deleteTransactionID of container in report mismatches with the
   * deleteTransactionID on SCM.
   */
  public static final Event<PendingDeleteStatusList> PENDING_DELETE_STATUS =
      new TypedEvent<>(PendingDeleteStatusList.class, "Pending_Delete_Status");

  /**
   * This is the command for ReplicationManager to handle under/over
   * replication. Sent by the ContainerReportHandler after processing the
   * heartbeat.
   */
  public static final TypedEvent<ReplicationRequest> REPLICATE_CONTAINER =
      new TypedEvent<>(ReplicationRequest.class);

  /**
   * This event is sent by the ReplicaManager to the
   * ReplicationCommandWatcher to track the in-progress replication.
   */
  public static final TypedEvent<ReplicationManager.ReplicationRequestToRepeat>
      TRACK_REPLICATE_COMMAND =
      new TypedEvent<>(ReplicationManager.ReplicationRequestToRepeat.class);

  /**
   * This event is sent by the ReplicaManager to the
   * DeleteContainerCommandWatcher to track the in-progress delete commands.
   */
  public static final TypedEvent<ReplicationManager.DeletionRequestToRepeat>
      TRACK_DELETE_CONTAINER_COMMAND =
      new TypedEvent<>(ReplicationManager.DeletionRequestToRepeat.class);
  /**
   * This event comes from the Heartbeat dispatcher (in fact from the
   * datanode) to notify the scm that the replication is done. This is
   * received by the replicate command watcher to mark in-progress task as
   * finished.
    <p>
   * TODO: Temporary event, should be replaced by specific Heartbeat
   * ActionRequred event.
   */
  public static final TypedEvent<ReplicationCompleted> REPLICATION_COMPLETE =
      new TypedEvent<>(ReplicationCompleted.class);

  public static final TypedEvent<DeleteContainerCommandCompleted>
      DELETE_CONTAINER_COMMAND_COMPLETE =
      new TypedEvent<>(DeleteContainerCommandCompleted.class);

  /**
   * Signal for all the components (but especially for the replication
   * manager and container report handler) that the replication could be
   * started. Should be send only if (almost) all the container state are
   * available from the datanodes.
   */
  public static final TypedEvent<Boolean> START_REPLICATION =
      new TypedEvent<>(Boolean.class);
  public static final TypedEvent<SafeModeStatus> SAFE_MODE_STATUS =
      new TypedEvent<>(SafeModeStatus.class);

  /**
   * Private Ctor. Never Constructed.
   */
  private SCMEvents() {
  }

}
