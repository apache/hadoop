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

package org.apache.hadoop.hdds.scm.server;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineActionsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerActionsProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.ReregisterCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import com.google.protobuf.GeneratedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hdds.scm.events.SCMEvents.CONTAINER_ACTIONS;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.CONTAINER_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents
    .INCREMENTAL_CONTAINER_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.NODE_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.CMD_STATUS_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.PIPELINE_ACTIONS;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.PIPELINE_REPORT;

/**
 * This class is responsible for dispatching heartbeat from datanode to
 * appropriate EventHandler at SCM.
 */
public final class SCMDatanodeHeartbeatDispatcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMDatanodeHeartbeatDispatcher.class);

  private final NodeManager nodeManager;
  private final EventPublisher eventPublisher;


  public SCMDatanodeHeartbeatDispatcher(NodeManager nodeManager,
                                        EventPublisher eventPublisher) {
    Preconditions.checkNotNull(nodeManager);
    Preconditions.checkNotNull(eventPublisher);
    this.nodeManager = nodeManager;
    this.eventPublisher = eventPublisher;
  }


  /**
   * Dispatches heartbeat to registered event handlers.
   *
   * @param heartbeat heartbeat to be dispatched.
   *
   * @return list of SCMCommand
   */
  public List<SCMCommand> dispatch(SCMHeartbeatRequestProto heartbeat) {
    DatanodeDetails datanodeDetails =
        DatanodeDetails.getFromProtoBuf(heartbeat.getDatanodeDetails());
    List<SCMCommand> commands;

    // If node is not registered, ask the node to re-register. Do not process
    // Heartbeat for unregistered nodes.
    if (!nodeManager.isNodeRegistered(datanodeDetails)) {
      LOG.info("SCM received heartbeat from an unregistered datanode {}. " +
          "Asking datanode to re-register.", datanodeDetails);
      UUID dnID = datanodeDetails.getUuid();
      nodeManager.addDatanodeCommand(dnID, new ReregisterCommand());

      commands = nodeManager.getCommandQueue(dnID);

    } else {

      // should we dispatch heartbeat through eventPublisher?
      commands = nodeManager.processHeartbeat(datanodeDetails);
      if (heartbeat.hasNodeReport()) {
        LOG.debug("Dispatching Node Report.");
        eventPublisher.fireEvent(
            NODE_REPORT,
            new NodeReportFromDatanode(
                datanodeDetails,
                heartbeat.getNodeReport()));
      }

      if (heartbeat.hasContainerReport()) {
        LOG.debug("Dispatching Container Report.");
        eventPublisher.fireEvent(
            CONTAINER_REPORT,
            new ContainerReportFromDatanode(
                datanodeDetails,
                heartbeat.getContainerReport()));

      }

      final List<IncrementalContainerReportProto> icrs =
          heartbeat.getIncrementalContainerReportList();

      if (icrs.size() > 0) {
        LOG.debug("Dispatching ICRs.");
        for (IncrementalContainerReportProto icr : icrs) {
          eventPublisher.fireEvent(INCREMENTAL_CONTAINER_REPORT,
              new IncrementalContainerReportFromDatanode(
                  datanodeDetails, icr));
        }
      }

      if (heartbeat.hasContainerActions()) {
        LOG.debug("Dispatching Container Actions.");
        eventPublisher.fireEvent(
            CONTAINER_ACTIONS,
            new ContainerActionsFromDatanode(
                datanodeDetails,
                heartbeat.getContainerActions()));
      }

      if (heartbeat.hasPipelineReports()) {
        LOG.debug("Dispatching Pipeline Report.");
        eventPublisher.fireEvent(
            PIPELINE_REPORT,
            new PipelineReportFromDatanode(
                datanodeDetails,
                heartbeat.getPipelineReports()));

      }

      if (heartbeat.hasPipelineActions()) {
        LOG.debug("Dispatching Pipeline Actions.");
        eventPublisher.fireEvent(
            PIPELINE_ACTIONS,
            new PipelineActionsFromDatanode(
                datanodeDetails,
                heartbeat.getPipelineActions()));
      }

      if (heartbeat.getCommandStatusReportsCount() != 0) {
        for (CommandStatusReportsProto commandStatusReport : heartbeat
            .getCommandStatusReportsList()) {
          eventPublisher.fireEvent(
              CMD_STATUS_REPORT,
              new CommandStatusReportFromDatanode(
                  datanodeDetails,
                  commandStatusReport));
        }
      }
    }

    return commands;
  }

  /**
   * Wrapper class for events with the datanode origin.
   */
  public static class ReportFromDatanode<T extends GeneratedMessage> {

    private final DatanodeDetails datanodeDetails;

    private final T report;

    public ReportFromDatanode(DatanodeDetails datanodeDetails, T report) {
      this.datanodeDetails = datanodeDetails;
      this.report = report;
    }

    public DatanodeDetails getDatanodeDetails() {
      return datanodeDetails;
    }

    public T getReport() {
      return report;
    }
  }

  /**
   * Node report event payload with origin.
   */
  public static class NodeReportFromDatanode
      extends ReportFromDatanode<NodeReportProto> {

    public NodeReportFromDatanode(DatanodeDetails datanodeDetails,
        NodeReportProto report) {
      super(datanodeDetails, report);
    }
  }

  /**
   * Container report event payload with origin.
   */
  public static class ContainerReportFromDatanode
      extends ReportFromDatanode<ContainerReportsProto> {

    public ContainerReportFromDatanode(DatanodeDetails datanodeDetails,
        ContainerReportsProto report) {
      super(datanodeDetails, report);
    }
  }

  /**
   * Incremental Container report event payload with origin.
   */
  public static class IncrementalContainerReportFromDatanode
      extends ReportFromDatanode<IncrementalContainerReportProto> {

    public IncrementalContainerReportFromDatanode(
        DatanodeDetails datanodeDetails,
        IncrementalContainerReportProto report) {
      super(datanodeDetails, report);
    }
  }

  /**
   * Container action event payload with origin.
   */
  public static class ContainerActionsFromDatanode
      extends ReportFromDatanode<ContainerActionsProto> {

    public ContainerActionsFromDatanode(DatanodeDetails datanodeDetails,
                                       ContainerActionsProto actions) {
      super(datanodeDetails, actions);
    }
  }

  /**
   * Pipeline report event payload with origin.
   */
  public static class PipelineReportFromDatanode
          extends ReportFromDatanode<PipelineReportsProto> {

    public PipelineReportFromDatanode(DatanodeDetails datanodeDetails,
                                      PipelineReportsProto report) {
      super(datanodeDetails, report);
    }
  }

  /**
   * Pipeline action event payload with origin.
   */
  public static class PipelineActionsFromDatanode
      extends ReportFromDatanode<PipelineActionsProto> {

    public PipelineActionsFromDatanode(DatanodeDetails datanodeDetails,
        PipelineActionsProto actions) {
      super(datanodeDetails, actions);
    }
  }

  /**
   * Container report event payload with origin.
   */
  public static class CommandStatusReportFromDatanode
      extends ReportFromDatanode<CommandStatusReportsProto> {

    public CommandStatusReportFromDatanode(DatanodeDetails datanodeDetails,
        CommandStatusReportsProto report) {
      super(datanodeDetails, report);
    }
  }

}
