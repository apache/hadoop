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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.TypedEvent;

import com.google.protobuf.GeneratedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for dispatching heartbeat from datanode to
 * appropriate EventHandler at SCM.
 */
public final class SCMDatanodeHeartbeatDispatcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMDatanodeHeartbeatDispatcher.class);

  private EventPublisher eventPublisher;

  public static final TypedEvent<NodeReportFromDatanode> NODE_REPORT =
      new TypedEvent<>(NodeReportFromDatanode.class);

  public static final TypedEvent<ContainerReportFromDatanode> CONTAINER_REPORT =
      new TypedEvent<ContainerReportFromDatanode>(ContainerReportFromDatanode.class);

  public SCMDatanodeHeartbeatDispatcher(EventPublisher eventPublisher) {
    this.eventPublisher = eventPublisher;
  }


  /**
   * Dispatches heartbeat to registered event handlers.
   *
   * @param heartbeat heartbeat to be dispatched.
   */
  public void dispatch(SCMHeartbeatRequestProto heartbeat) {
    DatanodeDetails datanodeDetails =
        DatanodeDetails.getFromProtoBuf(heartbeat.getDatanodeDetails());

    if (heartbeat.hasNodeReport()) {
      eventPublisher.fireEvent(NODE_REPORT,
          new NodeReportFromDatanode(datanodeDetails,
              heartbeat.getNodeReport()));
    }

    if (heartbeat.hasContainerReport()) {
      eventPublisher.fireEvent(CONTAINER_REPORT,
          new ContainerReportFromDatanode(datanodeDetails,
              heartbeat.getContainerReport()));

    }
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

}
