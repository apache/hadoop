/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container;

import java.io.IOException;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos
    .ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles incremental container reports from datanode.
 */
public class IncrementalContainerReportHandler extends
    AbstractContainerReportHandler
    implements EventHandler<IncrementalContainerReportFromDatanode> {

  private static final Logger LOG = LoggerFactory.getLogger(
      IncrementalContainerReportHandler.class);

  private final NodeManager nodeManager;

  public IncrementalContainerReportHandler(
      final NodeManager nodeManager,
      final ContainerManager containerManager)  {
    super(containerManager, LOG);
    this.nodeManager = nodeManager;
  }

  @Override
  public void onMessage(final IncrementalContainerReportFromDatanode report,
                        final EventPublisher publisher) {
    LOG.debug("Processing incremental container report from data node {}",
            report.getDatanodeDetails().getUuid());

    for (ContainerReplicaProto replicaProto :
        report.getReport().getReportList()) {
      try {
        final DatanodeDetails dd = report.getDatanodeDetails();
        final ContainerID id = ContainerID.valueof(
            replicaProto.getContainerID());
        nodeManager.addContainer(dd, id);
        processContainerReplica(dd, replicaProto);
      } catch (ContainerNotFoundException e) {
        LOG.warn("Container {} not found!", replicaProto.getContainerID());
      } catch (NodeNotFoundException ex) {
        LOG.error("Received ICR from unknown datanode {} {}",
            report.getDatanodeDetails(), ex);
      } catch (IOException e) {
        LOG.error("Exception while processing ICR for container {}",
            replicaProto.getContainerID());
      }
    }

  }

}
