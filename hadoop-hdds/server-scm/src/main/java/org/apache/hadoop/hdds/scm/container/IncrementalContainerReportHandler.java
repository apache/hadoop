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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server
    .SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handles incremental container reports from datanode.
 */
public class IncrementalContainerReportHandler implements
    EventHandler<IncrementalContainerReportFromDatanode> {

  private static final Logger LOG =
      LoggerFactory.getLogger(IncrementalContainerReportHandler.class);

  private final PipelineManager pipelineManager;
  private final ContainerManager containerManager;

  public IncrementalContainerReportHandler(
      final PipelineManager pipelineManager,
      final ContainerManager containerManager)  {
    Preconditions.checkNotNull(pipelineManager);
    Preconditions.checkNotNull(containerManager);
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;
  }

  @Override
  public void onMessage(
      final IncrementalContainerReportFromDatanode containerReportFromDatanode,
      final EventPublisher publisher) {

    for (ContainerReplicaProto replicaProto :
         containerReportFromDatanode.getReport().getReportList()) {
      try {
        final DatanodeDetails datanodeDetails = containerReportFromDatanode
            .getDatanodeDetails();
        final ContainerID containerID = ContainerID
            .valueof(replicaProto.getContainerID());
        ReportHandlerHelper.processContainerReplica(containerManager,
            containerID, replicaProto, datanodeDetails, publisher, LOG);
      } catch (ContainerNotFoundException e) {
        LOG.warn("Container {} not found!", replicaProto.getContainerID());
      } catch (IOException e) {
        LOG.error("Exception while processing ICR for container {}",
            replicaProto.getContainerID());
      }
    }

  }

}
