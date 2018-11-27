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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.block.PendingDeleteStatusList;
import org.apache.hadoop.hdds.scm.container.replication
    .ReplicationActivityStatus;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationRequest;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server
    .SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles container reports from datanode.
 */
public class ContainerReportHandler implements
    EventHandler<ContainerReportFromDatanode> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerReportHandler.class);

  private final NodeManager nodeManager;
  private final PipelineManager pipelineManager;
  private final ContainerManager containerManager;
  private final ReplicationActivityStatus replicationStatus;

  public ContainerReportHandler(final NodeManager nodeManager,
      final PipelineManager pipelineManager,
      final ContainerManager containerManager,
      final ReplicationActivityStatus replicationActivityStatus) {
    Preconditions.checkNotNull(nodeManager);
    Preconditions.checkNotNull(pipelineManager);
    Preconditions.checkNotNull(containerManager);
    Preconditions.checkNotNull(replicationActivityStatus);
    this.nodeManager = nodeManager;
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;
    this.replicationStatus = replicationActivityStatus;
  }

  @Override
  public void onMessage(final ContainerReportFromDatanode reportFromDatanode,
      final EventPublisher publisher) {

    final DatanodeDetails datanodeDetails =
        reportFromDatanode.getDatanodeDetails();

    final ContainerReportsProto containerReport =
        reportFromDatanode.getReport();

    try {

      final List<ContainerReplicaProto> replicas = containerReport
          .getReportsList();

      // ContainerIDs which SCM expects this datanode to have.
      final Set<ContainerID> expectedContainerIDs = nodeManager
          .getContainers(datanodeDetails);

      // ContainerIDs that this datanode actually has.
      final Set<ContainerID> actualContainerIDs = replicas.parallelStream()
          .map(ContainerReplicaProto::getContainerID)
          .map(ContainerID::valueof).collect(Collectors.toSet());

      // Container replicas which SCM is not aware of.
      final  Set<ContainerID> newReplicas =
          new HashSet<>(actualContainerIDs);
      newReplicas.removeAll(expectedContainerIDs);

      // Container replicas which are missing from datanode.
      final Set<ContainerID> missingReplicas =
          new HashSet<>(expectedContainerIDs);
      missingReplicas.removeAll(actualContainerIDs);

      processContainerReplicas(datanodeDetails, replicas, publisher);

      // Remove missing replica from ContainerManager
      for (ContainerID id : missingReplicas) {
        try {
          containerManager.getContainerReplicas(id)
              .stream()
              .filter(replica ->
                  replica.getDatanodeDetails().equals(datanodeDetails))
              .findFirst()
              .ifPresent(replica -> {
                try {
                  containerManager.removeContainerReplica(id, replica);
                } catch (ContainerNotFoundException |
                    ContainerReplicaNotFoundException e) {
                  // This should not happen, but even if it happens, not an
                  // issue
                }
              });
        } catch (ContainerNotFoundException e) {
          LOG.warn("Cannot remove container replica, container {} not found",
              id);
        }
      }

      // Update the latest set of containers for this datanode in NodeManager.
      nodeManager.setContainers(datanodeDetails, actualContainerIDs);

      // Replicate if needed.
      newReplicas.forEach(id -> checkReplicationState(id, publisher));
      missingReplicas.forEach(id -> checkReplicationState(id, publisher));

    } catch (NodeNotFoundException ex) {
      LOG.error("Received container report from unknown datanode {}",
          datanodeDetails);
    }

  }

  private void processContainerReplicas(final DatanodeDetails datanodeDetails,
      final List<ContainerReplicaProto> replicas,
      final EventPublisher publisher) {
    final PendingDeleteStatusList pendingDeleteStatusList =
        new PendingDeleteStatusList(datanodeDetails);
    for (ContainerReplicaProto replicaProto : replicas) {
      try {
        final ContainerID containerID = ContainerID.valueof(
            replicaProto.getContainerID());

        ReportHandlerHelper.processContainerReplica(containerManager,
            containerID, replicaProto, datanodeDetails, publisher, LOG);

        final ContainerInfo containerInfo = containerManager
            .getContainer(containerID);

        if (containerInfo.getDeleteTransactionId() >
            replicaProto.getDeleteTransactionId()) {
          pendingDeleteStatusList
              .addPendingDeleteStatus(replicaProto.getDeleteTransactionId(),
                  containerInfo.getDeleteTransactionId(),
                  containerInfo.getContainerID());
        }
      } catch (ContainerNotFoundException e) {
        LOG.error("Received container report for an unknown container {} from" +
                " datanode {}", replicaProto.getContainerID(), datanodeDetails);
      } catch (IOException e) {
        LOG.error("Exception while processing container report for container" +
                " {} from datanode {}",
            replicaProto.getContainerID(), datanodeDetails);
      }
    }
    if (pendingDeleteStatusList.getNumPendingDeletes() > 0) {
      publisher.fireEvent(SCMEvents.PENDING_DELETE_STATUS,
          pendingDeleteStatusList);
    }
  }

  private void checkReplicationState(ContainerID containerID,
      EventPublisher publisher) {
    try {
      ContainerInfo container = containerManager.getContainer(containerID);
      replicateIfNeeded(container, publisher);
    } catch (ContainerNotFoundException ex) {
      LOG.warn(
          "Container is missing from containerStateManager. Can't request "
              + "replication. {}",
          containerID);
    }

  }

  private void replicateIfNeeded(ContainerInfo container,
      EventPublisher publisher) throws ContainerNotFoundException {
    if (!container.isOpen() && replicationStatus.isReplicationEnabled()) {
      final int existingReplicas = containerManager
          .getContainerReplicas(container.containerID()).size();
      final int expectedReplicas = container.getReplicationFactor().getNumber();
      if (existingReplicas != expectedReplicas) {
        publisher.fireEvent(SCMEvents.REPLICATE_CONTAINER,
            new ReplicationRequest(container.getContainerID(),
                existingReplicas, expectedReplicas));
      }
    }
  }
}
