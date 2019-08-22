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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Base class for all the container report handlers.
 */
public class AbstractContainerReportHandler {

  private final ContainerManager containerManager;
  private final Logger logger;

  /**
   * Constructs AbstractContainerReportHandler instance with the
   * given ContainerManager instance.
   *
   * @param containerManager ContainerManager
   * @param logger Logger to be used for logging
   */
  AbstractContainerReportHandler(final ContainerManager containerManager,
                                 final Logger logger) {
    Preconditions.checkNotNull(containerManager);
    Preconditions.checkNotNull(logger);
    this.containerManager = containerManager;
    this.logger = logger;
  }

  /**
   * Process the given ContainerReplica received from specified datanode.
   *
   * @param datanodeDetails DatanodeDetails of the node which reported
   *                        this replica
   * @param replicaProto ContainerReplica
   *
   * @throws IOException In case of any Exception while processing the report
   */
  void processContainerReplica(final DatanodeDetails datanodeDetails,
                               final ContainerReplicaProto replicaProto)
      throws IOException {
    final ContainerID containerId = ContainerID
        .valueof(replicaProto.getContainerID());
    final ContainerReplica replica = ContainerReplica.newBuilder()
        .setContainerID(containerId)
        .setContainerState(replicaProto.getState())
        .setDatanodeDetails(datanodeDetails)
        .setOriginNodeId(UUID.fromString(replicaProto.getOriginNodeId()))
        .setSequenceId(replicaProto.getBlockCommitSequenceId())
        .build();

    logger.debug("Processing replica of container {} from datanode {}",
        containerId, datanodeDetails);
    // Synchronized block should be replaced by container lock,
    // once we have introduced lock inside ContainerInfo.
    synchronized (containerManager.getContainer(containerId)) {
      updateContainerStats(containerId, replicaProto);
      updateContainerState(datanodeDetails, containerId, replica);
      containerManager.updateContainerReplica(containerId, replica);
    }
  }

  /**
   * Update the container stats if it's lagging behind the stats in reported
   * replica.
   *
   * @param containerId ID of the container
   * @param replicaProto Container Replica information
   * @throws ContainerNotFoundException If the container is not present
   */
  private void updateContainerStats(final ContainerID containerId,
                                    final ContainerReplicaProto replicaProto)
      throws ContainerNotFoundException {

    if (!isUnhealthy(replicaProto::getState)) {
      final ContainerInfo containerInfo = containerManager
          .getContainer(containerId);

      if (containerInfo.getSequenceId() <
          replicaProto.getBlockCommitSequenceId()) {
        containerInfo.updateSequenceId(
            replicaProto.getBlockCommitSequenceId());
      }
      if (containerInfo.getUsedBytes() < replicaProto.getUsed()) {
        containerInfo.setUsedBytes(replicaProto.getUsed());
      }
      if (containerInfo.getNumberOfKeys() < replicaProto.getKeyCount()) {
        containerInfo.setNumberOfKeys(replicaProto.getKeyCount());
      }
    }
  }

  /**
   * Updates the container state based on the given replica state.
   *
   * @param datanode Datanode from which the report is received
   * @param containerId ID of the container
   * @param replica ContainerReplica
   * @throws IOException In case of Exception
   */
  private void updateContainerState(final DatanodeDetails datanode,
                                    final ContainerID containerId,
                                    final ContainerReplica replica)
      throws IOException {

    final ContainerInfo container = containerManager
        .getContainer(containerId);

    switch (container.getState()) {
    case OPEN:
      /*
       * If the state of a container is OPEN, datanodes cannot report
       * any other state.
       */
      if (replica.getState() != State.OPEN) {
        logger.warn("Container {} is in OPEN state, but the datanode {} " +
            "reports an {} replica.", containerId,
            datanode, replica.getState());
        // Should we take some action?
      }
      break;
    case CLOSING:
      /*
       * When the container is in CLOSING state the replicas can be in any
       * of the following states:
       *
       * - OPEN
       * - CLOSING
       * - QUASI_CLOSED
       * - CLOSED
       *
       * If all the replica are either in OPEN or CLOSING state, do nothing.
       *
       * If the replica is in QUASI_CLOSED state, move the container to
       * QUASI_CLOSED state.
       *
       * If the replica is in CLOSED state, mark the container as CLOSED.
       *
       */

      if (replica.getState() == State.QUASI_CLOSED) {
        logger.info("Moving container {} to QUASI_CLOSED state, datanode {} " +
                "reported QUASI_CLOSED replica.", containerId, datanode);
        containerManager.updateContainerState(containerId,
            LifeCycleEvent.QUASI_CLOSE);
      }

      if (replica.getState() == State.CLOSED) {
        logger.info("Moving container {} to CLOSED state, datanode {} " +
            "reported CLOSED replica.", containerId, datanode);
        Preconditions.checkArgument(replica.getSequenceId()
            == container.getSequenceId());
        containerManager.updateContainerState(containerId,
            LifeCycleEvent.CLOSE);
      }

      break;
    case QUASI_CLOSED:
      /*
       * The container is in QUASI_CLOSED state, this means that at least
       * one of the replica was QUASI_CLOSED.
       *
       * Now replicas can be in any of the following state.
       *
       * 1. OPEN
       * 2. CLOSING
       * 3. QUASI_CLOSED
       * 4. CLOSED
       *
       * If at least one of the replica is in CLOSED state, mark the
       * container as CLOSED.
       *
       */
      if (replica.getState() == State.CLOSED) {
        logger.info("Moving container {} to CLOSED state, datanode {} " +
            "reported CLOSED replica.", containerId, datanode);
        Preconditions.checkArgument(replica.getSequenceId()
            == container.getSequenceId());
        containerManager.updateContainerState(containerId,
            LifeCycleEvent.FORCE_CLOSE);
      }
      break;
    case CLOSED:
      /*
       * The container is already in closed state. do nothing.
       */
      break;
    case DELETING:
      throw new UnsupportedOperationException(
          "Unsupported container state 'DELETING'.");
    case DELETED:
      throw new UnsupportedOperationException(
          "Unsupported container state 'DELETED'.");
    default:
      break;
    }
  }

  /**
   * Returns true if the container replica is not marked as UNHEALTHY.
   *
   * @param replicaState State of the container replica.
   * @return true if unhealthy, false otherwise
   */
  private boolean isUnhealthy(final Supplier<State> replicaState) {
    return replicaState.get() == ContainerReplicaProto.State.UNHEALTHY;
  }

}