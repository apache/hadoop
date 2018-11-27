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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND;

/**
 * Helper functions to handler container reports.
 */
public final class ReportHandlerHelper {

  private ReportHandlerHelper() {}

  /**
   * Processes the container replica and updates the container state in SCM.
   * If needed, sends command to datanode to update the replica state.
   *
   * @param containerManager ContainerManager instance
   * @param containerId Id of the container
   * @param replicaProto replica of the container
   * @param datanodeDetails datanode where the replica resides
   * @param publisher event publisher
   * @param logger for logging
   * @throws IOException
   */
  static void processContainerReplica(final ContainerManager containerManager,
      final ContainerID containerId, final ContainerReplicaProto replicaProto,
      final DatanodeDetails datanodeDetails, final EventPublisher publisher,
      final Logger logger) throws IOException {

    final ContainerReplica replica = ContainerReplica.newBuilder()
        .setContainerID(containerId)
        .setContainerState(replicaProto.getState())
        .setDatanodeDetails(datanodeDetails)
        .setOriginNodeId(UUID.fromString(replicaProto.getOriginNodeId()))
        .setSequenceId(replicaProto.getBlockCommitSequenceId())
        .build();

    // This is an in-memory update.
    containerManager.updateContainerReplica(containerId, replica);
    ReportHandlerHelper.reconcileContainerState(containerManager,
        containerId, publisher, logger);

    final ContainerInfo containerInfo = containerManager
        .getContainer(containerId);
    if (containerInfo.getUsedBytes() < replicaProto.getUsed()) {
      containerInfo.setUsedBytes(replicaProto.getUsed());
    }

    if (containerInfo.getNumberOfKeys() < replicaProto.getKeyCount()) {
      containerInfo.setNumberOfKeys(replicaProto.getKeyCount());
    }

    // Now we have reconciled the container state. If the container state and
    // the replica state doesn't match, then take appropriate action.
    ReportHandlerHelper.sendReplicaCommands(
        datanodeDetails, containerInfo, replica, publisher, logger);
  }


  /**
   * Reconcile the container state based on the ContainerReplica states.
   * ContainerState is updated after the reconciliation.
   *
   * @param manager ContainerManager
   * @param containerId container id
   * @throws ContainerNotFoundException
   */
  private static void reconcileContainerState(final ContainerManager manager,
      final ContainerID containerId, final EventPublisher publisher,
      final Logger logger) throws IOException {
    // TODO: handle unhealthy replica.
    synchronized (manager.getContainer(containerId)) {
      final ContainerInfo container = manager.getContainer(containerId);
      final Set<ContainerReplica> replicas = manager.getContainerReplicas(
          containerId);
      final LifeCycleState containerState = container.getState();
      switch (containerState) {
      case OPEN:
        /*
         * If the container state is OPEN.
         * None of the replica should be in any other state.
         *
         */
        List<ContainerReplica> invalidReplicas = replicas.stream()
            .filter(replica -> replica.getState() != State.OPEN)
            .collect(Collectors.toList());
        if (!invalidReplicas.isEmpty()) {
          logger.warn("Container {} has invalid replica state." +
              "Invalid Replicas: {}", containerId, invalidReplicas);
        }
        // A container cannot be over replicated when in OPEN state.
        break;
      case CLOSING:
        /*
         * SCM has asked DataNodes to close the container. Now the replicas
         * can be in any of the following states.
         *
         * 1. OPEN
         * 2. CLOSING
         * 3. QUASI_CLOSED
         * 4. CLOSED
         *
         * If all the replica are either in OPEN or CLOSING state, do nothing.
         *
         * If any one of the replica is in QUASI_CLOSED state, move the
         * container to QUASI_CLOSED state.
         *
         * If any one of the replica is in CLOSED state, mark the container as
         * CLOSED. The close has happened via Ratis.
         *
         */
        Optional<ContainerReplica> closedReplica = replicas.stream()
            .filter(replica -> replica.getState() == State.CLOSED)
            .findFirst();
        if (closedReplica.isPresent()) {
          container.updateSequenceId(closedReplica.get().getSequenceId());
          manager.updateContainerState(
              containerId, HddsProtos.LifeCycleEvent.CLOSE);

          // TODO: remove container from OPEN pipeline, since the container is
          // closed we can go ahead and remove it from Ratis pipeline.
        } else if (replicas.stream()
            .anyMatch(replica -> replica.getState() == State.QUASI_CLOSED)) {
          manager.updateContainerState(
              containerId, HddsProtos.LifeCycleEvent.QUASI_CLOSE);
        }
        break;
      case QUASI_CLOSED:
        /*
         * The container is in QUASI_CLOSED state, this means that at least
         * one of the replica is in QUASI_CLOSED/CLOSED state.
         * Other replicas can be in any of the following state.
         *
         * 1. OPEN
         * 2. CLOSING
         * 3. QUASI_CLOSED
         * 4. CLOSED
         *
         * If <50% of container replicas are in QUASI_CLOSED state and all
         * the other replica are either in OPEN or CLOSING state, do nothing.
         * We cannot identify the correct replica since we don't have quorum
         * yet.
         *
         * If >50% (quorum) of replicas are in QUASI_CLOSED state and other
         * replicas are either in OPEN or CLOSING state, try to identify
         * the latest container replica using originNodeId and sequenceId.
         * Force close those replica(s) which have the latest sequenceId.
         *
         * If at least one of the replica is in CLOSED state, mark the
         * container as CLOSED. Force close the replicas which matches the
         * sequenceId of the CLOSED replica.
         *
         */
        if (replicas.stream()
            .anyMatch(replica -> replica.getState() == State.CLOSED)) {
          manager.updateContainerState(
              containerId, HddsProtos.LifeCycleEvent.FORCE_CLOSE);
          // TODO: remove container from OPEN pipeline, since the container is
          // closed we can go ahead and remove it from Ratis pipeline.
        } else {
          final int replicationFactor = container
              .getReplicationFactor().getNumber();
          final List<ContainerReplica> quasiClosedReplicas = replicas.stream()
              .filter(replica -> replica.getState() == State.QUASI_CLOSED)
              .collect(Collectors.toList());
          final long uniqueQuasiClosedReplicaCount = quasiClosedReplicas
              .stream()
              .map(ContainerReplica::getOriginDatanodeId)
              .distinct()
              .count();

          float quasiClosePercent = ((float) uniqueQuasiClosedReplicaCount) /
              ((float) replicationFactor);

          if (quasiClosePercent > 0.5F) {
            // Quorum of unique replica has been QUASI_CLOSED
            long sequenceId = forceCloseContainerReplicaWithHighestSequenceId(
                container, quasiClosedReplicas, publisher);
            if (sequenceId != -1L) {
              container.updateSequenceId(sequenceId);
            }
          }
        }
        break;
      case CLOSED:
        /*
         * The container is already in closed state. do nothing.
         */
        break;
      case DELETING:
        // Not handled.
        throw new UnsupportedOperationException("Unsupported container state" +
            " 'DELETING'.");
      case DELETED:
        // Not handled.
        throw new UnsupportedOperationException("Unsupported container state" +
            " 'DELETED'.");
      default:
        break;
      }
    }
  }

  /**
   * Compares the QUASI_CLOSED replicas of a container and sends close command.
   *
   * @param quasiClosedReplicas list of quasi closed replicas
   * @return the sequenceId of the closed replica.
   */
  private static long forceCloseContainerReplicaWithHighestSequenceId(
      final ContainerInfo container,
      final List<ContainerReplica> quasiClosedReplicas,
      final EventPublisher publisher) {

    final long highestSequenceId = quasiClosedReplicas.stream()
        .map(ContainerReplica::getSequenceId)
        .max(Long::compare)
        .orElse(-1L);

    if (highestSequenceId != -1L) {
      quasiClosedReplicas.stream()
          .filter(replica -> replica.getSequenceId() == highestSequenceId)
          .forEach(replica -> {
            CloseContainerCommand closeContainerCommand =
                new CloseContainerCommand(container.getContainerID(),
                    container.getPipelineID(), true);
            publisher.fireEvent(DATANODE_COMMAND,
                new CommandForDatanode<>(
                    replica.getDatanodeDetails().getUuid(),
                    closeContainerCommand));
          });
    }
    return highestSequenceId;
  }

  /**
   * Based on the container and replica state, send command to datanode if
   * required.
   *
   * @param datanodeDetails datanode where the replica resides
   * @param containerInfo container information
   * @param replica replica information
   * @param publisher queue to publish the datanode command event
   * @param log for logging
   */
  static void sendReplicaCommands(
      final DatanodeDetails datanodeDetails,
      final ContainerInfo containerInfo,
      final ContainerReplica replica,
      final EventPublisher publisher,
      final Logger log) {
    final HddsProtos.LifeCycleState containerState = containerInfo.getState();
    final ContainerReplicaProto.State replicaState = replica.getState();

    if(!ReportHandlerHelper.compareState(containerState, replicaState)) {
      if (containerState == HddsProtos.LifeCycleState.OPEN) {
        // When a container state in SCM is OPEN, there is no way a datanode
        // can quasi close/close the container.
        log.warn("Invalid container replica state for container {}" +
                " from datanode {}. Expected state is OPEN.",
            containerInfo.containerID(), datanodeDetails);
        // The replica can go CORRUPT, we have to handle it.
      }
      if (containerState == HddsProtos.LifeCycleState.CLOSING ||
          containerState == HddsProtos.LifeCycleState.QUASI_CLOSED) {
        // Resend close container event for this datanode if the container
        // replica state is OPEN/CLOSING.
        if (replicaState == ContainerReplicaProto.State.OPEN ||
            replicaState == ContainerReplicaProto.State.CLOSING) {
          CloseContainerCommand closeContainerCommand =
              new CloseContainerCommand(containerInfo.getContainerID(),
                  containerInfo.getPipelineID());
          publisher.fireEvent(DATANODE_COMMAND,
              new CommandForDatanode<>(
                  replica.getDatanodeDetails().getUuid(),
                  closeContainerCommand));
        }
      }
      if (containerState == HddsProtos.LifeCycleState.CLOSED) {
        if (replicaState == ContainerReplicaProto.State.OPEN ||
            replicaState == ContainerReplicaProto.State.CLOSING ||
            replicaState == ContainerReplicaProto.State.QUASI_CLOSED) {
          // Send force close container event for this datanode if the container
          // replica state is OPEN/CLOSING/QUASI_CLOSED.

          // Close command will be send only if this replica matches the
          // sequence of the container.
          if (containerInfo.getSequenceId() ==
              replica.getSequenceId()) {
            CloseContainerCommand closeContainerCommand =
                new CloseContainerCommand(containerInfo.getContainerID(),
                    containerInfo.getPipelineID(), true);
            publisher.fireEvent(DATANODE_COMMAND,
                new CommandForDatanode<>(
                    replica.getDatanodeDetails().getUuid(),
                    closeContainerCommand));
          }
          // TODO: delete the replica if the BCSID doesn't match.
        }
      }
    }

  }

  /**
   * Compares the container and replica state.
   *
   * @param containerState container state
   * @param replicaState replica state
   * @return true if the states are same, else false
   */
  private static boolean compareState(final LifeCycleState containerState,
                              final State replicaState) {
    // TODO: handle unhealthy replica.
    switch (containerState) {
    case OPEN:
      return replicaState == State.OPEN;
    case CLOSING:
      return replicaState == State.CLOSING;
    case QUASI_CLOSED:
      return replicaState == State.QUASI_CLOSED;
    case CLOSED:
      return replicaState == State.CLOSED;
    case DELETING:
      return false;
    case DELETED:
      return false;
    default:
      return false;
    }
  }

}
