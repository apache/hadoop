/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.DeleteContainerCommandWatcher;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.IdentifiableEventPayload;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static org.apache.hadoop.hdds.scm.events.SCMEvents
    .TRACK_DELETE_CONTAINER_COMMAND;
import static org.apache.hadoop.hdds.scm.events.SCMEvents
    .TRACK_REPLICATE_COMMAND;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replication Manager manages the replication of the closed container.
 */
public class ReplicationManager implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicationManager.class);

  private ReplicationQueue replicationQueue;

  private ContainerPlacementPolicy containerPlacement;

  private EventPublisher eventPublisher;

  private ReplicationCommandWatcher replicationCommandWatcher;
  private DeleteContainerCommandWatcher deleteContainerCommandWatcher;

  private boolean running = true;

  private ContainerManager containerManager;

  public ReplicationManager(ContainerPlacementPolicy containerPlacement,
      ContainerManager containerManager, EventQueue eventQueue,
      LeaseManager<Long> commandWatcherLeaseManager) {

    this.containerPlacement = containerPlacement;
    this.containerManager = containerManager;
    this.eventPublisher = eventQueue;

    this.replicationCommandWatcher =
        new ReplicationCommandWatcher(TRACK_REPLICATE_COMMAND,
            SCMEvents.REPLICATION_COMPLETE, commandWatcherLeaseManager);

    this.deleteContainerCommandWatcher =
        new DeleteContainerCommandWatcher(TRACK_DELETE_CONTAINER_COMMAND,
            SCMEvents.DELETE_CONTAINER_COMMAND_COMPLETE,
            commandWatcherLeaseManager);

    this.replicationQueue = new ReplicationQueue();

    eventQueue.addHandler(SCMEvents.REPLICATE_CONTAINER,
        (replicationRequest, publisher) -> replicationQueue
            .add(replicationRequest));

    this.replicationCommandWatcher.start(eventQueue);

  }

  public void start() {

    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("Replication Manager").build();

    threadFactory.newThread(this).start();
  }

  @Override
  public void run() {

    while (running) {
      ReplicationRequest request = null;
      try {
        //TODO: add throttling here
        request = replicationQueue.take();

        ContainerID containerID = new ContainerID(request.getContainerId());
        ContainerInfo container = containerManager.getContainer(containerID);
        final HddsProtos.LifeCycleState state = container.getState();

        if (state != LifeCycleState.CLOSED &&
            state != LifeCycleState.QUASI_CLOSED) {
          LOG.warn("Cannot replicate the container {} when in {} state.",
              containerID, state);
          continue;
        }

        //check the current replication
        List<ContainerReplica> containerReplicas =
            new ArrayList<>(getCurrentReplicas(request));

        if (containerReplicas.size() == 0) {
          LOG.warn(
              "Container {} should be replicated but can't find any existing "
                  + "replicas",
              containerID);
          return;
        }

        final ReplicationRequest finalRequest = request;

        int inFlightReplications = replicationCommandWatcher.getTimeoutEvents(
            e -> e.getRequest().getContainerId()
                == finalRequest.getContainerId())
            .size();

        int inFlightDelete = deleteContainerCommandWatcher.getTimeoutEvents(
            e -> e.getRequest().getContainerId()
                == finalRequest.getContainerId())
            .size();

        int deficit =
            (request.getExpecReplicationCount() - containerReplicas.size())
                - (inFlightReplications - inFlightDelete);

        if (deficit > 0) {

          List<DatanodeDetails> datanodes = containerReplicas.stream()
              .sorted((r1, r2) ->
                  r2.getSequenceId().compareTo(r1.getSequenceId()))
              .map(ContainerReplica::getDatanodeDetails)
              .collect(Collectors.toList());
          List<DatanodeDetails> selectedDatanodes = containerPlacement
              .chooseDatanodes(datanodes, deficit, container.getUsedBytes());

          //send the command
          for (DatanodeDetails datanode : selectedDatanodes) {

            LOG.info("Container {} is under replicated." +
                " Expected replica count is {}, but found {}." +
                " Re-replicating it on {}.",
                container.containerID(), request.getExpecReplicationCount(),
                containerReplicas.size(), datanode);

            ReplicateContainerCommand replicateCommand =
                new ReplicateContainerCommand(containerID.getId(), datanodes);

            eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
                new CommandForDatanode<>(
                    datanode.getUuid(), replicateCommand));

            ReplicationRequestToRepeat timeoutEvent =
                new ReplicationRequestToRepeat(replicateCommand.getId(),
                    request);

            eventPublisher.fireEvent(TRACK_REPLICATE_COMMAND, timeoutEvent);

          }

        } else if (deficit < 0) {

          int numberOfReplicasToDelete = Math.abs(deficit);

          final Map<UUID, List<DatanodeDetails>> originIdToDnMap =
              new LinkedHashMap<>();

          containerReplicas.stream()
              .sorted(Comparator.comparing(ContainerReplica::getSequenceId))
              .forEach(replica -> {
                originIdToDnMap.computeIfAbsent(
                    replica.getOriginDatanodeId(), key -> new ArrayList<>());
                originIdToDnMap.get(replica.getOriginDatanodeId())
                    .add(replica.getDatanodeDetails());
              });

          for (List<DatanodeDetails> listOfReplica : originIdToDnMap.values()) {
            if (listOfReplica.size() > 1) {
              final int toDelete = Math.min(listOfReplica.size() - 1,
                  numberOfReplicasToDelete);
              final DeleteContainerCommand deleteContainer =
                  new DeleteContainerCommand(containerID.getId(), true);
              for (int i = 0; i < toDelete; i++) {
                LOG.info("Container {} is over replicated." +
                    " Expected replica count is {}, but found {}." +
                    " Deleting the replica on {}.",
                    container.containerID(), request.getExpecReplicationCount(),
                    containerReplicas.size(), listOfReplica.get(i));
                eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
                    new CommandForDatanode<>(listOfReplica.get(i).getUuid(),
                        deleteContainer));
                DeletionRequestToRepeat timeoutEvent =
                    new DeletionRequestToRepeat(deleteContainer.getId(),
                        request);

                eventPublisher.fireEvent(
                    TRACK_DELETE_CONTAINER_COMMAND, timeoutEvent);
              }
              numberOfReplicasToDelete -= toDelete;
            }
            if (numberOfReplicasToDelete == 0) {
              break;
            }
          }

          if (numberOfReplicasToDelete != 0) {
            final int expectedReplicaCount = container
                .getReplicationFactor().getNumber();

            LOG.warn("Not able to delete the container replica of Container" +
                " {} even though it is over replicated. Expected replica" +
                " count is {}, current replica count is {}.",
                containerID, expectedReplicaCount,
                expectedReplicaCount + numberOfReplicasToDelete);
          }
        }

      } catch (Exception e) {
        LOG.error("Can't replicate container {}", request, e);
      }
    }

  }

  @VisibleForTesting
  protected Set<ContainerReplica> getCurrentReplicas(ReplicationRequest request)
      throws IOException {
    return containerManager
        .getContainerReplicas(new ContainerID(request.getContainerId()));
  }

  @VisibleForTesting
  public ReplicationQueue getReplicationQueue() {
    return replicationQueue;
  }

  public void stop() {
    running = false;
  }

  /**
   * Event for the ReplicationCommandWatcher to repeat the embedded request.
   * in case fof timeout.
   */
  public static class ReplicationRequestToRepeat
      extends ContainerRequestToRepeat {

    public ReplicationRequestToRepeat(
        long commandId, ReplicationRequest request) {
      super(commandId, request);
    }
  }

  /**
   * Event for the DeleteContainerCommandWatcher to repeat the
   * embedded request. In case fof timeout.
   */
  public static class DeletionRequestToRepeat
      extends ContainerRequestToRepeat {

    public DeletionRequestToRepeat(
        long commandId, ReplicationRequest request) {
      super(commandId, request);
    }
  }

  /**
   * Container Request wrapper which will be used by ReplicationManager to
   * perform the intended operation.
   */
  public static class ContainerRequestToRepeat
      implements IdentifiableEventPayload {

    private final long commandId;

    private final ReplicationRequest request;

    ContainerRequestToRepeat(long commandId,
        ReplicationRequest request) {
      this.commandId = commandId;
      this.request = request;
    }

    public ReplicationRequest getRequest() {
      return request;
    }

    @Override
    public long getId() {
      return commandId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ContainerRequestToRepeat that = (ContainerRequestToRepeat) o;
      return Objects.equals(request, that.request);
    }

    @Override
    public int hashCode() {

      return Objects.hash(request);
    }
  }

  /**
   * Event which indicates that the replicate operation is completed.
   */
  public static class ReplicationCompleted
      implements IdentifiableEventPayload {

    private final long uuid;

    public ReplicationCompleted(long uuid) {
      this.uuid = uuid;
    }

    @Override
    public long getId() {
      return uuid;
    }
  }

  /**
   * Event which indicates that the container deletion operation is completed.
   */
  public static class DeleteContainerCommandCompleted
      implements IdentifiableEventPayload {

    private final long uuid;

    public DeleteContainerCommandCompleted(long uuid) {
      this.uuid = uuid;
    }

    @Override
    public long getId() {
      return uuid;
    }
  }
}
