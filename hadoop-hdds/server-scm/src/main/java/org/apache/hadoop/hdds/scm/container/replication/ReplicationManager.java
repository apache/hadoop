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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.IdentifiableEventPayload;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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

  private boolean running = true;

  private ContainerStateManager containerStateManager;

  public ReplicationManager(ContainerPlacementPolicy containerPlacement,
      ContainerStateManager containerStateManager, EventQueue eventQueue,
      LeaseManager<Long> commandWatcherLeaseManager) {

    this.containerPlacement = containerPlacement;
    this.containerStateManager = containerStateManager;
    this.eventPublisher = eventQueue;

    this.replicationCommandWatcher =
        new ReplicationCommandWatcher(TRACK_REPLICATE_COMMAND,
            SCMEvents.REPLICATION_COMPLETE, commandWatcherLeaseManager);

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

  public void run() {

    while (running) {
      ReplicationRequest request = null;
      try {
        //TODO: add throttling here
        request = replicationQueue.take();

        ContainerID containerID = new ContainerID(request.getContainerId());
        ContainerInfo containerInfo =
            containerStateManager.getContainer(containerID);

        Preconditions.checkNotNull(containerInfo,
            "No information about the container " + request.getContainerId());

        Preconditions
            .checkState(containerInfo.getState() == LifeCycleState.CLOSED,
                "Container should be in closed state");

        //check the current replication
        List<DatanodeDetails> datanodesWithReplicas =
            new ArrayList<>(getCurrentReplicas(request));

        if (datanodesWithReplicas.size() == 0) {
          LOG.warn(
              "Container {} should be replicated but can't find any existing "
                  + "replicas",
              containerID);
          return;
        }

        ReplicationRequest finalRequest = request;

        int inFlightReplications = replicationCommandWatcher.getTimeoutEvents(
            e -> e.request.getContainerId() == finalRequest.getContainerId())
            .size();

        int deficit =
            request.getExpecReplicationCount() - datanodesWithReplicas.size()
                - inFlightReplications;

        if (deficit > 0) {

          List<DatanodeDetails> selectedDatanodes = containerPlacement
              .chooseDatanodes(datanodesWithReplicas, deficit,
                  containerInfo.getUsedBytes());

          //send the command
          for (DatanodeDetails datanode : selectedDatanodes) {

            ReplicateContainerCommand replicateCommand =
                new ReplicateContainerCommand(containerID.getId(),
                    datanodesWithReplicas);

            eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
                new CommandForDatanode<>(
                    datanode.getUuid(), replicateCommand));

            ReplicationRequestToRepeat timeoutEvent =
                new ReplicationRequestToRepeat(replicateCommand.getId(),
                    request);

            eventPublisher.fireEvent(TRACK_REPLICATE_COMMAND, timeoutEvent);

          }

        } else if (deficit < 0) {
          //TODO: too many replicas. Not handled yet.
        }

      } catch (Exception e) {
        LOG.error("Can't replicate container {}", request, e);
      }
    }

  }

  @VisibleForTesting
  protected Set<DatanodeDetails> getCurrentReplicas(ReplicationRequest request)
      throws IOException {
    return containerStateManager
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
   * Event for the ReplicationCommandWatcher to repeate the embedded request.
   * in case fof timeout.
   */
  public static class ReplicationRequestToRepeat
      implements IdentifiableEventPayload {

    private final long commandId;

    private final ReplicationRequest request;

    public ReplicationRequestToRepeat(long commandId,
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
      ReplicationRequestToRepeat that = (ReplicationRequestToRepeat) o;
      return Objects.equals(request, that.request);
    }

    @Override
    public int hashCode() {

      return Objects.hash(request);
    }
  }

  public static class ReplicationCompleted implements IdentifiableEventPayload {

    private final long uuid;

    public ReplicationCompleted(long uuid) {
      this.uuid = uuid;
    }

    @Override
    public long getId() {
      return uuid;
    }
  }
}
