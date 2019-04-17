/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.GeneratedMessage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.lock.LockManager;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Replication Manager (RM) is the one which is responsible for making sure
 * that the containers are properly replicated. Replication Manager deals only
 * with Quasi Closed / Closed container.
 */
public class ReplicationManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicationManager.class);

  /**
   * Reference to the ContainerManager.
   */
  private final ContainerManager containerManager;

  /**
   * PlacementPolicy which is used to identify where a container
   * should be replicated.
   */
  private final ContainerPlacementPolicy containerPlacement;

  /**
   * EventPublisher to fire Replicate and Delete container events.
   */
  private final EventPublisher eventPublisher;

  /**
   * Used for locking a container using its ID while processing it.
   */
  private final LockManager<ContainerID> lockManager;

  /**
   * This is used for tracking container replication commands which are issued
   * by ReplicationManager and not yet complete.
   */
  private final Map<ContainerID, List<InflightAction>> inflightReplication;

  /**
   * This is used for tracking container deletion commands which are issued
   * by ReplicationManager and not yet complete.
   */
  private final Map<ContainerID, List<InflightAction>> inflightDeletion;

  /**
   * ReplicationMonitor thread is the one which wakes up at configured
   * interval and processes all the containers.
   */
  private final Thread replicationMonitor;

  /**
   * The frequency in which ReplicationMonitor thread should run.
   */
  private final long interval;

  /**
   * Timeout for container replication & deletion command issued by
   * ReplicationManager.
   */
  private final long eventTimeout;

  /**
   * Flag used for checking if the ReplicationMonitor thread is running or
   * not.
   */
  private volatile boolean running;

  /**
   * Constructs ReplicationManager instance with the given configuration.
   *
   * @param conf OzoneConfiguration
   * @param containerManager ContainerManager
   * @param containerPlacement ContainerPlacementPolicy
   * @param eventPublisher EventPublisher
   */
  public ReplicationManager(final Configuration conf,
                            final ContainerManager containerManager,
                            final ContainerPlacementPolicy containerPlacement,
                            final EventPublisher eventPublisher) {
    this.containerManager = containerManager;
    this.containerPlacement = containerPlacement;
    this.eventPublisher = eventPublisher;
    this.lockManager = new LockManager<>(conf);
    this.inflightReplication = new HashMap<>();
    this.inflightDeletion = new HashMap<>();
    this.replicationMonitor = new Thread(this::run);
    this.replicationMonitor.setName("ReplicationMonitor");
    this.replicationMonitor.setDaemon(true);
    this.interval = conf.getTimeDuration(
        ScmConfigKeys.HDDS_SCM_REPLICATION_THREAD_INTERVAL,
        ScmConfigKeys.HDDS_SCM_REPLICATION_THREAD_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.eventTimeout = conf.getTimeDuration(
        ScmConfigKeys.HDDS_SCM_REPLICATION_EVENT_TIMEOUT,
        ScmConfigKeys.HDDS_SCM_REPLICATION_EVENT_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.running = false;
  }

  /**
   * Starts Replication Monitor thread.
   */
  public synchronized void start() {
    if (!running) {
      LOG.info("Starting Replication Monitor Thread.");
      running = true;
      replicationMonitor.start();
    } else {
      LOG.info("Replication Monitor Thread is already running.");
    }
  }

  /**
   * Returns true if the Replication Monitor Thread is running.
   *
   * @return true if running, false otherwise
   */
  public boolean isRunning() {
    return replicationMonitor.isAlive();
  }

  /**
   * Process all the containers immediately.
   */
  @VisibleForTesting
  @SuppressFBWarnings(value="NN_NAKED_NOTIFY",
      justification="Used only for testing")
  synchronized void processContainersNow() {
    notify();
  }

  /**
   * Stops Replication Monitor thread.
   */
  public synchronized void stop() {
    if (running) {
      LOG.info("Stopping Replication Monitor Thread.");
      running = false;
      notify();
    } else {
      LOG.info("Replication Monitor Thread is not running.");
    }
  }

  /**
   * ReplicationMonitor thread runnable. This wakes up at configured
   * interval and processes all the containers in the system.
   */
  private synchronized void run() {
    try {
      while (running) {
        final long start = Time.monotonicNow();
        final Set<ContainerID> containerIds =
            containerManager.getContainerIDs();
        containerIds.forEach(this::processContainer);

        LOG.info("Replication Monitor Thread took {} milliseconds for" +
                " processing {} containers.", Time.monotonicNow() - start,
            containerIds.size());

        wait(interval);
      }
    } catch (Throwable t) {
      // When we get runtime exception, we should terminate SCM.
      LOG.error("Exception in Replication Monitor Thread.", t);
      ExitUtil.terminate(1, t);
    }
  }

  /**
   * Process the given container.
   *
   * @param id ContainerID
   */
  private void processContainer(ContainerID id) {
    lockManager.lock(id);
    try {
      final ContainerInfo container = containerManager.getContainer(id);
      final Set<ContainerReplica> replicas = containerManager
          .getContainerReplicas(container.containerID());
      final LifeCycleState state = container.getState();

      /*
       * We don't take any action if the container is in OPEN state.
       */
      if (state == LifeCycleState.OPEN) {
        return;
      }

      /*
       * If the container is in CLOSING state, the replicas can either
       * be in OPEN or in CLOSING state. In both of this cases
       * we have to resend close container command to the datanodes.
       */
      if (state == LifeCycleState.CLOSING) {
        replicas.forEach(replica -> sendCloseCommand(
            container, replica.getDatanodeDetails(), false));
        return;
      }

      /*
       * If the container is in QUASI_CLOSED state, check and close the
       * container if possible.
       */
      if (state == LifeCycleState.QUASI_CLOSED &&
          canForceCloseContainer(container, replicas)) {
        forceCloseContainer(container, replicas);
        return;
      }

      /*
       * Before processing the container we have to reconcile the
       * inflightReplication and inflightDeletion actions.
       *
       * We remove the entry from inflightReplication and inflightDeletion
       * list, if the operation is completed or if it has timed out.
       */
      updateInflightAction(container, inflightReplication,
          action -> replicas.stream()
              .anyMatch(r -> r.getDatanodeDetails().equals(action.datanode)));

      updateInflightAction(container, inflightDeletion,
          action -> replicas.stream()
              .noneMatch(r -> r.getDatanodeDetails().equals(action.datanode)));


      /*
       * We don't have to take any action if the container is healthy.
       *
       * According to ReplicationMonitor container is considered healthy if
       * the container is either in QUASI_CLOSED or in CLOSED state and has
       * exact number of replicas in the same state.
       */
      if (isContainerHealthy(container, replicas)) {
        return;
      }

      /*
       * Check if the container is under replicated and take appropriate
       * action.
       */
      if (isContainerUnderReplicated(container, replicas)) {
        handleUnderReplicatedContainer(container, replicas);
        return;
      }

      /*
       * Check if the container is over replicated and take appropriate
       * action.
       */
      if (isContainerOverReplicated(container, replicas)) {
        handleOverReplicatedContainer(container, replicas);
        return;
      }

      /*
       * The container is neither under nor over replicated and the container
       * is not healthy. This means that the container has unhealthy/corrupted
       * replica.
       */
      handleUnstableContainer(container, replicas);

    } catch (ContainerNotFoundException ex) {
      LOG.warn("Missing container {}.", id);
    } finally {
      lockManager.unlock(id);
    }
  }

  /**
   * Reconciles the InflightActions for a given container.
   *
   * @param container Container to update
   * @param inflightActions inflightReplication (or) inflightDeletion
   * @param filter filter to check if the operation is completed
   */
  private void updateInflightAction(final ContainerInfo container,
      final Map<ContainerID, List<InflightAction>> inflightActions,
      final Predicate<InflightAction> filter) {
    final ContainerID id = container.containerID();
    final long deadline = Time.monotonicNow() - eventTimeout;
    if (inflightActions.containsKey(id)) {
      final List<InflightAction> actions = inflightActions.get(id);
      actions.removeIf(action -> action.time < deadline);
      actions.removeIf(filter);
      if (actions.isEmpty()) {
        inflightActions.remove(id);
      }
    }
  }

  /**
   * Returns true if the container is healthy according to ReplicationMonitor.
   *
   * According to ReplicationMonitor container is considered healthy if
   * it has exact number of replicas in the same state as the container.
   *
   * @param container Container to check
   * @param replicas Set of ContainerReplicas
   * @return true if the container is healthy, false otherwise
   */
  private boolean isContainerHealthy(final ContainerInfo container,
                                     final Set<ContainerReplica> replicas) {
    return container.getReplicationFactor().getNumber() == replicas.size() &&
        replicas.stream().allMatch(
            r -> compareState(container.getState(), r.getState()));
  }

  /**
   * Checks if the container is under replicated or not.
   *
   * @param container Container to check
   * @param replicas Set of ContainerReplicas
   * @return true if the container is under replicated, false otherwise
   */
  private boolean isContainerUnderReplicated(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    return container.getReplicationFactor().getNumber() >
        getReplicaCount(container.containerID(), replicas);
  }

  /**
   * Checks if the container is over replicated or not.
   *
   * @param container Container to check
   * @param replicas Set of ContainerReplicas
   * @return true if the container if over replicated, false otherwise
   */
  private boolean isContainerOverReplicated(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    return container.getReplicationFactor().getNumber() <
        getReplicaCount(container.containerID(), replicas);
  }

  /**
   * Returns the replication count of the given container. This also
   * considers inflight replication and deletion.
   *
   * @param id ContainerID
   * @param replicas Set of existing replicas
   * @return number of estimated replicas for this container
   */
  private int getReplicaCount(final ContainerID id,
                              final Set<ContainerReplica> replicas) {
    return replicas.size()
        + inflightReplication.getOrDefault(id, Collections.emptyList()).size()
        - inflightDeletion.getOrDefault(id, Collections.emptyList()).size();
  }

  /**
   * Returns true if more than 50% of the container replicas with unique
   * originNodeId are in QUASI_CLOSED state.
   *
   * @param container Container to check
   * @param replicas Set of ContainerReplicas
   * @return true if we can force close the container, false otherwise
   */
  private boolean canForceCloseContainer(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    Preconditions.assertTrue(container.getState() ==
        LifeCycleState.QUASI_CLOSED);
    final int replicationFactor = container.getReplicationFactor().getNumber();
    final long uniqueQuasiClosedReplicaCount = replicas.stream()
        .filter(r -> r.getState() == State.QUASI_CLOSED)
        .map(ContainerReplica::getOriginDatanodeId)
        .distinct()
        .count();
    return uniqueQuasiClosedReplicaCount > (replicationFactor / 2);
  }

  /**
   * Force close the container replica(s) with highest sequence Id.
   *
   * <p>
   *   Note: We should force close the container only if >50% (quorum)
   *   of replicas with unique originNodeId are in QUASI_CLOSED state.
   * </p>
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void forceCloseContainer(final ContainerInfo container,
                                   final Set<ContainerReplica> replicas) {
    Preconditions.assertTrue(container.getState() ==
        LifeCycleState.QUASI_CLOSED);

    final List<ContainerReplica> quasiClosedReplicas = replicas.stream()
        .filter(r -> r.getState() == State.QUASI_CLOSED)
        .collect(Collectors.toList());

    final Long sequenceId = quasiClosedReplicas.stream()
        .map(ContainerReplica::getSequenceId)
        .max(Long::compare)
        .orElse(-1L);

    LOG.info("Force closing container {} with BCSID {}," +
        " which is in QUASI_CLOSED state.",
        container.containerID(), sequenceId);

    quasiClosedReplicas.stream()
        .filter(r -> sequenceId != -1L)
        .filter(replica -> replica.getSequenceId().equals(sequenceId))
        .forEach(replica -> sendCloseCommand(
            container, replica.getDatanodeDetails(), true));
  }

  /**
   * If the given container is under replicated, identify a new set of
   * datanode(s) to replicate the container using ContainerPlacementPolicy
   * and send replicate container command to the identified datanode(s).
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void handleUnderReplicatedContainer(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    try {
      final ContainerID id = container.containerID();
      final List<DatanodeDetails> deletionInFlight = inflightDeletion
          .getOrDefault(id, Collections.emptyList())
          .stream()
          .map(action -> action.datanode)
          .collect(Collectors.toList());
      final List<DatanodeDetails> source = replicas.stream()
          .filter(r ->
              r.getState() == State.QUASI_CLOSED ||
              r.getState() == State.CLOSED)
          .filter(r -> !deletionInFlight.contains(r.getDatanodeDetails()))
          .sorted((r1, r2) -> r2.getSequenceId().compareTo(r1.getSequenceId()))
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());
      if (source.size() > 0) {
        final int replicationFactor = container
            .getReplicationFactor().getNumber();
        final int delta = replicationFactor - getReplicaCount(id, replicas);
        final List<DatanodeDetails> selectedDatanodes = containerPlacement
            .chooseDatanodes(source, delta, container.getUsedBytes());

        LOG.info("Container {} is under replicated. Expected replica count" +
                " is {}, but found {}.", id, replicationFactor,
            replicationFactor - delta);

        for (DatanodeDetails datanode : selectedDatanodes) {
          sendReplicateCommand(container, datanode, source);
        }
      } else {
        LOG.warn("Cannot replicate container {}, no healthy replica found.",
            container.containerID());
      }
    } catch (IOException ex) {
      LOG.warn("Exception while replicating container {}.",
          container.getContainerID(), ex);
    }
  }

  /**
   * If the given container is over replicated, identify the datanode(s)
   * to delete the container and send delete container command to the
   * identified datanode(s).
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void handleOverReplicatedContainer(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {

    final ContainerID id = container.containerID();
    final int replicationFactor = container.getReplicationFactor().getNumber();
    // Dont consider inflight replication while calculating excess here.
    final int excess = replicas.size() - replicationFactor -
        inflightDeletion.getOrDefault(id, Collections.emptyList()).size();

    if (excess > 0) {

      LOG.info("Container {} is over replicated. Expected replica count" +
              " is {}, but found {}.", id, replicationFactor,
          replicationFactor + excess);

      final Map<UUID, ContainerReplica> uniqueReplicas =
          new LinkedHashMap<>();

      replicas.stream()
          .filter(r -> compareState(container.getState(), r.getState()))
          .forEach(r -> uniqueReplicas
              .putIfAbsent(r.getOriginDatanodeId(), r));

      // Retain one healthy replica per origin node Id.
      final List<ContainerReplica> eligibleReplicas = new ArrayList<>(replicas);
      eligibleReplicas.removeAll(uniqueReplicas.values());

      final List<ContainerReplica> unhealthyReplicas = eligibleReplicas
          .stream()
          .filter(r -> !compareState(container.getState(), r.getState()))
          .collect(Collectors.toList());

      //Move the unhealthy replicas to the front of eligible replicas to delete
      eligibleReplicas.removeAll(unhealthyReplicas);
      eligibleReplicas.addAll(0, unhealthyReplicas);

      for (int i = 0; i < excess; i++) {
        sendDeleteCommand(container,
            eligibleReplicas.get(i).getDatanodeDetails(), true);
      }
    }
  }

  /**
   * Handles unstable container.
   * A container is inconsistent if any of the replica state doesn't
   * match the container state. We have to take appropriate action
   * based on state of the replica.
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void handleUnstableContainer(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    // Find unhealthy replicas
    List<ContainerReplica> unhealthyReplicas = replicas.stream()
        .filter(r -> !compareState(container.getState(), r.getState()))
        .collect(Collectors.toList());

    Iterator<ContainerReplica> iterator = unhealthyReplicas.iterator();
    while (iterator.hasNext()) {
      final ContainerReplica replica = iterator.next();
      final State state = replica.getState();
      if (state == State.OPEN || state == State.CLOSING) {
        sendCloseCommand(container, replica.getDatanodeDetails(), false);
        iterator.remove();
      }

      if (state == State.QUASI_CLOSED) {
        // Send force close command if the BCSID matches
        if (container.getSequenceId() == replica.getSequenceId()) {
          sendCloseCommand(container, replica.getDatanodeDetails(), true);
          iterator.remove();
        }
      }
    }

    // Now we are left with the replicas which are either unhealthy or
    // the BCSID doesn't match. These replicas should be deleted.

    /*
     * If we have unhealthy replicas we go under replicated and then
     * replicate the healthy copy.
     *
     * We also make sure that we delete only one unhealthy replica at a time.
     *
     * If there are two unhealthy replica:
     *  - Delete first unhealthy replica
     *  - Re-replicate the healthy copy
     *  - Delete second unhealthy replica
     *  - Re-replicate the healthy copy
     *
     * Note: Only one action will be executed in a single ReplicationMonitor
     *       iteration. So to complete all the above actions we need four
     *       ReplicationMonitor iterations.
     */

    unhealthyReplicas.stream().findFirst().ifPresent(replica ->
        sendDeleteCommand(container, replica.getDatanodeDetails(), false));

  }

  /**
   * Sends close container command for the given container to the given
   * datanode.
   *
   * @param container Container to be closed
   * @param datanode The datanode on which the container
   *                  has to be closed
   * @param force Should be set to true if we want to close a
   *               QUASI_CLOSED container
   */
  private void sendCloseCommand(final ContainerInfo container,
                                final DatanodeDetails datanode,
                                final boolean force) {

    LOG.info("Sending close container command for container {}" +
            " to datanode {}.", container.containerID(), datanode);

    CloseContainerCommand closeContainerCommand =
        new CloseContainerCommand(container.getContainerID(),
            container.getPipelineID(), force);
    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
        new CommandForDatanode<>(datanode.getUuid(), closeContainerCommand));
  }

  /**
   * Sends replicate container command for the given container to the given
   * datanode.
   *
   * @param container Container to be replicated
   * @param datanode The destination datanode to replicate
   * @param sources List of source nodes from where we can replicate
   */
  private void sendReplicateCommand(final ContainerInfo container,
                                    final DatanodeDetails datanode,
                                    final List<DatanodeDetails> sources) {

    LOG.info("Sending replicate container command for container {}" +
            " to datanode {}", container.containerID(), datanode);

    final ContainerID id = container.containerID();
    final ReplicateContainerCommand replicateCommand =
        new ReplicateContainerCommand(id.getId(), sources);
    inflightReplication.computeIfAbsent(id, k -> new ArrayList<>());
    sendAndTrackDatanodeCommand(datanode, replicateCommand,
        action -> inflightReplication.get(id).add(action));
  }

  /**
   * Sends delete container command for the given container to the given
   * datanode.
   *
   * @param container Container to be deleted
   * @param datanode The datanode on which the replica should be deleted
   * @param force Should be set to true to delete an OPEN replica
   */
  private void sendDeleteCommand(final ContainerInfo container,
                                 final DatanodeDetails datanode,
                                 final boolean force) {

    LOG.info("Sending delete container command for container {}" +
            " to datanode {}", container.containerID(), datanode);

    final ContainerID id = container.containerID();
    final DeleteContainerCommand deleteCommand =
        new DeleteContainerCommand(id.getId(), force);
    inflightDeletion.computeIfAbsent(id, k -> new ArrayList<>());
    sendAndTrackDatanodeCommand(datanode, deleteCommand,
        action -> inflightDeletion.get(id).add(action));
  }

  /**
   * Creates CommandForDatanode with the given SCMCommand and fires
   * DATANODE_COMMAND event to event queue.
   *
   * Tracks the command using the given tracker.
   *
   * @param datanode Datanode to which the command has to be sent
   * @param command SCMCommand to be sent
   * @param tracker Tracker which tracks the inflight actions
   * @param <T> Type of SCMCommand
   */
  private <T extends GeneratedMessage> void sendAndTrackDatanodeCommand(
      final DatanodeDetails datanode,
      final SCMCommand<T> command,
      final Consumer<InflightAction> tracker) {
    final CommandForDatanode<T> datanodeCommand =
        new CommandForDatanode<>(datanode.getUuid(), command);
    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND, datanodeCommand);
    tracker.accept(new InflightAction(datanode, Time.monotonicNow()));
  }

  /**
   * Compares the container state with the replica state.
   *
   * @param containerState ContainerState
   * @param replicaState ReplicaState
   * @return true if the state matches, false otherwise
   */
  private static boolean compareState(final LifeCycleState containerState,
                                      final State replicaState) {
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

  /**
   * Wrapper class to hold the InflightAction with its start time.
   */
  private static final class InflightAction {

    private final DatanodeDetails datanode;
    private final long time;

    private InflightAction(final DatanodeDetails datanode,
                           final long time) {
      this.datanode = datanode;
      this.time = time;
    }
  }
}