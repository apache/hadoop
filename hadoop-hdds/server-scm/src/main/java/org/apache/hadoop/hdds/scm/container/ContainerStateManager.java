/*
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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes
    .FAILED_TO_CHANGE_CONTAINER_STATE;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.states.ContainerState;
import org.apache.hadoop.hdds.scm.container.states.ContainerStateMap;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.ozone.common.statemachine
    .InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicLongMap;

/**
 * A container state manager keeps track of container states and returns
 * containers that match various queries.
 * <p>
 * This state machine is driven by a combination of server and client actions.
 * <p>
 * This is how a create container happens: 1. When a container is created, the
 * Server(or SCM) marks that Container as ALLOCATED state. In this state, SCM
 * has chosen a pipeline for container to live on. However, the container is not
 * created yet. This container along with the pipeline is returned to the
 * client.
 * <p>
 * 2. The client when it sees the Container state as ALLOCATED understands that
 * container needs to be created on the specified pipeline. The client lets the
 * SCM know that saw this flag and is initiating the on the data nodes.
 * <p>
 * This is done by calling into notifyObjectCreation(ContainerName,
 * BEGIN_CREATE) flag. When SCM gets this call, SCM puts the container state
 * into CREATING. All this state means is that SCM told Client to create a
 * container and client saw that request.
 * <p>
 * 3. Then client makes calls to datanodes directly, asking the datanodes to
 * create the container. This is done with the help of pipeline that supports
 * this container.
 * <p>
 * 4. Once the creation of the container is complete, the client will make
 * another call to the SCM, this time specifying the containerName and the
 * COMPLETE_CREATE as the Event.
 * <p>
 * 5. With COMPLETE_CREATE event, the container moves to an Open State. This is
 * the state when clients can write to a container.
 * <p>
 * 6. If the client does not respond with the COMPLETE_CREATE event with a
 * certain time, the state machine times out and triggers a delete operation of
 * the container.
 * <p>
 * Please see the function initializeStateMachine below to see how this looks in
 * code.
 * <p>
 * Reusing existing container :
 * <p>
 * The create container call is not made all the time, the system tries to use
 * open containers as much as possible. So in those cases, it looks thru the
 * list of open containers and will return containers that match the specific
 * signature.
 * <p>
 * Please note : Logically there are 3 separate state machines in the case of
 * containers.
 * <p>
 * The Create State Machine -- Commented extensively above.
 * <p>
 * Open/Close State Machine - Once the container is in the Open State,
 * eventually it will be closed, once sufficient data has been written to it.
 * <p>
 * TimeOut Delete Container State Machine - if the container creating times out,
 * then Container State manager decides to delete the container.
 */
public class ContainerStateManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerStateManager.class);

  private final StateMachine<HddsProtos.LifeCycleState,
      HddsProtos.LifeCycleEvent> stateMachine;

  private final long containerSize;
  private final ConcurrentHashMap<ContainerState, ContainerID> lastUsedMap;
  private final ContainerStateMap containers;
  private final AtomicLong containerCount;
  private final AtomicLongMap<LifeCycleState> containerStateCount =
      AtomicLongMap.create();

  /**
   * Constructs a Container State Manager that tracks all containers owned by
   * SCM for the purpose of allocation of blocks.
   * <p>
   * TODO : Add Container Tags so we know which containers are owned by SCM.
   */
  @SuppressWarnings("unchecked")
  public ContainerStateManager(final Configuration configuration) {

    // Initialize the container state machine.
    final Set<HddsProtos.LifeCycleState> finalStates = new HashSet();

    // These are the steady states of a container.
    finalStates.add(LifeCycleState.OPEN);
    finalStates.add(LifeCycleState.CLOSED);
    finalStates.add(LifeCycleState.DELETED);

    this.stateMachine = new StateMachine<>(LifeCycleState.OPEN,
        finalStates);
    initializeStateMachine();

    this.containerSize = (long) configuration.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES);

    this.lastUsedMap = new ConcurrentHashMap<>();
    this.containerCount = new AtomicLong(0);
    this.containers = new ContainerStateMap();
  }

  /*
   *
   * Event and State Transition Mapping:
   *
   * State: OPEN         ----------------> CLOSING
   * Event:                    FINALIZE
   *
   * State: CLOSING      ----------------> QUASI_CLOSED
   * Event:                  QUASI_CLOSE
   *
   * State: CLOSING      ----------------> CLOSED
   * Event:                     CLOSE
   *
   * State: QUASI_CLOSED ----------------> CLOSED
   * Event:                  FORCE_CLOSE
   *
   * State: CLOSED       ----------------> DELETING
   * Event:                    DELETE
   *
   * State: DELETING     ----------------> DELETED
   * Event:                    CLEANUP
   *
   *
   * Container State Flow:
   *
   * [OPEN]--------------->[CLOSING]--------------->[QUASI_CLOSED]
   *          (FINALIZE)      |      (QUASI_CLOSE)        |
   *                          |                           |
   *                          |                           |
   *                  (CLOSE) |             (FORCE_CLOSE) |
   *                          |                           |
   *                          |                           |
   *                          +--------->[CLOSED]<--------+
   *                                        |
   *                                (DELETE)|
   *                                        |
   *                                        |
   *                                   [DELETING]
   *                                        |
   *                              (CLEANUP) |
   *                                        |
   *                                        V
   *                                    [DELETED]
   *
   */
  private void initializeStateMachine() {
    stateMachine.addTransition(LifeCycleState.OPEN,
        LifeCycleState.CLOSING,
        LifeCycleEvent.FINALIZE);

    stateMachine.addTransition(LifeCycleState.CLOSING,
        LifeCycleState.QUASI_CLOSED,
        LifeCycleEvent.QUASI_CLOSE);

    stateMachine.addTransition(LifeCycleState.CLOSING,
        LifeCycleState.CLOSED,
        LifeCycleEvent.CLOSE);

    stateMachine.addTransition(LifeCycleState.QUASI_CLOSED,
        LifeCycleState.CLOSED,
        LifeCycleEvent.FORCE_CLOSE);

    stateMachine.addTransition(LifeCycleState.CLOSED,
        LifeCycleState.DELETING,
        LifeCycleEvent.DELETE);

    stateMachine.addTransition(LifeCycleState.DELETING,
        LifeCycleState.DELETED,
        LifeCycleEvent.CLEANUP);
  }


  void loadContainer(final ContainerInfo containerInfo) throws SCMException {
    containers.addContainer(containerInfo);
    containerCount.set(Long.max(
        containerInfo.getContainerID(), containerCount.get()));
    containerStateCount.incrementAndGet(containerInfo.getState());
  }

  /**
   * Allocates a new container based on the type, replication etc.
   *
   * @param pipelineManager -- Pipeline Manager class.
   * @param type -- Replication type.
   * @param replicationFactor - Replication replicationFactor.
   * @return ContainerWithPipeline
   * @throws IOException  on Failure.
   */
  ContainerInfo allocateContainer(final PipelineManager pipelineManager,
      final HddsProtos.ReplicationType type,
      final HddsProtos.ReplicationFactor replicationFactor, final String owner)
      throws IOException {

    Pipeline pipeline;
    try {
      // TODO: #CLUTIL remove creation logic when all replication types and
      // factors are handled by pipeline creator job.
      pipeline = pipelineManager.createPipeline(type, replicationFactor);
    } catch (IOException e) {
      final List<Pipeline> pipelines = pipelineManager
          .getPipelines(type, replicationFactor, Pipeline.PipelineState.OPEN);
      if (pipelines.isEmpty()) {
        throw new IOException("Could not allocate container. Cannot get any" +
            " matching pipeline for Type:" + type +
            ", Factor:" + replicationFactor + ", State:PipelineState.OPEN");
      }
      pipeline = pipelines.get((int) containerCount.get() % pipelines.size());
    }
    return allocateContainer(pipelineManager, owner, pipeline);
  }

  /**
   * Allocates a new container based on the type, replication etc.
   *
   * @param pipelineManager   - Pipeline Manager class.
   * @param owner             - Owner of the container.
   * @param pipeline          - Pipeline to which the container needs to be
   *                          allocated.
   * @return ContainerWithPipeline
   * @throws IOException on Failure.
   */
  ContainerInfo allocateContainer(
      final PipelineManager pipelineManager, final String owner,
      Pipeline pipeline) throws IOException {
    Preconditions.checkNotNull(pipeline,
        "Pipeline couldn't be found for the new container. "
            + "Do you have enough nodes?");

    final long containerID = containerCount.incrementAndGet();
    final ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setState(LifeCycleState.OPEN)
        .setPipelineID(pipeline.getId())
        .setUsedBytes(0)
        .setNumberOfKeys(0)
        .setStateEnterTime(Time.monotonicNow())
        .setOwner(owner)
        .setContainerID(containerID)
        .setDeleteTransactionId(0)
        .setReplicationFactor(pipeline.getFactor())
        .setReplicationType(pipeline.getType())
        .build();
    pipelineManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueof(containerID));
    Preconditions.checkNotNull(containerInfo);
    containers.addContainer(containerInfo);
    containerStateCount.incrementAndGet(containerInfo.getState());
    LOG.trace("New container allocated: {}", containerInfo);
    return containerInfo;
  }

  /**
   * Update the Container State to the next state.
   *
   * @param containerID - ContainerID
   * @param event - LifeCycle Event
   * @throws SCMException  on Failure.
   */
  void updateContainerState(final ContainerID containerID,
      final HddsProtos.LifeCycleEvent event)
      throws SCMException, ContainerNotFoundException {
    final ContainerInfo info = containers.getContainerInfo(containerID);
    try {
      final LifeCycleState oldState = info.getState();
      final LifeCycleState newState = stateMachine.getNextState(
          info.getState(), event);
      containers.updateState(containerID, info.getState(), newState);
      containerStateCount.incrementAndGet(newState);
      containerStateCount.decrementAndGet(oldState);
    } catch (InvalidStateTransitionException ex) {
      String error = String.format("Failed to update container state %s, " +
              "reason: invalid state transition from state: %s upon " +
              "event: %s.",
          containerID, info.getState(), event);
      LOG.error(error);
      throw new SCMException(error, FAILED_TO_CHANGE_CONTAINER_STATE);
    }
  }

  /**
   * Update deleteTransactionId for a container.
   *
   * @param deleteTransactionMap maps containerId to its new
   *                             deleteTransactionID
   */
  void updateDeleteTransactionId(
      final Map<Long, Long> deleteTransactionMap) {
    deleteTransactionMap.forEach((k, v) -> {
      try {
        containers.getContainerInfo(ContainerID.valueof(k))
            .updateDeleteTransactionId(v);
      } catch (ContainerNotFoundException e) {
        LOG.warn("Exception while updating delete transaction id.", e);
      }
    });
  }


  /**
   * Return a container matching the attributes specified.
   *
   * @param size         - Space needed in the Container.
   * @param owner        - Owner of the container - A specific nameservice.
   * @param pipelineID   - ID of the pipeline
   * @param containerIDs - Set of containerIDs to choose from
   * @return ContainerInfo, null if there is no match found.
   */
  ContainerInfo getMatchingContainer(final long size, String owner,
      PipelineID pipelineID, NavigableSet<ContainerID> containerIDs) {
    if (containerIDs.isEmpty()) {
      return null;
    }

    // Get the last used container and find container above the last used
    // container ID.
    final ContainerState key = new ContainerState(owner, pipelineID);
    final ContainerID lastID =
        lastUsedMap.getOrDefault(key, containerIDs.first());

    // There is a small issue here. The first time, we will skip the first
    // container. But in most cases it will not matter.
    NavigableSet<ContainerID> resultSet = containerIDs.tailSet(lastID, false);
    if (resultSet.size() == 0) {
      resultSet = containerIDs;
    }

    ContainerInfo selectedContainer =
        findContainerWithSpace(size, resultSet, owner, pipelineID);
    if (selectedContainer == null) {

      // If we did not find any space in the tailSet, we need to look for
      // space in the headset, we need to pass true to deal with the
      // situation that we have a lone container that has space. That is we
      // ignored the last used container under the assumption we can find
      // other containers with space, but if have a single container that is
      // not true. Hence we need to include the last used container as the
      // last element in the sorted set.

      resultSet = containerIDs.headSet(lastID, true);
      selectedContainer =
          findContainerWithSpace(size, resultSet, owner, pipelineID);
    }

    return selectedContainer;
  }

  private ContainerInfo findContainerWithSpace(final long size,
      final NavigableSet<ContainerID> searchSet, final String owner,
      final PipelineID pipelineID) {
    try {
      // Get the container with space to meet our request.
      for (ContainerID id : searchSet) {
        final ContainerInfo containerInfo = containers.getContainerInfo(id);
        if (containerInfo.getUsedBytes() + size <= this.containerSize) {
          containerInfo.updateLastUsedTime();
          return containerInfo;
        }
      }
    } catch (ContainerNotFoundException e) {
      // This should not happen!
      LOG.warn("Exception while finding container with space", e);
    }
    return null;
  }

  Set<ContainerID> getAllContainerIDs() {
    return containers.getAllContainerIDs();
  }

  /**
   * Returns Containers by State.
   *
   * @param state - State - Open, Closed etc.
   * @return List of containers by state.
   */
  Set<ContainerID> getContainerIDsByState(final LifeCycleState state) {
    return containers.getContainerIDsByState(state);
  }

  /**
   * Get count of containers in the current {@link LifeCycleState}.
   *
   * @param state {@link LifeCycleState}
   * @return Count of containers
   */
  Integer getContainerCountByState(final LifeCycleState state) {
    return Long.valueOf(containerStateCount.get(state)).intValue();
  }

  /**
   * Returns a set of ContainerIDs that match the Container.
   *
   * @param owner  Owner of the Containers.
   * @param type - Replication Type of the containers
   * @param factor - Replication factor of the containers.
   * @param state - Current State, like Open, Close etc.
   * @return Set of containers that match the specific query parameters.
   */
  NavigableSet<ContainerID> getMatchingContainerIDs(final String owner,
      final ReplicationType type, final ReplicationFactor factor,
      final LifeCycleState state) {
    return containers.getMatchingContainerIDs(state, owner,
        factor, type);
  }

  /**
   * Returns the containerInfo for the given container id.
   * @param containerID id of the container
   * @return ContainerInfo containerInfo
   * @throws IOException
   */
  ContainerInfo getContainer(final ContainerID containerID)
      throws ContainerNotFoundException {
    return containers.getContainerInfo(containerID);
  }

  void close() throws IOException {
  }

  /**
   * Returns the latest list of DataNodes where replica for given containerId
   * exist. Throws an SCMException if no entry is found for given containerId.
   *
   * @param containerID
   * @return Set<DatanodeDetails>
   */
  Set<ContainerReplica> getContainerReplicas(
      final ContainerID containerID) throws ContainerNotFoundException {
    return containers.getContainerReplicas(containerID);
  }

  /**
   * Add a container Replica for given DataNode.
   *
   * @param containerID
   * @param replica
   */
  void updateContainerReplica(final ContainerID containerID,
      final ContainerReplica replica) throws ContainerNotFoundException {
    containers.updateContainerReplica(containerID, replica);
  }

  /**
   * Remove a container Replica for given DataNode.
   *
   * @param containerID
   * @param replica
   * @return True of dataNode is removed successfully else false.
   */
  void removeContainerReplica(final ContainerID containerID,
      final ContainerReplica replica)
      throws ContainerNotFoundException, ContainerReplicaNotFoundException {
    containers.removeContainerReplica(containerID, replica);
  }

  void removeContainer(final ContainerID containerID)
      throws ContainerNotFoundException {
    containers.removeContainer(containerID);
  }

  /**
   * Update the lastUsedmap to update with ContainerState and containerID.
   * @param pipelineID
   * @param containerID
   * @param owner
   */
  public synchronized void updateLastUsedMap(PipelineID pipelineID,
      ContainerID containerID, String owner) {
    lastUsedMap.put(new ContainerState(owner, pipelineID),
        containerID);
  }

}
