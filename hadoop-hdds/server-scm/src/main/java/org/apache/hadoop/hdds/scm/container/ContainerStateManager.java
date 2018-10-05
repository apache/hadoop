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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationRequest;
import org.apache.hadoop.hdds.scm.container.states.ContainerState;
import org.apache.hadoop.hdds.scm.container.states.ContainerStateMap;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipelines.PipelineSelector;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.ozone.common.statemachine
    .InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;
import org.apache.hadoop.util.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes
    .FAILED_TO_CHANGE_CONTAINER_STATE;

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
public class ContainerStateManager implements Closeable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerStateManager.class);

  private final StateMachine<HddsProtos.LifeCycleState,
      HddsProtos.LifeCycleEvent> stateMachine;

  private final long containerSize;
  private final ConcurrentHashMap<ContainerState, ContainerID> lastUsedMap;
  private final ContainerStateMap containers;
  private final AtomicLong containerCount;

  /**
   * Constructs a Container State Manager that tracks all containers owned by
   * SCM for the purpose of allocation of blocks.
   * <p>
   * TODO : Add Container Tags so we know which containers are owned by SCM.
   */
  @SuppressWarnings("unchecked")
  public ContainerStateManager(Configuration configuration,
      ContainerManager containerManager, PipelineSelector pipelineSelector) {

    // Initialize the container state machine.
    Set<HddsProtos.LifeCycleState> finalStates = new HashSet();

    // These are the steady states of a container.
    finalStates.add(LifeCycleState.OPEN);
    finalStates.add(LifeCycleState.CLOSED);
    finalStates.add(LifeCycleState.DELETED);

    this.stateMachine = new StateMachine<>(LifeCycleState.ALLOCATED,
        finalStates);
    initializeStateMachine();

    this.containerSize = (long) configuration.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES);

    lastUsedMap = new ConcurrentHashMap<>();
    containerCount = new AtomicLong(0);
    containers = new ContainerStateMap();
  }

  /**
   * Return the info of all the containers kept by the in-memory mapping.
   *
   * @return the list of all container info.
   */
  public List<ContainerInfo> getAllContainers() {
    List<ContainerInfo> list = new ArrayList<>();

    //No Locking needed since the return value is an immutable map.
    containers.getContainerMap().forEach((key, value) -> list.add(value));
    return list;
  }

  /*
   *
   * Event and State Transition Mapping:
   *
   * State: ALLOCATED ---------------> CREATING
   * Event:                CREATE
   *
   * State: CREATING  ---------------> OPEN
   * Event:               CREATED
   *
   * State: OPEN      ---------------> CLOSING
   * Event:               FINALIZE
   *
   * State: CLOSING   ---------------> CLOSED
   * Event:                CLOSE
   *
   * State: CLOSED   ----------------> DELETING
   * Event:                DELETE
   *
   * State: DELETING ----------------> DELETED
   * Event:               CLEANUP
   *
   * State: CREATING  ---------------> DELETING
   * Event:               TIMEOUT
   *
   *
   * Container State Flow:
   *
   * [ALLOCATED]---->[CREATING]------>[OPEN]-------->[CLOSING]------->[CLOSED]
   *            (CREATE)     |    (CREATED)       (FINALIZE)     (CLOSE)    |
   *                         |                                              |
   *                         |                                              |
   *                         |(TIMEOUT)                             (DELETE)|
   *                         |                                              |
   *                         +-------------> [DELETING] <-------------------+
   *                                            |
   *                                            |
   *                                   (CLEANUP)|
   *                                            |
   *                                        [DELETED]
   */
  private void initializeStateMachine() {
    stateMachine.addTransition(LifeCycleState.ALLOCATED,
        LifeCycleState.CREATING,
        LifeCycleEvent.CREATE);

    stateMachine.addTransition(LifeCycleState.CREATING,
        LifeCycleState.OPEN,
        LifeCycleEvent.CREATED);

    stateMachine.addTransition(LifeCycleState.OPEN,
        LifeCycleState.CLOSING,
        LifeCycleEvent.FINALIZE);

    stateMachine.addTransition(LifeCycleState.CLOSING,
        LifeCycleState.CLOSED,
        LifeCycleEvent.CLOSE);

    stateMachine.addTransition(LifeCycleState.CLOSED,
        LifeCycleState.DELETING,
        LifeCycleEvent.DELETE);

    stateMachine.addTransition(LifeCycleState.CREATING,
        LifeCycleState.DELETING,
        LifeCycleEvent.TIMEOUT);

    stateMachine.addTransition(LifeCycleState.DELETING,
        LifeCycleState.DELETED,
        LifeCycleEvent.CLEANUP);
  }

  public void addExistingContainer(ContainerInfo containerInfo)
      throws SCMException {
    containers.addContainer(containerInfo);
    long containerID = containerInfo.getContainerID();
    if (containerCount.get() < containerID) {
      containerCount.set(containerID);
    }
  }

  /**
   * allocates a new container based on the type, replication etc.
   *
   * @param selector -- Pipeline selector class.
   * @param type -- Replication type.
   * @param replicationFactor - Replication replicationFactor.
   * @return ContainerWithPipeline
   * @throws IOException  on Failure.
   */
  public ContainerWithPipeline allocateContainer(PipelineSelector selector,
      HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor replicationFactor, String owner)
      throws IOException {

    Pipeline pipeline = selector.getReplicationPipeline(type,
        replicationFactor);

    Preconditions.checkNotNull(pipeline, "Pipeline type=%s/"
        + "replication=%s couldn't be found for the new container. "
        + "Do you have enough nodes?", type, replicationFactor);

    long containerID = containerCount.incrementAndGet();
    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setState(HddsProtos.LifeCycleState.ALLOCATED)
        .setPipelineID(pipeline.getId())
        // This is bytes allocated for blocks inside container, not the
        // container size
        .setAllocatedBytes(0)
        .setUsedBytes(0)
        .setNumberOfKeys(0)
        .setStateEnterTime(Time.monotonicNow())
        .setOwner(owner)
        .setContainerID(containerID)
        .setDeleteTransactionId(0)
        .setReplicationFactor(replicationFactor)
        .setReplicationType(pipeline.getType())
        .build();
    selector.addContainerToPipeline(pipeline.getId(), containerID);
    Preconditions.checkNotNull(containerInfo);
    containers.addContainer(containerInfo);
    LOG.trace("New container allocated: {}", containerInfo);
    return new ContainerWithPipeline(containerInfo, pipeline);
  }

  /**
   * Update the Container State to the next state.
   *
   * @param info - ContainerInfo
   * @param event - LifeCycle Event
   * @return Updated ContainerInfo.
   * @throws SCMException  on Failure.
   */
  public ContainerInfo updateContainerState(ContainerInfo
      info, HddsProtos.LifeCycleEvent event) throws SCMException {
    LifeCycleState newState;
    try {
      newState = this.stateMachine.getNextState(info.getState(), event);
    } catch (InvalidStateTransitionException ex) {
      String error = String.format("Failed to update container state %s, " +
              "reason: invalid state transition from state: %s upon " +
              "event: %s.",
          info.getContainerID(), info.getState(), event);
      LOG.error(error);
      throw new SCMException(error, FAILED_TO_CHANGE_CONTAINER_STATE);
    }

    // This is a post condition after executing getNextState.
    Preconditions.checkNotNull(newState);
    containers.updateState(info, info.getState(), newState);
    return containers.getContainerInfo(info);
  }

  /**
   * Update the container State.
   * @param info - Container Info
   * @return  ContainerInfo
   * @throws SCMException - on Error.
   */
  public ContainerInfo updateContainerInfo(ContainerInfo info)
      throws SCMException {
    containers.updateContainerInfo(info);
    return containers.getContainerInfo(info);
  }

  /**
   * Update deleteTransactionId for a container.
   *
   * @param deleteTransactionMap maps containerId to its new
   *                             deleteTransactionID
   */
  public void updateDeleteTransactionId(Map<Long, Long> deleteTransactionMap) {
    for (Map.Entry<Long, Long> entry : deleteTransactionMap.entrySet()) {
      containers.getContainerMap().get(ContainerID.valueof(entry.getKey()))
          .updateDeleteTransactionId(entry.getValue());
    }
  }

  /**
   * Return a container matching the attributes specified.
   *
   * @param size - Space needed in the Container.
   * @param owner - Owner of the container - A specific nameservice.
   * @param type - Replication Type {StandAlone, Ratis}
   * @param factor - Replication Factor {ONE, THREE}
   * @param state - State of the Container-- {Open, Allocated etc.}
   * @return ContainerInfo, null if there is no match found.
   */
  public ContainerInfo getMatchingContainer(final long size,
      String owner, ReplicationType type, ReplicationFactor factor,
      LifeCycleState state) {

    // Find containers that match the query spec, if no match return null.
    NavigableSet<ContainerID> matchingSet =
        containers.getMatchingContainerIDs(state, owner, factor, type);
    if (matchingSet == null || matchingSet.size() == 0) {
      return null;
    }

    // Get the last used container and find container above the last used
    // container ID.
    ContainerState key = new ContainerState(owner, type, factor);
    ContainerID lastID = lastUsedMap.get(key);
    if (lastID == null) {
      lastID = matchingSet.first();
    }

    // There is a small issue here. The first time, we will skip the first
    // container. But in most cases it will not matter.
    NavigableSet<ContainerID> resultSet = matchingSet.tailSet(lastID, false);
    if (resultSet.size() == 0) {
      resultSet = matchingSet;
    }

    ContainerInfo selectedContainer =
        findContainerWithSpace(size, resultSet, owner);
    if (selectedContainer == null) {

      // If we did not find any space in the tailSet, we need to look for
      // space in the headset, we need to pass true to deal with the
      // situation that we have a lone container that has space. That is we
      // ignored the last used container under the assumption we can find
      // other containers with space, but if have a single container that is
      // not true. Hence we need to include the last used container as the
      // last element in the sorted set.

      resultSet = matchingSet.headSet(lastID, true);
      selectedContainer = findContainerWithSpace(size, resultSet, owner);
    }
    // Update the allocated Bytes on this container.
    if (selectedContainer != null) {
      selectedContainer.updateAllocatedBytes(size);
    }
    return selectedContainer;

  }

  private ContainerInfo findContainerWithSpace(long size,
      NavigableSet<ContainerID> searchSet, String owner) {
    // Get the container with space to meet our request.
    for (ContainerID id : searchSet) {
      ContainerInfo containerInfo = containers.getContainerInfo(id);
      if (containerInfo.getAllocatedBytes() + size <= this.containerSize) {
        containerInfo.updateLastUsedTime();

        ContainerState key = new ContainerState(owner,
            containerInfo.getReplicationType(),
            containerInfo.getReplicationFactor());
        lastUsedMap.put(key, containerInfo.containerID());
        return containerInfo;
      }
    }
    return null;
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
  public NavigableSet<ContainerID> getMatchingContainerIDs(
      String owner, ReplicationType type, ReplicationFactor factor,
      LifeCycleState state) {
    return containers.getMatchingContainerIDs(state, owner,
        factor, type);
  }

  /**
   * Returns the containerInfo with pipeline for the given container id.
   * @param selector -- Pipeline selector class.
   * @param containerID id of the container
   * @return ContainerInfo containerInfo
   * @throws IOException
   */
  public ContainerWithPipeline getContainer(PipelineSelector selector,
      ContainerID containerID) {
    ContainerInfo info = containers.getContainerInfo(containerID.getId());
    Pipeline pipeline = selector.getPipeline(info.getPipelineID());
    return new ContainerWithPipeline(info, pipeline);
  }

  /**
   * Returns the containerInfo for the given container id.
   * @param containerID id of the container
   * @return ContainerInfo containerInfo
   * @throws IOException
   */
  public ContainerInfo getContainer(ContainerID containerID) {
    return containers.getContainerInfo(containerID);
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * Returns the latest list of DataNodes where replica for given containerId
   * exist. Throws an SCMException if no entry is found for given containerId.
   *
   * @param containerID
   * @return Set<DatanodeDetails>
   */
  public Set<DatanodeDetails> getContainerReplicas(ContainerID containerID)
      throws SCMException {
    return containers.getContainerReplicas(containerID);
  }

  /**
   * Add a container Replica for given DataNode.
   *
   * @param containerID
   * @param dn
   */
  public void addContainerReplica(ContainerID containerID, DatanodeDetails dn) {
    containers.addContainerReplica(containerID, dn);
  }

  /**
   * Remove a container Replica for given DataNode.
   *
   * @param containerID
   * @param dn
   * @return True of dataNode is removed successfully else false.
   */
  public boolean removeContainerReplica(ContainerID containerID,
      DatanodeDetails dn) throws SCMException {
    return containers.removeContainerReplica(containerID, dn);
  }

  /**
   * Compare the existing replication number with the expected one.
   */
  public ReplicationRequest checkReplicationState(ContainerID containerID)
      throws SCMException {
    int existingReplicas = getContainerReplicas(containerID).size();
    int expectedReplicas = getContainer(containerID)
        .getReplicationFactor().getNumber();
    if (existingReplicas != expectedReplicas) {
      return new ReplicationRequest(containerID.getId(), existingReplicas,
          expectedReplicas);
    }
    return null;
  }

  /**
   * Checks if the container is open.
   */
  public boolean isOpen(ContainerID containerID) {
    Preconditions.checkNotNull(containerID);
    ContainerInfo container = Preconditions
        .checkNotNull(getContainer(containerID),
            "Container can't be found " + containerID);
    return container.isContainerOpen();
  }

  @VisibleForTesting
  public ContainerStateMap getContainerStateMap() {
    return containers;
  }

}
