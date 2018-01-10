/**
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

package org.apache.hadoop.ozone.scm.container;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.scm.exceptions.SCMException;
import org.apache.hadoop.ozone.scm.pipelines.PipelineSelector;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.PriorityQueue;
import java.util.List;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.LifeCycleState;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationType;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.LifeCycleEvent;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationFactor;

import static org.apache.hadoop.ozone.scm.exceptions
    .SCMException.ResultCodes.FAILED_TO_CHANGE_CONTAINER_STATE;

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
 * another call to the SCM, this time specifing the containerName and the
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

  private final StateMachine<OzoneProtos.LifeCycleState,
      OzoneProtos.LifeCycleEvent> stateMachine;

  private final long containerSize;
  private final long cacheSize;
  private final long blockSize;

  // A map that maintains the ContainerKey to Containers of that type ordered
  // by last access time.
  private final ReadWriteLock lock;
  private final Queue<ContainerInfo> containerCloseQueue;
  private Map<ContainerKey, PriorityQueue<ContainerInfo>> containers;

  /**
   * Constructs a Container State Manager that tracks all containers owned by
   * SCM for the purpose of allocation of blocks.
   * <p>
   * TODO : Add Container Tags so we know which containers are owned by SCM.
   */
  public ContainerStateManager(Configuration configuration,
      Mapping containerMapping, final long cacheSize) throws IOException {
    this.cacheSize = cacheSize;

    // Initialize the container state machine.
    Set<OzoneProtos.LifeCycleState> finalStates = new HashSet();

    // These are the steady states of a container.
    finalStates.add(LifeCycleState.OPEN);
    finalStates.add(LifeCycleState.CLOSED);
    finalStates.add(LifeCycleState.DELETED);

    this.stateMachine = new StateMachine<>(LifeCycleState.ALLOCATED,
        finalStates);
    initializeStateMachine();

    this.containerSize = OzoneConsts.GB * configuration.getInt(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_GB,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT);

    this.blockSize = OzoneConsts.MB * configuration.getLong(
        OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_IN_MB,
        OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT);

    lock = new ReentrantReadWriteLock();
    containers = new HashMap<>();
    loadExistingContainers(containerMapping);
    containerCloseQueue = new ConcurrentLinkedQueue<>();
  }

  /**
   * Creates containers maps of following types.
   * <p>
   * OZONE  of type {Ratis, StandAlone, Chained} for each of these {ALLOCATED,
   * CREATING, OPEN, CLOSED, DELETING, DELETED}  container states
   * <p>
   * CBLOCK of type {Ratis, StandAlone, Chained} for each of these {ALLOCATED,
   * CREATING, OPEN, CLOSED, DELETING, DELETED}  container states
   * <p>
   * Commented out for now: HDFS of type {Ratis, StandAlone, Chained} for each
   * of these {ALLOCATED, CREATING, OPEN, CLOSED, DELETING, DELETED}  container
   * states
   */
  private void initializeContainerMaps(String owner) {
    // Called only from Ctor path, hence no lock is held.
    Preconditions.checkNotNull(containers);
    for (ReplicationType type : ReplicationType.values()) {
      for (ReplicationFactor factor : ReplicationFactor.values()) {
        for (LifeCycleState state : LifeCycleState.values()) {
          ContainerKey key = new ContainerKey(owner, type, factor, state);
          PriorityQueue<ContainerInfo> queue = new PriorityQueue<>();
          containers.put(key, queue);
        }
      }
    }
  }

  /**
   * Load containers from the container store into the containerMaps.
   *
   * @param containerMapping -- Mapping object containing container store.
   */
  private void loadExistingContainers(Mapping containerMapping) {
    try {
      List<String> ownerList = new ArrayList<>();
      List<ContainerInfo> containerList =
          containerMapping.listContainer(null, null, Integer.MAX_VALUE);
      for (ContainerInfo container : containerList) {
        String owner = container.getOwner();
        if (ownerList.isEmpty() || !ownerList.contains(owner)) {
          ownerList.add(owner);
          initializeContainerMaps(owner);
        }
        ContainerKey key =
            new ContainerKey(owner, container.getPipeline().getType(),
                container.getPipeline().getFactor(), container.getState());
        containers.get(key).add(container);
      }
    } catch (IOException e) {
      if (!e.getMessage().equals("No container exists in current db")) {
        LOG.info("Could not list the containers", e);
      }
    }
  }

  /**
   * Return the info of all the containers kept by the in-memory mapping.
   *
   * @return the list of all container info.
   */
  List<ContainerInfo> getAllContainers() {
    List<ContainerInfo> list = new ArrayList<>();
    containers.forEach((key, value) -> list.addAll(value));
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
   * [ALLOCATED]------->[CREATING]--------->[OPEN]---------->[CLOSING]------->[CLOSED]
   *            (CREATE)     |    (CREATED)       (FINALIZE)          (CLOSE)    |
   *                         |                                                   |
   *                         |                                                   |
   *                         |(TIMEOUT)                                  (DELETE)|
   *                         |                                                   |
   *                         +------------------> [DELETING] <-------------------+
   *                                                   |
   *                                                   |
   *                                          (CLEANUP)|
   *                                                   |
   *                                               [DELETED]
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

  /**
   * allocates a new container based on the type, replication etc.
   *
   * @param selector -- Pipeline selector class.
   * @param type -- Replication type.
   * @param replicationFactor - Replication replicationFactor.
   * @param containerName - Container Name.
   * @return Container Info.
   * @throws IOException
   */
  public ContainerInfo allocateContainer(PipelineSelector selector, OzoneProtos
      .ReplicationType type, OzoneProtos.ReplicationFactor replicationFactor,
      final String containerName, String owner) throws
      IOException {

    Pipeline pipeline = selector.getReplicationPipeline(type,
        replicationFactor, containerName);
    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setContainerName(containerName)
        .setState(OzoneProtos.LifeCycleState.ALLOCATED)
        .setPipeline(pipeline)
        // This is bytes allocated for blocks inside container, not the
        // container size
        .setAllocatedBytes(0)
        .setUsedBytes(0)
        .setNumberOfKeys(0)
        .setStateEnterTime(Time.monotonicNow())
        .setOwner(owner)
        .build();
    Preconditions.checkNotNull(containerInfo);
    lock.writeLock().lock();
    try {
      ContainerKey key = new ContainerKey(owner, type, replicationFactor,
          containerInfo.getState());
      PriorityQueue<ContainerInfo> queue = containers.get(key);
      if (queue == null) {
        initializeContainerMaps(owner);
        queue = containers.get(key);
      }
      queue.add(containerInfo);
      LOG.trace("New container allocated: {}", containerInfo);
    } finally {
      lock.writeLock().unlock();
    }
    return containerInfo;
  }

  /**
   * Update the Container State to the next state.
   *
   * @param info - ContainerInfo
   * @param event - LifeCycle Event
   * @return Updated ContainerInfo.
   * @throws SCMException
   */
  public ContainerInfo updateContainerState(ContainerInfo
      info, OzoneProtos.LifeCycleEvent event) throws SCMException {
    LifeCycleState newState;
    try {
      newState = this.stateMachine.getNextState(info.getState(), event);
    } catch (InvalidStateTransitionException ex) {
      String error = String.format("Failed to update container state %s, " +
              "reason: invalid state transition from state: %s upon event: %s.",
          info.getPipeline().getContainerName(), info.getState(), event);
      LOG.error(error);
      throw new SCMException(error, FAILED_TO_CHANGE_CONTAINER_STATE);
    }

    // This is a post condition after executing getNextState.
    Preconditions.checkNotNull(newState);
    Pipeline pipeline = info.getPipeline();

    ContainerKey oldKey = new ContainerKey(info.getOwner(), pipeline.getType(),
        pipeline.getFactor(), info.getState());

    ContainerKey newKey = new ContainerKey(info.getOwner(), pipeline.getType(),
        pipeline.getFactor(), newState);
    lock.writeLock().lock();
    try {

      PriorityQueue<ContainerInfo> currentQueue = containers.get(oldKey);
      // This should never happen, since we have initialized the map and
      // queues to all possible states. No harm in asserting that info.
      Preconditions.checkNotNull(currentQueue);

      // TODO : Should we read this container info from the database if this
      // is missing in the queue?. Right now we just add it into the queue.
      // We also need a background thread that will remove unused containers
      // from memory after 24 hours.  This is really a low priority work item
      // since typical clusters will have less than 10's of millions of open
      // containers at a given time, which we can easily keep in memory.

      if (currentQueue.contains(info)) {
        currentQueue.remove(info);
      }

      PriorityQueue<ContainerInfo> nextQueue = containers.get(newKey);
      Preconditions.checkNotNull(nextQueue);

      ContainerInfo containerInfo = new ContainerInfo.Builder()
          .setContainerName(info.getContainerName())
          .setState(newState)
          .setPipeline(info.getPipeline())
          .setAllocatedBytes(info.getAllocatedBytes())
          .setUsedBytes(info.getUsedBytes())
          .setNumberOfKeys(info.getNumberOfKeys())
          .setStateEnterTime(Time.monotonicNow())
          .setOwner(info.getOwner())
          .build();
      Preconditions.checkNotNull(containerInfo);
      nextQueue.add(containerInfo);

      return containerInfo;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Return a container matching the attributes specified.
   *
   * @param size - Space needed in the Container.
   * @param owner - Owner of the container {OZONE, CBLOCK}
   * @param type - Replication Type {StandAlone, Ratis}
   * @param factor - Replication Factor {ONE, THREE}
   * @param state - State of the Container-- {Open, Allocated etc.}
   * @return ContainerInfo
   */
  public ContainerInfo getMatchingContainer(final long size,
      String owner, ReplicationType type, ReplicationFactor factor,
      LifeCycleState state) {
    ContainerKey key = new ContainerKey(owner, type, factor, state);
    lock.writeLock().lock();
    try {
      PriorityQueue<ContainerInfo> queue = containers.get(key);
      if (queue == null) {
        initializeContainerMaps(owner);
        queue = containers.get(key);
      }
      if (queue.size() == 0) {
        // We don't have any Containers of this type.
        return null;
      }
      Iterator<ContainerInfo> iter = queue.iterator();
      // Two assumptions here.
      // 1. The Iteration on the heap is in ordered by the last used time.
      // 2. We remove and add the node back to push the node to the end of
      // the queue.

      while (iter.hasNext()) {
        ContainerInfo info = iter.next();
        if (info.getAllocatedBytes() + size <= this.containerSize) {
          queue.remove(info);
          info.allocate(size);
          info.updateLastUsedTime();
          queue.add(info);

          return info;
        }
      }

    } finally {
      lock.writeLock().unlock();
    }
    return null;
  }

  @VisibleForTesting
  public List<ContainerInfo> getMatchingContainers(String owner,
      ReplicationType type, ReplicationFactor factor, LifeCycleState state) {
    ContainerKey key = new ContainerKey(owner, type, factor, state);
    lock.readLock().lock();
    try {
      if (containers.get(key) == null) {
        return null;
      } else {
        return Arrays.asList((ContainerInfo[]) containers.get(key)
            .toArray(new ContainerInfo[0]));
      }
    } catch (Exception e) {
      LOG.error("Could not get matching containers", e);
    } finally {
      lock.readLock().unlock();
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    //TODO: update container metadata db with actual allocated bytes values.
  }

  /**
   * Class that acts as the container Key.
   */
  private static class ContainerKey {
    private final LifeCycleState state;
    private final ReplicationType type;
    private final String owner;
    private final ReplicationFactor replicationFactor;

    /**
     * Constructs a Container Key.
     *
     * @param owner - Container Owners
     * @param type - Replication Type.
     * @param factor - Replication Factors
     * @param state - LifeCycle State
     */
    ContainerKey(String owner, ReplicationType type,
        ReplicationFactor factor, LifeCycleState state) {
      this.state = state;
      this.type = type;
      this.owner = owner;
      this.replicationFactor = factor;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ContainerKey that = (ContainerKey) o;

      return new EqualsBuilder()
          .append(state, that.state)
          .append(type, that.type)
          .append(owner, that.owner)
          .append(replicationFactor, that.replicationFactor)
          .isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder(137, 757)
          .append(state)
          .append(type)
          .append(owner)
          .append(replicationFactor)
          .toHashCode();
    }

    @Override
    public String toString() {
      return "ContainerKey{" +
          "state=" + state +
          ", type=" + type +
          ", owner=" + owner +
          ", replicationFactor=" + replicationFactor +
          '}';
    }
  }
}
