/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.container.states;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes
    .CONTAINER_EXISTS;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes
    .FAILED_TO_CHANGE_CONTAINER_STATE;

/**
 * Container State Map acts like a unified map for various attributes that are
 * used to select containers when we need allocated blocks.
 * <p>
 * This class provides the ability to query 5 classes of attributes. They are
 * <p>
 * 1. LifeCycleStates - LifeCycle States of container describe in which state
 * a container is. For example, a container needs to be in Open State for a
 * client to able to write to it.
 * <p>
 * 2. Owners - Each instance of Name service, for example, Namenode of HDFS or
 * Ozone Manager (OM) of Ozone or CBlockServer --  is an owner. It is
 * possible to have many OMs for a Ozone cluster and only one SCM. But SCM
 * keeps the data from each OM in separate bucket, never mixing them. To
 * write data, often we have to find all open containers for a specific owner.
 * <p>
 * 3. ReplicationType - The clients are allowed to specify what kind of
 * replication pipeline they want to use. Each Container exists on top of a
 * pipeline, so we need to get ReplicationType that is specified by the user.
 * <p>
 * 4. ReplicationFactor - The replication factor represents how many copies
 * of data should be made, right now we support 2 different types, ONE
 * Replica and THREE Replica. User can specify how many copies should be made
 * for a ozone key.
 * <p>
 * The most common access pattern of this class is to select a container based
 * on all these parameters, for example, when allocating a block we will
 * select a container that belongs to user1, with Ratis replication which can
 * make 3 copies of data. The fact that we will look for open containers by
 * default and if we cannot find them we will add new containers.
 */
public class ContainerStateMap {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerStateMap.class);

  private final static NavigableSet<ContainerID> EMPTY_SET  =
      Collections.unmodifiableNavigableSet(new TreeSet<>());

  private final ContainerAttribute<LifeCycleState> lifeCycleStateMap;
  private final ContainerAttribute<String> ownerMap;
  private final ContainerAttribute<ReplicationFactor> factorMap;
  private final ContainerAttribute<ReplicationType> typeMap;
  private final Map<ContainerID, ContainerInfo> containerMap;
  private final Map<ContainerID, Set<ContainerReplica>> replicaMap;
  private final Map<ContainerQueryKey, NavigableSet<ContainerID>> resultCache;

  // Container State Map lock should be held before calling into
  // Update ContainerAttributes. The consistency of ContainerAttributes is
  // protected by this lock.
  private final ReadWriteLock lock;

  /**
   * Create a ContainerStateMap.
   */
  public ContainerStateMap() {
    this.lifeCycleStateMap = new ContainerAttribute<>();
    this.ownerMap = new ContainerAttribute<>();
    this.factorMap = new ContainerAttribute<>();
    this.typeMap = new ContainerAttribute<>();
    this.containerMap = new ConcurrentHashMap<>();
    this.lock = new ReentrantReadWriteLock();
    this.replicaMap = new ConcurrentHashMap<>();
    this.resultCache = new ConcurrentHashMap<>();
  }

  /**
   * Adds a ContainerInfo Entry in the ContainerStateMap.
   *
   * @param info - container info
   * @throws SCMException - throws if create failed.
   */
  public void addContainer(final ContainerInfo info)
      throws SCMException {
    Preconditions.checkNotNull(info, "Container Info cannot be null");
    Preconditions.checkArgument(info.getReplicationFactor().getNumber() > 0,
        "ExpectedReplicaCount should be greater than 0");

    lock.writeLock().lock();
    try {
      final ContainerID id = info.containerID();
      if (containerMap.putIfAbsent(id, info) != null) {
        LOG.debug("Duplicate container ID detected. {}", id);
        throw new
            SCMException("Duplicate container ID detected.",
            CONTAINER_EXISTS);
      }

      lifeCycleStateMap.insert(info.getState(), id);
      ownerMap.insert(info.getOwner(), id);
      factorMap.insert(info.getReplicationFactor(), id);
      typeMap.insert(info.getReplicationType(), id);
      replicaMap.put(id, ConcurrentHashMap.newKeySet());

      // Flush the cache of this container type, will be added later when
      // get container queries are executed.
      flushCache(info);
      LOG.trace("Created container with {} successfully.", id);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Removes a Container Entry from ContainerStateMap.
   *
   * @param containerID - ContainerID
   * @throws SCMException - throws if create failed.
   */
  public void removeContainer(final ContainerID containerID)
      throws ContainerNotFoundException {
    Preconditions.checkNotNull(containerID, "ContainerID cannot be null");
    lock.writeLock().lock();
    try {
      checkIfContainerExist(containerID);
      // Should we revert back to the original state if any of the below
      // remove operation fails?
      final ContainerInfo info = containerMap.remove(containerID);
      lifeCycleStateMap.remove(info.getState(), containerID);
      ownerMap.remove(info.getOwner(), containerID);
      factorMap.remove(info.getReplicationFactor(), containerID);
      typeMap.remove(info.getReplicationType(), containerID);
      // Flush the cache of this container type.
      flushCache(info);
      LOG.trace("Removed container with {} successfully.", containerID);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns the latest state of Container from SCM's Container State Map.
   *
   * @param containerID - ContainerID
   * @return container info, if found.
   */
  public ContainerInfo getContainerInfo(final ContainerID containerID)
      throws ContainerNotFoundException {
    lock.readLock().lock();
    try {
      checkIfContainerExist(containerID);
      return containerMap.get(containerID);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the latest list of DataNodes where replica for given containerId
   * exist. Throws an SCMException if no entry is found for given containerId.
   *
   * @param containerID
   * @return Set<DatanodeDetails>
   */
  public Set<ContainerReplica> getContainerReplicas(
      final ContainerID containerID) throws ContainerNotFoundException {
    Preconditions.checkNotNull(containerID);
    lock.readLock().lock();
    try {
      checkIfContainerExist(containerID);
      return Collections
          .unmodifiableSet(replicaMap.get(containerID));
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Adds given datanodes as nodes where replica for given containerId exist.
   * Logs a debug entry if a datanode is already added as replica for given
   * ContainerId.
   *
   * @param containerID
   * @param replica
   */
  public void updateContainerReplica(final ContainerID containerID,
      final ContainerReplica replica) throws ContainerNotFoundException {
    Preconditions.checkNotNull(containerID);
    lock.writeLock().lock();
    try {
      checkIfContainerExist(containerID);
      Set<ContainerReplica> replicas = replicaMap.get(containerID);
      replicas.remove(replica);
      replicas.add(replica);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Remove a container Replica for given DataNode.
   *
   * @param containerID
   * @param replica
   * @return True of dataNode is removed successfully else false.
   */
  public void removeContainerReplica(final ContainerID containerID,
      final ContainerReplica replica)
      throws ContainerNotFoundException, ContainerReplicaNotFoundException {
    Preconditions.checkNotNull(containerID);
    Preconditions.checkNotNull(replica);

    lock.writeLock().lock();
    try {
      checkIfContainerExist(containerID);
      if(!replicaMap.get(containerID).remove(replica)) {
        throw new ContainerReplicaNotFoundException(
            "Container #"
                + containerID.getId() + ", replica: " + replica);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Just update the container State.
   * @param info ContainerInfo.
   */
  public void updateContainerInfo(final ContainerInfo info)
      throws ContainerNotFoundException {
    lock.writeLock().lock();
    try {
      Preconditions.checkNotNull(info);
      checkIfContainerExist(info.containerID());
      final ContainerInfo currentInfo = containerMap.get(info.containerID());
      flushCache(info, currentInfo);
      containerMap.put(info.containerID(), info);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Update the State of a container.
   *
   * @param containerID - ContainerID
   * @param currentState - CurrentState
   * @param newState - NewState.
   * @throws SCMException - in case of failure.
   */
  public void updateState(ContainerID containerID, LifeCycleState currentState,
      LifeCycleState newState) throws SCMException, ContainerNotFoundException {
    Preconditions.checkNotNull(currentState);
    Preconditions.checkNotNull(newState);
    lock.writeLock().lock();
    try {
      checkIfContainerExist(containerID);
      final ContainerInfo currentInfo = containerMap.get(containerID);
      try {
        currentInfo.setState(newState);

        // We are updating two places before this update is done, these can
        // fail independently, since the code needs to handle it.

        // We update the attribute map, if that fails it will throw an
        // exception, so no issues, if we are successful, we keep track of the
        // fact that we have updated the lifecycle state in the map, and update
        // the container state. If this second update fails, we will attempt to
        // roll back the earlier change we did. If the rollback fails, we can
        // be in an inconsistent state,

        lifeCycleStateMap.update(currentState, newState, containerID);
        LOG.trace("Updated the container {} to new state. Old = {}, new = " +
            "{}", containerID, currentState, newState);

        // Just flush both old and new data sets from the result cache.
        flushCache(currentInfo);
      } catch (SCMException ex) {
        LOG.error("Unable to update the container state. {}", ex);
        // we need to revert the change in this attribute since we are not
        // able to update the hash table.
        LOG.info("Reverting the update to lifecycle state. Moving back to " +
                "old state. Old = {}, Attempted state = {}", currentState,
            newState);

        currentInfo.setState(currentState);

        // if this line throws, the state map can be in an inconsistent
        // state, since we will have modified the attribute by the
        // container state will not in sync since we were not able to put
        // that into the hash table.
        lifeCycleStateMap.update(newState, currentState, containerID);

        throw new SCMException("Updating the container map failed.", ex,
            FAILED_TO_CHANGE_CONTAINER_STATE);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Set<ContainerID> getAllContainerIDs() {
    return Collections.unmodifiableSet(containerMap.keySet());
  }

  /**
   * Returns A list of containers owned by a name service.
   *
   * @param ownerName - Name of the NameService.
   * @return - NavigableSet of ContainerIDs.
   */
  NavigableSet<ContainerID> getContainerIDsByOwner(final String ownerName) {
    Preconditions.checkNotNull(ownerName);
    lock.readLock().lock();
    try {
      return ownerMap.getCollection(ownerName);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns Containers in the System by the Type.
   *
   * @param type - Replication type -- StandAlone, Ratis etc.
   * @return NavigableSet
   */
  NavigableSet<ContainerID> getContainerIDsByType(final ReplicationType type) {
    Preconditions.checkNotNull(type);
    lock.readLock().lock();
    try {
      return typeMap.getCollection(type);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns Containers by replication factor.
   *
   * @param factor - Replication Factor.
   * @return NavigableSet.
   */
  NavigableSet<ContainerID> getContainerIDsByFactor(
      final ReplicationFactor factor) {
    Preconditions.checkNotNull(factor);
    lock.readLock().lock();
    try {
      return factorMap.getCollection(factor);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns Containers by State.
   *
   * @param state - State - Open, Closed etc.
   * @return List of containers by state.
   */
  public NavigableSet<ContainerID> getContainerIDsByState(
      final LifeCycleState state) {
    Preconditions.checkNotNull(state);
    lock.readLock().lock();
    try {
      return lifeCycleStateMap.getCollection(state);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Gets the containers that matches the  following filters.
   *
   * @param state - LifeCycleState
   * @param owner - Owner
   * @param factor - Replication Factor
   * @param type - Replication Type
   * @return ContainerInfo or Null if not container satisfies the criteria.
   */
  public NavigableSet<ContainerID> getMatchingContainerIDs(
      final LifeCycleState state, final String owner,
      final ReplicationFactor factor, final ReplicationType type) {

    Preconditions.checkNotNull(state, "State cannot be null");
    Preconditions.checkNotNull(owner, "Owner cannot be null");
    Preconditions.checkNotNull(factor, "Factor cannot be null");
    Preconditions.checkNotNull(type, "Type cannot be null");

    lock.readLock().lock();
    try {
      final ContainerQueryKey queryKey =
          new ContainerQueryKey(state, owner, factor, type);
      if(resultCache.containsKey(queryKey)){
        return resultCache.get(queryKey);
      }

      // If we cannot meet any one condition we return EMPTY_SET immediately.
      // Since when we intersect these sets, the result will be empty if any
      // one is empty.
      final NavigableSet<ContainerID> stateSet =
          lifeCycleStateMap.getCollection(state);
      if (stateSet.size() == 0) {
        return EMPTY_SET;
      }

      final NavigableSet<ContainerID> ownerSet =
          ownerMap.getCollection(owner);
      if (ownerSet.size() == 0) {
        return EMPTY_SET;
      }

      final NavigableSet<ContainerID> factorSet =
          factorMap.getCollection(factor);
      if (factorSet.size() == 0) {
        return EMPTY_SET;
      }

      final NavigableSet<ContainerID> typeSet =
          typeMap.getCollection(type);
      if (typeSet.size() == 0) {
        return EMPTY_SET;
      }


      // if we add more constraints we will just add those sets here..
      final NavigableSet<ContainerID>[] sets = sortBySize(stateSet,
          ownerSet, factorSet, typeSet);

      NavigableSet<ContainerID> currentSet = sets[0];
      // We take the smallest set and intersect against the larger sets. This
      // allows us to reduce the lookups to the least possible number.
      for (int x = 1; x < sets.length; x++) {
        currentSet = intersectSets(currentSet, sets[x]);
      }
      resultCache.put(queryKey, currentSet);
      return currentSet;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Calculates the intersection between sets and returns a new set.
   *
   * @param smaller - First Set
   * @param bigger - Second Set
   * @return resultSet which is the intersection of these two sets.
   */
  private NavigableSet<ContainerID> intersectSets(
      final NavigableSet<ContainerID> smaller,
      final NavigableSet<ContainerID> bigger) {
    Preconditions.checkState(smaller.size() <= bigger.size(),
        "This function assumes the first set is lesser or equal to second " +
            "set");
    final NavigableSet<ContainerID> resultSet = new TreeSet<>();
    for (ContainerID id : smaller) {
      if (bigger.contains(id)) {
        resultSet.add(id);
      }
    }
    return resultSet;
  }

  /**
   * Sorts a list of Sets based on Size. This is useful when we are
   * intersecting the sets.
   *
   * @param sets - varagrs of sets
   * @return Returns a sorted array of sets based on the size of the set.
   */
  @SuppressWarnings("unchecked")
  private NavigableSet<ContainerID>[] sortBySize(
      final NavigableSet<ContainerID>... sets) {
    for (int x = 0; x < sets.length - 1; x++) {
      for (int y = 0; y < sets.length - x - 1; y++) {
        if (sets[y].size() > sets[y + 1].size()) {
          final NavigableSet temp = sets[y];
          sets[y] = sets[y + 1];
          sets[y + 1] = temp;
        }
      }
    }
    return sets;
  }

  private void flushCache(final ContainerInfo... containerInfos) {
    for (ContainerInfo containerInfo : containerInfos) {
      final ContainerQueryKey key = new ContainerQueryKey(
          containerInfo.getState(),
          containerInfo.getOwner(),
          containerInfo.getReplicationFactor(),
          containerInfo.getReplicationType());
      resultCache.remove(key);
    }
  }

  private void checkIfContainerExist(ContainerID containerID)
      throws ContainerNotFoundException {
    if (!containerMap.containsKey(containerID)) {
      throw new ContainerNotFoundException("#" + containerID.getId());
    }
  }

}
