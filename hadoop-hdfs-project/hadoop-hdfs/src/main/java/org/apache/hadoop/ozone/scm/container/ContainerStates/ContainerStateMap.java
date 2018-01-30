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

package org.apache.hadoop.ozone.scm.container.ContainerStates;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.LifeCycleState;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationFactor;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationType;
import org.apache.hadoop.ozone.scm.exceptions.SCMException;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.util.AutoCloseableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.apache.hadoop.ozone.scm.exceptions.SCMException.ResultCodes
    .CONTAINER_EXISTS;
import static org.apache.hadoop.ozone.scm.exceptions.SCMException.ResultCodes
    .FAILED_TO_CHANGE_CONTAINER_STATE;
import static org.apache.hadoop.ozone.scm.exceptions.SCMException.ResultCodes
    .FAILED_TO_FIND_CONTAINER;

/**
 * Container State Map acts like a unified map for various attributes that are
 * used to select containers when we need allocated blocks.
 * <p>
 * This class provides the ability to query 4 classes of attributes. They are
 * <p>
 * 1. LifeCycleStates - LifeCycle States of container describe in which state
 * a container is. For example, a container needs to be in Open State for a
 * client to able to write to it.
 * <p>
 * 2. Owners - Each instance of Name service, for example, Namenode of HDFS or
 * Key Space Manager (KSM) of Ozone or CBlockServer --  is an owner. It is
 * possible to have many KSMs for a Ozone cluster and only one SCM. But SCM
 * keeps the data from each KSM in separate bucket, never mixing them. To
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

  private final ContainerAttribute<LifeCycleState> lifeCycleStateMap;
  private final ContainerAttribute<String> ownerMap;
  private final ContainerAttribute<ReplicationFactor> factorMap;
  private final ContainerAttribute<ReplicationType> typeMap;

  private final Map<ContainerID, ContainerInfo> containerMap;
  private final static NavigableSet<ContainerID> EMPTY_SET  =
      Collections.unmodifiableNavigableSet(new TreeSet<>());

  // Container State Map lock should be held before calling into
  // Update ContainerAttributes. The consistency of ContainerAttributes is
  // protected by this lock.
  private final AutoCloseableLock autoLock;

  /**
   * Create a ContainerStateMap.
   */
  public ContainerStateMap() {
    lifeCycleStateMap = new ContainerAttribute<>();
    ownerMap = new ContainerAttribute<>();
    factorMap = new ContainerAttribute<>();
    typeMap = new ContainerAttribute<>();
    containerMap = new HashMap<>();
    autoLock = new AutoCloseableLock();
//        new InstrumentedLock(getClass().getName(), LOG,
//            new ReentrantLock(),
//            1000,
//            300));
  }

  /**
   * Adds a ContainerInfo Entry in the ContainerStateMap.
   *
   * @param info - container info
   * @throws SCMException - throws if create failed.
   */
  public void addContainer(ContainerInfo info)
      throws SCMException {

    try (AutoCloseableLock lock = autoLock.acquire()) {
      ContainerID id = ContainerID.valueof(info.getContainerID());
      if (containerMap.putIfAbsent(id, info) != null) {
        LOG.debug("Duplicate container ID detected. {}", id);
        throw new
            SCMException("Duplicate container ID detected.",
            CONTAINER_EXISTS);
      }

      lifeCycleStateMap.insert(info.getState(), id);
      ownerMap.insert(info.getOwner(), id);
      factorMap.insert(info.getPipeline().getFactor(), id);
      typeMap.insert(info.getPipeline().getType(), id);
      LOG.trace("Created container with {} successfully.", id);
    }
  }

  /**
   * Returns the latest state of Container from SCM's Container State Map.
   *
   * @param info - ContainerInfo
   * @return ContainerInfo
   */
  public ContainerInfo getContainerInfo(ContainerInfo info) {
    return getContainerInfo(info.getContainerID());
  }

  /**
   * Returns the latest state of Container from SCM's Container State Map.
   *
   * @param containerID - int
   * @return container info, if found.
   */
  public ContainerInfo getContainerInfo(long containerID) {
    ContainerID id = new ContainerID(containerID);
    return containerMap.get(id);
  }

  /**
   * Returns the full container Map.
   *
   * @return - Map
   */
  public Map<ContainerID, ContainerInfo> getContainerMap() {
    try (AutoCloseableLock lock = autoLock.acquire()) {
      return Collections.unmodifiableMap(containerMap);
    }
  }

  /**
   * Just update the container State.
   * @param info ContainerInfo.
   */
  public void updateContainerInfo(ContainerInfo info) throws SCMException {
    Preconditions.checkNotNull(info);
    ContainerInfo currentInfo = null;
    try (AutoCloseableLock lock = autoLock.acquire()) {
      currentInfo = containerMap.get(
          ContainerID.valueof(info.getContainerID()));

      if (currentInfo == null) {
        throw new SCMException("No such container.", FAILED_TO_FIND_CONTAINER);
      }
      containerMap.put(info.containerID(), info);
    }
  }

  /**
   * Update the State of a container.
   *
   * @param info - ContainerInfo
   * @param currentState - CurrentState
   * @param newState - NewState.
   * @throws SCMException - in case of failure.
   */
  public void updateState(ContainerInfo info, LifeCycleState currentState,
      LifeCycleState newState) throws SCMException {
    Preconditions.checkNotNull(currentState);
    Preconditions.checkNotNull(newState);

    ContainerID id = new ContainerID(info.getContainerID());
    ContainerInfo currentInfo = null;

    try (AutoCloseableLock lock = autoLock.acquire()) {
      currentInfo = containerMap.get(id);

      if (currentInfo == null) {
        throw new
            SCMException("No such container.", FAILED_TO_FIND_CONTAINER);
      }
      // We are updating two places before this update is done, these can
      // fail independently, since the code needs to handle it.

      // We update the attribute map, if that fails it will throw an exception,
      // so no issues, if we are successful, we keep track of the fact that we
      // have updated the lifecycle state in the map, and update the container
      // state. If this second update fails, we will attempt to roll back the
      // earlier change we did. If the rollback fails, we can be in an
      // inconsistent state,

      info.setState(newState);
      containerMap.put(id, info);
      lifeCycleStateMap.update(currentState, newState, id);
      LOG.trace("Updated the container {} to new state. Old = {}, new = " +
          "{}", id, currentState, newState);
    } catch (SCMException ex) {
      LOG.error("Unable to update the container state. {}", ex);
      // we need to revert the change in this attribute since we are not
      // able to update the hash table.
      LOG.info("Reverting the update to lifecycle state. Moving back to " +
              "old state. Old = {}, Attempted state = {}", currentState,
          newState);

      containerMap.put(id, currentInfo);

      // if this line throws, the state map can be in an inconsistent
      // state, since we will have modified the attribute by the
      // container state will not in sync since we were not able to put
      // that into the hash table.
      lifeCycleStateMap.update(newState, currentState, id);

      throw new SCMException("Updating the container map failed.", ex,
          FAILED_TO_CHANGE_CONTAINER_STATE);
    }
  }

  /**
   * Returns A list of containers owned by a name service.
   *
   * @param ownerName - Name of the NameService.
   * @return - NavigableSet of ContainerIDs.
   */
  NavigableSet<ContainerID> getContainerIDsByOwner(String ownerName) {
    Preconditions.checkNotNull(ownerName);

    try (AutoCloseableLock lock = autoLock.acquire()) {
      return ownerMap.getCollection(ownerName);
    }
  }

  /**
   * Returns Containers in the System by the Type.
   *
   * @param type - Replication type -- StandAlone, Ratis etc.
   * @return NavigableSet
   */
  NavigableSet<ContainerID> getContainerIDsByType(ReplicationType type) {
    Preconditions.checkNotNull(type);

    try (AutoCloseableLock lock = autoLock.acquire()) {
      return typeMap.getCollection(type);
    }
  }

  /**
   * Returns Containers by replication factor.
   *
   * @param factor - Replication Factor.
   * @return NavigableSet.
   */
  NavigableSet<ContainerID> getContainerIDsByFactor(ReplicationFactor factor) {
    Preconditions.checkNotNull(factor);

    try (AutoCloseableLock lock = autoLock.acquire()) {
      return factorMap.getCollection(factor);
    }
  }

  /**
   * Returns Containers by State.
   *
   * @param state - State - Open, Closed etc.
   * @return List of containers by state.
   */
  NavigableSet<ContainerID> getContainerIDsByState(LifeCycleState state) {
    Preconditions.checkNotNull(state);

    try (AutoCloseableLock lock = autoLock.acquire()) {
      return lifeCycleStateMap.getCollection(state);
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
      LifeCycleState state, String owner,
      ReplicationFactor factor, ReplicationType type) {

    Preconditions.checkNotNull(state, "State cannot be null");
    Preconditions.checkNotNull(owner, "Owner cannot be null");
    Preconditions.checkNotNull(factor, "Factor cannot be null");
    Preconditions.checkNotNull(type, "Type cannot be null");

    try (AutoCloseableLock lock = autoLock.acquire()) {

      // If we cannot meet any one condition we return EMPTY_SET immediately.
      // Since when we intersect these sets, the result will be empty if any
      // one is empty.
      NavigableSet<ContainerID> stateSet =
          lifeCycleStateMap.getCollection(state);
      if (stateSet.size() == 0) {
        return EMPTY_SET;
      }

      NavigableSet<ContainerID> ownerSet = ownerMap.getCollection(owner);
      if (ownerSet.size() == 0) {
        return EMPTY_SET;
      }

      NavigableSet<ContainerID> factorSet = factorMap.getCollection(factor);
      if (factorSet.size() == 0) {
        return EMPTY_SET;
      }

      NavigableSet<ContainerID> typeSet = typeMap.getCollection(type);
      if (typeSet.size() == 0) {
        return EMPTY_SET;
      }


      // if we add more constraints we will just add those sets here..
      NavigableSet<ContainerID>[] sets = sortBySize(stateSet,
          ownerSet, factorSet, typeSet);

      NavigableSet<ContainerID> currentSet = sets[0];
      // We take the smallest set and intersect against the larger sets. This
      // allows us to reduce the lookups to the least possible number.
      for (int x = 1; x < sets.length; x++) {
        currentSet = intersectSets(currentSet, sets[x]);
      }
      return currentSet;
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
      NavigableSet<ContainerID> smaller,
      NavigableSet<ContainerID> bigger) {
    Preconditions.checkState(smaller.size() <= bigger.size(),
        "This function assumes the first set is lesser or equal to second " +
            "set");
    NavigableSet<ContainerID> resultSet = new TreeSet<>();
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
      NavigableSet<ContainerID>... sets) {
    for (int x = 0; x < sets.length - 1; x++) {
      for (int y = 0; y < sets.length - x - 1; y++) {
        if (sets[y].size() > sets[y + 1].size()) {
          NavigableSet temp = sets[y];
          sets[y] = sets[y + 1];
          sets[y + 1] = temp;
        }
      }
    }
    return sets;
  }
}
