/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common
    .interfaces.ContainerDeletionChoosingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;


/**
 * Class that manages Containers created on the datanode.
 */
public class ContainerSet {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerSet.class);

  private final ConcurrentSkipListMap<Long, Container> containerMap = new
      ConcurrentSkipListMap<>();
  private final ConcurrentSkipListSet<Long> missingContainerSet =
      new ConcurrentSkipListSet<>();
  /**
   * Add Container to container map.
   * @param container
   * @return If container is added to containerMap returns true, otherwise
   * false
   */
  public boolean addContainer(Container container) throws
      StorageContainerException {
    Preconditions.checkNotNull(container, "container cannot be null");

    long containerId = container.getContainerData().getContainerID();
    if(containerMap.putIfAbsent(containerId, container) == null) {
      LOG.debug("Container with container Id {} is added to containerMap",
          containerId);
      return true;
    } else {
      LOG.warn("Container already exists with container Id {}", containerId);
      throw new StorageContainerException("Container already exists with " +
          "container Id " + containerId,
          ContainerProtos.Result.CONTAINER_EXISTS);
    }
  }

  /**
   * Returns the Container with specified containerId.
   * @param containerId
   * @return Container
   */
  public Container getContainer(long containerId) {
    Preconditions.checkState(containerId >= 0,
        "Container Id cannot be negative.");
    return containerMap.get(containerId);
  }

  /**
   * Removes the Container matching with specified containerId.
   * @param containerId
   * @return If container is removed from containerMap returns true, otherwise
   * false
   */
  public boolean removeContainer(long containerId) {
    Preconditions.checkState(containerId >= 0,
        "Container Id cannot be negative.");
    Container removed = containerMap.remove(containerId);
    if(removed == null) {
      LOG.debug("Container with containerId {} is not present in " +
          "containerMap", containerId);
      return false;
    } else {
      LOG.debug("Container with containerId {} is removed from containerMap",
          containerId);
      return true;
    }
  }

  /**
   * Return number of containers in container map.
   * @return container count
   */
  @VisibleForTesting
  public int containerCount() {
    return containerMap.size();
  }

  /**
   * Return an container Iterator over {@link ContainerSet#containerMap}.
   * @return {@literal Iterator<Container>}
   */
  public Iterator<Container> getContainerIterator() {
    return containerMap.values().iterator();
  }

  /**
   * Return an containerMap iterator over {@link ContainerSet#containerMap}.
   * @return containerMap Iterator
   */
  public Iterator<Map.Entry<Long, Container>> getContainerMapIterator() {
    containerMap.keySet().stream().collect(Collectors.toSet());
    return containerMap.entrySet().iterator();
  }

  /**
   * Return a copy of the containerMap.
   * @return containerMap
   */
  @VisibleForTesting
  public Map<Long, Container> getContainerMapCopy() {
    return ImmutableMap.copyOf(containerMap);
  }

  /**
   * A simple interface for container Iterations.
   * <p>
   * This call make no guarantees about consistency of the data between
   * different list calls. It just returns the best known data at that point of
   * time. It is possible that using this iteration you can miss certain
   * container from the listing.
   *
   * @param startContainerId - Return containers with Id &gt;= startContainerId.
   * @param count - how many to return
   * @param data - Actual containerData
   * @throws StorageContainerException
   */
  public void listContainer(long startContainerId, long count,
                            List<ContainerData> data) throws
      StorageContainerException {
    Preconditions.checkNotNull(data,
        "Internal assertion: data cannot be null");
    Preconditions.checkState(startContainerId >= 0,
        "Start container Id cannot be negative");
    Preconditions.checkState(count > 0,
        "max number of containers returned " +
            "must be positive");
    LOG.debug("listContainer returns containerData starting from {} of count " +
        "{}", startContainerId, count);
    ConcurrentNavigableMap<Long, Container> map;
    if (startContainerId == 0) {
      map = containerMap.tailMap(containerMap.firstKey(), true);
    } else {
      map = containerMap.tailMap(startContainerId, true);
    }
    int currentCount = 0;
    for (Container entry : map.values()) {
      if (currentCount < count) {
        data.add(entry.getContainerData());
        currentCount++;
      } else {
        return;
      }
    }
  }

  /**
   * Get container report.
   *
   * @return The container report.
   * @throws IOException
   */
  public ContainerReportsProto getContainerReport() throws IOException {
    LOG.debug("Starting container report iteration.");

    // No need for locking since containerMap is a ConcurrentSkipListMap
    // And we can never get the exact state since close might happen
    // after we iterate a point.
    List<Container> containers = containerMap.values().stream().collect(
        Collectors.toList());

    ContainerReportsProto.Builder crBuilder =
        ContainerReportsProto.newBuilder();

    for (Container container: containers) {
      crBuilder.addReports(container.getContainerReport());
    }

    return crBuilder.build();
  }

  public List<ContainerData> chooseContainerForBlockDeletion(int count,
      ContainerDeletionChoosingPolicy deletionPolicy)
      throws StorageContainerException {
    Map<Long, ContainerData> containerDataMap = containerMap.entrySet().stream()
        .filter(e -> deletionPolicy.isValidContainerType(
            e.getValue().getContainerType()))
        .collect(Collectors.toMap(Map.Entry::getKey,
            e -> e.getValue().getContainerData()));
    return deletionPolicy
        .chooseContainerForBlockDeletion(count, containerDataMap);
  }

  public Set<Long> getMissingContainerSet() {
    return missingContainerSet;
  }

  /**
   * Builds the missing container set by taking a diff total no containers
   * actually found and number of containers which actually got created.
   * This will only be called during the initialization of Datanode Service
   * when  it still not a part of any write Pipeline.
   * @param createdContainerSet ContainerId set persisted in the Ratis snapshot
   */
  public void buildMissingContainerSet(Set<Long> createdContainerSet) {
    missingContainerSet.addAll(createdContainerSet);
    missingContainerSet.removeAll(containerMap.keySet());
  }
}
