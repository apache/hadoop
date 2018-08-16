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

package org.apache.hadoop.hdds.scm.node.states;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.DUPLICATE_DATANODE;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.NO_SUCH_DATANODE;

/**
 * This data structure maintains the list of containers that is on a datanode.
 * This information is built from the DN container reports.
 */
public class Node2ContainerMap {
  private final Map<UUID, Set<ContainerID>> dn2ContainerMap;

  /**
   * Constructs a Node2ContainerMap Object.
   */
  public Node2ContainerMap() {
    dn2ContainerMap = new ConcurrentHashMap<>();
  }

  /**
   * Returns true if this a datanode that is already tracked by
   * Node2ContainerMap.
   *
   * @param datanodeID - UUID of the Datanode.
   * @return True if this is tracked, false if this map does not know about it.
   */
  public boolean isKnownDatanode(UUID datanodeID) {
    Preconditions.checkNotNull(datanodeID);
    return dn2ContainerMap.containsKey(datanodeID);
  }

  /**
   * Insert a new datanode into Node2Container Map.
   *
   * @param datanodeID -- Datanode UUID
   * @param containerIDs - List of ContainerIDs.
   */
  public void insertNewDatanode(UUID datanodeID, Set<ContainerID> containerIDs)
      throws SCMException {
    Preconditions.checkNotNull(containerIDs);
    Preconditions.checkNotNull(datanodeID);
    if (dn2ContainerMap.putIfAbsent(datanodeID, new HashSet<>(containerIDs))
        != null) {
      throw new SCMException("Node already exists in the map",
                  DUPLICATE_DATANODE);
    }
  }

  /**
   * Updates the Container list of an existing DN.
   *
   * @param datanodeID - UUID of DN.
   * @param containers - Set of Containers tht is present on DN.
   * @throws SCMException - if we don't know about this datanode, for new DN
   *                      use insertNewDatanode.
   */
  public void setContainersForDatanode(UUID datanodeID, Set<ContainerID> containers)
      throws SCMException {
    Preconditions.checkNotNull(datanodeID);
    Preconditions.checkNotNull(containers);
    if (dn2ContainerMap
        .computeIfPresent(datanodeID, (k, v) -> new HashSet<>(containers))
        == null) {
      throw new SCMException("No such datanode", NO_SUCH_DATANODE);
    }
  }

  /**
   * Removes datanode Entry from the map.
   * @param datanodeID - Datanode ID.
   */
  public void removeDatanode(UUID datanodeID) {
    Preconditions.checkNotNull(datanodeID);
    dn2ContainerMap.computeIfPresent(datanodeID, (k, v) -> null);
  }

  /**
   * Returns null if there no containers associated with this datanode ID.
   *
   * @param datanode - UUID
   * @return Set of containers or Null.
   */
  public Set<ContainerID> getContainers(UUID datanode) {
    Preconditions.checkNotNull(datanode);
    return dn2ContainerMap.computeIfPresent(datanode, (k, v) ->
        Collections.unmodifiableSet(v));
  }

  public ReportResult processReport(UUID datanodeID, Set<ContainerID>
      containers) {
    Preconditions.checkNotNull(datanodeID);
    Preconditions.checkNotNull(containers);

    if (!isKnownDatanode(datanodeID)) {
      return ReportResult.ReportResultBuilder.newBuilder()
          .setStatus(ReportStatus.NEW_DATANODE_FOUND)
          .setNewContainers(containers)
          .build();
    }

    // Conditions like Zero length containers should be handled by removeAll.
    Set<ContainerID> currentSet = dn2ContainerMap.get(datanodeID);
    TreeSet<ContainerID> newContainers = new TreeSet<>(containers);
    newContainers.removeAll(currentSet);

    TreeSet<ContainerID> missingContainers = new TreeSet<>(currentSet);
    missingContainers.removeAll(containers);

    if (newContainers.isEmpty() && missingContainers.isEmpty()) {
      return ReportResult.ReportResultBuilder.newBuilder()
          .setStatus(ReportStatus.ALL_IS_WELL)
          .build();
    }

    if (newContainers.isEmpty() && !missingContainers.isEmpty()) {
      return ReportResult.ReportResultBuilder.newBuilder()
          .setStatus(ReportStatus.MISSING_CONTAINERS)
          .setMissingContainers(missingContainers)
          .build();
    }

    if (!newContainers.isEmpty() && missingContainers.isEmpty()) {
      return ReportResult.ReportResultBuilder.newBuilder()
          .setStatus(ReportStatus.NEW_CONTAINERS_FOUND)
          .setNewContainers(newContainers)
          .build();
    }

    if (!newContainers.isEmpty() && !missingContainers.isEmpty()) {
      return ReportResult.ReportResultBuilder.newBuilder()
          .setStatus(ReportStatus.MISSING_AND_NEW_CONTAINERS_FOUND)
          .setNewContainers(newContainers)
          .setMissingContainers(missingContainers)
          .build();
    }

    // default status & Make compiler happy
    return ReportResult.ReportResultBuilder.newBuilder()
        .setStatus(ReportStatus.ALL_IS_WELL)
        .build();
  }





  /**
   * Results possible from processing a container report by
   * Node2ContainerMapper.
   */
  public enum ReportStatus {
    ALL_IS_WELL,
    MISSING_CONTAINERS,
    NEW_CONTAINERS_FOUND,
    MISSING_AND_NEW_CONTAINERS_FOUND,
    NEW_DATANODE_FOUND
  }
}
