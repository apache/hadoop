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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;

import java.util.UUID;
import java.util.Set;
import java.util.Map;
import java.util.TreeSet;
import java.util.HashSet;
import java.util.Collections;

import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.DUPLICATE_DATANODE;

/**
 * This data structure maintains the list of containers that is on a datanode.
 * This information is built from the DN container reports.
 */
public class Node2ObjectsMap<T> {

  @SuppressWarnings("visibilitymodifier")
  protected final Map<UUID, Set<T>> dn2ObjectMap;

  /**
   * Constructs a Node2ContainerMap Object.
   */
  public Node2ObjectsMap() {
    dn2ObjectMap = new ConcurrentHashMap<>();
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
    return dn2ObjectMap.containsKey(datanodeID);
  }

  /**
   * Insert a new datanode into Node2Container Map.
   *
   * @param datanodeID   -- Datanode UUID
   * @param containerIDs - List of ContainerIDs.
   */
  @VisibleForTesting
  public void insertNewDatanode(UUID datanodeID, Set<T> containerIDs)
      throws SCMException {
    Preconditions.checkNotNull(containerIDs);
    Preconditions.checkNotNull(datanodeID);
    if (dn2ObjectMap.putIfAbsent(datanodeID, new HashSet<>(containerIDs))
        != null) {
      throw new SCMException("Node already exists in the map",
          DUPLICATE_DATANODE);
    }
  }

  /**
   * Removes datanode Entry from the map.
   *
   * @param datanodeID - Datanode ID.
   */
  @VisibleForTesting
  public void removeDatanode(UUID datanodeID) {
    Preconditions.checkNotNull(datanodeID);
    dn2ObjectMap.computeIfPresent(datanodeID, (k, v) -> null);
  }

  /**
   * Returns null if there no containers associated with this datanode ID.
   *
   * @param datanode - UUID
   * @return Set of containers or Null.
   */
  Set<T> getObjects(UUID datanode) {
    Preconditions.checkNotNull(datanode);
    final Set<T> s = dn2ObjectMap.get(datanode);
    return s != null? Collections.unmodifiableSet(s): Collections.emptySet();
  }

  public ReportResult.ReportResultBuilder<T> newBuilder() {
    return new ReportResult.ReportResultBuilder<>();
  }

  public ReportResult<T> processReport(UUID datanodeID, Set<T> objects) {
    Preconditions.checkNotNull(datanodeID);
    Preconditions.checkNotNull(objects);

    if (!isKnownDatanode(datanodeID)) {
      return newBuilder()
          .setStatus(ReportResult.ReportStatus.NEW_DATANODE_FOUND)
          .setNewEntries(objects)
          .build();
    }

    // Conditions like Zero length containers should be handled by removeAll.
    Set<T> currentSet = dn2ObjectMap.get(datanodeID);
    TreeSet<T> newObjects = new TreeSet<>(objects);
    newObjects.removeAll(currentSet);

    TreeSet<T> missingObjects = new TreeSet<>(currentSet);
    missingObjects.removeAll(objects);

    if (newObjects.isEmpty() && missingObjects.isEmpty()) {
      return newBuilder()
          .setStatus(ReportResult.ReportStatus.ALL_IS_WELL)
          .build();
    }

    if (newObjects.isEmpty() && !missingObjects.isEmpty()) {
      return newBuilder()
          .setStatus(ReportResult.ReportStatus.MISSING_ENTRIES)
          .setMissingEntries(missingObjects)
          .build();
    }

    if (!newObjects.isEmpty() && missingObjects.isEmpty()) {
      return newBuilder()
          .setStatus(ReportResult.ReportStatus.NEW_ENTRIES_FOUND)
          .setNewEntries(newObjects)
          .build();
    }

    if (!newObjects.isEmpty() && !missingObjects.isEmpty()) {
      return newBuilder()
          .setStatus(ReportResult.ReportStatus.MISSING_AND_NEW_ENTRIES_FOUND)
          .setNewEntries(newObjects)
          .setMissingEntries(missingObjects)
          .build();
    }

    // default status & Make compiler happy
    return newBuilder()
        .setStatus(ReportResult.ReportStatus.ALL_IS_WELL)
        .build();
  }

  @VisibleForTesting
  public int size() {
    return dn2ObjectMap.size();
  }
}
