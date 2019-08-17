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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes
    .NO_SUCH_DATANODE;

/**
 * This data structure maintains the list of containers that is on a datanode.
 * This information is built from the DN container reports.
 */
public class Node2ContainerMap extends Node2ObjectsMap<ContainerID> {

  /**
   * Constructs a Node2ContainerMap Object.
   */
  public Node2ContainerMap() {
    super();
  }

  /**
   * Returns null if there no containers associated with this datanode ID.
   *
   * @param datanode - UUID
   * @return Set of containers or Null.
   */
  public Set<ContainerID> getContainers(UUID datanode) {
    return getObjects(datanode);
  }

  /**
   * Insert a new datanode into Node2Container Map.
   *
   * @param datanodeID   -- Datanode UUID
   * @param containerIDs - List of ContainerIDs.
   */
  @Override
  public void insertNewDatanode(UUID datanodeID, Set<ContainerID> containerIDs)
      throws SCMException {
    super.insertNewDatanode(datanodeID, containerIDs);
  }

  /**
   * Updates the Container list of an existing DN.
   *
   * @param datanodeID - UUID of DN.
   * @param containers - Set of Containers tht is present on DN.
   * @throws SCMException - if we don't know about this datanode, for new DN
   *                        use addDatanodeInContainerMap.
   */
  public void setContainersForDatanode(UUID datanodeID,
      Set<ContainerID> containers) throws SCMException {
    Preconditions.checkNotNull(datanodeID);
    Preconditions.checkNotNull(containers);
    if (dn2ObjectMap
        .computeIfPresent(datanodeID, (k, v) -> new HashSet<>(containers))
        == null) {
      throw new SCMException("No such datanode", NO_SUCH_DATANODE);
    }
  }

  @VisibleForTesting
  @Override
  public int size() {
    return dn2ObjectMap.size();
  }
}
