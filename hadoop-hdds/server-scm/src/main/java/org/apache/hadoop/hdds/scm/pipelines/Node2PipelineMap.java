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

package org.apache.hadoop.hdds.scm.pipelines;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This data structure maintains the list of pipelines which the given datanode is a part of. This
 * information will be added whenever a new pipeline allocation happens.
 *
 * <p>TODO: this information needs to be regenerated from pipeline reports on SCM restart
 */
public class Node2PipelineMap {
  private final Map<UUID, Set<PipelineID>> dn2PipelineMap;

  /** Constructs a Node2PipelineMap Object. */
  public Node2PipelineMap() {
    dn2PipelineMap = new ConcurrentHashMap<>();
  }

  /**
   * Returns true if this a datanode that is already tracked by Node2PipelineMap.
   *
   * @param datanodeID - UUID of the Datanode.
   * @return True if this is tracked, false if this map does not know about it.
   */
  private boolean isKnownDatanode(UUID datanodeID) {
    Preconditions.checkNotNull(datanodeID);
    return dn2PipelineMap.containsKey(datanodeID);
  }

  /**
   * Removes datanode Entry from the map.
   *
   * @param datanodeID - Datanode ID.
   */
  public synchronized void removeDatanode(UUID datanodeID) {
    Preconditions.checkNotNull(datanodeID);
    dn2PipelineMap.computeIfPresent(datanodeID, (k, v) -> null);
  }

  /**
   * Returns null if there no pipelines associated with this datanode ID.
   *
   * @param datanode - UUID
   * @return Set of pipelines or Null.
   */
  public Set<PipelineID> getPipelines(UUID datanode) {
    Preconditions.checkNotNull(datanode);
    final Set<PipelineID> s = dn2PipelineMap.get(datanode);
    return s != null? Collections.unmodifiableSet(s): Collections.emptySet();
  }

  /**
   * Adds a pipeline entry to a given dataNode in the map.
   *
   * @param pipeline Pipeline to be added
   */
  public synchronized void addPipeline(Pipeline pipeline) {
    for (DatanodeDetails details : pipeline.getDatanodes().values()) {
      UUID dnId = details.getUuid();
      dn2PipelineMap.computeIfAbsent(dnId, k -> new HashSet<>())
          .add(pipeline.getId());
    }
  }

  public synchronized void removePipeline(Pipeline pipeline) {
    for (DatanodeDetails details : pipeline.getDatanodes().values()) {
      UUID dnId = details.getUuid();
      dn2PipelineMap.computeIfPresent(
          dnId,
          (k, v) -> {
            v.remove(pipeline.getId());
            return v;
          });
    }
  }

  public Map<UUID, Set<PipelineID>> getDn2PipelineMap() {
    return Collections.unmodifiableMap(dn2PipelineMap);
  }
}
