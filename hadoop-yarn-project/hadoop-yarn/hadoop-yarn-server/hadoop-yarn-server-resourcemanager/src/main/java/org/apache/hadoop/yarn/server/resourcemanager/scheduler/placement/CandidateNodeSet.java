/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.Map;

/**
 * A group of nodes which can be allocated by scheduler.
 *
 * It will have following part:
 *
 * 1) A map of nodes which can be schedule-able.
 * 2) Version of the node set, version should be updated if any node added or
 *    removed from the node set. This will be used by
 *    {@link AppPlacementAllocator} or other class to check if it's required to
 *    invalidate local caches, etc.
 * 3) Node partition of the candidate set.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface CandidateNodeSet<N extends SchedulerNode> {
  /**
   * Get all nodes for this CandidateNodeSet
   * @return all nodes for this CandidateNodeSet
   */
  Map<NodeId, N> getAllNodes();

  /**
   * Version of the CandidateNodeSet, can help {@link AppPlacementAllocator} to
   * decide if update is required
   * @return version
   */
  long getVersion();

  /**
   * Node partition of the node set.
   * @return node partition
   */
  String getPartition();
}
