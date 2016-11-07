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

import java.util.Iterator;
import java.util.Map;

/**
 * <p>
 * PlacementSet is the central place that decide the order of node to fit
 * asks by application.
 * </p>
 *
 * <p>
 * Also, PlacementSet can cache results (for example, ordered list) for
 * better performance.
 * </p>
 *
 * <p>
 * PlacementSet can depend on one or more other PlacementSets.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface PlacementSet<N extends SchedulerNode> {
  /**
   * Get all nodes for this PlacementSet
   * @return all nodes for this PlacementSet
   */
  Map<NodeId, N> getAllNodes();

  /**
   * Version of the PlacementSet, can help other PlacementSet with dependencies
   * deciding if update is required
   * @return version
   */
  long getVersion();

  /**
   * Partition of the PlacementSet.
   * @return node partition
   */
  String getPartition();
}
