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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * <p>
 * This class has the following functionality.
 *
 * <p>
 * Provide an interface for MultiNodeLookupPolicy so that different placement
 * allocator can choose nodes based on need.
 * </p>
 */
public interface MultiNodeLookupPolicy<N extends SchedulerNode> {
  /**
   * Get iterator of preferred node depends on requirement and/or availability.
   *
   * @param nodes
   *          List of Nodes
   * @param partition
   *          node label
   *
   * @return iterator of preferred node
   */
  Iterator<N> getPreferredNodeIterator(Collection<N> nodes, String partition);

  /**
   * Refresh working nodes set for re-ordering based on the algorithm selected.
   *
   * @param nodes
   *          a collection working nm's.
   * @param partition
   *          node label
   */
  void addAndRefreshNodesSet(Collection<N> nodes, String partition);

  /**
   * Get sorted nodes per partition.
   *
   * @param partition
   *          node label
   *
   * @return collection of sorted nodes
   */
  Set<N> getNodesPerPartition(String partition);

}
