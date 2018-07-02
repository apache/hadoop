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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * This interface abstracts out how a container contributes to
 * Resource Utilization of the node.
 * It is used by the {@link ContainerScheduler} to determine which
 * OPPORTUNISTIC containers to be killed to make room for a GUARANTEED
 * container.
 */
public interface ResourceUtilizationTracker {

  /**
   * Get the current total utilization of all the Containers running on
   * the node.
   * @return ResourceUtilization Resource Utilization.
   */
  ResourceUtilization getCurrentUtilization();

  /**
   * Get the total amount of resources allocated to running containers
   * in terms of resource utilization.
   * @return ResourceUtilization resource allocation
   */
  ResourceUtilization getTotalAllocation();

  /**
   * Get the amount of resources currently available to launch containers.
   * @return Resource resources available to launch containers
   */
  Resource getAvailableResources();

  /**
   * Get the amount of resources left un-allocated.
   * @return Resource unallocated resources
   */
  Resource getUnallocatedResources();

  /**
   * Add Container's resources to Node Utilization upon container launch.
   * @param container Container.
   */
  void containerLaunched(Container container);

  /**
   * Subtract Container's resources to Node Utilization upon container release.
   * @param container Container.
   */
  void containerReleased(Container container);
}
