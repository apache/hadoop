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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.Container;

import java.util.List;

/**
 * Object used by the Application Master when distributed scheduling is enabled,
 * in order to forward the {@link AllocateRequest} for GUARANTEED containers to
 * the Resource Manager, and to notify the Resource Manager about the allocation
 * of OPPORTUNISTIC containers through the Distributed Scheduler.
 */
@Public
@Evolving
public abstract class DistributedSchedulingAllocateRequest {

  /**
   * Get the underlying <code>AllocateRequest</code> object.
   * @return Allocate request
   */
  @Public
  @Evolving
  public abstract AllocateRequest getAllocateRequest();

  /**
   * Set the underlying <code>AllocateRequest</code> object.
   * @param allocateRequest  Allocate request
   */
  @Public
  @Evolving
  public abstract void  setAllocateRequest(AllocateRequest allocateRequest);

  /**
   * Get the list of <em>newly allocated</em> <code>Container</code> by the
   * Distributed Scheduling component on the NodeManager.
   * @return list of <em>newly allocated</em> <code>Container</code>
   */
  @Public
  @Evolving
  public abstract List<Container> getAllocatedContainers();

  /**
   * Set the list of <em>newly allocated</em> <code>Container</code> by the
   * Distributed Scheduling component on the NodeManager.
   * @param containers list of <em>newly allocated</em> <code>Container</code>
   */
  @Public
  @Evolving
  public abstract void setAllocatedContainers(List<Container> containers);
}
