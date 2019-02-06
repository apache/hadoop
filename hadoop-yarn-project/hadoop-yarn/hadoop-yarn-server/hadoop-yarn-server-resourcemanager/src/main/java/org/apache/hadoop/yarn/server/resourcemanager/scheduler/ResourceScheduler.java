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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;

/**
 * This interface is the one implemented by the schedulers. It mainly extends 
 * {@link YarnScheduler}. 
 *
 */
@LimitedPrivate("yarn")
@Evolving
public interface ResourceScheduler extends YarnScheduler, Recoverable {

  /**
   * Set RMContext for <code>ResourceScheduler</code>.
   * This method should be called immediately after instantiating
   * a scheduler once.
   * @param rmContext created by ResourceManager
   */
  void setRMContext(RMContext rmContext);

  /**
   * Re-initialize the <code>ResourceScheduler</code>.
   * @param conf configuration
   * @throws IOException
   */
  void reinitialize(Configuration conf, RMContext rmContext) throws IOException;

  /**
   * Get the {@link NodeId} available in the cluster by resource name.
   * @param resourceName resource name
   * @return the number of available {@link NodeId} by resource name.
   */
  List<NodeId> getNodeIds(String resourceName);

  /**
   * Attempts to allocate a SchedulerRequest on a Node.
   * NOTE: This ignores the numAllocations in the resource sizing and tries
   *       to allocate a SINGLE container only.
   * @param appAttempt ApplicationAttempt.
   * @param schedulingRequest SchedulingRequest.
   * @param schedulerNode SchedulerNode.
   * @return true if proposal was accepted.
   */
  boolean attemptAllocationOnNode(SchedulerApplicationAttempt appAttempt,
      SchedulingRequest schedulingRequest, SchedulerNode schedulerNode);

  /**
   * Reset scheduler metrics.
   */
  void resetSchedulerMetrics();
}
