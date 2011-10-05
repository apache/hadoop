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
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;

/**
 * This interface is used by the components to talk to the
 * scheduler for allocating of resources, cleaning up resources.
 *
 */
public interface YarnScheduler extends EventHandler<SchedulerEvent> {

  /**
   * Get queue information
   * @param queueName queue name
   * @param includeChildQueues include child queues?
   * @param recursive get children queues?
   * @return queue information
   * @throws IOException
   */
  @Public
  @Stable
  public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues,
      boolean recursive) throws IOException;

  /**
   * Get acls for queues for current user.
   * @return acls for queues for current user
   */
  @Public
  @Stable
  public List<QueueUserACLInfo> getQueueUserAclInfo();
  
  /**
   * Get minimum allocatable {@link Resource}.
   * @return minimum allocatable resource
   */
  @Public
  @Stable
  public Resource getMinimumResourceCapability();
  
  /**
   * Get maximum allocatable {@link Resource}.
   * @return maximum allocatable resource
   */
  @Public
  @Stable
  public Resource getMaximumResourceCapability();

  /**
   * The main api between the ApplicationMaster and the Scheduler.
   * The ApplicationMaster is updating his future resource requirements
   * and may release containers he doens't need.
   * 
   * @param appAttemptId
   * @param ask
   * @param release
   * @return the {@link Allocation} for the application
   */
  @Public
  @Stable
  Allocation 
  allocate(ApplicationAttemptId appAttemptId, 
      List<ResourceRequest> ask,
      List<ContainerId> release);

  /**
   * Get node resource usage report.
   * @param nodeId
   * @return the {@link SchedulerNodeReport} for the node or null
   * if nodeId does not point to a defined node.
   */
  @LimitedPrivate("yarn")
  @Stable
  public SchedulerNodeReport getNodeReport(NodeId nodeId);
  
  /**
   * Get the Scheduler app for a given app attempt Id.
   * @param appAttemptId the id of the application attempt
   * @return SchedulerApp for this given attempt.
   */
  @LimitedPrivate("yarn")
  @Stable
  SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId appAttemptId);

  /**
   * Get the root queue for the scheduler.
   * @return the root queue for the scheduler.
   */
  @LimitedPrivate("yarn")
  @Evolving
  QueueMetrics getRootQueueMetrics();
}
