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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.exceptions.YarnException;
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
   * Get the number of nodes available in the cluster.
   * @return the number of available nodes.
   */
  @Public
  @Stable
  public int getNumClusterNodes();
  
  /**
   * The main api between the ApplicationMaster and the Scheduler.
   * The ApplicationMaster is updating his future resource requirements
   * and may release containers he doens't need.
   * 
   * @param appAttemptId
   * @param ask
   * @param release
   * @param blacklistAdditions 
   * @param blacklistRemovals 
   * @return the {@link Allocation} for the application
   */
  @Public
  @Stable
  Allocation 
  allocate(ApplicationAttemptId appAttemptId, 
      List<ResourceRequest> ask,
      List<ContainerId> release, 
      List<String> blacklistAdditions, 
      List<String> blacklistRemovals);

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
   * Get a resource usage report from a given app attempt ID.
   * @param appAttemptId the id of the application attempt
   * @return resource usage report for this given attempt
   */
  @LimitedPrivate("yarn")
  @Evolving
  ApplicationResourceUsageReport getAppResourceUsageReport(
      ApplicationAttemptId appAttemptId);
  
  /**
   * Get the root queue for the scheduler.
   * @return the root queue for the scheduler.
   */
  @LimitedPrivate("yarn")
  @Evolving
  QueueMetrics getRootQueueMetrics();

  /**
   * Check if the user has permission to perform the operation.
   * If the user has {@link QueueACL#ADMINISTER_QUEUE} permission,
   * this user can view/modify the applications in this queue
   * @param callerUGI
   * @param acl
   * @param queueName
   * @return <code>true</code> if the user has the permission,
   *         <code>false</code> otherwise
   */
  boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName);
  
  /**
   * Gets the apps under a given queue
   * @param queueName the name of the queue.
   * @return a collection of app attempt ids in the given queue.
   */
  @LimitedPrivate("yarn")
  @Stable
  public List<ApplicationAttemptId> getAppsInQueue(String queueName);

  /**
   * Get the container for the given containerId.
   * @param containerId
   * @return the container for the given containerId.
   */
  @LimitedPrivate("yarn")
  @Unstable
  public RMContainer getRMContainer(ContainerId containerId);
  
  /**
   * Moves the given application to the given queue
   * @param appId
   * @param newQueue
   * @return the name of the queue the application was placed into
   * @throws YarnException if the move cannot be carried out
   */
  @LimitedPrivate("yarn")
  @Evolving
  public String moveApplication(ApplicationId appId, String newQueue)
      throws YarnException;
}
