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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;

/**
 * This interface is used by the components to talk to the
 * scheduler for allocating of resources, cleaning up resources.
 *
 */
public interface YarnScheduler extends EventHandler<SchedulerEvent> {

  /**
   * Get queue information.
   *
   * @param queueName queue name
   * @param includeChildQueues include child queues?
   * @param recursive get children queues?
   * @return queue information
   * @throws IOException an I/O exception has occurred.
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
   * Get the whole resource capacity of the cluster.
   * @return the whole resource capacity of the cluster.
   */
  @LimitedPrivate("yarn")
  @Unstable
  public Resource getClusterResource();

  /**
   * Get minimum allocatable {@link Resource}.
   * @return minimum allocatable resource
   */
  @Public
  @Stable
  public Resource getMinimumResourceCapability();
  
  /**
   * Get maximum allocatable {@link Resource} at the cluster level.
   * @return maximum allocatable resource
   */
  @Public
  @Stable
  public Resource getMaximumResourceCapability();

  /**
   * Get maximum allocatable {@link Resource} for the queue specified.
   * @param queueName queue name
   * @return maximum allocatable resource
   */
  @Public
  @Stable
  public Resource getMaximumResourceCapability(String queueName);

  @LimitedPrivate("yarn")
  @Evolving
  ResourceCalculator getResourceCalculator();

  /**
   * Get the number of nodes available in the cluster.
   * @return the number of available nodes.
   */
  @Public
  @Stable
  public int getNumClusterNodes();
  
  /**
   * The main API between the ApplicationMaster and the Scheduler.
   * The ApplicationMaster may request/update container resources,
   * number of containers, node/rack preference for allocations etc.
   * to the Scheduler.
   * @param appAttemptId the id of the application attempt.
   * @param ask the request made by an application to obtain various allocations
   * like host/rack, resource, number of containers, relaxLocality etc.,
   * see {@link ResourceRequest}.
   * @param schedulingRequests similar to ask, but with added ability to specify
   * allocation tags etc., see {@link SchedulingRequest}.
   * @param release the list of containers to be released.
   * @param blacklistAdditions places (node/rack) to be added to the blacklist.
   * @param blacklistRemovals places (node/rack) to be removed from the
   * blacklist.
   * @param updateRequests container promotion/demotion updates.
   * @return the {@link Allocation} for the application.
   */
  @Public
  @Stable
  Allocation allocate(ApplicationAttemptId appAttemptId,
      List<ResourceRequest> ask, List<SchedulingRequest> schedulingRequests,
      List<ContainerId> release, List<String> blacklistAdditions,
      List<String> blacklistRemovals, ContainerUpdates updateRequests);

  /**
   * Get node resource usage report.
   *
   * @param nodeId nodeId.
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
   * this user can view/modify the applications in this queue.
   *
   * @param callerUGI caller UserGroupInformation.
   * @param acl queue ACL.
   * @param queueName queue Name.
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
   *
   * @param containerId the given containerId.
   * @return the container for the given containerId.
   */
  @LimitedPrivate("yarn")
  @Unstable
  public RMContainer getRMContainer(ContainerId containerId);

  /**
   * Moves the given application to the given queue.
   * @param appId application Id
   * @param newQueue the given queue.
   * @return the name of the queue the application was placed into
   * @throws YarnException if the move cannot be carried out
   */
  @LimitedPrivate("yarn")
  @Evolving
  public String moveApplication(ApplicationId appId, String newQueue)
      throws YarnException;

  /**
   *
   * @param appId Application ID
   * @param newQueue Target QueueName
   * @throws YarnException if the pre-validation for move cannot be carried out
   */
  @LimitedPrivate("yarn")
  @Evolving
  public void preValidateMoveApplication(ApplicationId appId,
      String newQueue) throws YarnException;

  /**
   * Completely drain sourceQueue of applications, by moving all of them to
   * destQueue.
   *
   * @param sourceQueue sourceQueue.
   * @param destQueue destQueue.
   * @throws YarnException when yarn exception occur.
   */
  void moveAllApps(String sourceQueue, String destQueue) throws YarnException;

  /**
   * Terminate all applications in the specified queue.
   *
   * @param queueName the name of queue to be drained
   * @throws YarnException when yarn exception occur.
   */
  void killAllAppsInQueue(String queueName) throws YarnException;

  /**
   * Remove an existing queue. Implementations might limit when a queue could be
   * removed (e.g., must have zero entitlement, and no applications running, or
   * must be a leaf, etc..).
   *
   * @param queueName name of the queue to remove
   * @throws YarnException when yarn exception occur.
   */
  void removeQueue(String queueName) throws YarnException;

  /**
   * Add to the scheduler a new Queue. Implementations might limit what type of
   * queues can be dynamically added (e.g., Queue must be a leaf, must be
   * attached to existing parent, must have zero entitlement).
   *
   * @param newQueue the queue being added.
   * @throws YarnException when yarn exception occur.
   * @throws IOException when io exception occur.
   */
  void addQueue(Queue newQueue) throws YarnException, IOException;

  /**
   * This method increase the entitlement for current queue (must respect
   * invariants, e.g., no overcommit of parents, non negative, etc.).
   * Entitlement is a general term for weights in FairScheduler, capacity for
   * the CapacityScheduler, etc.
   *
   * @param queue the queue for which we change entitlement
   * @param entitlement the new entitlement for the queue (capacity,
   *              maxCapacity, etc..)
   * @throws YarnException when yarn exception occur.
   */
  void setEntitlement(String queue, QueueEntitlement entitlement)
      throws YarnException;

  /**
   * Gets the list of names for queues managed by the Reservation System.
   * @return the list of queues which support reservations
   * @throws YarnException when yarn exception occur.
   */
  public Set<String> getPlanQueues() throws YarnException;  

  /**
   * Return a collection of the resource types that are considered when
   * scheduling
   *
   * @return an EnumSet containing the resource types
   */
  public EnumSet<SchedulerResourceTypes> getSchedulingResourceTypes();

  /**
   *
   * Verify whether a submitted application priority is valid as per configured
   * Queue
   *
   * @param priorityRequestedByApp
   *          Submitted Application priority.
   * @param user
   *          User who submitted the Application
   * @param queuePath
   *          Name of the Queue
   * @param applicationId
   *          Application ID
   * @return Updated Priority from scheduler
   * @throws YarnException when yarn exception occur.
   */
  public Priority checkAndGetApplicationPriority(Priority priorityRequestedByApp,
      UserGroupInformation user, String queuePath, ApplicationId applicationId)
      throws YarnException;

  /**
   *
   * Change application priority of a submitted application at runtime
   *
   * @param newPriority Submitted Application priority.
   *
   * @param applicationId Application ID
   *
   * @param future Sets any type of exception happened from StateStore
   * @param user who submitted the application
   *
   * @return updated priority
   * @throws YarnException when yarn exception occur.
   */
  public Priority updateApplicationPriority(Priority newPriority,
      ApplicationId applicationId, SettableFuture<Object> future,
      UserGroupInformation user) throws YarnException;

  /**
   *
   * Get previous attempts' live containers for work-preserving AM restart.
   *
   * @param appAttemptId the id of the application attempt
   *
   * @return list of live containers for the given attempt
   */
  List<Container> getTransferredContainers(ApplicationAttemptId appAttemptId);

  /**
   * Set the cluster max priority.
   * 
   * @param conf Configuration.
   * @throws YarnException when yarn exception occur.
   */
  void setClusterMaxPriority(Configuration conf) throws YarnException;

  /**
   * Get pending resource request for specified application attempt.
   *
   * @param attemptId the id of the application attempt
   * @return pending resource requests.
   */
  List<ResourceRequest> getPendingResourceRequestsForAttempt(
      ApplicationAttemptId attemptId);

  /**
   * Get pending scheduling request for specified application attempt.
   *
   * @param attemptId the id of the application attempt
   *
   * @return pending scheduling requests
   */
  List<SchedulingRequest> getPendingSchedulingRequestsForAttempt(
      ApplicationAttemptId attemptId);

  /**
   * Get cluster max priority.
   * 
   * @return maximum priority of cluster
   */
  Priority getMaxClusterLevelAppPriority();

  /**
   * Get SchedulerNode corresponds to nodeId.
   *
   * @param nodeId the node id of RMNode
   *
   * @return SchedulerNode corresponds to nodeId
   */
  SchedulerNode getSchedulerNode(NodeId nodeId);

  /**
   * Normalize a resource request using scheduler level maximum resource or
   * queue based maximum resource.
   *
   * @param requestedResource the resource to be normalized
   * @param maxResourceCapability Maximum container allocation value, if null or
   *          empty scheduler level maximum container allocation value will be
   *          used
   * @return the normalized resource
   */
  Resource getNormalizedResource(Resource requestedResource,
      Resource maxResourceCapability);

  /**
   * Verify whether a submitted application lifetime is valid as per configured
   * Queue lifetime.
   * @param queueName Name of the Queue
   * @param lifetime configured application lifetime
   * @return valid lifetime as per queue
   */
  @Public
  @Evolving
  long checkAndGetApplicationLifetime(String queueName, long lifetime);

  /**
   * Get maximum lifetime for a queue.
   * @param queueName to get lifetime
   * @return maximum lifetime in seconds
   */
  @Public
  @Evolving
  long getMaximumApplicationLifetime(String queueName);
}
