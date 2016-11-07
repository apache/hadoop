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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedContainerChangeRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.PlacementSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SimplePlacementSet;

/**
 * <code>CSQueue</code> represents a node in the tree of 
 * hierarchical queues in the {@link CapacityScheduler}.
 */
@Stable
@Private
public interface CSQueue 
extends org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue {
  /**
   * Get the parent <code>Queue</code>.
   * @return the parent queue
   */
  public CSQueue getParent();

  /**
   * Set the parent <code>Queue</code>.
   * @param newParentQueue new parent queue
   */
  public void setParent(CSQueue newParentQueue);

  /**
   * Get the queue name.
   * @return the queue name
   */
  public String getQueueName();

  /**
   * Get the full name of the queue, including the heirarchy.
   * @return the full name of the queue
   */
  public String getQueuePath();

  public PrivilegedEntity getPrivilegedEntity();

  /**
   * Get the configured <em>capacity</em> of the queue.
   * @return configured queue capacity
   */
  public float getCapacity();

  /**
   * Get capacity of the parent of the queue as a function of the 
   * cumulative capacity in the cluster.
   * @return capacity of the parent of the queue as a function of the 
   *         cumulative capacity in the cluster
   */
  public float getAbsoluteCapacity();
  
  /**
   * Get the configured maximum-capacity of the queue. 
   * @return the configured maximum-capacity of the queue
   */
  public float getMaximumCapacity();
  
  /**
   * Get maximum-capacity of the queue as a funciton of the cumulative capacity
   * of the cluster.
   * @return maximum-capacity of the queue as a funciton of the cumulative capacity
   *         of the cluster
   */
  public float getAbsoluteMaximumCapacity();
  
  /**
   * Get the current absolute used capacity of the queue
   * relative to the entire cluster.
   * @return queue absolute used capacity
   */
  public float getAbsoluteUsedCapacity();

  /**
   * Set used capacity of the queue.
   * @param usedCapacity
   *          used capacity of the queue
   */
  public void setUsedCapacity(float usedCapacity);

  /**
   * Set absolute used capacity of the queue.
   * @param absUsedCapacity
   *          absolute used capacity of the queue
   */
  public void setAbsoluteUsedCapacity(float absUsedCapacity);

  /**
   * Get the current used capacity of nodes without label(s) of the queue
   * and it's children (if any).
   * @return queue used capacity
   */
  public float getUsedCapacity();

  /**
   * Get the currently utilized resources which allocated at nodes without any
   * labels in the cluster by the queue and children (if any).
   * 
   * @return used resources by the queue and it's children
   */
  public Resource getUsedResources();
  
  /**
   * Get the current run-state of the queue
   * @return current run-state
   */
  public QueueState getState();
  
  /**
   * Get child queues
   * @return child queues
   */
  public List<CSQueue> getChildQueues();
  
  /**
   * Check if the <code>user</code> has permission to perform the operation
   * @param acl ACL
   * @param user user
   * @return <code>true</code> if the user has the permission, 
   *         <code>false</code> otherwise
   */
  public boolean hasAccess(QueueACL acl, UserGroupInformation user);
  
  /**
   * Submit a new application to the queue.
   * @param applicationId the applicationId of the application being submitted
   * @param user user who submitted the application
   * @param queue queue to which the application is submitted
   */
  public void submitApplication(ApplicationId applicationId, String user,
      String queue) throws AccessControlException;

  /**
   * Submit an application attempt to the queue.
   */
  public void submitApplicationAttempt(FiCaSchedulerApp application,
      String userName);

  /**
   * An application submitted to this queue has finished.
   * @param applicationId
   * @param user user who submitted the application
   */
  public void finishApplication(ApplicationId applicationId, String user);

  /**
   * An application attempt submitted to this queue has finished.
   */
  public void finishApplicationAttempt(FiCaSchedulerApp application,
      String queue);

  /**
   * Assign containers to applications in the queue or it's children (if any).
   * @param clusterResource the resource of the cluster.
   * @param ps {@link PlacementSet} of nodes which resources are available
   * @param resourceLimits how much overall resource of this queue can use. 
   * @param schedulingMode Type of exclusive check when assign container on a 
   * NodeManager, see {@link SchedulingMode}.
   * @return the assignment
   */
  public CSAssignment assignContainers(Resource clusterResource,
      PlacementSet<FiCaSchedulerNode> ps, ResourceLimits resourceLimits,
      SchedulingMode schedulingMode);
  
  /**
   * A container assigned to the queue has completed.
   * @param clusterResource the resource of the cluster
   * @param application application to which the container was assigned
   * @param node node on which the container completed
   * @param container completed container, 
   *                  <code>null</code> if it was just a reservation
   * @param containerStatus <code>ContainerStatus</code> for the completed 
   *                        container
   * @param childQueue <code>CSQueue</code> to reinsert in childQueues 
   * @param event event to be sent to the container
   * @param sortQueues indicates whether it should re-sort the queues
   */
  public void completedContainer(Resource clusterResource,
      FiCaSchedulerApp application, FiCaSchedulerNode node, 
      RMContainer container, ContainerStatus containerStatus, 
      RMContainerEventType event, CSQueue childQueue,
      boolean sortQueues);

  /**
   * We have a reserved increased container in the queue, we need to unreserve
   * it. Since we just want to cancel the reserved increase request instead of
   * stop the container, we shouldn't call completedContainer for such purpose.
   */
  public void unreserveIncreasedContainer(Resource clusterResource,
      FiCaSchedulerApp app, FiCaSchedulerNode node, RMContainer rmContainer);

  /**
   * Get the number of applications in the queue.
   * @return number of applications
   */
  public int getNumApplications();

  
  /**
   * Reinitialize the queue.
   * @param newlyParsedQueue new queue to re-initalize from
   * @param clusterResource resources in the cluster
   */
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource) 
  throws IOException;

   /**
   * Update the cluster resource for queues as we add/remove nodes
   * @param clusterResource the current cluster resource
   * @param resourceLimits the current ResourceLimits
   */
  public void updateClusterResource(Resource clusterResource,
      ResourceLimits resourceLimits);
  
  /**
   * Get the {@link ActiveUsersManager} for the queue.
   * @return the <code>ActiveUsersManager</code> for the queue
   */
  public ActiveUsersManager getActiveUsersManager();
  
  /**
   * Adds all applications in the queue and its subqueues to the given collection.
   * @param apps the collection to add the applications to
   */
  public void collectSchedulerApplications(Collection<ApplicationAttemptId> apps);

  /**
  * Detach a container from this queue
  * @param clusterResource the current cluster resource
  * @param application application to which the container was assigned
  * @param container the container to detach
  */
  public void detachContainer(Resource clusterResource,
               FiCaSchedulerApp application, RMContainer container);

  /**
   * Attach a container to this queue
   * @param clusterResource the current cluster resource
   * @param application application to which the container was assigned
   * @param container the container to attach
   */
  public void attachContainer(Resource clusterResource,
               FiCaSchedulerApp application, RMContainer container);

  /**
   * Check whether <em>disable_preemption</em> property is set for this queue
   * @return true if <em>disable_preemption</em> is set, false if not
   */
  public boolean getPreemptionDisabled();
  
  /**
   * Get QueueCapacities of this queue
   * @return queueCapacities
   */
  public QueueCapacities getQueueCapacities();
  
  /**
   * Get ResourceUsage of this queue
   * @return resourceUsage
   */
  public ResourceUsage getQueueResourceUsage();

  /**
   * When partition of node updated, we will update queue's resource usage if it
   * has container(s) running on that.
   */
  public void incUsedResource(String nodePartition, Resource resourceToInc,
      SchedulerApplicationAttempt application);

  /**
   * When partition of node updated, we will update queue's resource usage if it
   * has container(s) running on that.
   */
  public void decUsedResource(String nodePartition, Resource resourceToDec,
      SchedulerApplicationAttempt application);

  /**
   * When an outstanding resource is fulfilled or canceled, calling this will
   * decrease pending resource in a queue.
   *
   * @param nodeLabel
   *          asked by application
   * @param resourceToDec
   *          new resource asked
   */
  public void decPendingResource(String nodeLabel, Resource resourceToDec);
  
  /**
   * Decrease container resource in the queue
   */
  public void decreaseContainer(Resource clusterResource,
      SchedContainerChangeRequest decreaseRequest,
      FiCaSchedulerApp app) throws InvalidResourceRequestException;

  /**
   * Get valid Node Labels for this queue
   * @return valid node labels
   */
  public Set<String> getNodeLabelsForQueue();

  @VisibleForTesting
  CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits resourceLimits,
      SchedulingMode schedulingMode);

  boolean accept(Resource cluster,
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request);

  void apply(Resource cluster,
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request);
}
