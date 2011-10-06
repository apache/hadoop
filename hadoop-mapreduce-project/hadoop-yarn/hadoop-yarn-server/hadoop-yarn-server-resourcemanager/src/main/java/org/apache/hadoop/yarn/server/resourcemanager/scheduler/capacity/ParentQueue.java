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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

@Private
@Evolving
public class ParentQueue implements CSQueue {

  private static final Log LOG = LogFactory.getLog(ParentQueue.class);

  private final CSQueue parent;
  private final String queueName;
  
  private float capacity;
  private float maximumCapacity;
  private float absoluteCapacity;
  private float absoluteMaxCapacity;

  private float usedCapacity = 0.0f;
  private float utilization = 0.0f;

  private final Set<CSQueue> childQueues;
  private final Comparator<CSQueue> queueComparator;
  
  private Resource usedResources = 
    Resources.createResource(0);
  
  private final boolean rootQueue;
  
  private final Resource minimumAllocation;

  private volatile int numApplications;
  private volatile int numContainers;

  private QueueState state;

  private final QueueMetrics metrics;

  private QueueInfo queueInfo; 

  private Map<QueueACL, AccessControlList> acls = 
    new HashMap<QueueACL, AccessControlList>();

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  public ParentQueue(CapacitySchedulerContext cs, 
      String queueName, Comparator<CSQueue> comparator, CSQueue parent, CSQueue old) {
    minimumAllocation = cs.getMinimumResourceCapability();
    
    this.parent = parent;
    this.queueName = queueName;
    this.rootQueue = (parent == null);

    // must be called after parent and queueName is set
    this.metrics = old != null ? old.getMetrics() :
        QueueMetrics.forQueue(getQueuePath(), parent,
        cs.getConfiguration().getEnableUserMetrics());

    int rawCapacity = cs.getConfiguration().getCapacity(getQueuePath());

    if (rootQueue &&
        (rawCapacity != CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE)) {
      throw new IllegalArgumentException("Illegal " +
          "capacity of " + rawCapacity + " for queue " + queueName +
          ". Must be " + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE);
    }

    float capacity = (float) rawCapacity / 100;

    float parentAbsoluteCapacity = 
      (parent == null) ? 1.0f : parent.getAbsoluteCapacity();
    float absoluteCapacity = parentAbsoluteCapacity * capacity; 

    float maximumCapacity = 
      cs.getConfiguration().getMaximumCapacity(getQueuePath());
    float absoluteMaxCapacity = 
      (maximumCapacity == CapacitySchedulerConfiguration.UNDEFINED) ? 
          1000000000f :  (parentAbsoluteCapacity * maximumCapacity) / 100;
    
    QueueState state = cs.getConfiguration().getState(getQueuePath());

    Map<QueueACL, AccessControlList> acls = 
      cs.getConfiguration().getAcls(getQueuePath());
    
    this.queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    this.queueInfo.setQueueName(queueName);
    this.queueInfo.setChildQueues(new ArrayList<QueueInfo>());

    setupQueueConfigs(capacity, absoluteCapacity, 
        maximumCapacity, absoluteMaxCapacity, state, acls);
    
    this.queueComparator = comparator;
    this.childQueues = new TreeSet<CSQueue>(queueComparator);

    LOG.info("Initialized parent-queue " + queueName + 
        " name=" + queueName + 
        ", fullname=" + getQueuePath()); 
  }

  private synchronized void setupQueueConfigs(
          float capacity, float absoluteCapacity, 
          float maximumCapacity, float absoluteMaxCapacity,
          QueueState state, Map<QueueACL, AccessControlList> acls
  ) {
    this.capacity = capacity;
    this.absoluteCapacity = absoluteCapacity;
    this.maximumCapacity = maximumCapacity;
    this.absoluteMaxCapacity = absoluteMaxCapacity;

    this.state = state;

    this.acls = acls;
    
    this.queueInfo.setCapacity(capacity);
    this.queueInfo.setMaximumCapacity(maximumCapacity);
    this.queueInfo.setQueueState(state);

    StringBuilder aclsString = new StringBuilder();
    for (Map.Entry<QueueACL, AccessControlList> e : acls.entrySet()) {
      aclsString.append(e.getKey() + ":" + e.getValue().getAclString());
    }

    LOG.info(queueName +
        ", capacity=" + capacity +
        ", asboluteCapacity=" + absoluteCapacity +
        ", maxCapacity=" + maximumCapacity +
        ", asboluteMaxCapacity=" + absoluteMaxCapacity + 
        ", state=" + state +
        ", acls=" + aclsString);
  }

  private static float PRECISION = 0.005f; // 0.05% precision
  void setChildQueues(Collection<CSQueue> childQueues) {
    
    // Validate
    float childCapacities = 0;
    for (CSQueue queue : childQueues) {
      childCapacities += queue.getCapacity();
    }
    float delta = Math.abs(1.0f - childCapacities);  // crude way to check
    if (delta > PRECISION) {
      throw new IllegalArgumentException("Illegal" +
      		" capacity of " + childCapacities + 
      		" for children of queue " + queueName);
    }
    
    this.childQueues.clear();
    this.childQueues.addAll(childQueues);
    if (LOG.isDebugEnabled()) {
      LOG.debug("setChildQueues: " + getChildQueuesToPrint());
    }
  }
  
  @Override
  public CSQueue getParent() {
    return parent;
  }

  @Override
  public String getQueueName() {
    return queueName;
  }

  @Override
  public String getQueuePath() {
    String parentPath = ((parent == null) ? "" : (parent.getQueuePath() + "."));
    return parentPath + getQueueName();
  }

  @Override
  public synchronized float getCapacity() {
    return capacity;
  }

  @Override
  public synchronized float getAbsoluteCapacity() {
    return absoluteCapacity;
  }

  @Override
  public float getAbsoluteMaximumCapacity() {
    return 0;
  }

  @Override
  public float getMaximumCapacity() {
    return 0;
  }

  @Override
  public synchronized float getUsedCapacity() {
    return usedCapacity;
  }

  @Override
  public synchronized Resource getUsedResources() {
    return usedResources;
  }
  
  @Override
  public synchronized float getUtilization() {
    return utilization;
  }

  @Override
  public synchronized List<CSQueue> getChildQueues() {
    return new ArrayList<CSQueue>(childQueues);
  }

  public synchronized int getNumContainers() {
    return numContainers;
  }
  
  public synchronized int getNumApplications() {
    return numApplications;
  }

  @Override
  public synchronized QueueState getState() {
    return state;
  }

  @Override
  public synchronized Map<QueueACL, AccessControlList> getQueueAcls() {
    return new HashMap<QueueACL, AccessControlList>(acls);
  }

  @Override
  public synchronized QueueInfo getQueueInfo( 
      boolean includeChildQueues, boolean recursive) {
    queueInfo.setCurrentCapacity(usedCapacity);

    List<QueueInfo> childQueuesInfo = new ArrayList<QueueInfo>();
    if (includeChildQueues) {
      for (CSQueue child : childQueues) {
        // Get queue information recursively?
        childQueuesInfo.add(
            child.getQueueInfo(recursive, recursive));
      }
    }
    queueInfo.setChildQueues(childQueuesInfo);
    
    return queueInfo;
  }

  private synchronized QueueUserACLInfo getUserAclInfo(
      UserGroupInformation user) {
    QueueUserACLInfo userAclInfo = 
      recordFactory.newRecordInstance(QueueUserACLInfo.class);
    List<QueueACL> operations = new ArrayList<QueueACL>();
    for (Map.Entry<QueueACL, AccessControlList> e : acls.entrySet()) {
      QueueACL operation = e.getKey();
      AccessControlList acl = e.getValue();
      
      if (acl.isUserAllowed(user)) {
        operations.add(operation);
      }
    }
    
    userAclInfo.setQueueName(getQueueName());
    userAclInfo.setUserAcls(operations);
    return userAclInfo;
  }
  
  @Override
  public synchronized List<QueueUserACLInfo> getQueueUserAclInfo(
      UserGroupInformation user) {
    List<QueueUserACLInfo> userAcls = new ArrayList<QueueUserACLInfo>();
    
    // Add parent queue acls
    userAcls.add(getUserAclInfo(user));
    
    // Add children queue acls
    for (CSQueue child : childQueues) {
      userAcls.addAll(child.getQueueUserAclInfo(user));
    }
    return userAcls;
  }

  public String toString() {
    return queueName + ":" + capacity + ":" + absoluteCapacity + ":" + 
      getUsedCapacity() + ":" + getUtilization() + ":" + 
      getNumApplications() + ":" + getNumContainers() + ":" + 
      childQueues.size() + " child-queues";
  }
  
  @Override
  public synchronized void reinitialize(CSQueue queue, Resource clusterResource)
  throws IOException {
    // Sanity check
    if (!(queue instanceof ParentQueue) ||
        !queue.getQueuePath().equals(getQueuePath())) {
      throw new IOException("Trying to reinitialize " + getQueuePath() +
          " from " + queue.getQueuePath());
    }

    ParentQueue parentQueue = (ParentQueue)queue;

    // Re-configure existing child queues and add new ones
    // The CS has already checked to ensure all existing child queues are present!
    Map<String, CSQueue> currentChildQueues = getQueues(childQueues);
    Map<String, CSQueue> newChildQueues = getQueues(parentQueue.childQueues);
    for (Map.Entry<String, CSQueue> e : newChildQueues.entrySet()) {
      String newChildQueueName = e.getKey();
      CSQueue newChildQueue = e.getValue();

      CSQueue childQueue = currentChildQueues.get(newChildQueueName);
      if (childQueue != null){
        childQueue.reinitialize(newChildQueue, clusterResource);
        LOG.info(getQueueName() + ": re-configured queue: " + childQueue);
      } else {
        currentChildQueues.put(newChildQueueName, newChildQueue);
        LOG.info(getQueueName() + ": added new child queue: " + newChildQueue);
      }
    }

    // Re-sort all queues
    childQueues.clear();
    childQueues.addAll(currentChildQueues.values());

    // Set new configs
    setupQueueConfigs(parentQueue.capacity, parentQueue.absoluteCapacity,
        parentQueue.maximumCapacity, parentQueue.absoluteMaxCapacity,
        parentQueue.state, parentQueue.acls);

    // Update
    updateResource(clusterResource);
  }

  Map<String, CSQueue> getQueues(Set<CSQueue> queues) {
    Map<String, CSQueue> queuesMap = new HashMap<String, CSQueue>();
    for (CSQueue queue : queues) {
      queuesMap.put(queue.getQueueName(), queue);
    }
    return queuesMap;
  }
  
  @Override
  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    synchronized (this) {
      if (acls.get(acl).isUserAllowed(user)) {
        return true;
      }
    }
    
    if (parent != null) {
      return parent.hasAccess(acl, user);
    }
    
    return false;
  }

  @Override
  public void submitApplication(SchedulerApp application, String user,
      String queue) throws AccessControlException {
    
    synchronized (this) {
      // Sanity check
      if (queue.equals(queueName)) {
        throw new AccessControlException("Cannot submit application " +
            "to non-leaf queue: " + queueName);
      }
      
      if (state != QueueState.RUNNING) {
        throw new AccessControlException("Queue " + getQueuePath() +
            " is STOPPED. Cannot accept submission of application: " +
            application.getApplicationId());
      }

      addApplication(application, user);
    }
    
    // Inform the parent queue
    if (parent != null) {
      try {
        parent.submitApplication(application, user, queue);
      } catch (AccessControlException ace) {
        LOG.info("Failed to submit application to parent-queue: " + 
            parent.getQueuePath(), ace);
        removeApplication(application, user);
        throw ace;
      }
    }
  }

  private synchronized void addApplication(SchedulerApp application, 
      String user) {
  
    ++numApplications;

    LOG.info("Application added -" +
        " appId: " + application.getApplicationId() + 
        " user: " + user + 
        " leaf-queue of parent: " + getQueueName() + 
        " #applications: " + getNumApplications());
  }
  
  @Override
  public void finishApplication(SchedulerApp application, String queue) {
    
    synchronized (this) {
      removeApplication(application, application.getUser());
    }
    
    // Inform the parent queue
    if (parent != null) {
      parent.finishApplication(application, queue);
    }
  }

  public synchronized void removeApplication(SchedulerApp application, 
      String user) {
    
    --numApplications;

    LOG.info("Application removed -" +
        " appId: " + application.getApplicationId() + 
        " user: " + user + 
        " leaf-queue of parent: " + getQueueName() + 
        " #applications: " + getNumApplications());
  }
  
  synchronized void setUsedCapacity(float usedCapacity) {
    this.usedCapacity = usedCapacity;
  }
  
  synchronized void setUtilization(float utilization) {
    this.utilization = utilization;
  }

  /**
   * Set maximum capacity - used only for testing.
   * @param maximumCapacity new max capacity
   */
  synchronized void setMaxCapacity(float maximumCapacity) {
    this.maximumCapacity = maximumCapacity;
    float parentAbsoluteCapacity = 
        (rootQueue) ? 100.0f : parent.getAbsoluteCapacity();
    this.absoluteMaxCapacity = 
      (maximumCapacity == CapacitySchedulerConfiguration.UNDEFINED) ? 
          Float.MAX_VALUE : 
          (parentAbsoluteCapacity * maximumCapacity);
  }

  @Override
  public synchronized Resource assignContainers(
      Resource clusterResource, SchedulerNode node) {
    Resource assigned = Resources.createResource(0);

    while (canAssign(node)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to assign containers to child-queue of "
          + getQueueName());
      }
      
      // Are we over maximum-capacity for this queue?
      if (!assignToQueue(clusterResource)) {
        break;
      }
      
      // Schedule
      Resource assignedToChild = 
          assignContainersToChildQueues(clusterResource, node);
      
      // Done if no child-queue assigned anything
      if (Resources.greaterThan(assignedToChild, Resources.none())) {
        // Track resource utilization for the parent-queue
        allocateResource(clusterResource, assignedToChild);
        
        // Track resource utilization in this pass of the scheduler
        Resources.addTo(assigned, assignedToChild);
        
        LOG.info("assignedContainer" +
            " queue=" + getQueueName() + 
            " util=" + getUtilization() + 
            " used=" + usedResources + 
            " cluster=" + clusterResource);

      } else {
        break;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("ParentQ=" + getQueueName()
          + " assignedSoFarInThisIteration=" + assigned
          + " utilization=" + getUtilization());
      }

      // Do not assign more than one container if this isn't the root queue
      if (!rootQueue) {
        break;
      }
    } 
    
    return assigned;
  }

  private synchronized boolean assignToQueue(Resource clusterResource) {
    // Check how of the cluster's absolute capacity we are currently using...
    float currentCapacity = 
      (float)(usedResources.getMemory()) / clusterResource.getMemory();
    if (currentCapacity >= absoluteMaxCapacity) {
      LOG.info(getQueueName() + 
          " used=" + usedResources.getMemory() + 
          " current-capacity (" + currentCapacity + ") " +
          " >= max-capacity (" + absoluteMaxCapacity + ")");
      return false;
    }
    return true;

  }
  
  private boolean canAssign(SchedulerNode node) {
    return (node.getReservedContainer() == null) && 
        Resources.greaterThanOrEqual(node.getAvailableResource(), 
                                     minimumAllocation);
  }
  
  synchronized Resource assignContainersToChildQueues(Resource cluster, 
      SchedulerNode node) {
    Resource assigned = Resources.createResource(0);
    
    printChildQueues();

    // Try to assign to most 'under-served' sub-queue
    for (Iterator<CSQueue> iter=childQueues.iterator(); iter.hasNext();) {
      CSQueue childQueue = iter.next();
      if(LOG.isDebugEnabled()) {
        LOG.debug("Trying to assign to queue: " + childQueue.getQueuePath()
          + " stats: " + childQueue);
      }
      assigned = childQueue.assignContainers(cluster, node);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Assignedto queue: " + childQueue.getQueuePath()
          + " stats: " + childQueue + " --> " + assigned.getMemory());
      }

      // If we do assign, remove the queue and re-insert in-order to re-sort
      if (Resources.greaterThan(assigned, Resources.none())) {
        // Remove and re-insert to sort
        iter.remove();
        LOG.info("Re-sorting queues since queue: " + childQueue.getQueuePath() + 
            " stats: " + childQueue);
        childQueues.add(childQueue);
        printChildQueues();
        break;
      }
    }
    
    return assigned;
  }

  String getChildQueuesToPrint() {
    StringBuilder sb = new StringBuilder();
    for (CSQueue q : childQueues) {
      sb.append(q.getQueuePath() + "(" + q.getUtilization() + "), ");
    }
    return sb.toString();
  }
  void printChildQueues() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("printChildQueues - queue: " + getQueuePath()
        + " child-queues: " + getChildQueuesToPrint());
    }
  }
  
  @Override
  public void completedContainer(Resource clusterResource,
      SchedulerApp application, SchedulerNode node, 
      RMContainer rmContainer, ContainerStatus containerStatus, RMContainerEventType event) {
    if (application != null) {
      // Careful! Locking order is important!
      // Book keeping
      synchronized (this) {
        releaseResource(clusterResource, 
            rmContainer.getContainer().getResource());

        LOG.info("completedContainer" +
            " queue=" + getQueueName() + 
            " util=" + getUtilization() + 
            " used=" + usedResources + 
            " cluster=" + clusterResource);
      }

      // Inform the parent
      if (parent != null) {
        parent.completedContainer(clusterResource, application, 
            node, rmContainer, null, event);
      }    
    }
  }
  
  synchronized void allocateResource(Resource clusterResource, 
      Resource resource) {
    Resources.addTo(usedResources, resource);
    updateResource(clusterResource);
    ++numContainers;
  }
  
  synchronized void releaseResource(Resource clusterResource, 
      Resource resource) {
    Resources.subtractFrom(usedResources, resource);
    updateResource(clusterResource);
    --numContainers;
  }

  @Override
  public synchronized void updateClusterResource(Resource clusterResource) {
    // Update all children
    for (CSQueue childQueue : childQueues) {
      childQueue.updateClusterResource(clusterResource);
    }
  }
  
  private synchronized void updateResource(Resource clusterResource) {
    float queueLimit = clusterResource.getMemory() * absoluteCapacity; 
    setUtilization(usedResources.getMemory() / queueLimit);
    setUsedCapacity(
        usedResources.getMemory() / (clusterResource.getMemory() * capacity));
    
    Resource resourceLimit = 
      Resources.createResource((int)queueLimit);
    metrics.setAvailableResourcesToQueue(
        Resources.subtractFrom(resourceLimit, usedResources));
  }

  @Override
  public QueueMetrics getMetrics() {
    return metrics;
  }

  
  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApp application, Container container) {
    // Careful! Locking order is important! 
    synchronized (this) {
      allocateResource(clusterResource, container.getResource());
    }
    if (parent != null) {
      parent.recoverContainer(clusterResource, application, container);
    }
  }
}
