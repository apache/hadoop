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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedContainerChangeRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@Private
@Evolving
public class ParentQueue extends AbstractCSQueue {

  private static final Log LOG = LogFactory.getLog(ParentQueue.class);

  protected final Set<CSQueue> childQueues;  
  private final boolean rootQueue;
  final Comparator<CSQueue> nonPartitionedQueueComparator;
  final PartitionedQueueComparator partitionQueueComparator;
  volatile int numApplications;
  private final CapacitySchedulerContext scheduler;
  private boolean needToResortQueuesAtNextAllocation = false;

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  public ParentQueue(CapacitySchedulerContext cs, 
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);
    this.scheduler = cs;
    this.nonPartitionedQueueComparator = cs.getNonPartitionedQueueComparator();
    this.partitionQueueComparator = cs.getPartitionedQueueComparator();

    this.rootQueue = (parent == null);

    float rawCapacity = cs.getConfiguration().getNonLabeledQueueCapacity(getQueuePath());

    if (rootQueue &&
        (rawCapacity != CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE)) {
      throw new IllegalArgumentException("Illegal " +
          "capacity of " + rawCapacity + " for queue " + queueName +
          ". Must be " + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE);
    }
    
    this.childQueues = new TreeSet<CSQueue>(nonPartitionedQueueComparator);
    
    setupQueueConfigs(cs.getClusterResource());

    LOG.info("Initialized parent-queue " + queueName + 
        " name=" + queueName + 
        ", fullname=" + getQueuePath()); 
  }

  synchronized void setupQueueConfigs(Resource clusterResource)
      throws IOException {
    super.setupQueueConfigs(clusterResource);
    StringBuilder aclsString = new StringBuilder();
    for (Map.Entry<AccessType, AccessControlList> e : acls.entrySet()) {
      aclsString.append(e.getKey() + ":" + e.getValue().getAclString());
    }

    StringBuilder labelStrBuilder = new StringBuilder(); 
    if (accessibleLabels != null) {
      for (String s : accessibleLabels) {
        labelStrBuilder.append(s);
        labelStrBuilder.append(",");
      }
    }

    LOG.info(queueName +
        ", capacity=" + this.queueCapacities.getCapacity() +
        ", asboluteCapacity=" + this.queueCapacities.getAbsoluteCapacity() +
        ", maxCapacity=" + this.queueCapacities.getMaximumCapacity() +
        ", asboluteMaxCapacity=" + this.queueCapacities.getAbsoluteMaximumCapacity() + 
        ", state=" + state +
        ", acls=" + aclsString + 
        ", labels=" + labelStrBuilder.toString() + "\n" +
        ", reservationsContinueLooking=" + reservationsContinueLooking);
  }

  private static float PRECISION = 0.0005f; // 0.05% precision
  synchronized void setChildQueues(Collection<CSQueue> childQueues) {
    // Validate
    float childCapacities = 0;
    for (CSQueue queue : childQueues) {
      childCapacities += queue.getCapacity();
    }
    float delta = Math.abs(1.0f - childCapacities);  // crude way to check
    // allow capacities being set to 0, and enforce child 0 if parent is 0
    if (((queueCapacities.getCapacity() > 0) && (delta > PRECISION)) || 
        ((queueCapacities.getCapacity() == 0) && (childCapacities > 0))) {
      throw new IllegalArgumentException("Illegal" +
      		" capacity of " + childCapacities + 
      		" for children of queue " + queueName);
    }
    // check label capacities
    for (String nodeLabel : queueCapacities.getExistingNodeLabels()) {
      float capacityByLabel = queueCapacities.getCapacity(nodeLabel);
      // check children's labels
      float sum = 0;
      for (CSQueue queue : childQueues) {
        sum += queue.getQueueCapacities().getCapacity(nodeLabel);
      }
      if ((capacityByLabel > 0 && Math.abs(1.0f - sum) > PRECISION)
          || (capacityByLabel == 0) && (sum > 0)) {
        throw new IllegalArgumentException("Illegal" + " capacity of "
            + sum + " for children of queue " + queueName
            + " for label=" + nodeLabel);
      }
    }
    
    this.childQueues.clear();
    this.childQueues.addAll(childQueues);
    if (LOG.isDebugEnabled()) {
      LOG.debug("setChildQueues: " + getChildQueuesToPrint());
    }
  }

  @Override
  public String getQueuePath() {
    String parentPath = ((parent == null) ? "" : (parent.getQueuePath() + "."));
    return parentPath + getQueueName();
  }

  @Override
  public synchronized QueueInfo getQueueInfo( 
      boolean includeChildQueues, boolean recursive) {
    QueueInfo queueInfo = getQueueInfo();

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
    for (QueueACL operation : QueueACL.values()) {
      if (hasAccess(operation, user)) {
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
    return queueName + ": " +
        "numChildQueue= " + childQueues.size() + ", " + 
        "capacity=" + queueCapacities.getCapacity() + ", " +  
        "absoluteCapacity=" + queueCapacities.getAbsoluteCapacity() + ", " +
        "usedResources=" + queueUsage.getUsed() + 
        "usedCapacity=" + getUsedCapacity() + ", " + 
        "numApps=" + getNumApplications() + ", " + 
        "numContainers=" + getNumContainers();
  }
  
  @Override
  public synchronized void reinitialize(CSQueue newlyParsedQueue,
      Resource clusterResource) throws IOException {
    // Sanity check
    if (!(newlyParsedQueue instanceof ParentQueue) ||
        !newlyParsedQueue.getQueuePath().equals(getQueuePath())) {
      throw new IOException("Trying to reinitialize " + getQueuePath() +
          " from " + newlyParsedQueue.getQueuePath());
    }

    ParentQueue newlyParsedParentQueue = (ParentQueue)newlyParsedQueue;

    // Set new configs
    setupQueueConfigs(clusterResource);

    // Re-configure existing child queues and add new ones
    // The CS has already checked to ensure all existing child queues are present!
    Map<String, CSQueue> currentChildQueues = getQueues(childQueues);
    Map<String, CSQueue> newChildQueues = 
        getQueues(newlyParsedParentQueue.childQueues);
    for (Map.Entry<String, CSQueue> e : newChildQueues.entrySet()) {
      String newChildQueueName = e.getKey();
      CSQueue newChildQueue = e.getValue();

      CSQueue childQueue = currentChildQueues.get(newChildQueueName);
      
      // Check if the child-queue already exists
      if (childQueue != null) {
        // Re-init existing child queues
        childQueue.reinitialize(newChildQueue, clusterResource);
        LOG.info(getQueueName() + ": re-configured queue: " + childQueue);
      } else {
        // New child queue, do not re-init
        
        // Set parent to 'this'
        newChildQueue.setParent(this);
        
        // Save in list of current child queues
        currentChildQueues.put(newChildQueueName, newChildQueue);
        
        LOG.info(getQueueName() + ": added new child queue: " + newChildQueue);
      }
    }

    // Re-sort all queues
    childQueues.clear();
    childQueues.addAll(currentChildQueues.values());
  }

  Map<String, CSQueue> getQueues(Set<CSQueue> queues) {
    Map<String, CSQueue> queuesMap = new HashMap<String, CSQueue>();
    for (CSQueue queue : queues) {
      queuesMap.put(queue.getQueueName(), queue);
    }
    return queuesMap;
  }

  @Override
  public void submitApplication(ApplicationId applicationId, String user,
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
            applicationId);
      }

      addApplication(applicationId, user);
    }
    
    // Inform the parent queue
    if (parent != null) {
      try {
        parent.submitApplication(applicationId, user, queue);
      } catch (AccessControlException ace) {
        LOG.info("Failed to submit application to parent-queue: " + 
            parent.getQueuePath(), ace);
        removeApplication(applicationId, user);
        throw ace;
      }
    }
  }


  @Override
  public void submitApplicationAttempt(FiCaSchedulerApp application,
      String userName) {
    // submit attempt logic.
  }

  @Override
  public void finishApplicationAttempt(FiCaSchedulerApp application,
      String queue) {
    // finish attempt logic.
  }

  private synchronized void addApplication(ApplicationId applicationId,
      String user) {

    ++numApplications;

    LOG.info("Application added -" +
        " appId: " + applicationId + 
        " user: " + user + 
        " leaf-queue of parent: " + getQueueName() + 
        " #applications: " + getNumApplications());
  }
  
  @Override
  public void finishApplication(ApplicationId application, String user) {
    
    synchronized (this) {
      removeApplication(application, user);
    }
    
    // Inform the parent queue
    if (parent != null) {
      parent.finishApplication(application, user);
    }
  }

  private synchronized void removeApplication(ApplicationId applicationId, 
      String user) {
    
    --numApplications;

    LOG.info("Application removed -" +
        " appId: " + applicationId + 
        " user: " + user + 
        " leaf-queue of parent: " + getQueueName() + 
        " #applications: " + getNumApplications());
  }

  @Override
  public synchronized CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits resourceLimits,
      SchedulingMode schedulingMode) {
    // if our queue cannot access this node, just return
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY
        && !accessibleToPartition(node.getPartition())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip this queue=" + getQueuePath()
            + ", because it is not able to access partition=" + node
            .getPartition());
      }
      return CSAssignment.NULL_ASSIGNMENT;
    }
    
    // Check if this queue need more resource, simply skip allocation if this
    // queue doesn't need more resources.
    if (!super.hasPendingResourceRequest(node.getPartition(),
        clusterResource, schedulingMode)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip this queue=" + getQueuePath()
            + ", because it doesn't need more resource, schedulingMode="
            + schedulingMode.name() + " node-partition=" + node.getPartition());
      }
      return CSAssignment.NULL_ASSIGNMENT;
    }
    
    CSAssignment assignment = 
        new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);
    
    while (canAssign(clusterResource, node)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to assign containers to child-queue of "
          + getQueueName());
      }
      
      // Are we over maximum-capacity for this queue?
      // This will also consider parent's limits and also continuous reservation
      // looking
      if (!super.canAssignToThisQueue(clusterResource, node.getPartition(),
          resourceLimits, Resources.createResource(
              getMetrics().getReservedMB(), getMetrics()
                  .getReservedVirtualCores()), schedulingMode)) {
        break;
      }
      
      // Schedule
      CSAssignment assignedToChild =
          assignContainersToChildQueues(clusterResource, node, resourceLimits,
              schedulingMode);
      assignment.setType(assignedToChild.getType());
      
      // Done if no child-queue assigned anything
      if (Resources.greaterThan(
              resourceCalculator, clusterResource, 
              assignedToChild.getResource(), Resources.none())) {
        // Track resource utilization for the parent-queue
        allocateResource(clusterResource, assignedToChild.getResource(),
            node.getPartition(), assignedToChild.isIncreasedAllocation());
        
        // Track resource utilization in this pass of the scheduler
        Resources
          .addTo(assignment.getResource(), assignedToChild.getResource());
        Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
          assignedToChild.getAssignmentInformation().getAllocated());
        Resources.addTo(assignment.getAssignmentInformation().getReserved(),
            assignedToChild.getAssignmentInformation().getReserved());
        assignment.getAssignmentInformation().incrAllocations(
          assignedToChild.getAssignmentInformation().getNumAllocations());
        assignment.getAssignmentInformation().incrReservations(
          assignedToChild.getAssignmentInformation().getNumReservations());
        assignment
          .getAssignmentInformation()
          .getAllocationDetails()
          .addAll(
              assignedToChild.getAssignmentInformation().getAllocationDetails());
        assignment
          .getAssignmentInformation()
          .getReservationDetails()
          .addAll(
              assignedToChild.getAssignmentInformation()
                  .getReservationDetails());
        assignment.setIncreasedAllocation(assignedToChild
            .isIncreasedAllocation());
        
        LOG.info("assignedContainer" +
            " queue=" + getQueueName() + 
            " usedCapacity=" + getUsedCapacity() +
            " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
            " used=" + queueUsage.getUsed() + 
            " cluster=" + clusterResource);

      } else {
        break;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("ParentQ=" + getQueueName()
          + " assignedSoFarInThisIteration=" + assignment.getResource()
          + " usedCapacity=" + getUsedCapacity()
          + " absoluteUsedCapacity=" + getAbsoluteUsedCapacity());
      }

      // Do not assign more than one container if this isn't the root queue
      // or if we've already assigned an off-switch container
      if (!rootQueue || assignment.getType() == NodeType.OFF_SWITCH) {
        if (LOG.isDebugEnabled()) {
          if (rootQueue && assignment.getType() == NodeType.OFF_SWITCH) {
            LOG.debug("Not assigning more than one off-switch container," +
                " assignments so far: " + assignment);
          }
        }
        break;
      }
    } 
    
    return assignment;
  }

  private boolean canAssign(Resource clusterResource, FiCaSchedulerNode node) {
    // Two conditions need to meet when trying to allocate:
    // 1) Node doesn't have reserved container
    // 2) Node's available-resource + killable-resource should > 0
    return node.getReservedContainer() == null && Resources.greaterThanOrEqual(
        resourceCalculator, clusterResource, Resources
            .add(node.getUnallocatedResource(), node.getTotalKillableResources()),
        minimumAllocation);
  }

  private ResourceLimits getResourceLimitsOfChild(CSQueue child,
      Resource clusterResource, ResourceLimits parentLimits,
      String nodePartition) {
    // Set resource-limit of a given child, child.limit =
    // min(my.limit - my.used + child.used, child.max)

    // Parent available resource = parent-limit - parent-used-resource
    Resource parentMaxAvailableResource = Resources.subtract(
        parentLimits.getLimit(), queueUsage.getUsed(nodePartition));
    // Deduct killable from used
    Resources.addTo(parentMaxAvailableResource,
        getTotalKillableResource(nodePartition));

    // Child's limit = parent-available-resource + child-used
    Resource childLimit = Resources.add(parentMaxAvailableResource,
        child.getQueueResourceUsage().getUsed(nodePartition));

    // Get child's max resource
    Resource childConfiguredMaxResource = Resources.multiplyAndNormalizeDown(
        resourceCalculator,
        labelManager.getResourceByLabel(nodePartition, clusterResource),
        child.getQueueCapacities().getAbsoluteMaximumCapacity(nodePartition),
        minimumAllocation);

    // Child's limit should be capped by child configured max resource
    childLimit =
        Resources.min(resourceCalculator, clusterResource, childLimit,
            childConfiguredMaxResource);

    // Normalize before return
    childLimit =
        Resources.roundDown(resourceCalculator, childLimit, minimumAllocation);

    return new ResourceLimits(childLimit);
  }
  
  private Iterator<CSQueue> sortAndGetChildrenAllocationIterator(FiCaSchedulerNode node) {
    if (node.getPartition().equals(RMNodeLabelsManager.NO_LABEL)) {
      if (needToResortQueuesAtNextAllocation) {
        // If we skipped resort queues last time, we need to re-sort queue
        // before allocation
        List<CSQueue> childrenList = new ArrayList<>(childQueues);
        childQueues.clear();
        childQueues.addAll(childrenList);
        needToResortQueuesAtNextAllocation = false;
      }
      return childQueues.iterator();
    }

    partitionQueueComparator.setPartitionToLookAt(node.getPartition());
    List<CSQueue> childrenList = new ArrayList<>(childQueues);
    Collections.sort(childrenList, partitionQueueComparator);
    return childrenList.iterator();
  }
  
  private synchronized CSAssignment assignContainersToChildQueues(
      Resource cluster, FiCaSchedulerNode node, ResourceLimits limits,
      SchedulingMode schedulingMode) {
    CSAssignment assignment = 
        new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);
    
    printChildQueues();

    // Try to assign to most 'under-served' sub-queue
    for (Iterator<CSQueue> iter = sortAndGetChildrenAllocationIterator(node); iter
        .hasNext();) {
      CSQueue childQueue = iter.next();
      if(LOG.isDebugEnabled()) {
        LOG.debug("Trying to assign to queue: " + childQueue.getQueuePath()
          + " stats: " + childQueue);
      }

      // Get ResourceLimits of child queue before assign containers
      ResourceLimits childLimits =
          getResourceLimitsOfChild(childQueue, cluster, limits, node.getPartition());
      
      assignment = childQueue.assignContainers(cluster, node, 
          childLimits, schedulingMode);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Assigned to queue: " + childQueue.getQueuePath() +
          " stats: " + childQueue + " --> " + 
          assignment.getResource() + ", " + assignment.getType());
      }

      // If we do assign, remove the queue and re-insert in-order to re-sort
      if (Resources.greaterThan(
              resourceCalculator, cluster, 
              assignment.getResource(), Resources.none())) {
        // Only update childQueues when we doing non-partitioned node
        // allocation.
        if (RMNodeLabelsManager.NO_LABEL.equals(node.getPartition())) {
          // Remove and re-insert to sort
          iter.remove();
          LOG.info("Re-sorting assigned queue: " + childQueue.getQueuePath()
              + " stats: " + childQueue);
          childQueues.add(childQueue);
          if (LOG.isDebugEnabled()) {
            printChildQueues();
          }
        }
        break;
      }
    }
    
    return assignment;
  }

  String getChildQueuesToPrint() {
    StringBuilder sb = new StringBuilder();
    for (CSQueue q : childQueues) {
      sb.append(q.getQueuePath() + 
          "usedCapacity=(" + q.getUsedCapacity() + "), " + 
          " label=("
          + StringUtils.join(q.getAccessibleNodeLabels().iterator(), ",") 
          + ")");
    }
    return sb.toString();
  }

  private void printChildQueues() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("printChildQueues - queue: " + getQueuePath()
        + " child-queues: " + getChildQueuesToPrint());
    }
  }
  
  private synchronized void internalReleaseResource(Resource clusterResource,
      FiCaSchedulerNode node, Resource releasedResource, boolean changeResource,
      CSQueue completedChildQueue, boolean sortQueues) {
    super.releaseResource(clusterResource,
        releasedResource, node.getPartition(),
        changeResource);

    if (LOG.isDebugEnabled()) {
      LOG.debug("completedContainer " + this + ", cluster=" + clusterResource);
    }

    // Note that this is using an iterator on the childQueues so this can't
    // be called if already within an iterator for the childQueues. Like
    // from assignContainersToChildQueues.
    if (sortQueues) {
      // reinsert the updated queue
      for (Iterator<CSQueue> iter = childQueues.iterator(); iter.hasNext();) {
        CSQueue csqueue = iter.next();
        if (csqueue.equals(completedChildQueue)) {
          iter.remove();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Re-sorting completed queue: " + csqueue);
          }
          childQueues.add(csqueue);
          break;
        }
      }
    }

    // If we skipped sort queue this time, we need to resort queues to make
    // sure we allocate from least usage (or order defined by queue policy)
    // queues.
    needToResortQueuesAtNextAllocation = !sortQueues;
  }
  
  @Override
  public void decreaseContainer(Resource clusterResource,
      SchedContainerChangeRequest decreaseRequest, FiCaSchedulerApp app)
      throws InvalidResourceRequestException {
    // delta capacity is negative when it's a decrease request
    Resource absDeltaCapacity =
        Resources.negate(decreaseRequest.getDeltaCapacity());

    internalReleaseResource(clusterResource,
        csContext.getNode(decreaseRequest.getNodeId()), absDeltaCapacity, false,
        null, false);

    // Inform the parent
    if (parent != null) {
      parent.decreaseContainer(clusterResource, decreaseRequest, app);
    }
  }
  
  @Override
  public void unreserveIncreasedContainer(Resource clusterResource,
      FiCaSchedulerApp app, FiCaSchedulerNode node, RMContainer rmContainer) {
    if (app != null) {
      internalReleaseResource(clusterResource, node,
          rmContainer.getReservedResource(), false, null, false);

      // Inform the parent
      if (parent != null) {
        parent.unreserveIncreasedContainer(clusterResource, app, node,
            rmContainer);
      }    
    }
  }

  @Override
  public void completedContainer(Resource clusterResource,
      FiCaSchedulerApp application, FiCaSchedulerNode node, 
      RMContainer rmContainer, ContainerStatus containerStatus, 
      RMContainerEventType event, CSQueue completedChildQueue,
      boolean sortQueues) {
    if (application != null) {
      internalReleaseResource(clusterResource, node,
          rmContainer.getContainer().getResource(), false, completedChildQueue,
          sortQueues);

      // Inform the parent
      if (parent != null) {
        // complete my parent
        parent.completedContainer(clusterResource, application, 
            node, rmContainer, null, event, this, sortQueues);
      }    
    }
  }

  @Override
  public synchronized void updateClusterResource(Resource clusterResource,
      ResourceLimits resourceLimits) {
    // Update all children
    for (CSQueue childQueue : childQueues) {
      // Get ResourceLimits of child queue before assign containers
      ResourceLimits childLimits = getResourceLimitsOfChild(childQueue,
          clusterResource, resourceLimits, RMNodeLabelsManager.NO_LABEL);
      childQueue.updateClusterResource(clusterResource, childLimits);
    }
    
    CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
        minimumAllocation, this, labelManager, null);
  }
  
  @Override
  public synchronized List<CSQueue> getChildQueues() {
    return new ArrayList<CSQueue>(childQueues);
  }
  
  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt attempt, RMContainer rmContainer) {
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }
    // Careful! Locking order is important! 
    synchronized (this) {
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource,
          rmContainer.getContainer().getResource(), node.getPartition(), false);
    }
    if (parent != null) {
      parent.recoverContainer(clusterResource, attempt, rmContainer);
    }
  }
  
  @Override
  public ActiveUsersManager getActiveUsersManager() {
    // Should never be called since all applications are submitted to LeafQueues
    return null;
  }

  @Override
  public synchronized void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    for (CSQueue queue : childQueues) {
      queue.collectSchedulerApplications(apps);
    }
  }

  @Override
  public void attachContainer(Resource clusterResource,
      FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource, rmContainer.getContainer()
          .getResource(), node.getPartition(), false);
      LOG.info("movedContainer" + " queueMoveIn=" + getQueueName()
          + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
          + getAbsoluteUsedCapacity() + " used=" + queueUsage.getUsed() + " cluster="
          + clusterResource);
      // Inform the parent
      if (parent != null) {
        parent.attachContainer(clusterResource, application, rmContainer);
      }
    }
  }

  @Override
  public void detachContainer(Resource clusterResource,
      FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      super.releaseResource(clusterResource,
          rmContainer.getContainer().getResource(),
          node.getPartition(), false);
      LOG.info("movedContainer" + " queueMoveOut=" + getQueueName()
          + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
          + getAbsoluteUsedCapacity() + " used=" + queueUsage.getUsed() + " cluster="
          + clusterResource);
      // Inform the parent
      if (parent != null) {
        parent.detachContainer(clusterResource, application, rmContainer);
      }
    }
  }
  
  public synchronized int getNumApplications() {
    return numApplications;
  }

  synchronized void allocateResource(Resource clusterResource,
      Resource resource, String nodePartition, boolean changeContainerResource) {
    super.allocateResource(clusterResource, resource, nodePartition,
        changeContainerResource);

    /**
     * check if we need to kill (killable) containers if maximum resource violated.
     * Doing this because we will deduct killable resource when going from root.
     * For example:
     * <pre>
     *      Root
     *      /   \
     *     a     b
     *   /  \
     *  a1  a2
     * </pre>
     *
     * a: max=10G, used=10G, killable=2G
     * a1: used=8G, killable=2G
     * a2: used=2G, pending=2G, killable=0G
     *
     * When we get queue-a to allocate resource, even if queue-a
     * reaches its max resource, we deduct its used by killable, so we can allocate
     * at most 2G resources. ResourceLimits passed down to a2 has headroom set to 2G.
     *
     * If scheduler finds a 2G available resource in existing cluster, and assigns it
     * to a2, now a2's used= 2G + 2G = 4G, and a's used = 8G + 4G = 12G > 10G
     *
     * When this happens, we have to preempt killable container (on same or different
     * nodes) of parent queue to avoid violating parent's max resource.
     */
    if (getQueueCapacities().getAbsoluteMaximumCapacity(nodePartition)
        < getQueueCapacities().getAbsoluteUsedCapacity(nodePartition)) {
      killContainersToEnforceMaxQueueCapacity(nodePartition, clusterResource);
    }
  }

  private void killContainersToEnforceMaxQueueCapacity(String partition,
      Resource clusterResource) {
    Iterator<RMContainer> killableContainerIter = getKillableContainers(
        partition);
    if (!killableContainerIter.hasNext()) {
      return;
    }

    Resource partitionResource = labelManager.getResourceByLabel(partition,
        null);
    Resource maxResource = Resources.multiply(partitionResource,
        getQueueCapacities().getAbsoluteMaximumCapacity(partition));

    while (Resources.greaterThan(resourceCalculator, partitionResource,
        queueUsage.getUsed(partition), maxResource)) {
      RMContainer toKillContainer = killableContainerIter.next();
      FiCaSchedulerApp attempt = csContext.getApplicationAttempt(
          toKillContainer.getContainerId().getApplicationAttemptId());
      FiCaSchedulerNode node = csContext.getNode(
          toKillContainer.getAllocatedNode());
      if (null != attempt && null != node) {
        LeafQueue lq = attempt.getCSLeafQueue();
        lq.completedContainer(clusterResource, attempt, node, toKillContainer,
            SchedulerUtils.createPreemptedContainerStatus(
                toKillContainer.getContainerId(),
                SchedulerUtils.PREEMPTED_CONTAINER), RMContainerEventType.KILL,
            null, false);
        LOG.info("Killed container=" + toKillContainer.getContainerId()
            + " from queue=" + lq.getQueueName() + " to make queue=" + this
            .getQueueName() + "'s max-capacity enforced");
      }

      if (!killableContainerIter.hasNext()) {
        break;
      }
    }
  }
}