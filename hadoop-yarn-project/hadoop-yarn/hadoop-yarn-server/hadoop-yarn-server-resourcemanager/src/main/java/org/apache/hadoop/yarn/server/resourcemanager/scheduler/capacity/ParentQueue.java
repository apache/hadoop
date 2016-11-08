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
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.AllocationState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.PlacementSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.PlacementSetUtils;
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
  private final Comparator<CSQueue> nonPartitionedQueueComparator;
  private final PartitionedQueueComparator partitionQueueComparator;
  private volatile int numApplications;
  private final CapacitySchedulerContext scheduler;

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  public ParentQueue(CapacitySchedulerContext cs,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);
    this.scheduler = cs;
    this.nonPartitionedQueueComparator = cs.getNonPartitionedQueueComparator();
    this.partitionQueueComparator = new PartitionedQueueComparator();

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

  void setupQueueConfigs(Resource clusterResource)
      throws IOException {
    try {
      writeLock.lock();
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

      LOG.info(queueName + ", capacity=" + this.queueCapacities.getCapacity()
          + ", absoluteCapacity=" + this.queueCapacities.getAbsoluteCapacity()
          + ", maxCapacity=" + this.queueCapacities.getMaximumCapacity()
          + ", absoluteMaxCapacity=" + this.queueCapacities
          .getAbsoluteMaximumCapacity() + ", state=" + state + ", acls="
          + aclsString + ", labels=" + labelStrBuilder.toString() + "\n"
          + ", reservationsContinueLooking=" + reservationsContinueLooking);
    } finally {
      writeLock.unlock();
    }
  }

  private static float PRECISION = 0.0005f; // 0.05% precision

  void setChildQueues(Collection<CSQueue> childQueues) {
    try {
      writeLock.lock();
      // Validate
      float childCapacities = 0;
      for (CSQueue queue : childQueues) {
        childCapacities += queue.getCapacity();
      }
      float delta = Math.abs(1.0f - childCapacities);  // crude way to check
      // allow capacities being set to 0, and enforce child 0 if parent is 0
      if (((queueCapacities.getCapacity() > 0) && (delta > PRECISION)) || (
          (queueCapacities.getCapacity() == 0) && (childCapacities > 0))) {
        throw new IllegalArgumentException(
            "Illegal" + " capacity of " + childCapacities
                + " for children of queue " + queueName);
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
          throw new IllegalArgumentException(
              "Illegal" + " capacity of " + sum + " for children of queue "
                  + queueName + " for label=" + nodeLabel);
        }
      }

      this.childQueues.clear();
      this.childQueues.addAll(childQueues);
      if (LOG.isDebugEnabled()) {
        LOG.debug("setChildQueues: " + getChildQueuesToPrint());
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public String getQueuePath() {
    String parentPath = ((parent == null) ? "" : (parent.getQueuePath() + "."));
    return parentPath + getQueueName();
  }

  @Override
  public QueueInfo getQueueInfo(
      boolean includeChildQueues, boolean recursive) {
    try {
      readLock.lock();
      QueueInfo queueInfo = getQueueInfo();

      List<QueueInfo> childQueuesInfo = new ArrayList<>();
      if (includeChildQueues) {
        for (CSQueue child : childQueues) {
          // Get queue information recursively?
          childQueuesInfo.add(child.getQueueInfo(recursive, recursive));
        }
      }
      queueInfo.setChildQueues(childQueuesInfo);

      return queueInfo;
    } finally {
      readLock.unlock();
    }

  }

  private QueueUserACLInfo getUserAclInfo(
      UserGroupInformation user) {
    try {
      readLock.lock();
      QueueUserACLInfo userAclInfo = recordFactory.newRecordInstance(
          QueueUserACLInfo.class);
      List<QueueACL> operations = new ArrayList<QueueACL>();
      for (QueueACL operation : QueueACL.values()) {
        if (hasAccess(operation, user)) {
          operations.add(operation);
        }
      }

      userAclInfo.setQueueName(getQueueName());
      userAclInfo.setUserAcls(operations);
      return userAclInfo;
    } finally {
      readLock.unlock();
    }

  }
  
  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo(
      UserGroupInformation user) {
    try {
      readLock.lock();
      List<QueueUserACLInfo> userAcls = new ArrayList<>();

      // Add parent queue acls
      userAcls.add(getUserAclInfo(user));

      // Add children queue acls
      for (CSQueue child : childQueues) {
        userAcls.addAll(child.getQueueUserAclInfo(user));
      }

      return userAcls;
    } finally {
      readLock.unlock();
    }

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
  public void reinitialize(CSQueue newlyParsedQueue,
      Resource clusterResource) throws IOException {
    try {
      writeLock.lock();
      // Sanity check
      if (!(newlyParsedQueue instanceof ParentQueue) || !newlyParsedQueue
          .getQueuePath().equals(getQueuePath())) {
        throw new IOException(
            "Trying to reinitialize " + getQueuePath() + " from "
                + newlyParsedQueue.getQueuePath());
      }

      ParentQueue newlyParsedParentQueue = (ParentQueue) newlyParsedQueue;

      // Set new configs
      setupQueueConfigs(clusterResource);

      // Re-configure existing child queues and add new ones
      // The CS has already checked to ensure all existing child queues are present!
      Map<String, CSQueue> currentChildQueues = getQueues(childQueues);
      Map<String, CSQueue> newChildQueues = getQueues(
          newlyParsedParentQueue.childQueues);
      for (Map.Entry<String, CSQueue> e : newChildQueues.entrySet()) {
        String newChildQueueName = e.getKey();
        CSQueue newChildQueue = e.getValue();

        CSQueue childQueue = currentChildQueues.get(newChildQueueName);

        // Check if the child-queue already exists
        if (childQueue != null) {
          // Re-init existing child queues
          childQueue.reinitialize(newChildQueue, clusterResource);
          LOG.info(getQueueName() + ": re-configured queue: " + childQueue);
        } else{
          // New child queue, do not re-init

          // Set parent to 'this'
          newChildQueue.setParent(this);

          // Save in list of current child queues
          currentChildQueues.put(newChildQueueName, newChildQueue);

          LOG.info(
              getQueueName() + ": added new child queue: " + newChildQueue);
        }
      }

      // Re-sort all queues
      childQueues.clear();
      childQueues.addAll(currentChildQueues.values());
    } finally {
      writeLock.unlock();
    }
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

    try {
      writeLock.lock();
      // Sanity check
      if (queue.equals(queueName)) {
        throw new AccessControlException(
            "Cannot submit application " + "to non-leaf queue: " + queueName);
      }

      if (state != QueueState.RUNNING) {
        throw new AccessControlException("Queue " + getQueuePath()
            + " is STOPPED. Cannot accept submission of application: "
            + applicationId);
      }

      addApplication(applicationId, user);
    } finally {
      writeLock.unlock();
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

  private void addApplication(ApplicationId applicationId,
      String user) {

    try {
      writeLock.lock();
      ++numApplications;

      LOG.info(
          "Application added -" + " appId: " + applicationId + " user: " + user
              + " leaf-queue of parent: " + getQueueName() + " #applications: "
              + getNumApplications());
    } finally {
      writeLock.unlock();
    }
  }
  
  @Override
  public void finishApplication(ApplicationId application, String user) {

    removeApplication(application, user);
    
    // Inform the parent queue
    if (parent != null) {
      parent.finishApplication(application, user);
    }
  }

  private void removeApplication(ApplicationId applicationId,
      String user) {
    try {
      writeLock.lock();
      --numApplications;

      LOG.info("Application removed -" + " appId: " + applicationId + " user: "
          + user + " leaf-queue of parent: " + getQueueName()
          + " #applications: " + getNumApplications());
    } finally {
      writeLock.unlock();
    }
  }

  private String getParentName() {
    return getParent() != null ? getParent().getQueueName() : "";
  }

  @Override
  public CSAssignment assignContainers(Resource clusterResource,
      PlacementSet<FiCaSchedulerNode> ps, ResourceLimits resourceLimits,
    SchedulingMode schedulingMode) {
    FiCaSchedulerNode node = PlacementSetUtils.getSingleNode(ps);

    // if our queue cannot access this node, just return
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY
        && !accessibleToPartition(ps.getPartition())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip this queue=" + getQueuePath()
            + ", because it is not able to access partition=" + ps
            .getPartition());
      }

      ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
          getParentName(), getQueueName(), ActivityState.REJECTED,
          ActivityDiagnosticConstant.NOT_ABLE_TO_ACCESS_PARTITION + node
              .getPartition());
      if (rootQueue) {
        ActivitiesLogger.NODE.finishSkippedNodeAllocation(activitiesManager,
            node);
      }

      return CSAssignment.NULL_ASSIGNMENT;
    }

    // Check if this queue need more resource, simply skip allocation if this
    // queue doesn't need more resources.
    if (!super.hasPendingResourceRequest(ps.getPartition(), clusterResource,
        schedulingMode)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip this queue=" + getQueuePath()
            + ", because it doesn't need more resource, schedulingMode="
            + schedulingMode.name() + " node-partition=" + ps
            .getPartition());
      }

      ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
          getParentName(), getQueueName(), ActivityState.SKIPPED,
          ActivityDiagnosticConstant.QUEUE_DO_NOT_NEED_MORE_RESOURCE);
      if (rootQueue) {
        ActivitiesLogger.NODE.finishSkippedNodeAllocation(activitiesManager,
            node);
      }

      return CSAssignment.NULL_ASSIGNMENT;
    }

    CSAssignment assignment = new CSAssignment(Resources.createResource(0, 0),
        NodeType.NODE_LOCAL);

    while (canAssign(clusterResource, node)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to assign containers to child-queue of "
            + getQueueName());
      }

      // Are we over maximum-capacity for this queue?
      // This will also consider parent's limits and also continuous reservation
      // looking
      if (!super.canAssignToThisQueue(clusterResource, ps.getPartition(),
          resourceLimits, Resources
              .createResource(getMetrics().getReservedMB(),
                  getMetrics().getReservedVirtualCores()), schedulingMode)) {

        ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
            getParentName(), getQueueName(), ActivityState.SKIPPED,
            ActivityDiagnosticConstant.QUEUE_MAX_CAPACITY_LIMIT);
        if (rootQueue) {
          ActivitiesLogger.NODE.finishSkippedNodeAllocation(activitiesManager,
              node);
        }

        break;
      }

      // Schedule
      CSAssignment assignedToChild = assignContainersToChildQueues(
          clusterResource, ps, resourceLimits, schedulingMode);
      assignment.setType(assignedToChild.getType());
      assignment.setRequestLocalityType(
          assignedToChild.getRequestLocalityType());
      assignment.setExcessReservation(assignedToChild.getExcessReservation());
      assignment.setContainersToKill(assignedToChild.getContainersToKill());

      // Done if no child-queue assigned anything
      if (Resources.greaterThan(resourceCalculator, clusterResource,
          assignedToChild.getResource(), Resources.none())) {

        ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
            getParentName(), getQueueName(), ActivityState.ACCEPTED,
            ActivityDiagnosticConstant.EMPTY);

        boolean isReserved =
            assignedToChild.getAssignmentInformation().getReservationDetails()
                != null && !assignedToChild.getAssignmentInformation()
                .getReservationDetails().isEmpty();
        if (node != null && !isReserved) {
          if (rootQueue) {
            ActivitiesLogger.NODE.finishAllocatedNodeAllocation(
                activitiesManager, node,
                assignedToChild.getAssignmentInformation()
                    .getFirstAllocatedOrReservedContainerId(),
                AllocationState.ALLOCATED);
          }
        } else{
          if (rootQueue) {
            ActivitiesLogger.NODE.finishAllocatedNodeAllocation(
                activitiesManager, node,
                assignedToChild.getAssignmentInformation()
                    .getFirstAllocatedOrReservedContainerId(),
                AllocationState.RESERVED);
          }
        }

        // Track resource utilization in this pass of the scheduler
        Resources.addTo(assignment.getResource(),
            assignedToChild.getResource());
        Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
            assignedToChild.getAssignmentInformation().getAllocated());
        Resources.addTo(assignment.getAssignmentInformation().getReserved(),
            assignedToChild.getAssignmentInformation().getReserved());
        assignment.getAssignmentInformation().incrAllocations(
            assignedToChild.getAssignmentInformation().getNumAllocations());
        assignment.getAssignmentInformation().incrReservations(
            assignedToChild.getAssignmentInformation().getNumReservations());
        assignment.getAssignmentInformation().getAllocationDetails().addAll(
            assignedToChild.getAssignmentInformation()
                .getAllocationDetails());
        assignment.getAssignmentInformation().getReservationDetails().addAll(
            assignedToChild.getAssignmentInformation()
                .getReservationDetails());
        assignment.setIncreasedAllocation(
            assignedToChild.isIncreasedAllocation());

        LOG.info("assignedContainer" + " queue=" + getQueueName()
            + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
            + getAbsoluteUsedCapacity() + " used=" + queueUsage.getUsed()
            + " cluster=" + clusterResource);

        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "ParentQ=" + getQueueName() + " assignedSoFarInThisIteration="
                  + assignment.getResource() + " usedCapacity="
                  + getUsedCapacity() + " absoluteUsedCapacity="
                  + getAbsoluteUsedCapacity());
        }
      } else{
        assignment.setSkippedType(assignedToChild.getSkippedType());

        ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
            getParentName(), getQueueName(), ActivityState.SKIPPED,
            ActivityDiagnosticConstant.EMPTY);
        if (rootQueue) {
          ActivitiesLogger.NODE.finishSkippedNodeAllocation(activitiesManager,
              node);
        }

        break;
      }

      /*
       * Previously here, we can allocate more than one container for each
       * allocation under rootQ. Now this logic is not proper any more
       * in global scheduling world.
       *
       * So here do not try to allocate more than one container for each
       * allocation, let top scheduler make the decision.
       */
      break;
    }

    return assignment;
  }

  private boolean canAssign(Resource clusterResource, FiCaSchedulerNode node) {
    // When node == null means global scheduling is enabled, always return true
    if (null == node) {
      return true;
    }

    // Two conditions need to meet when trying to allocate:
    // 1) Node doesn't have reserved container
    // 2) Node's available-resource + killable-resource should > 0
    return node.getReservedContainer() == null && Resources.greaterThanOrEqual(
        resourceCalculator, clusterResource, Resources
            .add(node.getUnallocatedResource(),
                node.getTotalKillableResources()), minimumAllocation);
  }

  private ResourceLimits getResourceLimitsOfChild(CSQueue child,
      Resource clusterResource, Resource parentLimits,
      String nodePartition) {
    // Set resource-limit of a given child, child.limit =
    // min(my.limit - my.used + child.used, child.max)

    // Parent available resource = parent-limit - parent-used-resource
    Resource parentMaxAvailableResource = Resources.subtract(
        parentLimits, queueUsage.getUsed(nodePartition));
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

  private Iterator<CSQueue> sortAndGetChildrenAllocationIterator(
      String partition) {
    // Previously we keep a sorted list for default partition, it is not good
    // when multi-threading scheduler is enabled, so to make a simpler code
    // now re-sort queue every time irrespective to node partition.
    partitionQueueComparator.setPartitionToLookAt(partition);
    List<CSQueue> childrenList = new ArrayList<>(childQueues);
    Collections.sort(childrenList, partitionQueueComparator);
    return childrenList.iterator();
  }

  private CSAssignment assignContainersToChildQueues(Resource cluster,
      PlacementSet<FiCaSchedulerNode> ps, ResourceLimits limits,
      SchedulingMode schedulingMode) {
    CSAssignment assignment = CSAssignment.NULL_ASSIGNMENT;

    Resource parentLimits = limits.getLimit();
    printChildQueues();

    // Try to assign to most 'under-served' sub-queue
    for (Iterator<CSQueue> iter = sortAndGetChildrenAllocationIterator(
        ps.getPartition()); iter.hasNext(); ) {
      CSQueue childQueue = iter.next();
      if(LOG.isDebugEnabled()) {
        LOG.debug("Trying to assign to queue: " + childQueue.getQueuePath()
          + " stats: " + childQueue);
      }

      // Get ResourceLimits of child queue before assign containers
      ResourceLimits childLimits =
          getResourceLimitsOfChild(childQueue, cluster, parentLimits,
              ps.getPartition());
      
      CSAssignment childAssignment = childQueue.assignContainers(cluster, ps,
          childLimits, schedulingMode);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Assigned to queue: " + childQueue.getQueuePath() +
            " stats: " + childQueue + " --> " +
            childAssignment.getResource() + ", " + childAssignment.getType());
      }

      if (Resources.greaterThan(
              resourceCalculator, cluster, 
              childAssignment.getResource(), Resources.none())) {
        assignment = childAssignment;
        break;
      } else if (childAssignment.getSkippedType() ==
          CSAssignment.SkippedType.QUEUE_LIMIT) {
        if (assignment.getSkippedType() !=
            CSAssignment.SkippedType.QUEUE_LIMIT) {
          assignment = childAssignment;
        }
        Resource resourceToSubtract = Resources.max(resourceCalculator,
            cluster, childLimits.getHeadroom(), Resources.none());
        if(LOG.isDebugEnabled()) {
          LOG.debug("Decrease parentLimits " + parentLimits +
              " for " + this.getQueueName() + " by " +
              resourceToSubtract + " as childQueue=" +
              childQueue.getQueueName() + " is blocked");
        }
        parentLimits = Resources.subtract(parentLimits,
            resourceToSubtract);
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

  private void internalReleaseResource(Resource clusterResource,
      FiCaSchedulerNode node, Resource releasedResource,
      boolean changeResource) {
    try {
      writeLock.lock();
      super.releaseResource(clusterResource, releasedResource,
          node.getPartition(), changeResource);

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "completedContainer " + this + ", cluster=" + clusterResource);
      }

    } finally {
      writeLock.unlock();
    }
  }
  
  @Override
  public void decreaseContainer(Resource clusterResource,
      SchedContainerChangeRequest decreaseRequest, FiCaSchedulerApp app)
      throws InvalidResourceRequestException {
    // delta capacity is negative when it's a decrease request
    Resource absDeltaCapacity =
        Resources.negate(decreaseRequest.getDeltaCapacity());

    internalReleaseResource(clusterResource,
        csContext.getNode(decreaseRequest.getNodeId()), absDeltaCapacity, false);

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
          rmContainer.getReservedResource(), false);

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
          rmContainer.getContainer().getResource(), false);

      // Inform the parent
      if (parent != null) {
        // complete my parent
        parent.completedContainer(clusterResource, application, 
            node, rmContainer, null, event, this, sortQueues);
      }    
    }
  }

  @Override
  public void updateClusterResource(Resource clusterResource,
      ResourceLimits resourceLimits) {
    try {
      writeLock.lock();
      // Update all children
      for (CSQueue childQueue : childQueues) {
        // Get ResourceLimits of child queue before assign containers
        ResourceLimits childLimits = getResourceLimitsOfChild(childQueue,
            clusterResource, resourceLimits.getLimit(),
            RMNodeLabelsManager.NO_LABEL);
        childQueue.updateClusterResource(clusterResource, childLimits);
      }

      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          minimumAllocation, this, labelManager, null);
    } finally {
      writeLock.unlock();
    }
  }
  
  @Override
  public List<CSQueue> getChildQueues() {
    try {
      readLock.lock();
      return new ArrayList<CSQueue>(childQueues);
    } finally {
      readLock.unlock();
    }

  }
  
  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt attempt, RMContainer rmContainer) {
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }

    // Careful! Locking order is important!
    try {
      writeLock.lock();
      FiCaSchedulerNode node = scheduler.getNode(
          rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource,
          rmContainer.getContainer().getResource(), node.getPartition(), false);
    } finally {
      writeLock.unlock();
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
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    try {
      readLock.lock();
      for (CSQueue queue : childQueues) {
        queue.collectSchedulerApplications(apps);
      }
    } finally {
      readLock.unlock();
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
  
  public int getNumApplications() {
    return numApplications;
  }

  void allocateResource(Resource clusterResource,
      Resource resource, String nodePartition, boolean changeContainerResource) {
    try {
      writeLock.lock();
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
    } finally {
      writeLock.unlock();
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

  public void apply(Resource cluster,
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request) {
    if (request.anythingAllocatedOrReserved()) {
      ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode>
          allocation = request.getFirstAllocatedOrReservedContainer();
      SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode>
          schedulerContainer = allocation.getAllocatedOrReservedContainer();

      // Do not modify queue when allocation from reserved container
      if (allocation.getAllocateFromReservedContainer() == null) {
        try {
          writeLock.lock();
          // Book-keeping
          // Note: Update headroom to account for current allocation too...
          allocateResource(cluster, allocation.getAllocatedOrReservedResource(),
              schedulerContainer.getNodePartition(),
              allocation.isIncreasedAllocation());

          LOG.info("assignedContainer" + " queue=" + getQueueName()
              + " usedCapacity=" + getUsedCapacity() + " absoluteUsedCapacity="
              + getAbsoluteUsedCapacity() + " used=" + queueUsage.getUsed()
              + " cluster=" + cluster);
        } finally {
          writeLock.unlock();
        }
      }
    }

    if (parent != null) {
      parent.apply(cluster, request);
    }
  }
}
