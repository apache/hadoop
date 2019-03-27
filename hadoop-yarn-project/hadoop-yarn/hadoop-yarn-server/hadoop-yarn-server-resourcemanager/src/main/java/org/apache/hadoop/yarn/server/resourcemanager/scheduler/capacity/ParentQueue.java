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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.AllocationState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.QueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSetUtils;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
@Evolving
public class ParentQueue extends AbstractCSQueue {

  private static final Logger LOG =
      LoggerFactory.getLogger(ParentQueue.class);

  protected final List<CSQueue> childQueues;
  private final boolean rootQueue;
  private volatile int numApplications;
  private final CapacitySchedulerContext scheduler;

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private QueueOrderingPolicy queueOrderingPolicy;

  private long lastSkipQueueDebugLoggingTimestamp = -1;

  public ParentQueue(CapacitySchedulerContext cs,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);
    this.scheduler = cs;
    this.rootQueue = (parent == null);

    float rawCapacity = cs.getConfiguration().getNonLabeledQueueCapacity(getQueuePath());

    if (rootQueue &&
        (rawCapacity != CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE)) {
      throw new IllegalArgumentException("Illegal " +
          "capacity of " + rawCapacity + " for queue " + queueName +
          ". Must be " + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE);
    }

    this.childQueues = new ArrayList<>();

    setupQueueConfigs(cs.getClusterResource());

    LOG.info("Initialized parent-queue " + queueName +
        " name=" + queueName +
        ", fullname=" + getQueuePath());
  }

  // returns what is configured queue ordering policy
  private String getQueueOrderingPolicyConfigName() {
    return queueOrderingPolicy == null ?
        null :
        queueOrderingPolicy.getConfigName();
  }

  protected void setupQueueConfigs(Resource clusterResource)
      throws IOException {
    writeLock.lock();
    try {
      super.setupQueueConfigs(clusterResource);
      StringBuilder aclsString = new StringBuilder();
      for (Map.Entry<AccessType, AccessControlList> e : acls.entrySet()) {
        aclsString.append(e.getKey() + ":" + e.getValue().getAclString());
      }

      StringBuilder labelStrBuilder = new StringBuilder();
      if (accessibleLabels != null) {
        for (String s : accessibleLabels) {
          labelStrBuilder.append(s)
              .append(",");
        }
      }

      // Initialize queue ordering policy
      queueOrderingPolicy = csContext.getConfiguration().getQueueOrderingPolicy(
          getQueuePath(), parent == null ?
              null :
              ((ParentQueue) parent).getQueueOrderingPolicyConfigName());
      queueOrderingPolicy.setQueues(childQueues);

      LOG.info(queueName + ", capacity=" + this.queueCapacities.getCapacity()
          + ", absoluteCapacity=" + this.queueCapacities.getAbsoluteCapacity()
          + ", maxCapacity=" + this.queueCapacities.getMaximumCapacity()
          + ", absoluteMaxCapacity=" + this.queueCapacities
          .getAbsoluteMaximumCapacity() + ", state=" + getState() + ", acls="
          + aclsString + ", labels=" + labelStrBuilder.toString() + "\n"
          + ", reservationsContinueLooking=" + reservationsContinueLooking
          + ", orderingPolicy=" + getQueueOrderingPolicyConfigName()
          + ", priority=" + priority);
    } finally {
      writeLock.unlock();
    }
  }

  private static float PRECISION = 0.0005f; // 0.05% precision

  void setChildQueues(Collection<CSQueue> childQueues) {
    writeLock.lock();
    try {
      // Validate
      float childCapacities = 0;
      Resource minResDefaultLabel = Resources.createResource(0, 0);
      for (CSQueue queue : childQueues) {
        childCapacities += queue.getCapacity();
        Resources.addTo(minResDefaultLabel, queue.getQueueResourceQuotas()
            .getConfiguredMinResource());

        // If any child queue is using percentage based capacity model vs parent
        // queues' absolute configuration or vice versa, throw back an
        // exception.
        if (!queueName.equals("root") && getCapacity() != 0f
            && !queue.getQueueResourceQuotas().getConfiguredMinResource()
                .equals(Resources.none())) {
          throw new IllegalArgumentException("Parent queue '" + getQueueName()
              + "' and child queue '" + queue.getQueueName()
              + "' should use either percentage based capacity"
              + " configuration or absolute resource together.");
        }
      }

      float delta = Math.abs(1.0f - childCapacities);  // crude way to check
      // allow capacities being set to 0, and enforce child 0 if parent is 0
      if ((minResDefaultLabel.equals(Resources.none())
          && (queueCapacities.getCapacity() > 0) && (delta > PRECISION))
          || ((queueCapacities.getCapacity() == 0) && (childCapacities > 0))) {
        throw new IllegalArgumentException("Illegal" + " capacity of "
            + childCapacities + " for children of queue " + queueName);
      }
      // check label capacities
      for (String nodeLabel : queueCapacities.getExistingNodeLabels()) {
        float capacityByLabel = queueCapacities.getCapacity(nodeLabel);
        // check children's labels
        float sum = 0;
        Resource minRes = Resources.createResource(0, 0);
        Resource resourceByLabel = labelManager.getResourceByLabel(nodeLabel,
            scheduler.getClusterResource());
        for (CSQueue queue : childQueues) {
          sum += queue.getQueueCapacities().getCapacity(nodeLabel);

          // If any child queue of a label is using percentage based capacity
          // model vs parent queues' absolute configuration or vice versa, throw
          // back an exception
          if (!queueName.equals("root") && !this.capacityConfigType
              .equals(queue.getCapacityConfigType())) {
            throw new IllegalArgumentException("Parent queue '" + getQueueName()
                + "' and child queue '" + queue.getQueueName()
                + "' should use either percentage based capacity"
                + "configuration or absolute resource together for label:"
                + nodeLabel);
          }

          // Accumulate all min/max resource configured for all child queues.
          Resources.addTo(minRes, queue.getQueueResourceQuotas()
              .getConfiguredMinResource(nodeLabel));
        }
        if ((minResDefaultLabel.equals(Resources.none()) && capacityByLabel > 0
            && Math.abs(1.0f - sum) > PRECISION)
            || (capacityByLabel == 0) && (sum > 0)) {
          throw new IllegalArgumentException(
              "Illegal" + " capacity of " + sum + " for children of queue "
                  + queueName + " for label=" + nodeLabel);
        }

        // Ensure that for each parent queue: parent.min-resource >=
        // Σ(child.min-resource).
        Resource parentMinResource = queueResourceQuotas
            .getConfiguredMinResource(nodeLabel);
        if (!parentMinResource.equals(Resources.none()) && Resources.lessThan(
            resourceCalculator, resourceByLabel, parentMinResource, minRes)) {
          throw new IllegalArgumentException("Parent Queues" + " capacity: "
              + parentMinResource + " is less than" + " to its children:"
              + minRes + " for queue:" + queueName);
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
  public QueueInfo getQueueInfo(
      boolean includeChildQueues, boolean recursive) {
    readLock.lock();
    try {
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
    readLock.lock();
    try {
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
    readLock.lock();
    try {
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
    writeLock.lock();
    try {
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
      Map<String, CSQueue> currentChildQueues = getQueuesMap(childQueues);
      Map<String, CSQueue> newChildQueues = getQueuesMap(
          newlyParsedParentQueue.childQueues);
      for (Map.Entry<String, CSQueue> e : newChildQueues.entrySet()) {
        String newChildQueueName = e.getKey();
        CSQueue newChildQueue = e.getValue();

        CSQueue childQueue = currentChildQueues.get(newChildQueueName);

        // Check if the child-queue already exists
        if (childQueue != null) {
          // Check if the child-queue has been converted into parent queue or
          // parent Queue has been converted to child queue. The CS has already
          // checked to ensure that this child-queue is in STOPPED state if
          // Child queue has been converted to ParentQueue.
          if ((childQueue instanceof LeafQueue
              && newChildQueue instanceof ParentQueue)
              || (childQueue instanceof ParentQueue
                  && newChildQueue instanceof LeafQueue)) {
            // We would convert this LeafQueue to ParentQueue, or vice versa.
            // consider this as the combination of DELETE then ADD.
            newChildQueue.setParent(this);
            currentChildQueues.put(newChildQueueName, newChildQueue);
            // inform CapacitySchedulerQueueManager
            CapacitySchedulerQueueManager queueManager =
                this.csContext.getCapacitySchedulerQueueManager();
            queueManager.addQueue(newChildQueueName, newChildQueue);
            continue;
          }
          // Re-init existing queues
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

      // remove the deleted queue in the refreshed xml.
      for (Iterator<Map.Entry<String, CSQueue>> itr = currentChildQueues
          .entrySet().iterator(); itr.hasNext();) {
        Map.Entry<String, CSQueue> e = itr.next();
        String queueName = e.getKey();
        if (!newChildQueues.containsKey(queueName)) {
          itr.remove();
        }
      }

      // Re-sort all queues
      childQueues.clear();
      childQueues.addAll(currentChildQueues.values());

      // Make sure we notifies QueueOrderingPolicy
      queueOrderingPolicy.setQueues(childQueues);
    } finally {
      writeLock.unlock();
    }
  }

  private Map<String, CSQueue> getQueuesMap(List<CSQueue> queues) {
    Map<String, CSQueue> queuesMap = new HashMap<String, CSQueue>();
    for (CSQueue queue : queues) {
      queuesMap.put(queue.getQueueName(), queue);
    }
    return queuesMap;
  }

  @Override
  public void submitApplication(ApplicationId applicationId, String user,
      String queue) throws AccessControlException {
    writeLock.lock();
    try {
      // Sanity check
      validateSubmitApplication(applicationId, user, queue);

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

  public void validateSubmitApplication(ApplicationId applicationId,
      String userName, String queue) throws AccessControlException {
    writeLock.lock();
    try {
      if (queue.equals(queueName)) {
        throw new AccessControlException(
            "Cannot submit application " + "to non-leaf queue: " + queueName);
      }

      if (getState() != QueueState.RUNNING) {
        throw new AccessControlException("Queue " + getQueuePath()
            + " is STOPPED. Cannot accept submission of application: "
            + applicationId);
      }
    } finally {
      writeLock.unlock();
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
    writeLock.lock();
    try {
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

    appFinished();

    // Inform the parent queue
    if (parent != null) {
      parent.finishApplication(application, user);
    }
  }

  private void removeApplication(ApplicationId applicationId,
      String user) {
    writeLock.lock();
    try {
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
      CandidateNodeSet<FiCaSchedulerNode> candidates,
      ResourceLimits resourceLimits, SchedulingMode schedulingMode) {
    FiCaSchedulerNode node = CandidateNodeSetUtils.getSingleNode(candidates);

    // if our queue cannot access this node, just return
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY
        && !accessibleToPartition(candidates.getPartition())) {
      if (LOG.isDebugEnabled()) {
        long now = System.currentTimeMillis();
        // Do logging every 1 sec to avoid excessive logging.
        if (now - this.lastSkipQueueDebugLoggingTimestamp > 1000) {
          LOG.debug("Skip this queue=" + getQueuePath()
              + ", because it is not able to access partition=" + candidates
              .getPartition());
          this.lastSkipQueueDebugLoggingTimestamp = now;
        }
      }

      ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
          getParentName(), getQueueName(), ActivityState.REJECTED,
          ActivityDiagnosticConstant.NOT_ABLE_TO_ACCESS_PARTITION
              + candidates.getPartition());
      if (rootQueue) {
        ActivitiesLogger.NODE.finishSkippedNodeAllocation(activitiesManager,
            node);
      }

      return CSAssignment.NULL_ASSIGNMENT;
    }

    // Check if this queue need more resource, simply skip allocation if this
    // queue doesn't need more resources.
    if (!super.hasPendingResourceRequest(candidates.getPartition(),
        clusterResource, schedulingMode)) {
      if (LOG.isDebugEnabled()) {
        long now = System.currentTimeMillis();
        // Do logging every 1 sec to avoid excessive logging.
        if (now - this.lastSkipQueueDebugLoggingTimestamp > 1000) {
          LOG.debug("Skip this queue=" + getQueuePath()
              + ", because it doesn't need more resource, schedulingMode="
              + schedulingMode.name() + " node-partition=" + candidates
              .getPartition());
          this.lastSkipQueueDebugLoggingTimestamp = now;
        }
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
      LOG.debug("Trying to assign containers to child-queue of {}",
          getQueueName());

      // Are we over maximum-capacity for this queue?
      // This will also consider parent's limits and also continuous reservation
      // looking
      if (!super.canAssignToThisQueue(clusterResource,
          candidates.getPartition(),
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
          clusterResource, candidates, resourceLimits, schedulingMode);
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

        if (LOG.isDebugEnabled()) {
          LOG.debug("assignedContainer reserved=" + isReserved + " queue="
              + getQueueName() + " usedCapacity=" + getUsedCapacity()
              + " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + " used="
              + queueUsage.getUsed() + " cluster=" + clusterResource);

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
    Resource childConfiguredMaxResource = child
        .getEffectiveMaxCapacityDown(nodePartition, minimumAllocation);

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
    return queueOrderingPolicy.getAssignmentIterator(partition);
  }

  private CSAssignment assignContainersToChildQueues(Resource cluster,
      CandidateNodeSet<FiCaSchedulerNode> candidates, ResourceLimits limits,
      SchedulingMode schedulingMode) {
    CSAssignment assignment = CSAssignment.NULL_ASSIGNMENT;

    printChildQueues();

    // Try to assign to most 'under-served' sub-queue
    for (Iterator<CSQueue> iter = sortAndGetChildrenAllocationIterator(
        candidates.getPartition()); iter.hasNext(); ) {
      CSQueue childQueue = iter.next();
      LOG.debug("Trying to assign to queue: {} stats: {}",
          childQueue.getQueuePath(), childQueue);

      // Get ResourceLimits of child queue before assign containers
      ResourceLimits childLimits =
          getResourceLimitsOfChild(childQueue, cluster, limits.getNetLimit(),
              candidates.getPartition());

      CSAssignment childAssignment = childQueue.assignContainers(cluster,
          candidates, childLimits, schedulingMode);
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
        Resource blockedHeadroom = null;
        if (childQueue instanceof LeafQueue) {
          blockedHeadroom = childLimits.getHeadroom();
        } else {
          blockedHeadroom = childLimits.getBlockedHeadroom();
        }
        Resource resourceToSubtract = Resources.max(resourceCalculator,
            cluster, blockedHeadroom, Resources.none());
        limits.addBlockedHeadroom(resourceToSubtract);
        if(LOG.isDebugEnabled()) {
          LOG.debug("Decrease parentLimits " + limits.getLimit() +
              " for " + this.getQueueName() + " by " +
              resourceToSubtract + " as childQueue=" +
              childQueue.getQueueName() + " is blocked");
        }
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
      FiCaSchedulerNode node, Resource releasedResource) {
    writeLock.lock();
    try {
      super.releaseResource(clusterResource, releasedResource,
          node.getPartition());

      LOG.debug("completedContainer {}, cluster={}", this, clusterResource);

    } finally {
      writeLock.unlock();
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
          rmContainer.getContainer().getResource());

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
    writeLock.lock();
    try {
      // Update effective capacity in all parent queue.
      Set<String> configuredNodelabels = csContext.getConfiguration()
          .getConfiguredNodeLabels(getQueuePath());
      for (String label : configuredNodelabels) {
        calculateEffectiveResourcesAndCapacity(label, clusterResource);
      }

      // Update all children
      for (CSQueue childQueue : childQueues) {
        // Get ResourceLimits of child queue before assign containers
        ResourceLimits childLimits = getResourceLimitsOfChild(childQueue,
            clusterResource, resourceLimits.getLimit(),
            RMNodeLabelsManager.NO_LABEL);
        childQueue.updateClusterResource(clusterResource, childLimits);
      }

      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, null);
      // Update configured capacity/max-capacity for default partition only
      CSQueueUtils.updateConfiguredCapacityMetrics(resourceCalculator,
          labelManager.getResourceByLabel(null, clusterResource),
          RMNodeLabelsManager.NO_LABEL, this);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean hasChildQueues() {
    return true;
  }

  private void calculateEffectiveResourcesAndCapacity(String label,
      Resource clusterResource) {

    // For root queue, ensure that max/min resource is updated to latest
    // cluster resource.
    Resource resourceByLabel = labelManager.getResourceByLabel(label,
        clusterResource);
    if (getQueueName().equals("root")) {
      queueResourceQuotas.setConfiguredMinResource(label, resourceByLabel);
      queueResourceQuotas.setConfiguredMaxResource(label, resourceByLabel);
      queueResourceQuotas.setEffectiveMinResource(label, resourceByLabel);
      queueResourceQuotas.setEffectiveMaxResource(label, resourceByLabel);
      queueCapacities.setAbsoluteCapacity(label, 1.0f);
    }

    // Total configured min resources of direct children of this given parent
    // queue.
    Resource configuredMinResources = Resource.newInstance(0L, 0);
    for (CSQueue childQueue : getChildQueues()) {
      Resources.addTo(configuredMinResources,
          childQueue.getQueueResourceQuotas().getConfiguredMinResource(label));
    }

    // Factor to scale down effective resource: When cluster has sufficient
    // resources, effective_min_resources will be same as configured
    // min_resources.
    Resource numeratorForMinRatio = null;
    ResourceCalculator rc = this.csContext.getResourceCalculator();
    if (getQueueName().equals("root")) {
      if (!resourceByLabel.equals(Resources.none()) && Resources.lessThan(rc,
          clusterResource, resourceByLabel, configuredMinResources)) {
        numeratorForMinRatio = resourceByLabel;
      }
    } else {
      if (Resources.lessThan(rc, clusterResource,
          queueResourceQuotas.getEffectiveMinResource(label),
          configuredMinResources)) {
        numeratorForMinRatio = queueResourceQuotas
            .getEffectiveMinResource(label);
      }
    }

    Map<String, Float> effectiveMinRatioPerResource = getEffectiveMinRatioPerResource(
        configuredMinResources, numeratorForMinRatio);

    // loop and do this for all child queues
    for (CSQueue childQueue : getChildQueues()) {
      Resource minResource = childQueue.getQueueResourceQuotas()
          .getConfiguredMinResource(label);

      // Update effective resource (min/max) to each child queue.
      if (childQueue.getCapacityConfigType()
          .equals(CapacityConfigType.ABSOLUTE_RESOURCE)) {
        childQueue.getQueueResourceQuotas().setEffectiveMinResource(label,
            getMinResourceNormalized(childQueue.getQueueName(), effectiveMinRatioPerResource,
                minResource));

        // Max resource of a queue should be a minimum of {configuredMaxRes,
        // parentMaxRes}. parentMaxRes could be configured value. But if not
        // present could also be taken from effective max resource of parent.
        Resource parentMaxRes = queueResourceQuotas
            .getConfiguredMaxResource(label);
        if (parent != null && parentMaxRes.equals(Resources.none())) {
          parentMaxRes = parent.getQueueResourceQuotas()
              .getEffectiveMaxResource(label);
        }

        // Minimum of {childMaxResource, parentMaxRes}. However if
        // childMaxResource is empty, consider parent's max resource alone.
        Resource childMaxResource = childQueue.getQueueResourceQuotas()
            .getConfiguredMaxResource(label);
        Resource effMaxResource = Resources.min(resourceCalculator,
            resourceByLabel, childMaxResource.equals(Resources.none())
                ? parentMaxRes
                : childMaxResource,
            parentMaxRes);
        childQueue.getQueueResourceQuotas().setEffectiveMaxResource(label,
            Resources.clone(effMaxResource));

        // In cases where we still need to update some units based on
        // percentage, we have to calculate percentage and update.
        deriveCapacityFromAbsoluteConfigurations(label, clusterResource, rc,
            childQueue);
      } else {
        childQueue.getQueueResourceQuotas().setEffectiveMinResource(label,
            Resources.multiply(resourceByLabel,
                childQueue.getQueueCapacities().getAbsoluteCapacity(label)));
        childQueue.getQueueResourceQuotas().setEffectiveMaxResource(label,
            Resources.multiply(resourceByLabel, childQueue.getQueueCapacities()
                .getAbsoluteMaximumCapacity(label)));
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating effective min resource for queue:"
            + childQueue.getQueueName() + " as effMinResource="
            + childQueue.getQueueResourceQuotas().getEffectiveMinResource(label)
            + "and Updating effective max resource as effMaxResource="
            + childQueue.getQueueResourceQuotas()
                .getEffectiveMaxResource(label));
      }
    }
  }

  private Resource getMinResourceNormalized(String name, Map<String, Float> effectiveMinRatio,
      Resource minResource) {
    Resource ret = Resource.newInstance(minResource);
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation nResourceInformation = minResource
          .getResourceInformation(i);

      Float ratio = effectiveMinRatio.get(nResourceInformation.getName());
      if (ratio != null) {
        ret.setResourceValue(i,
            (long) (nResourceInformation.getValue() * ratio.floatValue()));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Updating min resource for Queue: " + name + " as "
              + ret.getResourceInformation(i) + ", Actual resource: "
              + nResourceInformation.getValue() + ", ratio: "
              + ratio.floatValue());
        }
      }
    }
    return ret;
  }

  private Map<String, Float> getEffectiveMinRatioPerResource(
      Resource configuredMinResources, Resource numeratorForMinRatio) {
    Map<String, Float> effectiveMinRatioPerResource = new HashMap<>();
    if (numeratorForMinRatio != null) {
      int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
      for (int i = 0; i < maxLength; i++) {
        ResourceInformation nResourceInformation = numeratorForMinRatio
            .getResourceInformation(i);
        ResourceInformation dResourceInformation = configuredMinResources
            .getResourceInformation(i);

        long nValue = nResourceInformation.getValue();
        long dValue = UnitsConversionUtil.convert(
            dResourceInformation.getUnits(), nResourceInformation.getUnits(),
            dResourceInformation.getValue());
        if (dValue != 0) {
          effectiveMinRatioPerResource.put(nResourceInformation.getName(),
              (float) nValue / dValue);
        }
      }
    }
    return effectiveMinRatioPerResource;
  }

  private void deriveCapacityFromAbsoluteConfigurations(String label,
      Resource clusterResource, ResourceCalculator rc, CSQueue childQueue) {

    /*
     * In case when queues are configured with absolute resources, it is better
     * to update capacity/max-capacity etc w.r.t absolute resource as well. In
     * case of computation, these values wont be used any more. However for
     * metrics and UI, its better these values are pre-computed here itself.
     */

    // 1. Update capacity as a float based on parent's minResource
    childQueue.getQueueCapacities().setCapacity(label,
        rc.divide(clusterResource,
            childQueue.getQueueResourceQuotas().getEffectiveMinResource(label),
            getQueueResourceQuotas().getEffectiveMinResource(label)));

    // 2. Update max-capacity as a float based on parent's maxResource
    childQueue.getQueueCapacities().setMaximumCapacity(label,
        rc.divide(clusterResource,
            childQueue.getQueueResourceQuotas().getEffectiveMaxResource(label),
            getQueueResourceQuotas().getEffectiveMaxResource(label)));

    // 3. Update absolute capacity as a float based on parent's minResource and
    // cluster resource.
    childQueue.getQueueCapacities().setAbsoluteCapacity(label,
        (float) childQueue.getQueueCapacities().getCapacity()
            / getQueueCapacities().getAbsoluteCapacity(label));

    // 4. Update absolute max-capacity as a float based on parent's maxResource
    // and cluster resource.
    childQueue.getQueueCapacities().setAbsoluteMaximumCapacity(label,
        (float) childQueue.getQueueCapacities().getMaximumCapacity(label)
            / getQueueCapacities().getAbsoluteMaximumCapacity(label));

    // Re-visit max applications for a queue based on absolute capacity if
    // needed.
    if (childQueue instanceof LeafQueue) {
      LeafQueue leafQueue = (LeafQueue) childQueue;
      CapacitySchedulerConfiguration conf = csContext.getConfiguration();
      int maxApplications = (int) (conf.getMaximumSystemApplications()
          * childQueue.getQueueCapacities().getAbsoluteCapacity(label));
      leafQueue.setMaxApplications(maxApplications);

      int maxApplicationsPerUser = Math.min(maxApplications,
          (int) (maxApplications
              * (leafQueue.getUsersManager().getUserLimit() / 100.0f)
              * leafQueue.getUsersManager().getUserLimitFactor()));
      leafQueue.setMaxApplicationsPerUser(maxApplicationsPerUser);
      LOG.info("LeafQueue:" + leafQueue.getQueueName() + ", maxApplications="
          + maxApplications + ", maxApplicationsPerUser="
          + maxApplicationsPerUser + ", Abs Cap:"
          + childQueue.getQueueCapacities().getAbsoluteCapacity(label));
    }
  }

  @Override
  public List<CSQueue> getChildQueues() {
    readLock.lock();
    try {
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
    if (rmContainer.getExecutionType() != ExecutionType.GUARANTEED) {
      return;
    }

    // Careful! Locking order is important!
    writeLock.lock();
    try {
      FiCaSchedulerNode node = scheduler.getNode(
          rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource,
          rmContainer.getContainer().getResource(), node.getPartition());
    } finally {
      writeLock.unlock();
    }

    if (parent != null) {
      parent.recoverContainer(clusterResource, attempt, rmContainer);
    }
  }
  
  @Override
  public ActiveUsersManager getAbstractUsersManager() {
    // Should never be called since all applications are submitted to LeafQueues
    return null;
  }

  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    readLock.lock();
    try {
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
          .getResource(), node.getPartition());
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
          node.getPartition());
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
      Resource resource, String nodePartition) {
    writeLock.lock();
    try {
      super.allocateResource(clusterResource, resource, nodePartition);

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
      if (!queueResourceQuotas.getEffectiveMaxResource(nodePartition)
          .equals(Resources.none())) {
        if (Resources.lessThan(resourceCalculator, clusterResource,
            queueResourceQuotas.getEffectiveMaxResource(nodePartition),
            queueUsage.getUsed(nodePartition))) {
          killContainersToEnforceMaxQueueCapacity(nodePartition,
              clusterResource);
        }
      } else {
        if (getQueueCapacities()
            .getAbsoluteMaximumCapacity(nodePartition) < getQueueCapacities()
                .getAbsoluteUsedCapacity(nodePartition)) {
          killContainersToEnforceMaxQueueCapacity(nodePartition,
              clusterResource);
        }
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
    Resource maxResource = getEffectiveMaxCapacity(partition);

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
        writeLock.lock();
        try {
          // Book-keeping
          // Note: Update headroom to account for current allocation too...
          allocateResource(cluster, allocation.getAllocatedOrReservedResource(),
              schedulerContainer.getNodePartition());

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

  @Override
  public void stopQueue() {
    this.writeLock.lock();
    try {
      if (getNumApplications() > 0) {
        updateQueueState(QueueState.DRAINING);
      } else {
        updateQueueState(QueueState.STOPPED);
      }
      if (getChildQueues() != null) {
        for(CSQueue child : getChildQueues()) {
          child.stopQueue();
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  public QueueOrderingPolicy getQueueOrderingPolicy() {
    return queueOrderingPolicy;
  }
}
