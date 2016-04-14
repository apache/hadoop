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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.Sets;

public abstract class AbstractCSQueue implements CSQueue {
  private static final Log LOG = LogFactory.getLog(AbstractCSQueue.class);  
  CSQueue parent;
  final String queueName;
  volatile int numContainers;
  
  final Resource minimumAllocation;
  volatile Resource maximumAllocation;
  QueueState state;
  final CSQueueMetrics metrics;
  protected final PrivilegedEntity queueEntity;

  final ResourceCalculator resourceCalculator;
  Set<String> accessibleLabels;
  RMNodeLabelsManager labelManager;
  String defaultLabelExpression;
  
  Map<AccessType, AccessControlList> acls = 
      new HashMap<AccessType, AccessControlList>();
  volatile boolean reservationsContinueLooking;
  private boolean preemptionDisabled;

  // Track resource usage-by-label like used-resource/pending-resource, etc.
  volatile ResourceUsage queueUsage;
  
  // Track capacities like used-capcity/abs-used-capacity/capacity/abs-capacity,
  // etc.
  QueueCapacities queueCapacities;

  private final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);
  protected CapacitySchedulerContext csContext;
  protected YarnAuthorizationProvider authorizer = null;

  public AbstractCSQueue(CapacitySchedulerContext cs, 
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this.labelManager = cs.getRMContext().getNodeLabelManager();
    this.parent = parent;
    this.queueName = queueName;
    this.resourceCalculator = cs.getResourceCalculator();
    
    // must be called after parent and queueName is set
    this.metrics =
        old != null ? (CSQueueMetrics) old.getMetrics() : CSQueueMetrics
            .forQueue(getQueuePath(), parent, cs.getConfiguration()
                .getEnableUserMetrics(), cs.getConf());

    this.csContext = cs;
    this.minimumAllocation = csContext.getMinimumResourceCapability();
    
    // initialize ResourceUsage
    queueUsage = new ResourceUsage();
    queueEntity = new PrivilegedEntity(EntityType.QUEUE, getQueuePath());
    
    // initialize QueueCapacities
    queueCapacities = new QueueCapacities(parent == null);    
  }
  
  protected void setupConfigurableCapacities() {
    CSQueueUtils.loadUpdateAndCheckCapacities(
        getQueuePath(),
        csContext.getConfiguration(), 
        queueCapacities,
        parent == null ? null : parent.getQueueCapacities());
  }
  
  @Override
  public synchronized float getCapacity() {
    return queueCapacities.getCapacity();
  }

  @Override
  public synchronized float getAbsoluteCapacity() {
    return queueCapacities.getAbsoluteCapacity();
  }

  @Override
  public float getAbsoluteMaximumCapacity() {
    return queueCapacities.getAbsoluteMaximumCapacity();
  }

  @Override
  public float getAbsoluteUsedCapacity() {
    return queueCapacities.getAbsoluteUsedCapacity();
  }

  @Override
  public float getMaximumCapacity() {
    return queueCapacities.getMaximumCapacity();
  }

  @Override
  public float getUsedCapacity() {
    return queueCapacities.getUsedCapacity();
  }

  @Override
  public Resource getUsedResources() {
    return queueUsage.getUsed();
  }

  public int getNumContainers() {
    return numContainers;
  }

  @Override
  public synchronized QueueState getState() {
    return state;
  }
  
  @Override
  public CSQueueMetrics getMetrics() {
    return metrics;
  }
  
  @Override
  public String getQueueName() {
    return queueName;
  }

  @Override
  public PrivilegedEntity getPrivilegedEntity() {
    return queueEntity;
  }

  @Override
  public synchronized CSQueue getParent() {
    return parent;
  }

  @Override
  public synchronized void setParent(CSQueue newParentQueue) {
    this.parent = (ParentQueue)newParentQueue;
  }
  
  public Set<String> getAccessibleNodeLabels() {
    return accessibleLabels;
  }

  @Override
  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    return authorizer.checkPermission(
        new AccessRequest(queueEntity, user, SchedulerUtils.toAccessType(acl),
            null, null));
  }

  @Override
  public void setUsedCapacity(float usedCapacity) {
    queueCapacities.setUsedCapacity(usedCapacity);
  }
  
  @Override
  public void setAbsoluteUsedCapacity(float absUsedCapacity) {
    queueCapacities.setAbsoluteUsedCapacity(absUsedCapacity);
  }

  /**
   * Set maximum capacity - used only for testing.
   * @param maximumCapacity new max capacity
   */
  synchronized void setMaxCapacity(float maximumCapacity) {
    // Sanity check
    CSQueueUtils.checkMaxCapacity(getQueueName(),
        queueCapacities.getCapacity(), maximumCapacity);
    float absMaxCapacity =
        CSQueueUtils.computeAbsoluteMaximumCapacity(maximumCapacity, parent);
    CSQueueUtils.checkAbsoluteCapacity(getQueueName(),
        queueCapacities.getAbsoluteCapacity(),
        absMaxCapacity);
    
    queueCapacities.setMaximumCapacity(maximumCapacity);
    queueCapacities.setAbsoluteMaximumCapacity(absMaxCapacity);
  }

  @Override
  public String getDefaultNodeLabelExpression() {
    return defaultLabelExpression;
  }
  
  synchronized void setupQueueConfigs(Resource clusterResource)
      throws IOException {
    // get labels
    this.accessibleLabels =
        csContext.getConfiguration().getAccessibleNodeLabels(getQueuePath());
    this.defaultLabelExpression = csContext.getConfiguration()
        .getDefaultNodeLabelExpression(getQueuePath());

    // inherit from parent if labels not set
    if (this.accessibleLabels == null && parent != null) {
      this.accessibleLabels = parent.getAccessibleNodeLabels();
    }
    
    // inherit from parent if labels not set
    if (this.defaultLabelExpression == null && parent != null
        && this.accessibleLabels.containsAll(parent.getAccessibleNodeLabels())) {
      this.defaultLabelExpression = parent.getDefaultNodeLabelExpression();
    }

    // After we setup labels, we can setup capacities
    setupConfigurableCapacities();
    
    this.maximumAllocation =
        csContext.getConfiguration().getMaximumAllocationPerQueue(
            getQueuePath());
    
    authorizer = YarnAuthorizationProvider.getInstance(csContext.getConf());
    
    this.state = csContext.getConfiguration().getState(getQueuePath());
    this.acls = csContext.getConfiguration().getAcls(getQueuePath());

    // Update metrics
    CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
        minimumAllocation, this, labelManager, null);
    
    // Check if labels of this queue is a subset of parent queue, only do this
    // when we not root
    if (parent != null && parent.getParent() != null) {
      if (parent.getAccessibleNodeLabels() != null
          && !parent.getAccessibleNodeLabels().contains(RMNodeLabelsManager.ANY)) {
        // if parent isn't "*", child shouldn't be "*" too
        if (this.getAccessibleNodeLabels().contains(RMNodeLabelsManager.ANY)) {
          throw new IOException("Parent's accessible queue is not ANY(*), "
              + "but child's accessible queue is *");
        } else {
          Set<String> diff =
              Sets.difference(this.getAccessibleNodeLabels(),
                  parent.getAccessibleNodeLabels());
          if (!diff.isEmpty()) {
            throw new IOException("Some labels of child queue is not a subset "
                + "of parent queue, these labels=["
                + StringUtils.join(diff, ",") + "]");
          }
        }
      }
    }

    this.reservationsContinueLooking = csContext.getConfiguration()
        .getReservationContinueLook();

    this.preemptionDisabled = isQueueHierarchyPreemptionDisabled(this);
  }
  
  protected QueueInfo getQueueInfo() {
    QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    queueInfo.setQueueName(queueName);
    queueInfo.setAccessibleNodeLabels(accessibleLabels);
    queueInfo.setCapacity(queueCapacities.getCapacity());
    queueInfo.setMaximumCapacity(queueCapacities.getMaximumCapacity());
    queueInfo.setQueueState(state);
    queueInfo.setDefaultNodeLabelExpression(defaultLabelExpression);
    queueInfo.setCurrentCapacity(getUsedCapacity());
    queueInfo.setQueueStatistics(getQueueStatistics());
    queueInfo.setPreemptionDisabled(preemptionDisabled);
    return queueInfo;
  }

  public QueueStatistics getQueueStatistics() {
    QueueStatistics stats =
        recordFactory.newRecordInstance(QueueStatistics.class);
    stats.setNumAppsSubmitted(getMetrics().getAppsSubmitted());
    stats.setNumAppsRunning(getMetrics().getAppsRunning());
    stats.setNumAppsPending(getMetrics().getAppsPending());
    stats.setNumAppsCompleted(getMetrics().getAppsCompleted());
    stats.setNumAppsKilled(getMetrics().getAppsKilled());
    stats.setNumAppsFailed(getMetrics().getAppsFailed());
    stats.setNumActiveUsers(getMetrics().getActiveUsers());
    stats.setAvailableMemoryMB(getMetrics().getAvailableMB());
    stats.setAllocatedMemoryMB(getMetrics().getAllocatedMB());
    stats.setPendingMemoryMB(getMetrics().getPendingMB());
    stats.setReservedMemoryMB(getMetrics().getReservedMB());
    stats.setAvailableVCores(getMetrics().getAvailableVirtualCores());
    stats.setAllocatedVCores(getMetrics().getAllocatedVirtualCores());
    stats.setPendingVCores(getMetrics().getPendingVirtualCores());
    stats.setReservedVCores(getMetrics().getReservedVirtualCores());
    stats.setPendingContainers(getMetrics().getPendingContainers());
    stats.setAllocatedContainers(getMetrics().getAllocatedContainers());
    stats.setReservedContainers(getMetrics().getReservedContainers());
    return stats;
  }
  
  @Private
  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }
  
  @Private
  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }
  
  synchronized void allocateResource(Resource clusterResource,
      Resource resource, String nodePartition, boolean changeContainerResource) {
    queueUsage.incUsed(nodePartition, resource);

    if (!changeContainerResource) {
      ++numContainers;
    }
    CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
        minimumAllocation, this, labelManager, nodePartition);
  }
  
  protected synchronized void releaseResource(Resource clusterResource,
      Resource resource, String nodePartition, boolean changeContainerResource) {
    queueUsage.decUsed(nodePartition, resource);

    CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
        minimumAllocation, this, labelManager, nodePartition);

    if (!changeContainerResource) {
      --numContainers;
    }
  }
  
  @Private
  public boolean getReservationContinueLooking() {
    return reservationsContinueLooking;
  }
  
  @Private
  public Map<AccessType, AccessControlList> getACLs() {
    return acls;
  }

  @Private
  public boolean getPreemptionDisabled() {
    return preemptionDisabled;
  }
  
  @Private
  public QueueCapacities getQueueCapacities() {
    return queueCapacities;
  }
  
  @Private
  public ResourceUsage getQueueResourceUsage() {
    return queueUsage;
  }

  /**
   * The specified queue is preemptable if system-wide preemption is turned on
   * unless any queue in the <em>qPath</em> hierarchy has explicitly turned
   * preemption off.
   * NOTE: Preemptability is inherited from a queue's parent.
   * 
   * @return true if queue has preemption disabled, false otherwise
   */
  private boolean isQueueHierarchyPreemptionDisabled(CSQueue q) {
    CapacitySchedulerConfiguration csConf = csContext.getConfiguration();
    boolean systemWidePreemption =
        csConf.getBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
                       YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS);
    CSQueue parentQ = q.getParent();

    // If the system-wide preemption switch is turned off, all of the queues in
    // the qPath hierarchy have preemption disabled, so return true.
    if (!systemWidePreemption) return true;

    // If q is the root queue and the system-wide preemption switch is turned
    // on, then q does not have preemption disabled (default=false, below)
    // unless the preemption_disabled property is explicitly set.
    if (parentQ == null) {
      return csConf.getPreemptionDisabled(q.getQueuePath(), false);
    }

    // If this is not the root queue, inherit the default value for the
    // preemption_disabled property from the parent. Preemptability will be
    // inherited from the parent's hierarchy unless explicitly overridden at
    // this level.
    return csConf.getPreemptionDisabled(q.getQueuePath(),
                                        parentQ.getPreemptionDisabled());
  }
  
  private Resource getCurrentLimitResource(String nodePartition,
      Resource clusterResource, ResourceLimits currentResourceLimits,
      SchedulingMode schedulingMode) {
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY) {
      /*
       * Current limit resource: For labeled resource: limit = queue-max-resource
       * (TODO, this part need update when we support labeled-limit) For
       * non-labeled resource: limit = min(queue-max-resource,
       * limit-set-by-parent)
       */
      Resource queueMaxResource =
          getQueueMaxResource(nodePartition, clusterResource);

      return Resources.min(resourceCalculator, clusterResource,
          queueMaxResource, currentResourceLimits.getLimit());
    } else if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      // When we doing non-exclusive resource allocation, maximum capacity of
      // all queues on this label equals to total resource with the label.
      return labelManager.getResourceByLabel(nodePartition, clusterResource);
    }
    
    return Resources.none();
  }

  Resource getQueueMaxResource(String nodePartition, Resource clusterResource) {
    return Resources.multiplyAndNormalizeDown(resourceCalculator,
        labelManager.getResourceByLabel(nodePartition, clusterResource),
        queueCapacities.getAbsoluteMaximumCapacity(nodePartition),
        minimumAllocation);
  }

  synchronized boolean canAssignToThisQueue(Resource clusterResource,
      String nodePartition, ResourceLimits currentResourceLimits,
      Resource resourceCouldBeUnreserved, SchedulingMode schedulingMode) {
    // Get current limited resource: 
    // - When doing RESPECT_PARTITION_EXCLUSIVITY allocation, we will respect
    // queues' max capacity.
    // - When doing IGNORE_PARTITION_EXCLUSIVITY allocation, we will not respect
    // queue's max capacity, queue's max capacity on the partition will be
    // considered to be 100%. Which is a queue can use all resource in the
    // partition. 
    // Doing this because: for non-exclusive allocation, we make sure there's
    // idle resource on the partition, to avoid wastage, such resource will be
    // leveraged as much as we can, and preemption policy will reclaim it back
    // when partitoned-resource-request comes back.  
    Resource currentLimitResource =
        getCurrentLimitResource(nodePartition, clusterResource,
            currentResourceLimits, schedulingMode);

    Resource nowTotalUsed = queueUsage.getUsed(nodePartition);

    // Set headroom for currentResourceLimits:
    // When queue is a parent queue: Headroom = limit - used + killable
    // When queue is a leaf queue: Headroom = limit - used (leaf queue cannot preempt itself)
    Resource usedExceptKillable = nowTotalUsed;
    if (null != getChildQueues() && !getChildQueues().isEmpty()) {
      usedExceptKillable = Resources.subtract(nowTotalUsed,
          getTotalKillableResource(nodePartition));
    }
    currentResourceLimits.setHeadroom(
        Resources.subtract(currentLimitResource, usedExceptKillable));

    if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource,
        usedExceptKillable, currentLimitResource)) {

      // if reservation continous looking enabled, check to see if could we
      // potentially use this node instead of a reserved node if the application
      // has reserved containers.
      // TODO, now only consider reservation cases when the node has no label
      if (this.reservationsContinueLooking
          && nodePartition.equals(RMNodeLabelsManager.NO_LABEL)
          && Resources.greaterThan(resourceCalculator, clusterResource,
              resourceCouldBeUnreserved, Resources.none())) {
        // resource-without-reserved = used - reserved
        Resource newTotalWithoutReservedResource =
            Resources.subtract(usedExceptKillable, resourceCouldBeUnreserved);

        // when total-used-without-reserved-resource < currentLimit, we still
        // have chance to allocate on this node by unreserving some containers
        if (Resources.lessThan(resourceCalculator, clusterResource,
            newTotalWithoutReservedResource, currentLimitResource)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("try to use reserved: " + getQueueName()
                + " usedResources: " + queueUsage.getUsed()
                + ", clusterResources: " + clusterResource
                + ", reservedResources: " + resourceCouldBeUnreserved
                + ", capacity-without-reserved: "
                + newTotalWithoutReservedResource + ", maxLimitCapacity: "
                + currentLimitResource);
          }
          return true;
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(getQueueName()
            + "Check assign to queue, nodePartition="
            + nodePartition
            + " usedResources: "
            + queueUsage.getUsed(nodePartition)
            + " clusterResources: "
            + clusterResource
            + " currentUsedCapacity "
            + Resources.divide(resourceCalculator, clusterResource,
                queueUsage.getUsed(nodePartition),
                labelManager.getResourceByLabel(nodePartition, clusterResource))
            + " max-capacity: "
            + queueCapacities.getAbsoluteMaximumCapacity(nodePartition) + ")");
      }
      return false;
    }
    return true;
  }

  @Override
  public void incReservedResource(String partition, Resource reservedRes) {
    if (partition == null) {
      partition = RMNodeLabelsManager.NO_LABEL;
    }

    queueUsage.incReserved(partition, reservedRes);
    if(null != parent){
      parent.incReservedResource(partition, reservedRes);
    }
  }

  @Override
  public void decReservedResource(String partition, Resource reservedRes) {
    if (partition == null) {
      partition = RMNodeLabelsManager.NO_LABEL;
    }

    queueUsage.decReserved(partition, reservedRes);
    if(null != parent){
      parent.decReservedResource(partition, reservedRes);
    }
  }

  @Override
  public void incPendingResource(String nodeLabel, Resource resourceToInc) {
    if (nodeLabel == null) {
      nodeLabel = RMNodeLabelsManager.NO_LABEL;
    }
    // ResourceUsage has its own lock, no addition lock needs here.
    queueUsage.incPending(nodeLabel, resourceToInc);
    if (null != parent) {
      parent.incPendingResource(nodeLabel, resourceToInc);
    }
  }
  
  @Override
  public void decPendingResource(String nodeLabel, Resource resourceToDec) {
    if (nodeLabel == null) {
      nodeLabel = RMNodeLabelsManager.NO_LABEL;
    }
    // ResourceUsage has its own lock, no addition lock needs here.
    queueUsage.decPending(nodeLabel, resourceToDec);
    if (null != parent) {
      parent.decPendingResource(nodeLabel, resourceToDec);
    }
  }
  
  @Override
  public void incUsedResource(String nodeLabel, Resource resourceToInc,
      SchedulerApplicationAttempt application) {
    if (nodeLabel == null) {
      nodeLabel = RMNodeLabelsManager.NO_LABEL;
    }
    // ResourceUsage has its own lock, no addition lock needs here.
    queueUsage.incUsed(nodeLabel, resourceToInc);
    CSQueueUtils.updateUsedCapacity(resourceCalculator,
        labelManager.getResourceByLabel(nodeLabel, Resources.none()),
        minimumAllocation, queueUsage, queueCapacities, nodeLabel);
    if (null != parent) {
      parent.incUsedResource(nodeLabel, resourceToInc, null);
    }
  }

  @Override
  public void decUsedResource(String nodeLabel, Resource resourceToDec,
      SchedulerApplicationAttempt application) {
    if (nodeLabel == null) {
      nodeLabel = RMNodeLabelsManager.NO_LABEL;
    }
    // ResourceUsage has its own lock, no addition lock needs here.
    queueUsage.decUsed(nodeLabel, resourceToDec);
    CSQueueUtils.updateUsedCapacity(resourceCalculator,
        labelManager.getResourceByLabel(nodeLabel, Resources.none()),
        minimumAllocation, queueUsage, queueCapacities, nodeLabel);
    if (null != parent) {
      parent.decUsedResource(nodeLabel, resourceToDec, null);
    }
  }

  /**
   * Return if the queue has pending resource on given nodePartition and
   * schedulingMode. 
   */
  boolean hasPendingResourceRequest(String nodePartition, 
      Resource cluster, SchedulingMode schedulingMode) {
    return SchedulerUtils.hasPendingResourceRequest(resourceCalculator,
        queueUsage, nodePartition, cluster, schedulingMode);
  }
  
  public boolean accessibleToPartition(String nodePartition) {
    // if queue's label is *, it can access any node
    if (accessibleLabels != null
        && accessibleLabels.contains(RMNodeLabelsManager.ANY)) {
      return true;
    }
    // any queue can access to a node without label
    if (nodePartition == null
        || nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      return true;
    }
    // a queue can access to a node only if it contains any label of the node
    if (accessibleLabels != null && accessibleLabels.contains(nodePartition)) {
      return true;
    }
    // sorry, you cannot access
    return false;
  }

  @Override
  public Priority getDefaultApplicationPriority() {
    // TODO add dummy implementation
    return null;
  }

  @Override
  public Set<String> getNodeLabelsForQueue() {
    // if queue's label is *, queue can access any labels. Instead of
    // considering all labels in cluster, only those labels which are
    // use some resource of this queue can be considered.
    Set<String> nodeLabels = new HashSet<String>();
    if (this.getAccessibleNodeLabels() != null && this.getAccessibleNodeLabels()
        .contains(RMNodeLabelsManager.ANY)) {
      nodeLabels.addAll(Sets.union(this.getQueueCapacities().getNodePartitionsSet(),
          this.getQueueResourceUsage().getNodePartitionsSet()));
    } else {
      nodeLabels.addAll(this.getAccessibleNodeLabels());
    }

    // Add NO_LABEL also to this list as NO_LABEL also can be granted with
    // resource in many general cases.
    if (!nodeLabels.contains(RMNodeLabelsManager.NO_LABEL)) {
      nodeLabels.add(RMNodeLabelsManager.NO_LABEL);
    }
    return nodeLabels;
  }

  public Resource getTotalKillableResource(String partition) {
    return csContext.getPreemptionManager().getKillableResource(queueName,
        partition);
  }

  public Iterator<RMContainer> getKillableContainers(String partition) {
    return csContext.getPreemptionManager().getKillableContainers(queueName,
        partition);
  }
}
