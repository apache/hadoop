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
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
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
  Resource maximumAllocation;
  QueueState state;
  final QueueMetrics metrics;
  protected final PrivilegedEntity queueEntity;

  final ResourceCalculator resourceCalculator;
  Set<String> accessibleLabels;
  RMNodeLabelsManager labelManager;
  String defaultLabelExpression;
  
  Map<AccessType, AccessControlList> acls = 
      new HashMap<AccessType, AccessControlList>();
  boolean reservationsContinueLooking;
  private boolean preemptionDisabled;

  // Track resource usage-by-label like used-resource/pending-resource, etc.
  ResourceUsage queueUsage;
  
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
    this.metrics = old != null ? old.getMetrics() :
        QueueMetrics.forQueue(getQueuePath(), parent,
            cs.getConfiguration().getEnableUserMetrics(),
            cs.getConf());

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
  public synchronized float getAbsoluteUsedCapacity() {
    return queueCapacities.getAbsoluteUsedCapacity();
  }

  @Override
  public float getMaximumCapacity() {
    return queueCapacities.getMaximumCapacity();
  }

  @Override
  public synchronized float getUsedCapacity() {
    return queueCapacities.getUsedCapacity();
  }

  @Override
  public Resource getUsedResources() {
    return queueUsage.getUsed();
  }

  public synchronized int getNumContainers() {
    return numContainers;
  }

  @Override
  public synchronized QueueState getState() {
    return state;
  }
  
  @Override
  public QueueMetrics getMetrics() {
    return metrics;
  }
  
  @Override
  public String getQueueName() {
    return queueName;
  }

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
    return authorizer.checkPermission(SchedulerUtils.toAccessType(acl),
      queueEntity, user);
  }

  @Override
  public synchronized void setUsedCapacity(float usedCapacity) {
    queueCapacities.setUsedCapacity(usedCapacity);
  }
  
  @Override
  public synchronized void setAbsoluteUsedCapacity(float absUsedCapacity) {
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
    CSQueueUtils.updateQueueStatistics(
        resourceCalculator, this, parent, clusterResource, minimumAllocation);
    
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
    return queueInfo;
  }
  
  @Private
  public synchronized Resource getMaximumAllocation() {
    return maximumAllocation;
  }
  
  @Private
  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }
  
  synchronized void allocateResource(Resource clusterResource, 
      Resource resource, Set<String> nodeLabels) {
    
    // Update usedResources by labels
    if (nodeLabels == null || nodeLabels.isEmpty()) {
      queueUsage.incUsed(resource);
    } else {
      for (String label : Sets.intersection(accessibleLabels, nodeLabels)) {
        queueUsage.incUsed(label, resource);
      }
    }

    ++numContainers;
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
  }
  
  protected synchronized void releaseResource(Resource clusterResource,
      Resource resource, Set<String> nodeLabels) {
    // Update usedResources by labels
    if (null == nodeLabels || nodeLabels.isEmpty()) {
      queueUsage.decUsed(resource);
    } else {
      for (String label : Sets.intersection(accessibleLabels, nodeLabels)) {
        queueUsage.decUsed(label, resource);
      }
    }

    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation);
    --numContainers;
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
  
  private Resource getCurrentLimitResource(String nodeLabel,
      Resource clusterResource, ResourceLimits currentResourceLimits) {
    /*
     * Current limit resource: For labeled resource: limit = queue-max-resource
     * (TODO, this part need update when we support labeled-limit) For
     * non-labeled resource: limit = min(queue-max-resource,
     * limit-set-by-parent)
     */
    Resource queueMaxResource =
        Resources.multiplyAndNormalizeDown(resourceCalculator,
            labelManager.getResourceByLabel(nodeLabel, clusterResource),
            queueCapacities.getAbsoluteMaximumCapacity(nodeLabel), minimumAllocation);
    if (nodeLabel.equals(RMNodeLabelsManager.NO_LABEL)) {
      return Resources.min(resourceCalculator, clusterResource,
          queueMaxResource, currentResourceLimits.getLimit());
    }
    return queueMaxResource;
  }
  
  synchronized boolean canAssignToThisQueue(Resource clusterResource,
      Set<String> nodeLabels, ResourceLimits currentResourceLimits,
      Resource nowRequired, Resource resourceCouldBeUnreserved) {
    // Get label of this queue can access, it's (nodeLabel AND queueLabel)
    Set<String> labelCanAccess;
    if (null == nodeLabels || nodeLabels.isEmpty()) {
      labelCanAccess = new HashSet<String>();
      // Any queue can always access any node without label
      labelCanAccess.add(RMNodeLabelsManager.NO_LABEL);
    } else {
      labelCanAccess = new HashSet<String>(
          accessibleLabels.contains(CommonNodeLabelsManager.ANY) ? nodeLabels
              : Sets.intersection(accessibleLabels, nodeLabels));
    }
    
    for (String label : labelCanAccess) {
      // New total resource = used + required
      Resource newTotalResource =
          Resources.add(queueUsage.getUsed(label), nowRequired);

      Resource currentLimitResource =
          getCurrentLimitResource(label, clusterResource, currentResourceLimits);

      if (Resources.greaterThan(resourceCalculator, clusterResource,
          newTotalResource, currentLimitResource)) {

        // if reservation continous looking enabled, check to see if could we
        // potentially use this node instead of a reserved node if the application
        // has reserved containers.
        // TODO, now only consider reservation cases when the node has no label
        if (this.reservationsContinueLooking
            && label.equals(RMNodeLabelsManager.NO_LABEL)
            && Resources.greaterThan(resourceCalculator, clusterResource,
            resourceCouldBeUnreserved, Resources.none())) {
          // resource-without-reserved = used - reserved
          Resource newTotalWithoutReservedResource =
              Resources.subtract(newTotalResource, resourceCouldBeUnreserved);

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
            currentResourceLimits.setAmountNeededUnreserve(Resources.subtract(newTotalResource,
                currentLimitResource));
            return true;
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(getQueueName()
              + "Check assign to queue, label=" + label
              + " usedResources: " + queueUsage.getUsed(label)
              + " clusterResources: " + clusterResource
              + " currentUsedCapacity "
              + Resources.divide(resourceCalculator, clusterResource,
              queueUsage.getUsed(label),
              labelManager.getResourceByLabel(label, clusterResource))
              + " max-capacity: "
              + queueCapacities.getAbsoluteMaximumCapacity(label)
              + ")");
        }
        return false;
      }
      return true;
    }
    
    // Actually, this will not happen, since labelCanAccess will be always
    // non-empty
    return false;
  }
}
