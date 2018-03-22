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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueConfigurations;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AbsoluteResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SimpleCandidateNodeSet;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.Sets;

public abstract class AbstractCSQueue implements CSQueue {

  private static final Log LOG = LogFactory.getLog(AbstractCSQueue.class);  
  volatile CSQueue parent;
  final String queueName;
  private final String queuePath;
  volatile int numContainers;

  final Resource minimumAllocation;
  volatile Resource maximumAllocation;
  private volatile QueueState state = null;
  final CSQueueMetrics metrics;
  protected final PrivilegedEntity queueEntity;

  final ResourceCalculator resourceCalculator;
  Set<String> accessibleLabels;
  Set<String> resourceTypes;
  final RMNodeLabelsManager labelManager;
  String defaultLabelExpression;
  
  Map<AccessType, AccessControlList> acls = 
      new HashMap<AccessType, AccessControlList>();
  volatile boolean reservationsContinueLooking;
  private volatile boolean preemptionDisabled;
  // Indicates if the in-queue preemption setting is ever disabled within the
  // hierarchy of this queue.
  private boolean intraQueuePreemptionDisabledInHierarchy;

  // Track resource usage-by-label like used-resource/pending-resource, etc.
  volatile ResourceUsage queueUsage;
  
  // Track capacities like used-capcity/abs-used-capacity/capacity/abs-capacity,
  // etc.
  QueueCapacities queueCapacities;

  QueueResourceQuotas queueResourceQuotas;

  protected enum CapacityConfigType {
    NONE, PERCENTAGE, ABSOLUTE_RESOURCE
  };
  protected CapacityConfigType capacityConfigType =
      CapacityConfigType.NONE;

  private final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);
  protected CapacitySchedulerContext csContext;
  protected YarnAuthorizationProvider authorizer = null;

  protected ActivitiesManager activitiesManager;

  protected ReentrantReadWriteLock.ReadLock readLock;
  protected ReentrantReadWriteLock.WriteLock writeLock;

  volatile Priority priority = Priority.newInstance(0);
  private Map<String, Float> userWeights = new HashMap<String, Float>();

  public AbstractCSQueue(CapacitySchedulerContext cs,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this(cs, cs.getConfiguration(), queueName, parent, old);
  }

  public AbstractCSQueue(CapacitySchedulerContext cs,
      CapacitySchedulerConfiguration configuration, String queueName,
      CSQueue parent, CSQueue old) {

    this.labelManager = cs.getRMContext().getNodeLabelManager();
    this.parent = parent;
    this.queueName = queueName;
    this.queuePath = ((parent == null) ? "" : (parent.getQueuePath() + "."))
        + this.queueName;
    this.resourceCalculator = cs.getResourceCalculator();
    this.activitiesManager = cs.getActivitiesManager();

    // must be called after parent and queueName is set
    this.metrics = old != null ?
        (CSQueueMetrics) old.getMetrics() :
        CSQueueMetrics.forQueue(getQueuePath(), parent,
            cs.getConfiguration().getEnableUserMetrics(), cs.getConf());

    this.csContext = cs;
    this.minimumAllocation = csContext.getMinimumResourceCapability();

    // initialize ResourceUsage
    queueUsage = new ResourceUsage();
    queueEntity = new PrivilegedEntity(EntityType.QUEUE, getQueuePath());

    // initialize QueueCapacities
    queueCapacities = new QueueCapacities(parent == null);

    // initialize queueResourceQuotas
    queueResourceQuotas = new QueueResourceQuotas();

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  protected void setupConfigurableCapacities() {
    setupConfigurableCapacities(csContext.getConfiguration());
  }

  protected void setupConfigurableCapacities(
      CapacitySchedulerConfiguration configuration) {
    CSQueueUtils.loadUpdateAndCheckCapacities(
        getQueuePath(),
        configuration,
        queueCapacities,
        parent == null ? null : parent.getQueueCapacities());
  }

  @Override
  public String getQueuePath() {
    return queuePath;
  }
  
  @Override
  public float getCapacity() {
    return queueCapacities.getCapacity();
  }

  @Override
  public float getAbsoluteCapacity() {
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
  public QueueState getState() {
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
  public CSQueue getParent() {
    return parent;
  }

  @Override
  public void setParent(CSQueue newParentQueue) {
    this.parent = newParentQueue;
  }
  
  public Set<String> getAccessibleNodeLabels() {
    return accessibleLabels;
  }

  @Override
  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    return authorizer.checkPermission(
        new AccessRequest(queueEntity, user, SchedulerUtils.toAccessType(acl),
            null, null, Server.getRemoteAddress(), null));
  }

  /**
   * Set maximum capacity - used only for testing.
   * @param maximumCapacity new max capacity
   */
  void setMaxCapacity(float maximumCapacity) {
    try {
      writeLock.lock();
      // Sanity check
      CSQueueUtils.checkMaxCapacity(getQueueName(),
          queueCapacities.getCapacity(), maximumCapacity);
      float absMaxCapacity = CSQueueUtils.computeAbsoluteMaximumCapacity(
          maximumCapacity, parent);
      CSQueueUtils.checkAbsoluteCapacity(getQueueName(),
          queueCapacities.getAbsoluteCapacity(), absMaxCapacity);

      queueCapacities.setMaximumCapacity(maximumCapacity);
      queueCapacities.setAbsoluteMaximumCapacity(absMaxCapacity);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Set maximum capacity
   * @param maximumCapacity new max capacity
   */
  void setMaxCapacity(String nodeLabel, float maximumCapacity) {
    try {
      writeLock.lock();
      // Sanity check
      CSQueueUtils.checkMaxCapacity(getQueueName(),
          queueCapacities.getCapacity(nodeLabel), maximumCapacity);
      float absMaxCapacity = CSQueueUtils.computeAbsoluteMaximumCapacity(
          maximumCapacity, parent);
      CSQueueUtils.checkAbsoluteCapacity(getQueueName(),
          queueCapacities.getAbsoluteCapacity(nodeLabel), absMaxCapacity);

      queueCapacities.setMaximumCapacity(maximumCapacity);
      queueCapacities.setAbsoluteMaximumCapacity(absMaxCapacity);
    } finally {
      writeLock.unlock();
    }
  }


  @Override
  public String getDefaultNodeLabelExpression() {
    return defaultLabelExpression;
  }
  
  void setupQueueConfigs(Resource clusterResource)
      throws IOException {
    setupQueueConfigs(clusterResource, csContext.getConfiguration());
  }

  protected void setupQueueConfigs(Resource clusterResource,
      CapacitySchedulerConfiguration configuration) throws
      IOException {

    try {
      writeLock.lock();
      // get labels
      this.accessibleLabels =
          configuration.getAccessibleNodeLabels(getQueuePath());
      this.defaultLabelExpression =
          configuration.getDefaultNodeLabelExpression(
              getQueuePath());
      this.resourceTypes = new HashSet<String>();
      for (AbsoluteResourceType type : AbsoluteResourceType.values()) {
        resourceTypes.add(type.toString().toLowerCase());
      }

      // inherit from parent if labels not set
      if (this.accessibleLabels == null && parent != null) {
        this.accessibleLabels = parent.getAccessibleNodeLabels();
      }

      // inherit from parent if labels not set
      if (this.defaultLabelExpression == null && parent != null
          && this.accessibleLabels.containsAll(
          parent.getAccessibleNodeLabels())) {
        this.defaultLabelExpression = parent.getDefaultNodeLabelExpression();
      }

      // After we setup labels, we can setup capacities
      setupConfigurableCapacities(configuration);

      // Also fetch minimum/maximum resource constraint for this queue if
      // configured.
      capacityConfigType = CapacityConfigType.NONE;
      updateConfigurableResourceRequirement(getQueuePath(), clusterResource);

      this.maximumAllocation =
          configuration.getMaximumAllocationPerQueue(
              getQueuePath());

      // initialized the queue state based on previous state, configured state
      // and its parent state.
      QueueState previous = getState();
      QueueState configuredState = configuration
          .getConfiguredState(getQueuePath());
      QueueState parentState = (parent == null) ? null : parent.getState();
      initializeQueueState(previous, configuredState, parentState);

      authorizer = YarnAuthorizationProvider.getInstance(csContext.getConf());

      this.acls = configuration.getAcls(getQueuePath());

      // Update metrics
      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, null);

      // Check if labels of this queue is a subset of parent queue, only do this
      // when we not root
      if (parent != null && parent.getParent() != null) {
        if (parent.getAccessibleNodeLabels() != null && !parent
            .getAccessibleNodeLabels().contains(RMNodeLabelsManager.ANY)) {
          // if parent isn't "*", child shouldn't be "*" too
          if (this.getAccessibleNodeLabels().contains(
              RMNodeLabelsManager.ANY)) {
            throw new IOException("Parent's accessible queue is not ANY(*), "
                + "but child's accessible queue is *");
          } else{
            Set<String> diff = Sets.difference(this.getAccessibleNodeLabels(),
                parent.getAccessibleNodeLabels());
            if (!diff.isEmpty()) {
              throw new IOException(
                  "Some labels of child queue is not a subset "
                      + "of parent queue, these labels=[" + StringUtils
                      .join(diff, ",") + "]");
            }
          }
        }
      }

      this.reservationsContinueLooking =
          csContext.getConfiguration().getReservationContinueLook();

      this.preemptionDisabled = isQueueHierarchyPreemptionDisabled(this,
          configuration);
      this.intraQueuePreemptionDisabledInHierarchy =
          isIntraQueueHierarchyPreemptionDisabled(this, configuration);

      this.priority = configuration.getQueuePriority(
          getQueuePath());

      this.userWeights = getUserWeightsFromHierarchy(configuration);
    } finally {
      writeLock.unlock();
    }
  }

  private Map<String, Float> getUserWeightsFromHierarchy
      (CapacitySchedulerConfiguration configuration) throws
      IOException {
    Map<String, Float> unionInheritedWeights = new HashMap<String, Float>();
    CSQueue parentQ = getParent();
    if (parentQ != null) {
      // Inherit all of parent's user's weights
      unionInheritedWeights.putAll(parentQ.getUserWeights());
    }
    // Insert this queue's user's weights, overriding parent's user's weights if
    // there is overlap.
    unionInheritedWeights.putAll(
        configuration.getAllUserWeightsForQueue(getQueuePath()));
    return unionInheritedWeights;
  }

  protected void updateConfigurableResourceRequirement(String queuePath,
      Resource clusterResource) {
    CapacitySchedulerConfiguration conf = csContext.getConfiguration();
    Set<String> configuredNodelabels = conf.getConfiguredNodeLabels(queuePath);

    for (String label : configuredNodelabels) {
      Resource minResource = conf.getMinimumResourceRequirement(label,
          queuePath, resourceTypes);
      Resource maxResource = conf.getMaximumResourceRequirement(label,
          queuePath, resourceTypes);

      if (LOG.isDebugEnabled()) {
        LOG.debug("capacityConfigType is '" + capacityConfigType
            + "' for queue '" + getQueueName());
      }
      if (this.capacityConfigType.equals(CapacityConfigType.NONE)) {
        this.capacityConfigType = (!minResource.equals(Resources.none())
            && queueCapacities.getAbsoluteCapacity(label) == 0f)
                ? CapacityConfigType.ABSOLUTE_RESOURCE
                : CapacityConfigType.PERCENTAGE;
        if (LOG.isDebugEnabled()) {
          LOG.debug("capacityConfigType is updated as '" + capacityConfigType
              + "' for queue '" + getQueueName());
        }
      }

      validateAbsoluteVsPercentageCapacityConfig(minResource);

      // If min resource for a resource type is greater than its max resource,
      // throw exception to handle such error configs.
      if (!maxResource.equals(Resources.none()) && Resources.greaterThan(
          resourceCalculator, clusterResource, minResource, maxResource)) {
        throw new IllegalArgumentException("Min resource configuration "
            + minResource + " is greater than its max value:" + maxResource
            + " in queue:" + getQueueName());
      }

      // If parent's max resource is lesser to a specific child's max
      // resource, throw exception to handle such error configs.
      if (parent != null) {
        Resource parentMaxRes = parent.getQueueResourceQuotas()
            .getConfiguredMaxResource(label);
        if (Resources.greaterThan(resourceCalculator, clusterResource,
            parentMaxRes, Resources.none())) {
          if (Resources.greaterThan(resourceCalculator, clusterResource,
              maxResource, parentMaxRes)) {
            throw new IllegalArgumentException("Max resource configuration "
                + maxResource + " is greater than parents max value:"
                + parentMaxRes + " in queue:" + getQueueName());
          }

          // If child's max resource is not set, but its parent max resource is
          // set, we must set child max resource to its parent's.
          if (maxResource.equals(Resources.none())
              && !minResource.equals(Resources.none())) {
            maxResource = Resources.clone(parentMaxRes);
          }
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating absolute resource configuration for queue:"
            + getQueueName() + " as minResource=" + minResource
            + " and maxResource=" + maxResource);
      }

      queueResourceQuotas.setConfiguredMinResource(label, minResource);
      queueResourceQuotas.setConfiguredMaxResource(label, maxResource);
    }
  }

  private void validateAbsoluteVsPercentageCapacityConfig(
      Resource minResource) {
    CapacityConfigType localType = CapacityConfigType.PERCENTAGE;
    if (!minResource.equals(Resources.none())) {
      localType = CapacityConfigType.ABSOLUTE_RESOURCE;
    }

    if (!queueName.equals("root")
        && !this.capacityConfigType.equals(localType)) {
      throw new IllegalArgumentException("Queue '" + getQueueName()
          + "' should use either percentage based capacity"
          + " configuration or absolute resource.");
    }
  }

  @Override
  public CapacityConfigType getCapacityConfigType() {
    return capacityConfigType;
  }

  @Override
  public Resource getEffectiveCapacity(String label) {
    return Resources
        .clone(getQueueResourceQuotas().getEffectiveMinResource(label));
  }

  @Override
  public Resource getEffectiveCapacityDown(String label, Resource factor) {
    return Resources.normalizeDown(resourceCalculator,
        getQueueResourceQuotas().getEffectiveMinResource(label),
        minimumAllocation);
  }

  @Override
  public Resource getEffectiveMaxCapacity(String label) {
    return Resources
        .clone(getQueueResourceQuotas().getEffectiveMaxResource(label));
  }

  @Override
  public Resource getEffectiveMaxCapacityDown(String label, Resource factor) {
    return Resources.normalizeDown(resourceCalculator,
        getQueueResourceQuotas().getEffectiveMaxResource(label),
        minimumAllocation);
  }

  private void initializeQueueState(QueueState previousState,
      QueueState configuredState, QueueState parentState) {
    // verify that we can not any value for State other than RUNNING/STOPPED
    if (configuredState != null && configuredState != QueueState.RUNNING
        && configuredState != QueueState.STOPPED) {
      throw new IllegalArgumentException("Invalid queue state configuration."
          + " We can only use RUNNING or STOPPED.");
    }
    // If we did not set state in configuration, use Running as default state
    QueueState defaultState = QueueState.RUNNING;

    if (previousState == null) {
      // If current state of the queue is null, we would inherit the state
      // from its parent. If this queue does not has parent, such as root queue,
      // we would use the configured state.
      if (parentState == null) {
        updateQueueState((configuredState == null) ? defaultState
            : configuredState);
      } else {
        if (configuredState == null) {
          updateQueueState((parentState == QueueState.DRAINING) ?
              QueueState.STOPPED : parentState);
        } else if (configuredState == QueueState.RUNNING
            && parentState != QueueState.RUNNING) {
          throw new IllegalArgumentException(
              "The parent queue:" + parent.getQueueName()
              + " state is STOPPED, child queue:" + queueName
              + " state cannot be RUNNING.");
        } else {
          updateQueueState(configuredState);
        }
      }
    } else {
      // when we get a refreshQueue request from AdminService,
      if (previousState == QueueState.RUNNING) {
        if (configuredState == QueueState.STOPPED) {
          stopQueue();
        }
      } else {
        if (configuredState == QueueState.RUNNING) {
          try {
            activeQueue();
          } catch (YarnException ex) {
            throw new IllegalArgumentException(ex.getMessage());
          }
        }
      }
    }
  }

  protected QueueInfo getQueueInfo() {
    // Deliberately doesn't use lock here, because this method will be invoked
    // from schedulerApplicationAttempt, to avoid deadlock, sacrifice
    // consistency here.
    // TODO, improve this
    QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    queueInfo.setQueueName(queueName);
    queueInfo.setAccessibleNodeLabels(accessibleLabels);
    queueInfo.setCapacity(queueCapacities.getCapacity());
    queueInfo.setMaximumCapacity(queueCapacities.getMaximumCapacity());
    queueInfo.setQueueState(getState());
    queueInfo.setDefaultNodeLabelExpression(defaultLabelExpression);
    queueInfo.setCurrentCapacity(getUsedCapacity());
    queueInfo.setQueueStatistics(getQueueStatistics());
    queueInfo.setPreemptionDisabled(preemptionDisabled);
    queueInfo.setIntraQueuePreemptionDisabled(
        getIntraQueuePreemptionDisabled());
    queueInfo.setQueueConfigurations(getQueueConfigurations());
    return queueInfo;
  }

  public QueueStatistics getQueueStatistics() {
    // Deliberately doesn't use lock here, because this method will be invoked
    // from schedulerApplicationAttempt, to avoid deadlock, sacrifice
    // consistency here.
    // TODO, improve this
    QueueStatistics stats = recordFactory.newRecordInstance(
        QueueStatistics.class);
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
  
  public Map<String, QueueConfigurations> getQueueConfigurations() {
    Map<String, QueueConfigurations> queueConfigurations = new HashMap<>();
    Set<String> nodeLabels = getNodeLabelsForQueue();
    for (String nodeLabel : nodeLabels) {
      QueueConfigurations queueConfiguration =
          recordFactory.newRecordInstance(QueueConfigurations.class);
      float capacity = queueCapacities.getCapacity(nodeLabel);
      float absoluteCapacity = queueCapacities.getAbsoluteCapacity(nodeLabel);
      float maxCapacity = queueCapacities.getMaximumCapacity(nodeLabel);
      float absMaxCapacity =
          queueCapacities.getAbsoluteMaximumCapacity(nodeLabel);
      float maxAMPercentage =
          queueCapacities.getMaxAMResourcePercentage(nodeLabel);
      queueConfiguration.setCapacity(capacity);
      queueConfiguration.setAbsoluteCapacity(absoluteCapacity);
      queueConfiguration.setMaxCapacity(maxCapacity);
      queueConfiguration.setAbsoluteMaxCapacity(absMaxCapacity);
      queueConfiguration.setMaxAMPercentage(maxAMPercentage);
      queueConfiguration.setConfiguredMinCapacity(
          queueResourceQuotas.getConfiguredMinResource(nodeLabel));
      queueConfiguration.setConfiguredMaxCapacity(
          queueResourceQuotas.getConfiguredMaxResource(nodeLabel));
      queueConfiguration.setEffectiveMinCapacity(
          queueResourceQuotas.getEffectiveMinResource(nodeLabel));
      queueConfiguration.setEffectiveMaxCapacity(
          queueResourceQuotas.getEffectiveMaxResource(nodeLabel));
      queueConfigurations.put(nodeLabel, queueConfiguration);
    }
    return queueConfigurations;
  }

  @Private
  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }
  
  @Private
  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }
  
  void allocateResource(Resource clusterResource,
      Resource resource, String nodePartition) {
    try {
      writeLock.lock();
      queueUsage.incUsed(nodePartition, resource);

      ++numContainers;

      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, nodePartition);
    } finally {
      writeLock.unlock();
    }
  }
  
  protected void releaseResource(Resource clusterResource,
      Resource resource, String nodePartition) {
    try {
      writeLock.lock();
      queueUsage.decUsed(nodePartition, resource);

      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, nodePartition);

      --numContainers;
    } finally {
      writeLock.unlock();
    }
  }
  
  @Private
  public boolean getReservationContinueLooking() {
    return reservationsContinueLooking;
  }
  
  @Private
  public Map<AccessType, AccessControlList> getACLs() {
    try {
      readLock.lock();
      return acls;
    } finally {
      readLock.unlock();
    }
  }

  @Private
  public boolean getPreemptionDisabled() {
    return preemptionDisabled;
  }

  @Private
  public boolean getIntraQueuePreemptionDisabled() {
    return intraQueuePreemptionDisabledInHierarchy || preemptionDisabled;
  }

  @Private
  public boolean getIntraQueuePreemptionDisabledInHierarchy() {
    return intraQueuePreemptionDisabledInHierarchy;
  }
  
  @Private
  public QueueCapacities getQueueCapacities() {
    return queueCapacities;
  }
  
  @Private
  public ResourceUsage getQueueResourceUsage() {
    return queueUsage;
  }

  @Override
  public QueueResourceQuotas getQueueResourceQuotas() {
    return queueResourceQuotas;
  }

  @Override
  public ReentrantReadWriteLock.ReadLock getReadLock() {
    return readLock;
  }

  /**
   * The specified queue is cross-queue preemptable if system-wide cross-queue
   * preemption is turned on unless any queue in the <em>qPath</em> hierarchy
   * has explicitly turned cross-queue preemption off.
   * NOTE: Cross-queue preemptability is inherited from a queue's parent.
   *
   * @param q queue to check preemption state
   * @param configuration capacity scheduler config
   * @return true if queue has cross-queue preemption disabled, false otherwise
   */
  private boolean isQueueHierarchyPreemptionDisabled(CSQueue q,
      CapacitySchedulerConfiguration configuration) {
    boolean systemWidePreemption =
        csContext.getConfiguration()
            .getBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
                       YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS);
    CSQueue parentQ = q.getParent();

    // If the system-wide preemption switch is turned off, all of the queues in
    // the qPath hierarchy have preemption disabled, so return true.
    if (!systemWidePreemption) return true;

    // If q is the root queue and the system-wide preemption switch is turned
    // on, then q does not have preemption disabled (default=false, below)
    // unless the preemption_disabled property is explicitly set.
    if (parentQ == null) {
      return configuration.getPreemptionDisabled(q.getQueuePath(), false);
    }

    // If this is not the root queue, inherit the default value for the
    // preemption_disabled property from the parent. Preemptability will be
    // inherited from the parent's hierarchy unless explicitly overridden at
    // this level.
    return configuration.getPreemptionDisabled(q.getQueuePath(),
                                        parentQ.getPreemptionDisabled());
  }

  /**
   * The specified queue is intra-queue preemptable if
   * 1) system-wide intra-queue preemption is turned on
   * 2) no queue in the <em>qPath</em> hierarchy has explicitly turned off intra
   *    queue preemption.
   * NOTE: Intra-queue preemptability is inherited from a queue's parent.
   *
   * @param q queue to check intra-queue preemption state
   * @param configuration capacity scheduler config
   * @return true if queue has intra-queue preemption disabled, false otherwise
   */
  private boolean isIntraQueueHierarchyPreemptionDisabled(CSQueue q,
      CapacitySchedulerConfiguration configuration) {
    boolean systemWideIntraQueuePreemption =
        csContext.getConfiguration().getBoolean(
            CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED,
            CapacitySchedulerConfiguration
            .DEFAULT_INTRAQUEUE_PREEMPTION_ENABLED);
    // Intra-queue preemption is disabled for this queue if the system-wide
    // intra-queue preemption flag is false
    if (!systemWideIntraQueuePreemption) return true;

    // Check if this is the root queue and the root queue's intra-queue
    // preemption disable switch is set
    CSQueue parentQ = q.getParent();
    if (parentQ == null) {
      return configuration
          .getIntraQueuePreemptionDisabled(q.getQueuePath(), false);
    }

    // At this point, the master preemption switch is enabled down to this
    // queue's level. Determine whether or not intra-queue preemption is enabled
    // down to this queu's level and return that value.
    return configuration.getIntraQueuePreemptionDisabled(q.getQueuePath(),
        parentQ.getIntraQueuePreemptionDisabledInHierarchy());
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
          getQueueMaxResource(nodePartition);

      return Resources.min(resourceCalculator, clusterResource,
          queueMaxResource, currentResourceLimits.getLimit());
    } else if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      // When we doing non-exclusive resource allocation, maximum capacity of
      // all queues on this label equals to total resource with the label.
      return labelManager.getResourceByLabel(nodePartition, clusterResource);
    }
    
    return Resources.none();
  }

  Resource getQueueMaxResource(String nodePartition) {
    return getEffectiveMaxCapacity(nodePartition);
  }

  public boolean hasChildQueues() {
    List<CSQueue> childQueues = getChildQueues();
    return childQueues != null && !childQueues.isEmpty();
  }

  boolean canAssignToThisQueue(Resource clusterResource,
      String nodePartition, ResourceLimits currentResourceLimits,
      Resource resourceCouldBeUnreserved, SchedulingMode schedulingMode) {
    try {
      readLock.lock();
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
      Resource currentLimitResource = getCurrentLimitResource(nodePartition,
          clusterResource, currentResourceLimits, schedulingMode);

      Resource nowTotalUsed = queueUsage.getUsed(nodePartition);

      // Set headroom for currentResourceLimits:
      // When queue is a parent queue: Headroom = limit - used + killable
      // When queue is a leaf queue: Headroom = limit - used (leaf queue cannot preempt itself)
      Resource usedExceptKillable = nowTotalUsed;
      if (hasChildQueues()) {
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
        if (this.reservationsContinueLooking && nodePartition.equals(
            RMNodeLabelsManager.NO_LABEL) && Resources.greaterThan(
            resourceCalculator, clusterResource, resourceCouldBeUnreserved,
            Resources.none())) {
          // resource-without-reserved = used - reserved
          Resource newTotalWithoutReservedResource = Resources.subtract(
              usedExceptKillable, resourceCouldBeUnreserved);

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
                  + newTotalWithoutReservedResource
                  + ", maxLimitCapacity: " + currentLimitResource);
            }
            return true;
          }
        }

        // Can not assign to this queue
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to assign to queue: " + getQueueName()
              + " nodePatrition: " + nodePartition
              + ", usedResources: " + queueUsage.getUsed(nodePartition)
              + ", clusterResources: " + clusterResource
              + ", reservedResources: " + resourceCouldBeUnreserved
              + ", maxLimitCapacity: " + currentLimitResource
              + ", currTotalUsed:" + usedExceptKillable);
        }
        return false;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Check assign to queue: " + getQueueName()
            + " nodePartition: " + nodePartition
            + ", usedResources: " + queueUsage.getUsed(nodePartition)
            + ", clusterResources: " + clusterResource
            + ", currentUsedCapacity: " + Resources
            .divide(resourceCalculator, clusterResource,
                queueUsage.getUsed(nodePartition), labelManager
                    .getResourceByLabel(nodePartition, clusterResource))
            + ", max-capacity: " + queueCapacities
            .getAbsoluteMaximumCapacity(nodePartition));
      }
      return true;
    } finally {
      readLock.unlock();
    }

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
        nodeLabel, this);
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
        nodeLabel, this);
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

  @VisibleForTesting
  @Override
  public CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits resourceLimits,
      SchedulingMode schedulingMode) {
    return assignContainers(clusterResource, new SimpleCandidateNodeSet<>(node),
        resourceLimits, schedulingMode);
  }

  @Override
  public boolean accept(Resource cluster,
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request) {
    // Do we need to check parent queue before making this decision?
    boolean checkParentQueue = false;

    ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode> allocation =
        request.getFirstAllocatedOrReservedContainer();
    SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> schedulerContainer =
        allocation.getAllocatedOrReservedContainer();

    // Do not check when allocating new container from a reserved container
    if (allocation.getAllocateFromReservedContainer() == null) {
      Resource required = allocation.getAllocatedOrReservedResource();
      Resource netAllocated = Resources.subtract(required,
          request.getTotalReleasedResource());

      try {
        readLock.lock();

        String partition = schedulerContainer.getNodePartition();
        Resource maxResourceLimit;
        if (allocation.getSchedulingMode()
            == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY) {
          maxResourceLimit = getQueueMaxResource(partition);
        } else{
          maxResourceLimit = labelManager.getResourceByLabel(
              schedulerContainer.getNodePartition(), cluster);
        }
        if (!Resources.fitsIn(resourceCalculator,
            Resources.add(queueUsage.getUsed(partition), netAllocated),
            maxResourceLimit)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Used resource=" + queueUsage.getUsed(partition)
                + " exceeded maxResourceLimit of the queue ="
                + maxResourceLimit);
          }
          return false;
        }
      } finally {
        readLock.unlock();
      }

      // Only check parent queue when something new allocated or reserved.
      checkParentQueue = true;
    }


    if (parent != null && checkParentQueue) {
      return parent.accept(cluster, request);
    }

    return true;
  }

  @Override
  public void validateSubmitApplication(ApplicationId applicationId,
      String userName, String queue) throws AccessControlException {
    // Dummy implementation
  }

  @Override
  public void updateQueueState(QueueState queueState) {
    this.state = queueState;
  }

  @Override
  public void activeQueue() throws YarnException {
    try {
      this.writeLock.lock();
      if (getState() == QueueState.RUNNING) {
        LOG.info("The specified queue:" + queueName
            + " is already in the RUNNING state.");
      } else if (getState() == QueueState.DRAINING) {
        throw new YarnException(
            "The queue:" + queueName + " is in the Stopping process. "
            + "Please wait for the queue getting fully STOPPED.");
      } else {
        CSQueue parent = getParent();
        if (parent == null || parent.getState() == QueueState.RUNNING) {
          updateQueueState(QueueState.RUNNING);
        } else {
          throw new YarnException("The parent Queue:" + parent.getQueueName()
              + " is not running. Please activate the parent queue first");
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  protected void appFinished() {
    try {
      this.writeLock.lock();
      if (getState() == QueueState.DRAINING) {
        if (getNumApplications() == 0) {
          updateQueueState(QueueState.STOPPED);
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public Priority getPriority() {
    return this.priority;
  }

  @Override
  public Map<String, Float> getUserWeights() {
    return userWeights;
  }
}
