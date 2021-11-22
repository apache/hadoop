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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueConfigurations;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AbsoluteResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SimpleCandidateNodeSet;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

public abstract class AbstractCSQueue implements CSQueue {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractCSQueue.class);
  protected final QueueAllocationSettings queueAllocationSettings;
  volatile CSQueue parent;
  protected final QueuePath queuePath;
  final String queueName;
  protected QueueNodeLabelsSettings queueNodeLabelsSettings;
  private volatile QueueAppLifetimeAndLimitSettings queueAppLifetimeSettings;
  private CSQueuePreemptionSettings preemptionSettings;

  private volatile QueueState state = null;
  protected final PrivilegedEntity queueEntity;

  final ResourceCalculator resourceCalculator;
  Set<String> resourceTypes;
  final RMNodeLabelsManager labelManager;
  private String multiNodeSortingPolicyName = null;

  Map<AccessType, AccessControlList> acls =
      new HashMap<AccessType, AccessControlList>();
  volatile boolean reservationsContinueLooking;

  // Track capacities like
  // used-capacity/abs-used-capacity/capacity/abs-capacity,
  // etc.
  QueueCapacities queueCapacities;
  CSQueueUsageTracker usageTracker;

  public enum CapacityConfigType {
    // FIXME, from what I can see, Percentage mode can almost apply to weighted
    // and percentage mode at the same time, there's only small area need to be
    // changed, we need to rename "PERCENTAGE" to "PERCENTAGE" and "WEIGHT"
    NONE, PERCENTAGE, ABSOLUTE_RESOURCE
  };
  protected CapacityConfigType capacityConfigType =
      CapacityConfigType.NONE;

  protected Map<String, QueueCapacityVector> configuredCapacityVectors;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  protected CapacitySchedulerContext csContext;
  protected YarnAuthorizationProvider authorizer = null;

  protected ActivitiesManager activitiesManager;

  protected ReentrantReadWriteLock.ReadLock readLock;
  protected ReentrantReadWriteLock.WriteLock writeLock;

  volatile Priority priority = Priority.newInstance(0);
  private UserWeights userWeights = UserWeights.createEmpty();

  // is it a dynamic queue?
  private boolean dynamicQueue = false;

  public AbstractCSQueue(CapacitySchedulerContext cs,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this(cs, cs.getConfiguration(), queueName, parent, old);
  }

  public AbstractCSQueue(CapacitySchedulerContext cs,
      CapacitySchedulerConfiguration configuration, String queueName,
      CSQueue parent, CSQueue old) {
    this.labelManager = cs.getRMContext().getNodeLabelManager();
    this.parent = parent;
    this.queuePath = createQueuePath(parent, queueName);
    this.queueName = queuePath.getLeafName();
    this.resourceCalculator = cs.getResourceCalculator();
    this.activitiesManager = cs.getActivitiesManager();

    // must be called after parent and queueName is set
    CSQueueMetrics metrics = old != null ?
        (CSQueueMetrics) old.getMetrics() :
        CSQueueMetrics.forQueue(getQueuePath(), parent,
            cs.getConfiguration().getEnableUserMetrics(), configuration);
    usageTracker = new CSQueueUsageTracker(metrics);
    this.csContext = cs;
    this.queueAllocationSettings = new QueueAllocationSettings(csContext);
    queueEntity = new PrivilegedEntity(EntityType.QUEUE, getQueuePath());
    queueCapacities = new QueueCapacities(parent == null);
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  private static QueuePath createQueuePath(CSQueue parent, String queueName) {
    if (parent == null) {
      return new QueuePath(null, queueName);
    }
    return new QueuePath(parent.getQueuePath(), queueName);
  }

  @VisibleForTesting
  protected void setupConfigurableCapacities() {
    setupConfigurableCapacities(csContext.getConfiguration());
  }

  protected void setupConfigurableCapacities(
      CapacitySchedulerConfiguration configuration) {
    CSQueueUtils.loadCapacitiesByLabelsFromConf(getQueuePath(), queueCapacities,
        configuration, this.queueNodeLabelsSettings.getConfiguredNodeLabels());
  }

  @Override
  public String getQueuePath() {
    return queuePath.getFullPath();
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
    return usageTracker.getQueueUsage().getUsed();
  }

  public int getNumContainers() {
    return usageTracker.getNumContainers();
  }

  @Override
  public QueueState getState() {
    return state;
  }

  @Override
  public CSQueueMetrics getMetrics() {
    return usageTracker.getMetrics();
  }

  @Override
  public String getQueueShortName() {
    return queuePath.getLeafName();
  }

  @Override
  public String getQueueName() {
    return queueName;
  }

  @Override
  public CSQueue getParent() {
    return parent;
  }

  @Override
  public void setParent(CSQueue newParentQueue) {
    this.parent = newParentQueue;
  }

  @Override
  public PrivilegedEntity getPrivilegedEntity() {
    return queueEntity;
  }

  public Set<String> getAccessibleNodeLabels() {
    return queueNodeLabelsSettings.getAccessibleNodeLabels();
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
  @VisibleForTesting
  void setMaxCapacity(float maximumCapacity) {
    writeLock.lock();
    try {
      // Sanity check
      CSQueueUtils.checkMaxCapacity(getQueuePath(),
          queueCapacities.getCapacity(), maximumCapacity);
      float absMaxCapacity = CSQueueUtils.computeAbsoluteMaximumCapacity(
          maximumCapacity, parent);
      CSQueueUtils.checkAbsoluteCapacity(getQueuePath(),
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
    writeLock.lock();
    try {
      // Sanity check
      CSQueueUtils.checkMaxCapacity(getQueuePath(),
          queueCapacities.getCapacity(nodeLabel), maximumCapacity);
      float absMaxCapacity = CSQueueUtils.computeAbsoluteMaximumCapacity(
          maximumCapacity, parent);
      CSQueueUtils.checkAbsoluteCapacity(getQueuePath(),
          queueCapacities.getAbsoluteCapacity(nodeLabel), absMaxCapacity);

      queueCapacities.setMaximumCapacity(maximumCapacity);
      queueCapacities.setAbsoluteMaximumCapacity(absMaxCapacity);
    } finally {
      writeLock.unlock();
    }
  }


  @Override
  public String getDefaultNodeLabelExpression() {
    return this.queueNodeLabelsSettings.getDefaultLabelExpression();
  }

  protected void setupQueueConfigs(Resource clusterResource,
      CapacitySchedulerConfiguration configuration) throws
      IOException {

    writeLock.lock();
    try {
      if (isDynamicQueue() || this instanceof AbstractAutoCreatedLeafQueue) {
        setDynamicQueueProperties(configuration);
      }

      // Collect and set the Node label configuration
      this.queueNodeLabelsSettings = new QueueNodeLabelsSettings(configuration, parent,
          getQueuePath(), csContext);

      // Initialize the queue capacities
      setupConfigurableCapacities(configuration);
      updateAbsoluteCapacities();

      updateCapacityConfigType();

      // Fetch minimum/maximum resource limits for this queue if
      // configured
      this.resourceTypes = new HashSet<>();
      for (AbsoluteResourceType type : AbsoluteResourceType.values()) {
        resourceTypes.add(type.toString().toLowerCase());
      }
      updateConfigurableResourceLimits(clusterResource);

      // Setup queue's maximumAllocation respecting the global
      // and the queue settings
      this.queueAllocationSettings.setupMaximumAllocation(configuration, getQueuePath(),
          parent, csContext);

      // Initialize the queue state based on previous state, configured state
      // and its parent state
      initializeQueueState(configuration);

      authorizer = YarnAuthorizationProvider.getInstance(configuration);

      this.acls = configuration.getAcls(getQueuePath());

      this.userWeights = getUserWeightsFromHierarchy(configuration);

      this.reservationsContinueLooking =
          configuration.getReservationContinueLook();
      this.configuredCapacityVectors = csContext.getConfiguration()
          .parseConfiguredResourceVector(queuePath.getFullPath(),
              this.queueNodeLabelsSettings.getConfiguredNodeLabels());

      // Update metrics
      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, null);

      // Store preemption settings
      this.preemptionSettings = new CSQueuePreemptionSettings(this, csContext, configuration);
      this.priority = configuration.getQueuePriority(
          getQueuePath());

      // Update multi-node sorting algorithm for scheduling as configured.
      setMultiNodeSortingPolicyName(
          configuration.getMultiNodesSortingAlgorithmPolicy(getQueuePath()));

      // Setup application related limits
      this.queueAppLifetimeSettings = new QueueAppLifetimeAndLimitSettings(configuration,
          this, getQueuePath());
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Set properties specific to dynamic queues.
   * @param configuration configuration on which the properties are set
   */
  protected void setDynamicQueueProperties(
      CapacitySchedulerConfiguration configuration) {
    // Set properties from parent template
    if (parent instanceof ParentQueue) {
      ((ParentQueue) parent).getAutoCreatedQueueTemplate()
          .setTemplateEntriesForChild(configuration, getQueuePath());

      String parentTemplate = String.format("%s.%s", parent.getQueuePath(),
          AutoCreatedQueueTemplate.AUTO_QUEUE_TEMPLATE_PREFIX);
      parentTemplate = parentTemplate.substring(0, parentTemplate.lastIndexOf(
          DOT));
      Set<String> parentNodeLabels = csContext
          .getCapacitySchedulerQueueManager().getConfiguredNodeLabels()
          .getLabelsByQueue(parentTemplate);

      if (parentNodeLabels != null && parentNodeLabels.size() > 1) {
        csContext.getCapacitySchedulerQueueManager().getConfiguredNodeLabels()
            .setLabelsByQueue(getQueuePath(), new HashSet<>(parentNodeLabels));
      }
    }
  }

  private UserWeights getUserWeightsFromHierarchy(
      CapacitySchedulerConfiguration configuration) {
    UserWeights unionInheritedWeights = UserWeights.createEmpty();
    CSQueue parentQ = parent;
    if (parentQ != null) {
      // Inherit all of parent's userWeights
      unionInheritedWeights.addFrom(parentQ.getUserWeights());
    }
    // Insert this queue's userWeights, overriding parent's userWeights if
    // there is an overlap.
    unionInheritedWeights.addFrom(
        configuration.getAllUserWeightsForQueue(getQueuePath()));
    return unionInheritedWeights;
  }

  protected Resource getMinimumAbsoluteResource(String queuePath, String label) {
    Resource minResource = csContext.getConfiguration()
        .getMinimumResourceRequirement(label, queuePath, resourceTypes);
    return minResource;
  }

  protected Resource getMaximumAbsoluteResource(String queuePath, String label) {
    Resource maxResource = csContext.getConfiguration()
        .getMaximumResourceRequirement(label, queuePath, resourceTypes);
    return maxResource;
  }

  protected boolean checkConfigTypeIsAbsoluteResource(String queuePath,
      String label) {
    return csContext.getConfiguration().checkConfigTypeIsAbsoluteResource(label,
        queuePath, resourceTypes);
  }

  protected void updateCapacityConfigType() {
    this.capacityConfigType = CapacityConfigType.NONE;
    for (String label : queueNodeLabelsSettings.getConfiguredNodeLabels()) {
      LOG.debug("capacityConfigType is '{}' for queue {}",
          capacityConfigType, getQueuePath());

      CapacityConfigType localType = checkConfigTypeIsAbsoluteResource(
          getQueuePath(), label) ? CapacityConfigType.ABSOLUTE_RESOURCE
          : CapacityConfigType.PERCENTAGE;

      if (this.capacityConfigType.equals(CapacityConfigType.NONE)) {
        this.capacityConfigType = localType;
        LOG.debug("capacityConfigType is updated as '{}' for queue {}",
            capacityConfigType, getQueuePath());
      } else {
        validateAbsoluteVsPercentageCapacityConfig(localType);
      }
    }
  }

  protected void updateConfigurableResourceLimits(Resource clusterResource) {
    for (String label : queueNodeLabelsSettings.getConfiguredNodeLabels()) {
      final Resource minResource = getMinimumAbsoluteResource(getQueuePath(), label);
      Resource maxResource = getMaximumAbsoluteResource(getQueuePath(), label);

      if (parent != null) {
        final Resource parentMax = parent.getQueueResourceQuotas()
            .getConfiguredMaxResource(label);
        validateMinResourceIsNotGreaterThanMaxResource(maxResource, parentMax, clusterResource,
            "Max resource configuration "
                + maxResource + " is greater than parents max value:"
                + parentMax + " in queue:" + getQueuePath());

        // If child's max resource is not set, but its parent max resource is
        // set, we must set child max resource to its parent's.
        if (maxResource.equals(Resources.none()) &&
            !minResource.equals(Resources.none()) &&
            !parentMax.equals(Resources.none())) {
          maxResource = Resources.clone(parentMax);
        }
      }

      validateMinResourceIsNotGreaterThanMaxResource(minResource, maxResource, clusterResource,
          "Min resource configuration "
              + minResource + " is greater than its max value:" + maxResource
              + " in queue:" + getQueuePath());

      LOG.debug("Updating absolute resource configuration for queue:{} as"
              + " minResource={} and maxResource={}", getQueuePath(), minResource,
          maxResource);

      usageTracker.getQueueResourceQuotas().setConfiguredMinResource(label, minResource);
      usageTracker.getQueueResourceQuotas().setConfiguredMaxResource(label, maxResource);
    }
  }

  private void validateMinResourceIsNotGreaterThanMaxResource(Resource minResource,
                                                              Resource maxResource,
                                                              Resource clusterResource,
                                                              String validationError) {
    if (!maxResource.equals(Resources.none()) && Resources.greaterThan(
        resourceCalculator, clusterResource, minResource, maxResource)) {
      throw new IllegalArgumentException(validationError);
    }
  }

  private void validateAbsoluteVsPercentageCapacityConfig(
      CapacityConfigType localType) {
    if (!getQueuePath().equals("root")
        && !this.capacityConfigType.equals(localType)) {
      throw new IllegalArgumentException("Queue '" + getQueuePath()
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
        queueAllocationSettings.getMinimumAllocation());
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
        queueAllocationSettings.getMinimumAllocation());
  }

  @Override
  public QueueCapacityVector getConfiguredCapacityVector(
      String label) {
    return configuredCapacityVectors.get(label);
  }

  private void initializeQueueState(CapacitySchedulerConfiguration configuration) {
    QueueState previousState = getState();
    QueueState configuredState = configuration
        .getConfiguredState(getQueuePath());
    QueueState parentState = (parent == null) ? null : parent.getState();

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
              "The parent queue:" + parent.getQueuePath()
                  + " cannot be STOPPED as the child queue:" + getQueuePath()
                  + " is in RUNNING state.");
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
            activateQueue();
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
    queueInfo.setQueuePath(getQueuePath());
    queueInfo.setAccessibleNodeLabels(queueNodeLabelsSettings.getAccessibleNodeLabels());
    queueInfo.setCapacity(queueCapacities.getCapacity());
    queueInfo.setMaximumCapacity(queueCapacities.getMaximumCapacity());
    queueInfo.setQueueState(getState());
    queueInfo.setDefaultNodeLabelExpression(queueNodeLabelsSettings.getDefaultLabelExpression());
    queueInfo.setCurrentCapacity(getUsedCapacity());
    queueInfo.setQueueStatistics(getQueueStatistics());
    queueInfo.setPreemptionDisabled(preemptionSettings.isPreemptionDisabled());
    queueInfo.setIntraQueuePreemptionDisabled(
        getIntraQueuePreemptionDisabled());
    queueInfo.setQueueConfigurations(getQueueConfigurations());
    queueInfo.setWeight(queueCapacities.getWeight());
    queueInfo.setMaxParallelApps(queueAppLifetimeSettings.getMaxParallelApps());
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
    QueueResourceQuotas queueResourceQuotas = usageTracker.getQueueResourceQuotas();
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
    return queueAllocationSettings.getMaximumAllocation();
  }

  @Private
  public Resource getMinimumAllocation() {
    return queueAllocationSettings.getMinimumAllocation();
  }

  void allocateResource(Resource clusterResource,
      Resource resource, String nodePartition) {
    writeLock.lock();
    try {
      usageTracker.getQueueUsage().incUsed(nodePartition, resource);
      usageTracker.increaseNumContainers();
      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, nodePartition);
    } finally {
      writeLock.unlock();
    }
  }

  protected void releaseResource(Resource clusterResource,
      Resource resource, String nodePartition) {
    writeLock.lock();
    try {
      usageTracker.getQueueUsage().decUsed(nodePartition, resource);

      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, nodePartition);

      usageTracker.decreaseNumContainers();
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
    readLock.lock();
    try {
      return acls;
    } finally {
      readLock.unlock();
    }
  }

  @Private
  public boolean getPreemptionDisabled() {
    return preemptionSettings.isPreemptionDisabled();
  }

  @Private
  public boolean getIntraQueuePreemptionDisabled() {
    return preemptionSettings.getIntraQueuePreemptionDisabled();
  }

  @Private
  public boolean getIntraQueuePreemptionDisabledInHierarchy() {
    return preemptionSettings.isIntraQueuePreemptionDisabledInHierarchy();
  }

  @Private
  public QueueCapacities getQueueCapacities() {
    return queueCapacities;
  }

  @Private
  public ResourceUsage getQueueResourceUsage() {
    return usageTracker.getQueueUsage();
  }

  @Override
  public QueueResourceQuotas getQueueResourceQuotas() {
    return usageTracker.getQueueResourceQuotas();
  }

  @Override
  public ReentrantReadWriteLock.ReadLock getReadLock() {
    return readLock;
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

  @VisibleForTesting
  boolean hasChildQueues() {
    List<CSQueue> childQueues = getChildQueues();
    return childQueues != null && !childQueues.isEmpty();
  }

  boolean canAssignToThisQueue(Resource clusterResource,
      String nodePartition, ResourceLimits currentResourceLimits,
      Resource resourceCouldBeUnreserved, SchedulingMode schedulingMode) {
    readLock.lock();
    try {
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
      // when partitioned-resource-request comes back.
      Resource currentLimitResource = getCurrentLimitResource(nodePartition,
          clusterResource, currentResourceLimits, schedulingMode);

      Resource nowTotalUsed = usageTracker.getQueueUsage().getUsed(nodePartition);

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

        // if reservation continue looking enabled, check to see if could we
        // potentially use this node instead of a reserved node if the application
        // has reserved containers.
        if (this.reservationsContinueLooking
            && Resources.greaterThan(resourceCalculator, clusterResource,
            resourceCouldBeUnreserved, Resources.none())) {
          // resource-without-reserved = used - reserved
          Resource newTotalWithoutReservedResource = Resources.subtract(
              usedExceptKillable, resourceCouldBeUnreserved);

          // when total-used-without-reserved-resource < currentLimit, we still
          // have chance to allocate on this node by unreserving some containers
          if (Resources.lessThan(resourceCalculator, clusterResource,
              newTotalWithoutReservedResource, currentLimitResource)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("try to use reserved: " + getQueuePath()
                  + " usedResources: " + usageTracker.getQueueUsage().getUsed()
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
          LOG.debug("Failed to assign to queue: " + getQueuePath()
              + " nodePartition: " + nodePartition
              + ", usedResources: " + usageTracker.getQueueUsage().getUsed(nodePartition)
              + ", clusterResources: " + clusterResource
              + ", reservedResources: " + resourceCouldBeUnreserved
              + ", maxLimitCapacity: " + currentLimitResource
              + ", currTotalUsed:" + usedExceptKillable);
        }
        return false;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Check assign to queue: " + getQueuePath()
            + " nodePartition: " + nodePartition
            + ", usedResources: " + usageTracker.getQueueUsage().getUsed(nodePartition)
            + ", clusterResources: " + clusterResource
            + ", currentUsedCapacity: " + Resources
            .divide(resourceCalculator, clusterResource,
                usageTracker.getQueueUsage().getUsed(nodePartition), labelManager
                    .getResourceByLabel(nodePartition, clusterResource))
            + ", max-capacity: " + queueCapacities
            .getAbsoluteMaximumCapacity(nodePartition));
      }
      return true;
    } finally {
      readLock.unlock();
    }

  }

  private static String ensurePartition(String partition) {
    return Optional.ofNullable(partition).orElse(RMNodeLabelsManager.NO_LABEL);
  }

  @FunctionalInterface
  interface Counter {
    void count(String partition, Resource resource);
  }

  @FunctionalInterface
  interface CounterWithApp {
    void count(String partition, Resource reservedRes, SchedulerApplicationAttempt application);
  }

  private void count(String partition, Resource resource, Counter counter, Counter parentCounter) {
    final String checkedPartition = ensurePartition(partition);
    counter.count(checkedPartition, resource);
    Optional.ofNullable(parentCounter).ifPresent(c -> c.count(checkedPartition, resource));
  }

  private void countAndUpdate(String partition, Resource resource,
                              Counter counter, CounterWithApp parentCounter) {
    final String checkedPartition = ensurePartition(partition);
    counter.count(checkedPartition, resource);
    CSQueueUtils.updateUsedCapacity(resourceCalculator,
        labelManager.getResourceByLabel(checkedPartition, Resources.none()),
        checkedPartition, this);
    Optional.ofNullable(parentCounter).ifPresent(c -> c.count(checkedPartition, resource, null));
  }

  @Override
  public void incReservedResource(String partition, Resource reservedRes) {
    count(partition, reservedRes, usageTracker.getQueueUsage()::incReserved,
        parent == null ? null : parent::incReservedResource);
  }

  @Override
  public void decReservedResource(String partition, Resource reservedRes) {
    count(partition, reservedRes, usageTracker.getQueueUsage()::decReserved,
        parent == null ? null : parent::decReservedResource);
  }

  @Override
  public void incPendingResource(String nodeLabel, Resource resourceToInc) {
    count(nodeLabel, resourceToInc, usageTracker.getQueueUsage()::incPending,
        parent == null ? null : parent::incPendingResource);
  }

  @Override
  public void decPendingResource(String nodeLabel, Resource resourceToDec) {
    count(nodeLabel, resourceToDec, usageTracker.getQueueUsage()::decPending,
        parent == null ? null : parent::decPendingResource);
  }

  @Override
  public void incUsedResource(String nodeLabel, Resource resourceToInc,
      SchedulerApplicationAttempt application) {
    countAndUpdate(nodeLabel, resourceToInc, usageTracker.getQueueUsage()::incUsed,
        parent == null ? null : parent::incUsedResource);
  }

  @Override
  public void decUsedResource(String nodeLabel, Resource resourceToDec,
      SchedulerApplicationAttempt application) {
    countAndUpdate(nodeLabel, resourceToDec, usageTracker.getQueueUsage()::decUsed,
        parent == null ? null : parent::decUsedResource);
  }

  /**
   * Return if the queue has pending resource on given nodePartition and
   * schedulingMode.
   */
  boolean hasPendingResourceRequest(String nodePartition,
      Resource cluster, SchedulingMode schedulingMode) {
    return SchedulerUtils.hasPendingResourceRequest(resourceCalculator,
        usageTracker.getQueueUsage(), nodePartition, cluster, schedulingMode);
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
    return csContext.getPreemptionManager().getKillableResource(getQueuePath(),
        partition);
  }

  public Iterator<RMContainer> getKillableContainers(String partition) {
    return csContext.getPreemptionManager().getKillableContainers(
        getQueuePath(),
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

      readLock.lock();
      try {
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
            Resources.add(usageTracker.getQueueUsage().getUsed(partition), netAllocated),
            maxResourceLimit)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Used resource=" + usageTracker.getQueueUsage().getUsed(partition)
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
  public void activateQueue() throws YarnException {
    this.writeLock.lock();
    try {
      if (getState() == QueueState.RUNNING) {
        LOG.info("The specified queue:" + getQueuePath()
            + " is already in the RUNNING state.");
      } else {
        CSQueue parentQueue = parent;
        if (parentQueue == null || parentQueue.getState() == QueueState.RUNNING) {
          updateQueueState(QueueState.RUNNING);
        } else {
          throw new YarnException("The parent Queue:" + parentQueue.getQueuePath()
              + " is not running. Please activate the parent queue first");
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  protected void appFinished() {
    this.writeLock.lock();
    try {
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
  public UserWeights getUserWeights() {
    return userWeights;
  }

  public void recoverDrainingState() {
    this.writeLock.lock();
    try {
      if (getState() == QueueState.STOPPED) {
        updateQueueState(QueueState.DRAINING);
      }
      LOG.info("Recover draining state for queue " + this.getQueuePath());
      if (parent != null && parent.getState() == QueueState.STOPPED) {
        ((AbstractCSQueue) parent).recoverDrainingState();
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public String getMultiNodeSortingPolicyName() {
    return this.multiNodeSortingPolicyName;
  }

  public void setMultiNodeSortingPolicyName(String policyName) {
    this.multiNodeSortingPolicyName = policyName;
  }

  public long getMaximumApplicationLifetime() {
    return queueAppLifetimeSettings.getMaxApplicationLifetime();
  }

  public long getDefaultApplicationLifetime() {
    return queueAppLifetimeSettings.getDefaultApplicationLifetime();
  }

  public boolean getDefaultAppLifetimeWasSpecifiedInConfig() {
    return queueAppLifetimeSettings.isDefaultAppLifetimeWasSpecifiedInConfig();
  }

  public void setMaxParallelApps(int maxParallelApps) {
    this.queueAppLifetimeSettings.setMaxParallelApps(maxParallelApps);
  }

  @Override
  public int getMaxParallelApps() {
    return this.queueAppLifetimeSettings.getMaxParallelApps();
  }

  abstract int getNumRunnableApps();

  protected void updateAbsoluteCapacities() {
    QueueCapacities parentQueueCapacities = null;
    if (parent != null) {
      parentQueueCapacities = parent.getQueueCapacities();
    }

    CSQueueUtils.updateAbsoluteCapacitiesByNodeLabels(queueCapacities,
        parentQueueCapacities, queueCapacities.getExistingNodeLabels());
  }

  private Resource createNormalizedMinResource(Resource minResource,
      Map<String, Float> effectiveMinRatio) {
    Resource ret = Resource.newInstance(minResource);
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation nResourceInformation =
          minResource.getResourceInformation(i);

      Float ratio = effectiveMinRatio.get(nResourceInformation.getName());
      if (ratio != null) {
        ret.setResourceValue(i,
            (long) (nResourceInformation.getValue() * ratio));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Updating min resource for Queue: " + getQueuePath() + " as " + ret
              .getResourceInformation(i) + ", Actual resource: "
              + nResourceInformation.getValue() + ", ratio: " + ratio);
        }
      }
    }
    return ret;
  }

  private Resource getOrInheritMaxResource(Resource resourceByLabel, String label) {
    Resource parentMaxResource =
        parent.getQueueResourceQuotas().getConfiguredMaxResource(label);
    if (parentMaxResource.equals(Resources.none())) {
      parentMaxResource =
          parent.getQueueResourceQuotas().getEffectiveMaxResource(label);
    }

    Resource configuredMaxResource =
        getQueueResourceQuotas().getConfiguredMaxResource(label);
    if (configuredMaxResource.equals(Resources.none())) {
      return Resources.clone(parentMaxResource);
    }

    return Resources.clone(Resources.min(resourceCalculator, resourceByLabel,
        configuredMaxResource, parentMaxResource));
  }

  void deriveCapacityFromAbsoluteConfigurations(String label,
      Resource clusterResource) {
    // Update capacity with a float calculated from the parent's minResources
    // and the recently changed queue minResources.
    // capacity = effectiveMinResource / {parent's effectiveMinResource}
    float result = resourceCalculator.divide(clusterResource,
        usageTracker.getQueueResourceQuotas().getEffectiveMinResource(label),
        parent.getQueueResourceQuotas().getEffectiveMinResource(label));
    queueCapacities.setCapacity(label,
        Float.isInfinite(result) ? 0 : result);

    // Update maxCapacity with a float calculated from the parent's maxResources
    // and the recently changed queue maxResources.
    // maxCapacity = effectiveMaxResource / parent's effectiveMaxResource
    result = resourceCalculator.divide(clusterResource,
        usageTracker.getQueueResourceQuotas().getEffectiveMaxResource(label),
        parent.getQueueResourceQuotas().getEffectiveMaxResource(label));
    queueCapacities.setMaximumCapacity(label,
        Float.isInfinite(result) ? 0 : result);

    // Update absolute capacity (as in fraction of the
    // whole cluster's resources) with a float calculated from the queue's
    // capacity and the parent's absoluteCapacity.
    // absoluteCapacity = capacity * parent's absoluteCapacity
    queueCapacities.setAbsoluteCapacity(label,
        queueCapacities.getCapacity(label) * parent.getQueueCapacities()
            .getAbsoluteCapacity(label));

    // Update absolute maxCapacity (as in fraction of the
    // whole cluster's resources) with a float calculated from the queue's
    // maxCapacity and the parent's absoluteMaxCapacity.
    // absoluteMaxCapacity = maxCapacity * parent's absoluteMaxCapacity
    queueCapacities.setAbsoluteMaximumCapacity(label,
        queueCapacities.getMaximumCapacity(label) *
            parent.getQueueCapacities()
                .getAbsoluteMaximumCapacity(label));
  }

  void updateEffectiveResources(Resource clusterResource) {
    for (String label : queueNodeLabelsSettings.getConfiguredNodeLabels()) {
      Resource resourceByLabel = labelManager.getResourceByLabel(label,
          clusterResource);
      Resource newEffectiveMinResource;
      Resource newEffectiveMaxResource;

      // Absolute and relative/weight mode needs different handling.
      if (getCapacityConfigType().equals(
          CapacityConfigType.ABSOLUTE_RESOURCE)) {
        newEffectiveMinResource = createNormalizedMinResource(
            usageTracker.getQueueResourceQuotas().getConfiguredMinResource(label),
            ((ParentQueue) parent).getEffectiveMinRatioPerResource());

        // Max resource of a queue should be the minimum of {parent's maxResources,
        // this queue's maxResources}. Both parent's maxResources and this queue's
        // maxResources can be configured. If this queue's maxResources is not
        // configured, inherit the value from the parent. If parent's
        // maxResources is not configured its inherited value must be collected.
        newEffectiveMaxResource =
            getOrInheritMaxResource(resourceByLabel, label);
      } else {
        newEffectiveMinResource = Resources
            .multiply(resourceByLabel,
                queueCapacities.getAbsoluteCapacity(label));
        newEffectiveMaxResource = Resources
            .multiply(resourceByLabel,
                queueCapacities.getAbsoluteMaximumCapacity(label));
      }

      // Update the effective min
      usageTracker.getQueueResourceQuotas().setEffectiveMinResource(label,
          newEffectiveMinResource);
      usageTracker.getQueueResourceQuotas().setEffectiveMaxResource(label,
          newEffectiveMaxResource);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating queue:" + getQueuePath()
            + " with effective minimum resource=" + newEffectiveMinResource
            + "and effective maximum resource="
            + newEffectiveMaxResource);
      }

      if (getCapacityConfigType().equals(
          CapacityConfigType.ABSOLUTE_RESOURCE)) {
        /*
         * If the queues are configured with absolute resources, it is advised
         * to update capacity/max-capacity/etc. based on the newly calculated
         * resource values. These values won't be used for actual resource
         * distribution, however, for accurate metrics and the UI
         * they should be re-calculated.
         */
        deriveCapacityFromAbsoluteConfigurations(label, clusterResource);
      }
    }
  }

  public boolean isDynamicQueue() {
    readLock.lock();

    try {
      return dynamicQueue;
    } finally {
      readLock.unlock();
    }
  }

  public void setDynamicQueue(boolean dynamicQueue) {
    writeLock.lock();

    try {
      this.dynamicQueue = dynamicQueue;
    } finally {
      writeLock.unlock();
    }
  }

  protected String getCapacityOrWeightString() {
    if (queueCapacities.getWeight() != -1) {
      return "weight=" + queueCapacities.getWeight() + ", " +
          "normalizedWeight=" + queueCapacities.getNormalizedWeight();
    } else {
      return "capacity=" + queueCapacities.getCapacity();
    }
  }

  public boolean isEligibleForAutoDeletion() {
    return false;
  }

  public boolean isInactiveDynamicQueue() {
    long idleDurationSeconds =
        (Time.monotonicNow() - getLastSubmittedTimestamp())/1000;
    return isDynamicQueue() && isEligibleForAutoDeletion() &&
        (idleDurationSeconds > this.csContext.getConfiguration().
            getAutoExpiredDeletionTime());
  }

  void updateLastSubmittedTimeStamp() {
    writeLock.lock();
    try {
      usageTracker.setLastSubmittedTimestamp(Time.monotonicNow());
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  long getLastSubmittedTimestamp() {
    readLock.lock();

    try {
      return usageTracker.getLastSubmittedTimestamp();
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  void setLastSubmittedTimestamp(long lastSubmittedTimestamp) {
    writeLock.lock();
    try {
      usageTracker.setLastSubmittedTimestamp(lastSubmittedTimestamp);
    } finally {
      writeLock.unlock();
    }
  }
}
