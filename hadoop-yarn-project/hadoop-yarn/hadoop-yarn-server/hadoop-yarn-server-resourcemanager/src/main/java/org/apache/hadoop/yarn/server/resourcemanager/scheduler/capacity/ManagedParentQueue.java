/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica
    .FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAXIMUM_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.getQueueCapacityConfigParser;

/**
 * Auto Creation enabled Parent queue. This queue initially does not have any
 * children to start with and all child
 * leaf queues will be auto created. Currently this does not allow other
 * pre-configured leaf or parent queues to
 * co-exist along with auto-created leaf queues. The auto creation is limited
 * to leaf queues currently.
 */
public class ManagedParentQueue extends AbstractManagedParentQueue {

  private boolean shouldFailAutoCreationWhenGuaranteedCapacityExceeded = false;

  private static final Logger LOG = LoggerFactory.getLogger(
      ManagedParentQueue.class);

  public ManagedParentQueue(final CapacitySchedulerQueueContext queueContext,
      final String queueName, final CSQueue parent, final CSQueue old)
      throws IOException {
    super(queueContext, queueName, parent, old);
    super.setupQueueConfigs(queueContext.getClusterResource());
    shouldFailAutoCreationWhenGuaranteedCapacityExceeded =
        queueContext.getConfiguration()
            .getShouldFailAutoQueueCreationWhenGuaranteedCapacityExceeded(
                getQueuePath());

    leafQueueTemplate = initializeLeafQueueConfigs().build();

    initializeQueueManagementPolicy();
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
      throws IOException {

    writeLock.lock();
    try {
      validate(newlyParsedQueue);

      shouldFailAutoCreationWhenGuaranteedCapacityExceeded =
          queueContext.getConfiguration()
              .getShouldFailAutoQueueCreationWhenGuaranteedCapacityExceeded(
                  getQueuePath());

      //validate if capacity is exceeded for child queues
      if (shouldFailAutoCreationWhenGuaranteedCapacityExceeded) {
        float childCap = sumOfChildCapacities();
        if (getCapacity() < childCap) {
          throw new IOException(
              "Total of Auto Created leaf queues guaranteed capacity : "
                  + childCap + " exceeds Parent queue's " + getQueuePath()
                  + " guaranteed capacity " + getCapacity() + ""
                  + ".Cannot enforce policy to auto"
                  + " create queues beyond parent queue's capacity");
        }
      }

      leafQueueTemplate = initializeLeafQueueConfigs().build();

      super.reinitialize(newlyParsedQueue, clusterResource);

      // run reinitialize on each existing queue, to trigger absolute cap
      // recomputations
      for (CSQueue res : this.getChildQueues()) {
        res.reinitialize(res, clusterResource);
      }

      //clear state in policy
      reinitializeQueueManagementPolicy();

      //reassign capacities according to policy
      final List<QueueManagementChange> queueManagementChanges =
          queueManagementPolicy.computeQueueManagementChanges();

      validateAndApplyQueueManagementChanges(queueManagementChanges);

      LOG.info(
          "Reinitialized Managed Parent Queue: [{}] with capacity [{}]"
              + " with max capacity [{}]",
          getQueueName(), super.getCapacity(), super.getMaximumCapacity());
    } catch (YarnException ye) {
      LOG.error("Exception while computing policy changes for leaf queue : "
          + getQueuePath(), ye);
      throw new IOException(ye);
    } finally {
      writeLock.unlock();
    }
  }

  private void initializeQueueManagementPolicy() throws IOException {
    queueManagementPolicy =
        queueContext.getConfiguration().getAutoCreatedQueueManagementPolicyClass(
            getQueuePath());

    queueManagementPolicy.init(this);
  }

  private void reinitializeQueueManagementPolicy() throws IOException {
    AutoCreatedQueueManagementPolicy managementPolicy =
        queueContext.getConfiguration().getAutoCreatedQueueManagementPolicyClass(
            getQueuePath());

    if (!(managementPolicy.getClass().equals(
        this.queueManagementPolicy.getClass()))) {
      queueManagementPolicy = managementPolicy;
      queueManagementPolicy.init(this);
    } else{
      queueManagementPolicy.reinitialize(this);
    }
  }

  protected AutoCreatedLeafQueueConfig.Builder initializeLeafQueueConfigs() throws IOException {

    AutoCreatedLeafQueueConfig.Builder builder =
        new AutoCreatedLeafQueueConfig.Builder();

    CapacitySchedulerConfiguration configuration =
        queueContext.getConfiguration();

    // TODO load configs into CapacitySchedulerConfiguration instead of duplicating them
    String leafQueueTemplateConfPrefix = getLeafQueueConfigPrefix(
        configuration);
    //Load template configuration into CapacitySchedulerConfiguration
    CapacitySchedulerConfiguration autoCreatedTemplateConfig =
        super.initializeLeafQueueConfigs(leafQueueTemplateConfPrefix);
    builder.configuration(autoCreatedTemplateConfig);
    QueueResourceQuotas queueResourceQuotas = new QueueResourceQuotas();
    setAbsoluteResourceTemplates(configuration, queueResourceQuotas);

    QueuePath templateQueuePath = configuration
        .getAutoCreatedQueueObjectTemplateConfPrefix(getQueuePath());
    Set<String> templateConfiguredNodeLabels = queueContext
        .getQueueManager().getConfiguredNodeLabelsForAllQueues()
        .getLabelsByQueue(templateQueuePath.getFullPath());
    //Load template capacities
    QueueCapacities queueCapacities = new QueueCapacities(false);
    CSQueueUtils.loadCapacitiesByLabelsFromConf(templateQueuePath,
        queueCapacities,
        configuration,
        templateConfiguredNodeLabels);

    /**
     * Populate leaf queue template (of Parent resources configured in
     * ABSOLUTE_RESOURCE) capacities with actual values for which configured has
     * been defined in ABSOLUTE_RESOURCE format.
     *
     */
    if (this.capacityConfigType.equals(CapacityConfigType.ABSOLUTE_RESOURCE)) {
      updateQueueCapacities(queueCapacities);
    }
    builder.capacities(queueCapacities);
    builder.resourceQuotas(queueResourceQuotas);
    return builder;
  }

  private void setAbsoluteResourceTemplates(CapacitySchedulerConfiguration configuration,
                                            QueueResourceQuotas queueResourceQuotas) throws IOException {
    QueuePath templateQueuePath = configuration
        .getAutoCreatedQueueObjectTemplateConfPrefix(getQueuePath());
    Set<String> templateConfiguredNodeLabels = queueContext
        .getQueueManager().getConfiguredNodeLabelsForAllQueues()
        .getLabelsByQueue(templateQueuePath.getFullPath());

    for (String nodeLabel : templateConfiguredNodeLabels) {
      Resource templateMinResource = configuration.getMinimumResourceRequirement(
          nodeLabel, templateQueuePath.getFullPath(), resourceTypes);
      queueResourceQuotas.setConfiguredMinResource(nodeLabel, templateMinResource);

      if (this.capacityConfigType.equals(CapacityConfigType.PERCENTAGE)
          && !templateMinResource.equals(Resources.none())) {
        throw new IOException("Managed Parent Queue " + this.getQueuePath()
            + " config type is different from leaf queue template config type");
      }
    }
  }

  private void updateQueueCapacities(QueueCapacities queueCapacities) {
    CapacitySchedulerConfiguration configuration =
        queueContext.getConfiguration();

    for (String label : queueCapacities.getExistingNodeLabels()) {
      queueCapacities.setCapacity(label,
          resourceCalculator.divide(
              queueContext.getClusterResource(),
              configuration.getMinimumResourceRequirement(
                  label,
                  configuration
                      .getAutoCreatedQueueTemplateConfPrefix(getQueuePath()),
                  resourceTypes),
              getQueueResourceQuotas().getConfiguredMinResource(label)));

      Resource childMaxResource = configuration
          .getMaximumResourceRequirement(label,
              configuration
                  .getAutoCreatedQueueTemplateConfPrefix(getQueuePath()),
              resourceTypes);
      Resource parentMaxRes = getQueueResourceQuotas()
          .getConfiguredMaxResource(label);

      Resource effMaxResource = Resources.min(
          resourceCalculator,
          queueContext.getClusterResource(),
          childMaxResource.equals(Resources.none()) ? parentMaxRes
              : childMaxResource,
          parentMaxRes);

      queueCapacities.setMaximumCapacity(
          label, resourceCalculator.divide(
              queueContext.getClusterResource(),
               effMaxResource,
               getQueueResourceQuotas().getConfiguredMaxResource(label)));

      queueCapacities.setAbsoluteCapacity(
          label, queueCapacities.getCapacity(label)
          * getQueueCapacities().getAbsoluteCapacity(label));

      queueCapacities.setAbsoluteMaximumCapacity(label,
          queueCapacities.getMaximumCapacity(label)
          * getQueueCapacities().getAbsoluteMaximumCapacity(label));
    }
  }

  protected void validate(final CSQueue newlyParsedQueue) throws IOException {
    // Sanity check
    if (!(newlyParsedQueue instanceof ManagedParentQueue) || !newlyParsedQueue
        .getQueuePath().equals(getQueuePath())) {
      throw new IOException(
          "Trying to reinitialize " + getQueuePath() + " from "
              + newlyParsedQueue.getQueuePath());
    }
  }

  @Override
  public void addChildQueue(CSQueue childQueue)
      throws SchedulerDynamicEditException, IOException {

    writeLock.lock();
    try {
      if (childQueue == null || !(childQueue instanceof AutoCreatedLeafQueue)) {
        throw new SchedulerDynamicEditException(
            "Expected child queue to be an instance of AutoCreatedLeafQueue");
      }

      CapacitySchedulerConfiguration conf = queueContext.getConfiguration();
      ManagedParentQueue parentQueue =
          (ManagedParentQueue) childQueue.getParent();

      if (parentQueue == null) {
        throw new SchedulerDynamicEditException(
            "Parent Queue is null, should not add child queue!");
      }

      String leafQueuePath = childQueue.getQueuePath();
      int maxQueues = conf.getAutoCreatedQueuesMaxChildQueuesLimit(
          parentQueue.getQueuePath());

      if (parentQueue.getChildQueues().size() >= maxQueues) {
        throw new SchedulerDynamicEditException(
            "Cannot auto create leaf queue " + leafQueuePath + ".Max Child "
                + "Queue limit exceeded which is configured as : " + maxQueues
                + " and number of child queues is : " + parentQueue
                .getChildQueues().size());
      }

      if (shouldFailAutoCreationWhenGuaranteedCapacityExceeded) {
        if (getLeafQueueTemplate().getQueueCapacities().getAbsoluteCapacity()
            + parentQueue.sumOfChildAbsCapacities() > parentQueue
            .getAbsoluteCapacity()) {
          throw new SchedulerDynamicEditException(
              "Cannot auto create leaf queue " + leafQueuePath + ". Child "
                  + "queues capacities have reached parent queue : "
                  + parentQueue.getQueuePath() + "'s guaranteed capacity");
        }
      }

      ((GuaranteedOrZeroCapacityOverTimePolicy) queueManagementPolicy)
          .updateTemplateAbsoluteCapacities(parentQueue.getQueueCapacities());

      AutoCreatedLeafQueue leafQueue = (AutoCreatedLeafQueue) childQueue;
      super.addChildQueue(leafQueue);

      /* Below is to avoid Setting Queue Capacity to NaN when ClusterResource
         is zero during RM Startup with DominantResourceCalculator */
      if (this.capacityConfigType.equals(
          CapacityConfigType.ABSOLUTE_RESOURCE)) {
        QueueCapacities queueCapacities =
            getLeafQueueTemplate().getQueueCapacities();
        updateQueueCapacities(queueCapacities);
      }

      setLeafQueuesCapacityVector(leafQueue);

      final AutoCreatedLeafQueueConfig initialLeafQueueTemplate =
          queueManagementPolicy.getInitialLeafQueueConfiguration(leafQueue);
      leafQueue.reinitializeFromTemplate(initialLeafQueueTemplate);

      // Do one update cluster resource call to make sure all absolute resources
      // effective resources are updated.
      updateClusterResource(queueContext.getClusterResource(),
          new ResourceLimits(queueContext.getClusterResource()));
    } finally {
      writeLock.unlock();
    }
  }

  private void setLeafQueuesCapacityVector(AutoCreatedLeafQueue leafQueue) {
    // Parse the capacityVector specified in the leaf-template
    CapacitySchedulerConfiguration leafConfig = leafQueueTemplate.getLeafQueueConfigs();
    Set<String> templateConfiguredNodeLabels = queueContext
        .getQueueManager().getConfiguredNodeLabelsForAllQueues()
        .getLabelsByQueue(queuePath.getFullPath());
    for (String label : templateConfiguredNodeLabels) {
      final String leafConfigPath =
          CapacitySchedulerConfiguration.getNodeLabelPrefix(
              getQueuePath() + DOT + AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX, label);
      String capacityString = leafConfig.get(leafConfigPath + CAPACITY, "0");
      leafQueue.setConfiguredMinCapacityVector(label,
          getQueueCapacityConfigParser().parse(capacityString, leafQueue.getQueuePath()));
      String maxCapacityString = leafConfig.get(leafConfigPath + MAXIMUM_CAPACITY, "100");
      leafQueue.setConfiguredMaxCapacityVector(label,
          getQueueCapacityConfigParser().parse(maxCapacityString, leafQueue.getQueuePath()));
    }
  }

  public List<FiCaSchedulerApp> getScheduleableApplications() {
    readLock.lock();
    try {
      List<FiCaSchedulerApp> apps = new ArrayList<>();
      for (CSQueue childQueue : getChildQueues()) {
        apps.addAll(((AbstractLeafQueue) childQueue).getApplications());
      }
      return Collections.unmodifiableList(apps);
    } finally {
      readLock.unlock();
    }
  }

  public List<FiCaSchedulerApp> getPendingApplications() {
    readLock.lock();
    try {
      List<FiCaSchedulerApp> apps = new ArrayList<>();
      for (CSQueue childQueue : getChildQueues()) {
        apps.addAll(((AbstractLeafQueue) childQueue).getPendingApplications());
      }
      return Collections.unmodifiableList(apps);
    } finally {
      readLock.unlock();
    }
  }

  public List<FiCaSchedulerApp> getAllApplications() {
    readLock.lock();
    try {
      List<FiCaSchedulerApp> apps = new ArrayList<>();
      for (CSQueue childQueue : getChildQueues()) {
        apps.addAll(((AbstractLeafQueue) childQueue).getAllApplications());
      }
      return Collections.unmodifiableList(apps);
    } finally {
      readLock.unlock();
    }
  }

  public String getLeafQueueConfigPrefix(CapacitySchedulerConfiguration conf) {
    return CapacitySchedulerConfiguration.PREFIX + conf
        .getAutoCreatedQueueTemplateConfPrefix(getQueuePath());
  }

  public boolean shouldFailAutoCreationWhenGuaranteedCapacityExceeded() {
    return shouldFailAutoCreationWhenGuaranteedCapacityExceeded;
  }

  /**
   * Asynchronously called from scheduler to apply queue management changes.
   *
   * @param queueManagementChanges QueueManagementChange List.
   * @throws IOException an I/O exception has occurred.
   * @throws SchedulerDynamicEditException when validate and apply QueueManagementChanges fails.
   */
  public void validateAndApplyQueueManagementChanges(
      List<QueueManagementChange> queueManagementChanges)
      throws IOException, SchedulerDynamicEditException {

    writeLock.lock();
    try {
      validateQueueManagementChanges(queueManagementChanges);

      applyQueueManagementChanges(queueManagementChanges);

      AutoCreatedQueueManagementPolicy policy =
          getAutoCreatedQueueManagementPolicy();

      //acquires write lock on policy
      policy.commitQueueManagementChanges(queueManagementChanges);

    } finally {
      writeLock.unlock();
    }
  }

  public void validateQueueManagementChanges(
      List<QueueManagementChange> queueManagementChanges)
      throws SchedulerDynamicEditException {

    for (QueueManagementChange queueManagementChange : queueManagementChanges) {

      CSQueue childQueue = queueManagementChange.getQueue();

      if (!(childQueue instanceof AutoCreatedLeafQueue)) {
        throw new SchedulerDynamicEditException(
            "queue should be " + "AutoCreatedLeafQueue. Found " + childQueue
                .getClass());
      }

      if (!(AbstractManagedParentQueue.class.
          isAssignableFrom(childQueue.getParent().getClass()))) {
        LOG.error("Queue " + getQueuePath()
            + " is not an instance of PlanQueue or ManagedParentQueue." + " "
            + "Ignoring update " + queueManagementChanges);
        throw new SchedulerDynamicEditException(
            "Queue " + getQueuePath() + " is not a AutoEnabledParentQueue."
                + " Ignoring update " + queueManagementChanges);
      }

      if (queueManagementChange.getQueueAction() ==
          QueueManagementChange.QueueAction.UPDATE_QUEUE) {
        AutoCreatedLeafQueueConfig template =
            queueManagementChange.getUpdatedQueueTemplate();
        ((AutoCreatedLeafQueue) childQueue).validateConfigurations(template);
      }

    }
  }

  private void applyQueueManagementChanges(
      List<QueueManagementChange> queueManagementChanges)
      throws SchedulerDynamicEditException, IOException {
    for (QueueManagementChange queueManagementChange : queueManagementChanges) {
      if (queueManagementChange.getQueueAction() ==
          QueueManagementChange.QueueAction.UPDATE_QUEUE) {
        AutoCreatedLeafQueue childQueueToBeUpdated =
            (AutoCreatedLeafQueue) queueManagementChange.getQueue();
        setLeafQueuesCapacityVector(childQueueToBeUpdated);
        //acquires write lock on leaf queue
        childQueueToBeUpdated.reinitializeFromTemplate(
            queueManagementChange.getUpdatedQueueTemplate());
      }
    }
  }

  public void setLeafQueueConfigs(String leafQueueName) {
    CapacitySchedulerConfiguration templateConfig = leafQueueTemplate.getLeafQueueConfigs();
    for (Map.Entry<String, String> confKeyValuePair : templateConfig) {
      final String name = confKeyValuePair.getKey()
          .replaceFirst(AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX,
              leafQueueName);
      queueContext.setConfigurationEntry(name, confKeyValuePair.getValue());
    }
  }
}