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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerChain;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor
    .ChangeMonitoringContainerResourceEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;

import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService
    .RecoveredContainerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.yarnpp.util.ContainerSchedulerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ContainerScheduler manages a collection of runnable containers. It
 * ensures that a container is launched only if all its launch criteria are
 * met. It also ensures that OPPORTUNISTIC containers are killed to make
 * room for GUARANTEED containers.
 */
public class ContainerScheduler extends AbstractService implements
    EventHandler<ContainerSchedulerEvent> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerScheduler.class);

  private final Context context;

  private final RunningContainersManager runningContainersManager;

  private final ContainerQueueManager containerQueueManager;

  private final ContainerPreemptionManager containerPreemptionManager;

  private final OpportunisticContainersStatus opportunisticContainersStatus;

  // Resource Utilization Tracker that decides how utilization of the cluster
  // increases / decreases based on container start / finish
  private final ResourceUtilizationTracker utilizationTracker;

  private final AsyncDispatcher dispatcher;
  private final NodeManagerMetrics metrics;

  private static int getMaxOppQueueLengthFromConf(final Context context) {
    if (context == null || context.getConf() == null) {
      return YarnConfiguration
          .DEFAULT_NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH;
    }

    return context.getConf().getInt(
        YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH,
        YarnConfiguration.DEFAULT_NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH
    );
  }

  private static OpportunisticContainersQueuePolicy
    getOppContainersQueuePolicyFromConf(final Context context) {
    final OpportunisticContainersQueuePolicy queuePolicy;
    if (context == null || context.getConf() == null) {
      queuePolicy = OpportunisticContainersQueuePolicy.DEFAULT;
    } else {
      queuePolicy = context.getConf().getEnum(
          YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_QUEUE_POLICY,
          OpportunisticContainersQueuePolicy.DEFAULT
      );
    }
    return queuePolicy;
  }

  @VisibleForTesting
  ResourceHandlerChain resourceHandlerChain = null;

  /**
   * Instantiate a Container Scheduler.
   * @param context NodeManager Context.
   * @param dispatcher AsyncDispatcher.
   * @param metrics NodeManagerMetrics.
   */
  public ContainerScheduler(Context context, AsyncDispatcher dispatcher,
      NodeManagerMetrics metrics) {
    this(context, dispatcher, metrics,
        getOppContainersQueuePolicyFromConf(context),
        getMaxOppQueueLengthFromConf(context));
  }


  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    this.containerPreemptionManager.initConfigs(conf);
    if (resourceHandlerChain == null) {
      resourceHandlerChain = ResourceHandlerModule
          .getConfiguredResourceHandlerChain(conf, context);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Resource handler chain enabled = " + (resourceHandlerChain
          != null));
    }
  }

  @VisibleForTesting
  public ContainerScheduler(Context context, AsyncDispatcher dispatcher,
      NodeManagerMetrics metrics, int qLength) {
    this(context, dispatcher, metrics,
        getOppContainersQueuePolicyFromConf(context), qLength);
  }

  @VisibleForTesting
  public ContainerScheduler(Context context, AsyncDispatcher dispatcher,
      NodeManagerMetrics metrics,
      OpportunisticContainersQueuePolicy oppContainersQueuePolicy,
      int qLength) {
    super(ContainerScheduler.class.getName());
    this.context = context;
    this.dispatcher = dispatcher;
    this.metrics = metrics;
    this.utilizationTracker =
        new AllocationBasedResourceUtilizationTracker(this);
    this.containerQueueManager =
        new ContainerQueueManager(oppContainersQueuePolicy, qLength,
            utilizationTracker, metrics, context);
    this.runningContainersManager =
        new RunningContainersManager(utilizationTracker, metrics,
            containerQueueManager);
    this.opportunisticContainersStatus =
        OpportunisticContainersStatus.newInstance();
    this.containerPreemptionManager =
        new ContainerPreemptionManager(this.containerQueueManager,
            this.runningContainersManager, context, this.utilizationTracker);
  }

  /**
   * Handle ContainerSchedulerEvents.
   * @param event ContainerSchedulerEvent.
   */
  @Override
  public void handle(ContainerSchedulerEvent event) {
    switch (event.getType()) {
    case SCHEDULE_CONTAINER:
      scheduleContainer(event.getContainer());
      break;
    // NOTE: Is sent only after container state has changed to PAUSED...
    case CONTAINER_PAUSED:
      // NOTE: Is sent only after container state has changed to DONE...
    case CONTAINER_COMPLETED:
      onResourcesReclaimed(event.getContainer());
      break;
    case UPDATE_CONTAINER:
      if (event instanceof UpdateContainerSchedulerEvent) {
        onUpdateContainer((UpdateContainerSchedulerEvent) event);
      } else {
        LOG.error("Unknown event type on UpdateCOntainer: " + event.getType());
      }
      break;
    case SHED_QUEUED_CONTAINERS:
      this.containerPreemptionManager.shedQueuedOpportunisticContainers();
      break;
    case RECOVERY_COMPLETED:
      this.runningContainersManager.startPendingContainers(
          this.containerQueueManager.getForceStartGuaranteedContainers());
      break;
    default:
      LOG.error("Unknown event arrived at ContainerScheduler: "
          + event.toString());
    }
  }

  /**
   * We assume that the ContainerManager has already figured out what kind
   * of update this is.
   */
  private void onUpdateContainer(UpdateContainerSchedulerEvent updateEvent) {
    ContainerId containerId = updateEvent.getContainer().getContainerId();
    if (updateEvent.isResourceChange()) {
      if (this.runningContainersManager.isContainerInRunningQueue(containerId)) {
        this.utilizationTracker.subtractContainerResource(
            new ContainerImpl(getConfig(), null, null, null, null,
                updateEvent.getOriginalToken(), context));
        this.utilizationTracker.addContainerResources(
            updateEvent.getContainer());
        getContainersMonitor().handle(
            new ChangeMonitoringContainerResourceEvent(containerId,
                updateEvent.getUpdatedToken().getResource()));
      }
    }

    if (updateEvent.isExecTypeUpdate()) {
      // Promotion or not (Increase signifies either a promotion
      // or container size increase)
      if (updateEvent.isIncrease()) {
        // Promotion of queued container..
        if (this.containerQueueManager.removeOpportunisticContainerFromQueue(
            containerId) != null) {
          this.containerQueueManager.addGuaranteedContainerToQueue(
              updateEvent.getContainer());
          //Kill/pause opportunistic containers if any to make room for
          // promotion request
          this.containerPreemptionManager.reclaimContainerResources(
              updateEvent.getContainer());
        }
      } else {
        // Demotion of queued container. Should not happen too often
        // since you should not find too many queued guaranteed
        // containers
        if (this.containerQueueManager.removeGuaranteedContainerFromQueue(
            containerId) != null) {
          this.containerQueueManager.addOpportunisticContainerToQueue(
              updateEvent.getContainer());
        }
      }
      try {
        resourceHandlerChain.updateContainer(updateEvent.getContainer());
      } catch (Exception ex) {
        LOG.warn(String.format("Could not update resources on " +
            "continer update of %s", containerId), ex);
      }
      this.runningContainersManager.startPendingContainers(
          this.containerQueueManager.getForceStartGuaranteedContainers);
    }
  }

  /**
   * Populates auxiliary data structures used by the ContainerScheduler on
   * recovery.
   * @param container container recovered
   * @param rcs Recovered Container status
   */
  public void recoverActiveContainer(Container container,
      RecoveredContainerState rcs) {
    ExecutionType execType =
        container.getContainerTokenIdentifier().getExecutionType();
    if (rcs.getStatus() == RecoveredContainerStatus.QUEUED
        || rcs.getStatus() == RecoveredContainerStatus.PAUSED) {
      if (execType == ExecutionType.GUARANTEED) {
        this.containerQueueManager.addGuaranteedContainerToQueue(container);
      } else if (execType == ExecutionType.OPPORTUNISTIC) {
        this.containerQueueManager.addOpportunisticContainerToQueue(container);
      } else {
        LOG.error(
            "UnKnown execution type received " + container.getContainerId()
                + ", execType " + execType);
      }
    } else if (rcs.getStatus() == RecoveredContainerStatus.LAUNCHED) {
      this.runningContainersManager.addContainerToRunningQueue(container);
      utilizationTracker.addContainerResources(container);
    }
    if (rcs.getStatus() != RecoveredContainerStatus.COMPLETED
        && rcs.getCapability() != null) {
      metrics.launchedContainer();
      metrics.allocateContainer(rcs.getCapability());
    }
  }

  /**
   * Return number of queued containers.
   * @return Number of queued containers.
   */
  public int getNumQueuedContainers() {
    return this.containerQueueManager.getNumberOfQueuedGuaranteedContainers()
        + this.containerQueueManager.getNumberOfQueuedOpportunisticContainers();
  }

  @VisibleForTesting
  public int getNumQueuedGuaranteedContainers() {
    return this.containerQueueManager.getNumberOfQueuedGuaranteedContainers();
  }

  @VisibleForTesting
  public int getNumQueuedOpportunisticContainers() {
    return this.containerQueueManager.getNumberOfQueuedOpportunisticContainers();
  }

  @VisibleForTesting
  public int getNumRunningContainers() {
    return this.runningContainersManager.getNumberOfRunningContainers();
  }

  @VisibleForTesting
  public void setUsePauseEventForPreemption(
      boolean usePauseEventForPreemption) {
    this.containerPreemptionManager.setUsePauseEventForPreemption(
        usePauseEventForPreemption);
  }

  public OpportunisticContainersStatus getOpportunisticContainersStatus() {
    this.opportunisticContainersStatus.setQueuedOpportContainers(
        getNumQueuedOpportunisticContainers());
    this.opportunisticContainersStatus.setWaitQueueLength(
        getNumQueuedContainers());
    this.opportunisticContainersStatus.setOpportMemoryUsed(
        metrics.getAllocatedOpportunisticGB());
    this.opportunisticContainersStatus.setOpportCoresUsed(
        metrics.getAllocatedOpportunisticVCores());
    this.opportunisticContainersStatus.setRunningOpportContainers(
        metrics.getRunningOpportunisticContainers());
    this.opportunisticContainersStatus.setOpportQueueCapacity(
        this.containerQueueManager.getMaxOppQueueLength());
    return this.opportunisticContainersStatus;
  }

  private void onResourcesReclaimed(Container container) {
    this.containerPreemptionManager.removeContainer(container.getContainerId());

    // This could be killed externally for eg. by the ContainerManager,
    // in which case, the container might still be queued.
    Container queued =
        this.containerQueueManager.removeOpportunisticContainerFromQueue(
            container.getContainerId());
    if (queued == null) {
      this.containerQueueManager.removeGuaranteedContainerFromQueue(
          container.getContainerId());
    }

    // Requeue PAUSED containers
    if (container.getContainerState() == ContainerState.PAUSED) {
      if (container.getContainerTokenIdentifier().getExecutionType() ==
          ExecutionType.GUARANTEED) {
        this.containerQueueManager.addGuaranteedContainerToQueue(container);
      } else {
        this.containerQueueManager.addOpportunisticContainerToQueue(container);
      }
    }
    // decrement only if it was a running container
    Container completedContainer = this.runningContainersManager.deleteRunningContainer(
        container.getContainerId());
    // only a running container releases resources upon completion
    boolean resourceReleased = completedContainer != null;
    if (resourceReleased) {
      this.utilizationTracker.subtractContainerResource(container);
      if (container.getContainerTokenIdentifier().getExecutionType() ==
          ExecutionType.OPPORTUNISTIC) {
        this.metrics.completeOpportunisticContainer(container.getResource());
      }
      this.runningContainersManager.startPendingContainers(
          this.containerQueueManager.getForceStartGuaranteedContainers());
    }
  }

  @VisibleForTesting
  protected void scheduleContainer(Container container) {
    boolean isGuaranteedContainer = container.getContainerTokenIdentifier().
        getExecutionType() == ExecutionType.GUARANTEED;

    // Given a guaranteed container, we enqueue it first and then try to start
    // as many queuing guaranteed containers as possible followed by queuing
    // opportunistic containers based on remaining resources available. If the
    // container still stays in the queue afterwards, we need to preempt just
    // enough number of opportunistic containers.
    if (isGuaranteedContainer) {
      this.containerQueueManager.enqueueContainer(container);

      // When opportunistic container not allowed (which is determined by
      // max-queue length of pending opportunistic containers <= 0), start
      // guaranteed containers without looking at available resources.
      this.runningContainersManager.startPendingContainers(
          this.containerQueueManager.getForceStartGuaranteedContainers());
      // if the guaranteed container is queued, we need to preempt opportunistic
      // containers for make room for it
      if (this.containerQueueManager.isGuaranteedContainerQueued(
          container.getContainerId())) {
        this.containerPreemptionManager.reclaimContainerResources(container);
      }
    } else {
      // Given an opportunistic container, we first try to start as many queuing
      // guaranteed containers as possible followed by queuing opportunistic
      // containers based on remaining resource available, then enqueue the
      // opportunistic container. If the container is enqueued, we do another
      // pass to try to start the newly enqueued opportunistic container.
      this.runningContainersManager.startPendingContainers(false);
      boolean containerQueued =
          this.containerQueueManager.enqueueContainer(container);
      // container may not get queued because the max opportunistic container
      // queue length is reached. If so, there is no point doing another pass
      if (containerQueued) {
        this.runningContainersManager.startPendingContainers(false);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void updateQueuingLimit(ContainerQueuingLimit limit) {
    this.containerQueueManager.setMaxOpportunisticQueueingLimit(
        limit.getMaxQueueLength());
    // YARN-2886 should add support for wait-times. Include wait time as
    // well once it is implemented
    if ((this.containerQueueManager.getMaxOpportunisticQueueingLimit() > -1) &&
        (this.containerQueueManager.getMaxOpportunisticQueueingLimit() <
            this.containerQueueManager.getNumberOfQueuedOpportunisticContainers())) {
      dispatcher.getEventHandler().handle(
          new ContainerSchedulerEvent(null,
              ContainerSchedulerEventType.SHED_QUEUED_CONTAINERS));
    }
  }

  public ContainersMonitor getContainersMonitor() {
    return this.context.getContainerManager().getContainersMonitor();
  }

  @VisibleForTesting
  public ResourceUtilization getCurrentUtilization() {
    return this.utilizationTracker.getCurrentUtilization();
  }

}