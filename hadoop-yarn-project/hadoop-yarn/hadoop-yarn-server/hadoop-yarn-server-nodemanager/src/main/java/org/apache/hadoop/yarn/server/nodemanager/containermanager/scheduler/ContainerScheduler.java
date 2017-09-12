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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
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

import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor
    .ChangeMonitoringContainerResourceEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;


import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
  private final int maxOppQueueLength;

  // Queue of Guaranteed Containers waiting for resources to run
  private final LinkedHashMap<ContainerId, Container>
      queuedGuaranteedContainers = new LinkedHashMap<>();
  // Queue of Opportunistic Containers waiting for resources to run
  private final LinkedHashMap<ContainerId, Container>
      queuedOpportunisticContainers = new LinkedHashMap<>();

  // Used to keep track of containers that have been marked to be killed
  // to make room for a guaranteed container.
  private final Map<ContainerId, Container> oppContainersToKill =
      new HashMap<>();

  // Containers launched by the Scheduler will take a while to actually
  // move to the RUNNING state, but should still be fair game for killing
  // by the scheduler to make room for guaranteed containers. This holds
  // containers that are in RUNNING as well as those in SCHEDULED state that
  // have been marked to run, but not yet RUNNING.
  private final LinkedHashMap<ContainerId, Container> runningContainers =
      new LinkedHashMap<>();

  private final ContainerQueuingLimit queuingLimit =
      ContainerQueuingLimit.newInstance();

  private final OpportunisticContainersStatus opportunisticContainersStatus;

  // Resource Utilization Tracker that decides how utilization of the cluster
  // increases / decreases based on container start / finish
  private ResourceUtilizationTracker utilizationTracker;

  private final AsyncDispatcher dispatcher;
  private final NodeManagerMetrics metrics;

  /**
   * Instantiate a Container Scheduler.
   * @param context NodeManager Context.
   * @param dispatcher AsyncDispatcher.
   * @param metrics NodeManagerMetrics.
   */
  public ContainerScheduler(Context context, AsyncDispatcher dispatcher,
      NodeManagerMetrics metrics) {
    this(context, dispatcher, metrics, context.getConf().getInt(
        YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH,
        YarnConfiguration.
            DEFAULT_NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH));
  }

  @VisibleForTesting
  public ContainerScheduler(Context context, AsyncDispatcher dispatcher,
      NodeManagerMetrics metrics, int qLength) {
    super(ContainerScheduler.class.getName());
    this.context = context;
    this.dispatcher = dispatcher;
    this.metrics = metrics;
    this.maxOppQueueLength = (qLength <= 0) ? 0 : qLength;
    this.utilizationTracker =
        new AllocationBasedResourceUtilizationTracker(this);
    this.opportunisticContainersStatus =
        OpportunisticContainersStatus.newInstance();
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
    case CONTAINER_COMPLETED:
      onContainerCompleted(event.getContainer());
      break;
    case UPDATE_CONTAINER:
      if (event instanceof UpdateContainerSchedulerEvent) {
        onUpdateContainer((UpdateContainerSchedulerEvent) event);
      } else {
        LOG.error("Unknown event type on UpdateCOntainer: " + event.getType());
      }
      break;
    case SHED_QUEUED_CONTAINERS:
      shedQueuedOpportunisticContainers();
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
      if (runningContainers.containsKey(containerId)) {
        this.utilizationTracker.subtractContainerResource(
            updateEvent.getContainer());
        updateEvent.getContainer().setContainerTokenIdentifier(
            updateEvent.getUpdatedToken());
        this.utilizationTracker.addContainerResources(
            updateEvent.getContainer());
        getContainersMonitor().handle(
            new ChangeMonitoringContainerResourceEvent(containerId,
                updateEvent.getUpdatedToken().getResource()));
      } else {
        // Is Queued or localizing..
        updateEvent.getContainer().setContainerTokenIdentifier(
            updateEvent.getUpdatedToken());
      }
      try {
        // Persist change in the state store.
        this.context.getNMStateStore().storeContainerResourceChanged(
            containerId,
            updateEvent.getUpdatedToken().getVersion(),
            updateEvent.getUpdatedToken().getResource());
      } catch (IOException e) {
        LOG.warn("Could not store container [" + containerId + "] resource " +
            "change..", e);
      }
    }

    if (updateEvent.isExecTypeUpdate()) {
      updateEvent.getContainer().setContainerTokenIdentifier(
          updateEvent.getUpdatedToken());
      // If this is a running container.. just change the execution type
      // and be done with it.
      if (!runningContainers.containsKey(containerId)) {
        // Promotion or not (Increase signifies either a promotion
        // or container size increase)
        if (updateEvent.isIncrease()) {
          // Promotion of queued container..
          if (queuedOpportunisticContainers.remove(containerId) != null) {
            queuedGuaranteedContainers.put(containerId,
                updateEvent.getContainer());
          }
          //Kill opportunistic containers if any to make room for
          // promotion request
          killOpportunisticContainers(updateEvent.getContainer());
        } else {
          // Demotion of queued container.. Should not happen too often
          // since you should not find too many queued guaranteed
          // containers
          if (queuedGuaranteedContainers.remove(containerId) != null) {
            queuedOpportunisticContainers.put(containerId,
                updateEvent.getContainer());
          }
        }
      }
    }
  }

  /**
   * Return number of queued containers.
   * @return Number of queued containers.
   */
  public int getNumQueuedContainers() {
    return this.queuedGuaranteedContainers.size()
        + this.queuedOpportunisticContainers.size();
  }

  @VisibleForTesting
  public int getNumQueuedGuaranteedContainers() {
    return this.queuedGuaranteedContainers.size();
  }

  @VisibleForTesting
  public int getNumQueuedOpportunisticContainers() {
    return this.queuedOpportunisticContainers.size();
  }

  @VisibleForTesting
  public int getNumRunningContainers() {
    return this.runningContainers.size();
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
    return this.opportunisticContainersStatus;
  }

  private void onContainerCompleted(Container container) {
    oppContainersToKill.remove(container.getContainerId());

    // This could be killed externally for eg. by the ContainerManager,
    // in which case, the container might still be queued.
    Container queued =
        queuedOpportunisticContainers.remove(container.getContainerId());
    if (queued == null) {
      queuedGuaranteedContainers.remove(container.getContainerId());
    }

    // decrement only if it was a running container
    Container completedContainer = runningContainers.remove(container
        .getContainerId());
    // only a running container releases resources upon completion
    boolean resourceReleased = completedContainer != null;
    if (resourceReleased) {
      this.utilizationTracker.subtractContainerResource(container);
      if (container.getContainerTokenIdentifier().getExecutionType() ==
          ExecutionType.OPPORTUNISTIC) {
        this.metrics.completeOpportunisticContainer(container.getResource());
      }
      startPendingContainers(false);
    }
  }

  /**
   * Start pending containers in the queue.
   * @param forceStartGuaranteedContaieners When this is true, start guaranteed
   *        container without looking at available resource
   */
  private void startPendingContainers(boolean forceStartGuaranteedContaieners) {
    // Start pending guaranteed containers, if resources available.
    boolean resourcesAvailable = startContainers(
        queuedGuaranteedContainers.values(), forceStartGuaranteedContaieners);
    // Start opportunistic containers, if resources available.
    if (resourcesAvailable) {
      startContainers(queuedOpportunisticContainers.values(), false);
    }
  }

  private boolean startContainers(
      Collection<Container> containersToBeStarted, boolean force) {
    Iterator<Container> cIter = containersToBeStarted.iterator();
    boolean resourcesAvailable = true;
    while (cIter.hasNext() && resourcesAvailable) {
      Container container = cIter.next();
      if (tryStartContainer(container, force)) {
        cIter.remove();
      } else {
        resourcesAvailable = false;
      }
    }
    return resourcesAvailable;
  }

  private boolean tryStartContainer(Container container, boolean force) {
    boolean containerStarted = false;
    // call startContainer without checking available resource when force==true
    if (force || resourceAvailableToStartContainer(
        container)) {
      startContainer(container);
      containerStarted = true;
    }
    return containerStarted;
  }

  /**
   * Check if there is resource available to start a given container
   * immediately. (This can be extended to include overallocated resources)
   * @param container the container to start
   * @return true if container can be launched directly
   */
  private boolean resourceAvailableToStartContainer(Container container) {
    return this.utilizationTracker.hasResourcesAvailable(container);
  }

  private boolean enqueueContainer(Container container) {
    boolean isGuaranteedContainer = container.getContainerTokenIdentifier().
        getExecutionType() == ExecutionType.GUARANTEED;

    boolean isQueued;
    if (isGuaranteedContainer) {
      queuedGuaranteedContainers.put(container.getContainerId(), container);
      isQueued = true;
    } else {
      if (queuedOpportunisticContainers.size() < maxOppQueueLength) {
        LOG.info("Opportunistic container {} will be queued at the NM.",
            container.getContainerId());
        queuedOpportunisticContainers.put(
            container.getContainerId(), container);
        isQueued = true;
      } else {
        LOG.info("Opportunistic container [{}] will not be queued at the NM" +
                "since max queue length [{}] has been reached",
            container.getContainerId(), maxOppQueueLength);
        container.sendKillEvent(
            ContainerExitStatus.KILLED_BY_CONTAINER_SCHEDULER,
            "Opportunistic container queue is full.");
        isQueued = false;
      }
    }

    if (isQueued) {
      try {
        this.context.getNMStateStore().storeContainerQueued(
            container.getContainerId());
      } catch (IOException e) {
        LOG.warn("Could not store container [" + container.getContainerId()
            + "] state. The Container has been queued.", e);
      }
    }

    return isQueued;
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
      enqueueContainer(container);

      // When opportunistic container not allowed (which is determined by
      // max-queue length of pending opportunistic containers <= 0), start
      // guaranteed containers without looking at available resources.
      boolean forceStartGuaranteedContainers = (maxOppQueueLength <= 0);
      startPendingContainers(forceStartGuaranteedContainers);

      // if the guaranteed container is queued, we need to preempt opportunistic
      // containers for make room for it
      if (queuedGuaranteedContainers.containsKey(container.getContainerId())) {
        killOpportunisticContainers(container);
      }
    } else {
      // Given an opportunistic container, we first try to start as many queuing
      // guaranteed containers as possible followed by queuing opportunistic
      // containers based on remaining resource available, then enqueue the
      // opportunistic container. If the container is enqueued, we do another
      // pass to try to start the newly enqueued opportunistic container.
      startPendingContainers(false);
      boolean containerQueued = enqueueContainer(container);
      // container may not get queued because the max opportunistic container
      // queue length is reached. If so, there is no point doing another pass
      if (containerQueued) {
        startPendingContainers(false);
      }
    }
  }

  private void killOpportunisticContainers(Container container) {
    List<Container> extraOpportContainersToKill =
        pickOpportunisticContainersToKill(container.getContainerId());
    // Kill the opportunistic containers that were chosen.
    for (Container contToKill : extraOpportContainersToKill) {
      contToKill.sendKillEvent(
          ContainerExitStatus.KILLED_BY_CONTAINER_SCHEDULER,
          "Container Killed to make room for Guaranteed Container.");
      oppContainersToKill.put(contToKill.getContainerId(), contToKill);
      LOG.info(
          "Opportunistic container {} will be killed in order to start the "
              + "execution of guaranteed container {}.",
          contToKill.getContainerId(), container.getContainerId());
    }
  }

  private void startContainer(Container container) {
    LOG.info("Starting container [" + container.getContainerId()+ "]");
    runningContainers.put(container.getContainerId(), container);
    this.utilizationTracker.addContainerResources(container);
    if (container.getContainerTokenIdentifier().getExecutionType() ==
        ExecutionType.OPPORTUNISTIC) {
      this.metrics.startOpportunisticContainer(container.getResource());
    }
    container.sendLaunchEvent();
  }

  private List<Container> pickOpportunisticContainersToKill(
      ContainerId containerToStartId) {
    // The opportunistic containers that need to be killed for the
    // given container to start.
    List<Container> extraOpportContainersToKill = new ArrayList<>();
    // Track resources that need to be freed.
    ResourceUtilization resourcesToFreeUp = resourcesToFreeUp(
        containerToStartId);

    // Go over the running opportunistic containers.
    // Use a descending iterator to kill more recently started containers.
    Iterator<Container> lifoIterator = new LinkedList<>(
        runningContainers.values()).descendingIterator();
    while(lifoIterator.hasNext() &&
        !hasSufficientResources(resourcesToFreeUp)) {
      Container runningCont = lifoIterator.next();
      if (runningCont.getContainerTokenIdentifier().getExecutionType() ==
          ExecutionType.OPPORTUNISTIC) {

        if (oppContainersToKill.containsKey(
            runningCont.getContainerId())) {
          // These containers have already been marked to be killed.
          // So exclude them..
          continue;
        }
        extraOpportContainersToKill.add(runningCont);
        ContainersMonitor.decreaseResourceUtilization(
            getContainersMonitor(), resourcesToFreeUp,
            runningCont.getResource());
      }
    }
    if (!hasSufficientResources(resourcesToFreeUp)) {
      LOG.warn("There are no sufficient resources to start guaranteed [{}]" +
          "at the moment. Opportunistic containers are in the process of" +
          "being killed to make room.", containerToStartId);
    }
    return extraOpportContainersToKill;
  }

  private boolean hasSufficientResources(
      ResourceUtilization resourcesToFreeUp) {
    return resourcesToFreeUp.getPhysicalMemory() <= 0 &&
        resourcesToFreeUp.getVirtualMemory() <= 0 &&
        // Convert the number of cores to nearest integral number, due to
        // imprecision of direct float comparison.
        Math.round(resourcesToFreeUp.getCPU()
            * getContainersMonitor().getVCoresAllocatedForContainers()) <= 0;
  }

  private ResourceUtilization resourcesToFreeUp(
      ContainerId containerToStartId) {
    // Get allocation of currently allocated containers.
    ResourceUtilization resourceAllocationToFreeUp = ResourceUtilization
        .newInstance(this.utilizationTracker.getCurrentUtilization());

    // Add to the allocation the allocation of the pending guaranteed
    // containers that will start before the current container will be started.
    for (Container container : queuedGuaranteedContainers.values()) {
      ContainersMonitor.increaseResourceUtilization(
          getContainersMonitor(), resourceAllocationToFreeUp,
          container.getResource());
      if (container.getContainerId().equals(containerToStartId)) {
        break;
      }
    }

    // These resources are being freed, likely at the behest of another
    // guaranteed container..
    for (Container container : oppContainersToKill.values()) {
      ContainersMonitor.decreaseResourceUtilization(
          getContainersMonitor(), resourceAllocationToFreeUp,
          container.getResource());
    }

    // Subtract the overall node resources.
    getContainersMonitor().subtractNodeResourcesFromResourceUtilization(
        resourceAllocationToFreeUp);
    return resourceAllocationToFreeUp;
  }

  @SuppressWarnings("unchecked")
  public void updateQueuingLimit(ContainerQueuingLimit limit) {
    this.queuingLimit.setMaxQueueLength(limit.getMaxQueueLength());
    // YARN-2886 should add support for wait-times. Include wait time as
    // well once it is implemented
    if ((queuingLimit.getMaxQueueLength() > -1) &&
        (queuingLimit.getMaxQueueLength() <
            queuedOpportunisticContainers.size())) {
      dispatcher.getEventHandler().handle(
          new ContainerSchedulerEvent(null,
              ContainerSchedulerEventType.SHED_QUEUED_CONTAINERS));
    }
  }

  private void shedQueuedOpportunisticContainers() {
    int numAllowed = this.queuingLimit.getMaxQueueLength();
    Iterator<Container> containerIter =
        queuedOpportunisticContainers.values().iterator();
    while (containerIter.hasNext()) {
      Container container = containerIter.next();
      if (numAllowed <= 0) {
        container.sendKillEvent(
            ContainerExitStatus.KILLED_BY_CONTAINER_SCHEDULER,
            "Container De-queued to meet NM queuing limits.");
        containerIter.remove();
        LOG.info(
            "Opportunistic container {} will be killed to meet NM queuing" +
                " limits.", container.getContainerId());
      }
      numAllowed--;
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
