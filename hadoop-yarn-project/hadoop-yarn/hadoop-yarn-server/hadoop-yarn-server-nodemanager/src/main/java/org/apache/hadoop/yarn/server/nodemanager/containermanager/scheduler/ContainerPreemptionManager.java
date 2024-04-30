/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * ContainerPreemptionManager is responsible for managing container preemption
 * policies. When the required resources are not present for Guaranteed
 * containers, running opportunistic containers are preempted by this class
 */
public class ContainerPreemptionManager {
  private static Logger LOG =
      LoggerFactory.getLogger(ContainerPreemptionManager.class);
  // Used to keep track of containers that have been marked to be killed
  // or paused to make room for a guaranteed container.
  private final Map<ContainerId, Container> containersToKill = new HashMap<>();

  private boolean usePauseEventForPreemption = false;
  private final ContainerQueueManager containerQueueManager;
  private final RunningContainersManager runningContainersManager;

  private final Context context;

  private final ResourceUtilizationTracker utilizationTracker;

  /**
   * Instantiate an object of ContainerPreemptionManager.
   *
   * @param containerQueueManager
   * @param runningContainersManager
   * @param context
   * @param utilizationTracker
   */
  public ContainerPreemptionManager(ContainerQueueManager containerQueueManager,
      RunningContainersManager runningContainersManager, Context context,
      ResourceUtilizationTracker utilizationTracker) {
    this.containerQueueManager = containerQueueManager;
    this.runningContainersManager = runningContainersManager;
    this.context = context;
    this.utilizationTracker = utilizationTracker;
  }

  /**
   * Initialize configurations
   *
   * @param config
   */
  public void initConfigs(Configuration config) {
    this.setUsePauseEventForPreemption(config.getBoolean(
        YarnConfiguration.NM_CONTAINER_QUEUING_USE_PAUSE_FOR_PREEMPTION,
        YarnConfiguration.DEFAULT_NM_CONTAINER_QUEUING_USE_PAUSE_FOR_PREEMPTION));
  }

  /**
   * Stop tracking and remove given container from containersToKill queue
   *
   * @param containerId
   */
  public void removeContainer(ContainerId containerId) {
    containersToKill.remove(containerId);
  }

  /**
   * Set flag for using pause while preempting
   *
   * @param usePauseEventForPreemption
   */
  public void setUsePauseEventForPreemption(
      boolean usePauseEventForPreemption) {
    this.usePauseEventForPreemption = usePauseEventForPreemption;
  }

  /**
   * Reclaim resources from running opportunistic containers to make room for
   * current and previously queued guaranteed container
   *
   * @param container
   */
  public void reclaimContainerResources(Container container) {
    List<Container> extraOppContainersToReclaim =
        pickOpportunisticContainersToReclaimResources(
            container.getContainerId());
    // Kill the opportunistic containers that were chosen.
    for (Container contToReclaim : extraOppContainersToReclaim) {
      String preemptionAction =
          usePauseEventForPreemption == true ? "paused" : "killed";
      LOG.info("Container {} will be {} to start the "
              + "execution of guaranteed container {}.",
          contToReclaim.getContainerId(), preemptionAction,
          container.getContainerId());

      if (usePauseEventForPreemption) {
        contToReclaim.sendPauseEvent(
            "Container Paused to make room for Guaranteed Container");
      } else {
        contToReclaim.sendKillEvent(
            ContainerExitStatus.KILLED_BY_CONTAINER_SCHEDULER,
            "Container Killed to make room for Guaranteed Container.");
      }
      this.containersToKill.put(contToReclaim.getContainerId(), contToReclaim);
    }
  }

  private List<Container> pickOpportunisticContainersToReclaimResources(
      ContainerId containerToStartId) {
    // The opportunistic containers that need to be killed for the
    // given container to start.
    List<Container> extraOpportContainersToKill = new ArrayList<>();
    // Track resources that need to be freed.
    ResourceUtilization resourcesToFreeUp =
        resourcesToFreeUp(containerToStartId);

    // Go over the running opportunistic containers.
    // Use a descending iterator to kill more recently started containers.
    Iterator<Container> lifoIterator = new LinkedList<>(
        this.runningContainersManager.getRunningContainers()
            .values()).descendingIterator();
    while (lifoIterator.hasNext() && !hasSufficientResources(
        resourcesToFreeUp)) {
      Container runningCont = lifoIterator.next();
      if (runningCont.getContainerTokenIdentifier().getExecutionType()
          == ExecutionType.OPPORTUNISTIC) {

        if (this.containersToKill.containsKey(runningCont.getContainerId())) {
          // These containers have already been marked to be killed.
          // So exclude them..
          continue;
        }
        extraOpportContainersToKill.add(runningCont);
        ContainersMonitor.decreaseResourceUtilization(getContainersMonitor(),
            resourcesToFreeUp, runningCont.getResource());
      }
    }
    if (!hasSufficientResources(resourcesToFreeUp)) {
      LOG.warn("There are no sufficient resources to start guaranteed [{}]"
          + "at the moment. Opportunistic containers are in the process of"
          + "being killed to make room.", containerToStartId);

    }
    return extraOpportContainersToKill;
  }

  private boolean hasSufficientResources(
      ResourceUtilization resourcesToFreeUp) {
    return resourcesToFreeUp.getPhysicalMemory() <= 0
        && resourcesToFreeUp.getVirtualMemory() <= 0
        && resourcesToFreeUp.getCPU() <= 0;
  }

  private ResourceUtilization resourcesToFreeUp(
      ContainerId containerToStartId) {
    // Get allocation of currently allocated containers.
    ResourceUtilization resourceAllocationToFreeUp =
        ResourceUtilization.newInstance(
            this.utilizationTracker.getCurrentUtilization());

    // Add the allocation of the pending guaranteed
    // containers that will start before the current container.
    for (Container container : this.containerQueueManager.getQueuedGuaranteedContainers()
        .values()) {
      ContainersMonitor.increaseResourceUtilization(getContainersMonitor(),
          resourceAllocationToFreeUp, container.getResource());
      if (container.getContainerId().equals(containerToStartId)) {
        break;
      }
    }

    // These resources are already marked to be freed, likely at
    // the behest of another guaranteed container..
    for (Container container : this.containersToKill.values()) {
      ContainersMonitor.decreaseResourceUtilization(getContainersMonitor(),
          resourceAllocationToFreeUp, container.getResource());
    }

    // Subtract the overall node resources.
    getContainersMonitor().subtractNodeResourcesFromResourceUtilization(
        resourceAllocationToFreeUp);
    return resourceAllocationToFreeUp;
  }

  private ContainersMonitor getContainersMonitor() {
    return this.context.getContainerManager().getContainersMonitor();
  }

  /**
   * Trim the opportunistic container queue, killing containers over the queuing
   * limit
   */
  public void shedQueuedOpportunisticContainers() {
    int numAllowed =
        this.containerQueueManager.getMaxOpportunisticQueueingLimit();
    Iterator<Container> containerIter =
        this.containerQueueManager.getQueuedOpportunisticContainers().values()
            .iterator();
    while (containerIter.hasNext()) {
      Container container = containerIter.next();
      // Do not shed PAUSED containers
      if (container.getContainerState() != ContainerState.PAUSED) {
        if (numAllowed <= 0) {
          container.sendKillEvent(
              ContainerExitStatus.KILLED_BY_CONTAINER_SCHEDULER,
              "Container De-queued to meet NM queuing limits.");
          containerIter.remove();
          LOG.info(
              "Opportunistic container {} will be killed to meet NM queuing"
                  + " limits.", container.getContainerId());
        }
        numAllowed--;
      }
    }
    this.containerQueueManager.setUpdatedQueuedContainersMetrics();
  }
}
