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
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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
  private final Set<ContainerId> oppContainersMarkedForKill = new HashSet<>();

  // Containers launched by the Scheduler will take a while to actually
  // move to the RUNNING state, but should still be fair game for killing
  // by the scheduler to make room for guaranteed containers.
  private final LinkedHashMap<ContainerId, Container> runningContainers =
      new LinkedHashMap<>();

  private final ContainerQueuingLimit queuingLimit =
      ContainerQueuingLimit.newInstance();

  //
  private ResourceUtilizationManager utilizationManager;

  public ContainerScheduler(Context context) {
    super(ContainerScheduler.class.getName());
    this.context = context;
    int qLength = context.getConf().getInt(
        YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH,
        YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH_DEFAULT);
    this.maxOppQueueLength = (qLength <= 0) ? 0 : qLength;
    this.utilizationManager = new ResourceUtilizationManager(this);
  }

  @Override
  public void handle(ContainerSchedulerEvent event) {
    switch (event.getType()) {
    case SCHEDULE_CONTAINER:
      scheduleContainer(event.getContainer());
      break;
    case CONTAINER_COMPLETED:
      onContainerCompleted(event.getContainer());
      break;
    default:
      LOG.error("Unknown event arrived at ContainerScheduler: "
          + event.toString());
    }
  }

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

  private void onContainerCompleted(Container container) {
    // decrement only if it was a running container
    if (runningContainers.containsKey(container.getContainerId())) {
      this.utilizationManager.subtractResource(container.getResource());
    }
    runningContainers.remove(container.getContainerId());
    oppContainersMarkedForKill.remove(container.getContainerId());
    startPendingContainers();
  }

  private void startPendingContainers() {
    // Start pending guaranteed containers, if resources available.
    boolean resourcesAvailable =
        startContainersFromQueue(queuedGuaranteedContainers.values());
    // Start opportunistic containers, if resources available.
    if (resourcesAvailable) {
      startContainersFromQueue(queuedOpportunisticContainers.values());
    }
  }

  private boolean startContainersFromQueue(
      Collection<Container> queuedContainers) {
    Iterator<Container> cIter = queuedContainers.iterator();
    boolean resourcesAvailable = true;
    while (cIter.hasNext() && resourcesAvailable) {
      Container container = cIter.next();
      if (hasResourcesAvailable(container.getResource())) {
        startAllocatedContainer(container);
        cIter.remove();
      } else {
        resourcesAvailable = false;
      }
    }
    return resourcesAvailable;
  }

  private void scheduleContainer(Container container) {
    if (maxOppQueueLength <= 0) {
      startAllocatedContainer(container);
      return;
    }
    if (queuedGuaranteedContainers.isEmpty() &&
        queuedOpportunisticContainers.isEmpty() &&
        hasResourcesAvailable(container.getResource())) {
      startAllocatedContainer(container);
    } else {
      try {
        this.context.getNMStateStore().storeContainerQueued(
            container.getContainerId());
      } catch (IOException e) {
        LOG.warn("Could not store container state into store..", e);
      }
      LOG.info("No available resources for container {} to start its execution "
          + "immediately.", container.getContainerId());
      if (container.getContainerTokenIdentifier().getExecutionType() ==
          ExecutionType.GUARANTEED) {
        queuedGuaranteedContainers.put(container.getContainerId(), container);
        // Kill running opportunistic containers to make space for
        // guaranteed container.
        killOpportunisticContainers(container);
      } else {
        if (queuedOpportunisticContainers.size() <= maxOppQueueLength) {
          LOG.info("Opportunistic container {} will be queued at the NM.",
              container.getContainerId());
          queuedOpportunisticContainers.put(
              container.getContainerId(), container);
        } else {
          LOG.info("Opportunistic container [{}] will not be queued at the NM" +
              "since max queue length [{}] has been reached",
              container.getContainerId(), maxOppQueueLength);
          container.sendKillEvent(
              ContainerExitStatus.KILLED_BY_CONTAINER_SCHEDULER,
              "Opportunistic container queue is full.");
        }
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
      oppContainersMarkedForKill.add(contToKill.getContainerId());
      LOG.info(
          "Opportunistic container {} will be killed in order to start the "
              + "execution of guaranteed container {}.",
          contToKill.getContainerId(), container.getContainerId());
    }
  }

  private void startAllocatedContainer(Container container) {
    LOG.info("Starting container [" + container.getContainerId()+ "]");
    runningContainers.put(container.getContainerId(), container);
    this.utilizationManager.addResource(container.getResource());
    container.sendLaunchEvent();
  }

  protected List<Container> pickOpportunisticContainersToKill(
      ContainerId containerToStartId) {
    // The additional opportunistic containers that need to be killed for the
    // given container to start.
    List<Container> extraOpportContainersToKill = new ArrayList<>();
    // Track resources that need to be freed.
    ResourceUtilization resourcesToFreeUp = resourcesToFreeUp(
        containerToStartId);

    // Go over the running opportunistic containers.
    // Use a descending iterator to kill more recently started containers.
    Iterator<Container> reverseContainerIterator =
        new LinkedList<>(runningContainers.values()).descendingIterator();
    while(reverseContainerIterator.hasNext() &&
        !hasSufficientResources(resourcesToFreeUp)) {
      Container runningCont = reverseContainerIterator.next();
      if (runningCont.getContainerTokenIdentifier().getExecutionType() ==
          ExecutionType.OPPORTUNISTIC) {

        if (oppContainersMarkedForKill.contains(runningCont.getContainerId())) {
          // These containers have already been marked to be killed.
          // So exclude them..
          continue;
        }
        extraOpportContainersToKill.add(runningCont);
        ResourceUtilizationManager.decreaseResourceUtilization(
            getContainersMonitor(), resourcesToFreeUp,
            runningCont.getResource());
      }
    }
    if (!hasSufficientResources(resourcesToFreeUp)) {
      LOG.warn("There are no sufficient resources to start guaranteed [{}]" +
          "even after attempting to kill all running" +
          "opportunistic containers.", containerToStartId);
    }
    return extraOpportContainersToKill;
  }

  private boolean hasSufficientResources(
      ResourceUtilization resourcesToFreeUp) {
    return resourcesToFreeUp.getPhysicalMemory() <= 0 &&
        resourcesToFreeUp.getVirtualMemory() <= 0 &&
        resourcesToFreeUp.getCPU() <= 0.0f;
  }

  private ResourceUtilization resourcesToFreeUp(
      ContainerId containerToStartId) {
    // Get allocation of currently allocated containers.
    ResourceUtilization resourceAllocationToFreeUp = ResourceUtilization
        .newInstance(this.utilizationManager.getCurrentUtilization());

    // Add to the allocation the allocation of the pending guaranteed
    // containers that will start before the current container will be started.
    for (Container container : queuedGuaranteedContainers.values()) {
      ResourceUtilizationManager.increaseResourceUtilization(
          getContainersMonitor(), resourceAllocationToFreeUp,
          container.getResource());
      if (container.getContainerId().equals(containerToStartId)) {
        break;
      }
    }
    // Subtract the overall node resources.
    getContainersMonitor().subtractNodeResourcesFromResourceUtilization(
        resourceAllocationToFreeUp);
    return resourceAllocationToFreeUp;
  }

  public void updateQueuingLimit(ContainerQueuingLimit limit) {
    this.queuingLimit.setMaxQueueLength(limit.getMaxQueueLength());
    // TODO: Include wait time as well once it is implemented
    if (this.queuingLimit.getMaxQueueLength() > -1) {
      shedQueuedOpportunisticContainers();
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
            "Container Killed to make room for Guaranteed Container.");
        containerIter.remove();
        LOG.info(
            "Opportunistic container {} will be killed to meet NM queuing" +
                " limits.", container.getContainerId());
      }
      numAllowed--;
    }
  }

  private boolean hasResourcesAvailable(Resource resource) {
    long pMemBytes = resource.getMemorySize() * 1024 * 1024L;
    return hasResourcesAvailable(pMemBytes,
        (long) (getContainersMonitor().getVmemRatio()* pMemBytes),
        resource.getVirtualCores());
  }

  private boolean hasResourcesAvailable(long pMemBytes, long vMemBytes,
      int cpuVcores) {
    ResourceUtilization currentUtilization = this.utilizationManager
        .getCurrentUtilization();
    synchronized (currentUtilization) {
      // Check physical memory.
      if (LOG.isDebugEnabled()) {
        LOG.debug("pMemCheck [current={} + asked={} > allowed={}]",
            currentUtilization.getPhysicalMemory(),
            (pMemBytes >> 20),
            (getContainersMonitor().getPmemAllocatedForContainers() >> 20));
      }
      if (currentUtilization.getPhysicalMemory() +
          (int) (pMemBytes >> 20) >
          (int) (getContainersMonitor()
              .getPmemAllocatedForContainers() >> 20)) {
        return false;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("before vMemCheck" +
            "[isEnabled={}, current={} + asked={} > allowed={}]",
            getContainersMonitor().isVmemCheckEnabled(),
            currentUtilization.getVirtualMemory(), (vMemBytes >> 20),
            (getContainersMonitor().getVmemAllocatedForContainers() >> 20));
      }
      // Check virtual memory.
      if (getContainersMonitor().isVmemCheckEnabled() &&
          currentUtilization.getVirtualMemory() +
              (int) (vMemBytes >> 20) >
              (int) (getContainersMonitor()
                  .getVmemAllocatedForContainers() >> 20)) {
        return false;
      }

      float vCores = (float) cpuVcores /
              getContainersMonitor().getVCoresAllocatedForContainers();
      if (LOG.isDebugEnabled()) {
        LOG.debug("before cpuCheck [asked={} > allowed={}]",
            currentUtilization.getCPU(), vCores);
      }
      // Check CPU.
      if (currentUtilization.getCPU() + vCores > 1.0f) {
        return false;
      }
    }
    return true;
  }

  public ContainersMonitor getContainersMonitor() {
    return this.context.getContainerManager().getContainersMonitor();
  }
}
