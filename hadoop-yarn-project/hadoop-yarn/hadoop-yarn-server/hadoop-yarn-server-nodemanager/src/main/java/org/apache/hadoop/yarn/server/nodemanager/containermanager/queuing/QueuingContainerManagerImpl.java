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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.queuing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl.ProcessTreeInfo;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerStatus;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class extending {@link ContainerManagerImpl} and is used when queuing at the
 * NM is enabled.
 */
public class QueuingContainerManagerImpl extends ContainerManagerImpl {

  private static final Logger LOG = LoggerFactory
      .getLogger(QueuingContainerManagerImpl.class);

  private ConcurrentMap<ContainerId, AllocatedContainerInfo>
        allocatedGuaranteedContainers;
  private ConcurrentMap<ContainerId, AllocatedContainerInfo>
        allocatedOpportunisticContainers;

  private Queue<AllocatedContainerInfo> queuedGuaranteedContainers;
  private Queue<AllocatedContainerInfo> queuedOpportunisticContainers;

  private Set<ContainerId> opportunisticContainersToKill;
  private final ContainerQueuingLimit queuingLimit;

  public QueuingContainerManagerImpl(Context context, ContainerExecutor exec,
      DeletionService deletionContext, NodeStatusUpdater nodeStatusUpdater,
      NodeManagerMetrics metrics, LocalDirsHandlerService dirsHandler) {
    super(context, exec, deletionContext, nodeStatusUpdater, metrics,
        dirsHandler);
    this.allocatedGuaranteedContainers = new ConcurrentHashMap<>();
    this.allocatedOpportunisticContainers = new ConcurrentHashMap<>();
    this.queuedGuaranteedContainers = new ConcurrentLinkedQueue<>();
    this.queuedOpportunisticContainers = new ConcurrentLinkedQueue<>();
    this.opportunisticContainersToKill = Collections.synchronizedSet(
        new HashSet<ContainerId>());
    this.queuingLimit = ContainerQueuingLimit.newInstance();
  }

  @Override
  protected EventHandler<ApplicationEvent> createApplicationEventDispatcher() {
    return new QueuingApplicationEventDispatcher(
        super.createApplicationEventDispatcher());
  }

  @Override
  protected void startContainerInternal(
      ContainerTokenIdentifier containerTokenIdentifier,
      StartContainerRequest request) throws YarnException, IOException {
    this.context.getQueuingContext().getQueuedContainers().put(
        containerTokenIdentifier.getContainerID(), containerTokenIdentifier);

    AllocatedContainerInfo allocatedContInfo = new AllocatedContainerInfo(
        containerTokenIdentifier, request,
        containerTokenIdentifier.getExecutionType(), containerTokenIdentifier
            .getResource(), getConfig());

    // If there are already free resources for the container to start, and
    // there are no queued containers waiting to be executed, start this
    // container immediately.
    if (queuedGuaranteedContainers.isEmpty() &&
        queuedOpportunisticContainers.isEmpty() &&
        getContainersMonitor().
            hasResourcesAvailable(allocatedContInfo.getPti())) {
      startAllocatedContainer(allocatedContInfo);
    } else {
      ContainerId cIdToStart = containerTokenIdentifier.getContainerID();
      this.context.getNMStateStore().storeContainer(cIdToStart, request);
      this.context.getNMStateStore().storeContainerQueued(cIdToStart);
      LOG.info("No available resources for container {} to start its execution "
          + "immediately.", cIdToStart);
      if (allocatedContInfo.getExecutionType() == ExecutionType.GUARANTEED) {
        queuedGuaranteedContainers.add(allocatedContInfo);
        // Kill running opportunistic containers to make space for
        // guaranteed container.
        killOpportunisticContainers(allocatedContInfo);
      } else {
        LOG.info("Opportunistic container {} will be queued at the NM.",
            cIdToStart);
        queuedOpportunisticContainers.add(allocatedContInfo);
      }
    }
  }

  @Override
  protected void stopContainerInternal(ContainerId containerID)
      throws YarnException, IOException {
    Container container = this.context.getContainers().get(containerID);
    // If container is null and distributed scheduling is enabled, container
    // might be queued. Otherwise, container might not be handled by this NM.
    if (container == null && this.context.getQueuingContext()
        .getQueuedContainers().containsKey(containerID)) {
      ContainerTokenIdentifier containerTokenId = this.context
          .getQueuingContext().getQueuedContainers().remove(containerID);

      boolean foundInQueue = removeQueuedContainer(containerID,
          containerTokenId.getExecutionType());

      if (foundInQueue) {
        this.context.getQueuingContext().getKilledQueuedContainers().put(
            containerTokenId,
            "Queued container request removed by ApplicationMaster.");
        this.context.getNMStateStore().storeContainerKilled(containerID);
      } else {
        // The container started execution in the meanwhile.
        try {
          stopContainerInternalIfRunning(containerID);
        } catch (YarnException | IOException e) {
          LOG.error("Container did not get removed successfully.", e);
        }
      }

      nodeStatusUpdater.sendOutofBandHeartBeat();
    }
    super.stopContainerInternal(containerID);
  }

  /**
   * Start the execution of the given container. Also add it to the allocated
   * containers, and update allocated resource utilization.
   */
  private void startAllocatedContainer(
      AllocatedContainerInfo allocatedContainerInfo) {
    ProcessTreeInfo pti = allocatedContainerInfo.getPti();

    if (allocatedContainerInfo.getExecutionType() ==
        ExecutionType.GUARANTEED) {
      allocatedGuaranteedContainers.put(pti.getContainerId(),
          allocatedContainerInfo);
    } else {
      allocatedOpportunisticContainers.put(pti.getContainerId(),
          allocatedContainerInfo);
    }

    getContainersMonitor().increaseContainersAllocation(pti);

    // Start execution of container.
    ContainerId containerId = allocatedContainerInfo
        .getContainerTokenIdentifier().getContainerID();
    this.context.getQueuingContext().getQueuedContainers().remove(containerId);
    try {
      LOG.info("Starting container [" + containerId + "]");
      super.startContainerInternal(
          allocatedContainerInfo.getContainerTokenIdentifier(),
          allocatedContainerInfo.getStartRequest());
    } catch (YarnException | IOException e) {
      containerFailedToStart(pti.getContainerId(),
          allocatedContainerInfo.getContainerTokenIdentifier());
      LOG.error("Container failed to start.", e);
    }
  }

  private void containerFailedToStart(ContainerId containerId,
      ContainerTokenIdentifier containerTokenId) {
    this.context.getQueuingContext().getQueuedContainers().remove(containerId);

    removeAllocatedContainer(containerId);

    this.context.getQueuingContext().getKilledQueuedContainers().put(
        containerTokenId,
        "Container removed from queue as it failed to start.");
  }

  /**
   * Remove the given container from the container queues.
   *
   * @return true if the container was found in one of the queues.
   */
  private boolean removeQueuedContainer(ContainerId containerId,
      ExecutionType executionType) {
    Queue<AllocatedContainerInfo> queue =
        (executionType == ExecutionType.GUARANTEED) ?
            queuedGuaranteedContainers : queuedOpportunisticContainers;

    boolean foundInQueue = false;
    Iterator<AllocatedContainerInfo> iter = queue.iterator();
    while (iter.hasNext() && !foundInQueue) {
      if (iter.next().getPti().getContainerId().equals(containerId)) {
        iter.remove();
        foundInQueue = true;
      }
    }

    return foundInQueue;
  }

  /**
   * Remove the given container from the allocated containers, and update
   * allocated container utilization accordingly.
   */
  private void removeAllocatedContainer(ContainerId containerId) {
    AllocatedContainerInfo contToRemove = null;

    contToRemove = allocatedGuaranteedContainers.remove(containerId);

    if (contToRemove == null) {
      contToRemove = allocatedOpportunisticContainers.remove(containerId);
    }

    // If container was indeed running, update allocated resource utilization.
    if (contToRemove != null) {
      getContainersMonitor().decreaseContainersAllocation(contToRemove
          .getPti());
    }
  }

  /**
   * Stop a container only if it is currently running. If queued, do not stop
   * it.
   */
  private void stopContainerInternalIfRunning(ContainerId containerID)
      throws YarnException, IOException {
    if (this.context.getContainers().containsKey(containerID)) {
      stopContainerInternal(containerID);
    }
  }

  /**
   * Kill opportunistic containers to free up resources for running the given
   * container.
   *
   * @param allocatedContInfo
   *          the container whose execution needs to start by freeing up
   *          resources occupied by opportunistic containers.
   */
  private void killOpportunisticContainers(
      AllocatedContainerInfo allocatedContInfo) {
    ContainerId containerToStartId = allocatedContInfo.getPti()
        .getContainerId();
    List<ContainerId> extraOpportContainersToKill =
        pickOpportunisticContainersToKill(containerToStartId);

    // Kill the opportunistic containers that were chosen.
    for (ContainerId contIdToKill : extraOpportContainersToKill) {
      try {
        stopContainerInternalIfRunning(contIdToKill);
      } catch (YarnException | IOException e) {
        LOG.error("Container did not get removed successfully.", e);
      }
      LOG.info(
          "Opportunistic container {} will be killed in order to start the "
              + "execution of guaranteed container {}.",
              contIdToKill, containerToStartId);
    }
  }

  /**
   * Choose the opportunistic containers to kill in order to free up resources
   * for running the given container.
   *
   * @param containerToStartId
   *          the container whose execution needs to start by freeing up
   *          resources occupied by opportunistic containers.
   * @return the additional opportunistic containers that need to be killed.
   */
  protected List<ContainerId> pickOpportunisticContainersToKill(
      ContainerId containerToStartId) {
    // The additional opportunistic containers that need to be killed for the
    // given container to start.
    List<ContainerId> extraOpportContainersToKill = new ArrayList<>();
    // Track resources that need to be freed.
    ResourceUtilization resourcesToFreeUp = resourcesToFreeUp(
        containerToStartId);

    // Go over the running opportunistic containers. Avoid containers that have
    // already been marked for killing.
    boolean hasSufficientResources = false;
    for (Map.Entry<ContainerId, AllocatedContainerInfo> runningOpportCont :
        allocatedOpportunisticContainers.entrySet()) {
      ContainerId runningOpportContId = runningOpportCont.getKey();

      // If there are sufficient resources to execute the given container, do
      // not kill more opportunistic containers.
      if (resourcesToFreeUp.getPhysicalMemory() <= 0 &&
          resourcesToFreeUp.getVirtualMemory() <= 0 &&
          resourcesToFreeUp.getCPU() <= 0.0f) {
        hasSufficientResources = true;
        break;
      }

      if (!opportunisticContainersToKill.contains(runningOpportContId)) {
        extraOpportContainersToKill.add(runningOpportContId);
        opportunisticContainersToKill.add(runningOpportContId);
        getContainersMonitor().decreaseResourceUtilization(resourcesToFreeUp,
            runningOpportCont.getValue().getPti());
      }
    }

    if (!hasSufficientResources) {
      LOG.info(
          "There are no sufficient resources to start guaranteed {} even after "
              + "attempting to kill any running opportunistic containers.",
          containerToStartId);
    }

    return extraOpportContainersToKill;
  }

  /**
   * Calculates the amount of resources that need to be freed up (by killing
   * opportunistic containers) in order for the given guaranteed container to
   * start its execution. Resource allocation to be freed up =
   * <code>containersAllocation</code> -
   *   allocation of <code>opportunisticContainersToKill</code> +
   *   allocation of <code>queuedGuaranteedContainers</code> that will start
   *     before the given container +
   *   allocation of given container -
   *   total resources of node.
   *
   * @param containerToStartId
   *          the ContainerId of the guaranteed container for which we need to
   *          free resources, so that its execution can start.
   * @return the resources that need to be freed up for the given guaranteed
   *         container to start.
   */
  private ResourceUtilization resourcesToFreeUp(
      ContainerId containerToStartId) {
    // Get allocation of currently allocated containers.
    ResourceUtilization resourceAllocationToFreeUp = ResourceUtilization
        .newInstance(getContainersMonitor().getContainersAllocation());

    // Subtract from the allocation the allocation of the opportunistic
    // containers that are marked for killing.
    for (ContainerId opportContId : opportunisticContainersToKill) {
      if (allocatedOpportunisticContainers.containsKey(opportContId)) {
        getContainersMonitor().decreaseResourceUtilization(
            resourceAllocationToFreeUp,
            allocatedOpportunisticContainers.get(opportContId).getPti());
      }
    }
    // Add to the allocation the allocation of the pending guaranteed
    // containers that will start before the current container will be started.
    for (AllocatedContainerInfo guarContInfo : queuedGuaranteedContainers) {
      getContainersMonitor().increaseResourceUtilization(
          resourceAllocationToFreeUp, guarContInfo.getPti());
      if (guarContInfo.getPti().getContainerId().equals(containerToStartId)) {
        break;
      }
    }
    // Subtract the overall node resources.
    getContainersMonitor().subtractNodeResourcesFromResourceUtilization(
        resourceAllocationToFreeUp);
    return resourceAllocationToFreeUp;
  }

  /**
   * If there are available resources, try to start as many pending containers
   * as possible.
   */
  private void startPendingContainers() {
    // Start pending guaranteed containers, if resources available.
    boolean resourcesAvailable =
        startContainersFromQueue(queuedGuaranteedContainers);

    // Start opportunistic containers, if resources available.
    if (resourcesAvailable) {
      startContainersFromQueue(queuedOpportunisticContainers);
    }
  }

  private boolean startContainersFromQueue(
      Queue<AllocatedContainerInfo> queuedContainers) {
    Iterator<AllocatedContainerInfo> guarIter = queuedContainers.iterator();
    boolean resourcesAvailable = true;

    while (guarIter.hasNext() && resourcesAvailable) {
      AllocatedContainerInfo allocatedContInfo = guarIter.next();

      if (getContainersMonitor().hasResourcesAvailable(
          allocatedContInfo.getPti())) {
        startAllocatedContainer(allocatedContInfo);
        guarIter.remove();
      } else {
        resourcesAvailable = false;
      }
    }
    return resourcesAvailable;
  }

  @Override
  protected ContainerStatus getContainerStatusInternal(ContainerId containerID,
      NMTokenIdentifier nmTokenIdentifier) throws YarnException {
    Container container = this.context.getContainers().get(containerID);
    if (container == null) {
      ContainerTokenIdentifier containerTokenId = this.context
          .getQueuingContext().getQueuedContainers().get(containerID);
      if (containerTokenId != null) {
        ExecutionType executionType = this.context.getQueuingContext()
            .getQueuedContainers().get(containerID).getExecutionType();
        return BuilderUtils.newContainerStatus(containerID,
            org.apache.hadoop.yarn.api.records.ContainerState.QUEUED, "",
            ContainerExitStatus.INVALID, this.context.getQueuingContext()
                .getQueuedContainers().get(containerID).getResource(),
            executionType);
      }
    }
    return super.getContainerStatusInternal(containerID, nmTokenIdentifier);
  }

  /**
   * Recover running or queued container.
   */
  @Override
  protected void recoverActiveContainer(
      ContainerLaunchContext launchContext, ContainerTokenIdentifier token,
      RecoveredContainerState rcs) throws IOException {
    if (rcs.getStatus() ==
        RecoveredContainerStatus.QUEUED && !rcs.getKilled()) {
      LOG.info(token.getContainerID()
          + "will be added to the queued containers.");

      AllocatedContainerInfo allocatedContInfo = new AllocatedContainerInfo(
          token, rcs.getStartRequest(), token.getExecutionType(),
              token.getResource(), getConfig());

      this.context.getQueuingContext().getQueuedContainers().put(
          token.getContainerID(), token);

      if (allocatedContInfo.getExecutionType() == ExecutionType.GUARANTEED) {
        queuedGuaranteedContainers.add(allocatedContInfo);
        // Kill running opportunistic containers to make space for
        // guaranteed container.
        killOpportunisticContainers(allocatedContInfo);
      } else {
        queuedOpportunisticContainers.add(allocatedContInfo);
      }
    } else {
      super.recoverActiveContainer(launchContext, token, rcs);
    }
  }

  @VisibleForTesting
  public int getNumAllocatedGuaranteedContainers() {
    return allocatedGuaranteedContainers.size();
  }

  @VisibleForTesting
  public int getNumAllocatedOpportunisticContainers() {
    return allocatedOpportunisticContainers.size();
  }

  class QueuingApplicationEventDispatcher implements
      EventHandler<ApplicationEvent> {
    private EventHandler<ApplicationEvent> applicationEventDispatcher;

    public QueuingApplicationEventDispatcher(
        EventHandler<ApplicationEvent> applicationEventDispatcher) {
      this.applicationEventDispatcher = applicationEventDispatcher;
    }

    @Override
    public void handle(ApplicationEvent event) {
      if (event.getType() ==
          ApplicationEventType.APPLICATION_CONTAINER_FINISHED) {
        if (!(event instanceof ApplicationContainerFinishedEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        ApplicationContainerFinishedEvent finishEvent =
            (ApplicationContainerFinishedEvent) event;
        // Remove finished container from the allocated containers, and
        // attempt to start new containers.
        ContainerId contIdToRemove = finishEvent.getContainerID();
        removeAllocatedContainer(contIdToRemove);
        opportunisticContainersToKill.remove(contIdToRemove);
        startPendingContainers();
      }
      this.applicationEventDispatcher.handle(event);
    }
  }

  @Override
  public void updateQueuingLimit(ContainerQueuingLimit limit) {
    this.queuingLimit.setMaxQueueLength(limit.getMaxQueueLength());
    // TODO: Include wait time as well once it is implemented
    if (this.queuingLimit.getMaxQueueLength() > -1) {
      shedQueuedOpportunisticContainers();
    }
  }

  private void shedQueuedOpportunisticContainers() {
    int numAllowed = this.queuingLimit.getMaxQueueLength();
    Iterator<AllocatedContainerInfo> containerIter =
        queuedOpportunisticContainers.iterator();
    while (containerIter.hasNext()) {
      AllocatedContainerInfo cInfo = containerIter.next();
      if (numAllowed <= 0) {
        containerIter.remove();
        ContainerTokenIdentifier containerTokenIdentifier = this.context
            .getQueuingContext().getQueuedContainers().remove(
                cInfo.getContainerTokenIdentifier().getContainerID());
        // The Container might have already started while we were
        // iterating..
        if (containerTokenIdentifier != null) {
          this.context.getQueuingContext().getKilledQueuedContainers()
              .putIfAbsent(cInfo.getContainerTokenIdentifier(),
                  "Container de-queued to meet NM queuing limits. "
                      + "Max Queue length["
                      + this.queuingLimit.getMaxQueueLength() + "]");
        }
      }
      numAllowed--;
    }
  }


  static class AllocatedContainerInfo {
    private final ContainerTokenIdentifier containerTokenIdentifier;
    private final StartContainerRequest startRequest;
    private final ExecutionType executionType;
    private final ProcessTreeInfo pti;

    AllocatedContainerInfo(ContainerTokenIdentifier containerTokenIdentifier,
        StartContainerRequest startRequest, ExecutionType executionType,
        Resource resource, Configuration conf) {
      this.containerTokenIdentifier = containerTokenIdentifier;
      this.startRequest = startRequest;
      this.executionType = executionType;
      this.pti = createProcessTreeInfo(containerTokenIdentifier
          .getContainerID(), resource, conf);
    }

    private ContainerTokenIdentifier getContainerTokenIdentifier() {
      return this.containerTokenIdentifier;
    }

    private StartContainerRequest getStartRequest() {
      return this.startRequest;
    }

    private ExecutionType getExecutionType() {
      return this.executionType;
    }

    protected ProcessTreeInfo getPti() {
      return this.pti;
    }

    private ProcessTreeInfo createProcessTreeInfo(ContainerId containerId,
        Resource resource, Configuration conf) {
      long pmemBytes = resource.getMemorySize() * 1024 * 1024L;
      float pmemRatio = conf.getFloat(YarnConfiguration.NM_VMEM_PMEM_RATIO,
          YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO);
      long vmemBytes = (long) (pmemRatio * pmemBytes);
      int cpuVcores = resource.getVirtualCores();

      return new ProcessTreeInfo(containerId, null, null, vmemBytes, pmemBytes,
          cpuVcores);
    }

    @Override
    public boolean equals(Object obj) {
      boolean equal = false;
      if (obj instanceof AllocatedContainerInfo) {
        AllocatedContainerInfo otherContInfo = (AllocatedContainerInfo) obj;
        equal = this.getPti().getContainerId()
            .equals(otherContInfo.getPti().getContainerId());
      }
      return equal;
    }

    @Override
    public int hashCode() {
      return this.getPti().getContainerId().hashCode();
    }
  }
}
