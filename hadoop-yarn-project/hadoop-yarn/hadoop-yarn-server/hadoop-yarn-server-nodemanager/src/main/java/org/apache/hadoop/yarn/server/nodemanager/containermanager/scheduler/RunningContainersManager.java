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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * RunningContainersManager is responsible for starting containers and
 * maintaining information of the running containers on the node.
 */
public class RunningContainersManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(RunningContainersManager.class);

  // Containers launched by the Scheduler will take a while to actually
  // move to the RUNNING state, but should still be fair game for killing
  // by the scheduler to make room for guaranteed containers. This holds
  // containers that are in RUNNING as well as those in SCHEDULED state that
  // have been marked to run, but not yet RUNNING.
  private final Map<ContainerId, Container> runningContainers =
      new LinkedHashMap<>();

  private final ResourceUtilizationTracker utilizationTracker;

  private final NodeManagerMetrics metrics;

  private final ContainerQueueManager containerQueueManager;

  /**
   * Instantiate an object of RunningContainersManager
   *
   * @param utilizationTracker
   * @param metrics
   */
  public RunningContainersManager(ResourceUtilizationTracker utilizationTracker,
      NodeManagerMetrics metrics, ContainerQueueManager containerQueueManager) {
    this.utilizationTracker = utilizationTracker;
    this.metrics = metrics;
    this.containerQueueManager = containerQueueManager;
  }

  /**
   * Get all running containers on the node
   *
   * @return Map containing running containers mapped by ContainerId
   */
  public Map<ContainerId, Container> getRunningContainers() {
    return runningContainers;
  }

  /**
   * Start pending containers in the queue.
   */
  public void startPendingContainers(boolean forceStartGuaranteedContainers) {
    // Start guaranteed containers that are paused, if resources available.
    boolean resourcesAvailable = startContainers(
        this.containerQueueManager.getQueuedGuaranteedContainers().values(),
        forceStartGuaranteedContainers);
    // Start opportunistic containers, if resources available.
    if (resourcesAvailable) {
      startContainers(
          this.containerQueueManager.getQueuedOpportunisticContainers()
              .values(), false);
    }
  }

  private boolean startContainers(Collection<Container> containersToBeStarted,
      boolean force) {
    Iterator<Container> containerIterator = containersToBeStarted.iterator();
    boolean resourcesAvailable = true;
    while (containerIterator.hasNext() && resourcesAvailable) {
      Container container = containerIterator.next();
      if (tryStartContainer(container, force)) {
        containerIterator.remove();
      } else {
        resourcesAvailable = false;
      }
    }
    this.containerQueueManager.setUpdatedQueuedContainersMetrics();
    return resourcesAvailable;
  }

  private boolean tryStartContainer(Container container, boolean force) {
    boolean containerStarted = false;
    // call startContainer without checking available resource when force==true
    if (force || resourceAvailableToStartContainer(container)) {
      startContainer(container);
      containerStarted = true;
    }
    return containerStarted;
  }

  /**
   * Check if there is resource available to start a given container
   * immediately. (This can be extended to include overallocated resources)
   *
   * @param container the container to start
   * @return true if container can be launched directly
   */
  private boolean resourceAvailableToStartContainer(Container container) {
    return this.utilizationTracker.hasResourcesAvailable(container);
  }

  private void startContainer(Container container) {
    LOG.info("Starting container [" + container.getContainerId() + "]");
    // Skip to put into runningContainers and addUtilization when recover
    if (!runningContainers.containsKey(container.getContainerId())) {
      addContainerToRunningQueue(container);
      this.utilizationTracker.addContainerResources(container);
    }
    if (container.getContainerTokenIdentifier().getExecutionType()
        == ExecutionType.OPPORTUNISTIC) {
      this.metrics.startOpportunisticContainer(container.getResource());
    }
    container.sendLaunchEvent();
  }

  /**
   * Remove the associated container from running queue
   *
   * @param containerId
   * @return Container that was deleted
   */
  public Container deleteRunningContainer(ContainerId containerId) {
    return runningContainers.remove(containerId);
  }

  /**
   * Add a container to running queue
   *
   * @param container
   */
  public void addContainerToRunningQueue(Container container) {
    runningContainers.put(container.getContainerId(), container);
  }

  /**
   * Check if container is in running queue
   *
   * @param containerId
   * @return true if container is in running queue
   */
  public boolean isContainerInRunningQueue(ContainerId containerId) {
    return runningContainers.containsKey(containerId);
  }

  /**
   * Get number of containers in running queue
   *
   * @return number of containers in running queue
   */
  public int getNumberOfRunningContainers() {
    return runningContainers.size();
  }
}
