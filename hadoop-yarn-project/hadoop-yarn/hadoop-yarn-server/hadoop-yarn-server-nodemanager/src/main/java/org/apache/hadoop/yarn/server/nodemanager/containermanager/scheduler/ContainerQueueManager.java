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
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.yarnpp.YarnppConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ContainerQueueManager class is responsible for maintaining opportunistic and
 * guaranteed container queues.
 */
public class ContainerQueueManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerQueueManager.class);

  // Queue of Guaranteed Containers waiting for resources to run
  private final Map<ContainerId, Container> queuedGuaranteedContainers =
      new LinkedHashMap<>();
  // Queue of Opportunistic Containers waiting for resources to run
  private final Map<ContainerId, Container> queuedOpportunisticContainers =
      new LinkedHashMap<>();
  private final OpportunisticContainersQueuePolicy oppContainersQueuePolicy;

  // Capacity of the queue for opportunistic Containers.
  private int maxOppQueueLength;
  private final Context context;

  private boolean forceStartGuaranteedContainers;

  private final ResourceUtilizationTracker utilizationTracker;
  private final ContainerQueuingLimit queuingLimit =
      ContainerQueuingLimit.newInstance();

  private final NodeManagerMetrics metrics;

  /**
   * Instantiate a new object of ContainerQueueManager
   *
   * @param oppContainersQueuePolicy
   * @param qLength
   * @param utilizationTracker
   * @param metrics
   * @param context
   */
  public ContainerQueueManager(
      OpportunisticContainersQueuePolicy oppContainersQueuePolicy, int qLength,
      ResourceUtilizationTracker utilizationTracker, NodeManagerMetrics metrics,
      Context context) {
    this.utilizationTracker = utilizationTracker;
    this.metrics = metrics;
    this.oppContainersQueuePolicy = oppContainersQueuePolicy;
    this.context = context;
    setOpportunisticContainersQueuePolicy(oppContainersQueuePolicy, qLength);
  }

  /**
   * Set the maximum queuing limit for opportunistic containers
   *
   * @param maxQueueLength
   */
  public void setMaxOpportunisticQueueingLimit(int maxQueueLength) {
    this.queuingLimit.setMaxQueueLength(maxQueueLength);
  }

  /**
   * Get the maximum queuing limit for opportunistic containers
   *
   * @return maximum queuing limit
   */
  public int getMaxOpportunisticQueueingLimit() {
    return this.queuingLimit.getMaxQueueLength();
  }

  /**
   * Set max opportunistic queue length and force start guaranteed container
   *
   * @param oppContainersQueuePolicy
   * @param qLength
   */
  private void setOpportunisticContainersQueuePolicy(
      OpportunisticContainersQueuePolicy oppContainersQueuePolicy,
      int qLength) {
    if (oppContainersQueuePolicy
        == OpportunisticContainersQueuePolicy.BY_RESOURCES) {
      this.maxOppQueueLength = 0;
      this.forceStartGuaranteedContainers = false;
      LOG.info("Setting max opportunistic queue length to 0,"
              + " as {} is incompatible with queue length",
          oppContainersQueuePolicy);
    } else {
      this.maxOppQueueLength = qLength;
      this.forceStartGuaranteedContainers = (maxOppQueueLength <= 0);
    }
  }

  /**
   * Get queued guaranteed containers
   *
   * @return Map containing the id to container mapping of queued guaranteed
   *     containers
   */
  public Map<ContainerId, Container> getQueuedGuaranteedContainers() {
    return queuedGuaranteedContainers;
  }

  /**
   * Get queued opportunistic containers
   *
   * @return Map containing the id to container mapping of queued opportunistic
   *     containers
   */
  public Map<ContainerId, Container> getQueuedOpportunisticContainers() {
    return queuedOpportunisticContainers;
  }

  /**
   * Get the number of queued opportunistic containers
   *
   * @return number of queued opportunistic containers
   */
  public int getNumberOfQueuedOpportunisticContainers() {
    return queuedOpportunisticContainers.size();
  }

  /**
   * Get the number of queued guaranteed containers
   *
   * @return number of queued guaranteed containers
   */
  public int getNumberOfQueuedGuaranteedContainers() {
    return queuedGuaranteedContainers.size();
  }

  /**
   * Get the maximum length of opportunistic container queue
   *
   * @return maximum length of opportunistic container queue
   */
  public int getMaxOppQueueLength() {
    return this.maxOppQueueLength;
  }

  /**
   * Get if guaranteed containers are to be force started
   *
   * @return true if guaranteed containers can be force started
   */
  public boolean getForceStartGuaranteedContainers() {
    return forceStartGuaranteedContainers;
  }

  /**
   * Check if the given guaranteed container is queued
   *
   * @param containerId
   * @return true if the guaranteed container is queued
   */
  public boolean isGuaranteedContainerQueued(ContainerId containerId) {
    return queuedGuaranteedContainers.containsKey(containerId);
  }

  /**
   * Add a guaranteed/opportunistic container to its corresponding queue based
   * on the queuing policy
   *
   * @param container
   * @return true if the container was queued
   */
  public boolean enqueueContainer(Container container) {
    boolean isGuaranteedContainer =
        container.getContainerTokenIdentifier().getExecutionType()
            == ExecutionType.GUARANTEED;

    boolean isQueued;
    if (isGuaranteedContainer) {
      queuedGuaranteedContainers.put(container.getContainerId(), container);
      isQueued = true;
    } else {
      switch (this.oppContainersQueuePolicy) {
      case BY_RESOURCES:
        isQueued = resourceAvailableToQueueOppContainer(container);
        break;
      case BY_QUEUE_LEN:
      default:
        if (maxOppQueueLength <= 0) {
          isQueued = false;
        } else {
          isQueued = queuedOpportunisticContainers.size() < maxOppQueueLength;
        }
      }

      if (isQueued) {
        LOG.info("Opportunistic container {} will be queued at the NM.",
            container.getContainerId());
        queuedOpportunisticContainers.put(container.getContainerId(),
            container);
        isQueued = true;
      } else {
        LOG.info("Opportunistic container [{}] will not be queued at the NM"
                + "since max queue length [{}] has been reached",
            container.getContainerId(), maxOppQueueLength);
        container.sendKillEvent(
            ContainerExitStatus.KILLED_BY_CONTAINER_SCHEDULER,
            "Opportunistic container queue is full.");
      }
    }

    if (isQueued) {
      try {
        this.context.getNMStateStore()
            .storeContainerQueued(container.getContainerId());
      } catch (IOException e) {
        LOG.warn("Could not store container [" + container.getContainerId()
            + "] state. The Container has been queued.", e);
      }
    }
    setUpdatedQueuedContainersMetrics();
    return isQueued;
  }

  private boolean isOpportunisticQueueLengthLimitDisabled() {
    return this.opportunisticQueueLengthLimitDisabled;
  }

  private boolean resourceAvailableToQueueOppContainer(
      Container newOppContainer) {
    final Resource cumulativeResource = Resource.newInstance(Resources.none());
    for (final Container container : queuedGuaranteedContainers.values()) {
      Resources.addTo(cumulativeResource, container.getResource());
    }

    for (final Container container : queuedOpportunisticContainers.values()) {
      Resources.addTo(cumulativeResource, container.getResource());
    }

    Resources.addTo(cumulativeResource, newOppContainer.getResource());
    return this.utilizationTracker.hasResourcesAvailable(cumulativeResource);
  }

  /**
   * Remove opportunistic container from queue
   *
   * @param containerId
   * @return opportunitic container that was dequeued
   */
  public Container removeOpportunisticContainerFromQueue(
      ContainerId containerId) {
    Container removedContainer =
        queuedOpportunisticContainers.remove(containerId);
    setUpdatedQueuedContainersMetrics();
    return removedContainer;
  }

  /**
   * Remove guaranteed container from queue
   *
   * @param containerId
   * @return guaranteed container that was dequeued
   */
  public Container removeGuaranteedContainerFromQueue(ContainerId containerId) {
    Container removedContainer = queuedGuaranteedContainers.remove(containerId);
    setUpdatedQueuedContainersMetrics();
    return removedContainer;
  }

  /**
   * Add a guaranteed container to queue
   *
   * @param container
   */
  public void addGuaranteedContainerToQueue(Container container) {
    queuedGuaranteedContainers.put(container.getContainerId(), container);
    setUpdatedQueuedContainersMetrics();
  }

  /**
   * Add an opportunistic container to queue
   *
   * @param container
   */
  public void addOpportunisticContainerToQueue(Container container) {
    queuedOpportunisticContainers.put(container.getContainerId(), container);
    setUpdatedQueuedContainersMetrics();
  }

  /**
   * Set queued container metrics
   */
  public void setUpdatedQueuedContainersMetrics() {
    this.metrics.setQueuedContainers(getNumberOfQueuedOpportunisticContainers(),
        getNumberOfQueuedGuaranteedContainers());
  }

}
