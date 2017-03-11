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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

@Private
@Unstable
public class FSSchedulerNode extends SchedulerNode {

  private static final Log LOG = LogFactory.getLog(FSSchedulerNode.class);
  private static final Comparator<RMContainer> comparator =
      new Comparator<RMContainer>() {
    @Override
    public int compare(RMContainer o1, RMContainer o2) {
      return Long.compare(o1.getContainerId().getContainerId(),
          o2.getContainerId().getContainerId());
    }
  };
  private FSAppAttempt reservedAppSchedulable;
  // Stores preemption list until the container is completed
  @VisibleForTesting
  final Map<RMContainer, FSAppAttempt> containersForPreemption =
      new ConcurrentSkipListMap<>(comparator);

  // Stores preemption list after the container is completed before assigned
  @VisibleForTesting
  Map<FSAppAttempt, Resource> reservedApp =
      new LinkedHashMap<>();

  public FSSchedulerNode(RMNode node, boolean usePortForNodeName) {
    super(node, usePortForNodeName);
  }

  @Override
  public synchronized void reserveResource(
      SchedulerApplicationAttempt application, SchedulerRequestKey schedulerKey,
      RMContainer container) {
    // Check if it's already reserved
    RMContainer reservedContainer = getReservedContainer();
    if (reservedContainer != null) {
      // Sanity check
      if (!container.getContainer().getNodeId().equals(getNodeID())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + container +
            " on node " + container.getReservedNode() + 
            " when currently" + " reserved resource " + reservedContainer +
            " on node " + reservedContainer.getReservedNode());
      }
      
      // Cannot reserve more than one application on a given node!
      if (!reservedContainer.getContainer().getId().getApplicationAttemptId()
          .equals(container.getContainer().getId().getApplicationAttemptId())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + container + 
            " for application " + application.getApplicationId() + 
            " when currently" +
            " reserved container " + reservedContainer +
            " on node " + this);
      }

      LOG.info("Updated reserved container " + container.getContainer().getId()
          + " on node " + this + " for application "
          + application.getApplicationId());
    } else {
      LOG.info("Reserved container " + container.getContainer().getId()
          + " on node " + this + " for application "
          + application.getApplicationId());
    }
    setReservedContainer(container);
    this.reservedAppSchedulable = (FSAppAttempt) application;
  }

  @Override
  public synchronized void unreserveResource(
      SchedulerApplicationAttempt application) {
    // Cannot unreserve for wrong application...
    ApplicationAttemptId reservedApplication = 
        getReservedContainer().getContainer().getId().getApplicationAttemptId(); 
    if (!reservedApplication.equals(
        application.getApplicationAttemptId())) {
      throw new IllegalStateException("Trying to unreserve " +  
          " for application " + application.getApplicationId() + 
          " when currently reserved " + 
          " for application " + reservedApplication.getApplicationId() + 
          " on node " + this);
    }
    
    setReservedContainer(null);
    this.reservedAppSchedulable = null;
  }

  synchronized FSAppAttempt getReservedAppSchedulable() {
    return reservedAppSchedulable;
  }

  /**
   * List reserved resources after preemption and assign them to the
   * appropriate applications in a FIFO order.
   * @return if any resources were allocated
   */
  synchronized boolean assignContainersToPreemptionReservees() {
    boolean assignedContainers = false;
    Iterator<FSAppAttempt> iterator = reservedApp.keySet().iterator();
    while (iterator.hasNext()) {
      FSAppAttempt app = iterator.next();
      boolean removeApp = false;
      if (!app.isStopped()) {
        Resource assigned = app.assignContainer(this);
        if (!Resources.isNone(assigned)) {
          assignedContainers = true;
          Resource remaining =
              Resources.subtractFrom(reservedApp.get(app), assigned);
          if (remaining.getMemorySize() <= 0 &&
              remaining.getVirtualCores() <= 0) {
            // No more preempted containers
            removeApp = true;
          }
        }
      } else {
        // App is stopped
        removeApp = true;
      }
      if (Resources.equals(app.getPendingDemand(), Resources.none())) {
        // App does not need more resources
        removeApp = true;
      }
      if (removeApp) {
        iterator.remove();
      }
    }
    return assignedContainers;
  }

  /**
   * Mark {@code containers} as being considered for preemption so they are
   * not considered again. A call to this requires a corresponding call to
   * {@link #removeContainerForPreemption} to ensure we do not mark a
   * container for preemption and never consider it again and avoid memory
   * leaks.
   *
   * @param containers container to mark
   */
  void addContainersForPreemption(Collection<RMContainer> containers,
                                  FSAppAttempt appAttempt) {
    for(RMContainer container : containers) {
      containersForPreemption.put(container, appAttempt);
    }
  }

  /**
   * @return set of containers marked for preemption.
   */
  Set<RMContainer> getContainersForPreemption() {
    return containersForPreemption.keySet();
  }

  /**
   * Remove container from the set of containers marked for preemption.
   * Reserve the preempted resources for the app that requested
   * the preemption.
   *
   * @param container container to remove
   */
  private synchronized void removeContainerForPreemption(RMContainer container) {
    FSAppAttempt app = containersForPreemption.get(container);
    if (app != null) {
      Resource containerSize =
          Resources.clone(container.getAllocatedResource());
      if (!reservedApp.containsKey(app)) {
        reservedApp.put(app, containerSize);
      } else {
        Resources.addTo(reservedApp.get(app),
            Resources.clone(containerSize));
      }
    }
    containersForPreemption.remove(container);
  }

  /**
   * Release an allocated container on this node.
   * It also releases from the reservation list to trigger preemption
   * allocations.
   * @param containerId ID of container to be released.
   * @param releasedByNode whether the release originates from a node update.
   */
  @Override
  public synchronized void releaseContainer(ContainerId containerId,
                                            boolean releasedByNode) {
    RMContainer container = getContainer(containerId);
    super.releaseContainer(containerId, releasedByNode);
    if (container != null) {
      removeContainerForPreemption(container);
    }
  }
}
