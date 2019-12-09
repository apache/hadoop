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
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Fair Scheduler specific node features.
 */
@Private
@Unstable
public class FSSchedulerNode extends SchedulerNode {

  private static final Log LOG = LogFactory.getLog(FSSchedulerNode.class);
  private FSAppAttempt reservedAppSchedulable;
  // Stores list of containers still to be preempted
  @VisibleForTesting
  final Set<RMContainer> containersForPreemption =
      new ConcurrentSkipListSet<>();
  // Stores amount of resources preempted and reserved for each app
  @VisibleForTesting
  final Map<FSAppAttempt, Resource>
      resourcesPreemptedForApp = new LinkedHashMap<>();
  private final Map<ApplicationAttemptId, FSAppAttempt> appIdToAppMap =
      new HashMap<>();
  // Sum of resourcesPreemptedForApp values, total resources that are
  // slated for preemption
  private Resource totalResourcesPreempted = Resource.newInstance(0, 0);

  public FSSchedulerNode(RMNode node, boolean usePortForNodeName) {
    super(node, usePortForNodeName);
  }

  /**
   * Total amount of reserved resources including reservations and preempted
   * containers.
   * @return total resources reserved
   */
  Resource getTotalReserved() {
    Resource totalReserved = Resources.clone(getReservedContainer() != null
        ? getReservedContainer().getAllocatedResource()
        : Resource.newInstance(0, 0));
    Resources.addTo(totalReserved, totalResourcesPreempted);
    return totalReserved;
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
        getReservedContainer().getContainer().getId()
            .getApplicationAttemptId();
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
  @VisibleForTesting
  synchronized LinkedHashMap<FSAppAttempt, Resource> getPreemptionList() {
    cleanupPreemptionList();
    return new LinkedHashMap<>(resourcesPreemptedForApp);
  }

  /**
   * Returns whether a preemption is tracked on the node for the specified app.
   * @return if preempted containers are reserved for the app
   */
  synchronized boolean isPreemptedForApp(FSAppAttempt app){
    return resourcesPreemptedForApp.containsKey(app);
  }

  /**
   * Remove apps that have their preemption requests fulfilled.
   */
  private void cleanupPreemptionList() {
    // Synchronize separately to avoid potential deadlocks
    // This may cause delayed deletion of reservations
    LinkedList<FSAppAttempt> candidates;
    synchronized (this) {
      candidates = Lists.newLinkedList(resourcesPreemptedForApp.keySet());
    }
    for (FSAppAttempt app : candidates) {
      if (app.isStopped() || !app.isStarved() ||
          (Resources.isNone(app.getFairshareStarvation()) &&
           Resources.isNone(app.getMinshareStarvation()))) {
        // App does not need more resources
        synchronized (this) {
          Resource removed = resourcesPreemptedForApp.remove(app);
          if (removed != null) {
            Resources.subtractFrom(totalResourcesPreempted,
                removed);
            appIdToAppMap.remove(app.getApplicationAttemptId());
          }
        }
      }
    }
  }

  /**
   * Mark {@code containers} as being considered for preemption so they are
   * not considered again. A call to this requires a corresponding call to
   * {@code releaseContainer} to ensure we do not mark a container for
   * preemption and never consider it again and avoid memory leaks.
   *
   * @param containers container to mark
   */
  void addContainersForPreemption(Collection<RMContainer> containers,
                                  FSAppAttempt app) {

    Resource appReserved = Resources.createResource(0);

    for(RMContainer container : containers) {
      if(containersForPreemption.add(container)) {
        Resources.addTo(appReserved, container.getAllocatedResource());
      }
    }

    synchronized (this) {
      if (!Resources.isNone(appReserved)) {
        Resources.addTo(totalResourcesPreempted,
            appReserved);
        appIdToAppMap.putIfAbsent(app.getApplicationAttemptId(), app);
        resourcesPreemptedForApp.
            putIfAbsent(app, Resource.newInstance(0, 0));
        Resources.addTo(resourcesPreemptedForApp.get(app), appReserved);
      }
    }
  }

  /**
   * @return set of containers marked for preemption.
   */
  Set<RMContainer> getContainersForPreemption() {
    return containersForPreemption;
  }

  /**
   * The Scheduler has allocated containers on this node to the given
   * application.
   * @param rmContainer Allocated container
   * @param launchedOnNode True if the container has been launched
   */
  @Override
  protected synchronized void allocateContainer(RMContainer rmContainer,
                                                boolean launchedOnNode) {
    super.allocateContainer(rmContainer, launchedOnNode);
    if (LOG.isDebugEnabled()) {
      final Container container = rmContainer.getContainer();
      LOG.debug("Assigned container " + container.getId() + " of capacity "
          + container.getResource() + " on host " + getRMNode().getNodeAddress()
          + ", which has " + getNumContainers() + " containers, "
          + getAllocatedResource() + " used and " + getUnallocatedResource()
          + " available after allocation");
    }

    Resource allocated = rmContainer.getAllocatedResource();
    if (!Resources.isNone(allocated)) {
      // check for satisfied preemption request and update bookkeeping
      FSAppAttempt app =
          appIdToAppMap.get(rmContainer.getApplicationAttemptId());
      if (app != null) {
        Resource reserved = resourcesPreemptedForApp.get(app);
        Resource fulfilled = Resources.componentwiseMin(reserved, allocated);
        Resources.subtractFrom(reserved, fulfilled);
        Resources.subtractFrom(totalResourcesPreempted, fulfilled);
        if (Resources.isNone(reserved)) {
          // No more preempted containers
          resourcesPreemptedForApp.remove(app);
          appIdToAppMap.remove(rmContainer.getApplicationAttemptId());
        }
      }
    } else {
      LOG.error("Allocated empty container" + rmContainer.getContainerId());
    }
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
      containersForPreemption.remove(container);
    }
  }
}
