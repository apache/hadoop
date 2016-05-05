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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FiCaSchedulerNode extends SchedulerNode {

  private static final Log LOG = LogFactory.getLog(FiCaSchedulerNode.class);
  private Map<ContainerId, RMContainer> killableContainers = new HashMap<>();
  private Resource totalKillableResources = Resource.newInstance(0, 0);
  
  public FiCaSchedulerNode(RMNode node, boolean usePortForNodeName,
      Set<String> nodeLabels) {
    super(node, usePortForNodeName, nodeLabels);
  }

  public FiCaSchedulerNode(RMNode node, boolean usePortForNodeName) {
    this(node, usePortForNodeName, CommonNodeLabelsManager.EMPTY_STRING_SET);
  }

  @Override
  public synchronized void reserveResource(
      SchedulerApplicationAttempt application, Priority priority,
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
      
      // Cannot reserve more than one application attempt on a given node!
      // Reservation is still against attempt.
      if (!reservedContainer.getContainer().getId().getApplicationAttemptId()
          .equals(container.getContainer().getId().getApplicationAttemptId())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + container + 
            " for application " + application.getApplicationAttemptId() + 
            " when currently" +
            " reserved container " + reservedContainer +
            " on node " + this);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Updated reserved container "
            + container.getContainer().getId() + " on node " + this
            + " for application attempt "
            + application.getApplicationAttemptId());
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reserved container "
            + container.getContainer().getId() + " on node " + this
            + " for application attempt "
            + application.getApplicationAttemptId());
      }
    }
    setReservedContainer(container);
  }

  @Override
  public synchronized void unreserveResource(
      SchedulerApplicationAttempt application) {
    // adding NP checks as this can now be called for preemption
    if (getReservedContainer() != null
        && getReservedContainer().getContainer() != null
        && getReservedContainer().getContainer().getId() != null
        && getReservedContainer().getContainer().getId()
          .getApplicationAttemptId() != null) {

      // Cannot unreserve for wrong application...
      ApplicationAttemptId reservedApplication =
          getReservedContainer().getContainer().getId()
            .getApplicationAttemptId();
      if (!reservedApplication.equals(
          application.getApplicationAttemptId())) {
        throw new IllegalStateException("Trying to unreserve " +
            " for application " + application.getApplicationAttemptId() +
            " when currently reserved " +
            " for application " + reservedApplication.getApplicationId() +
            " on node " + this);
      }
    }
    setReservedContainer(null);
  }

  // According to decisions from preemption policy, mark the container to killable
  public synchronized void markContainerToKillable(ContainerId containerId) {
    RMContainer c = launchedContainers.get(containerId);
    if (c != null && !killableContainers.containsKey(containerId)) {
      killableContainers.put(containerId, c);
      Resources.addTo(totalKillableResources, c.getAllocatedResource());
    }
  }

  // According to decisions from preemption policy, mark the container to
  // non-killable
  public synchronized void markContainerToNonKillable(ContainerId containerId) {
    RMContainer c = launchedContainers.get(containerId);
    if (c != null && killableContainers.containsKey(containerId)) {
      killableContainers.remove(containerId);
      Resources.subtractFrom(totalKillableResources, c.getAllocatedResource());
    }
  }

  @Override
  protected synchronized void updateResourceForReleasedContainer(
      Container container) {
    super.updateResourceForReleasedContainer(container);
    if (killableContainers.containsKey(container.getId())) {
      Resources.subtractFrom(totalKillableResources, container.getResource());
      killableContainers.remove(container.getId());
    }
  }

  @Override
  protected synchronized void changeContainerResource(ContainerId containerId,
      Resource deltaResource, boolean increase) {
    super.changeContainerResource(containerId, deltaResource, increase);

    if (killableContainers.containsKey(containerId)) {
      if (increase) {
        Resources.addTo(totalKillableResources, deltaResource);
      } else {
        Resources.subtractFrom(totalKillableResources, deltaResource);
      }
    }
  }

  public synchronized Resource getTotalKillableResources() {
    return totalKillableResources;
  }

  public synchronized Map<ContainerId, RMContainer> getKillableContainers() {
    return Collections.unmodifiableMap(killableContainers);
  }
}
