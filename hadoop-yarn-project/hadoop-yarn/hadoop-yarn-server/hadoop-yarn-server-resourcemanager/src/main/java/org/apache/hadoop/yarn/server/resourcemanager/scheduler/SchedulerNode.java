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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.ImmutableSet;


/**
 * Represents a YARN Cluster Node from the viewpoint of the scheduler.
 */
@Private
@Unstable
public abstract class SchedulerNode {

  private static final Log LOG = LogFactory.getLog(SchedulerNode.class);

  private Resource unallocatedResource = Resource.newInstance(0, 0);
  private Resource allocatedResource = Resource.newInstance(0, 0);
  private Resource totalResource;
  private RMContainer reservedContainer;
  private volatile int numContainers;
  private volatile ResourceUtilization containersUtilization =
      ResourceUtilization.newInstance(0, 0, 0f);
  private volatile ResourceUtilization nodeUtilization =
      ResourceUtilization.newInstance(0, 0, 0f);

  /* set of containers that are allocated containers */
  private final Map<ContainerId, ContainerInfo> launchedContainers =
      new HashMap<>();

  private final RMNode rmNode;
  private final String nodeName;

  private volatile Set<String> labels = null;

  // Last updated time
  private volatile long lastHeartbeatMonotonicTime;

  public SchedulerNode(RMNode node, boolean usePortForNodeName,
      Set<String> labels) {
    this.rmNode = node;
    this.unallocatedResource = Resources.clone(node.getTotalCapability());
    this.totalResource = Resources.clone(node.getTotalCapability());
    if (usePortForNodeName) {
      nodeName = rmNode.getHostName() + ":" + node.getNodeID().getPort();
    } else {
      nodeName = rmNode.getHostName();
    }
    this.labels = ImmutableSet.copyOf(labels);
    this.lastHeartbeatMonotonicTime = Time.monotonicNow();
  }

  public SchedulerNode(RMNode node, boolean usePortForNodeName) {
    this(node, usePortForNodeName, CommonNodeLabelsManager.EMPTY_STRING_SET);
  }

  public RMNode getRMNode() {
    return this.rmNode;
  }

  /**
   * Set total resources on the node.
   * @param resource Total resources on the node.
   */
  public synchronized void updateTotalResource(Resource resource){
    this.totalResource = resource;
    this.unallocatedResource = Resources.subtract(totalResource,
        this.allocatedResource);
  }

  /**
   * Get the ID of the node which contains both its hostname and port.
   * @return The ID of the node.
   */
  public NodeId getNodeID() {
    return this.rmNode.getNodeID();
  }

  /**
   * Get HTTP address for the node.
   * @return HTTP address for the node.
   */
  public String getHttpAddress() {
    return this.rmNode.getHttpAddress();
  }

  /**
   * Get the name of the node for scheduling matching decisions.
   * <p>
   * Typically this is the 'hostname' reported by the node, but it could be
   * configured to be 'hostname:port' reported by the node via the
   * {@link YarnConfiguration#RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME} constant.
   * The main usecase of this is YARN minicluster to be able to differentiate
   * node manager instances by their port number.
   * @return Name of the node for scheduling matching decisions.
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Get rackname.
   * @return rackname
   */
  public String getRackName() {
    return this.rmNode.getRackName();
  }

  /**
   * The Scheduler has allocated containers on this node to the given
   * application.
   * @param rmContainer Allocated container
   */
  public void allocateContainer(RMContainer rmContainer) {
    allocateContainer(rmContainer, false);
  }

  /**
   * The Scheduler has allocated containers on this node to the given
   * application.
   * @param rmContainer Allocated container
   * @param launchedOnNode True if the container has been launched
   */
  protected synchronized void allocateContainer(RMContainer rmContainer,
      boolean launchedOnNode) {
    Container container = rmContainer.getContainer();
    if (rmContainer.getExecutionType() == ExecutionType.GUARANTEED) {
      deductUnallocatedResource(container.getResource());
      ++numContainers;
    }

    launchedContainers.put(container.getId(),
        new ContainerInfo(rmContainer, launchedOnNode));
  }

  /**
   * Get unallocated resources on the node.
   * @return Unallocated resources on the node
   */
  public synchronized Resource getUnallocatedResource() {
    return this.unallocatedResource;
  }

  /**
   * Get allocated resources on the node.
   * @return Allocated resources on the node
   */
  public synchronized Resource getAllocatedResource() {
    return this.allocatedResource;
  }

  /**
   * Get total resources on the node.
   * @return Total resources on the node.
   */
  public synchronized Resource getTotalResource() {
    return this.totalResource;
  }

  /**
   * Check if a container is launched by this node.
   * @return If the container is launched by the node.
   */
  public synchronized boolean isValidContainer(ContainerId containerId) {
    if (launchedContainers.containsKey(containerId)) {
      return true;
    }
    return false;
  }

  /**
   * Update the resources of the node when releasing a container.
   * @param container Container to release.
   */
  protected synchronized void updateResourceForReleasedContainer(
      Container container) {
    if (container.getExecutionType() == ExecutionType.GUARANTEED) {
      addUnallocatedResource(container.getResource());
      --numContainers;
    }
  }

  /**
   * Release an allocated container on this node.
   * @param containerId ID of container to be released.
   * @param releasedByNode whether the release originates from a node update.
   */
  public synchronized void releaseContainer(ContainerId containerId,
      boolean releasedByNode) {
    ContainerInfo info = launchedContainers.get(containerId);
    if (info == null) {
      return;
    }
    if (!releasedByNode && info.launchedOnNode) {
      // wait until node reports container has completed
      return;
    }

    launchedContainers.remove(containerId);
    Container container = info.container.getContainer();
    updateResourceForReleasedContainer(container);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Released container " + container.getId() + " of capacity "
              + container.getResource() + " on host " + rmNode.getNodeAddress()
              + ", which currently has " + numContainers + " containers, "
              + getAllocatedResource() + " used and " + getUnallocatedResource()
              + " available" + ", release resources=" + true);
    }
  }

  /**
   * Inform the node that a container has launched.
   * @param containerId ID of the launched container
   */
  public synchronized void containerStarted(ContainerId containerId) {
    ContainerInfo info = launchedContainers.get(containerId);
    if (info != null) {
      info.launchedOnNode = true;
    }
  }

  /**
   * Add unallocated resources to the node. This is used when unallocating a
   * container.
   * @param resource Resources to add.
   */
  private synchronized void addUnallocatedResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid resource addition of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.addTo(unallocatedResource, resource);
    Resources.subtractFrom(allocatedResource, resource);
  }

  /**
   * Deduct unallocated resources from the node. This is used when allocating a
   * container.
   * @param resource Resources to deduct.
   */
  @VisibleForTesting
  public synchronized void deductUnallocatedResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid deduction of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.subtractFrom(unallocatedResource, resource);
    Resources.addTo(allocatedResource, resource);
  }

  /**
   * Reserve container for the attempt on this node.
   * @param attempt Application attempt asking for the reservation.
   * @param schedulerKey Priority of the reservation.
   * @param container Container reserving resources for.
   */
  public abstract void reserveResource(SchedulerApplicationAttempt attempt,
      SchedulerRequestKey schedulerKey, RMContainer container);

  /**
   * Unreserve resources on this node.
   * @param attempt Application attempt that had done the reservation.
   */
  public abstract void unreserveResource(SchedulerApplicationAttempt attempt);

  @Override
  public String toString() {
    return "host: " + rmNode.getNodeAddress() + " #containers="
        + getNumContainers() + " available=" + getUnallocatedResource()
        + " used=" + getAllocatedResource();
  }

  /**
   * Get number of active containers on the node.
   * @return Number of active containers on the node.
   */
  public int getNumContainers() {
    return numContainers;
  }

  /**
   * Get the containers running on the node.
   * @return A copy of containers running on the node.
   */
  public synchronized List<RMContainer> getCopiedListOfRunningContainers() {
    List<RMContainer> result = new ArrayList<>(launchedContainers.size());
    for (ContainerInfo info : launchedContainers.values()) {
      result.add(info.container);
    }
    return result;
  }

  /**
   * Get the containers running on the node with AM containers at the end.
   * @return A copy of running containers with AM containers at the end.
   */
  public synchronized List<RMContainer> getRunningContainersWithAMsAtTheEnd() {
    LinkedList<RMContainer> result = new LinkedList<>();
    for (ContainerInfo info : launchedContainers.values()) {
      if(info.container.isAMContainer()) {
        result.addLast(info.container);
      } else {
        result.addFirst(info.container);
      }
    }
    return result;
  }

  /**
   * Get the container for the specified container ID.
   * @param containerId The container ID
   * @return The container for the specified container ID
   */
  protected synchronized RMContainer getContainer(ContainerId containerId) {
    RMContainer container = null;
    ContainerInfo info = launchedContainers.get(containerId);
    if (info != null) {
      container = info.container;
    }
    return container;
  }

  /**
   * Get the reserved container in the node.
   * @return Reserved container in the node.
   */
  public synchronized RMContainer getReservedContainer() {
    return reservedContainer;
  }

  /**
   * Set the reserved container in the node.
   * @param reservedContainer Reserved container in the node.
   */
  public synchronized void
  setReservedContainer(RMContainer reservedContainer) {
    this.reservedContainer = reservedContainer;
  }

  /**
   * Recover a container.
   * @param rmContainer Container to recover.
   */
  public synchronized void recoverContainer(RMContainer rmContainer) {
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }
    allocateContainer(rmContainer, true);
  }

  /**
   * Get the labels for the node.
   * @return Set of labels for the node.
   */
  public Set<String> getLabels() {
    return labels;
  }

  /**
   * Update the labels for the node.
   * @param labels Set of labels for the node.
   */
  public void updateLabels(Set<String> labels) {
    this.labels = labels;
  }

  /**
   * Get partition of which the node belongs to, if node-labels of this node is
   * empty or null, it belongs to NO_LABEL partition. And since we only support
   * one partition for each node (YARN-2694), first label will be its partition.
   * @return Partition for the node.
   */
  public String getPartition() {
    if (this.labels == null || this.labels.isEmpty()) {
      return RMNodeLabelsManager.NO_LABEL;
    } else {
      return this.labels.iterator().next();
    }
  }

  /**
   * Set the resource utilization of the containers in the node.
   * @param containersUtilization Resource utilization of the containers.
   */
  public void setAggregatedContainersUtilization(
      ResourceUtilization containersUtilization) {
    this.containersUtilization = containersUtilization;
  }

  /**
   * Get the resource utilization of the containers in the node.
   * @return Resource utilization of the containers.
   */
  public ResourceUtilization getAggregatedContainersUtilization() {
    return this.containersUtilization;
  }

  /**
   * Set the resource utilization of the node. This includes the containers.
   * @param nodeUtilization Resource utilization of the node.
   */
  public void setNodeUtilization(ResourceUtilization nodeUtilization) {
    this.nodeUtilization = nodeUtilization;
  }

  /**
   * Get the resource utilization of the node.
   * @return Resource utilization of the node.
   */
  public ResourceUtilization getNodeUtilization() {
    return this.nodeUtilization;
  }

  public long getLastHeartbeatMonotonicTime() {
    return lastHeartbeatMonotonicTime;
  }

  /**
   * This will be called for each node heartbeat.
   */
  public void notifyNodeUpdate() {
    this.lastHeartbeatMonotonicTime = Time.monotonicNow();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SchedulerNode)) {
      return false;
    }

    SchedulerNode that = (SchedulerNode) o;

    return getNodeID().equals(that.getNodeID());
  }

  @Override
  public int hashCode() {
    return getNodeID().hashCode();
  }

  private static class ContainerInfo {
    private final RMContainer container;
    private boolean launchedOnNode;

    public ContainerInfo(RMContainer container, boolean launchedOnNode) {
      this.container = container;
      this.launchedOnNode = launchedOnNode;
    }
  }
}
