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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
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
  protected final Map<ContainerId, RMContainer> launchedContainers =
      new HashMap<>();

  private final RMNode rmNode;
  private final String nodeName;

  private volatile Set<String> labels = null;

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
   * The main usecase of this is Yarn minicluster to be able to differentiate
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
  public synchronized void allocateContainer(RMContainer rmContainer) {
    Container container = rmContainer.getContainer();
    deductUnallocatedResource(container.getResource());
    ++numContainers;

    launchedContainers.put(container.getId(), rmContainer);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Assigned container " + container.getId() + " of capacity "
              + container.getResource() + " on host " + rmNode.getNodeAddress()
              + ", which has " + numContainers + " containers, "
              + getAllocatedResource() + " used and " + getUnallocatedResource()
              + " available after allocation");
    }
  }

  /**
   * Change the resources allocated for a container.
   * @param containerId Identifier of the container to change.
   * @param deltaResource Change in the resource allocation.
   * @param increase True if the change is an increase of allocation.
   */
  protected synchronized void changeContainerResource(ContainerId containerId,
      Resource deltaResource, boolean increase) {
    if (increase) {
      deductUnallocatedResource(deltaResource);
    } else {
      addUnallocatedResource(deltaResource);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug((increase ? "Increased" : "Decreased") + " container "
              + containerId + " of capacity " + deltaResource + " on host "
              + rmNode.getNodeAddress() + ", which has " + numContainers
              + " containers, " + getAllocatedResource() + " used and "
              + getUnallocatedResource() + " available after allocation");
    }
  }

  /**
   * Increase the resources allocated to a container.
   * @param containerId Identifier of the container to change.
   * @param deltaResource Increase of resource allocation.
   */
  public synchronized void increaseContainer(ContainerId containerId,
      Resource deltaResource) {
    changeContainerResource(containerId, deltaResource, true);
  }

  /**
   * Decrease the resources allocated to a container.
   * @param containerId Identifier of the container to change.
   * @param deltaResource Decrease of resource allocation.
   */
  public synchronized void decreaseContainer(ContainerId containerId,
      Resource deltaResource) {
    changeContainerResource(containerId, deltaResource, false);
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
    addUnallocatedResource(container.getResource());
    --numContainers;
  }

  /**
   * Release an allocated container on this node.
   * @param container Container to be released.
   */
  public synchronized void releaseContainer(Container container) {
    if (!isValidContainer(container.getId())) {
      LOG.error("Invalid container released " + container);
      return;
    }

    // Remove the containers from the nodemanger
    if (null != launchedContainers.remove(container.getId())) {
      updateResourceForReleasedContainer(container);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Released container " + container.getId() + " of capacity "
              + container.getResource() + " on host " + rmNode.getNodeAddress()
              + ", which currently has " + numContainers + " containers, "
              + getAllocatedResource() + " used and " + getUnallocatedResource()
              + " available" + ", release resources=" + true);
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
  private synchronized void deductUnallocatedResource(Resource resource) {
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
   * @param priority Priority of the reservation.
   * @param container Container reserving resources for.
   */
  public abstract void reserveResource(SchedulerApplicationAttempt attempt,
      Priority priority, RMContainer container);

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
   * Get the running containers in the node.
   * @return List of running containers in the node.
   */
  public synchronized List<RMContainer> getCopiedListOfRunningContainers() {
    return new ArrayList<RMContainer>(launchedContainers.values());
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
  protected synchronized void
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
    allocateContainer(rmContainer);
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
}