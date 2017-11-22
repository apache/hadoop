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
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.api.records.OverAllocationInfo;
import org.apache.hadoop.yarn.server.api.records.ResourceThresholds;
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

  private Resource capacity;
  // The resource available within the node's capacity that can be given out
  // to run GUARANTEED containers, including reserved, preempted and any
  // remaining free resources. Resources allocated to OPPORTUNISTIC containers
  // are tracked in allocatedResourceOpportunistic
  private Resource unallocatedResource = Resource.newInstance(0, 0);

  private RMContainer reservedContainer;
  private volatile ResourceUtilization containersUtilization =
      ResourceUtilization.newInstance(0, 0, 0f);
  private volatile ResourceUtilization nodeUtilization =
      ResourceUtilization.newInstance(0, 0, 0f);

  private final Map<ContainerId, ContainerInfo>
      allocatedContainers = new HashMap<>();

  private volatile int numGuaranteedContainers = 0;
  private Resource allocatedResourceGuaranteed = Resource.newInstance(0, 0);

  private volatile int numOpportunisticContainers = 0;
  private Resource allocatedResourceOpportunistic = Resource.newInstance(0, 0);

  private final RMNode rmNode;
  private final String nodeName;
  private final RMContext rmContext;

  // The total amount of resources requested by containers that have been
  // allocated but not yet launched on the node.
  protected Resource resourceAllocatedPendingLaunch =
      Resource.newInstance(0, 0);

  // The max amount of resources that can be allocated to opportunistic
  // containers on the node, specified as a ratio to its capacity
  private final float maxOverAllocationRatio;

  private volatile Set<String> labels = null;

  private volatile Set<NodeAttribute> nodeAttributes = null;

  // Last updated time
  private volatile long lastHeartbeatMonotonicTime;

  public SchedulerNode(RMNode node, boolean usePortForNodeName,
      Set<String> labels, float maxOverAllocationRatio) {
    this.rmNode = node;
    this.rmContext = node.getRMContext();
    this.unallocatedResource = Resources.clone(node.getTotalCapability());
    this.capacity = Resources.clone(node.getTotalCapability());
    if (usePortForNodeName) {
      nodeName = rmNode.getHostName() + ":" + node.getNodeID().getPort();
    } else {
      nodeName = rmNode.getHostName();
    }
    this.labels = ImmutableSet.copyOf(labels);
    this.lastHeartbeatMonotonicTime = Time.monotonicNow();
    this.maxOverAllocationRatio = maxOverAllocationRatio;
  }

  public SchedulerNode(RMNode node, boolean usePortForNodeName,
      Set<String> labels) {
    this(node, usePortForNodeName, labels,
        YarnConfiguration.DEFAULT_PER_NODE_MAX_OVERALLOCATION_RATIO);
  }

  public SchedulerNode(RMNode node, boolean usePortForNodeName,
      float maxOverAllocationRatio) {
    this(node, usePortForNodeName, CommonNodeLabelsManager.EMPTY_STRING_SET,
        maxOverAllocationRatio);
  }

  public SchedulerNode(RMNode node, boolean usePortForNodeName) {
    this(node, usePortForNodeName, CommonNodeLabelsManager.EMPTY_STRING_SET,
        YarnConfiguration.DEFAULT_PER_NODE_MAX_OVERALLOCATION_RATIO);
  }

  public RMNode getRMNode() {
    return this.rmNode;
  }

  /**
   * Set total resources on the node.
   * @param resource Total resources on the node.
   */
  public synchronized void updateTotalResource(Resource resource){
    this.capacity = Resources.clone(resource);
    this.unallocatedResource = Resources.subtract(capacity,
        this.allocatedResourceGuaranteed);
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

    if (container.getExecutionType() == ExecutionType.GUARANTEED) {
      guaranteedContainerResourceAllocated(rmContainer, launchedOnNode);
    } else {
      opportunisticContainerResourceAllocated(rmContainer, launchedOnNode);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Assigned container " + container.getId() + " of capacity "
          + container.getResource() + " and type " +
          container.getExecutionType() + " on host " + toString());
    }
  }

  /**
   * Handle an allocation of a GUARANTEED container.
   * @param rmContainer the allocated GUARANTEED container
   * @param launchedOnNode true if the container has been launched
   */
  private void guaranteedContainerResourceAllocated(
      RMContainer rmContainer, boolean launchedOnNode) {
    Container container = rmContainer.getContainer();

    if (container.getExecutionType() != ExecutionType.GUARANTEED) {
      throw new YarnRuntimeException("Inapplicable ExecutionType: " +
          container.getExecutionType());
    }

    allocatedContainers.put(container.getId(),
        new ContainerInfo(rmContainer, launchedOnNode));

    Resource resource = container.getResource();
    if (containerResourceAllocated(resource, allocatedResourceGuaranteed)) {
      Resources.subtractFrom(unallocatedResource, resource);
    }

    numGuaranteedContainers++;
  }

  /**
   * Handle an allocation of a OPPORTUNISTIC container.
   * @param rmContainer the allocated OPPORTUNISTIC container
   * @param launchedOnNode true if the container has been launched
   */
  private void opportunisticContainerResourceAllocated(
      RMContainer rmContainer, boolean launchedOnNode) {
    Container container = rmContainer.getContainer();

    if (container.getExecutionType() != ExecutionType.OPPORTUNISTIC) {
      throw new YarnRuntimeException("Inapplicable ExecutionType: " +
          container.getExecutionType());
    }

    allocatedContainers.put(rmContainer.getContainerId(),
        new ContainerInfo(rmContainer, launchedOnNode));
    if (containerResourceAllocated(
        container.getResource(), allocatedResourceOpportunistic)) {
      // nothing to do here
    }
    numOpportunisticContainers++;
  }

  private boolean containerResourceAllocated(Resource allocated,
      Resource aggregatedResources) {
    if (allocated == null) {
      LOG.error("Invalid deduction of null resource for "
          + rmNode.getNodeAddress());
      return false;
    }
    Resources.addTo(resourceAllocatedPendingLaunch, allocated);
    Resources.addTo(aggregatedResources, allocated);
    return true;
  }


  /**
   * Get resources that are not allocated to GUARANTEED containers on the node.
   * @return Unallocated resources on the node
   */
  public synchronized Resource getUnallocatedResource() {
    return this.unallocatedResource;
  }

  /**
   * Get resources allocated to GUARANTEED containers on the node.
   * @return Allocated resources to GUARANTEED containers on the node
   */
  public synchronized Resource getAllocatedResource() {
    return this.allocatedResourceGuaranteed;
  }

  /**
   * Get resources allocated to OPPORTUNISTIC containers on the node.
   * @return Allocated resources to OPPORTUNISTIC containers on the node
   */
  public synchronized Resource getOpportunisticResourceAllocated() {
    return this.allocatedResourceOpportunistic;
  }

  @VisibleForTesting
  public synchronized Resource getResourceAllocatedPendingLaunch() {
    return this.resourceAllocatedPendingLaunch;
  }

  /**
   * Get total resources on the node.
   * @return Total resources on the node.
   */
  public synchronized Resource getCapacity() {
    return this.capacity;
  }

  /**
   * Check if a GUARANTEED container is launched by this node.
   * @return If the container is launched by the node.
   */
  @VisibleForTesting
  public synchronized boolean isValidGuaranteedContainer(
      ContainerId containerId) {
    ContainerInfo containerInfo = allocatedContainers.get(containerId);
    return containerInfo != null && ExecutionType.GUARANTEED ==
        containerInfo.container.getExecutionType();
  }

  /**
   * Check if an OPPORTUNISTIC container is launched by this node.
   * @param containerId id of the container to check
   * @return If the container is launched by the node.
   */
  @VisibleForTesting
  public synchronized boolean isValidOpportunisticContainer(
      ContainerId containerId)  {
    ContainerInfo containerInfo = allocatedContainers.get(containerId);
    return containerInfo != null && ExecutionType.OPPORTUNISTIC ==
        containerInfo.container.getExecutionType();
  }

  /**
   * Release an allocated container on this node.
   * @param containerId ID of container to be released.
   * @param releasedByNode whether the release originates from a node update.
   */
  public synchronized void releaseContainer(ContainerId containerId,
      boolean releasedByNode) {
    RMContainer rmContainer = getContainer(containerId);
    if (rmContainer == null) {
      LOG.warn("Invalid container " + containerId + " is released.");
      return;
    }

    if (!allocatedContainers.containsKey(containerId)) {
      // do not process if the container is never allocated on the node
      return;
    }

    if (!releasedByNode &&
        allocatedContainers.get(containerId).launchedOnNode) {
      // only process if the container has not been launched on a node
      // yet or it is released by node.
      return;
    }

    Container container = rmContainer.getContainer();
    if (container.getExecutionType() == ExecutionType.GUARANTEED) {
      guaranteedContainerReleased(container);
    } else {
      opportunisticContainerReleased(container);
    }

    // We remove allocation tags when a container is actually
    // released on NM. This is to avoid running into situation
    // when AM releases a container and NM has some delay to
    // actually release it, then the tag can still be visible
    // at RM so that RM can respect it during scheduling new containers.
    if (rmContext != null && rmContext.getAllocationTagsManager() != null) {
      rmContext.getAllocationTagsManager()
          .removeContainer(container.getNodeId(),
              container.getId(), container.getAllocationTags());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Released container " + container.getId() + " of capacity "
          + container.getResource() + " on host " + rmNode.getNodeAddress()
          + ", with " + numGuaranteedContainers
          + " guaranteed containers taking"
          + getAllocatedResource() + " and " + numOpportunisticContainers
          + " opportunistic containers taking "
          + getOpportunisticResourceAllocated()
          + " and " + getUnallocatedResource() + " (guaranteed) available"
          + ", release resources=" + true);
    }
  }

  /**
   * Inform the node that a container has launched.
   * @param containerId ID of the launched container
   */
  public synchronized void containerLaunched(ContainerId containerId) {
    ContainerInfo info = allocatedContainers.get(containerId);
    if (info != null && !info.launchedOnNode) {
      info.launchedOnNode = true;
      Resources.subtractFrom(resourceAllocatedPendingLaunch,
          info.container.getContainer().getResource());
    }
  }

  /**
   * Handle the release of a GUARANTEED container.
   * @param container Container to release.
   */
  protected synchronized void guaranteedContainerReleased(
      Container container) {
    if (container.getExecutionType() != ExecutionType.GUARANTEED) {
      throw new YarnRuntimeException("Inapplicable ExecutionType: " +
          container.getExecutionType());
    }

    if (containerResourceReleased(container, allocatedResourceGuaranteed)) {
      Resources.addTo(unallocatedResource, container.getResource());
    }
    // do not update allocated containers until the resources of
    // the container are released because we need to check if we
    // need to update resourceAllocatedPendingLaunch in case the
    // container has not been launched on the node.
    allocatedContainers.remove(container.getId());
    numGuaranteedContainers--;
  }

  /**
   * Handle the release of an OPPORTUNISTIC container.
   * @param container Container to release.
   */
  private void opportunisticContainerReleased(
      Container container) {
    if (container.getExecutionType() != ExecutionType.OPPORTUNISTIC) {
      throw new YarnRuntimeException("Inapplicable ExecutionType: " +
          container.getExecutionType());
    }

    if (containerResourceReleased(container, allocatedResourceOpportunistic)) {
      // nothing to do here
    }
    // do not update allocated containers until the resources of
    // the container are released because we need to check if we
    // need to update resourceAllocatedPendingLaunch in case the
    // container has not been launched on the node.
    allocatedContainers.remove(container.getId());
    numOpportunisticContainers--;
  }

  private boolean containerResourceReleased(Container container,
      Resource aggregatedResource) {
    Resource released = container.getResource();
    if (released == null) {
      LOG.error("Invalid resource addition of null resource for "
          + rmNode.getNodeAddress());
      return false;
    }
    Resources.subtractFrom(aggregatedResource, released);

    if (!allocatedContainers.get(container.getId()).launchedOnNode) {
      // update resourceAllocatedPendingLaunch if the container is has
      // not yet been launched on the node
      Resources.subtractFrom(resourceAllocatedPendingLaunch, released);
    }
    return true;
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
    return "host: " + rmNode.getNodeAddress() + " #guaranteed containers=" +
        getNumGuaranteedContainers() + " #opportunistic containers="  +
        getNumOpportunisticContainers() + " available=" +
        getUnallocatedResource() + " used by guaranteed containers=" +
        allocatedResourceGuaranteed + " used by opportunistic containers=" +
        allocatedResourceOpportunistic;
  }

  /**
   * Get number of active GUARANTEED containers on the node.
   * @return Number of active OPPORTUNISTIC containers on the node.
   */
  public int getNumGuaranteedContainers() {
    return numGuaranteedContainers;
  }

  /**
   * Get number of active OPPORTUNISTIC containers on the node.
   * @return Number of active OPPORTUNISTIC containers on the node.
   */
  public int getNumOpportunisticContainers() {
    return numOpportunisticContainers;
  }

  /**
   * Get the containers running on the node.
   * @return A copy of containers running on the node.
   */
  public synchronized List<RMContainer> getCopiedListOfRunningContainers() {
    List<RMContainer> result = new ArrayList<>(allocatedContainers.size());
    for (ContainerInfo info : allocatedContainers.values()) {
      result.add(info.container);
    }
    return result;
  }

  /**
   * Get the containers running on the node with AM containers at the end.
   * @return A copy of running containers with AM containers at the end.
   */
  public synchronized List<RMContainer>
      getRunningGuaranteedContainersWithAMsAtTheEnd() {
    LinkedList<RMContainer> result = new LinkedList<>();
    for (ContainerInfo info : allocatedContainers.values()) {
      if(info.container.isAMContainer()) {
        result.addLast(info.container);
      } else if (info.container.getExecutionType() ==
          ExecutionType.GUARANTEED){
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
    ContainerInfo info = allocatedContainers.get(containerId);

    return info != null ? info.container : null;
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

  public Set<NodeAttribute> getNodeAttributes() {
    return nodeAttributes;
  }

  public void updateNodeAttributes(Set<NodeAttribute> attributes) {
    this.nodeAttributes = attributes;
  }

  /**
   * Get the amount of resources that can be allocated to opportunistic
   * containers in the case of overallocation, calculated as
   * node capacity - (node utilization + resources of allocated-yet-not-started
   * containers), subject to the maximum amount of resources that can be
   * allocated to opportunistic containers on the node specified as a ratio to
   * its capacity.
   * @return the amount of resources that are available to be allocated to
   *         opportunistic containers
   */
  public synchronized Resource allowedResourceForOverAllocation() {
    OverAllocationInfo overAllocationInfo = rmNode.getOverAllocationInfo();
    if (overAllocationInfo == null) {
      LOG.debug("Overallocation is disabled on node: " + rmNode.getHostName());
      return Resources.none();
    }

    ResourceUtilization projectedNodeUtilization = ResourceUtilization.
        newInstance(getNodeUtilization());
    // account for resources allocated in this heartbeat
    projectedNodeUtilization.addTo(
        (int) (resourceAllocatedPendingLaunch.getMemorySize()), 0,
        (float) resourceAllocatedPendingLaunch.getVirtualCores() /
            capacity.getVirtualCores());

    ResourceThresholds thresholds =
        overAllocationInfo.getOverAllocationThresholds();
    Resource overAllocationThreshold = Resources.createResource(
        (long) (capacity.getMemorySize() * thresholds.getMemoryThreshold()),
        (int) (capacity.getVirtualCores() * thresholds.getCpuThreshold()));
    long allowedMemory = Math.max(0, overAllocationThreshold.getMemorySize()
        - projectedNodeUtilization.getPhysicalMemory());
    int allowedCpu = Math.max(0, (int)
        (overAllocationThreshold.getVirtualCores() -
            projectedNodeUtilization.getCPU() * capacity.getVirtualCores()));

    Resource resourceAllowedForOpportunisticContainers =
        Resources.createResource(allowedMemory, allowedCpu);

    // cap the total amount of resources allocated to OPPORTUNISTIC containers
    Resource maxOverallocation = getMaxOverallocationAllowed();
    Resources.subtractFrom(maxOverallocation, allocatedResourceOpportunistic);
    resourceAllowedForOpportunisticContainers = Resources.componentwiseMin(
        maxOverallocation, resourceAllowedForOpportunisticContainers);

    return resourceAllowedForOpportunisticContainers;
  }

  private Resource getMaxOverallocationAllowed() {
    long maxMemory = (long) (capacity.getMemorySize() * maxOverAllocationRatio);
    int maxVcore = (int) (capacity.getVirtualCores() * maxOverAllocationRatio);
    return Resource.newInstance(maxMemory, maxVcore);
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
