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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.resource.Resources;


/**
 * Represents a YARN Cluster Node from the viewpoint of the scheduler.
 */
@Private
@Unstable
public abstract class SchedulerNode {

  private static final Log LOG = LogFactory.getLog(SchedulerNode.class);

  private Resource availableResource = Resource.newInstance(0, 0);
  private Resource usedResource = Resource.newInstance(0, 0);
  private Resource totalResourceCapability;
  private RMContainer reservedContainer;
  private volatile int numContainers;


  /* set of containers that are allocated containers */
  private final Map<ContainerId, RMContainer> launchedContainers =
      new HashMap<ContainerId, RMContainer>();

  private final RMNode rmNode;
  private final String nodeName;

  public SchedulerNode(RMNode node, boolean usePortForNodeName) {
    this.rmNode = node;
    this.availableResource = Resources.clone(node.getTotalCapability());
    this.totalResourceCapability = Resources.clone(node.getTotalCapability());
    if (usePortForNodeName) {
      nodeName = rmNode.getHostName() + ":" + node.getNodeID().getPort();
    } else {
      nodeName = rmNode.getHostName();
    }
  }

  public RMNode getRMNode() {
    return this.rmNode;
  }

  /**
   * Set total resources on the node.
   * @param resource total resources on the node.
   */
  public synchronized void setTotalResource(Resource resource){
    this.totalResourceCapability = resource;
    this.availableResource = Resources.subtract(totalResourceCapability,
      this.usedResource);
  }
  
  /**
   * Get the ID of the node which contains both its hostname and port.
   * 
   * @return the ID of the node
   */
  public NodeId getNodeID() {
    return this.rmNode.getNodeID();
  }

  public String getHttpAddress() {
    return this.rmNode.getHttpAddress();
  }

  /**
   * Get the name of the node for scheduling matching decisions.
   * <p/>
   * Typically this is the 'hostname' reported by the node, but it could be
   * configured to be 'hostname:port' reported by the node via the
   * {@link YarnConfiguration#RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME} constant.
   * The main usecase of this is Yarn minicluster to be able to differentiate
   * node manager instances by their port number.
   * 
   * @return name of the node for scheduling matching decisions.
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Get rackname.
   * 
   * @return rackname
   */
  public String getRackName() {
    return this.rmNode.getRackName();
  }

  /**
   * The Scheduler has allocated containers on this node to the given
   * application.
   * 
   * @param rmContainer
   *          allocated container
   */
  public synchronized void allocateContainer(RMContainer rmContainer) {
    Container container = rmContainer.getContainer();
    deductAvailableResource(container.getResource());
    ++numContainers;

    launchedContainers.put(container.getId(), rmContainer);

    LOG.info("Assigned container " + container.getId() + " of capacity "
        + container.getResource() + " on host " + rmNode.getNodeAddress()
        + ", which has " + numContainers + " containers, "
        + getUsedResource() + " used and " + getAvailableResource()
        + " available after allocation");
  }

  /**
   * Get available resources on the node.
   * 
   * @return available resources on the node
   */
  public synchronized Resource getAvailableResource() {
    return this.availableResource;
  }

  /**
   * Get used resources on the node.
   * 
   * @return used resources on the node
   */
  public synchronized Resource getUsedResource() {
    return this.usedResource;
  }

  /**
   * Get total resources on the node.
   * 
   * @return total resources on the node.
   */
  public synchronized Resource getTotalResource() {
    return this.totalResourceCapability;
  }

  public synchronized boolean isValidContainer(ContainerId containerId) {
    if (launchedContainers.containsKey(containerId)) {
      return true;
    }
    return false;
  }

  private synchronized void updateResource(Container container) {
    addAvailableResource(container.getResource());
    --numContainers;
  }

  /**
   * Release an allocated container on this node.
   * 
   * @param container
   *          container to be released
   */
  public synchronized void releaseContainer(Container container) {
    if (!isValidContainer(container.getId())) {
      LOG.error("Invalid container released " + container);
      return;
    }

    /* remove the containers from the nodemanger */
    if (null != launchedContainers.remove(container.getId())) {
      updateResource(container);
    }

    LOG.info("Released container " + container.getId() + " of capacity "
        + container.getResource() + " on host " + rmNode.getNodeAddress()
        + ", which currently has " + numContainers + " containers, "
        + getUsedResource() + " used and " + getAvailableResource()
        + " available" + ", release resources=" + true);
  }

  private synchronized void addAvailableResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid resource addition of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.addTo(availableResource, resource);
    Resources.subtractFrom(usedResource, resource);
  }

  private synchronized void deductAvailableResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid deduction of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.subtractFrom(availableResource, resource);
    Resources.addTo(usedResource, resource);
  }

  /**
   * Reserve container for the attempt on this node.
   */
  public abstract void reserveResource(SchedulerApplicationAttempt attempt,
      Priority priority, RMContainer container);

  /**
   * Unreserve resources on this node.
   */
  public abstract void unreserveResource(SchedulerApplicationAttempt attempt);

  @Override
  public String toString() {
    return "host: " + rmNode.getNodeAddress() + " #containers="
        + getNumContainers() + " available="
        + getAvailableResource().getMemory() + " used="
        + getUsedResource().getMemory();
  }

  /**
   * Get number of active containers on the node.
   * 
   * @return number of active containers on the node
   */
  public int getNumContainers() {
    return numContainers;
  }

  public synchronized List<RMContainer> getRunningContainers() {
    return new ArrayList<RMContainer>(launchedContainers.values());
  }

  public synchronized RMContainer getReservedContainer() {
    return reservedContainer;
  }

  protected synchronized void
      setReservedContainer(RMContainer reservedContainer) {
    this.reservedContainer = reservedContainer;
  }

  public synchronized void recoverContainer(RMContainer rmContainer) {
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }
    allocateContainer(rmContainer);
  }
}
