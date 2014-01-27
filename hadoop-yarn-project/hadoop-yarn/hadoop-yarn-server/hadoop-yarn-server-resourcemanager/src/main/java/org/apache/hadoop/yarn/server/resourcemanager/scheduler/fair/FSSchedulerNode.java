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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
@Unstable
public class FSSchedulerNode extends SchedulerNode {

  private static final Log LOG = LogFactory.getLog(FSSchedulerNode.class);

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private Resource availableResource;
  private Resource usedResource = recordFactory.newRecordInstance(Resource.class);
  private Resource totalResourceCapability;

  private volatile int numContainers;

  private RMContainer reservedContainer;
  private AppSchedulable reservedAppSchedulable;
  
  /* set of containers that are allocated containers */
  private final Map<ContainerId, RMContainer> launchedContainers = 
    new HashMap<ContainerId, RMContainer>();
  
  private final RMNode rmNode;
  private final String nodeName;

  public FSSchedulerNode(RMNode node, boolean usePortForNodeName) {
    this.rmNode = node;
    this.availableResource = Resources.clone(node.getTotalCapability());
    totalResourceCapability =
        Resource.newInstance(node.getTotalCapability().getMemory(), node
            .getTotalCapability().getVirtualCores());
    if (usePortForNodeName) {
      nodeName = rmNode.getHostName() + ":" + node.getNodeID().getPort();
    } else {
      nodeName = rmNode.getHostName();
    }
  }

  public RMNode getRMNode() {
    return rmNode;
  }

  public NodeId getNodeID() {
    return rmNode.getNodeID();
  }

  public String getHttpAddress() {
    return rmNode.getHttpAddress();
  }

  @Override
  public String getNodeName() {
    return nodeName;
  }

  @Override
  public String getRackName() {
    return rmNode.getRackName();
  }

  /**
   * The Scheduler has allocated containers on this node to the 
   * given application.
   * 
   * @param applicationId application
   * @param rmContainer allocated container
   */
  public synchronized void allocateContainer(ApplicationId applicationId, 
      RMContainer rmContainer) {
    Container container = rmContainer.getContainer();
    deductAvailableResource(container.getResource());
    ++numContainers;
    
    launchedContainers.put(container.getId(), rmContainer);

    LOG.info("Assigned container " + container.getId() + 
        " of capacity " + container.getResource() + " on host " + rmNode.getNodeAddress() + 
        ", which currently has " + numContainers + " containers, " + 
        getUsedResource() + " used and " + 
        getAvailableResource() + " available");
  }

  @Override
  public synchronized Resource getAvailableResource() {
    return availableResource;
  }

  @Override
  public synchronized Resource getUsedResource() {
    return usedResource;
  }

  private synchronized boolean isValidContainer(Container c) {    
    if (launchedContainers.containsKey(c.getId())) {
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
   * @param container container to be released
   */
  public synchronized void releaseContainer(Container container) {
    if (!isValidContainer(container)) {
      LOG.error("Invalid container released " + container);
      return;
    }

    /* remove the containers from the nodemanger */
    launchedContainers.remove(container.getId());
    updateResource(container);

    LOG.info("Released container " + container.getId() + 
        " of capacity " + container.getResource() + " on host " + rmNode.getNodeAddress() + 
        ", which currently has " + numContainers + " containers, " + 
        getUsedResource() + " used and " + getAvailableResource()
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

  @Override
  public Resource getTotalResource() {
    return this.totalResourceCapability;
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

  @Override
  public String toString() {
    return "host: " + rmNode.getNodeAddress() + " #containers=" + getNumContainers() +  
      " available=" + getAvailableResource() + 
      " used=" + getUsedResource();
  }

  @Override
  public int getNumContainers() {
    return numContainers;
  }

  public synchronized List<RMContainer> getRunningContainers() {
    return new ArrayList<RMContainer>(launchedContainers.values());
  }

  public synchronized void reserveResource(
      FSSchedulerApp application, Priority priority, 
      RMContainer reservedContainer) {
    // Check if it's already reserved
    if (this.reservedContainer != null) {
      // Sanity check
      if (!reservedContainer.getContainer().getNodeId().equals(getNodeID())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + reservedContainer +
            " on node " + reservedContainer.getReservedNode() + 
            " when currently" + " reserved resource " + this.reservedContainer +
            " on node " + this.reservedContainer.getReservedNode());
      }
      
      // Cannot reserve more than one application on a given node!
      if (!this.reservedContainer.getContainer().getId().getApplicationAttemptId().equals(
          reservedContainer.getContainer().getId().getApplicationAttemptId())) {
        throw new IllegalStateException("Trying to reserve" +
        		" container " + reservedContainer + 
            " for application " + application.getApplicationId() + 
            " when currently" +
            " reserved container " + this.reservedContainer +
            " on node " + this);
      }

      LOG.info("Updated reserved container " + 
          reservedContainer.getContainer().getId() + " on node " + 
          this + " for application " + application);
    } else {
      LOG.info("Reserved container " + reservedContainer.getContainer().getId() + 
          " on node " + this + " for application " + application);
    }
    this.reservedContainer = reservedContainer;
    this.reservedAppSchedulable = application.getAppSchedulable();
  }

  public synchronized void unreserveResource(
      FSSchedulerApp application) {
    // Cannot unreserve for wrong application...
    ApplicationAttemptId reservedApplication = 
        reservedContainer.getContainer().getId().getApplicationAttemptId(); 
    if (!reservedApplication.equals(
        application.getApplicationAttemptId())) {
      throw new IllegalStateException("Trying to unreserve " +  
          " for application " + application.getApplicationId() + 
          " when currently reserved " + 
          " for application " + reservedApplication.getApplicationId() + 
          " on node " + this);
    }
    
    this.reservedContainer = null;
    this.reservedAppSchedulable = null;
  }

  public synchronized RMContainer getReservedContainer() {
    return reservedContainer;
  }

  public synchronized AppSchedulable getReservedAppSchedulable() {
    return reservedAppSchedulable;
  }
  
  @Override
  public synchronized void applyDeltaOnAvailableResource(Resource deltaResource) {
    // we can only adjust available resource if total resource is changed.
    Resources.addTo(this.availableResource, deltaResource);
  }
  
}
