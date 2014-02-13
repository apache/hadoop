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

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
@Unstable
public class AppSchedulable extends Schedulable {
  private static final DefaultResourceCalculator RESOURCE_CALCULATOR
    = new DefaultResourceCalculator();
  
  private FairScheduler scheduler;
  private FSSchedulerApp app;
  private Resource demand = Resources.createResource(0);
  private long startTime;
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private static final Log LOG = LogFactory.getLog(AppSchedulable.class);
  private FSLeafQueue queue;
  private RMContainerTokenSecretManager containerTokenSecretManager;

  public AppSchedulable(FairScheduler scheduler, FSSchedulerApp app, FSLeafQueue queue) {
    this.scheduler = scheduler;
    this.app = app;
    this.startTime = scheduler.getClock().getTime();
    this.queue = queue;
    this.containerTokenSecretManager = scheduler.
    		getContainerTokenSecretManager();
  }

  @Override
  public String getName() {
    return app.getApplicationId().toString();
  }

  public FSSchedulerApp getApp() {
    return app;
  }

  @Override
  public void updateDemand() {
    demand = Resources.createResource(0);
    // Demand is current consumption plus outstanding requests
    Resources.addTo(demand, app.getCurrentConsumption());

    // Add up outstanding resource requests
    synchronized (app) {
      for (Priority p : app.getPriorities()) {
        for (ResourceRequest r : app.getResourceRequests(p).values()) {
          Resource total = Resources.multiply(r.getCapability(), r.getNumContainers());
          Resources.addTo(demand, total);
        }
      }
    }
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public Resource getResourceUsage() {
    return app.getCurrentConsumption();
  }


  @Override
  public Resource getMinShare() {
    return Resources.none();
  }
  
  @Override
  public Resource getMaxShare() {
    return Resources.unbounded();
  }

  /**
   * Get metrics reference from containing queue.
   */
  public QueueMetrics getMetrics() {
    return queue.getMetrics();
  }

  @Override
  public ResourceWeights getWeights() {
    return scheduler.getAppWeight(this);
  }

  @Override
  public Priority getPriority() {
    // Right now per-app priorities are not passed to scheduler,
    // so everyone has the same priority.
    Priority p = recordFactory.newRecordInstance(Priority.class);
    p.setPriority(1);
    return p;
  }

  /**
   * Create and return a container object reflecting an allocation for the
   * given appliction on the given node with the given capability and
   * priority.
   */
  public Container createContainer(
      FSSchedulerApp application, FSSchedulerNode node,
      Resource capability, Priority priority) {

    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId = BuilderUtils.newContainerId(application
        .getApplicationAttemptId(), application.getNewContainerId());

    // Create the container
    Container container =
        BuilderUtils.newContainer(containerId, nodeId, node.getRMNode()
          .getHttpAddress(), capability, priority, null);

    return container;
  }

  /**
   * Reserve a spot for {@code container} on this {@code node}. If
   * the container is {@code alreadyReserved} on the node, simply
   * update relevant bookeeping. This dispatches ro relevant handlers
   * in the {@link FSSchedulerNode} and {@link SchedulerApp} classes.
   */
  private void reserve(Priority priority, FSSchedulerNode node,
      Container container, boolean alreadyReserved) {
    LOG.info("Making reservation: node=" + node.getNodeName() +
                                 " app_id=" + app.getApplicationId());
    if (!alreadyReserved) {
      getMetrics().reserveResource(app.getUser(), container.getResource());
      RMContainer rmContainer = app.reserve(node, priority, null,
          container);
      node.reserveResource(app, priority, rmContainer);
    }

    else {
      RMContainer rmContainer = node.getReservedContainer();
      app.reserve(node, priority, rmContainer, container);
      node.reserveResource(app, priority, rmContainer);
    }
  }

  /**
   * Remove the reservation on {@code node} at the given
   * {@link Priority}. This dispatches to the SchedulerApp and SchedulerNode
   * handlers for an unreservation.
   */
  public void unreserve(Priority priority, FSSchedulerNode node) {
    RMContainer rmContainer = node.getReservedContainer();
    app.unreserve(node, priority);
    node.unreserveResource(app);
    getMetrics().unreserveResource(
        app.getUser(), rmContainer.getContainer().getResource());
  }

  /**
   * Assign a container to this node to facilitate {@code request}. If node does
   * not have enough memory, create a reservation. This is called once we are
   * sure the particular request should be facilitated by this node.
   */
  private Resource assignContainer(FSSchedulerNode node,
      Priority priority, ResourceRequest request, NodeType type,
      boolean reserved) {

    // How much does this request need?
    Resource capability = request.getCapability();

    // How much does the node have?
    Resource available = node.getAvailableResource();

    Container container = null;
    if (reserved) {
      container = node.getReservedContainer().getContainer();
    } else {
      container = createContainer(app, node, capability, priority);
    }

    // Can we allocate a container on this node?
    if (Resources.fitsIn(capability, available)) {
      // Inform the application of the new container for this request
      RMContainer allocatedContainer =
          app.allocate(type, node, priority, request, container);
      if (allocatedContainer == null) {
        // Did the application need this resource?
        if (reserved) {
          unreserve(priority, node);
        }
        return Resources.none();
      }

      // If we had previously made a reservation, delete it
      if (reserved) {
        unreserve(priority, node);
      }

      // Inform the node
      node.allocateContainer(app.getApplicationId(),
          allocatedContainer);

      return container.getResource();
    } else {
      // The desired container won't fit here, so reserve
      reserve(priority, node, container, reserved);

      return FairScheduler.CONTAINER_RESERVED;
    }
  }

  private Resource assignContainer(FSSchedulerNode node, boolean reserved) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node offered to app: " + getName() + " reserved: " + reserved);
    }

    if (reserved) {
      RMContainer rmContainer = node.getReservedContainer();
      Priority priority = rmContainer.getReservedPriority();

      // Make sure the application still needs requests at this priority
      if (app.getTotalRequiredResources(priority) == 0) {
        unreserve(priority, node);
        return Resources.none();
      }
    }

    Collection<Priority> prioritiesToTry = (reserved) ? 
        Arrays.asList(node.getReservedContainer().getReservedPriority()) : 
        app.getPriorities();
    
    // For each priority, see if we can schedule a node local, rack local
    // or off-switch request. Rack of off-switch requests may be delayed
    // (not scheduled) in order to promote better locality.
    synchronized (app) {
      for (Priority priority : prioritiesToTry) {
        if (app.getTotalRequiredResources(priority) <= 0 ||
            !hasContainerForNode(priority, node)) {
          continue;
        }
        
        app.addSchedulingOpportunity(priority);

        ResourceRequest rackLocalRequest = app.getResourceRequest(priority,
            node.getRackName());
        ResourceRequest localRequest = app.getResourceRequest(priority,
            node.getNodeName());
        
        if (localRequest != null && !localRequest.getRelaxLocality()) {
          LOG.warn("Relax locality off is not supported on local request: "
              + localRequest);
        }
        
        NodeType allowedLocality;
        if (scheduler.isContinuousSchedulingEnabled()) {
          allowedLocality = app.getAllowedLocalityLevelByTime(priority,
                  scheduler.getNodeLocalityDelayMs(),
                  scheduler.getRackLocalityDelayMs(),
                  scheduler.getClock().getTime());
        } else {
          allowedLocality = app.getAllowedLocalityLevel(priority,
                  scheduler.getNumClusterNodes(),
                  scheduler.getNodeLocalityThreshold(),
                  scheduler.getRackLocalityThreshold());
        }

        if (rackLocalRequest != null && rackLocalRequest.getNumContainers() != 0
            && localRequest != null && localRequest.getNumContainers() != 0) {
          return assignContainer(node, priority,
              localRequest, NodeType.NODE_LOCAL, reserved);
        }
        
        if (rackLocalRequest != null && !rackLocalRequest.getRelaxLocality()) {
          continue;
        }

        if (rackLocalRequest != null && rackLocalRequest.getNumContainers() != 0
            && (allowedLocality.equals(NodeType.RACK_LOCAL) ||
                allowedLocality.equals(NodeType.OFF_SWITCH))) {
          return assignContainer(node, priority, rackLocalRequest,
              NodeType.RACK_LOCAL, reserved);
        }

        ResourceRequest offSwitchRequest = app.getResourceRequest(priority,
            ResourceRequest.ANY);
        if (offSwitchRequest != null && !offSwitchRequest.getRelaxLocality()) {
          continue;
        }
        
        if (offSwitchRequest != null && offSwitchRequest.getNumContainers() != 0
            && allowedLocality.equals(NodeType.OFF_SWITCH)) {
          return assignContainer(node, priority, offSwitchRequest,
              NodeType.OFF_SWITCH, reserved);
        }
      }
    }
    return Resources.none();
  }

  public Resource assignReservedContainer(FSSchedulerNode node) {
    return assignContainer(node, true);
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    return assignContainer(node, false);
  }
  
  /**
   * Whether this app has containers requests that could be satisfied on the
   * given node, if the node had full space.
   */
  public boolean hasContainerForNode(Priority prio, FSSchedulerNode node) {
    ResourceRequest anyRequest = app.getResourceRequest(prio, ResourceRequest.ANY);
    ResourceRequest rackRequest = app.getResourceRequest(prio, node.getRackName());
    ResourceRequest nodeRequest = app.getResourceRequest(prio, node.getNodeName());

    return
        // There must be outstanding requests at the given priority:
        anyRequest != null && anyRequest.getNumContainers() > 0 &&
        // If locality relaxation is turned off at *-level, there must be a
        // non-zero request for the node's rack:
        (anyRequest.getRelaxLocality() ||
            (rackRequest != null && rackRequest.getNumContainers() > 0)) &&
        // If locality relaxation is turned off at rack-level, there must be a
        // non-zero request at the node:
        (rackRequest == null || rackRequest.getRelaxLocality() ||
            (nodeRequest != null && nodeRequest.getNumContainers() > 0)) &&
        // The requested container must be able to fit on the node:
        Resources.lessThanOrEqual(RESOURCE_CALCULATOR, null,
            anyRequest.getCapability(), node.getRMNode().getTotalCapability());
  }
}
