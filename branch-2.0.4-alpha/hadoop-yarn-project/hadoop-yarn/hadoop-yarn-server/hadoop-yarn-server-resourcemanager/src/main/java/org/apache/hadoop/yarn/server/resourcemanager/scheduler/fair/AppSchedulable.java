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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.BuilderUtils;

@Private
@Unstable
public class AppSchedulable extends Schedulable {
  private FairScheduler scheduler;
  private FSSchedulerApp app;
  private Resource demand = Resources.createResource(0);
  private boolean runnable = false; // everyone starts as not runnable
  private long startTime;
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private static final Log LOG = LogFactory.getLog(AppSchedulable.class);
  private FSLeafQueue queue;
  private RMContainerTokenSecretManager containerTokenSecretManager;

  public AppSchedulable(FairScheduler scheduler, FSSchedulerApp app, FSLeafQueue queue) {
    this.scheduler = scheduler;
    this.app = app;
    this.startTime = System.currentTimeMillis();
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
    for (Priority p : app.getPriorities()) {
      for (ResourceRequest r : app.getResourceRequests(p).values()) {
        Resource total = Resources.multiply(r.getCapability(), r.getNumContainers());
        Resources.addTo(demand, total);
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
    return Resources.createResource(0);
  }

  /**
   * Get metrics reference from containing queue.
   */
  public QueueMetrics getMetrics() {
    return queue.getMetrics();
  }

  @Override
  public double getWeight() {
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
   * Is this application runnable? Runnable means that the user and queue
   * application counts are within configured quotas.
   */
  public boolean getRunnable() {
    return runnable;
  }

  public void setRunnable(boolean runnable) {
    this.runnable = runnable;
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
    ContainerToken containerToken = null;

    // If security is enabled, send the container-tokens too.
    if (UserGroupInformation.isSecurityEnabled()) {
      containerToken =
          containerTokenSecretManager.createContainerToken(containerId, nodeId,
            application.getUser(), capability);
      if (containerToken == null) {
        return null; // Try again later.
      }
    }

    // Create the container
    Container container = BuilderUtils.newContainer(containerId, nodeId,
        node.getRMNode().getHttpAddress(), capability, priority,
        containerToken);

    return container;
  }

  /**
   * Reserve a spot for {@code container} on this {@code node}. If
   * the container is {@code alreadyReserved} on the node, simply
   * update relevant bookeeping. This dispatches ro relevant handlers
   * in the {@link FSSchedulerNode} and {@link SchedulerApp} classes.
   */
  private void reserve(FSSchedulerApp application, Priority priority,
      FSSchedulerNode node, Container container, boolean alreadyReserved) {
    LOG.info("Making reservation: node=" + node.getHostName() +
                                 " app_id=" + app.getApplicationId());
    if (!alreadyReserved) {
      getMetrics().reserveResource(application.getUser(), container.getResource());
      RMContainer rmContainer = application.reserve(node, priority, null,
          container);
      node.reserveResource(application, priority, rmContainer);
      getMetrics().reserveResource(app.getUser(),
          container.getResource());
      scheduler.getRootQueueMetrics().reserveResource(app.getUser(),
          container.getResource());
    }

    else {
      RMContainer rmContainer = node.getReservedContainer();
      application.reserve(node, priority, rmContainer, container);
      node.reserveResource(application, priority, rmContainer);
    }
  }

  /**
   * Remove the reservation on {@code node} for {@ application} at the given
   * {@link Priority}. This dispatches to the SchedulerApp and SchedulerNode
   * handlers for an unreservation.
   */
  private void unreserve(FSSchedulerApp application, Priority priority,
      FSSchedulerNode node) {
    RMContainer rmContainer = node.getReservedContainer();
    application.unreserve(node, priority);
    node.unreserveResource(application);
    getMetrics().unreserveResource(
        application.getUser(), rmContainer.getContainer().getResource());
    scheduler.getRootQueueMetrics().unreserveResource(
        application.getUser(), rmContainer.getContainer().getResource());
  }

  /**
   * Assign a container to this node to facilitate {@code request}. If node does
   * not have enough memory, create a reservation. This is called once we are
   * sure the particular request should be facilitated by this node.
   */
  private Resource assignContainer(FSSchedulerNode node,
      FSSchedulerApp application, Priority priority,
      ResourceRequest request, NodeType type, boolean reserved) {

    // How much does this request need?
    Resource capability = request.getCapability();

    // How much does the node have?
    Resource available = node.getAvailableResource();

    Container container = null;
    if (reserved) {
      container = node.getReservedContainer().getContainer();
    } else {
      container = createContainer(application, node, capability, priority);
    }

    // Can we allocate a container on this node?
    int availableContainers =
        available.getMemory() / capability.getMemory();

    if (availableContainers > 0) {
      // Inform the application of the new container for this request
      RMContainer allocatedContainer =
          application.allocate(type, node, priority, request, container);
      if (allocatedContainer == null) {
        // Did the application need this resource?
        return Resources.none();
      }
      else {
        // TODO this should subtract resource just assigned
        // TEMPROARY
        getMetrics().setAvailableResourcesToQueue(
            scheduler.getClusterCapacity());
      }


      // If we had previously made a reservation, delete it
      if (reserved) {
        unreserve(application, priority, node);
      }

      // Inform the node
      node.allocateContainer(application.getApplicationId(),
          allocatedContainer);

      return container.getResource();
    } else {
      // The desired container won't fit here, so reserve
      reserve(application, priority, node, container, reserved);

      return FairScheduler.CONTAINER_RESERVED;
    }
  }


  @Override
  public Resource assignContainer(FSSchedulerNode node, boolean reserved) {
    LOG.info("Node offered to app: " + getName() + " reserved: " + reserved);

    if (reserved) {
      RMContainer rmContainer = node.getReservedContainer();
      Priority priority = rmContainer.getReservedPriority();

      // Make sure the application still needs requests at this priority
      if (app.getTotalRequiredResources(priority) == 0) {
        unreserve(app, priority, node);
        return Resources.none();
      }
    } else {
      // If this app is over quota, don't schedule anything
      if (!(getRunnable())) { return Resources.none(); }
    }

    Collection<Priority> prioritiesToTry = (reserved) ? 
        Arrays.asList(node.getReservedContainer().getReservedPriority()) : 
        app.getPriorities();
    
    // For each priority, see if we can schedule a node local, rack local
    // or off-switch request. Rack of off-switch requests may be delayed
    // (not scheduled) in order to promote better locality.
    synchronized (app) {
      for (Priority priority : prioritiesToTry) {
        if (app.getTotalRequiredResources(priority) <= 0) {
          continue;
        }
        
        app.addSchedulingOpportunity(priority);

        ResourceRequest rackLocalRequest = app.getResourceRequest(priority,
            node.getRackName());
        ResourceRequest localRequest = app.getResourceRequest(priority,
            node.getHostName());
        
        NodeType allowedLocality = app.getAllowedLocalityLevel(priority,
            scheduler.getNumClusterNodes(), scheduler.getNodeLocalityThreshold(),
            scheduler.getRackLocalityThreshold());
        
        if (rackLocalRequest != null && rackLocalRequest.getNumContainers() != 0
            && localRequest != null && localRequest.getNumContainers() != 0) {
          return assignContainer(node, app, priority,
              localRequest, NodeType.NODE_LOCAL, reserved);
        }

        if (rackLocalRequest != null && rackLocalRequest.getNumContainers() != 0
            && (allowedLocality.equals(NodeType.RACK_LOCAL) ||
                allowedLocality.equals(NodeType.OFF_SWITCH))) {
          return assignContainer(node, app, priority, rackLocalRequest,
              NodeType.RACK_LOCAL, reserved);
        }

        ResourceRequest offSwitchRequest = app.getResourceRequest(priority,
            RMNode.ANY);
        if (offSwitchRequest != null && offSwitchRequest.getNumContainers() != 0
            && allowedLocality.equals(NodeType.OFF_SWITCH)) {
          return assignContainer(node, app, priority, offSwitchRequest,
              NodeType.OFF_SWITCH, reserved);
        }
      }
    }
    return Resources.none();
  }
}