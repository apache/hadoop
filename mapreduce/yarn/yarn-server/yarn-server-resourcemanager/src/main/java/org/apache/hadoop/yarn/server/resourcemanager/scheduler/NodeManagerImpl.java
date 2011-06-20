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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;

/**
 * This class is used to keep track of all the applications/containers
 * running on a node.
 *
 */
@Private
@Unstable
public class NodeManagerImpl implements NodeManager {
  private static final Log LOG = LogFactory.getLog(NodeManagerImpl.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private final NodeId nodeId;
  private final String hostName;
  private final int commandPort;
  private final int httpPort;
  private final String nodeAddress; // The containerManager address
  private final String httpAddress;
  private Resource totalCapability;
  private Resource availableResource = recordFactory.newRecordInstance(Resource.class);
  private Resource usedResource = recordFactory.newRecordInstance(Resource.class);
  private final Node node;
  private final NodeHealthStatus nodeHealthStatus = recordFactory
      .newRecordInstance(NodeHealthStatus.class);
  
  private static final Container[] EMPTY_CONTAINER_ARRAY = new Container[] {};
  private static final List<Container> EMPTY_CONTAINER_LIST = Arrays.asList(EMPTY_CONTAINER_ARRAY);
  private static final ApplicationId[] EMPTY_APPLICATION_ARRAY = new ApplicationId[]{};
  private static final List<ApplicationId> EMPTY_APPLICATION_LIST = Arrays.asList(EMPTY_APPLICATION_ARRAY);
  
  public static final String ANY = "*";  
  /* set of containers that are allocated containers */
  private final Map<ContainerId, Container> runningContainers = 
    new TreeMap<ContainerId, Container>();
  
  /* set of containers that need to be cleaned */
  private final Set<Container> containersToClean = 
    new TreeSet<Container>(new org.apache.hadoop.yarn.util.BuilderUtils.ContainerComparator());
  
  /* the list of applications that have finished and need to be purged */
  private final List<ApplicationId> finishedApplications = new ArrayList<ApplicationId>();
  
  private volatile int numContainers;
  
  public NodeManagerImpl(NodeId nodeId, String hostName, 
      int cmPort, int httpPort,
      Node node, Resource capability) {
    this.nodeId = nodeId;   
    this.hostName = hostName;
    this.commandPort = cmPort;
    this.httpPort = httpPort;
    this.totalCapability = capability; 
    this.nodeAddress = hostName + ":" + cmPort;
    this.httpAddress = hostName + ":" + httpPort;
    Resources.addTo(availableResource, capability);
    this.node = node;
    this.nodeHealthStatus.setIsNodeHealthy(true);
    this.nodeHealthStatus.setLastHealthReportTime(System.currentTimeMillis());
  }

  /**
   * NodeInfo for this node.
   * @return the {@link NodeInfo} for this node.
   */
  public NodeInfo getNodeInfo() {
    return this;
  }
  
  @Override
  public String getNodeHostName() {
    return hostName;
  }

  @Override
  public int getCommandPort() {
    return commandPort;
  }

  @Override
  public int getHttpPort() {
    return httpPort;
  }

  /**
   * The Scheduler has allocated containers on this node to the 
   * given application.
   * 
   * @param applicationId application
   * @param containers allocated containers
   */
  public synchronized void allocateContainer(ApplicationId applicationId, 
      List<Container> containers) {
    if (containers == null) {
      LOG.error("Adding null containers for application " + applicationId);
      return;
    }   
    for (Container container : containers) {
      allocateContainer(container);
    }

    LOG.info("addContainers:" +
        " node=" + getNodeAddress() + 
        " #containers=" + containers.size() + 
        " available=" + getAvailableResource().getMemory() + 
        " used=" + getUsedResource().getMemory());
  }

  /**
   * Status update from the NodeManager
   * @param nodeStatus node status
   * @return the set of containers no longer should be used by the
   * node manager.
   */
  public synchronized NodeResponse 
    statusUpdate(Map<String,List<Container>> allContainers) {

    if (allContainers == null) {
      return new NodeResponse(EMPTY_APPLICATION_LIST, EMPTY_CONTAINER_LIST,
          EMPTY_CONTAINER_LIST);
    }
       
    List<Container> listContainers = new ArrayList<Container>();
    // Iterate through the running containers and update their status
    for (Map.Entry<String, List<Container>> e : 
      allContainers.entrySet()) {
      listContainers.addAll(e.getValue());
    }
    NodeResponse statusCheck = update(listContainers);
    return statusCheck;
  }
  
  /**
   * Status update for an application running on a given node
   * @param node node
   * @param containers containers update.
   * @return containers that are completed or need to be preempted.
   */
  private synchronized NodeResponse update(List<Container> containers) {
    List<Container> completedContainers = new ArrayList<Container>();
    List<Container> containersToCleanUp = new ArrayList<Container>();
    List<ApplicationId> lastfinishedApplications = new ArrayList<ApplicationId>();
    
    for (Container container : containers) {
    
      if (container.getState() == ContainerState.COMPLETE) {
        if (runningContainers.remove(container.getId()) != null) {
          updateResource(container);
          LOG.info("Completed container " + container);
        }
        completedContainers.add(container);
        LOG.info("Removed completed container " + container.getId() + " on node " + 
            getNodeAddress());
      }
      else if (container.getState() != ContainerState.COMPLETE && 
          (!runningContainers.containsKey(container.getId()))) {
        containersToCleanUp.add(container);
      }
    }
    containersToCleanUp.addAll(containersToClean);
    /* clear out containers to clean */
    containersToClean.clear();
    lastfinishedApplications.addAll(finishedApplications);
    finishedApplications.clear();
    return new NodeResponse(lastfinishedApplications, completedContainers, 
        containersToCleanUp);
  }
  
  private synchronized void allocateContainer(Container container) {
    deductAvailableResource(container.getResource());
    ++numContainers;
    
    runningContainers.put(container.getId(), container);
    LOG.info("Allocated container " + container.getId() + 
        " to node " + getNodeAddress());
    
    LOG.info("Assigned container " + container.getId() + 
        " of capacity " + container.getResource() + " on host " + getNodeAddress() + 
        ", which currently has " + numContainers + " containers, " + 
        getUsedResource() + " used and " + 
        getAvailableResource() + " available");
  }

  private synchronized boolean isValidContainer(Container c) {    
    if (runningContainers.containsKey(c.getId()))
      return true;
    return false;
  }

  private synchronized void updateResource(Container container) {
    addAvailableResource(container.getResource());
    --numContainers;
  }
  
  /**
   * Release an allocated container on this node.
   * @param container container to be released
   * @return <code>true</code> iff the container was unused, 
   *         <code>false</code> otherwise
   */
  public synchronized boolean releaseContainer(Container container) {
    if (!isValidContainer(container)) {
      LOG.error("Invalid container released " + container);
      return false;
    }
    
    /* remove the containers from the nodemanger */
    
    // Was this container launched?
    runningContainers.remove(container.getId());
    containersToClean.add(container);
    updateResource(container);

    LOG.info("Released container " + container.getId() + 
        " of capacity " + container.getResource() + " on host " + getNodeAddress() + 
        ", which currently has " + numContainers + " containers, " + 
        getUsedResource() + " used and " + getAvailableResource()
        + " available" + ", release resources=" + true);
    return true;
  }

  @Override
  public NodeId getNodeID() {
    return this.nodeId;
  }

  @Override
  public String getNodeAddress() {
    return this.nodeAddress;
  }

  @Override
  public String getHttpAddress() {
    return this.httpAddress;
  }

  @Override
  public Resource getTotalCapability() {
   return this.totalCapability;
  }

  @Override
  public String getRackName() {
    return node.getNetworkLocation();
  }

  @Override
  public Node getNode() {
    return this.node;
  }

  @Override
  public synchronized Resource getAvailableResource() {
    return this.availableResource;
  }

  @Override
  public synchronized Resource getUsedResource() {
    return this.usedResource;
  }

  
  @Override
  public List<Container> getRunningContainers() {
    List<Container> containers = new ArrayList<Container>();
    containers.addAll(runningContainers.values());
    return containers;
  }

  public synchronized void addAvailableResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid resource addition of null resource for "
          + this.nodeAddress);
      return;
    }
    Resources.addTo(availableResource, resource);
    Resources.subtractFrom(usedResource, resource);
  }

  public synchronized void deductAvailableResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid deduction of null resource for "
          + this.nodeAddress);
    }
    Resources.subtractFrom(availableResource, resource);
    Resources.addTo(usedResource, resource);
  }

  public synchronized void finishedApplication(ApplicationId applicationId) {  
    finishedApplications.add(applicationId);
    /* make sure to iterate through the list and remove all the containers that 
     * belong to this application.
     */
  }

  @Override
  public int getNumContainers() {
    return numContainers;
  }

  @Override
  public NodeHealthStatus getNodeHealthStatus() {
    synchronized (this.nodeHealthStatus) {
      return this.nodeHealthStatus;
    }
  }

  @Override
  public void updateHealthStatus(NodeHealthStatus healthStatus) {
    synchronized (this.nodeHealthStatus) {
      this.nodeHealthStatus.setIsNodeHealthy(healthStatus.getIsNodeHealthy());
      this.nodeHealthStatus.setHealthReport(healthStatus.getHealthReport());
      this.nodeHealthStatus.setLastHealthReportTime(healthStatus
          .getLastHealthReportTime());
    }
  }

  private Application reservedApplication = null;
  private Resource reservedResource = null;

  @Override
  public synchronized void reserveResource(
      Application application, Priority priority, Resource resource) {
    // Check if it's already reserved
    if (reservedApplication != null) {

      // Cannot reserve more than one application on a given node!
      if (!reservedApplication.applicationId.equals(application.applicationId)) {
        throw new IllegalStateException("Trying to reserve resource " + resource + 
            " for application " + application.getApplicationId() + 
            " when currently reserved resource " + reservedResource +
            " for application " + reservedApplication.getApplicationId() + 
            " on node " + this);
      }

      LOG.info("Updated reserved resource " + resource + " on node " + 
          this + " for application " + application);
    } else {
      this.reservedApplication = application;
      LOG.info("Reserved resource " + resource + " on node " + this + 
          " for application " + application);
    }
    reservedResource = resource;
  }

  @Override
  public synchronized void unreserveResource(Application application, 
      Priority priority) {
    // Cannot unreserve for wrong application...
    if (!reservedApplication.applicationId.equals(application.applicationId)) {
      throw new IllegalStateException("Trying to unreserve " +  
          " for application " + application.getApplicationId() + 
          " when currently reserved " + 
          " for application " + reservedApplication.getApplicationId() + 
          " on node " + this);
    }
    
    reservedApplication = null;
    reservedResource = null;
  }

  @Override
  public synchronized Application getReservedApplication() {
    return reservedApplication;
  }

  @Override
  public synchronized Resource getReservedResource() {
    return reservedResource;
  }

  @Override
  public String toString() {
    return "host: " + getNodeAddress() + " #containers=" + getNumContainers() +  
      " available=" + getAvailableResource().getMemory() + 
      " used=" + getUsedResource().getMemory();
  }

 }
