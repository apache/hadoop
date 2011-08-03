package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

public class SchedulerNode {

  private static final Log LOG = LogFactory.getLog(SchedulerNode.class);

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private Resource availableResource = recordFactory.newRecordInstance(Resource.class);
  private Resource usedResource = recordFactory.newRecordInstance(Resource.class);

  private volatile int numContainers;

  /* set of containers that are allocated containers */
  private final Map<ContainerId, Container> launchedContainers = 
    new TreeMap<ContainerId, Container>();
  
  private final RMNode rmNode;

  public static final String ANY = "*";

  public SchedulerNode(RMNode node) {
    this.rmNode = node;
    this.availableResource.setMemory(node.getTotalCapability().getMemory());
  }

  public RMNode getRMNode() {
    return this.rmNode;
  }

  public NodeId getNodeID() {
    return this.rmNode.getNodeID();
  }

  public String getHttpAddress() {
    return this.rmNode.getHttpAddress();
  }

  public String getNodeAddress() {
    return this.rmNode.getNodeAddress();
  }

  public String getRackName() {
    return this.rmNode.getRackName();
  }

  /**
   * The Scheduler has allocated containers on this node to the 
   * given application.
   * 
   * @param applicationId application
   * @param containers allocated containers
   */
  public synchronized void allocateContainer(ApplicationId applicationId, 
      Container container) {
    deductAvailableResource(container.getResource());
    ++numContainers;
    
    launchedContainers.put(container.getId(), container);
    LOG.info("Allocated container " + container.getId() + 
        " to node " + rmNode.getNodeAddress());
    
    LOG.info("Assigned container " + container.getId() + 
        " of capacity " + container.getResource() + " on host " + rmNode.getNodeAddress() + 
        ", which currently has " + numContainers + " containers, " + 
        getUsedResource() + " used and " + 
        getAvailableResource() + " available");
  }

  public synchronized Resource getAvailableResource() {
    return this.availableResource;
  }

  public synchronized Resource getUsedResource() {
    return this.usedResource;
  }

  private synchronized boolean isValidContainer(Container c) {    
    if (launchedContainers.containsKey(c.getId()))
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
    
    launchedContainers.remove(container.getId());
    updateResource(container);

    LOG.info("Released container " + container.getId() + 
        " of capacity " + container.getResource() + " on host " + rmNode.getNodeAddress() + 
        ", which currently has " + numContainers + " containers, " + 
        getUsedResource() + " used and " + getAvailableResource()
        + " available" + ", release resources=" + true);
    return true;
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
    }
    Resources.subtractFrom(availableResource, resource);
    Resources.addTo(usedResource, resource);
  }

  @Override
  public String toString() {
    return "host: " + rmNode.getNodeAddress() + " #containers=" + getNumContainers() +  
      " available=" + getAvailableResource().getMemory() + 
      " used=" + getUsedResource().getMemory();
  }

  public int getNumContainers() {
    return numContainers;
  }

  public synchronized List<Container> getRunningContainers() {
    return new ArrayList<Container>(launchedContainers.values());
  }
}
