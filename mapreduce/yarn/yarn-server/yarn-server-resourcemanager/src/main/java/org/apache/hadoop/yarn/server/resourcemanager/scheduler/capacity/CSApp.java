package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;

public class CSApp extends SchedulerApp {

  private static final Log LOG = LogFactory.getLog(CSApp.class);

  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  final Map<Priority, Set<CSNode>> reservedContainers = new HashMap<Priority, Set<CSNode>>();
  Map<Priority, Integer> schedulingOpportunities = new HashMap<Priority, Integer>();

  final Resource currentReservation = recordFactory
      .newRecordInstance(Resource.class);

  /* Reserved containers */
  private final Comparator<CSNode> nodeComparator = new Comparator<CSNode>() {
    @Override
    public int compare(CSNode o1, CSNode o2) {
      return o1.getNodeID().getId() - o2.getNodeID().getId();
    }
  };

  public CSApp(AppSchedulingInfo appSchedulingInfo, Queue queue) {
    super(appSchedulingInfo, queue);
  }

  synchronized public void resetSchedulingOpportunities(Priority priority) {
    Integer schedulingOpportunities = this.schedulingOpportunities
        .get(priority);
    schedulingOpportunities = 0;
    this.schedulingOpportunities.put(priority, schedulingOpportunities);
  }

  synchronized public void addSchedulingOpportunity(Priority priority) {
    Integer schedulingOpportunities = this.schedulingOpportunities
        .get(priority);
    if (schedulingOpportunities == null) {
      schedulingOpportunities = 0;
    }
    ++schedulingOpportunities;
    this.schedulingOpportunities.put(priority, schedulingOpportunities);
  }

  synchronized public int getSchedulingOpportunities(Priority priority) {
    Integer schedulingOpportunities = this.schedulingOpportunities
        .get(priority);
    if (schedulingOpportunities == null) {
      schedulingOpportunities = 0;
      this.schedulingOpportunities.put(priority, schedulingOpportunities);
    }
    return schedulingOpportunities;
  }

  public synchronized int getReservedContainers(Priority priority) {
    Set<CSNode> reservedNodes = this.reservedContainers.get(priority);
    return (reservedNodes == null) ? 0 : reservedNodes.size();
  }

  public synchronized void reserveResource(CSNode node, Priority priority,
      Resource resource) {
    Set<CSNode> reservedNodes = this.reservedContainers.get(priority);
    if (reservedNodes == null) {
      reservedNodes = new TreeSet<CSNode>(nodeComparator);
      reservedContainers.put(priority, reservedNodes);
    }
    reservedNodes.add(node);
    Resources.add(currentReservation, resource);
    LOG.info("Application " + getApplicationId() + " reserved " + resource
        + " on node " + node + ", currently has " + reservedNodes.size()
        + " at priority " + priority 
        + "; currentReservation " + currentReservation);
    getQueue().getMetrics().reserveResource(
        getUser(), resource);
  }

  public synchronized void unreserveResource(CSNode node, Priority priority) {
    Set<CSNode> reservedNodes = reservedContainers.get(priority);
    reservedNodes.remove(node);
    if (reservedNodes.isEmpty()) {
      this.reservedContainers.remove(priority);
    }
    
    Resource resource = getResource(priority);
    Resources.subtract(currentReservation, resource);

    LOG.info("Application " + getApplicationId() + " unreserved " + " on node "
        + node + ", currently has " + reservedNodes.size() + " at priority "
        + priority + "; currentReservation " + currentReservation);
    getQueue().getMetrics().unreserveResource(
        getUser(), node.getReservedResource());
  }

  public synchronized boolean isReserved(CSNode node, Priority priority) {
    Set<CSNode> reservedNodes = reservedContainers.get(priority);
    if (reservedNodes != null) {
      return reservedNodes.contains(node);
    }
    return false;
  }

  public float getLocalityWaitFactor(Priority priority, int clusterNodes) {
    // Estimate: Required unique resources (i.e. hosts + racks)
    int requiredResources = Math.max(this.getResourceRequests(priority).size() - 1, 1);
    return ((float) requiredResources / clusterNodes);
  }

  public Map<Priority, Set<CSNode>> getAllReservations() {
    return new HashMap<Priority, Set<CSNode>>(reservedContainers);
  }

  public synchronized Resource getHeadroom() {
    Resource limit = Resources.subtract(super.getHeadroom(),
        currentReservation);

    // Corner case to deal with applications being slightly over-limit
    if (limit.getMemory() < 0) {
      limit.setMemory(0);
    }

    return limit;
  }
}
