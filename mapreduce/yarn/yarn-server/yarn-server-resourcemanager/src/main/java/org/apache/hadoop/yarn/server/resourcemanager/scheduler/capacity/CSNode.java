package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

public class CSNode extends SchedulerNode {

  private static final Log LOG = LogFactory.getLog(CSNode.class);

  private CSApp reservedApplication = null;
  private Resource reservedResource = null;

  public CSNode(RMNode node) {
    super(node);
  }

  public synchronized void reserveResource(
      CSApp application, Priority priority, Resource resource) {
    // Check if it's already reserved
    if (reservedApplication != null) {

      // Cannot reserve more than one application on a given node!
      if (!reservedApplication.getApplicationId().equals(
          application.getApplicationId())) {
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

  public synchronized void unreserveResource(CSApp application, 
      Priority priority) {
    // Cannot unreserve for wrong application...
    if (!reservedApplication.getApplicationId().equals(
        application.getApplicationId())) {
      throw new IllegalStateException("Trying to unreserve " +  
          " for application " + application.getApplicationId() + 
          " when currently reserved " + 
          " for application " + reservedApplication.getApplicationId() + 
          " on node " + this);
    }
    
    reservedApplication = null;
    reservedResource = null;
  }

  public synchronized CSApp getReservedApplication() {
    return reservedApplication;
  }

  public synchronized Resource getReservedResource() {
    return reservedResource;
  }

}
