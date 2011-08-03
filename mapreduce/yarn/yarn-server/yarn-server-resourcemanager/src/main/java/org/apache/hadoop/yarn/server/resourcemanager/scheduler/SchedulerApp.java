package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

public class SchedulerApp {

  private static final Log LOG = LogFactory.getLog(SchedulerApp.class);

  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private final AppSchedulingInfo appSchedulingInfo;
  private final Queue queue;

  private final Resource currentConsumption = recordFactory
      .newRecordInstance(Resource.class);
  private Resource resourceLimit = recordFactory
      .newRecordInstance(Resource.class);

  public SchedulerApp(AppSchedulingInfo application, Queue queue) {
    this.appSchedulingInfo = application;
    this.queue = queue;
    application.setQueue(queue);
  }

  public ApplicationId getApplicationId() {
    return this.appSchedulingInfo.getApplicationId();
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return this.appSchedulingInfo.getApplicationAttemptId();
  }

  public String getUser() {
    return this.appSchedulingInfo.getUser();
  }

  public void updateResourceRequests(List<ResourceRequest> requests) {
    this.appSchedulingInfo.updateResourceRequests(requests);
  }

  public Map<String, ResourceRequest> getResourceRequests(Priority priority) {
    return this.appSchedulingInfo.getResourceRequests(priority);
  }

  public int getNewContainerId() {
    return this.appSchedulingInfo.getNewContainerId();
  }
  
  public List<Container> getCurrentContainers() {
    return this.appSchedulingInfo.getCurrentContainers();
  }

  public Collection<Priority> getPriorities() {
    return this.appSchedulingInfo.getPriorities();
  }

  public ResourceRequest getResourceRequest(Priority priority, String nodeAddress) {
    return this.appSchedulingInfo.getResourceRequest(priority, nodeAddress);
  }

  public Resource getResource(Priority priority) {
    return this.appSchedulingInfo.getResource(priority);
  }

  public void allocate(NodeType type, SchedulerNode node, Priority priority,
      ResourceRequest request, List<Container> containers) {
    this.appSchedulingInfo
        .allocate(type, node, priority, request, containers);
  }

  public boolean isPending() {
    return this.appSchedulingInfo.isPending();
  }

  public String getQueueName() {
    return this.appSchedulingInfo.getQueueName();
  }

  public Queue getQueue() {
    return this.queue;
  }

  public void stop() {
    this.appSchedulingInfo.stop();
  }

  synchronized public void completedContainer(Container container, 
      Resource containerResource) {
    if (container != null) {
      LOG.info("Completed container: " + container);
    }
    queue.getMetrics().releaseResources(getUser(), 1,
        containerResource);
    Resources.subtractFrom(currentConsumption, containerResource);
  }

  synchronized public void allocate(List<Container> containers) {
    // Update consumption and track allocations
    for (Container container : containers) {
      Resources.addTo(currentConsumption, container.getResource());
      LOG.debug("allocate: applicationId=" + container.getId().getAppId()
          + " container=" + container.getId() + " host="
          + container.getContainerManagerAddress());
    }
  }

  public Resource getCurrentConsumption() {
    return this.currentConsumption;
  }

  synchronized public void showRequests() {
    if (LOG.isDebugEnabled()) {
      for (Priority priority : getPriorities()) {
        Map<String, ResourceRequest> requests = getResourceRequests(priority);
        if (requests != null) {
          LOG.debug("showRequests:" + " application=" + getApplicationId() + 
              " available=" + getHeadroom() + 
              " current=" + currentConsumption);
          for (ResourceRequest request : requests.values()) {
            LOG.debug("showRequests:" + " application=" + getApplicationId()
                + " request=" + request);
          }
        }
      }
    }
  }

  public synchronized void setAvailableResourceLimit(Resource globalLimit) {
    this.resourceLimit = globalLimit; 
  }

  /**
   * Get available headroom in terms of resources for the application's user.
   * @return available resource headroom
   */
  public synchronized Resource getHeadroom() {
    Resource limit = Resources.subtract(resourceLimit, currentConsumption);

    // Corner case to deal with applications being slightly over-limit
    if (limit.getMemory() < 0) {
      limit.setMemory(0);
    }
    
    return limit;
  }
}
