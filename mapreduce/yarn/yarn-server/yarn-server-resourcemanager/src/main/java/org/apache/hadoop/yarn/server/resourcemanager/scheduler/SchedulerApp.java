package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;

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

  private Map<ContainerId, RMContainer> liveContainers
  = new HashMap<ContainerId, RMContainer>();
  private List<RMContainer> newlyAllocatedContainers = 
      new ArrayList<RMContainer>();

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

  public synchronized void updateResourceRequests(
      List<ResourceRequest> requests) {
    this.appSchedulingInfo.updateResourceRequests(requests);
  }

  public Map<String, ResourceRequest> getResourceRequests(Priority priority) {
    return this.appSchedulingInfo.getResourceRequests(priority);
  }

  public int getNewContainerId() {
    return this.appSchedulingInfo.getNewContainerId();
  }
  
  @Deprecated
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

  public boolean isPending() {
    return this.appSchedulingInfo.isPending();
  }

  public String getQueueName() {
    return this.appSchedulingInfo.getQueueName();
  }

  public Queue getQueue() {
    return this.queue;
  }

  public synchronized Collection<RMContainer> getLiveContainers() {
    return new ArrayList<RMContainer>(liveContainers.values());
  }

  public synchronized void stop(RMAppAttemptState rmAppAttemptFinalState) {
    // Cleanup all scheduling information
    this.appSchedulingInfo.stop(rmAppAttemptFinalState);
  }

  synchronized public void containerLaunchedOnNode(ContainerId containerId) {
    // Inform the container
    RMContainer rmContainer = 
        getRMContainer(containerId);
    rmContainer.handle(
        new RMContainerEvent(containerId, 
            RMContainerEventType.LAUNCHED));
  }

  public synchronized void killContainers(
      SchedulerApp application) {
  }

  synchronized public void containerCompleted(Container cont,
      RMContainerEventType event) {
    ContainerId containerId = cont.getId();
    // Inform the container
    RMContainer container = getRMContainer(containerId);

    if (container == null) {
      LOG.error("Invalid container completed " + cont.getId());
      return;
    }

    if (event.equals(RMContainerEventType.FINISHED)) {
      // Have to send diagnostics for finished containers.
      container.handle(new RMContainerFinishedEvent(containerId,
          cont.getContainerStatus()));
    } else {
      container.handle(new RMContainerEvent(containerId, event));
    }
    LOG.info("Completed container: " + container.getContainerId() + 
        " in state: " + container.getState());
    
    // Remove from the list of containers
    liveContainers.remove(container.getContainerId());
    
    // Update usage metrics 
    Resource containerResource = container.getContainer().getResource();
    queue.getMetrics().releaseResources(getUser(), 1, containerResource);
    Resources.subtractFrom(currentConsumption, containerResource);
  }

  synchronized public void allocate(NodeType type, SchedulerNode node,
      Priority priority, ResourceRequest request, 
      List<RMContainer> containers) {
    // Update consumption and track allocations
    List<Container> allocatedContainers = 
        new ArrayList<Container>();
    for (RMContainer container : containers) {
      Container c = container.getContainer();
      // Inform the container
      container.handle(
          new RMContainerEvent(c.getId(), RMContainerEventType.START));
      allocatedContainers.add(c);
      
      Resources.addTo(currentConsumption, c.getResource());
      LOG.debug("allocate: applicationId=" + c.getId().getAppId()
          + " container=" + c.getId() + " host="
          + c.getNodeId().toString());
      
      // Add it to allContainers list.
      newlyAllocatedContainers.add(container);
      liveContainers.put(c.getId(), container);
    }
    
    appSchedulingInfo.allocate(type, node, priority, 
        request, allocatedContainers);
  }
  
  synchronized public List<Container> pullNewlyAllocatedContainers() {
    List<Container> returnContainerList = new ArrayList<Container>(
        newlyAllocatedContainers.size());
    for (RMContainer rmContainer : newlyAllocatedContainers) {
      rmContainer.handle(new RMContainerEvent(rmContainer.getContainerId(),
          RMContainerEventType.ACQUIRED));
      returnContainerList.add(rmContainer.getContainer());
    }
    newlyAllocatedContainers.clear();
    return returnContainerList;
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
              " headRoom=" + getHeadroom() + 
              " currentConsumption=" + currentConsumption.getMemory());
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

  public synchronized RMContainer getRMContainer(ContainerId id) {
    return liveContainers.get(id);
  }
}
