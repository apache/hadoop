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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.Task.State;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;


import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
public class Application {
  private static final Log LOG = LogFactory.getLog(Application.class);
  
  private AtomicInteger taskCounter = new AtomicInteger(0);

  private AtomicInteger numAttempts = new AtomicInteger(0);
  final private String user;
  final private String queue;
  final private ApplicationId applicationId;
  final private ApplicationAttemptId applicationAttemptId;
  final private ResourceManager resourceManager;
  private final static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  final private Map<SchedulerRequestKey, Resource> requestSpec =
      new TreeMap<>();

  final private Map<SchedulerRequestKey, Map<String, ResourceRequest>>
      requests = new TreeMap<>();

  final Map<SchedulerRequestKey, Set<Task>> tasks = new TreeMap<>();

  final private Set<ResourceRequest> ask =
      new TreeSet<>(
          new org.apache.hadoop.yarn.api.records.ResourceRequest
              .ResourceRequestComparator());

  final private Map<String, NodeManager> nodes = new HashMap<>();
  
  Resource used = recordFactory.newRecordInstance(Resource.class);
  
  public Application(String user, ResourceManager resourceManager) 
      throws YarnException {
    this(user, "default", resourceManager);
  }
  
  public Application(String user, String queue, ResourceManager resourceManager) 
      throws YarnException {
    this.user = user;
    this.queue = queue;
    this.resourceManager = resourceManager;
    // register an application
    GetNewApplicationRequest request =
            Records.newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse newApp = 
        this.resourceManager.getClientRMService().getNewApplication(request);
    this.applicationId = newApp.getApplicationId();
  
    this.applicationAttemptId =
        ApplicationAttemptId.newInstance(this.applicationId,
          this.numAttempts.getAndIncrement());
  }

  public String getUser() {
    return user;
  }

  public String getQueue() {
    return queue;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }
  
  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public static String resolve(String hostName) {
    return NetworkTopology.DEFAULT_RACK;
  }
  
  public int getNextTaskId() {
    return taskCounter.incrementAndGet();
  }
  
  public Resource getUsedResources() {
    return used;
  }
  
  @SuppressWarnings("deprecation")
  public synchronized void submit() throws IOException, YarnException {
    ApplicationSubmissionContext context = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    context.setApplicationId(this.applicationId);
    context.setQueue(this.queue);
    
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer
        = Records.newRecord(ContainerLaunchContext.class);
    context.setAMContainerSpec(amContainer);
    context.setResource(Resources.createResource(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
    
    SubmitApplicationRequest request = recordFactory
        .newRecordInstance(SubmitApplicationRequest.class);
    request.setApplicationSubmissionContext(context);
    final ResourceScheduler scheduler = resourceManager.getResourceScheduler();
    
    resourceManager.getClientRMService().submitApplication(request);

    RMAppEvent event =
        new RMAppEvent(this.applicationId, RMAppEventType.START);
    resourceManager.getRMContext().getRMApps().get(applicationId).handle(event);
    event =
        new RMAppEvent(this.applicationId, RMAppEventType.APP_NEW_SAVED);
    resourceManager.getRMContext().getRMApps().get(applicationId).handle(event);
    event =
        new RMAppEvent(this.applicationId, RMAppEventType.APP_ACCEPTED);
    resourceManager.getRMContext().getRMApps().get(applicationId).handle(event);

    // Notify scheduler
    AppAddedSchedulerEvent addAppEvent =
        new AppAddedSchedulerEvent(this.applicationId, this.queue, "user");
    scheduler.handle(addAppEvent);
    AppAttemptAddedSchedulerEvent addAttemptEvent =
        new AppAttemptAddedSchedulerEvent(this.applicationAttemptId, false);
    scheduler.handle(addAttemptEvent);
  }

  public synchronized void addResourceRequestSpec(
      Priority priority, Resource capability) {
    addResourceRequestSpec(TestUtils.toSchedulerKey(priority.getPriority()),
        capability);
  }
  public synchronized void addResourceRequestSpec(
      SchedulerRequestKey schedulerKey, Resource capability) {
    Resource currentSpec = requestSpec.put(schedulerKey, capability);
    if (currentSpec != null) {
      throw new IllegalStateException("Resource spec already exists for " +
          "priority " + schedulerKey.getPriority().getPriority()
          + " - " + currentSpec.getMemorySize());
    }
  }
  
  public synchronized void addNodeManager(String host,
      int containerManagerPort, NodeManager nodeManager) {
    nodes.put(host + ":" + containerManagerPort, nodeManager);
  }
  
  private synchronized NodeManager getNodeManager(String host) {
    return nodes.get(host);
  }
  
  public synchronized void addTask(Task task) {
    SchedulerRequestKey schedulerKey = task.getSchedulerKey();
    Map<String, ResourceRequest> requests = this.requests.get(schedulerKey);
    if (requests == null) {
      requests = new HashMap<String, ResourceRequest>();
      this.requests.put(schedulerKey, requests);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Added priority=" + schedulerKey.getPriority()
            + " application="+ applicationId);
      }
    }
    
    final Resource capability = requestSpec.get(schedulerKey);
    
    // Note down the task
    Set<Task> tasks = this.tasks.get(schedulerKey);
    if (tasks == null) {
      tasks = new HashSet<Task>();
      this.tasks.put(schedulerKey, tasks);
    }
    tasks.add(task);
    
    LOG.info("Added task " + task.getTaskId() + " to application " + 
        applicationId + " at priority " + schedulerKey.getPriority());
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("addTask: application=" + applicationId
        + " #asks=" + ask.size());
    }
    
    // Create resource requests
    for (String host : task.getHosts()) {
      // Data-local
      addResourceRequest(schedulerKey, requests, host, capability);
    }
        
    // Rack-local
    for (String rack : task.getRacks()) {
      addResourceRequest(schedulerKey, requests, rack, capability);
    }
      
    // Off-switch
    addResourceRequest(schedulerKey, requests, ResourceRequest.ANY, capability);
  }
  
  public synchronized void finishTask(Task task) throws IOException,
      YarnException {
    Set<Task> tasks = this.tasks.get(task.getSchedulerKey());
    if (!tasks.remove(task)) {
      throw new IllegalStateException(
          "Finishing unknown task " + task.getTaskId() + 
          " from application " + applicationId);
    }
    
    NodeManager nodeManager = task.getNodeManager();
    ContainerId containerId = task.getContainerId();
    task.stop();
    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(containerId);
    StopContainersRequest stopRequest =
        StopContainersRequest.newInstance(containerIds);
    nodeManager.stopContainers(stopRequest);
    
    Resources.subtractFrom(used, requestSpec.get(task.getSchedulerKey()));
    
    LOG.info("Finished task " + task.getTaskId() + 
        " of application " + applicationId + 
        " on node " + nodeManager.getHostName() + 
        ", currently using " + used + " resources");
  }
  
  private synchronized void addResourceRequest(
      SchedulerRequestKey schedulerKey, Map<String, ResourceRequest> requests,
      String resourceName, Resource capability) {
    ResourceRequest request = requests.get(resourceName);
    if (request == null) {
      request = 
        org.apache.hadoop.yarn.server.utils.BuilderUtils.newResourceRequest(
            schedulerKey.getPriority(), resourceName, capability, 1);
      requests.put(resourceName, request);
    } else {
      request.setNumContainers(request.getNumContainers() + 1);
    }
    if (request.getNodeLabelExpression() == null) {
      request.setNodeLabelExpression(RMNodeLabelsManager.NO_LABEL);
    }
    
    // Note this down for next interaction with ResourceManager
    ask.remove(request);
    // clone to ensure the RM doesn't manipulate the same obj
    ask.add(ResourceRequest.clone(request));

    if (LOG.isDebugEnabled()) {
      LOG.debug("addResourceRequest: applicationId=" + applicationId.getId()
          + " priority=" + schedulerKey.getPriority().getPriority()
          + " resourceName=" + resourceName + " capability=" + capability
          + " numContainers=" + request.getNumContainers()
          + " #asks=" + ask.size());
    }
  }
  
  public synchronized List<Container> getResources() throws IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("getResources begin:" + " application=" + applicationId
        + " #ask=" + ask.size());

      for (ResourceRequest request : ask) {
        LOG.debug("getResources:" + " application=" + applicationId
          + " ask-request=" + request);
      }
    }
    
    // Get resources from the ResourceManager
    Allocation allocation = resourceManager.getResourceScheduler().allocate(
        applicationAttemptId, new ArrayList<ResourceRequest>(ask), null, new ArrayList<ContainerId>(), null, null,
        new ContainerUpdates());

    if (LOG.isInfoEnabled()) {
      LOG.info("-=======" + applicationAttemptId + System.lineSeparator() +
          "----------" + resourceManager.getRMContext().getRMApps()
              .get(applicationId).getRMAppAttempt(applicationAttemptId));
    }

    List<Container> containers = allocation.getContainers();

    // Clear state for next interaction with ResourceManager
    ask.clear();
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("getResources() for " + applicationId + ":"
        + " ask=" + ask.size() + " received=" + containers.size());
    }
    
    return containers;
  }
  
  public synchronized void assign(List<Container> containers) 
  throws IOException, YarnException {
    
    int numContainers = containers.size();
    // Schedule in priority order
    for (SchedulerRequestKey schedulerKey: requests.keySet()) {
      assign(schedulerKey, NodeType.NODE_LOCAL, containers);
      assign(schedulerKey, NodeType.RACK_LOCAL, containers);
      assign(schedulerKey, NodeType.OFF_SWITCH, containers);

      if (containers.isEmpty()) { 
        break;
      }
    }
    
    int assignedContainers = numContainers - containers.size();
    LOG.info("Application " + applicationId + " assigned " + 
        assignedContainers + "/" + numContainers);
  }
  
  public synchronized void schedule() throws IOException, YarnException {
    assign(getResources());
  }
  
  private synchronized void assign(SchedulerRequestKey schedulerKey,
      NodeType type, List<Container> containers)
      throws IOException, YarnException {
    for (Iterator<Container> i=containers.iterator(); i.hasNext();) {
      Container container = i.next();
      String host = container.getNodeId().toString();
      
      if (Resources.equals(requestSpec.get(schedulerKey),
          container.getResource())) {
        // See which task can use this container
        for (Iterator<Task> t=tasks.get(schedulerKey).iterator();
             t.hasNext();) {
          Task task = t.next();
          if (task.getState() == State.PENDING && task.canSchedule(type, host)) {
            NodeManager nodeManager = getNodeManager(host);
            
            task.start(nodeManager, container.getId());
            i.remove();
            
            // Track application resource usage
            Resources.addTo(used, container.getResource());

            LOG.info("Assigned container (" + container + ") of type " + type +
                " to task " + task.getTaskId() + " at priority " +
                schedulerKey.getPriority() +
                " on node " + nodeManager.getHostName() +
                ", currently using " + used + " resources");

            // Update resource requests
            updateResourceRequests(requests.get(schedulerKey), type, task);

            // Launch the container
            StartContainerRequest scRequest =
                StartContainerRequest.newInstance(createCLC(),
                  container.getContainerToken());
            List<StartContainerRequest> list =
                new ArrayList<StartContainerRequest>();
            list.add(scRequest);
            StartContainersRequest allRequests =
                StartContainersRequest.newInstance(list);
            nodeManager.startContainers(allRequests);
            break;
          }
        }
      }
    }
  }

  private void updateResourceRequests(Map<String, ResourceRequest> requests, 
      NodeType type, Task task) {
    if (type == NodeType.NODE_LOCAL) {
      for (String host : task.getHosts()) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("updateResourceDemands:" + " application=" + applicationId
            + " type=" + type + " host=" + host
            + " request=" + ((requests == null) ? "null" : requests.get(host)));
        }
        updateResourceRequest(requests.get(host));
      }
    }
    
    if (type == NodeType.NODE_LOCAL || type == NodeType.RACK_LOCAL) {
      for (String rack : task.getRacks()) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("updateResourceDemands:" + " application=" + applicationId
            + " type=" + type + " rack=" + rack
            + " request=" + ((requests == null) ? "null" : requests.get(rack)));
        }
        updateResourceRequest(requests.get(rack));
      }
    }
    
    updateResourceRequest(requests.get(ResourceRequest.ANY));
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("updateResourceDemands:" + " application=" + applicationId
        + " #asks=" + ask.size());
    }
  }
  
  private void updateResourceRequest(ResourceRequest request) {
    request.setNumContainers(request.getNumContainers() - 1);

    // Note this for next interaction with ResourceManager
    ask.remove(request);
    // clone to ensure the RM doesn't manipulate the same obj
    ask.add(ResourceRequest.clone(request));

    if(LOG.isDebugEnabled()) {
      LOG.debug("updateResourceRequest:" + " application=" + applicationId
        + " request=" + request);
    }
  }

  private ContainerLaunchContext createCLC() {
    ContainerLaunchContext clc = recordFactory.newRecordInstance(ContainerLaunchContext.class);
    return clc;
  }
}
