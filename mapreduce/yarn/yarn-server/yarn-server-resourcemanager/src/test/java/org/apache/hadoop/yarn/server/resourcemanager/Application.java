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
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.Task.State;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.util.Records;

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
  
  final private Map<Priority, Resource> requestSpec = 
    new TreeMap<Priority, Resource>(
        new org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.Comparator());
  
  final private Map<Priority, Map<String, ResourceRequest>> requests = 
    new TreeMap<Priority, Map<String, ResourceRequest>>(
        new org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.Comparator());
  
  final Map<Priority, Set<Task>> tasks = 
    new TreeMap<Priority, Set<Task>>(
        new org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.Comparator());
  
  final private Set<ResourceRequest> ask = 
    new TreeSet<ResourceRequest>(
        new org.apache.hadoop.yarn.util.BuilderUtils.ResourceRequestComparator());

  final private Map<String, NodeManager> nodes = 
    new HashMap<String, NodeManager>();
  
  Resource used = recordFactory.newRecordInstance(Resource.class);
  
  public Application(String user, ResourceManager resourceManager) {
    this(user, "default", resourceManager);
  }
  
  public Application(String user, String queue, ResourceManager resourceManager) {
    this.user = user;
    this.queue = queue;
    this.resourceManager = resourceManager;
    this.applicationId =
      this.resourceManager.getClientRMService().getNewApplicationId();
    this.applicationAttemptId = Records.newRecord(ApplicationAttemptId.class);
    this.applicationAttemptId.setApplicationId(this.applicationId);
    this.applicationAttemptId.setAttemptId(this.numAttempts.getAndIncrement());
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

  public static String resolve(String hostName) {
    return NetworkTopology.DEFAULT_RACK;
  }
  
  public int getNextTaskId() {
    return taskCounter.incrementAndGet();
  }
  
  public Resource getUsedResources() {
    return used;
  }
  
  public synchronized void submit() throws IOException {
    ApplicationSubmissionContext context = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    context.setApplicationId(this.applicationId);
    context.setUser(this.user);
    context.setQueue(this.queue);
    SubmitApplicationRequest request = recordFactory
        .newRecordInstance(SubmitApplicationRequest.class);
    request.setApplicationSubmissionContext(context);
    resourceManager.getClientRMService().submitApplication(request);
  }
  
  public synchronized void addResourceRequestSpec(
      Priority priority, Resource capability) {
    Resource currentSpec = requestSpec.put(priority, capability);
    if (currentSpec != null) {
      throw new IllegalStateException("Resource spec already exists for " +
      		"priority " + priority.getPriority() + " - " + currentSpec.getMemory());
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
    Priority priority = task.getPriority();
    Map<String, ResourceRequest> requests = this.requests.get(priority);
    if (requests == null) {
      requests = new HashMap<String, ResourceRequest>();
      this.requests.put(priority, requests);
      LOG.info("DEBUG --- Added" +
      		" priority=" + priority + 
      		" application=" + applicationId);
    }
    
    final Resource capability = requestSpec.get(priority);
    
    // Note down the task
    Set<Task> tasks = this.tasks.get(priority);
    if (tasks == null) {
      tasks = new HashSet<Task>();
      this.tasks.put(priority, tasks);
    }
    tasks.add(task);
    
    LOG.info("Added task " + task.getTaskId() + " to application " + 
        applicationId + " at priority " + priority);
    
    LOG.info("DEBUG --- addTask:" +
    		" application=" + applicationId + 
    		" #asks=" + ask.size());
    
    // Create resource requests
    for (String host : task.getHosts()) {
      // Data-local
      addResourceRequest(priority, requests, host, capability);
    }
        
    // Rack-local
    for (String rack : task.getRacks()) {
      addResourceRequest(priority, requests, rack, capability);
    }
      
    // Off-switch
    addResourceRequest(priority, requests, 
        org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode.ANY, 
        capability);
  }
  
  public synchronized void finishTask(Task task) throws IOException {
    Set<Task> tasks = this.tasks.get(task.getPriority());
    if (!tasks.remove(task)) {
      throw new IllegalStateException(
          "Finishing unknown task " + task.getTaskId() + 
          " from application " + applicationId);
    }
    
    NodeManager nodeManager = task.getNodeManager();
    ContainerId containerId = task.getContainerId();
    task.stop();
    StopContainerRequest stopRequest = recordFactory.newRecordInstance(StopContainerRequest.class);
    stopRequest.setContainerId(containerId);
    nodeManager.stopContainer(stopRequest);
    
    Resources.subtractFrom(used, requestSpec.get(task.getPriority()));
    
    LOG.info("Finished task " + task.getTaskId() + 
        " of application " + applicationId + 
        " on node " + nodeManager.getHostName() + 
        ", currently using " + used + " resources");
  }
  
  private synchronized void addResourceRequest(
      Priority priority, Map<String, ResourceRequest> requests, 
      String resourceName, Resource capability) {
    ResourceRequest request = requests.get(resourceName);
    if (request == null) {
      request = 
        org.apache.hadoop.yarn.util.BuilderUtils.newResourceRequest(
            priority, resourceName, capability, 1);
      requests.put(resourceName, request);
    } else {
      request.setNumContainers(request.getNumContainers() + 1);
    }
    
    // Note this down for next interaction with ResourceManager
    ask.remove(request);
    ask.add(
        org.apache.hadoop.yarn.util.BuilderUtils.newResourceRequest(
            request)); // clone to ensure the RM doesn't manipulate the same obj
    
    LOG.info("DEBUG --- addResourceRequest:" +
    		" applicationId=" + applicationId.getId() +
    		" priority=" + priority.getPriority() + 
        " resourceName=" + resourceName + 
        " capability=" + capability +
        " numContainers=" + request.getNumContainers() + 
        " #asks=" + ask.size());
  }
  
  public synchronized List<Container> getResources() throws IOException {
    LOG.info("DEBUG --- getResources begin:" +
        " application=" + applicationId + 
        " #ask=" + ask.size());
    for (ResourceRequest request : ask) {
      LOG.info("DEBUG --- getResources:" +
          " application=" + applicationId + 
          " ask-request=" + request);
    }
    
    // Get resources from the ResourceManager
    resourceManager.getResourceScheduler().allocate(applicationAttemptId,
        new ArrayList<ResourceRequest>(ask), new ArrayList<ContainerId>());
    System.out.println("-=======" + applicationAttemptId);
    System.out.println("----------" + resourceManager.getRMContext().getRMApps()
        .get(applicationId).getRMAppAttempt(applicationAttemptId));
    
     List<Container> containers = null;
     // TODO: Fix
//       resourceManager.getRMContext().getRMApps()
//        .get(applicationId).getRMAppAttempt(applicationAttemptId)
//        .pullNewlyAllocatedContainers();

    // Clear state for next interaction with ResourceManager
    ask.clear();
    
    LOG.info("DEBUG --- getResources() for " + applicationId + ":" +
    		" ask=" + ask.size() + 
    		" recieved=" + containers.size());
    
    return containers;
  }
  
  public synchronized void assign(List<Container> containers) 
  throws IOException {
    
    int numContainers = containers.size();
    // Schedule in priority order
    for (Priority priority : requests.keySet()) {
      assign(priority, NodeType.DATA_LOCAL, containers);
      assign(priority, NodeType.RACK_LOCAL, containers);
      assign(priority, NodeType.OFF_SWITCH, containers);

      if (containers.isEmpty()) { 
        break;
      }
    }
    
    int assignedContainers = numContainers - containers.size();
    LOG.info("Application " + applicationId + " assigned " + 
        assignedContainers + "/" + numContainers);
  }
  
  public synchronized void schedule() throws IOException {
    assign(getResources());
  }
  
  private synchronized void assign(Priority priority, NodeType type, 
      List<Container> containers) throws IOException {
    for (Iterator<Container> i=containers.iterator(); i.hasNext();) {
      Container container = i.next();
      String host = container.getNodeId().toString();
      
      if (Resources.equals(requestSpec.get(priority), container.getResource())) { 
        // See which task can use this container
        for (Iterator<Task> t=tasks.get(priority).iterator(); t.hasNext();) {
          Task task = t.next();
          if (task.getState() == State.PENDING && task.canSchedule(type, host)) {
            NodeManager nodeManager = getNodeManager(host);
            
            task.start(nodeManager, container.getId());
            i.remove();
            
            // Track application resource usage
            Resources.addTo(used, container.getResource());
            
            LOG.info("Assigned container (" + container + ") of type " + type +
                " to task " + task.getTaskId() + " at priority " + priority + 
                " on node " + nodeManager.getHostName() +
                ", currently using " + used + " resources");

            // Update resource requests
            updateResourceRequests(requests.get(priority), type, task);

            // Launch the container
            StartContainerRequest startRequest = recordFactory.newRecordInstance(StartContainerRequest.class);
            startRequest.setContainerLaunchContext(createCLC(container));
            nodeManager.startContainer(startRequest);
            break;
          }
        }
      }
    }
  }

  private void updateResourceRequests(Map<String, ResourceRequest> requests, 
      NodeType type, Task task) {
    if (type == NodeType.DATA_LOCAL) {
      for (String host : task.getHosts()) {
        LOG.info("DEBUG --- updateResourceRequests:" +
            " application=" + applicationId +
        		" type=" + type + 
        		" host=" + host + 
        		" request=" + ((requests == null) ? "null" : requests.get(host)));
        updateResourceRequest(requests.get(host));
      }
    }
    
    if (type == NodeType.DATA_LOCAL || type == NodeType.RACK_LOCAL) {
      for (String rack : task.getRacks()) {
        LOG.info("DEBUG --- updateResourceRequests:" +
            " application=" + applicationId +
            " type=" + type + 
            " rack=" + rack + 
            " request=" + ((requests == null) ? "null" : requests.get(rack)));
        updateResourceRequest(requests.get(rack));
      }
    }
    
    updateResourceRequest(
        requests.get(
            org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode.ANY)
            );
    
    LOG.info("DEBUG --- updateResourceRequests:" +
        " application=" + applicationId +
    		" #asks=" + ask.size());
  }
  
  private void updateResourceRequest(ResourceRequest request) {
    request.setNumContainers(request.getNumContainers() - 1);

    // Note this for next interaction with ResourceManager
    ask.remove(request);
    ask.add(
        org.apache.hadoop.yarn.util.BuilderUtils.newResourceRequest(
        request)); // clone to ensure the RM doesn't manipulate the same obj

    LOG.info("DEBUG --- updateResourceRequest:" +
        " application=" + applicationId +
    		" request=" + request);
  }

  private ContainerLaunchContext createCLC(Container container) {
    ContainerLaunchContext clc = recordFactory.newRecordInstance(ContainerLaunchContext.class);
    clc.setContainerId(container.getId());
    clc.setUser(this.user);
    clc.setResource(container.getResource());
    return clc;
  }
}
