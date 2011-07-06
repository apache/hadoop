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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMConfig;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationTrackerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.ApplicationInfo;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.ClusterTracker;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Application;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.BuilderUtils;

@LimitedPrivate("yarn")
@Evolving
public class FifoScheduler implements ResourceScheduler {

  private static final Log LOG = LogFactory.getLog(FifoScheduler.class);

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  Configuration conf;
  private ContainerTokenSecretManager containerTokenSecretManager;

  private final static Container[] EMPTY_CONTAINER_ARRAY = new Container[] {};
  private final static List<Container> EMPTY_CONTAINER_LIST = Arrays.asList(EMPTY_CONTAINER_ARRAY);
  private ClusterTracker clusterTracker;

  private static final int MINIMUM_MEMORY = 1024;

  private static final String FIFO_PREFIX = 
    YarnConfiguration.RM_PREFIX + "fifo.";
  @Private
  public static final String MINIMUM_ALLOCATION = 
    FIFO_PREFIX + "minimum-allocation-mb";

  private static final int MAXIMUM_MEMORY = 10240;

  @Private
  public static final String MAXIMUM_ALLOCATION = 
    FIFO_PREFIX + "maximum-allocation-mb";

  private Resource minimumAllocation;
  private Resource maximumAllocation;

  Map<ApplicationId, Application> applications =
      new TreeMap<ApplicationId, Application>(
          new BuilderUtils.ApplicationIdComparator());

  private static final String DEFAULT_QUEUE_NAME = "default";
  private final QueueMetrics metrics =
    QueueMetrics.forQueue(DEFAULT_QUEUE_NAME, null, false);

  private final Queue DEFAULT_QUEUE = new Queue() {
    @Override
    public String getQueueName() {
      return DEFAULT_QUEUE_NAME;
    }

    @Override
    public QueueMetrics getMetrics() {
      return metrics;
    }

    @Override
    public QueueInfo getQueueInfo(boolean includeApplications, 
        boolean includeChildQueues, boolean recursive) {
      QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
      queueInfo.setQueueName(DEFAULT_QUEUE.getQueueName());
      queueInfo.setCapacity(100.0f);
      queueInfo.setMaximumCapacity(100.0f);
      queueInfo.setChildQueues(new ArrayList<QueueInfo>());

      if (includeApplications) {
        queueInfo.setApplications(getApplications());
      } else {
        queueInfo.setApplications(
            new ArrayList<org.apache.hadoop.yarn.api.records.ApplicationReport>());
      }
      return queueInfo;
    }

    @Override
    public Map<QueueACL, AccessControlList> getQueueAcls() {
      Map<QueueACL, AccessControlList> acls =
        new HashMap<QueueACL, AccessControlList>();
      for (QueueACL acl : QueueACL.values()) {
        acls.put(acl, new AccessControlList("*"));
      }
      return acls;
    }

    @Override
    public List<QueueUserACLInfo> getQueueUserAclInfo(
        UserGroupInformation unused) {
      QueueUserACLInfo queueUserAclInfo = 
        recordFactory.newRecordInstance(QueueUserACLInfo.class);
      queueUserAclInfo.setQueueName(DEFAULT_QUEUE_NAME);
      queueUserAclInfo.setUserAcls(Arrays.asList(QueueACL.values()));
      return Collections.singletonList(queueUserAclInfo);
    }
  };

  public FifoScheduler() {
  }

  public FifoScheduler(ClusterTracker clusterTracker) {
    this.clusterTracker = clusterTracker;
    if (clusterTracker != null) {
      this.clusterTracker.addListener(this);
    }
  }
  
  @Override
  public Resource getMinimumResourceCapability() {
    return minimumAllocation;
  }

  @Override
  public Resource getMaximumResourceCapability() {
    return maximumAllocation;
  }

  @Override
  public synchronized void reinitialize(Configuration conf,
      ContainerTokenSecretManager containerTokenSecretManager, 
      ClusterTracker clusterTracker) 
  throws IOException 
  {
    this.conf = conf;
    this.containerTokenSecretManager = containerTokenSecretManager;
    this.clusterTracker = clusterTracker;
    if (clusterTracker != null) this.clusterTracker.addListener(this);
    this.minimumAllocation = 
      Resources.createResource(conf.getInt(MINIMUM_ALLOCATION, MINIMUM_MEMORY));
    this.maximumAllocation = 
      Resources.createResource(conf.getInt(MAXIMUM_ALLOCATION, MAXIMUM_MEMORY));
  }

  @Override
  public synchronized Allocation allocate(ApplicationId applicationId,
      List<ResourceRequest> ask, List<Container> release) 
      throws IOException {
    Application application = getApplication(applicationId);
    if (application == null) {
      LOG.error("Calling allocate on removed " +
          "or non existant application " + applicationId);
      return new Allocation(EMPTY_CONTAINER_LIST, Resources.none()); 
    }

    // Sanity check
    normalizeRequests(ask);
    
    List<Container> allocatedContainers = null;
    Resource limit = null;
    synchronized (application) {

      LOG.debug("allocate: pre-update" +
          " applicationId=" + applicationId + 
          " application=" + application);
      application.showRequests();

      // Update application requests
      application.updateResourceRequests(ask);

      // Release containers
      releaseContainers(application, release);

      LOG.debug("allocate: post-update" +
          " applicationId=" + applicationId + 
          " application=" + application);
      application.showRequests();

      allocatedContainers = application.acquire();
      limit = application.getHeadroom();
      LOG.debug("allocate:" +
          " applicationId=" + applicationId + 
          " #ask=" + ask.size() + 
          " #release=" + release.size() +
          " #allocatedContainers=" + allocatedContainers.size() + 
          " limit=" + limit);
    }
    
    return new Allocation(allocatedContainers, limit);
  }

  private void releaseContainers(Application application, List<Container> release) {
    application.releaseContainers(release);
    for (Container container : release) {
      releaseContainer(application.getApplicationId(), container);
    }
  }

  private void normalizeRequests(List<ResourceRequest> asks) {
    for (ResourceRequest ask : asks) {
      normalizeRequest(ask);
    }
  }

  private void normalizeRequest(ResourceRequest ask) {
    int memory = ask.getCapability().getMemory();
    // FIXME: TestApplicationCleanup is relying on unnormalized behavior.
    //ask.capability.memory = MINIMUM_MEMORY *
    memory = MINIMUM_MEMORY *
    ((memory/MINIMUM_MEMORY) + (memory%MINIMUM_MEMORY > 0 ? 1 : 0));
  }

  private synchronized Application getApplication(ApplicationId applicationId) {
    return applications.get(applicationId);
  }

  @Override
  public synchronized void addApplication(ApplicationId applicationId, ApplicationMaster master,
      String user, String unusedQueue, Priority unusedPriority, ApplicationStore appStore) 
  throws IOException {
    applications.put(applicationId, 
        new Application(applicationId, master, DEFAULT_QUEUE, user, appStore));
    metrics.submitApp(user);
    LOG.info("Application Submission: " + applicationId.getId() + " from " + user + 
        ", currently active: " + applications.size());
  }

  @Override
  public synchronized void doneApplication(ApplicationId applicationId, boolean finishApplication)
  throws IOException {
    Application application = getApplication(applicationId);
    if (application == null) {
      throw new IOException("Unknown application " + applicationId + 
      " has completed!");
    }

    // Release current containers
    releaseContainers(application, application.getCurrentContainers());
    
    // Clean up pending requests, metrics etc.
    application.stop();
    
    if (finishApplication) {
      // Let the cluster know that the applications are done
      finishedApplication(applicationId, 
          application.getAllNodesForApplication());
      // Remove the application
      applications.remove(applicationId);
    }
  }

  /**
   * Heart of the scheduler...
   * 
   * @param node node on which resources are available to be allocated
   */
  private synchronized void assignContainers(NodeInfo node) {
    LOG.debug("assignContainers:" +
        " node=" + node.getNodeAddress() + 
        " #applications=" + applications.size());

    // Try to assign containers to applications in fifo order
    for (Map.Entry<ApplicationId, Application> e : applications.entrySet()) {
      Application application = e.getValue();
      LOG.debug("pre-assignContainers");
      application.showRequests();
      synchronized (application) {
        for (Priority priority : application.getPriorities()) {
          int maxContainers = 
            getMaxAllocatableContainers(application, priority, node, 
                NodeType.OFF_SWITCH); 
          // Ensure the application needs containers of this priority
          if (maxContainers > 0) {
            int assignedContainers = 
              assignContainersOnNode(node, application, priority);
            // Do not assign out of order w.r.t priorities
            if (assignedContainers == 0) {
              break;
            }
          }
        }
      }
      
      application.setAvailableResourceLimit(clusterResource);
      
      LOG.debug("post-assignContainers");
      application.showRequests();

      // Done
      if (Resources.lessThan(node.getAvailableResource(), minimumAllocation)) {
        return;
      }
    }
  }

  private int getMaxAllocatableContainers(Application application,
      Priority priority, NodeInfo node, NodeType type) {
    ResourceRequest offSwitchRequest = 
      application.getResourceRequest(priority, NodeManager.ANY);
    int maxContainers = offSwitchRequest.getNumContainers();

    if (type == NodeType.OFF_SWITCH) {
      return maxContainers;
    }

    if (type == NodeType.RACK_LOCAL) {
      ResourceRequest rackLocalRequest = 
        application.getResourceRequest(priority, node.getRackName());
      if (rackLocalRequest == null) {
        return maxContainers;
      }

      maxContainers = Math.min(maxContainers, rackLocalRequest.getNumContainers());
    }

    if (type == NodeType.DATA_LOCAL) {
      ResourceRequest nodeLocalRequest = 
        application.getResourceRequest(priority, node.getNodeAddress());
      if (nodeLocalRequest != null) {
        maxContainers = Math.min(maxContainers, nodeLocalRequest.getNumContainers());
      }
    }

    return maxContainers;
  }


  private int assignContainersOnNode(NodeInfo node, 
      Application application, Priority priority 
  ) {
    // Data-local
    int nodeLocalContainers = 
      assignNodeLocalContainers(node, application, priority); 

    // Rack-local
    int rackLocalContainers = 
      assignRackLocalContainers(node, application, priority);

    // Off-switch
    int offSwitchContainers =
      assignOffSwitchContainers(node, application, priority);


    LOG.debug("assignContainersOnNode:" +
        " node=" + node.getNodeAddress() + 
        " application=" + application.getApplicationId().getId() +
        " priority=" + priority.getPriority() + 
        " #assigned=" + 
        (nodeLocalContainers + rackLocalContainers + offSwitchContainers));


    return (nodeLocalContainers + rackLocalContainers + offSwitchContainers);
  }

  private int assignNodeLocalContainers(NodeInfo node, 
      Application application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getNodeAddress());
    if (request != null) {
      int assignableContainers = 
        Math.min(
            getMaxAllocatableContainers(application, priority, node, 
                NodeType.DATA_LOCAL), 
                request.getNumContainers());
      assignedContainers = 
        assignContainers(node, application, priority, 
            assignableContainers, request, NodeType.DATA_LOCAL);
    }
    return assignedContainers;
  }

  private int assignRackLocalContainers(NodeInfo node, 
      Application application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getRackName());
    if (request != null) {
      int assignableContainers = 
        Math.min(
            getMaxAllocatableContainers(application, priority, node, 
                NodeType.RACK_LOCAL), 
                request.getNumContainers());
      assignedContainers = 
        assignContainers(node, application, priority, 
            assignableContainers, request, NodeType.RACK_LOCAL);
    }
    return assignedContainers;
  }

  private int assignOffSwitchContainers(NodeInfo node, 
      Application application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, NodeManager.ANY);
    if (request != null) {
      assignedContainers = 
        assignContainers(node, application, priority, 
            request.getNumContainers(), request, NodeType.OFF_SWITCH);
    }
    return assignedContainers;
  }

  private int assignContainers(NodeInfo node, Application application, 
      Priority priority, int assignableContainers, 
      ResourceRequest request, NodeType type) {
    LOG.debug("assignContainers:" +
        " node=" + node.getNodeAddress() + 
        " application=" + application.getApplicationId().getId() + 
        " priority=" + priority.getPriority() + 
        " assignableContainers=" + assignableContainers +
        " request=" + request + " type=" + type);
    Resource capability = request.getCapability();

    int availableContainers = 
      node.getAvailableResource().getMemory() / capability.getMemory(); // TODO: A buggy
    // application
    // with this
    // zero would
    // crash the
    // scheduler.
    int assignedContainers = 
      Math.min(assignableContainers, availableContainers);

    if (assignedContainers > 0) {
      List<Container> containers =
        new ArrayList<Container>(assignedContainers);
      for (int i=0; i < assignedContainers; ++i) {
        Container container =
            BuilderUtils.newContainer(recordFactory,
                application.getApplicationId(),
                application.getNewContainerId(), node.getNodeAddress(),
                node.getHttpAddress(), capability);
        // If security is enabled, send the container-tokens too.
        if (UserGroupInformation.isSecurityEnabled()) {
          ContainerToken containerToken =
              recordFactory.newRecordInstance(ContainerToken.class);
          ContainerTokenIdentifier tokenidentifier =
            new ContainerTokenIdentifier(container.getId(),
                container.getContainerManagerAddress(), container.getResource());
          containerToken.setIdentifier(
              ByteBuffer.wrap(tokenidentifier.getBytes()));
          containerToken.setKind(ContainerTokenIdentifier.KIND.toString());
          containerToken.setPassword(
              ByteBuffer.wrap(containerTokenSecretManager
                  .createPassword(tokenidentifier)));
          containerToken.setService(container.getContainerManagerAddress());
          container.setContainerToken(containerToken);
        }
        containers.add(container);
      }
      application.allocate(type, node, priority, request, containers);
      addAllocatedContainers(node, application.getApplicationId(), containers);
      Resources.addTo(usedResource,
          Resources.multiply(capability, assignedContainers));
    }
    return assignedContainers;
  }

  private synchronized void killContainers(List<Container> containers) {
    for (Container container : containers) {
      container.setState(ContainerState.COMPLETE);
    }
    applicationCompletedContainers(containers);
  }
  
  private synchronized void applicationCompletedContainers(
      List<Container> completedContainers) {
    for (Container c: completedContainers) {
      Application app = applications.get(c.getId().getAppId());
      /** this is possible, since an application can be removed from scheduler but
       * the nodemanger is just updating about a completed container.
       */
      if (app != null) {
        app.completedContainer(c, c.getResource());
      }
    }
  }

  private List<Container> getCompletedContainers(Map<String, List<Container>> allContainers) {
    if (allContainers == null) {
      return new ArrayList<Container>();
    }
    List<Container> completedContainers = new ArrayList<Container>();
    // Iterate through the running containers and update their status
    for (Map.Entry<String, List<Container>> e : 
      allContainers.entrySet()) {
      for (Container c: e.getValue()) {
        if (c.getState() == ContainerState.COMPLETE) {
          completedContainers.add(c);
        }
      }
    }
    return completedContainers;
  }

  @Override
  public synchronized void nodeUpdate(NodeInfo node, 
      Map<String,List<Container>> containers ) {

    applicationCompletedContainers(getCompletedContainers(containers));
    LOG.info("Node heartbeat " + node.getNodeID() + " resource = " + node.getAvailableResource());
    if (Resources.greaterThanOrEqual(node.getAvailableResource(),
        minimumAllocation)) {
      assignContainers(node);
    }
    metrics.setAvailableResourcesToQueue(
        Resources.subtract(clusterResource, usedResource));
    LOG.info("Node after allocation " + node.getNodeID() + " resource = "
        + node.getAvailableResource());

    // TODO: Add the list of containers to be preempted when we support
  }  

  @Override
  public synchronized void handle(ASMEvent<ApplicationTrackerEventType> event) {
    switch(event.getType()) {
    case ADD:
      /**
       * ignore add since its called syncronously from applications manager.
       */
      break;
    case REMOVE:
      try {
        doneApplication(event.getApplication().getApplicationID(), true);
      } catch(IOException ie) {
        LOG.error("Unable to remove application " + event.getApplication().getApplicationID(), ie);
      }
      break;  
    case EXPIRE:
      try {
        doneApplication(event.getApplication().getApplicationID(), false);
      } catch(IOException ie) {
        LOG.error("Unable to remove application " + event.getApplication().getApplicationID(), ie);
      }
      break;
    }
  }


  private Resource clusterResource = recordFactory.newRecordInstance(Resource.class);
  private Resource usedResource = recordFactory.newRecordInstance(Resource.class);

  public synchronized Resource getClusterResource() {
    return clusterResource;
  }

  @Override
  public synchronized void removeNode(NodeInfo nodeInfo) {
    Resources.subtractFrom(clusterResource, nodeInfo.getTotalCapability());
    killContainers(nodeInfo.getRunningContainers());
  }


  @Override
  public QueueInfo getQueueInfo(String queueName, boolean includeApplications,
      boolean includeChildQueues, boolean recursive) {
    return DEFAULT_QUEUE.getQueueInfo(includeApplications, false, false);
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    return DEFAULT_QUEUE.getQueueUserAclInfo(null); 
  }

  private synchronized List<org.apache.hadoop.yarn.api.records.ApplicationReport> 
  getApplications() {
    List<org.apache.hadoop.yarn.api.records.ApplicationReport> applications = 
      new ArrayList<org.apache.hadoop.yarn.api.records.ApplicationReport>();
    for (Application application : this.applications.values()) {
      applications.add(application.getApplicationInfo());
    }
    return applications;
  }

  @Override
  public synchronized void addNode(NodeInfo nodeManager) {
    Resources.addTo(clusterResource, nodeManager.getTotalCapability());
  }

  public synchronized void releaseContainer(ApplicationId applicationId, 
      Container container) {
    // Reap containers
    LOG.info("Application " + applicationId + " released container " +
        container.getId());
    clusterTracker.releaseContainer(container);
  }

  public synchronized void addAllocatedContainers(NodeInfo nodeInfo, 
      ApplicationId applicationId, List<Container> containers) {
    nodeInfo.allocateContainer(applicationId, containers);
  }

  public synchronized void finishedApplication(ApplicationId applicationId,
      List<NodeInfo> nodesToNotify) {
    clusterTracker.finishedApplication(applicationId, nodesToNotify);
  }

  @Override
  public void recover(RMState state) {
    for (Map.Entry<ApplicationId, ApplicationInfo> entry: state.getStoredApplications().entrySet()) {
      ApplicationId appId = entry.getKey();
      ApplicationInfo appInfo = entry.getValue();
      Application app = applications.get(appId);
      app.allocate(appInfo.getContainers());
    }
  }
}
