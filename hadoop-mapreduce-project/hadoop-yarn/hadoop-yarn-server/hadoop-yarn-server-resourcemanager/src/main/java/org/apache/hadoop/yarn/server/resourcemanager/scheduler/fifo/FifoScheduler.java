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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.Lock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.BuilderUtils;

@LimitedPrivate("yarn")
@Evolving
public class FifoScheduler implements ResourceScheduler {

  private static final Log LOG = LogFactory.getLog(FifoScheduler.class);

  private static final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  Configuration conf;
  private ContainerTokenSecretManager containerTokenSecretManager;

  private final static Container[] EMPTY_CONTAINER_ARRAY = new Container[] {};
  private final static List<Container> EMPTY_CONTAINER_LIST = Arrays.asList(EMPTY_CONTAINER_ARRAY);
  private RMContext rmContext;

  private Map<NodeId, SchedulerNode> nodes = new ConcurrentHashMap<NodeId, SchedulerNode>();

  private static final int MINIMUM_MEMORY = 1024;

  private static final String FIFO_PREFIX =  "yarn.scheduler.fifo.";
  @Private
  public static final String MINIMUM_ALLOCATION = 
    FIFO_PREFIX + "minimum-allocation-mb";

  private static final int MAXIMUM_MEMORY = 10240;

  @Private
  public static final String MAXIMUM_ALLOCATION = 
    FIFO_PREFIX + "maximum-allocation-mb";

  private boolean initialized;
  private Resource minimumAllocation;
  private Resource maximumAllocation;

  private Map<ApplicationAttemptId, SchedulerApp> applications
      = new TreeMap<ApplicationAttemptId, SchedulerApp>();

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
    public QueueInfo getQueueInfo( 
        boolean includeChildQueues, boolean recursive) {
      QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
      queueInfo.setQueueName(DEFAULT_QUEUE.getQueueName());
      queueInfo.setCapacity(100.0f);
      queueInfo.setMaximumCapacity(100.0f);
      queueInfo.setChildQueues(new ArrayList<QueueInfo>());
      queueInfo.setQueueState(QueueState.RUNNING);
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

  public synchronized Resource getUsedResource(NodeId nodeId) {
    return getNode(nodeId).getUsedResource();
  }

  public synchronized Resource getAvailableResource(NodeId nodeId) {
    return getNode(nodeId).getAvailableResource();
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
      RMContext rmContext) 
  throws IOException 
  {
    if (!this.initialized) {
      this.conf = conf;
      this.containerTokenSecretManager = containerTokenSecretManager;
      this.rmContext = rmContext;
      this.minimumAllocation = 
        Resources.createResource(conf.getInt(MINIMUM_ALLOCATION, MINIMUM_MEMORY));
      this.maximumAllocation = 
        Resources.createResource(conf.getInt(MAXIMUM_ALLOCATION, MAXIMUM_MEMORY));
      this.initialized = true;
    } else {
      this.conf = conf;
    }
  }

  private static final Allocation EMPTY_ALLOCATION = 
      new Allocation(EMPTY_CONTAINER_LIST, Resources.createResource(0));
  @Override
  public Allocation allocate(
      ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask,
      List<ContainerId> release) {
    SchedulerApp application = getApplication(applicationAttemptId);
    if (application == null) {
      LOG.error("Calling allocate on removed " +
          "or non existant application " + applicationAttemptId);
      return EMPTY_ALLOCATION;
    }

    // Sanity check
    normalizeRequests(ask);

    // Release containers
    for (ContainerId releasedContainer : release) {
      RMContainer rmContainer = getRMContainer(releasedContainer);
      if (rmContainer == null) {
         RMAuditLogger.logFailure(application.getUser(),
             AuditConstants.RELEASE_CONTAINER, 
             "Unauthorized access or invalid container", "FifoScheduler", 
             "Trying to release container not owned by app or with invalid id",
             application.getApplicationId(), releasedContainer);
      }
      containerCompleted(rmContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              releasedContainer, 
              SchedulerUtils.RELEASED_CONTAINER),
          RMContainerEventType.RELEASED);
    }

    if (!ask.isEmpty()) {
      LOG.debug("allocate: pre-update" +
          " applicationId=" + applicationAttemptId + 
          " application=" + application);
      application.showRequests();

      // Update application requests
      application.updateResourceRequests(ask);

      LOG.debug("allocate: post-update" +
          " applicationId=" + applicationAttemptId + 
          " application=" + application);
      application.showRequests();

      LOG.debug("allocate:" +
          " applicationId=" + applicationAttemptId + 
          " #ask=" + ask.size());
    }

    return new Allocation(
        application.pullNewlyAllocatedContainers(), 
        application.getHeadroom());
  }

  private void normalizeRequests(List<ResourceRequest> asks) {
    for (ResourceRequest ask : asks) {
      normalizeRequest(ask);
    }
  }

  private void normalizeRequest(ResourceRequest ask) {
    int memory = ask.getCapability().getMemory();
    // FIXME: TestApplicationCleanup is relying on unnormalized behavior.
    memory = 
        MINIMUM_MEMORY * 
        ((memory/MINIMUM_MEMORY) + (memory%MINIMUM_MEMORY > 0 ? 1 : 0));
    ask.setCapability(Resources.createResource(memory));
  }

  private SchedulerApp getApplication(
      ApplicationAttemptId applicationAttemptId) {
    return applications.get(applicationAttemptId);
  }

  @Override
  public SchedulerAppReport getSchedulerAppInfo(
      ApplicationAttemptId applicationAttemptId) {
    SchedulerApp app = getApplication(applicationAttemptId);
    return app == null ? null : new SchedulerAppReport(app);
  }
  
  private SchedulerNode getNode(NodeId nodeId) {
    return nodes.get(nodeId);
  }
  
  @SuppressWarnings("unchecked")
  private synchronized void addApplication(ApplicationAttemptId appAttemptId,
      String user) {
    // TODO: Fix store
    SchedulerApp schedulerApp = 
        new SchedulerApp(appAttemptId, user, DEFAULT_QUEUE, 
            this.rmContext, null);
    applications.put(appAttemptId, schedulerApp);
    metrics.submitApp(user);
    LOG.info("Application Submission: " + appAttemptId.getApplicationId() + 
        " from " + user + ", currently active: " + applications.size());
    rmContext.getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(appAttemptId,
            RMAppAttemptEventType.APP_ACCEPTED));
  }

  private synchronized void doneApplication(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState)
      throws IOException {
    SchedulerApp application = getApplication(applicationAttemptId);
    if (application == null) {
      throw new IOException("Unknown application " + applicationAttemptId + 
      " has completed!");
    }

    // Kill all 'live' containers
    for (RMContainer container : application.getLiveContainers()) {
      containerCompleted(container, 
          SchedulerUtils.createAbnormalContainerStatus(
              container.getContainerId(), 
              SchedulerUtils.COMPLETED_APPLICATION),
          RMContainerEventType.KILL);
    }

    // Clean up pending requests, metrics etc.
    application.stop(rmAppAttemptFinalState);

    // Remove the application
    applications.remove(applicationAttemptId);
  }
  
  /**
   * Heart of the scheduler...
   * 
   * @param node node on which resources are available to be allocated
   */
  private void assignContainers(SchedulerNode node) {
    LOG.debug("assignContainers:" +
        " node=" + node.getRMNode().getNodeAddress() + 
        " #applications=" + applications.size());

    // Try to assign containers to applications in fifo order
    for (Map.Entry<ApplicationAttemptId, SchedulerApp> e : applications
        .entrySet()) {
      SchedulerApp application = e.getValue();
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

  private int getMaxAllocatableContainers(SchedulerApp application,
      Priority priority, SchedulerNode node, NodeType type) {
    ResourceRequest offSwitchRequest = 
      application.getResourceRequest(priority, SchedulerNode.ANY);
    int maxContainers = offSwitchRequest.getNumContainers();

    if (type == NodeType.OFF_SWITCH) {
      return maxContainers;
    }

    if (type == NodeType.RACK_LOCAL) {
      ResourceRequest rackLocalRequest = 
        application.getResourceRequest(priority, node.getRMNode().getRackName());
      if (rackLocalRequest == null) {
        return maxContainers;
      }

      maxContainers = Math.min(maxContainers, rackLocalRequest.getNumContainers());
    }

    if (type == NodeType.NODE_LOCAL) {
      ResourceRequest nodeLocalRequest = 
        application.getResourceRequest(priority, node.getRMNode().getNodeAddress());
      if (nodeLocalRequest != null) {
        maxContainers = Math.min(maxContainers, nodeLocalRequest.getNumContainers());
      }
    }

    return maxContainers;
  }


  private int assignContainersOnNode(SchedulerNode node, 
      SchedulerApp application, Priority priority 
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
        " node=" + node.getRMNode().getNodeAddress() + 
        " application=" + application.getApplicationId().getId() +
        " priority=" + priority.getPriority() + 
        " #assigned=" + 
        (nodeLocalContainers + rackLocalContainers + offSwitchContainers));


    return (nodeLocalContainers + rackLocalContainers + offSwitchContainers);
  }

  private int assignNodeLocalContainers(SchedulerNode node, 
      SchedulerApp application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getRMNode().getNodeAddress());
    if (request != null) {
      // Don't allocate on this node if we don't need containers on this rack
      ResourceRequest rackRequest =
          application.getResourceRequest(priority, 
              node.getRMNode().getRackName());
      if (rackRequest == null || rackRequest.getNumContainers() <= 0) {
        return 0;
      }
      
      int assignableContainers = 
        Math.min(
            getMaxAllocatableContainers(application, priority, node, 
                NodeType.NODE_LOCAL), 
                request.getNumContainers());
      assignedContainers = 
        assignContainer(node, application, priority, 
            assignableContainers, request, NodeType.NODE_LOCAL);
    }
    return assignedContainers;
  }

  private int assignRackLocalContainers(SchedulerNode node, 
      SchedulerApp application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getRMNode().getRackName());
    if (request != null) {
      // Don't allocate on this rack if the application doens't need containers
      ResourceRequest offSwitchRequest =
          application.getResourceRequest(priority, SchedulerNode.ANY);
      if (offSwitchRequest.getNumContainers() <= 0) {
        return 0;
      }
      
      int assignableContainers = 
        Math.min(
            getMaxAllocatableContainers(application, priority, node, 
                NodeType.RACK_LOCAL), 
                request.getNumContainers());
      assignedContainers = 
        assignContainer(node, application, priority, 
            assignableContainers, request, NodeType.RACK_LOCAL);
    }
    return assignedContainers;
  }

  private int assignOffSwitchContainers(SchedulerNode node, 
      SchedulerApp application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, SchedulerNode.ANY);
    if (request != null) {
      assignedContainers = 
        assignContainer(node, application, priority, 
            request.getNumContainers(), request, NodeType.OFF_SWITCH);
    }
    return assignedContainers;
  }

  private int assignContainer(SchedulerNode node, SchedulerApp application, 
      Priority priority, int assignableContainers, 
      ResourceRequest request, NodeType type) {
    LOG.debug("assignContainers:" +
        " node=" + node.getRMNode().getNodeAddress() + 
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
      for (int i=0; i < assignedContainers; ++i) {
        // Create the container
        Container container =
            BuilderUtils.newContainer(recordFactory,
                application.getApplicationAttemptId(),
                application.getNewContainerId(),
                node.getRMNode().getNodeID(),
                node.getRMNode().getHttpAddress(), 
                capability, priority);
        
        // If security is enabled, send the container-tokens too.
        if (UserGroupInformation.isSecurityEnabled()) {
          ContainerToken containerToken =
              recordFactory.newRecordInstance(ContainerToken.class);
          NodeId nodeId = container.getNodeId();
          ContainerTokenIdentifier tokenidentifier =
            new ContainerTokenIdentifier(container.getId(),
                nodeId.toString(), container.getResource());
          containerToken.setIdentifier(
              ByteBuffer.wrap(tokenidentifier.getBytes()));
          containerToken.setKind(ContainerTokenIdentifier.KIND.toString());
          containerToken.setPassword(
              ByteBuffer.wrap(containerTokenSecretManager
                  .createPassword(tokenidentifier)));
          // RPC layer client expects ip:port as service for tokens
          InetSocketAddress addr = NetUtils.createSocketAddr(
              nodeId.getHost(), nodeId.getPort());
          containerToken.setService(addr.getAddress().getHostAddress() + ":"
              + addr.getPort());
          container.setContainerToken(containerToken);
        }
        
        // Allocate!
        
        // Inform the application
        RMContainer rmContainer =
            application.allocate(type, node, priority, request, container);
        
        // Inform the node
        node.allocateContainer(application.getApplicationId(), 
            rmContainer);
      }
      
      // Update total usage
      Resources.addTo(usedResource,
          Resources.multiply(capability, assignedContainers));
    }
    
    return assignedContainers;
  }

  private synchronized void nodeUpdate(RMNode rmNode, 
      List<ContainerStatus> newlyLaunchedContainers,
      List<ContainerStatus> completedContainers) {
    SchedulerNode node = getNode(rmNode.getNodeID());
    
    // Processing the newly launched containers
    for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
      containerLaunchedOnNode(launchedContainer.getContainerId(), node);
    }

    // Process completed containers
    for (ContainerStatus completedContainer : completedContainers) {
      ContainerId containerId = completedContainer.getContainerId();
      LOG.debug("Container FINISHED: " + containerId);
      containerCompleted(getRMContainer(containerId), 
          completedContainer, RMContainerEventType.FINISHED);
    }

    if (Resources.greaterThanOrEqual(node.getAvailableResource(),
        minimumAllocation)) {
      LOG.info("Node heartbeat " + rmNode.getNodeID() + 
          " available resource = " + node.getAvailableResource());
      
      assignContainers(node);

      LOG.info("Node after allocation " + rmNode.getNodeID() + " resource = "
          + node.getAvailableResource());
    }
    
    metrics.setAvailableResourcesToQueue(
        Resources.subtract(clusterResource, usedResource));
  }  

  @Override
  public void handle(SchedulerEvent event) {
    switch(event.getType()) {
    case NODE_ADDED:
    {
      NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
      addNode(nodeAddedEvent.getAddedRMNode());
    }
    break;
    case NODE_REMOVED:
    {
      NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)event;
      removeNode(nodeRemovedEvent.getRemovedRMNode());
    }
    break;
    case NODE_UPDATE:
    {
      NodeUpdateSchedulerEvent nodeUpdatedEvent = 
      (NodeUpdateSchedulerEvent)event;
      nodeUpdate(nodeUpdatedEvent.getRMNode(), 
          nodeUpdatedEvent.getNewlyLaunchedContainers(),
          nodeUpdatedEvent.getCompletedContainers());
    }
    break;
    case APP_ADDED:
    {
      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
      addApplication(appAddedEvent.getApplicationAttemptId(), appAddedEvent
          .getUser());
    }
    break;
    case APP_REMOVED:
    {
      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
      try {
        doneApplication(appRemovedEvent.getApplicationAttemptID(),
            appRemovedEvent.getFinalAttemptState());
      } catch(IOException ie) {
        LOG.error("Unable to remove application "
            + appRemovedEvent.getApplicationAttemptID(), ie);
      }
    }
    break;
    case CONTAINER_EXPIRED:
    {
      ContainerExpiredSchedulerEvent containerExpiredEvent = 
          (ContainerExpiredSchedulerEvent) event;
      ContainerId containerid = containerExpiredEvent.getContainerId();
      containerCompleted(getRMContainer(containerid), 
          SchedulerUtils.createAbnormalContainerStatus(
              containerid, 
              SchedulerUtils.EXPIRED_CONTAINER),
          RMContainerEventType.EXPIRE);
    }
    break;
    default:
      LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }
  }

  private void containerLaunchedOnNode(ContainerId containerId, SchedulerNode node) {
    // Get the application for the finished container
    ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();
    SchedulerApp application = getApplication(applicationAttemptId);
    if (application == null) {
      LOG.info("Unknown application: " + applicationAttemptId + 
          " launched container " + containerId +
          " on node: " + node);
      return;
    }
    
    application.containerLaunchedOnNode(containerId);
  }

  @Lock(FifoScheduler.class)
  private synchronized void containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    if (rmContainer == null) {
      LOG.info("Null container completed...");
      return;
    }

    // Get the application for the finished container
    Container container = rmContainer.getContainer();
    ApplicationAttemptId applicationAttemptId = container.getId().getApplicationAttemptId();
    SchedulerApp application = getApplication(applicationAttemptId);
    
    // Get the node on which the container was allocated
    SchedulerNode node = getNode(container.getNodeId());
    
    if (application == null) {
      LOG.info("Unknown application: " + applicationAttemptId + 
          " released container " + container.getId() +
          " on node: " + node + 
          " with event: " + event);
      return;
    }

    // Inform the application
    application.containerCompleted(rmContainer, containerStatus, event);

    // Inform the node
    node.releaseContainer(container);

    LOG.info("Application " + applicationAttemptId + 
        " released container " + container.getId() +
        " on node: " + node + 
        " with event: " + event);
     
  }
  
  private Resource clusterResource = recordFactory.newRecordInstance(Resource.class);
  private Resource usedResource = recordFactory.newRecordInstance(Resource.class);

  private synchronized void removeNode(RMNode nodeInfo) {
    SchedulerNode node = getNode(nodeInfo.getNodeID());
    // Kill running containers
    for(RMContainer container : node.getRunningContainers()) {
      containerCompleted(container, 
          SchedulerUtils.createAbnormalContainerStatus(
              container.getContainerId(), 
              SchedulerUtils.LOST_CONTAINER),
              RMContainerEventType.KILL);
    }
    
    //Remove the node
    this.nodes.remove(nodeInfo.getNodeID());
    
    // Update cluster metrics
    Resources.subtractFrom(clusterResource, nodeInfo.getTotalCapability());
  }

  @Override
  public QueueInfo getQueueInfo(String queueName,
      boolean includeChildQueues, boolean recursive) {
    return DEFAULT_QUEUE.getQueueInfo(false, false);
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    return DEFAULT_QUEUE.getQueueUserAclInfo(null); 
  }

  private synchronized void addNode(RMNode nodeManager) {
    this.nodes.put(nodeManager.getNodeID(), new SchedulerNode(nodeManager));
    Resources.addTo(clusterResource, nodeManager.getTotalCapability());
  }

  @Override
  public void recover(RMState state) {
    // TODO fix recovery
//    for (Map.Entry<ApplicationId, ApplicationInfo> entry: state.getStoredApplications().entrySet()) {
//      ApplicationId appId = entry.getKey();
//      ApplicationInfo appInfo = entry.getValue();
//      SchedulerApp app = applications.get(appId);
//      app.allocate(appInfo.getContainers());
//    }
  }

  @Override
  public synchronized SchedulerNodeReport getNodeReport(NodeId nodeId) {
    SchedulerNode node = getNode(nodeId);
    return node == null ? null : new SchedulerNodeReport(node);
  }
  
  private RMContainer getRMContainer(ContainerId containerId) {
    SchedulerApp application = 
        getApplication(containerId.getApplicationAttemptId());
    return (application == null) ? null : application.getRMContainer(containerId);
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return DEFAULT_QUEUE.getMetrics();
  }

}
