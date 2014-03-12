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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.ContainersAndNMTokensAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class FifoScheduler extends AbstractYarnScheduler implements
    Configurable {

  private static final Log LOG = LogFactory.getLog(FifoScheduler.class);

  private static final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  Configuration conf;

  protected Map<NodeId, FiCaSchedulerNode> nodes = new ConcurrentHashMap<NodeId, FiCaSchedulerNode>();

  private boolean initialized;
  private Resource minimumAllocation;
  private Resource maximumAllocation;
  private boolean usePortForNodeName;

  private ActiveUsersManager activeUsersManager;

  private static final String DEFAULT_QUEUE_NAME = "default";
  private QueueMetrics metrics;
  
  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

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
      queueInfo.setCapacity(1.0f);
      if (clusterResource.getMemory() == 0) {
        queueInfo.setCurrentCapacity(0.0f);
      } else {
        queueInfo.setCurrentCapacity((float) usedResource.getMemory()
            / clusterResource.getMemory());
      }
      queueInfo.setMaximumCapacity(1.0f);
      queueInfo.setChildQueues(new ArrayList<QueueInfo>());
      queueInfo.setQueueState(QueueState.RUNNING);
      return queueInfo;
    }

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

    @Override
    public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
      return getQueueAcls().get(acl).isUserAllowed(user);
    }
    
    @Override
    public ActiveUsersManager getActiveUsersManager() {
      return activeUsersManager;
    }
  };

  @Override
  public synchronized void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  private void validateConf(Configuration conf) {
    // validate scheduler memory allocation setting
    int minMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem <= 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
        + "=" + minMem
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
        + "=" + maxMem + ", min and max should be greater than 0"
        + ", max should be no smaller than min.");
    }
  }
  
  @Override
  public synchronized Configuration getConf() {
    return conf;
  }

  @Override
  public Resource getMinimumResourceCapability() {
    return minimumAllocation;
  }

  @Override
  public int getNumClusterNodes() {
    return nodes.size();
  }
  
  @Override
  public Resource getMaximumResourceCapability() {
    return maximumAllocation;
  }

  @Override
  public synchronized void
      reinitialize(Configuration conf, RMContext rmContext) throws IOException
  {
    setConf(conf);
    if (!this.initialized) {
      validateConf(conf);
      this.rmContext = rmContext;
      //Use ConcurrentSkipListMap because applications need to be ordered
      this.applications =
          new ConcurrentSkipListMap<ApplicationId, SchedulerApplication>();
      this.minimumAllocation = 
        Resources.createResource(conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
      this.maximumAllocation = 
        Resources.createResource(conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));
      this.usePortForNodeName = conf.getBoolean(
          YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, 
          YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
      this.metrics = QueueMetrics.forQueue(DEFAULT_QUEUE_NAME, null, false,
          conf);
      this.activeUsersManager = new ActiveUsersManager(metrics);
      this.initialized = true;
    }
  }


  @Override
  public Allocation allocate(
      ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask,
      List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals) {
    FiCaSchedulerApp application = getApplicationAttempt(applicationAttemptId);
    if (application == null) {
      LOG.error("Calling allocate on removed " +
          "or non existant application " + applicationAttemptId);
      return EMPTY_ALLOCATION;
    }

    // Sanity check
    SchedulerUtils.normalizeRequests(ask, resourceCalculator, 
        clusterResource, minimumAllocation, maximumAllocation);

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

    synchronized (application) {

      // make sure we aren't stopping/removing the application
      // when the allocate comes in
      if (application.isStopped()) {
        LOG.info("Calling allocate on a stopped " +
            "application " + applicationAttemptId);
        return EMPTY_ALLOCATION;
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

      application.updateBlacklist(blacklistAdditions, blacklistRemovals);
      ContainersAndNMTokensAllocation allocation =
          application.pullNewlyAllocatedContainersAndNMTokens();
      return new Allocation(allocation.getContainerList(),
        application.getHeadroom(), null, null, null,
        allocation.getNMTokenList());
    }
  }

  @VisibleForTesting
  FiCaSchedulerApp getApplicationAttempt(ApplicationAttemptId applicationAttemptId) {
    SchedulerApplication app =
        applications.get(applicationAttemptId.getApplicationId());
    if (app != null) {
      return (FiCaSchedulerApp) app.getCurrentAppAttempt();
    }
    return null;
  }

  @Override
  public SchedulerAppReport getSchedulerAppInfo(
      ApplicationAttemptId applicationAttemptId) {
    FiCaSchedulerApp app = getApplicationAttempt(applicationAttemptId);
    return app == null ? null : new SchedulerAppReport(app);
  }
  
  @Override
  public ApplicationResourceUsageReport getAppResourceUsageReport(
      ApplicationAttemptId applicationAttemptId) {
    FiCaSchedulerApp app = getApplicationAttempt(applicationAttemptId);
    return app == null ? null : app.getResourceUsageReport();
  }
  
  private FiCaSchedulerNode getNode(NodeId nodeId) {
    return nodes.get(nodeId);
  }

  private synchronized void addApplication(ApplicationId applicationId,
      String queue, String user) {
    SchedulerApplication application =
        new SchedulerApplication(DEFAULT_QUEUE, user);
    applications.put(applicationId, application);
    metrics.submitApp(user);
    LOG.info("Accepted application " + applicationId + " from user: " + user
        + ", currently num of applications: " + applications.size());
    rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
  }

  private synchronized void
      addApplicationAttempt(ApplicationAttemptId appAttemptId,
          boolean transferStateFromPreviousAttempt) {
    SchedulerApplication application =
        applications.get(appAttemptId.getApplicationId());
    String user = application.getUser();
    // TODO: Fix store
    FiCaSchedulerApp schedulerApp =
        new FiCaSchedulerApp(appAttemptId, user, DEFAULT_QUEUE,
          activeUsersManager, this.rmContext);

    if (transferStateFromPreviousAttempt) {
      schedulerApp.transferStateFromPreviousAttempt(application
        .getCurrentAppAttempt());
    }
    application.setCurrentAppAttempt(schedulerApp);

    metrics.submitAppAttempt(user);
    LOG.info("Added Application Attempt " + appAttemptId
        + " to scheduler from user " + application.getUser());
    rmContext.getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(appAttemptId,
            RMAppAttemptEventType.ATTEMPT_ADDED));
  }

  private synchronized void doneApplication(ApplicationId applicationId,
      RMAppState finalState) {
    SchedulerApplication application = applications.get(applicationId);
    if (application == null){
      LOG.warn("Couldn't find application " + applicationId);
      return;
    }

    // Inform the activeUsersManager
    activeUsersManager.deactivateApplication(application.getUser(),
      applicationId);
    application.stop(finalState);
    applications.remove(applicationId);
  }

  private synchronized void doneApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers)
      throws IOException {
    FiCaSchedulerApp attempt = getApplicationAttempt(applicationAttemptId);
    SchedulerApplication application =
        applications.get(applicationAttemptId.getApplicationId());
    if (application == null || attempt == null) {
      throw new IOException("Unknown application " + applicationAttemptId + 
      " has completed!");
    }

    // Kill all 'live' containers
    for (RMContainer container : attempt.getLiveContainers()) {
      if (keepContainers
          && container.getState().equals(RMContainerState.RUNNING)) {
        // do not kill the running container in the case of work-preserving AM
        // restart.
        LOG.info("Skip killing " + container.getContainerId());
        continue;
      }
      containerCompleted(container,
        SchedulerUtils.createAbnormalContainerStatus(
          container.getContainerId(), SchedulerUtils.COMPLETED_APPLICATION),
        RMContainerEventType.KILL);
    }

    // Clean up pending requests, metrics etc.
    attempt.stop(rmAppAttemptFinalState);
  }
  
  /**
   * Heart of the scheduler...
   * 
   * @param node node on which resources are available to be allocated
   */
  private void assignContainers(FiCaSchedulerNode node) {
    LOG.debug("assignContainers:" +
        " node=" + node.getRMNode().getNodeAddress() + 
        " #applications=" + applications.size());

    // Try to assign containers to applications in fifo order
    for (Map.Entry<ApplicationId, SchedulerApplication> e : applications
        .entrySet()) {
      FiCaSchedulerApp application =
          (FiCaSchedulerApp) e.getValue().getCurrentAppAttempt();
      LOG.debug("pre-assignContainers");
      application.showRequests();
      synchronized (application) {
        // Check if this resource is on the blacklist
        if (SchedulerAppUtils.isBlacklisted(application, node, LOG)) {
          continue;
        }
        
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
      
      LOG.debug("post-assignContainers");
      application.showRequests();

      // Done
      if (Resources.lessThan(resourceCalculator, clusterResource,
              node.getAvailableResource(), minimumAllocation)) {
        break;
      }
    }

    // Update the applications' headroom to correctly take into
    // account the containers assigned in this update.
    for (SchedulerApplication application : applications.values()) {
      FiCaSchedulerApp attempt =
          (FiCaSchedulerApp) application.getCurrentAppAttempt();
      attempt.setHeadroom(Resources.subtract(clusterResource, usedResource));
    }
  }

  private int getMaxAllocatableContainers(FiCaSchedulerApp application,
      Priority priority, FiCaSchedulerNode node, NodeType type) {
    int maxContainers = 0;
    
    ResourceRequest offSwitchRequest = 
      application.getResourceRequest(priority, ResourceRequest.ANY);
    if (offSwitchRequest != null) {
      maxContainers = offSwitchRequest.getNumContainers();
    }

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


  private int assignContainersOnNode(FiCaSchedulerNode node, 
      FiCaSchedulerApp application, Priority priority 
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

  private int assignNodeLocalContainers(FiCaSchedulerNode node, 
      FiCaSchedulerApp application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getNodeName());
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

  private int assignRackLocalContainers(FiCaSchedulerNode node, 
      FiCaSchedulerApp application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getRMNode().getRackName());
    if (request != null) {
      // Don't allocate on this rack if the application doens't need containers
      ResourceRequest offSwitchRequest =
          application.getResourceRequest(priority, ResourceRequest.ANY);
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

  private int assignOffSwitchContainers(FiCaSchedulerNode node, 
      FiCaSchedulerApp application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, ResourceRequest.ANY);
    if (request != null) {
      assignedContainers = 
        assignContainer(node, application, priority, 
            request.getNumContainers(), request, NodeType.OFF_SWITCH);
    }
    return assignedContainers;
  }

  private int assignContainer(FiCaSchedulerNode node, FiCaSchedulerApp application, 
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

        NodeId nodeId = node.getRMNode().getNodeID();
        ContainerId containerId = BuilderUtils.newContainerId(application
            .getApplicationAttemptId(), application.getNewContainerId());

        // Create the container
        Container container =
            BuilderUtils.newContainer(containerId, nodeId, node.getRMNode()
              .getHttpAddress(), capability, priority, null);
        
        // Allocate!
        
        // Inform the application
        RMContainer rmContainer =
            application.allocate(type, node, priority, request, container);
        
        // Inform the node
        node.allocateContainer(application.getApplicationId(), 
            rmContainer);

        // Update usage for this container
        Resources.addTo(usedResource, capability);
      }

    }
    
    return assignedContainers;
  }

  private synchronized void nodeUpdate(RMNode rmNode) {
    FiCaSchedulerNode node = getNode(rmNode.getNodeID());
    
    // Update resource if any change
    SchedulerUtils.updateResourceIfChanged(node, rmNode, clusterResource, LOG);
    
    List<UpdatedContainerInfo> containerInfoList = rmNode.pullContainerUpdates();
    List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
    List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
    for(UpdatedContainerInfo containerInfo : containerInfoList) {
      newlyLaunchedContainers.addAll(containerInfo.getNewlyLaunchedContainers());
      completedContainers.addAll(containerInfo.getCompletedContainers());
    }
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

    if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource,
            node.getAvailableResource(),minimumAllocation)) {
      LOG.debug("Node heartbeat " + rmNode.getNodeID() + 
          " available resource = " + node.getAvailableResource());

      assignContainers(node);

      LOG.debug("Node after allocation " + rmNode.getNodeID() + " resource = "
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
      nodeUpdate(nodeUpdatedEvent.getRMNode());
    }
    break;
    case APP_ADDED:
    {
      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
      addApplication(appAddedEvent.getApplicationId(),
        appAddedEvent.getQueue(), appAddedEvent.getUser());
    }
    break;
    case APP_REMOVED:
    {
      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
      doneApplication(appRemovedEvent.getApplicationID(),
        appRemovedEvent.getFinalState());
    }
    break;
    case APP_ATTEMPT_ADDED:
    {
      AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
          (AppAttemptAddedSchedulerEvent) event;
      addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
        appAttemptAddedEvent.getTransferStateFromPreviousAttempt());
    }
    break;
    case APP_ATTEMPT_REMOVED:
    {
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          (AppAttemptRemovedSchedulerEvent) event;
      try {
        doneApplicationAttempt(
          appAttemptRemovedEvent.getApplicationAttemptID(),
          appAttemptRemovedEvent.getFinalAttemptState(),
          appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
      } catch(IOException ie) {
        LOG.error("Unable to remove application "
            + appAttemptRemovedEvent.getApplicationAttemptID(), ie);
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

  private void containerLaunchedOnNode(ContainerId containerId, FiCaSchedulerNode node) {
    // Get the application for the finished container
    FiCaSchedulerApp application = getCurrentAttemptForContainer(containerId);
    if (application == null) {
      LOG.info("Unknown application "
          + containerId.getApplicationAttemptId().getApplicationId()
          + " launched container " + containerId + " on node: " + node);
      // Some unknown container sneaked into the system. Kill it.
      this.rmContext.getDispatcher().getEventHandler()
        .handle(new RMNodeCleanContainerEvent(node.getNodeID(), containerId));

      return;
    }
    
    application.containerLaunchedOnNode(containerId, node.getNodeID());
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
    FiCaSchedulerApp application =
        getCurrentAttemptForContainer(container.getId());
    ApplicationId appId =
        container.getId().getApplicationAttemptId().getApplicationId();
    
    // Get the node on which the container was allocated
    FiCaSchedulerNode node = getNode(container.getNodeId());
    
    if (application == null) {
      LOG.info("Unknown application: " + appId + 
          " released container " + container.getId() +
          " on node: " + node + 
          " with event: " + event);
      return;
    }

    // Inform the application
    application.containerCompleted(rmContainer, containerStatus, event);

    // Inform the node
    node.releaseContainer(container);
    
    // Update total usage
    Resources.subtractFrom(usedResource, container.getResource());

    LOG.info("Application attempt " + application.getApplicationAttemptId() + 
        " released container " + container.getId() +
        " on node: " + node + 
        " with event: " + event);
     
  }
  
  private Resource clusterResource = recordFactory.newRecordInstance(Resource.class);
  private Resource usedResource = recordFactory.newRecordInstance(Resource.class);

  private synchronized void removeNode(RMNode nodeInfo) {
    FiCaSchedulerNode node = getNode(nodeInfo.getNodeID());
    if (node == null) {
      return;
    }
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
    Resources.subtractFrom(clusterResource, node.getRMNode().getTotalCapability());
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
    this.nodes.put(nodeManager.getNodeID(), new FiCaSchedulerNode(nodeManager,
        usePortForNodeName));
    Resources.addTo(clusterResource, nodeManager.getTotalCapability());
  }

  @Override
  public void recover(RMState state) {
    // NOT IMPLEMENTED
  }

  @Override
  public synchronized SchedulerNodeReport getNodeReport(NodeId nodeId) {
    FiCaSchedulerNode node = getNode(nodeId);
    return node == null ? null : new SchedulerNodeReport(node);
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    FiCaSchedulerApp attempt = getCurrentAttemptForContainer(containerId);
    return (attempt == null) ? null : attempt.getRMContainer(containerId);
  }

  private FiCaSchedulerApp getCurrentAttemptForContainer(
      ContainerId containerId) {
    SchedulerApplication app =
        applications.get(containerId.getApplicationAttemptId()
          .getApplicationId());
    if (app != null) {
      return (FiCaSchedulerApp) app.getCurrentAppAttempt();
    }
    return null;
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return DEFAULT_QUEUE.getMetrics();
  }

  @Override
  public synchronized boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName) {
    return DEFAULT_QUEUE.hasAccess(acl, callerUGI);
  }
  
  @Override
  public synchronized List<ApplicationAttemptId> getAppsInQueue(String queueName) {
    if (queueName.equals(DEFAULT_QUEUE.getQueueName())) {
      List<ApplicationAttemptId> attempts = new ArrayList<ApplicationAttemptId>(
          applications.size());
      for (SchedulerApplication app : applications.values()) {
        attempts.add(app.getCurrentAppAttempt().getApplicationAttemptId());
      }
      return attempts;
    } else {
      return null;
    }
  }

}
