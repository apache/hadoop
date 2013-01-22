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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.Lock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class CapacityScheduler 
implements ResourceScheduler, CapacitySchedulerContext, Configurable {

  private static final Log LOG = LogFactory.getLog(CapacityScheduler.class);

  private CSQueue root;

  private final static List<Container> EMPTY_CONTAINER_LIST = 
    new ArrayList<Container>();

  static final Comparator<CSQueue> queueComparator = new Comparator<CSQueue>() {
    @Override
    public int compare(CSQueue q1, CSQueue q2) {
      if (q1.getUsedCapacity() < q2.getUsedCapacity()) {
        return -1;
      } else if (q1.getUsedCapacity() > q2.getUsedCapacity()) {
        return 1;
      }

      return q1.getQueuePath().compareTo(q2.getQueuePath());
    }
  };

  static final Comparator<FiCaSchedulerApp> applicationComparator = 
    new Comparator<FiCaSchedulerApp>() {
    @Override
    public int compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2) {
      return a1.getApplicationId().getId() - a2.getApplicationId().getId();
    }
  };

  @Override
  public void setConf(Configuration conf) {
      yarnConf = conf;
  }

  @Override
  public Configuration getConf() {
    return yarnConf;
  }

  private CapacitySchedulerConfiguration conf;
  private Configuration yarnConf;
  private RMContext rmContext;

  private Map<String, CSQueue> queues = new ConcurrentHashMap<String, CSQueue>();

  private Map<NodeId, FiCaSchedulerNode> nodes = 
      new ConcurrentHashMap<NodeId, FiCaSchedulerNode>();

  private Resource clusterResource = 
    RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Resource.class);
  private int numNodeManagers = 0;

  private Resource minimumAllocation;
  private Resource maximumAllocation;

  private Map<ApplicationAttemptId, FiCaSchedulerApp> applications = 
      new ConcurrentHashMap<ApplicationAttemptId, FiCaSchedulerApp>();

  private boolean initialized = false;

  private ResourceCalculator calculator;
  
  public CapacityScheduler() {}

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return root.getMetrics();
  }

  public CSQueue getRootQueue() {
    return root;
  }
  
  @Override
  public CapacitySchedulerConfiguration getConfiguration() {
    return conf;
  }

  @Override
  public RMContainerTokenSecretManager getContainerTokenSecretManager() {
    return this.rmContext.getContainerTokenSecretManager();
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
  public Comparator<FiCaSchedulerApp> getApplicationComparator() {
    return applicationComparator;
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return calculator;
  }

  @Override
  public Comparator<CSQueue> getQueueComparator() {
    return queueComparator;
  }

  @Override
  public synchronized int getNumClusterNodes() {
    return numNodeManagers;
  }

  @Override
  public RMContext getRMContext() {
    return this.rmContext;
  }

  @Override
  public Resource getClusterResources() {
    return clusterResource;
  }
  
  @Override
  public synchronized void
      reinitialize(Configuration conf, RMContext rmContext) throws IOException {
    if (!initialized) {
      this.conf = new CapacitySchedulerConfiguration(conf);
      
      this.minimumAllocation = this.conf.getMinimumAllocation();
      this.maximumAllocation = this.conf.getMaximumAllocation();
      this.calculator = this.conf.getResourceCalculator();

      this.rmContext = rmContext;
      
      initializeQueues(this.conf);
      
      initialized = true;
      LOG.info("Initialized CapacityScheduler with " +
          "calculator=" + getResourceCalculator().getClass() + ", " +
          "minimumAllocation=<" + getMinimumResourceCapability() + ">, " +
          "maximumAllocation=<" + getMaximumResourceCapability() + ">");
    } else {

      CapacitySchedulerConfiguration oldConf = this.conf; 
      this.conf = new CapacitySchedulerConfiguration(conf);
      try {
        LOG.info("Re-initializing queues...");
        reinitializeQueues(this.conf);
      } catch (Throwable t) {
        this.conf = oldConf;
        throw new IOException("Failed to re-init queues", t);
      }
    }
  }

  @Private
  public static final String ROOT_QUEUE = 
    CapacitySchedulerConfiguration.PREFIX + CapacitySchedulerConfiguration.ROOT;

  static class QueueHook {
    public CSQueue hook(CSQueue queue) {
      return queue;
    }
  }
  private static final QueueHook noop = new QueueHook();
  
  @Lock(CapacityScheduler.class)
  private void initializeQueues(CapacitySchedulerConfiguration conf)
    throws IOException {
    root = 
        parseQueue(this, conf, null, CapacitySchedulerConfiguration.ROOT, 
            queues, queues, noop);
    LOG.info("Initialized root queue " + root);
  }

  @Lock(CapacityScheduler.class)
  private void reinitializeQueues(CapacitySchedulerConfiguration conf) 
  throws IOException {
    // Parse new queues
    Map<String, CSQueue> newQueues = new HashMap<String, CSQueue>();
    CSQueue newRoot = 
        parseQueue(this, conf, null, CapacitySchedulerConfiguration.ROOT, 
            newQueues, queues, noop); 
    
    // Ensure all existing queues are still present
    validateExistingQueues(queues, newQueues);

    // Add new queues
    addNewQueues(queues, newQueues);
    
    // Re-configure queues
    root.reinitialize(newRoot, clusterResource);
  }

  /**
   * Ensure all existing queues are present. Queues cannot be deleted
   * @param queues existing queues
   * @param newQueues new queues
   */
  @Lock(CapacityScheduler.class)
  private void validateExistingQueues(
      Map<String, CSQueue> queues, Map<String, CSQueue> newQueues) 
  throws IOException {
    for (String queue : queues.keySet()) {
      if (!newQueues.containsKey(queue)) {
        throw new IOException(queue + " cannot be found during refresh!");
      }
    }
  }

  /**
   * Add the new queues (only) to our list of queues...
   * ... be careful, do not overwrite existing queues.
   * @param queues
   * @param newQueues
   */
  @Lock(CapacityScheduler.class)
  private void addNewQueues(
      Map<String, CSQueue> queues, Map<String, CSQueue> newQueues) 
  {
    for (Map.Entry<String, CSQueue> e : newQueues.entrySet()) {
      String queueName = e.getKey();
      CSQueue queue = e.getValue();
      if (!queues.containsKey(queueName)) {
        queues.put(queueName, queue);
      }
    }
  }
  
  @Lock(CapacityScheduler.class)
  static CSQueue parseQueue(
      CapacitySchedulerContext csContext, 
      CapacitySchedulerConfiguration conf, 
      CSQueue parent, String queueName, Map<String, CSQueue> queues,
      Map<String, CSQueue> oldQueues, 
      QueueHook hook) throws IOException {
    CSQueue queue;
    String[] childQueueNames = 
      conf.getQueues((parent == null) ? 
          queueName : (parent.getQueuePath()+"."+queueName));
    if (childQueueNames == null || childQueueNames.length == 0) {
      if (null == parent) {
        throw new IllegalStateException(
            "Queue configuration missing child queue names for " + queueName);
      }
      queue = 
          new LeafQueue(csContext, queueName, parent,oldQueues.get(queueName));
      
      // Used only for unit tests
      queue = hook.hook(queue);
    } else {
      ParentQueue parentQueue = 
        new ParentQueue(csContext, queueName, parent,oldQueues.get(queueName));

      // Used only for unit tests
      queue = hook.hook(parentQueue);
      
      List<CSQueue> childQueues = new ArrayList<CSQueue>();
      for (String childQueueName : childQueueNames) {
        CSQueue childQueue = 
          parseQueue(csContext, conf, queue, childQueueName, 
              queues, oldQueues, hook);
        childQueues.add(childQueue);
      }
      parentQueue.setChildQueues(childQueues);
    }

    if(queue instanceof LeafQueue == true && queues.containsKey(queueName)
      && queues.get(queueName) instanceof LeafQueue == true) {
      throw new IOException("Two leaf queues were named " + queueName
        + ". Leaf queue names must be distinct");
    }
    queues.put(queueName, queue);

    LOG.info("Initialized queue: " + queue);
    return queue;
  }

  synchronized CSQueue getQueue(String queueName) {
    return queues.get(queueName);
  }
  
  private synchronized void
      addApplication(ApplicationAttemptId applicationAttemptId,
          String queueName, String user) {

    // Sanity checks
    CSQueue queue = getQueue(queueName);
    if (queue == null) {
      String message = "Application " + applicationAttemptId + 
      " submitted by user " + user + " to unknown queue: " + queueName;
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptRejectedEvent(applicationAttemptId, message));
      return;
    }
    if (!(queue instanceof LeafQueue)) {
      String message = "Application " + applicationAttemptId + 
          " submitted by user " + user + " to non-leaf queue: " + queueName;
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptRejectedEvent(applicationAttemptId, message));
      return;
    }

    // TODO: Fix store
    FiCaSchedulerApp SchedulerApp = 
        new FiCaSchedulerApp(applicationAttemptId, user, queue, 
            queue.getActiveUsersManager(), rmContext);

    // Submit to the queue
    try {
      queue.submitApplication(SchedulerApp, user, queueName);
    } catch (AccessControlException ace) {
      LOG.info("Failed to submit application " + applicationAttemptId + 
          " to queue " + queueName + " from user " + user, ace);
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptRejectedEvent(applicationAttemptId, 
              ace.toString()));
      return;
    }

    applications.put(applicationAttemptId, SchedulerApp);

    LOG.info("Application Submission: " + applicationAttemptId + 
        ", user: " + user +
        " queue: " + queue +
        ", currently active: " + applications.size());

    rmContext.getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(applicationAttemptId,
            RMAppAttemptEventType.APP_ACCEPTED));
  }

  private synchronized void doneApplication(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState) {
    LOG.info("Application " + applicationAttemptId + " is done." +
    		" finalState=" + rmAppAttemptFinalState);
    
    FiCaSchedulerApp application = getApplication(applicationAttemptId);

    if (application == null) {
      //      throw new IOException("Unknown application " + applicationId + 
      //          " has completed!");
      LOG.info("Unknown application " + applicationAttemptId + " has completed!");
      return;
    }
    
    // Release all the running containers 
    for (RMContainer rmContainer : application.getLiveContainers()) {
      completedContainer(rmContainer, 
          SchedulerUtils.createAbnormalContainerStatus(
              rmContainer.getContainerId(), 
              SchedulerUtils.COMPLETED_APPLICATION), 
          RMContainerEventType.KILL);
    }
    
     // Release all reserved containers
    for (RMContainer rmContainer : application.getReservedContainers()) {
      completedContainer(rmContainer, 
          SchedulerUtils.createAbnormalContainerStatus(
              rmContainer.getContainerId(), 
              "Application Complete"), 
          RMContainerEventType.KILL);
    }
    
    // Clean up pending requests, metrics etc.
    application.stop(rmAppAttemptFinalState);
    
    // Inform the queue
    String queueName = application.getQueue().getQueueName();
    CSQueue queue = queues.get(queueName);
    if (!(queue instanceof LeafQueue)) {
      LOG.error("Cannot finish application " + "from non-leaf queue: "
          + queueName);
    } else {
      queue.finishApplication(application, queue.getQueueName());
    }
    
    // Remove from our data-structure
    applications.remove(applicationAttemptId);
  }

  private static final Allocation EMPTY_ALLOCATION = 
      new Allocation(EMPTY_CONTAINER_LIST, Resources.createResource(0, 0));

  @Override
  @Lock(Lock.NoLock.class)
  public Allocation allocate(ApplicationAttemptId applicationAttemptId,
      List<ResourceRequest> ask, List<ContainerId> release) {

    FiCaSchedulerApp application = getApplication(applicationAttemptId);
    if (application == null) {
      LOG.info("Calling allocate on removed " +
          "or non existant application " + applicationAttemptId);
      return EMPTY_ALLOCATION;
    }
    
    // Sanity check
    SchedulerUtils.normalizeRequests(
        ask, calculator, getClusterResources(), minimumAllocation);

    // Release containers
    for (ContainerId releasedContainerId : release) {
      RMContainer rmContainer = getRMContainer(releasedContainerId);
      if (rmContainer == null) {
         RMAuditLogger.logFailure(application.getUser(),
             AuditConstants.RELEASE_CONTAINER, 
             "Unauthorized access or invalid container", "CapacityScheduler",
             "Trying to release container not owned by app or with invalid id",
             application.getApplicationId(), releasedContainerId);
      }
      completedContainer(rmContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              releasedContainerId, 
              SchedulerUtils.RELEASED_CONTAINER),
          RMContainerEventType.RELEASED);
    }

    synchronized (application) {

      if (!ask.isEmpty()) {

        if(LOG.isDebugEnabled()) {
          LOG.debug("allocate: pre-update" +
            " applicationAttemptId=" + applicationAttemptId + 
            " application=" + application);
        }
        application.showRequests();
  
        // Update application requests
        application.updateResourceRequests(ask);
  
        LOG.debug("allocate: post-update");
        application.showRequests();
      }

      if(LOG.isDebugEnabled()) {
        LOG.debug("allocate:" +
          " applicationAttemptId=" + applicationAttemptId + 
          " #ask=" + ask.size());
      }

      return new Allocation(
          application.pullNewlyAllocatedContainers(), 
          application.getHeadroom());
    }
  }

  @Override
  @Lock(Lock.NoLock.class)
  public QueueInfo getQueueInfo(String queueName, 
      boolean includeChildQueues, boolean recursive) 
  throws IOException {
    CSQueue queue = null;

    synchronized (this) {
      queue = this.queues.get(queueName); 
    }

    if (queue == null) {
      throw new IOException("Unknown queue: " + queueName);
    }
    return queue.getQueueInfo(includeChildQueues, recursive);
  }

  @Override
  @Lock(Lock.NoLock.class)
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    UserGroupInformation user = null;
    try {
      user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      // should never happen
      return new ArrayList<QueueUserACLInfo>();
    }

    return root.getQueueUserAclInfo(user);
  }

  private synchronized void nodeUpdate(RMNode nm, 
      List<ContainerStatus> newlyLaunchedContainers,
      List<ContainerStatus> completedContainers) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("nodeUpdate: " + nm + " clusterResources: " + clusterResource);
    }
                  
    FiCaSchedulerNode node = getNode(nm.getNodeID());

    // Processing the newly launched containers
    for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
      containerLaunchedOnNode(launchedContainer.getContainerId(), node);
    }

    // Process completed containers
    for (ContainerStatus completedContainer : completedContainers) {
      ContainerId containerId = completedContainer.getContainerId();
      LOG.debug("Container FINISHED: " + containerId);
      completedContainer(getRMContainer(containerId), 
          completedContainer, RMContainerEventType.FINISHED);
    }

    // Now node data structures are upto date and ready for scheduling.
    if(LOG.isDebugEnabled()) {
      LOG.debug("Node being looked for scheduling " + nm
        + " availableResource: " + node.getAvailableResource());
    }

    // Assign new containers...
    // 1. Check for reserved applications
    // 2. Schedule if there are no reservations

    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      FiCaSchedulerApp reservedApplication = 
          getApplication(reservedContainer.getApplicationAttemptId());
      
      // Try to fulfill the reservation
      LOG.info("Trying to fulfill reservation for application " + 
          reservedApplication.getApplicationId() + " on node: " + nm);
      
      LeafQueue queue = ((LeafQueue)reservedApplication.getQueue());
      CSAssignment assignment = queue.assignContainers(clusterResource, node);
      
      RMContainer excessReservation = assignment.getExcessReservation();
      if (excessReservation != null) {
      Container container = excessReservation.getContainer();
      queue.completedContainer(
          clusterResource, assignment.getApplication(), node, 
          excessReservation, 
          SchedulerUtils.createAbnormalContainerStatus(
              container.getId(), 
              SchedulerUtils.UNRESERVED_CONTAINER), 
          RMContainerEventType.RELEASED);
      }

    }

    // Try to schedule more if there are no reservations to fulfill
    if (node.getReservedContainer() == null) {
      root.assignContainers(clusterResource, node);
    } else {
      LOG.info("Skipping scheduling since node " + nm + 
          " is reserved by application " + 
          node.getReservedContainer().getContainerId().getApplicationAttemptId()
          );
    }

  }

  private void containerLaunchedOnNode(ContainerId containerId, FiCaSchedulerNode node) {
    // Get the application for the finished container
    ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();
    FiCaSchedulerApp application = getApplication(applicationAttemptId);
    if (application == null) {
      LOG.info("Unknown application: " + applicationAttemptId + 
          " launched container " + containerId +
          " on node: " + node);
      this.rmContext.getDispatcher().getEventHandler()
        .handle(new RMNodeCleanContainerEvent(node.getNodeID(), containerId));
      return;
    }
    
    application.containerLaunchedOnNode(containerId, node.getNodeID());
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
      NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
      nodeUpdate(nodeUpdatedEvent.getRMNode(), 
          nodeUpdatedEvent.getNewlyLaunchedContainers(),
          nodeUpdatedEvent.getCompletedContainers());
    }
    break;
    case APP_ADDED:
    {
      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent)event;
      addApplication(appAddedEvent.getApplicationAttemptId(), appAddedEvent
          .getQueue(), appAddedEvent.getUser());
    }
    break;
    case APP_REMOVED:
    {
      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
      doneApplication(appRemovedEvent.getApplicationAttemptID(),
          appRemovedEvent.getFinalAttemptState());
    }
    break;
    case CONTAINER_EXPIRED:
    {
      ContainerExpiredSchedulerEvent containerExpiredEvent = 
          (ContainerExpiredSchedulerEvent) event;
      ContainerId containerId = containerExpiredEvent.getContainerId();
      completedContainer(getRMContainer(containerId), 
          SchedulerUtils.createAbnormalContainerStatus(
              containerId, 
              SchedulerUtils.EXPIRED_CONTAINER), 
          RMContainerEventType.EXPIRE);
    }
    break;
    default:
      LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }
  }

  private synchronized void addNode(RMNode nodeManager) {
    this.nodes.put(nodeManager.getNodeID(), new FiCaSchedulerNode(nodeManager));
    Resources.addTo(clusterResource, nodeManager.getTotalCapability());
    root.updateClusterResource(clusterResource);
    ++numNodeManagers;
    LOG.info("Added node " + nodeManager.getNodeAddress() + 
        " clusterResource: " + clusterResource);
  }

  private synchronized void removeNode(RMNode nodeInfo) {
    FiCaSchedulerNode node = this.nodes.get(nodeInfo.getNodeID());
    if (node == null) {
      return;
    }
    Resources.subtractFrom(clusterResource, node.getRMNode().getTotalCapability());
    root.updateClusterResource(clusterResource);
    --numNodeManagers;

    // Remove running containers
    List<RMContainer> runningContainers = node.getRunningContainers();
    for (RMContainer container : runningContainers) {
      completedContainer(container, 
          SchedulerUtils.createAbnormalContainerStatus(
              container.getContainerId(), 
              SchedulerUtils.LOST_CONTAINER), 
          RMContainerEventType.KILL);
    }
    
    // Remove reservations, if any
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      completedContainer(reservedContainer, 
          SchedulerUtils.createAbnormalContainerStatus(
              reservedContainer.getContainerId(), 
              SchedulerUtils.LOST_CONTAINER), 
          RMContainerEventType.KILL);
    }

    this.nodes.remove(nodeInfo.getNodeID());
    LOG.info("Removed node " + nodeInfo.getNodeAddress() + 
        " clusterResource: " + clusterResource);
  }
  
  @Lock(CapacityScheduler.class)
  private synchronized void completedContainer(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    if (rmContainer == null) {
      LOG.info("Null container completed...");
      return;
    }
    
    Container container = rmContainer.getContainer();
    
    // Get the application for the finished container
    ApplicationAttemptId applicationAttemptId = container.getId().getApplicationAttemptId();
    FiCaSchedulerApp application = getApplication(applicationAttemptId);
    if (application == null) {
      LOG.info("Container " + container + " of" +
      		" unknown application " + applicationAttemptId + 
          " completed with event " + event);
      return;
    }
    
    // Get the node on which the container was allocated
    FiCaSchedulerNode node = getNode(container.getNodeId());
    
    // Inform the queue
    LeafQueue queue = (LeafQueue)application.getQueue();
    queue.completedContainer(clusterResource, application, node, 
        rmContainer, containerStatus, event);

    LOG.info("Application " + applicationAttemptId + 
        " released container " + container.getId() +
        " on node: " + node + 
        " with event: " + event);
  }

  @Lock(Lock.NoLock.class)
  FiCaSchedulerApp getApplication(ApplicationAttemptId applicationAttemptId) {
    return applications.get(applicationAttemptId);
  }

  @Override
  public SchedulerAppReport getSchedulerAppInfo(
      ApplicationAttemptId applicationAttemptId) {
    FiCaSchedulerApp app = getApplication(applicationAttemptId);
    return app == null ? null : new SchedulerAppReport(app);
  }
  
  @Lock(Lock.NoLock.class)
  FiCaSchedulerNode getNode(NodeId nodeId) {
    return nodes.get(nodeId);
  }

  private RMContainer getRMContainer(ContainerId containerId) {
    FiCaSchedulerApp application = 
        getApplication(containerId.getApplicationAttemptId());
    return (application == null) ? null : application.getRMContainer(containerId);
  }

  @Override
  @Lock(Lock.NoLock.class)
  public void recover(RMState state) throws Exception {
    // NOT IMPLEMENTED
  }

  @Override
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    FiCaSchedulerNode node = getNode(nodeId);
    return node == null ? null : new SchedulerNodeReport(node);
  }
  
}
