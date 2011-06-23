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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.Lock;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationTrackerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.ApplicationInfo;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.ClusterTracker;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Application;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.BuilderUtils;

@LimitedPrivate("yarn")
@Evolving
public class CapacityScheduler 
implements ResourceScheduler, CapacitySchedulerContext {

  private static final Log LOG = LogFactory.getLog(CapacityScheduler.class);

  private Queue root;

  private final static List<Container> EMPTY_CONTAINER_LIST = 
    new ArrayList<Container>();

  private final Comparator<Queue> queueComparator = new Comparator<Queue>() {
    @Override
    public int compare(Queue q1, Queue q2) {
      if (q1.getUtilization() < q2.getUtilization()) {
        return -1;
      } else if (q1.getUtilization() > q2.getUtilization()) {
        return 1;
      }

      return q1.getQueuePath().compareTo(q2.getQueuePath());
    }
  };

  private final Comparator<Application> applicationComparator = 
    new Comparator<Application>() {
    @Override
    public int compare(Application a1, Application a2) {
      return a1.getApplicationId().getId() - a2.getApplicationId().getId();
    }
  };

  private CapacitySchedulerConfiguration conf;
  private ContainerTokenSecretManager containerTokenSecretManager;
  private ClusterTracker clusterTracker;

  private Map<String, Queue> queues = new ConcurrentHashMap<String, Queue>();

  private Resource clusterResource = 
    RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Resource.class);
  private int numNodeManagers = 0;

  private Resource minimumAllocation;
  private Resource maximumAllocation;

  private Map<ApplicationId, Application> applications =
    Collections.synchronizedMap(
    new TreeMap<ApplicationId, Application>(
        new BuilderUtils.ApplicationIdComparator()));

  private boolean initialized = false;

  public Queue getRootQueue() {
    return root;
  }

  @Override
  public CapacitySchedulerConfiguration getConfiguration() {
    return conf;
  }

  @Override
  public ContainerTokenSecretManager getContainerTokenSecretManager() {
    return containerTokenSecretManager;
  }

  @Override
  public Resource getMinimumResourceCapability() {
    return minimumAllocation;
  }

  @Override
  public Resource getMaximumResourceCapability() {
    return maximumAllocation;
  }

  public synchronized int getNumClusterNodes() {
    return numNodeManagers;
  }
  
  @Override
  public synchronized void reinitialize(Configuration conf,
      ContainerTokenSecretManager containerTokenSecretManager, ClusterTracker clusterTracker) 
  throws IOException {
    if (!initialized) {
      this.conf = new CapacitySchedulerConfiguration(conf);
      this.minimumAllocation = this.conf.getMinimumAllocation();
      this.maximumAllocation = this.conf.getMaximumAllocation();
      this.containerTokenSecretManager = containerTokenSecretManager;
      this.clusterTracker = clusterTracker;
      if (clusterTracker != null) clusterTracker.addListener(this);
      initializeQueues(this.conf);
      initialized = true;
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
  public static final String ROOT = "root";

  @Private
  public static final String ROOT_QUEUE = 
    CapacitySchedulerConfiguration.PREFIX + ROOT;

  @Lock(CapacityScheduler.class)
  private void initializeQueues(CapacitySchedulerConfiguration conf) {
    root = parseQueue(conf, null, ROOT, queues, queues);
    LOG.info("Initialized root queue " + root);
  }

  @Lock(CapacityScheduler.class)
  private void reinitializeQueues(CapacitySchedulerConfiguration conf) 
  throws IOException {
    // Parse new queues
    Map<String, Queue> newQueues = new HashMap<String, Queue>();
    Queue newRoot = parseQueue(conf, null, ROOT, newQueues, queues);
    
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
      Map<String, Queue> queues, Map<String, Queue> newQueues) 
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
      Map<String, Queue> queues, Map<String, Queue> newQueues) 
  {
    for (Map.Entry<String, Queue> e : newQueues.entrySet()) {
      String queueName = e.getKey();
      Queue queue = e.getValue();
      if (!queues.containsKey(queueName)) {
        queues.put(queueName, queue);
      }
    }
  }
  
  @Lock(CapacityScheduler.class)
  private Queue parseQueue(CapacitySchedulerConfiguration conf, 
      Queue parent, String queueName, Map<String, Queue> queues,
      Map<String, Queue> oldQueues) {
    Queue queue;
    String[] childQueueNames = 
      conf.getQueues((parent == null) ? 
          queueName : (parent.getQueuePath()+"."+queueName));
    if (childQueueNames == null || childQueueNames.length == 0) {
      if (null == parent) {
        throw new IllegalStateException(
            "Queue configuration missing child queue names for " + queueName);
      }
      queue = new LeafQueue(this, queueName, parent, applicationComparator,
                            oldQueues.get(queueName));
    } else {
      ParentQueue parentQueue = 
        new ParentQueue(this, queueName, queueComparator, parent,
                        oldQueues.get(queueName));
      List<Queue> childQueues = new ArrayList<Queue>();
      for (String childQueueName : childQueueNames) {
        Queue childQueue = 
          parseQueue(conf, parentQueue, childQueueName, queues, oldQueues);
        childQueues.add(childQueue);
      }
      parentQueue.setChildQueues(childQueues);

      queue = parentQueue;
    }

    queues.put(queueName, queue);

    LOG.info("Initialized queue: " + queue);
    return queue;
  }

  @Override
  public synchronized void addApplication(
      ApplicationId applicationId, ApplicationMaster master,
      String user, String queueName, Priority priority, ApplicationStore appStore)
  throws IOException {
    // Sanity checks
    Queue queue = queues.get(queueName);
    if (queue == null) {
      throw new IOException("Application " + applicationId + 
          " submitted by user " + user + " to unknown queue: " + queueName);
    }
    if (!(queue instanceof LeafQueue)) {
      throw new IOException("Application " + applicationId + 
          " submitted by user " + user + " to non-leaf queue: " + queueName);
    }

    // Create the application
    Application application = 
      new Application(applicationId, master, queue, user, appStore);
    
    // Submit to the queue
    try {
      queue.submitApplication(application, user, queueName, priority);
    } catch (AccessControlException ace) {
      throw new IOException(ace);
    }

    applications.put(applicationId, application);

    LOG.info("Application Submission: " + applicationId.getId() + 
        ", user: " + user +
        " queue: " + queue +
        ", currently active: " + applications.size());
  }

  @Override
  public synchronized void doneApplication(
      ApplicationId applicationId, boolean finishApplication)
  throws IOException {
    LOG.info("Application " + applicationId + " is done." +
    		" finish=" + finishApplication);
    
    Application application = getApplication(applicationId);

    if (application == null) {
      //      throw new IOException("Unknown application " + applicationId + 
      //          " has completed!");
      LOG.info("Unknown application " + applicationId + " has completed!");
      return;
    }
    
    // Release all the running containers 
    processReleasedContainers(application, application.getCurrentContainers());
    
     // Release all reserved containers
    releaseReservedContainers(application);
    
    // Clean up pending requests, metrics etc.
    application.stop();
    
    /** The application can be retried. So only remove it from scheduler data
     * structures if the finishApplication flag is set.
     */
    if (finishApplication) {
      // Inform the queue
      Queue queue = queues.get(application.getQueue().getQueueName());
      queue.finishApplication(application, queue.getQueueName());
      
      // Inform the resource-tracker
      clusterTracker.finishedApplication(applicationId, 
          application.getAllNodesForApplication());
      
      // Remove from our data-structure
      applications.remove(applicationId);
    }
  }

  @Override
  @Lock(Lock.NoLock.class)
  public Allocation allocate(ApplicationId applicationId,
      List<ResourceRequest> ask, List<Container> release)
      throws IOException {

    Application application = getApplication(applicationId);
    if (application == null) {
      LOG.info("Calling allocate on removed " +
          "or non existant application " + applicationId);
      return new Allocation(EMPTY_CONTAINER_LIST, Resources.none()); 
    }
    
    // Sanity check
    normalizeRequests(ask);

    LOG.info("DEBUG --- allocate: pre-update" +
        " applicationId=" + applicationId + 
        " application=" + application);
    application.showRequests();

    // Update application requests
    application.updateResourceRequests(ask);

    // Release ununsed containers and update queue capacities
    processReleasedContainers(application, release);

    LOG.info("DEBUG --- allocate: post-update");
    application.showRequests();

    // Acquire containers
    List<Container> allocatedContainers = application.acquire();

    // Resource limit
    Resource limit = application.getHeadroom();
    
    LOG.info("DEBUG --- allocate:" +
        " applicationId=" + applicationId + 
        " #ask=" + ask.size() + 
        " #release=" + release.size() +
        " #allocatedContainers=" + allocatedContainers.size() +
        " limit=" + limit);

      
      return new Allocation(allocatedContainers, limit);
  }

  @Override
  @Lock(Lock.NoLock.class)
  public QueueInfo getQueueInfo(String queueName, 
      boolean includeApplications, boolean includeChildQueues, boolean recursive) 
  throws IOException {
    Queue queue = null;

    synchronized (this) {
      queue = this.queues.get(queueName); 
    }

    if (queue == null) {
      throw new IOException("Unknown queue: " + queueName);
    }
    return queue.getQueueInfo(includeApplications, includeChildQueues, recursive);
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

  @Lock(Lock.NoLock.class)
  private void normalizeRequests(List<ResourceRequest> asks) {
    for (ResourceRequest ask : asks) {
      normalizeRequest(ask);
    }
  }

  @Lock(Lock.NoLock.class)
  private void normalizeRequest(ResourceRequest ask) {
    int minMemory = minimumAllocation.getMemory();
    int memory = Math.max(ask.getCapability().getMemory(), minMemory);
    ask.getCapability().setMemory (
        minMemory * ((memory/minMemory) + (memory%minMemory > 0 ? 1 : 0)));
  }

  @Lock(CapacityScheduler.class)
  private List<Container> getCompletedContainers(
      Map<String, List<Container>> allContainers) {
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
  public synchronized void nodeUpdate(NodeInfo nm, 
      Map<String,List<Container>> containers ) {
    LOG.info("nodeUpdate: " + nm + " clusterResources: " + clusterResource);


    // Completed containers
    processCompletedContainers(getCompletedContainers(containers));

    // Assign new containers
    // 1. Check for reserved applications
    // 2. Schedule if there are no reservations

    Application reservedApplication = nm.getReservedApplication();
    if (reservedApplication != null) {
      // Try to fulfill the reservation
      LOG.info("Trying to fulfill reservation for application " + 
          reservedApplication.getApplicationId() + " on node: " + nm);
      LeafQueue queue = ((LeafQueue)reservedApplication.getQueue());
      Resource released = queue.assignContainers(clusterResource, nm);
      
      // Is the reservation necessary? If not, release the reservation
      if (org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.greaterThan(
          released, org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE)) {
        queue.completedContainer(clusterResource, null, released, reservedApplication);
      }
    }

    // Try to schedule more if there are no reservations to fulfill
    if (nm.getReservedApplication() == null) {
      root.assignContainers(clusterResource, nm);
    } else {
      LOG.info("Skipping scheduling since node " + nm + 
          " is reserved by application " + 
          nm.getReservedApplication().getApplicationId());
    }

  }

  @Lock(CapacityScheduler.class)
  private void killRunningContainers(List<Container> containers) {
    for (Container container : containers) {
      container.setState(ContainerState.COMPLETE);
      LOG.info("Killing running container " + container.getId());
      Application application = applications.get(container.getId().getAppId());
      processReleasedContainers(application, Collections.singletonList(container));
    }
  }
  
  @Lock(Lock.NoLock.class)
  private void processCompletedContainers(
      List<Container> completedContainers) {
    for (Container container: completedContainers) {
      Application application = getApplication(container.getId().getAppId());

      // this is possible, since an application can be removed from scheduler 
      // but the nodemanger is just updating about a completed container.
      if (application != null) {

        // Inform the queue
        LeafQueue queue = (LeafQueue)application.getQueue();
        queue.completedContainer(clusterResource, container, 
            container.getResource(), application);
      }
    }
  }

  @Lock(Lock.NoLock.class)
  private synchronized void processReleasedContainers(Application application,
      List<Container> releasedContainers) {
    // Inform the application
    application.releaseContainers(releasedContainers);

    // Inform clusterTracker
    List<Container> unusedContainers = new ArrayList<Container>();
    for (Container container : releasedContainers) {
      if (releaseContainer(
          application.getApplicationId(), 
          container)) {
        unusedContainers.add(container);
      }
    }

    // Update queue capacities
    processCompletedContainers(unusedContainers);
  }

  @Lock(CapacityScheduler.class)
  private void releaseReservedContainers(Application application) {
    LOG.info("Releasing reservations for completed application: " + 
        application.getApplicationId());
    Queue queue = queues.get(application.getQueue().getQueueName());
    Map<Priority, Set<NodeInfo>> reservations = application.getAllReservations();
    for (Map.Entry<Priority, Set<NodeInfo>> e : reservations.entrySet()) {
      Priority priority = e.getKey();
      Set<NodeInfo> reservedNodes = new HashSet<NodeInfo>(e.getValue());
      for (NodeInfo node : reservedNodes) {
        Resource allocatedResource = 
          application.getResourceRequest(priority, NodeManagerImpl.ANY).getCapability();
    
        application.unreserveResource(node, priority);
        node.unreserveResource(application, priority);
        
        queue.completedContainer(clusterResource, null, allocatedResource, application);
      }
    }
  }
  
  @Lock(Lock.NoLock.class)
  private Application getApplication(ApplicationId applicationId) {
    return applications.get(applicationId);
  }

  @Override
  public synchronized void handle(ASMEvent<ApplicationTrackerEventType> event) {
    switch(event.getType()) {
    case ADD:
      /** ignore add since its called sychronously from the applications manager 
       * 
       */
      break;
    case REMOVE:
      try {
        doneApplication(event.getAppContext().getApplicationID(), true);
      } catch(IOException ie) {
        LOG.error("Error in removing 'done' application", ie);
        //TODO have to be shutdown the RM in case of this.
        // do a graceful shutdown.
      }
      break;
    case EXPIRE:
      try {
        /** do not remove the application. Just do everything else exception 
         * removing the application
         */
        doneApplication(event.getAppContext().getApplicationID(), false);
      } catch(IOException ie) {
        LOG.error("Error in removing 'expired' application", ie);
        //TODO have to be shutdown the RM in case of this.
        // do a graceful shutdown.
      }
      break;
    }
  }

  public synchronized Resource getClusterResource() {
    return clusterResource;
  }

  @Override
  public synchronized void addNode(NodeInfo nodeManager) {
    Resources.addTo(clusterResource, nodeManager.getTotalCapability());
    ++numNodeManagers;
    LOG.info("Added node " + nodeManager.getNodeAddress() + 
        " clusterResource: " + clusterResource);
  }

  @Override
  public synchronized void removeNode(NodeInfo nodeInfo) {
    Resources.subtractFrom(clusterResource, nodeInfo.getTotalCapability());
    --numNodeManagers;

    // Remove running containers
    List<Container> runningContainers = nodeInfo.getRunningContainers();
    killRunningContainers(runningContainers);
    
    // Remove reservations, if any
    Application reservedApplication = nodeInfo.getReservedApplication();
    if (reservedApplication != null) {
      LeafQueue queue = ((LeafQueue)reservedApplication.getQueue());
      Resource released = nodeInfo.getReservedResource();
      queue.completedContainer(clusterResource, null, released, reservedApplication);
    }
    
    LOG.info("Removed node " + nodeInfo.getNodeAddress() + 
        " clusterResource: " + clusterResource);
  }
  
  @Lock(CapacityScheduler.class)
  private boolean releaseContainer(ApplicationId applicationId, 
      Container container) {
    // Reap containers
    LOG.info("Application " + applicationId + " released container " + container);
    return clusterTracker.releaseContainer(container);
  }


  @Override
  @Lock(Lock.NoLock.class)
  public void recover(RMState state) throws Exception {
    applications.clear();
    for (Map.Entry<ApplicationId, ApplicationInfo> entry : state.getStoredApplications().entrySet()) {
      ApplicationId appId = entry.getKey();
      ApplicationInfo appInfo = entry.getValue();
      Application app = applications.get(appId);
      app.allocate(appInfo.getContainers());
      for (Container c: entry.getValue().getContainers()) {
        Queue queue = queues.get(appInfo.getApplicationSubmissionContext().getQueue());
        queue.recoverContainer(clusterResource, applications.get(appId), c);
      }
    }
  }
}
