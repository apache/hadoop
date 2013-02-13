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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.Lock;
import org.apache.hadoop.yarn.Lock.NoLock;
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
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.BuilderUtils;

@Private
@Unstable
public class LeafQueue implements CSQueue {
  private static final Log LOG = LogFactory.getLog(LeafQueue.class);

  private final String queueName;
  private CSQueue parent;
  private float capacity;
  private float absoluteCapacity;
  private float maximumCapacity;
  private float absoluteMaxCapacity;
  private float absoluteUsedCapacity = 0.0f;
  private int userLimit;
  private float userLimitFactor;

  private int maxApplications;
  private int maxApplicationsPerUser;
  
  private float maxAMResourcePerQueuePercent;
  private int maxActiveApplications; // Based on absolute max capacity
  private int maxActiveAppsUsingAbsCap; // Based on absolute capacity
  private int maxActiveApplicationsPerUser;
  
  private Resource usedResources = Resources.createResource(0);
  private float usedCapacity = 0.0f;
  private volatile int numContainers;

  Set<SchedulerApp> activeApplications;
  Map<ApplicationAttemptId, SchedulerApp> applicationsMap = 
      new HashMap<ApplicationAttemptId, SchedulerApp>();
  
  Set<SchedulerApp> pendingApplications;
  
  private final Resource minimumAllocation;
  private final Resource maximumAllocation;
  private final float minimumAllocationFactor;

  private RMContainerTokenSecretManager containerTokenSecretManager;

  private Map<String, User> users = new HashMap<String, User>();
  
  private final QueueMetrics metrics;

  private QueueInfo queueInfo; 

  private QueueState state;

  private Map<QueueACL, AccessControlList> acls = 
    new HashMap<QueueACL, AccessControlList>();

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private CapacitySchedulerContext scheduler;
  
  private final ActiveUsersManager activeUsersManager;
  
  private final int nodeLocalityDelay;
  
  public LeafQueue(CapacitySchedulerContext cs, 
      String queueName, CSQueue parent, 
      Comparator<SchedulerApp> applicationComparator, CSQueue old) {
    this.scheduler = cs;
    this.queueName = queueName;
    this.parent = parent;
    // must be after parent and queueName are initialized
    this.metrics = old != null ? old.getMetrics() :
        QueueMetrics.forQueue(getQueuePath(), parent,
			      cs.getConfiguration().getEnableUserMetrics(),
			      cs.getConf());
    this.activeUsersManager = new ActiveUsersManager(metrics);
    this.minimumAllocation = cs.getMinimumResourceCapability();
    this.maximumAllocation = cs.getMaximumResourceCapability();
    this.minimumAllocationFactor = 
        (float)(maximumAllocation.getMemory() - minimumAllocation.getMemory()) / 
         maximumAllocation.getMemory();
    this.containerTokenSecretManager = cs.getContainerTokenSecretManager();

    float capacity = 
      (float)cs.getConfiguration().getCapacity(getQueuePath()) / 100;
    float absoluteCapacity = parent.getAbsoluteCapacity() * capacity;

    float maximumCapacity = 
        (float)cs.getConfiguration().getMaximumCapacity(getQueuePath()) / 100;
    float absoluteMaxCapacity = 
        CSQueueUtils.computeAbsoluteMaximumCapacity(maximumCapacity, parent);

    int userLimit = cs.getConfiguration().getUserLimit(getQueuePath());
    float userLimitFactor = 
      cs.getConfiguration().getUserLimitFactor(getQueuePath());

    int maxApplications = cs.getConfiguration().getMaximumApplicationsPerQueue(getQueuePath());
    if (maxApplications < 0) {
      int maxSystemApps = cs.getConfiguration().getMaximumSystemApplications();
      maxApplications = (int)(maxSystemApps * absoluteCapacity);
    }
    maxApplicationsPerUser = 
      (int)(maxApplications * (userLimit / 100.0f) * userLimitFactor);

    this.maxAMResourcePerQueuePercent = 
        cs.getConfiguration().
            getMaximumApplicationMasterResourcePerQueuePercent(getQueuePath());
    int maxActiveApplications = 
        CSQueueUtils.computeMaxActiveApplications(
            cs.getClusterResources(), this.minimumAllocation,
            maxAMResourcePerQueuePercent, absoluteMaxCapacity);
    this.maxActiveAppsUsingAbsCap = 
            CSQueueUtils.computeMaxActiveApplications(
                cs.getClusterResources(), this.minimumAllocation,
                maxAMResourcePerQueuePercent, absoluteCapacity);
    int maxActiveApplicationsPerUser = 
        CSQueueUtils.computeMaxActiveApplicationsPerUser(maxActiveAppsUsingAbsCap, userLimit, 
            userLimitFactor);

    this.queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    this.queueInfo.setQueueName(queueName);
    this.queueInfo.setChildQueues(new ArrayList<QueueInfo>());

    QueueState state = cs.getConfiguration().getState(getQueuePath());

    Map<QueueACL, AccessControlList> acls = 
      cs.getConfiguration().getAcls(getQueuePath());

    this.nodeLocalityDelay = 
        cs.getConfiguration().getNodeLocalityDelay();
    
    setupQueueConfigs(
        cs.getClusterResources(),
        capacity, absoluteCapacity, 
        maximumCapacity, absoluteMaxCapacity, 
        userLimit, userLimitFactor, 
        maxApplications, maxApplicationsPerUser,
        maxActiveApplications, maxActiveApplicationsPerUser,
        state, acls);

    if(LOG.isDebugEnabled()) {
      LOG.debug("LeafQueue:" + " name=" + queueName
        + ", fullname=" + getQueuePath());
    }

    this.pendingApplications = 
        new TreeSet<SchedulerApp>(applicationComparator);
    this.activeApplications = new TreeSet<SchedulerApp>(applicationComparator);
  }

  private synchronized void setupQueueConfigs(
      Resource clusterResource,
      float capacity, float absoluteCapacity, 
      float maximumCapacity, float absoluteMaxCapacity,
      int userLimit, float userLimitFactor,
      int maxApplications, int maxApplicationsPerUser,
      int maxActiveApplications, int maxActiveApplicationsPerUser,
      QueueState state, Map<QueueACL, AccessControlList> acls)
  {
    // Sanity check
    CSQueueUtils.checkMaxCapacity(getQueueName(), capacity, maximumCapacity);
    float absCapacity = getParent().getAbsoluteCapacity() * capacity;
    CSQueueUtils.checkAbsoluteCapacities(getQueueName(), absCapacity, absoluteMaxCapacity);

    this.capacity = capacity; 
    this.absoluteCapacity = absCapacity;

    this.maximumCapacity = maximumCapacity;
    this.absoluteMaxCapacity = absoluteMaxCapacity;

    this.userLimit = userLimit;
    this.userLimitFactor = userLimitFactor;

    this.maxApplications = maxApplications;
    this.maxApplicationsPerUser = maxApplicationsPerUser;

    this.maxActiveApplications = maxActiveApplications;
    this.maxActiveApplicationsPerUser = maxActiveApplicationsPerUser;
    
    this.state = state;

    this.acls = acls;

    this.queueInfo.setCapacity(this.capacity);
    this.queueInfo.setMaximumCapacity(this.maximumCapacity);
    this.queueInfo.setQueueState(this.state);

    StringBuilder aclsString = new StringBuilder();
    for (Map.Entry<QueueACL, AccessControlList> e : acls.entrySet()) {
      aclsString.append(e.getKey() + ":" + e.getValue().getAclString());
    }
    
    // Update metrics
    CSQueueUtils.updateQueueStatistics(
        this, getParent(), clusterResource, minimumAllocation);

    LOG.info("Initializing " + queueName + "\n" +
        "capacity = " + capacity +
        " [= (float) configuredCapacity / 100 ]" + "\n" + 
        "asboluteCapacity = " + absoluteCapacity +
        " [= parentAbsoluteCapacity * capacity ]" + "\n" +
        "maxCapacity = " + maximumCapacity +
        " [= configuredMaxCapacity ]" + "\n" +
        "absoluteMaxCapacity = " + absoluteMaxCapacity +
        " [= 1.0 maximumCapacity undefined, " +
        "(parentAbsoluteMaxCapacity * maximumCapacity) / 100 otherwise ]" + 
        "\n" +
        "userLimit = " + userLimit +
        " [= configuredUserLimit ]" + "\n" +
        "userLimitFactor = " + userLimitFactor +
        " [= configuredUserLimitFactor ]" + "\n" +
        "maxApplications = " + maxApplications +
        " [= configuredMaximumSystemApplicationsPerQueue or" + 
        " (int)(configuredMaximumSystemApplications * absoluteCapacity)]" + 
        "\n" +
        "maxApplicationsPerUser = " + maxApplicationsPerUser +
        " [= (int)(maxApplications * (userLimit / 100.0f) * " +
        "userLimitFactor) ]" + "\n" +
        "maxActiveApplications = " + maxActiveApplications +
        " [= max(" + 
        "(int)ceil((clusterResourceMemory / minimumAllocation) * " + 
        "maxAMResourcePerQueuePercent * absoluteMaxCapacity)," + 
        "1) ]" + "\n" +
        "maxActiveAppsUsingAbsCap = " + maxActiveAppsUsingAbsCap +
        " [= max(" + 
        "(int)ceil((clusterResourceMemory / minimumAllocation) *" + 
        "maxAMResourcePercent * absoluteCapacity)," + 
        "1) ]" + "\n" +
        "maxActiveApplicationsPerUser = " + maxActiveApplicationsPerUser +
        " [= max(" +
        "(int)(maxActiveApplications * (userLimit / 100.0f) * " +
        "userLimitFactor)," +
        "1) ]" + "\n" +
        "usedCapacity = " + usedCapacity +
        " [= usedResourcesMemory / " +
        "(clusterResourceMemory * absoluteCapacity)]" + "\n" +
        "absoluteUsedCapacity = " + absoluteUsedCapacity +
        " [= usedResourcesMemory / clusterResourceMemory]" + "\n" +
        "maxAMResourcePerQueuePercent = " + maxAMResourcePerQueuePercent +
        " [= configuredMaximumAMResourcePercent ]" + "\n" +
        "minimumAllocationFactor = " + minimumAllocationFactor +
        " [= (float)(maximumAllocationMemory - minimumAllocationMemory) / " +
        "maximumAllocationMemory ]" + "\n" +
        "numContainers = " + numContainers +
        " [= currentNumContainers ]" + "\n" +
        "state = " + state +
        " [= configuredState ]" + "\n" +
        "acls = " + aclsString +
        " [= configuredAcls ]" + "\n");
  }
  
  @Override
  public synchronized float getCapacity() {
    return capacity;
  }

  @Override
  public synchronized float getAbsoluteCapacity() {
    return absoluteCapacity;
  }

  @Override
  public synchronized float getMaximumCapacity() {
    return maximumCapacity;
  }

  @Override
  public synchronized float getAbsoluteMaximumCapacity() {
    return absoluteMaxCapacity;
  }

  @Override
  public synchronized float getAbsoluteUsedCapacity() {
    return absoluteUsedCapacity;
  }

  @Override
  public synchronized CSQueue getParent() {
    return parent;
  }
  
  @Override
  public synchronized void setParent(CSQueue newParentQueue) {
    this.parent = (ParentQueue)newParentQueue;
  }
  
  @Override
  public String getQueueName() {
    return queueName;
  }

  @Override
  public String getQueuePath() {
    return getParent().getQueuePath() + "." + getQueueName();
  }

  /**
   * Used only by tests.
   */
  @Private
  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }

  /**
   * Used only by tests.
   */
  @Private
  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }

  /**
   * Used only by tests.
   */
  @Private
  public float getMinimumAllocationFactor() {
    return minimumAllocationFactor;
  }

  public int getMaxApplications() {
    return maxApplications;
  }

  public synchronized int getMaxApplicationsPerUser() {
    return maxApplicationsPerUser;
  }

  public synchronized int getMaximumActiveApplications() {
    return maxActiveApplications;
  }

  public synchronized int getMaximumActiveApplicationsPerUser() {
    return maxActiveApplicationsPerUser;
  }

  @Override
  public ActiveUsersManager getActiveUsersManager() {
    return activeUsersManager;
  }

  @Override
  public synchronized float getUsedCapacity() {
    return usedCapacity;
  }

  @Override
  public synchronized Resource getUsedResources() {
    return usedResources;
  }

  @Override
  public List<CSQueue> getChildQueues() {
    return null;
  }

  @Override
  public synchronized void setUsedCapacity(float usedCapacity) {
    this.usedCapacity = usedCapacity;
  }

  @Override
  public synchronized void setAbsoluteUsedCapacity(float absUsedCapacity) {
    this.absoluteUsedCapacity = absUsedCapacity;
  }

  /**
   * Set maximum capacity - used only for testing.
   * @param maximumCapacity new max capacity
   */
  synchronized void setMaxCapacity(float maximumCapacity) {
    // Sanity check
    CSQueueUtils.checkMaxCapacity(getQueueName(), capacity, maximumCapacity);
    float absMaxCapacity = 
        CSQueueUtils.computeAbsoluteMaximumCapacity(
            maximumCapacity, getParent());
    CSQueueUtils.checkAbsoluteCapacities(getQueueName(), absoluteCapacity, absMaxCapacity);
    
    this.maximumCapacity = maximumCapacity;
    this.absoluteMaxCapacity = absMaxCapacity;
  }
  
  /**
   * Set user limit - used only for testing.
   * @param userLimit new user limit
   */
  synchronized void setUserLimit(int userLimit) {
    this.userLimit = userLimit;
  }

  /**
   * Set user limit factor - used only for testing.
   * @param userLimitFactor new user limit factor
   */
  synchronized void setUserLimitFactor(int userLimitFactor) {
    this.userLimitFactor = userLimitFactor;
  }

  @Override
  public synchronized int getNumApplications() {
    return getNumPendingApplications() + getNumActiveApplications();
  }

  public synchronized int getNumPendingApplications() {
    return pendingApplications.size();
  }

  public synchronized int getNumActiveApplications() {
    return activeApplications.size();
  }

  @Private
  public synchronized int getNumApplications(String user) {
    return getUser(user).getTotalApplications();
  }

  @Private
  public synchronized int getNumPendingApplications(String user) {
    return getUser(user).getPendingApplications();
  }

  @Private
  public synchronized int getNumActiveApplications(String user) {
    return getUser(user).getActiveApplications();
  }
  
  public synchronized int getNumContainers() {
    return numContainers;
  }

  @Override
  public synchronized QueueState getState() {
    return state;
  }

  @Private
  public synchronized int getUserLimit() {
    return userLimit;
  }

  @Private
  public synchronized float getUserLimitFactor() {
    return userLimitFactor;
  }

  @Override
  public synchronized Map<QueueACL, AccessControlList> getQueueAcls() {
    return new HashMap<QueueACL, AccessControlList>(acls);
  }

  @Override
  public synchronized QueueInfo getQueueInfo(
      boolean includeChildQueues, boolean recursive) {
    queueInfo.setCurrentCapacity(usedCapacity);
    return queueInfo;
  }

  @Override
  public synchronized List<QueueUserACLInfo> 
  getQueueUserAclInfo(UserGroupInformation user) {
    QueueUserACLInfo userAclInfo = 
      recordFactory.newRecordInstance(QueueUserACLInfo.class);
    List<QueueACL> operations = new ArrayList<QueueACL>();
    for (QueueACL operation : QueueACL.values()) {
      if (hasAccess(operation, user)) {
        operations.add(operation);
      }
    }

    userAclInfo.setQueueName(getQueueName());
    userAclInfo.setUserAcls(operations);
    return Collections.singletonList(userAclInfo);
  }

  @Private
  public int getNodeLocalityDelay() {
    return nodeLocalityDelay;
  }
  
  public String toString() {
    return queueName + ": " + 
        "capacity=" + capacity + ", " + 
        "absoluteCapacity=" + absoluteCapacity + ", " + 
        "usedResources=" + usedResources.getMemory() + "MB, " + 
        "usedCapacity=" + getUsedCapacity() + ", " + 
        "absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + ", " +
        "numApps=" + getNumApplications() + ", " + 
        "numContainers=" + getNumContainers();  
  }

  private synchronized User getUser(String userName) {
    User user = users.get(userName);
    if (user == null) {
      user = new User();
      users.put(userName, user);
    }
    return user;
  }

  /**
   * @return an ArrayList of UserInfo objects who are active in this queue
   */
  public synchronized ArrayList<UserInfo> getUsers() {
    ArrayList<UserInfo> usersToReturn = new ArrayList<UserInfo>();
    for (Map.Entry<String, User> entry: users.entrySet()) {
      usersToReturn.add(new UserInfo(entry.getKey(), Resources.clone(
        entry.getValue().consumed), entry.getValue().getActiveApplications(),
        entry.getValue().getPendingApplications()));
    }
    return usersToReturn;
  }

  @Override
  public synchronized void reinitialize(
      CSQueue newlyParsedQueue, Resource clusterResource) 
  throws IOException {
    // Sanity check
    if (!(newlyParsedQueue instanceof LeafQueue) || 
        !newlyParsedQueue.getQueuePath().equals(getQueuePath())) {
      throw new IOException("Trying to reinitialize " + getQueuePath() + 
          " from " + newlyParsedQueue.getQueuePath());
    }

    LeafQueue newlyParsedLeafQueue = (LeafQueue)newlyParsedQueue;
    setupQueueConfigs(
        clusterResource,
        newlyParsedLeafQueue.capacity, newlyParsedLeafQueue.absoluteCapacity, 
        newlyParsedLeafQueue.maximumCapacity, 
        newlyParsedLeafQueue.absoluteMaxCapacity, 
        newlyParsedLeafQueue.userLimit, newlyParsedLeafQueue.userLimitFactor, 
        newlyParsedLeafQueue.maxApplications,
        newlyParsedLeafQueue.getMaxApplicationsPerUser(),
        newlyParsedLeafQueue.getMaximumActiveApplications(), 
        newlyParsedLeafQueue.getMaximumActiveApplicationsPerUser(),
        newlyParsedLeafQueue.state, newlyParsedLeafQueue.acls);
  }

  @Override
  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    // Check if the leaf-queue allows access
    synchronized (this) {
      if (acls.get(acl).isUserAllowed(user)) {
        return true;
      }
    }

    // Check if parent-queue allows access
    return getParent().hasAccess(acl, user);
  }

  @Override
  public void submitApplication(SchedulerApp application, String userName,
      String queue)  throws AccessControlException {
    // Careful! Locking order is important!

    // Check queue ACLs
    UserGroupInformation userUgi = UserGroupInformation.createRemoteUser(userName);
    if (!hasAccess(QueueACL.SUBMIT_APPLICATIONS, userUgi)) {
      throw new AccessControlException("User " + userName + " cannot submit" +
          " applications to queue " + getQueuePath());
    }

    User user = null;
    synchronized (this) {

      // Check if the queue is accepting jobs
      if (getState() != QueueState.RUNNING) {
        String msg = "Queue " + getQueuePath() +
        " is STOPPED. Cannot accept submission of application: " +
        application.getApplicationId();
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Check submission limits for queues
      if (getNumApplications() >= getMaxApplications()) {
        String msg = "Queue " + getQueuePath() + 
        " already has " + getNumApplications() + " applications," +
        " cannot accept submission of application: " + 
        application.getApplicationId();
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Check submission limits for the user on this queue
      user = getUser(userName);
      if (user.getTotalApplications() >= getMaxApplicationsPerUser()) {
        String msg = "Queue " + getQueuePath() + 
        " already has " + user.getTotalApplications() + 
        " applications from user " + userName + 
        " cannot accept submission of application: " + 
        application.getApplicationId();
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Add the application to our data-structures
      addApplication(application, user);
    }

    int attemptId = application.getApplicationAttemptId().getAttemptId();
    metrics.submitApp(userName, attemptId);

    // Inform the parent queue
    try {
      getParent().submitApplication(application, userName, queue);
    } catch (AccessControlException ace) {
      LOG.info("Failed to submit application to parent-queue: " + 
          getParent().getQueuePath(), ace);
      removeApplication(application, user);
      throw ace;
    }
  }

  private synchronized void activateApplications() {
    for (Iterator<SchedulerApp> i=pendingApplications.iterator(); 
         i.hasNext(); ) {
      SchedulerApp application = i.next();
      
      // Check queue limit
      if (getNumActiveApplications() >= getMaximumActiveApplications()) {
        break;
      }
      
      // Check user limit
      User user = getUser(application.getUser());
      if (user.getActiveApplications() < getMaximumActiveApplicationsPerUser()) {
        user.activateApplication();
        activeApplications.add(application);
        i.remove();
        LOG.info("Application " + application.getApplicationId() +
            " from user: " + application.getUser() + 
            " activated in queue: " + getQueueName());
      }
    }
  }
  
  private synchronized void addApplication(SchedulerApp application, User user) {
    // Accept 
    user.submitApplication();
    pendingApplications.add(application);
    applicationsMap.put(application.getApplicationAttemptId(), application);

    // Activate applications
    activateApplications();
    
    LOG.info("Application added -" +
        " appId: " + application.getApplicationId() +
        " user: " + user + "," + " leaf-queue: " + getQueueName() +
        " #user-pending-applications: " + user.getPendingApplications() +
        " #user-active-applications: " + user.getActiveApplications() +
        " #queue-pending-applications: " + getNumPendingApplications() +
        " #queue-active-applications: " + getNumActiveApplications()
        );
  }

  @Override
  public void finishApplication(SchedulerApp application, String queue) {
    // Careful! Locking order is important!
    synchronized (this) {
      removeApplication(application, getUser(application.getUser()));
    }

    // Inform the parent queue
    getParent().finishApplication(application, queue);
  }

  public synchronized void removeApplication(SchedulerApp application, User user) {
    boolean wasActive = activeApplications.remove(application);
    if (!wasActive) {
      pendingApplications.remove(application);
    }
    applicationsMap.remove(application.getApplicationAttemptId());

    user.finishApplication(wasActive);
    if (user.getTotalApplications() == 0) {
      users.remove(application.getUser());
    }

    // Check if we can activate more applications
    activateApplications();
    
    // Inform the activeUsersManager
    synchronized (application) {
      activeUsersManager.deactivateApplication(
          application.getUser(), application.getApplicationId());
    }
    
    LOG.info("Application removed -" +
        " appId: " + application.getApplicationId() + 
        " user: " + application.getUser() + 
        " queue: " + getQueueName() +
        " #user-pending-applications: " + user.getPendingApplications() +
        " #user-active-applications: " + user.getActiveApplications() +
        " #queue-pending-applications: " + getNumPendingApplications() +
        " #queue-active-applications: " + getNumActiveApplications()
        );
  }
  
  private synchronized SchedulerApp getApplication(
      ApplicationAttemptId applicationAttemptId) {
    return applicationsMap.get(applicationAttemptId);
  }

  private static final CSAssignment NULL_ASSIGNMENT =
      new CSAssignment(Resources.createResource(0), NodeType.NODE_LOCAL);
  
  @Override
  public synchronized CSAssignment 
  assignContainers(Resource clusterResource, SchedulerNode node) {

    if(LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getHostName()
        + " #applications=" + activeApplications.size());
    }
    
    // Check for reserved resources
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      SchedulerApp application = 
          getApplication(reservedContainer.getApplicationAttemptId());
      return 
          assignReservedContainer(application, node, reservedContainer, 
              clusterResource); 
    }
    
    // Try to assign containers to applications in order
    for (SchedulerApp application : activeApplications) {
      
      if(LOG.isDebugEnabled()) {
        LOG.debug("pre-assignContainers for application "
        + application.getApplicationId());
        application.showRequests();
      }

      synchronized (application) {
        // Schedule in priority order
        for (Priority priority : application.getPriorities()) {
          // Required resource
          Resource required = 
              application.getResourceRequest(priority, RMNode.ANY).getCapability();

          // Do we need containers at this 'priority'?
          if (!needContainers(application, priority, required)) {
            continue;
          }

          // Compute user-limit & set headroom
          // Note: We compute both user-limit & headroom with the highest 
          //       priority request as the target. 
          //       This works since we never assign lower priority requests
          //       before all higher priority ones are serviced.
          Resource userLimit = 
              computeUserLimitAndSetHeadroom(application, clusterResource, 
                  required);          
          
          // Check queue max-capacity limit
          if (!assignToQueue(clusterResource, required)) {
            return NULL_ASSIGNMENT;
          }

          // Check user limit
          if (!assignToUser(application.getUser(), userLimit)) {
            break; 
          }

          // Inform the application it is about to get a scheduling opportunity
          application.addSchedulingOpportunity(priority);
          
          // Try to schedule
          CSAssignment assignment =  
            assignContainersOnNode(clusterResource, node, application, priority, 
                null);

          // Did we schedule or reserve a container?
          Resource assigned = assignment.getResource();
          if (Resources.greaterThan(assigned, Resources.none())) {

            // Book-keeping 
            // Note: Update headroom to account for current allocation too...
            allocateResource(clusterResource, application, assigned);
            
            // Don't reset scheduling opportunities for non-local assignments
            // otherwise the app will be delayed for each non-local assignment.
            // This helps apps with many off-cluster requests schedule faster.
            if (assignment.getType() != NodeType.OFF_SWITCH) {
              application.resetSchedulingOpportunities(priority);
            }
            
            // Done
            return assignment;
          } else {
            // Do not assign out of order w.r.t priorities
            break;
          }
        }
      }

      if(LOG.isDebugEnabled()) {
        LOG.debug("post-assignContainers for application "
          + application.getApplicationId());
      }
      application.showRequests();
    }
  
    return NULL_ASSIGNMENT;

  }

  private synchronized CSAssignment assignReservedContainer(
      SchedulerApp application, SchedulerNode node, RMContainer rmContainer, 
      Resource clusterResource) {
    // Do we still need this reservation?
    Priority priority = rmContainer.getReservedPriority();
    if (application.getTotalRequiredResources(priority) == 0) {
      // Release
      return new CSAssignment(application, rmContainer);
    }

    // Try to assign if we have sufficient resources
    assignContainersOnNode(clusterResource, node, application, priority, 
        rmContainer);
    
    // Doesn't matter... since it's already charged for at time of reservation
    // "re-reservation" is *free*
    return new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
  }

  private synchronized boolean assignToQueue(Resource clusterResource, 
      Resource required) {
    // Check how of the cluster's absolute capacity we are currently using...
    float potentialNewCapacity = 
      (float)(usedResources.getMemory() + required.getMemory()) / 
        clusterResource.getMemory();
    if (potentialNewCapacity > absoluteMaxCapacity) {
      LOG.info(getQueueName() + 
          " usedResources: " + usedResources.getMemory() +
          " clusterResources: " + clusterResource.getMemory() +
          " currentCapacity " + ((float)usedResources.getMemory())/clusterResource.getMemory() + 
          " required " + required.getMemory() +
          " potentialNewCapacity: " + potentialNewCapacity + " ( " +
          " max-capacity: " + absoluteMaxCapacity + ")");
      return false;
    }
    return true;
  }

  @Lock({LeafQueue.class, SchedulerApp.class})
  private Resource computeUserLimitAndSetHeadroom(
      SchedulerApp application, Resource clusterResource, Resource required) {
    
    String user = application.getUser();
    
    /** 
     * Headroom is min((userLimit, queue-max-cap) - consumed)
     */

    Resource userLimit =                          // User limit
        computeUserLimit(application, clusterResource, required);
    

    Resource queueMaxCap =                        // Queue Max-Capacity
        Resources.createResource(
            CSQueueUtils.roundDown(minimumAllocation, 
                (int)(absoluteMaxCapacity * clusterResource.getMemory()))
            );
    
    Resource userConsumed = getUser(user).getConsumedResources(); 
    Resource headroom = 
        Resources.subtract(Resources.min(userLimit, queueMaxCap), userConsumed);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Headroom calculation for user " + user + ": " + 
          " userLimit=" + userLimit + 
          " queueMaxCap=" + queueMaxCap + 
          " consumed=" + userConsumed + 
          " headroom=" + headroom);
    }
    
    application.setHeadroom(headroom);
    metrics.setAvailableResourcesToUser(user, headroom);
    
    return userLimit;
  }
  
  @Lock(NoLock.class)
  private Resource computeUserLimit(SchedulerApp application, 
      Resource clusterResource, Resource required) {
    // What is our current capacity? 
    // * It is equal to the max(required, queue-capacity) if
    //   we're running below capacity. The 'max' ensures that jobs in queues
    //   with miniscule capacity (< 1 slot) make progress
    // * If we're running over capacity, then its
    //   (usedResources + required) (which extra resources we are allocating)

    // Allow progress for queues with miniscule capacity
    final int queueCapacity = 
      Math.max(
          CSQueueUtils.roundUp(
              minimumAllocation, 
              (int)(absoluteCapacity * clusterResource.getMemory())), 
          required.getMemory()
          );

    final int consumed = usedResources.getMemory();
    final int currentCapacity = 
      (consumed < queueCapacity) ? 
          queueCapacity : (consumed + required.getMemory());

    // Never allow a single user to take more than the 
    // queue's configured capacity * user-limit-factor.
    // Also, the queue's configured capacity should be higher than 
    // queue-hard-limit * ulMin
    
    final int activeUsers = activeUsersManager.getNumActiveUsers();  

    int limit = 
      CSQueueUtils.roundUp(
          minimumAllocation,
          Math.min(
              Math.max(divideAndCeil(currentCapacity, activeUsers), 
                       divideAndCeil((int)userLimit*currentCapacity, 100)),
              (int)(queueCapacity * userLimitFactor)
              )
          );

    if (LOG.isDebugEnabled()) {
      String userName = application.getUser();
      LOG.debug("User limit computation for " + userName + 
          " in queue " + getQueueName() +
          " userLimit=" + userLimit +
          " userLimitFactor=" + userLimitFactor +
          " required: " + required + 
          " consumed: " + getUser(userName).getConsumedResources() + 
          " limit: " + limit +
          " queueCapacity: " + queueCapacity + 
          " qconsumed: " + consumed +
          " currentCapacity: " + currentCapacity +
          " activeUsers: " + activeUsers +
          " clusterCapacity: " + clusterResource.getMemory()
      );
    }

    return Resources.createResource(limit);
  }
  
  private synchronized boolean assignToUser(String userName, Resource limit) {

    User user = getUser(userName);
    
    // Note: We aren't considering the current request since there is a fixed
    // overhead of the AM, but it's a > check, not a >= check, so... 
    if ((user.getConsumedResources().getMemory()) > limit.getMemory()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("User " + userName + " in queue " + getQueueName() + 
            " will exceed limit - " +  
            " consumed: " + user.getConsumedResources() + 
            " limit: " + limit
        );
      }
      return false;
    }

    return true;
  }

  static int divideAndCeil(int a, int b) {
    if (b == 0) {
      LOG.info("divideAndCeil called with a=" + a + " b=" + b);
      return 0;
    }
    return (a + (b - 1)) / b;
  }

  boolean needContainers(SchedulerApp application, Priority priority, Resource required) {
    int requiredContainers = application.getTotalRequiredResources(priority);
    int reservedContainers = application.getNumReservedContainers(priority);
    int starvation = 0;
    if (reservedContainers > 0) {
      float nodeFactor = 
          ((float)required.getMemory() / getMaximumAllocation().getMemory());
      
      // Use percentage of node required to bias against large containers...
      // Protect against corner case where you need the whole node with
      // Math.min(nodeFactor, minimumAllocationFactor)
      starvation = 
          (int)((application.getReReservations(priority) / (float)reservedContainers) * 
                (1.0f - (Math.min(nodeFactor, getMinimumAllocationFactor())))
               );
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("needsContainers:" +
            " app.#re-reserve=" + application.getReReservations(priority) + 
            " reserved=" + reservedContainers + 
            " nodeFactor=" + nodeFactor + 
            " minAllocFactor=" + minimumAllocationFactor +
            " starvation=" + starvation);
      }
    }
    return (((starvation + requiredContainers) - reservedContainers) > 0);
  }

  private CSAssignment assignContainersOnNode(Resource clusterResource, 
      SchedulerNode node, SchedulerApp application, 
      Priority priority, RMContainer reservedContainer) {

    Resource assigned = Resources.none();

    // Data-local
    assigned = 
        assignNodeLocalContainers(clusterResource, node, application, priority,
            reservedContainer); 
    if (Resources.greaterThan(assigned, Resources.none())) {
      return new CSAssignment(assigned, NodeType.NODE_LOCAL);
    }

    // Rack-local
    assigned = 
        assignRackLocalContainers(clusterResource, node, application, priority, 
            reservedContainer);
    if (Resources.greaterThan(assigned, Resources.none())) {
      return new CSAssignment(assigned, NodeType.RACK_LOCAL);
    }
    
    // Off-switch
    return new CSAssignment(
        assignOffSwitchContainers(clusterResource, node, application, 
            priority, reservedContainer), 
        NodeType.OFF_SWITCH);
  }

  private Resource assignNodeLocalContainers(Resource clusterResource, 
      SchedulerNode node, SchedulerApp application, 
      Priority priority, RMContainer reservedContainer) {
    ResourceRequest request = 
        application.getResourceRequest(priority, node.getHostName());
    if (request != null) {
      if (canAssign(application, priority, node, NodeType.NODE_LOCAL, 
          reservedContainer)) {
        return assignContainer(clusterResource, node, application, priority, 
            request, NodeType.NODE_LOCAL, reservedContainer);
      }
    }
    
    return Resources.none();
  }

  private Resource assignRackLocalContainers(Resource clusterResource,  
      SchedulerNode node, SchedulerApp application, Priority priority,
      RMContainer reservedContainer) {
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getRackName());
    if (request != null) {
      if (canAssign(application, priority, node, NodeType.RACK_LOCAL, 
          reservedContainer)) {
        return assignContainer(clusterResource, node, application, priority, request, 
            NodeType.RACK_LOCAL, reservedContainer);
      } 
    }
    return Resources.none();
  }

  private Resource assignOffSwitchContainers(Resource clusterResource, SchedulerNode node, 
      SchedulerApp application, Priority priority, 
      RMContainer reservedContainer) {
    ResourceRequest request = 
      application.getResourceRequest(priority, RMNode.ANY);
    if (request != null) {
      if (canAssign(application, priority, node, NodeType.OFF_SWITCH, 
          reservedContainer)) {
        return assignContainer(clusterResource, node, application, priority, request, 
            NodeType.OFF_SWITCH, reservedContainer);
      }
    }
    return Resources.none();
  }

  boolean canAssign(SchedulerApp application, Priority priority, 
      SchedulerNode node, NodeType type, RMContainer reservedContainer) {

    // Clearly we need containers for this application...
    if (type == NodeType.OFF_SWITCH) {
      if (reservedContainer != null) {
        return true;
      }

      // 'Delay' off-switch
      ResourceRequest offSwitchRequest = 
          application.getResourceRequest(priority, RMNode.ANY);
      long missedOpportunities = application.getSchedulingOpportunities(priority);
      long requiredContainers = offSwitchRequest.getNumContainers(); 
      
      float localityWaitFactor = 
        application.getLocalityWaitFactor(priority, 
            scheduler.getNumClusterNodes());
      
      return ((requiredContainers * localityWaitFactor) < missedOpportunities);
    }

    // Check if we need containers on this rack 
    ResourceRequest rackLocalRequest = 
      application.getResourceRequest(priority, node.getRackName());
    if (rackLocalRequest == null || rackLocalRequest.getNumContainers() <= 0) {
      return false;
    }
      
    // If we are here, we do need containers on this rack for RACK_LOCAL req
    if (type == NodeType.RACK_LOCAL) {
      // 'Delay' rack-local just a little bit...
      long missedOpportunities = application.getSchedulingOpportunities(priority);
      return (
          Math.min(scheduler.getNumClusterNodes(), getNodeLocalityDelay()) < 
          missedOpportunities
          );
    }

    // Check if we need containers on this host
    if (type == NodeType.NODE_LOCAL) {
      // Now check if we need containers on this host...
      ResourceRequest nodeLocalRequest = 
        application.getResourceRequest(priority, node.getHostName());
      if (nodeLocalRequest != null) {
        return nodeLocalRequest.getNumContainers() > 0;
      }
    }

    return false;
  }
  
  private Container getContainer(RMContainer rmContainer, 
      SchedulerApp application, SchedulerNode node, 
      Resource capability, Priority priority) {
    return (rmContainer != null) ? rmContainer.getContainer() :
      createContainer(application, node, capability, priority);
  }
  

  Container createContainer(SchedulerApp application, SchedulerNode node, 
      Resource capability, Priority priority) {

    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId = BuilderUtils.newContainerId(application
        .getApplicationAttemptId(), application.getNewContainerId());

    // Create the container
    Container container = BuilderUtils.newContainer(containerId, nodeId,
        node.getRMNode().getHttpAddress(), capability, priority,
        null);

    return container;
  }

  /**
   * Create <code>ContainerToken</code>, only in secure-mode
   */
  ContainerToken createContainerToken(
      SchedulerApp application, Container container) {
    return containerTokenSecretManager.createContainerToken(
        container.getId(), container.getNodeId(),
        application.getUser(), container.getResource());
  }
  
  private Resource assignContainer(Resource clusterResource, SchedulerNode node, 
      SchedulerApp application, Priority priority, 
      ResourceRequest request, NodeType type, RMContainer rmContainer) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getHostName()
        + " application=" + application.getApplicationId().getId()
        + " priority=" + priority.getPriority()
        + " request=" + request + " type=" + type);
    }
    Resource capability = request.getCapability();

    Resource available = node.getAvailableResource();

    assert (available.getMemory() >  0);

    // Create the container if necessary
    Container container = 
        getContainer(rmContainer, application, node, capability, priority);
  
    // something went wrong getting/creating the container 
    if (container == null) {
      return Resources.none();
    }

    // Can we allocate a container on this node?
    int availableContainers = 
        available.getMemory() / capability.getMemory();         
    if (availableContainers > 0) {
      // Allocate...

      // Did we previously reserve containers at this 'priority'?
      if (rmContainer != null){
        unreserve(application, priority, node, rmContainer);
      }

      // Create container tokens in secure-mode
      if (UserGroupInformation.isSecurityEnabled()) {
        ContainerToken containerToken = 
            createContainerToken(application, container);
        if (containerToken == null) {
          // Something went wrong...
          return Resources.none();
        }
        container.setContainerToken(containerToken);
      }

      // Inform the application
      RMContainer allocatedContainer = 
          application.allocate(type, node, priority, request, container);
      if (allocatedContainer == null) {
        // Did the application need this resource?
        return Resources.none();
      }

      // Inform the node
      node.allocateContainer(application.getApplicationId(), 
          allocatedContainer);

      LOG.info("assignedContainer" +
          " application=" + application.getApplicationId() +
          " container=" + container + 
          " containerId=" + container.getId() + 
          " queue=" + this + 
          " usedCapacity=" + getUsedCapacity() +
          " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
          " used=" + usedResources + 
          " cluster=" + clusterResource);

      return container.getResource();
    } else {
      // Reserve by 'charging' in advance...
      reserve(application, priority, node, rmContainer, container);

      LOG.info("Reserved container " + 
          " application=" + application.getApplicationId() +
          " resource=" + request.getCapability() + 
          " queue=" + this.toString() + 
          " usedCapacity=" + getUsedCapacity() +
          " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
          " used=" + usedResources + 
          " cluster=" + clusterResource);

      return request.getCapability();
    }
  }

  private void reserve(SchedulerApp application, Priority priority, 
      SchedulerNode node, RMContainer rmContainer, Container container) {
    // Update reserved metrics if this is the first reservation
    if (rmContainer == null) {
      getMetrics().reserveResource(
          application.getUser(), container.getResource());
    }

    // Inform the application 
    rmContainer = application.reserve(node, priority, rmContainer, container);
    
    // Update the node
    node.reserveResource(application, priority, rmContainer);
  }

  private void unreserve(SchedulerApp application, Priority priority, 
      SchedulerNode node, RMContainer rmContainer) {
    // Done with the reservation?
    application.unreserve(node, priority);
    node.unreserveResource(application);
      
      // Update reserved metrics
    getMetrics().unreserveResource(
        application.getUser(), rmContainer.getContainer().getResource());
  }


  @Override
  public void completedContainer(Resource clusterResource, 
      SchedulerApp application, SchedulerNode node, RMContainer rmContainer, 
      ContainerStatus containerStatus, RMContainerEventType event) {
    if (application != null) {
      // Careful! Locking order is important!
      synchronized (this) {

        Container container = rmContainer.getContainer();
        
        // Inform the application & the node
        // Note: It's safe to assume that all state changes to RMContainer
        // happen under scheduler's lock... 
        // So, this is, in effect, a transaction across application & node
        if (rmContainer.getState() == RMContainerState.RESERVED) {
          unreserve(application, rmContainer.getReservedPriority(), 
              node, rmContainer);
        } else {
          application.containerCompleted(rmContainer, containerStatus, event);
          node.releaseContainer(container);
        }


        // Book-keeping
        releaseResource(clusterResource, 
            application, container.getResource());

        LOG.info("completedContainer" +
            " container=" + container +
            " resource=" + container.getResource() +
        		" queue=" + this + 
            " usedCapacity=" + getUsedCapacity() +
            " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
            " used=" + usedResources + 
            " cluster=" + clusterResource);
      }

      // Inform the parent queue
      getParent().completedContainer(clusterResource, application, 
          node, rmContainer, null, event);
    }
  }

  synchronized void allocateResource(Resource clusterResource, 
      SchedulerApp application, Resource resource) {
    // Update queue metrics
    Resources.addTo(usedResources, resource);
    CSQueueUtils.updateQueueStatistics(
        this, getParent(), clusterResource, minimumAllocation);
    ++numContainers;

    // Update user metrics
    String userName = application.getUser();
    User user = getUser(userName);
    user.assignContainer(resource);
    Resources.subtractFrom(application.getHeadroom(), resource); // headroom
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
    
    if (LOG.isDebugEnabled()) {
      LOG.info(getQueueName() + 
          " user=" + userName + 
          " used=" + usedResources + " numContainers=" + numContainers +
          " headroom = " + application.getHeadroom() +
          " user-resources=" + user.getConsumedResources()
          );
    }
  }

  synchronized void releaseResource(Resource clusterResource, 
      SchedulerApp application, Resource resource) {
    // Update queue metrics
    Resources.subtractFrom(usedResources, resource);
    CSQueueUtils.updateQueueStatistics(
        this, getParent(), clusterResource, minimumAllocation);
    --numContainers;

    // Update user metrics
    String userName = application.getUser();
    User user = getUser(userName);
    user.releaseContainer(resource);
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
      
    LOG.info(getQueueName() + 
        " used=" + usedResources + " numContainers=" + numContainers + 
        " user=" + userName + " user-resources=" + user.getConsumedResources());
  }

  @Override
  public synchronized void updateClusterResource(Resource clusterResource) {
    // Update queue properties
    maxActiveApplications = 
        CSQueueUtils.computeMaxActiveApplications(
            clusterResource, minimumAllocation, 
            maxAMResourcePerQueuePercent, absoluteMaxCapacity);
    maxActiveAppsUsingAbsCap = 
        CSQueueUtils.computeMaxActiveApplications(
            clusterResource, minimumAllocation, 
            maxAMResourcePerQueuePercent, absoluteCapacity);
    maxActiveApplicationsPerUser = 
        CSQueueUtils.computeMaxActiveApplicationsPerUser(
            maxActiveAppsUsingAbsCap, userLimit, userLimitFactor);
    
    // Update metrics
    CSQueueUtils.updateQueueStatistics(
        this, getParent(), clusterResource, minimumAllocation);
    
    // Update application properties
    for (SchedulerApp application : activeApplications) {
      synchronized (application) {
        computeUserLimitAndSetHeadroom(application, clusterResource, 
            Resources.none());
      }
    }
  }
  
  @Override
  public QueueMetrics getMetrics() {
    return metrics;
  }

  static class User {
    Resource consumed = Resources.createResource(0);
    int pendingApplications = 0;
    int activeApplications = 0;

    public Resource getConsumedResources() {
      return consumed;
    }

    public int getPendingApplications() {
      return pendingApplications;
    }

    public int getActiveApplications() {
      return activeApplications;
    }

    public int getTotalApplications() {
      return getPendingApplications() + getActiveApplications();
    }
    
    public synchronized void submitApplication() {
      ++pendingApplications;
    }
    
    public synchronized void activateApplication() {
      --pendingApplications;
      ++activeApplications;
    }

    public synchronized void finishApplication(boolean wasActive) {
      if (wasActive) {
        --activeApplications;
      }
      else {
        --pendingApplications;
      }
    }

    public synchronized void assignContainer(Resource resource) {
      Resources.addTo(consumed, resource);
    }

    public synchronized void releaseContainer(Resource resource) {
      Resources.subtractFrom(consumed, resource);
    }
  }

  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApp application, Container container) {
    // Careful! Locking order is important! 
    synchronized (this) {
      allocateResource(clusterResource, application, container.getResource());
    }
    getParent().recoverContainer(clusterResource, application, container);

  }
  
}
