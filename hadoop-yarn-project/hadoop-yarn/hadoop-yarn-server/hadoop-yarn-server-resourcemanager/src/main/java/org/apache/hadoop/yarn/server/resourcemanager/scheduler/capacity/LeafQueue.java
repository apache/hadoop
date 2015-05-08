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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.server.utils.Lock.NoLock;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class LeafQueue extends AbstractCSQueue {
  private static final Log LOG = LogFactory.getLog(LeafQueue.class);

  private float absoluteUsedCapacity = 0.0f;
  private int userLimit;
  private float userLimitFactor;

  protected int maxApplications;
  protected int maxApplicationsPerUser;
  
  private float maxAMResourcePerQueuePercent;
  
  private int nodeLocalityDelay;

  Map<ApplicationAttemptId, FiCaSchedulerApp> applicationAttemptMap = 
      new HashMap<ApplicationAttemptId, FiCaSchedulerApp>();
  
  Set<FiCaSchedulerApp> pendingApplications;
  
  private float minimumAllocationFactor;

  private Map<String, User> users = new HashMap<String, User>();

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private CapacitySchedulerContext scheduler;
  
  private final ActiveUsersManager activeUsersManager;

  // cache last cluster resource to compute actual capacity
  private Resource lastClusterResource = Resources.none();
  
  // absolute capacity as a resource (based on cluster resource)
  private Resource absoluteCapacityResource = Resources.none();
  
  private final QueueResourceLimitsInfo queueResourceLimitsInfo =
      new QueueResourceLimitsInfo();

  private volatile ResourceLimits cachedResourceLimitsForHeadroom = null;

  private OrderingPolicy<FiCaSchedulerApp> 
    orderingPolicy = new FifoOrderingPolicy<FiCaSchedulerApp>();

  // record all ignore partition exclusivityRMContainer, this will be used to do
  // preemption, key is the partition of the RMContainer allocated on
  private Map<String, TreeSet<RMContainer>> ignorePartitionExclusivityRMContainers =
      new HashMap<>();
  
  public LeafQueue(CapacitySchedulerContext cs, 
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);
    this.scheduler = cs;

    this.activeUsersManager = new ActiveUsersManager(metrics); 

    if(LOG.isDebugEnabled()) {
      LOG.debug("LeafQueue:" + " name=" + queueName
        + ", fullname=" + getQueuePath());
    }

    Comparator<FiCaSchedulerApp> applicationComparator =
        cs.getApplicationComparator();
    this.pendingApplications = 
        new TreeSet<FiCaSchedulerApp>(applicationComparator);
    
    setupQueueConfigs(cs.getClusterResource());
  }

  protected synchronized void setupQueueConfigs(Resource clusterResource)
      throws IOException {
    super.setupQueueConfigs(clusterResource);
    
    this.lastClusterResource = clusterResource;
    updateAbsoluteCapacityResource(clusterResource);
    
    this.cachedResourceLimitsForHeadroom = new ResourceLimits(clusterResource);
    
    // Initialize headroom info, also used for calculating application 
    // master resource limits.  Since this happens during queue initialization
    // and all queues may not be realized yet, we'll use (optimistic) 
    // absoluteMaxCapacity (it will be replaced with the more accurate 
    // absoluteMaxAvailCapacity during headroom/userlimit/allocation events)
    setQueueResourceLimitsInfo(clusterResource);

    CapacitySchedulerConfiguration conf = csContext.getConfiguration();
    
    setOrderingPolicy(conf.<FiCaSchedulerApp>getOrderingPolicy(getQueuePath()));
    
    userLimit = conf.getUserLimit(getQueuePath());
    userLimitFactor = conf.getUserLimitFactor(getQueuePath());

    maxApplications = conf.getMaximumApplicationsPerQueue(getQueuePath());
    if (maxApplications < 0) {
      int maxSystemApps = conf.getMaximumSystemApplications();
      maxApplications =
          (int) (maxSystemApps * queueCapacities.getAbsoluteCapacity());
    }
    maxApplicationsPerUser = 
      (int)(maxApplications * (userLimit / 100.0f) * userLimitFactor);
    
    maxAMResourcePerQueuePercent =
        conf.getMaximumApplicationMasterResourcePerQueuePercent(getQueuePath());

    if (!SchedulerUtils.checkQueueLabelExpression(
        this.accessibleLabels, this.defaultLabelExpression, null)) {
      throw new IOException("Invalid default label expression of "
          + " queue="
          + getQueueName()
          + " doesn't have permission to access all labels "
          + "in default label expression. labelExpression of resource request="
          + (this.defaultLabelExpression == null ? ""
              : this.defaultLabelExpression)
          + ". Queue labels="
          + (getAccessibleNodeLabels() == null ? "" : StringUtils.join(
              getAccessibleNodeLabels().iterator(), ',')));
    }
    
    nodeLocalityDelay = conf.getNodeLocalityDelay();

    // re-init this since max allocation could have changed
    this.minimumAllocationFactor =
        Resources.ratio(resourceCalculator,
            Resources.subtract(maximumAllocation, minimumAllocation),
            maximumAllocation);

    StringBuilder aclsString = new StringBuilder();
    for (Map.Entry<AccessType, AccessControlList> e : acls.entrySet()) {
      aclsString.append(e.getKey() + ":" + e.getValue().getAclString());
    }

    StringBuilder labelStrBuilder = new StringBuilder(); 
    if (accessibleLabels != null) {
      for (String s : accessibleLabels) {
        labelStrBuilder.append(s);
        labelStrBuilder.append(",");
      }
    }

    LOG.info("Initializing " + queueName + "\n" +
        "capacity = " + queueCapacities.getCapacity() +
        " [= (float) configuredCapacity / 100 ]" + "\n" + 
        "asboluteCapacity = " + queueCapacities.getAbsoluteCapacity() +
        " [= parentAbsoluteCapacity * capacity ]" + "\n" +
        "maxCapacity = " + queueCapacities.getMaximumCapacity() +
        " [= configuredMaxCapacity ]" + "\n" +
        "absoluteMaxCapacity = " + queueCapacities.getAbsoluteMaximumCapacity() +
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
        "usedCapacity = " + queueCapacities.getUsedCapacity() +
        " [= usedResourcesMemory / " +
        "(clusterResourceMemory * absoluteCapacity)]" + "\n" +
        "absoluteUsedCapacity = " + absoluteUsedCapacity +
        " [= usedResourcesMemory / clusterResourceMemory]" + "\n" +
        "maxAMResourcePerQueuePercent = " + maxAMResourcePerQueuePercent +
        " [= configuredMaximumAMResourcePercent ]" + "\n" +
        "minimumAllocationFactor = " + minimumAllocationFactor +
        " [= (float)(maximumAllocationMemory - minimumAllocationMemory) / " +
        "maximumAllocationMemory ]" + "\n" +
        "maximumAllocation = " + maximumAllocation +
        " [= configuredMaxAllocation ]" + "\n" +
        "numContainers = " + numContainers +
        " [= currentNumContainers ]" + "\n" +
        "state = " + state +
        " [= configuredState ]" + "\n" +
        "acls = " + aclsString +
        " [= configuredAcls ]" + "\n" + 
        "nodeLocalityDelay = " + nodeLocalityDelay + "\n" +
        "labels=" + labelStrBuilder.toString() + "\n" +
        "nodeLocalityDelay = " +  nodeLocalityDelay + "\n" +
        "reservationsContinueLooking = " +
        reservationsContinueLooking + "\n" +
        "preemptionDisabled = " + getPreemptionDisabled() + "\n");
  }

  @Override
  public String getQueuePath() {
    return getParent().getQueuePath() + "." + getQueueName();
  }

  /**
   * Used only by tests.
   */
  @Private
  public float getMinimumAllocationFactor() {
    return minimumAllocationFactor;
  }
  
  /**
   * Used only by tests.
   */
  @Private
  public float getMaxAMResourcePerQueuePercent() {
    return maxAMResourcePerQueuePercent;
  }

  public int getMaxApplications() {
    return maxApplications;
  }

  public synchronized int getMaxApplicationsPerUser() {
    return maxApplicationsPerUser;
  }

  @Override
  public ActiveUsersManager getActiveUsersManager() {
    return activeUsersManager;
  }

  @Override
  public List<CSQueue> getChildQueues() {
    return null;
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
  synchronized void setUserLimitFactor(float userLimitFactor) {
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
    return orderingPolicy.getNumSchedulableEntities();
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
  public synchronized QueueInfo getQueueInfo(
      boolean includeChildQueues, boolean recursive) {
    QueueInfo queueInfo = getQueueInfo();
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
        "capacity=" + queueCapacities.getCapacity() + ", " + 
        "absoluteCapacity=" + queueCapacities.getAbsoluteCapacity() + ", " + 
        "usedResources=" + queueUsage.getUsed() +  ", " +
        "usedCapacity=" + getUsedCapacity() + ", " + 
        "absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + ", " +
        "numApps=" + getNumApplications() + ", " + 
        "numContainers=" + getNumContainers();  
  }
  
  @VisibleForTesting
  public synchronized void setNodeLabelManager(RMNodeLabelsManager mgr) {
    this.labelManager = mgr;
  }

  @VisibleForTesting
  public synchronized User getUser(String userName) {
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
    for (Map.Entry<String, User> entry : users.entrySet()) {
      User user = entry.getValue();
      usersToReturn.add(new UserInfo(entry.getKey(), Resources.clone(user
          .getUsed()), user.getActiveApplications(), user
          .getPendingApplications(), Resources.clone(user
          .getConsumedAMResources()), Resources.clone(user
          .getUserResourceLimit())));
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

    // don't allow the maximum allocation to be decreased in size
    // since we have already told running AM's the size
    Resource oldMax = getMaximumAllocation();
    Resource newMax = newlyParsedLeafQueue.getMaximumAllocation();
    if (newMax.getMemory() < oldMax.getMemory()
        || newMax.getVirtualCores() < oldMax.getVirtualCores()) {
      throw new IOException(
          "Trying to reinitialize "
              + getQueuePath()
              + " the maximum allocation size can not be decreased!"
              + " Current setting: " + oldMax
              + ", trying to set it to: " + newMax);
    }

    setupQueueConfigs(clusterResource);

    // queue metrics are updated, more resource may be available
    // activate the pending applications if possible
    activateApplications();
  }

  @Override
  public void submitApplicationAttempt(FiCaSchedulerApp application,
      String userName) {
    // Careful! Locking order is important!
    synchronized (this) {
      User user = getUser(userName);
      // Add the attempt to our data-structures
      addApplicationAttempt(application, user);
    }

    // We don't want to update metrics for move app
    if (application.isPending()) {
      metrics.submitAppAttempt(userName);
    }
    getParent().submitApplicationAttempt(application, userName);
  }

  @Override
  public void submitApplication(ApplicationId applicationId, String userName,
      String queue)  throws AccessControlException {
    // Careful! Locking order is important!

    // Check queue ACLs
    UserGroupInformation userUgi = UserGroupInformation.createRemoteUser(userName);
    if (!hasAccess(QueueACL.SUBMIT_APPLICATIONS, userUgi)
        && !hasAccess(QueueACL.ADMINISTER_QUEUE, userUgi)) {
      throw new AccessControlException("User " + userName + " cannot submit" +
          " applications to queue " + getQueuePath());
    }

    User user = null;
    synchronized (this) {

      // Check if the queue is accepting jobs
      if (getState() != QueueState.RUNNING) {
        String msg = "Queue " + getQueuePath() +
        " is STOPPED. Cannot accept submission of application: " + applicationId;
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Check submission limits for queues
      if (getNumApplications() >= getMaxApplications()) {
        String msg = "Queue " + getQueuePath() + 
        " already has " + getNumApplications() + " applications," +
        " cannot accept submission of application: " + applicationId;
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Check submission limits for the user on this queue
      user = getUser(userName);
      if (user.getTotalApplications() >= getMaxApplicationsPerUser()) {
        String msg = "Queue " + getQueuePath() + 
        " already has " + user.getTotalApplications() + 
        " applications from user " + userName + 
        " cannot accept submission of application: " + applicationId;
        LOG.info(msg);
        throw new AccessControlException(msg);
      }
    }

    // Inform the parent queue
    try {
      getParent().submitApplication(applicationId, userName, queue);
    } catch (AccessControlException ace) {
      LOG.info("Failed to submit application to parent-queue: " + 
          getParent().getQueuePath(), ace);
      throw ace;
    }

  }
  
  public synchronized Resource getAMResourceLimit() {
     /* 
      * The limit to the amount of resources which can be consumed by
      * application masters for applications running in the queue
      * is calculated by taking the greater of the max resources currently
      * available to the queue (see absoluteMaxAvailCapacity) and the absolute
      * resources guaranteed for the queue and multiplying it by the am
      * resource percent.
      *
      * This is to allow a queue to grow its (proportional) application 
      * master resource use up to its max capacity when other queues are 
      * idle but to scale back down to it's guaranteed capacity as they 
      * become busy.
      *
      */
     Resource queueCurrentLimit;
     synchronized (queueResourceLimitsInfo) {
       queueCurrentLimit = queueResourceLimitsInfo.getQueueCurrentLimit();
     }
     Resource queueCap = Resources.max(resourceCalculator, lastClusterResource,
       absoluteCapacityResource, queueCurrentLimit);
    Resource amResouceLimit =
        Resources.multiplyAndNormalizeUp(resourceCalculator, queueCap,
            maxAMResourcePerQueuePercent, minimumAllocation);

    metrics.setAMResouceLimit(amResouceLimit);
    return amResouceLimit;
  }
  
  public synchronized Resource getUserAMResourceLimit() {
     /*
      * The user amresource limit is based on the same approach as the 
      * user limit (as it should represent a subset of that).  This means that
      * it uses the absolute queue capacity instead of the max and is modified
      * by the userlimit and the userlimit factor as is the userlimit
      *
      */ 
     float effectiveUserLimit = Math.max(userLimit / 100.0f, 1.0f /    
       Math.max(getActiveUsersManager().getNumActiveUsers(), 1));
     
     return Resources.multiplyAndNormalizeUp( 
          resourceCalculator,
          absoluteCapacityResource, 
          maxAMResourcePerQueuePercent * effectiveUserLimit  *
            userLimitFactor, minimumAllocation);
  }

  private synchronized void activateApplications() {
    //limit of allowed resource usage for application masters
    Resource amLimit = getAMResourceLimit();
    Resource userAMLimit = getUserAMResourceLimit();
        
    for (Iterator<FiCaSchedulerApp> i=pendingApplications.iterator(); 
         i.hasNext(); ) {
      FiCaSchedulerApp application = i.next();
      
      // Check am resource limit
      Resource amIfStarted = 
        Resources.add(application.getAMResource(), queueUsage.getAMUsed());
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("application AMResource " + application.getAMResource() +
          " maxAMResourcePerQueuePercent " + maxAMResourcePerQueuePercent +
          " amLimit " + amLimit +
          " lastClusterResource " + lastClusterResource +
          " amIfStarted " + amIfStarted);
      }
      
      if (!Resources.lessThanOrEqual(
        resourceCalculator, lastClusterResource, amIfStarted, amLimit)) {
        if (getNumActiveApplications() < 1) {
          LOG.warn("maximum-am-resource-percent is insufficient to start a" +
            " single application in queue, it is likely set too low." +
            " skipping enforcement to allow at least one application to start"); 
        } else {
          LOG.info("not starting application as amIfStarted exceeds amLimit");
          continue;
        }
      }
      
      // Check user am resource limit
      
      User user = getUser(application.getUser());
      
      Resource userAmIfStarted = 
        Resources.add(application.getAMResource(),
          user.getConsumedAMResources());
        
      if (!Resources.lessThanOrEqual(
          resourceCalculator, lastClusterResource, userAmIfStarted, 
          userAMLimit)) {
        if (getNumActiveApplications() < 1) {
          LOG.warn("maximum-am-resource-percent is insufficient to start a" +
            " single application in queue for user, it is likely set too low." +
            " skipping enforcement to allow at least one application to start"); 
        } else {
          LOG.info("not starting application as amIfStarted exceeds " +
            "userAmLimit");
          continue;
        }
      }
      user.activateApplication();
      orderingPolicy.addSchedulableEntity(application);
      queueUsage.incAMUsed(application.getAMResource());
      user.getResourceUsage().incAMUsed(application.getAMResource());
      metrics.incAMUsed(application.getUser(), application.getAMResource());
      metrics.setAMResouceLimitForUser(application.getUser(), userAMLimit);
      i.remove();
      LOG.info("Application " + application.getApplicationId() +
          " from user: " + application.getUser() + 
          " activated in queue: " + getQueueName());
    }
  }
  
  private synchronized void addApplicationAttempt(FiCaSchedulerApp application,
      User user) {
    // Accept 
    user.submitApplication();
    pendingApplications.add(application);
    applicationAttemptMap.put(application.getApplicationAttemptId(), application);

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
  public void finishApplication(ApplicationId application, String user) {
    // Inform the activeUsersManager
    activeUsersManager.deactivateApplication(user, application);
    // Inform the parent queue
    getParent().finishApplication(application, user);
  }

  @Override
  public void finishApplicationAttempt(FiCaSchedulerApp application, String queue) {
    // Careful! Locking order is important!
    synchronized (this) {
      removeApplicationAttempt(application, getUser(application.getUser()));
    }
    getParent().finishApplicationAttempt(application, queue);
  }

  public synchronized void removeApplicationAttempt(
      FiCaSchedulerApp application, User user) {
    boolean wasActive =
      orderingPolicy.removeSchedulableEntity(application);
    if (!wasActive) {
      pendingApplications.remove(application);
    } else {
      queueUsage.decAMUsed(application.getAMResource());
      user.getResourceUsage().decAMUsed(application.getAMResource());
      metrics.decAMUsed(application.getUser(), application.getAMResource());
    }
    applicationAttemptMap.remove(application.getApplicationAttemptId());

    user.finishApplication(wasActive);
    if (user.getTotalApplications() == 0) {
      users.remove(application.getUser());
    }

    // Check if we can activate more applications
    activateApplications();

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

  private synchronized FiCaSchedulerApp getApplication(
      ApplicationAttemptId applicationAttemptId) {
    return applicationAttemptMap.get(applicationAttemptId);
  }
  
  @Override
  public synchronized CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits currentResourceLimits,
      SchedulingMode schedulingMode) {
    updateCurrentResourceLimits(currentResourceLimits, clusterResource);
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getNodeName()
        + " #applications=" + 
        orderingPolicy.getNumSchedulableEntities());
    }
    
    // Check for reserved resources
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      FiCaSchedulerApp application = 
          getApplication(reservedContainer.getApplicationAttemptId());
      synchronized (application) {
        return assignReservedContainer(application, node, reservedContainer,
            clusterResource, schedulingMode);
      }
    }
    
    // if our queue cannot access this node, just return
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY
        && !accessibleToPartition(node.getPartition())) {
      return NULL_ASSIGNMENT;
    }
    
    // Check if this queue need more resource, simply skip allocation if this
    // queue doesn't need more resources.
    if (!hasPendingResourceRequest(node.getPartition(),
        clusterResource, schedulingMode)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip this queue=" + getQueuePath()
            + ", because it doesn't need more resource, schedulingMode="
            + schedulingMode.name() + " node-partition=" + node.getPartition());
      }
      return NULL_ASSIGNMENT;
    }
    
    for (Iterator<FiCaSchedulerApp> assignmentIterator =
        orderingPolicy.getAssignmentIterator();
        assignmentIterator.hasNext();) {
      FiCaSchedulerApp application = assignmentIterator.next();
      if(LOG.isDebugEnabled()) {
        LOG.debug("pre-assignContainers for application "
        + application.getApplicationId());
        application.showRequests();
      }
      
      // Check if application needs more resource, skip if it doesn't need more.
      if (!application.hasPendingResourceRequest(resourceCalculator,
          node.getPartition(), clusterResource, schedulingMode)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip app_attempt=" + application.getApplicationAttemptId()
              + ", because it doesn't need more resource, schedulingMode="
              + schedulingMode.name() + " node-label=" + node.getPartition());
        }
        continue;
      }

      synchronized (application) {
        // Check if this resource is on the blacklist
        if (SchedulerAppUtils.isBlacklisted(application, node, LOG)) {
          continue;
        }
        
        // Schedule in priority order
        for (Priority priority : application.getPriorities()) {
          ResourceRequest anyRequest =
              application.getResourceRequest(priority, ResourceRequest.ANY);
          if (null == anyRequest) {
            continue;
          }
          
          // Required resource
          Resource required = anyRequest.getCapability();

          // Do we need containers at this 'priority'?
          if (application.getTotalRequiredResources(priority) <= 0) {
            continue;
          }
          
          // AM container allocation doesn't support non-exclusive allocation to
          // avoid painful of preempt an AM container
          if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
            RMAppAttempt rmAppAttempt =
                csContext.getRMContext().getRMApps()
                    .get(application.getApplicationId()).getCurrentAppAttempt();
            if (null == rmAppAttempt.getMasterContainer()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Skip allocating AM container to app_attempt="
                    + application.getApplicationAttemptId()
                    + ", don't allow to allocate AM container in non-exclusive mode");
              }
              break;
            }
          }
          
          // Is the node-label-expression of this offswitch resource request
          // matches the node's label?
          // If not match, jump to next priority.
          if (!SchedulerUtils.checkResourceRequestMatchingNodePartition(
              anyRequest, node.getPartition(), schedulingMode)) {
            continue;
          }
          
          if (!this.reservationsContinueLooking) {
            if (!shouldAllocOrReserveNewContainer(application, priority, required)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("doesn't need containers based on reservation algo!");
              }
              continue;
            }
          }
          
          // Compute user-limit & set headroom
          // Note: We compute both user-limit & headroom with the highest 
          //       priority request as the target. 
          //       This works since we never assign lower priority requests
          //       before all higher priority ones are serviced.
          Resource userLimit = 
              computeUserLimitAndSetHeadroom(application, clusterResource, 
                  required, node.getPartition(), schedulingMode);

          // Check queue max-capacity limit
          if (!super.canAssignToThisQueue(clusterResource, node.getPartition(),
              currentResourceLimits, required,
              application.getCurrentReservation(), schedulingMode)) {
            return NULL_ASSIGNMENT;
          }

          // Check user limit
          if (!canAssignToUser(clusterResource, application.getUser(), userLimit,
              application, node.getPartition(), currentResourceLimits)) {
            break;
          }

          // Inform the application it is about to get a scheduling opportunity
          application.addSchedulingOpportunity(priority);
          
          // Increase missed-non-partitioned-resource-request-opportunity.
          // This is to make sure non-partitioned-resource-request will prefer
          // to be allocated to non-partitioned nodes
          int missedNonPartitionedRequestSchedulingOpportunity = 0;
          if (anyRequest.getNodeLabelExpression().equals(
              RMNodeLabelsManager.NO_LABEL)) {
            missedNonPartitionedRequestSchedulingOpportunity =
                application
                    .addMissedNonPartitionedRequestSchedulingOpportunity(priority);
          }
          
          if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
            // Before doing allocation, we need to check scheduling opportunity to
            // make sure : non-partitioned resource request should be scheduled to
            // non-partitioned partition first.
            if (missedNonPartitionedRequestSchedulingOpportunity < scheduler
                .getNumClusterNodes()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Skip app_attempt="
                    + application.getApplicationAttemptId()
                    + " priority="
                    + priority
                    + " because missed-non-partitioned-resource-request"
                    + " opportunity under requred:"
                    + " Now=" + missedNonPartitionedRequestSchedulingOpportunity
                    + " required="
                    + scheduler.getNumClusterNodes());
              }

              break;
            }
          }
          
          // Try to schedule
          CSAssignment assignment =
            assignContainersOnNode(clusterResource, node, application, priority,
                null, schedulingMode, currentResourceLimits);

          // Did the application skip this node?
          if (assignment.getSkipped()) {
            // Don't count 'skipped nodes' as a scheduling opportunity!
            application.subtractSchedulingOpportunity(priority);
            continue;
          }
          
          // Did we schedule or reserve a container?
          Resource assigned = assignment.getResource();
          if (Resources.greaterThan(
              resourceCalculator, clusterResource, assigned, Resources.none())) {
            // Get reserved or allocated container from application
            RMContainer reservedOrAllocatedRMContainer =
                application.getRMContainer(assignment
                    .getAssignmentInformation()
                    .getFirstAllocatedOrReservedContainerId());

            // Book-keeping 
            // Note: Update headroom to account for current allocation too...
            allocateResource(clusterResource, application, assigned,
                node.getPartition(), reservedOrAllocatedRMContainer);
            
            // Don't reset scheduling opportunities for offswitch assignments
            // otherwise the app will be delayed for each non-local assignment.
            // This helps apps with many off-cluster requests schedule faster.
            if (assignment.getType() != NodeType.OFF_SWITCH) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Resetting scheduling opportunities");
              }
              application.resetSchedulingOpportunities(priority);
            }
            // Non-exclusive scheduling opportunity is different: we need reset
            // it every time to make sure non-labeled resource request will be
            // most likely allocated on non-labeled nodes first. 
            application.resetMissedNonPartitionedRequestSchedulingOpportunity(priority);
            
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
      FiCaSchedulerApp application, FiCaSchedulerNode node,
      RMContainer rmContainer, Resource clusterResource,
      SchedulingMode schedulingMode) {
    // Do we still need this reservation?
    Priority priority = rmContainer.getReservedPriority();
    if (application.getTotalRequiredResources(priority) == 0) {
      // Release
      return new CSAssignment(application, rmContainer);
    }

    // Try to assign if we have sufficient resources
    CSAssignment tmp =
        assignContainersOnNode(clusterResource, node, application, priority,
          rmContainer, schedulingMode, new ResourceLimits(Resources.none()));
    
    // Doesn't matter... since it's already charged for at time of reservation
    // "re-reservation" is *free*
    CSAssignment ret = new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
    if (tmp.getAssignmentInformation().getNumAllocations() > 0) {
      ret.setFulfilledReservation(true);
    }
    return ret;
  }
  
  protected Resource getHeadroom(User user, Resource queueCurrentLimit,
      Resource clusterResource, FiCaSchedulerApp application, Resource required) {
    return getHeadroom(user, queueCurrentLimit, clusterResource,
        computeUserLimit(application, clusterResource, required, user,
            RMNodeLabelsManager.NO_LABEL, SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY));
  }
  
  private Resource getHeadroom(User user, Resource currentResourceLimit,
      Resource clusterResource, Resource userLimit) {
    /** 
     * Headroom is:
     *    min(
     *        min(userLimit, queueMaxCap) - userConsumed,
     *        queueMaxCap - queueUsedResources
     *       )
     * 
     * ( which can be expressed as, 
     *  min (userLimit - userConsumed, queuMaxCap - userConsumed, 
     *    queueMaxCap - queueUsedResources)
     *  )
     *
     * given that queueUsedResources >= userConsumed, this simplifies to
     *
     * >> min (userlimit - userConsumed,   queueMaxCap - queueUsedResources) << 
     *
     */
    Resource headroom = 
      Resources.min(resourceCalculator, clusterResource,
        Resources.subtract(userLimit, user.getUsed()),
        Resources.subtract(currentResourceLimit, queueUsage.getUsed())
        );
    // Normalize it before return
    headroom =
        Resources.roundDown(resourceCalculator, headroom, minimumAllocation);
    return headroom;
  }
  
  private void setQueueResourceLimitsInfo(
      Resource clusterResource) {
    synchronized (queueResourceLimitsInfo) {
      queueResourceLimitsInfo.setQueueCurrentLimit(cachedResourceLimitsForHeadroom
          .getLimit());
      queueResourceLimitsInfo.setClusterResource(clusterResource);
    }
  }

  @Lock({LeafQueue.class, FiCaSchedulerApp.class})
  Resource computeUserLimitAndSetHeadroom(FiCaSchedulerApp application,
      Resource clusterResource, Resource required, String nodePartition,
      SchedulingMode schedulingMode) {
    String user = application.getUser();
    User queueUser = getUser(user);

    // Compute user limit respect requested labels,
    // TODO, need consider headroom respect labels also
    Resource userLimit =
        computeUserLimit(application, clusterResource, required,
            queueUser, nodePartition, schedulingMode);

    setQueueResourceLimitsInfo(clusterResource);
    
    Resource headroom =
        getHeadroom(queueUser, cachedResourceLimitsForHeadroom.getLimit(),
            clusterResource, userLimit);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Headroom calculation for user " + user + ": " + 
          " userLimit=" + userLimit + 
          " queueMaxAvailRes=" + cachedResourceLimitsForHeadroom.getLimit() +
          " consumed=" + queueUser.getUsed() + 
          " headroom=" + headroom);
    }
    
    CapacityHeadroomProvider headroomProvider = new CapacityHeadroomProvider(
      queueUser, this, application, required, queueResourceLimitsInfo);
    
    application.setHeadroomProvider(headroomProvider);

    metrics.setAvailableResourcesToUser(user, headroom);
    
    return userLimit;
  }
  
  @Lock(NoLock.class)
  private Resource computeUserLimit(FiCaSchedulerApp application,
      Resource clusterResource, Resource required, User user,
      String nodePartition, SchedulingMode schedulingMode) {
    // What is our current capacity? 
    // * It is equal to the max(required, queue-capacity) if
    //   we're running below capacity. The 'max' ensures that jobs in queues
    //   with miniscule capacity (< 1 slot) make progress
    // * If we're running over capacity, then its
    //   (usedResources + required) (which extra resources we are allocating)
    Resource queueCapacity =
        Resources.multiplyAndNormalizeUp(resourceCalculator,
            labelManager.getResourceByLabel(nodePartition, clusterResource),
            queueCapacities.getAbsoluteCapacity(nodePartition),
            minimumAllocation);

    // Allow progress for queues with miniscule capacity
    queueCapacity =
        Resources.max(
            resourceCalculator, clusterResource, 
            queueCapacity, 
            required);

    Resource currentCapacity =
        Resources.lessThan(resourceCalculator, clusterResource,
            queueUsage.getUsed(nodePartition), queueCapacity) ? queueCapacity
            : Resources.add(queueUsage.getUsed(nodePartition), required);
    
    // Never allow a single user to take more than the 
    // queue's configured capacity * user-limit-factor.
    // Also, the queue's configured capacity should be higher than 
    // queue-hard-limit * ulMin
    
    final int activeUsers = activeUsersManager.getNumActiveUsers();
    
    // User limit resource is determined by:
    // max{currentCapacity / #activeUsers, currentCapacity * user-limit-percentage%)
    Resource userLimitResource = Resources.max(
        resourceCalculator, clusterResource, 
        Resources.divideAndCeil(
            resourceCalculator, currentCapacity, activeUsers),
        Resources.divideAndCeil(
            resourceCalculator, 
            Resources.multiplyAndRoundDown(
                currentCapacity, userLimit), 
            100)
        );
    
    // User limit is capped by maxUserLimit
    // - maxUserLimit = queueCapacity * user-limit-factor (RESPECT_PARTITION_EXCLUSIVITY)
    // - maxUserLimit = total-partition-resource (IGNORE_PARTITION_EXCLUSIVITY)
    //
    // In IGNORE_PARTITION_EXCLUSIVITY mode, if a queue cannot access a
    // partition, its guaranteed resource on that partition is 0. And
    // user-limit-factor computation is based on queue's guaranteed capacity. So
    // we will not cap user-limit as well as used resource when doing
    // IGNORE_PARTITION_EXCLUSIVITY allocation.
    Resource maxUserLimit = Resources.none();
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY) {
      maxUserLimit =
          Resources.multiplyAndRoundDown(queueCapacity, userLimitFactor);
    } else if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      maxUserLimit =
          labelManager.getResourceByLabel(nodePartition, clusterResource);
    }
    
    // Cap final user limit with maxUserLimit
    userLimitResource =
        Resources.roundUp(
            resourceCalculator, 
            Resources.min(
                resourceCalculator, clusterResource,   
                  userLimitResource,
                  maxUserLimit
                ), 
            minimumAllocation);

    if (LOG.isDebugEnabled()) {
      String userName = application.getUser();
      LOG.debug("User limit computation for " + userName + 
          " in queue " + getQueueName() +
          " userLimitPercent=" + userLimit +
          " userLimitFactor=" + userLimitFactor +
          " required: " + required + 
          " consumed: " + user.getUsed() + 
          " user-limit-resource: " + userLimitResource +
          " queueCapacity: " + queueCapacity + 
          " qconsumed: " + queueUsage.getUsed() +
          " currentCapacity: " + currentCapacity +
          " activeUsers: " + activeUsers +
          " clusterCapacity: " + clusterResource
      );
    }
    user.setUserResourceLimit(userLimitResource);
    return userLimitResource;
  }
  
  @Private
  protected synchronized boolean canAssignToUser(Resource clusterResource,
      String userName, Resource limit, FiCaSchedulerApp application,
      String nodePartition, ResourceLimits currentResoureLimits) {
    User user = getUser(userName);

    // Note: We aren't considering the current request since there is a fixed
    // overhead of the AM, but it's a > check, not a >= check, so...
    if (Resources
        .greaterThan(resourceCalculator, clusterResource,
            user.getUsed(nodePartition),
            limit)) {
      // if enabled, check to see if could we potentially use this node instead
      // of a reserved node if the application has reserved containers
      if (this.reservationsContinueLooking &&
          nodePartition.equals(CommonNodeLabelsManager.NO_LABEL)) {
        if (Resources.lessThanOrEqual(
            resourceCalculator,
            clusterResource,
            Resources.subtract(user.getUsed(),application.getCurrentReservation()),
            limit)) {

          if (LOG.isDebugEnabled()) {
            LOG.debug("User " + userName + " in queue " + getQueueName()
                + " will exceed limit based on reservations - " + " consumed: "
                + user.getUsed() + " reserved: "
                + application.getCurrentReservation() + " limit: " + limit);
          }
          Resource amountNeededToUnreserve = Resources.subtract(user.getUsed(nodePartition), limit);
          // we can only acquire a new container if we unreserve first since we ignored the
          // user limit. Choose the max of user limit or what was previously set by max
          // capacity.
          currentResoureLimits.setAmountNeededUnreserve(
              Resources.max(resourceCalculator, clusterResource,
                  currentResoureLimits.getAmountNeededUnreserve(), amountNeededToUnreserve));
          return true;
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("User " + userName + " in queue " + getQueueName()
            + " will exceed limit - " + " consumed: "
            + user.getUsed(nodePartition) + " limit: " + limit);
      }
      return false;
    }
    return true;
  }

  boolean shouldAllocOrReserveNewContainer(FiCaSchedulerApp application,
      Priority priority, Resource required) {
    int requiredContainers = application.getTotalRequiredResources(priority);
    int reservedContainers = application.getNumReservedContainers(priority);
    int starvation = 0;
    if (reservedContainers > 0) {
      float nodeFactor = 
          Resources.ratio(
              resourceCalculator, required, getMaximumAllocation()
              );
      
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
            " minAllocFactor=" + getMinimumAllocationFactor() +
            " starvation=" + starvation);
      }
    }
    return (((starvation + requiredContainers) - reservedContainers) > 0);
  }

  private CSAssignment assignContainersOnNode(Resource clusterResource,
      FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, SchedulingMode schedulingMode,
      ResourceLimits currentResoureLimits) {

    CSAssignment assigned;

    NodeType requestType = null;
    MutableObject allocatedContainer = new MutableObject();
    // Data-local
    ResourceRequest nodeLocalResourceRequest =
        application.getResourceRequest(priority, node.getNodeName());
    if (nodeLocalResourceRequest != null) {
      requestType = NodeType.NODE_LOCAL;
      assigned =
          assignNodeLocalContainers(clusterResource, nodeLocalResourceRequest, 
            node, application, priority, reservedContainer,
            allocatedContainer, schedulingMode, currentResoureLimits);
      if (Resources.greaterThan(resourceCalculator, clusterResource,
        assigned.getResource(), Resources.none())) {

        //update locality statistics
        if (allocatedContainer.getValue() != null) {
          application.incNumAllocatedContainers(NodeType.NODE_LOCAL,
            requestType);
        }
        assigned.setType(NodeType.NODE_LOCAL);
        return assigned;
      }
    }

    // Rack-local
    ResourceRequest rackLocalResourceRequest =
        application.getResourceRequest(priority, node.getRackName());
    if (rackLocalResourceRequest != null) {
      if (!rackLocalResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }

      if (requestType != NodeType.NODE_LOCAL) {
        requestType = NodeType.RACK_LOCAL;
      }

      assigned = 
          assignRackLocalContainers(clusterResource, rackLocalResourceRequest, 
            node, application, priority, reservedContainer,
            allocatedContainer, schedulingMode, currentResoureLimits);
      if (Resources.greaterThan(resourceCalculator, clusterResource,
        assigned.getResource(), Resources.none())) {

        //update locality statistics
        if (allocatedContainer.getValue() != null) {
          application.incNumAllocatedContainers(NodeType.RACK_LOCAL,
            requestType);
        }
        assigned.setType(NodeType.RACK_LOCAL);
        return assigned;
      }
    }
    
    // Off-switch
    ResourceRequest offSwitchResourceRequest =
        application.getResourceRequest(priority, ResourceRequest.ANY);
    if (offSwitchResourceRequest != null) {
      if (!offSwitchResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }
      if (requestType != NodeType.NODE_LOCAL
          && requestType != NodeType.RACK_LOCAL) {
        requestType = NodeType.OFF_SWITCH;
      }

      assigned =
          assignOffSwitchContainers(clusterResource, offSwitchResourceRequest,
            node, application, priority, reservedContainer,
            allocatedContainer, schedulingMode, currentResoureLimits);

      // update locality statistics
      if (allocatedContainer.getValue() != null) {
        application.incNumAllocatedContainers(NodeType.OFF_SWITCH, requestType);
      }
      assigned.setType(NodeType.OFF_SWITCH);
      return assigned;
    }
    
    return SKIP_ASSIGNMENT;
  }

  @Private
  protected boolean findNodeToUnreserve(Resource clusterResource,
      FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
      Resource minimumUnreservedResource) {
    // need to unreserve some other container first
    NodeId idToUnreserve =
        application.getNodeIdToUnreserve(priority, minimumUnreservedResource,
            resourceCalculator, clusterResource);
    if (idToUnreserve == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("checked to see if could unreserve for app but nothing "
            + "reserved that matches for this app");
      }
      return false;
    }
    FiCaSchedulerNode nodeToUnreserve = scheduler.getNode(idToUnreserve);
    if (nodeToUnreserve == null) {
      LOG.error("node to unreserve doesn't exist, nodeid: " + idToUnreserve);
      return false;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("unreserving for app: " + application.getApplicationId()
        + " on nodeId: " + idToUnreserve
        + " in order to replace reserved application and place it on node: "
        + node.getNodeID() + " needing: " + minimumUnreservedResource);
    }

    // headroom
    Resources.addTo(application.getHeadroom(), nodeToUnreserve
        .getReservedContainer().getReservedResource());

    // Make sure to not have completedContainers sort the queues here since
    // we are already inside an iterator loop for the queues and this would
    // cause an concurrent modification exception.
    completedContainer(clusterResource, application, nodeToUnreserve,
        nodeToUnreserve.getReservedContainer(),
        SchedulerUtils.createAbnormalContainerStatus(nodeToUnreserve
            .getReservedContainer().getContainerId(),
            SchedulerUtils.UNRESERVED_CONTAINER),
        RMContainerEventType.RELEASED, null, false);
    return true;
  }

  private CSAssignment assignNodeLocalContainers(Resource clusterResource,
      ResourceRequest nodeLocalResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(application, priority, node, NodeType.NODE_LOCAL, 
        reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
          nodeLocalResourceRequest, NodeType.NODE_LOCAL, reservedContainer,
          allocatedContainer, schedulingMode, currentResoureLimits);
    }

    return new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
  }

  private CSAssignment assignRackLocalContainers(Resource clusterResource,
      ResourceRequest rackLocalResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(application, priority, node, NodeType.RACK_LOCAL,
        reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
          rackLocalResourceRequest, NodeType.RACK_LOCAL, reservedContainer,
          allocatedContainer, schedulingMode, currentResoureLimits);
    }

    return new CSAssignment(Resources.none(), NodeType.RACK_LOCAL);
  }

  private CSAssignment assignOffSwitchContainers(Resource clusterResource,
      ResourceRequest offSwitchResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, MutableObject allocatedContainer,
      SchedulingMode schedulingMode, ResourceLimits currentResoureLimits) {
    if (canAssign(application, priority, node, NodeType.OFF_SWITCH,
        reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
          offSwitchResourceRequest, NodeType.OFF_SWITCH, reservedContainer,
          allocatedContainer, schedulingMode, currentResoureLimits);
    }
    
    return new CSAssignment(Resources.none(), NodeType.OFF_SWITCH);
  }
  
  private int getActualNodeLocalityDelay() {
    return Math.min(scheduler.getNumClusterNodes(), getNodeLocalityDelay());
  }

  boolean canAssign(FiCaSchedulerApp application, Priority priority, 
      FiCaSchedulerNode node, NodeType type, RMContainer reservedContainer) {

    // Clearly we need containers for this application...
    if (type == NodeType.OFF_SWITCH) {
      if (reservedContainer != null) {
        return true;
      }

      // 'Delay' off-switch
      ResourceRequest offSwitchRequest = 
          application.getResourceRequest(priority, ResourceRequest.ANY);
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
      return getActualNodeLocalityDelay() < missedOpportunities;
    }

    // Check if we need containers on this host
    if (type == NodeType.NODE_LOCAL) {
      // Now check if we need containers on this host...
      ResourceRequest nodeLocalRequest = 
        application.getResourceRequest(priority, node.getNodeName());
      if (nodeLocalRequest != null) {
        return nodeLocalRequest.getNumContainers() > 0;
      }
    }

    return false;
  }
  
  private Container getContainer(RMContainer rmContainer, 
      FiCaSchedulerApp application, FiCaSchedulerNode node, 
      Resource capability, Priority priority) {
    return (rmContainer != null) ? rmContainer.getContainer() :
      createContainer(application, node, capability, priority);
  }

  Container createContainer(FiCaSchedulerApp application, FiCaSchedulerNode node, 
      Resource capability, Priority priority) {
  
    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId = BuilderUtils.newContainerId(application
        .getApplicationAttemptId(), application.getNewContainerId());
  
    // Create the container
    return BuilderUtils.newContainer(containerId, nodeId, node.getRMNode()
      .getHttpAddress(), capability, priority, null);

  }


  private CSAssignment assignContainer(Resource clusterResource, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority, 
      ResourceRequest request, NodeType type, RMContainer rmContainer,
      MutableObject createdContainer, SchedulingMode schedulingMode,
      ResourceLimits currentResoureLimits) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getNodeName()
        + " application=" + application.getApplicationId()
        + " priority=" + priority.getPriority()
        + " request=" + request + " type=" + type);
    }
    
    // check if the resource request can access the label
    if (!SchedulerUtils.checkResourceRequestMatchingNodePartition(request,
        node.getPartition(), schedulingMode)) {
      // this is a reserved container, but we cannot allocate it now according
      // to label not match. This can be caused by node label changed
      // We should un-reserve this container.
      if (rmContainer != null) {
        unreserve(application, priority, node, rmContainer);
      }
      return new CSAssignment(Resources.none(), type);
    }
    
    Resource capability = request.getCapability();
    Resource available = node.getAvailableResource();
    Resource totalResource = node.getTotalResource();

    if (!Resources.lessThanOrEqual(resourceCalculator, clusterResource,
        capability, totalResource)) {
      LOG.warn("Node : " + node.getNodeID()
          + " does not have sufficient resource for request : " + request
          + " node total capability : " + node.getTotalResource());
      return new CSAssignment(Resources.none(), type);
    }

    assert Resources.greaterThan(
        resourceCalculator, clusterResource, available, Resources.none());

    // Create the container if necessary
    Container container = 
        getContainer(rmContainer, application, node, capability, priority);
  
    // something went wrong getting/creating the container 
    if (container == null) {
      LOG.warn("Couldn't get container for allocation!");
      return new CSAssignment(Resources.none(), type);
    }

    boolean shouldAllocOrReserveNewContainer = shouldAllocOrReserveNewContainer(
        application, priority, capability);

    // Can we allocate a container on this node?
    int availableContainers = 
        resourceCalculator.computeAvailableContainers(available, capability);

    boolean needToUnreserve = Resources.greaterThan(resourceCalculator,clusterResource,
        currentResoureLimits.getAmountNeededUnreserve(), Resources.none());

    if (availableContainers > 0) {
      // Allocate...

      // Did we previously reserve containers at this 'priority'?
      if (rmContainer != null) {
        unreserve(application, priority, node, rmContainer);
      } else if (this.reservationsContinueLooking && node.getLabels().isEmpty()) {
        // when reservationsContinueLooking is set, we may need to unreserve
        // some containers to meet this queue, its parents', or the users' resource limits.
        // TODO, need change here when we want to support continuous reservation
        // looking for labeled partitions.
        if (!shouldAllocOrReserveNewContainer || needToUnreserve) {
          // If we shouldn't allocate/reserve new container then we should unreserve one the same
          // size we are asking for since the currentResoureLimits.getAmountNeededUnreserve
          // could be zero. If the limit was hit then use the amount we need to unreserve to be
          // under the limit.
          Resource amountToUnreserve = capability;
          if (needToUnreserve) {
            amountToUnreserve = currentResoureLimits.getAmountNeededUnreserve();
          }
          boolean containerUnreserved =
              findNodeToUnreserve(clusterResource, node, application, priority,
                  amountToUnreserve);
          // When (minimum-unreserved-resource > 0 OR we cannot allocate new/reserved 
          // container (That means we *have to* unreserve some resource to
          // continue)). If we failed to unreserve some resource, we can't continue.
          if (!containerUnreserved) {
            return new CSAssignment(Resources.none(), type);
          }
        }
      }

      // Inform the application
      RMContainer allocatedContainer = 
          application.allocate(type, node, priority, request, container);

      // Does the application need this resource?
      if (allocatedContainer == null) {
        return new CSAssignment(Resources.none(), type);
      }

      // Inform the node
      node.allocateContainer(allocatedContainer);
            
      // Inform the ordering policy
      orderingPolicy.containerAllocated(application, allocatedContainer);

      LOG.info("assignedContainer" +
          " application attempt=" + application.getApplicationAttemptId() +
          " container=" + container + 
          " queue=" + this + 
          " clusterResource=" + clusterResource);
      createdContainer.setValue(allocatedContainer);
      CSAssignment assignment = new CSAssignment(container.getResource(), type);
      assignment.getAssignmentInformation().addAllocationDetails(
        container.getId(), getQueuePath());
      assignment.getAssignmentInformation().incrAllocations();
      Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
        container.getResource());
      return assignment;
    } else {
      // if we are allowed to allocate but this node doesn't have space, reserve it or
      // if this was an already a reserved container, reserve it again
      if (shouldAllocOrReserveNewContainer || rmContainer != null) {

        if (reservationsContinueLooking && rmContainer == null) {
          // we could possibly ignoring queue capacity or user limits when
          // reservationsContinueLooking is set. Make sure we didn't need to unreserve
          // one.
          if (needToUnreserve) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("we needed to unreserve to be able to allocate");
            }
            return new CSAssignment(Resources.none(), type);
          }
        }

        // Reserve by 'charging' in advance...
        reserve(application, priority, node, rmContainer, container);

        LOG.info("Reserved container " + 
            " application=" + application.getApplicationId() + 
            " resource=" + request.getCapability() + 
            " queue=" + this.toString() + 
            " usedCapacity=" + getUsedCapacity() + 
            " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + 
            " used=" + queueUsage.getUsed() +
            " cluster=" + clusterResource);
        CSAssignment assignment =
            new CSAssignment(request.getCapability(), type);
        assignment.getAssignmentInformation().addReservationDetails(
          container.getId(), getQueuePath());
        assignment.getAssignmentInformation().incrReservations();
        Resources.addTo(assignment.getAssignmentInformation().getReserved(),
          request.getCapability());
        return assignment;
      }
      return new CSAssignment(Resources.none(), type);
    }
  }

  private void reserve(FiCaSchedulerApp application, Priority priority, 
      FiCaSchedulerNode node, RMContainer rmContainer, Container container) {
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

  private boolean unreserve(FiCaSchedulerApp application, Priority priority,
      FiCaSchedulerNode node, RMContainer rmContainer) {
    // Done with the reservation?
    if (application.unreserve(node, priority)) {
      node.unreserveResource(application);

      // Update reserved metrics
      getMetrics().unreserveResource(application.getUser(),
          rmContainer.getContainer().getResource());
      return true;
    }
    return false;
  }

  @Override
  public void completedContainer(Resource clusterResource, 
      FiCaSchedulerApp application, FiCaSchedulerNode node, RMContainer rmContainer, 
      ContainerStatus containerStatus, RMContainerEventType event, CSQueue childQueue,
      boolean sortQueues) {
    if (application != null) {

      boolean removed = false;

      // Careful! Locking order is important!
      synchronized (this) {

        Container container = rmContainer.getContainer();

        // Inform the application & the node
        // Note: It's safe to assume that all state changes to RMContainer
        // happen under scheduler's lock... 
        // So, this is, in effect, a transaction across application & node
        if (rmContainer.getState() == RMContainerState.RESERVED) {
          removed = unreserve(application, rmContainer.getReservedPriority(),
              node, rmContainer);
        } else {
          removed =
              application.containerCompleted(rmContainer, containerStatus,
                  event, node.getPartition());
          
          node.releaseContainer(container);
        }

        // Book-keeping
        if (removed) {
          
          // Inform the ordering policy
          orderingPolicy.containerReleased(application, rmContainer);
          
          releaseResource(clusterResource, application,
              container.getResource(), node.getPartition(), rmContainer);
          LOG.info("completedContainer" +
              " container=" + container +
              " queue=" + this +
              " cluster=" + clusterResource);
        }
      }

      if (removed) {
        // Inform the parent queue _outside_ of the leaf-queue lock
        getParent().completedContainer(clusterResource, application, node,
          rmContainer, null, event, this, sortQueues);
      }
    }
  }

  synchronized void allocateResource(Resource clusterResource,
      SchedulerApplicationAttempt application, Resource resource,
      String nodePartition, RMContainer rmContainer) {
    super.allocateResource(clusterResource, resource, nodePartition);
    
    // handle ignore exclusivity container
    if (null != rmContainer && rmContainer.getNodeLabelExpression().equals(
        RMNodeLabelsManager.NO_LABEL)
        && !nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      TreeSet<RMContainer> rmContainers = null;
      if (null == (rmContainers =
          ignorePartitionExclusivityRMContainers.get(nodePartition))) {
        rmContainers = new TreeSet<>();
        ignorePartitionExclusivityRMContainers.put(nodePartition, rmContainers);
      }
      rmContainers.add(rmContainer);
    }

    // Update user metrics
    String userName = application.getUser();
    User user = getUser(userName);
    user.assignContainer(resource, nodePartition);
    // Note this is a bit unconventional since it gets the object and modifies
    // it here, rather then using set routine
    Resources.subtractFrom(application.getHeadroom(), resource); // headroom
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
    
    if (LOG.isDebugEnabled()) {
      LOG.info(getQueueName() + 
          " user=" + userName + 
          " used=" + queueUsage.getUsed() + " numContainers=" + numContainers +
          " headroom = " + application.getHeadroom() +
          " user-resources=" + user.getUsed()
          );
    }
  }

  synchronized void releaseResource(Resource clusterResource,
      FiCaSchedulerApp application, Resource resource, String nodePartition,
      RMContainer rmContainer) {
    super.releaseResource(clusterResource, resource, nodePartition);
    
    // handle ignore exclusivity container
    if (null != rmContainer && rmContainer.getNodeLabelExpression().equals(
        RMNodeLabelsManager.NO_LABEL)
        && !nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      if (ignorePartitionExclusivityRMContainers.containsKey(nodePartition)) {
        Set<RMContainer> rmContainers =
            ignorePartitionExclusivityRMContainers.get(nodePartition);
        rmContainers.remove(rmContainer);
        if (rmContainers.isEmpty()) {
          ignorePartitionExclusivityRMContainers.remove(nodePartition);
        }
      }
    }

    // Update user metrics
    String userName = application.getUser();
    User user = getUser(userName);
    user.releaseContainer(resource, nodePartition);
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
      
    LOG.info(getQueueName() + 
        " used=" + queueUsage.getUsed() + " numContainers=" + numContainers + 
        " user=" + userName + " user-resources=" + user.getUsed());
  }
  
  private void updateAbsoluteCapacityResource(Resource clusterResource) {
    absoluteCapacityResource =
        Resources.multiplyAndNormalizeUp(resourceCalculator, labelManager
            .getResourceByLabel(RMNodeLabelsManager.NO_LABEL, clusterResource),
            queueCapacities.getAbsoluteCapacity(), minimumAllocation);
  }
  
  private void updateCurrentResourceLimits(
      ResourceLimits currentResourceLimits, Resource clusterResource) {
    // TODO: need consider non-empty node labels when resource limits supports
    // node labels
    // Even if ParentQueue will set limits respect child's max queue capacity,
    // but when allocating reserved container, CapacityScheduler doesn't do
    // this. So need cap limits by queue's max capacity here.
    this.cachedResourceLimitsForHeadroom = new ResourceLimits(currentResourceLimits.getLimit());
    Resource queueMaxResource =
        Resources.multiplyAndNormalizeDown(resourceCalculator, labelManager
            .getResourceByLabel(RMNodeLabelsManager.NO_LABEL, clusterResource),
            queueCapacities
                .getAbsoluteMaximumCapacity(RMNodeLabelsManager.NO_LABEL),
            minimumAllocation);
    this.cachedResourceLimitsForHeadroom.setLimit(Resources.min(resourceCalculator,
        clusterResource, queueMaxResource, currentResourceLimits.getLimit()));
  }

  @Override
  public synchronized void updateClusterResource(Resource clusterResource,
      ResourceLimits currentResourceLimits) {
    updateCurrentResourceLimits(currentResourceLimits, clusterResource);
    lastClusterResource = clusterResource;
    updateAbsoluteCapacityResource(clusterResource);
    
    // Update headroom info based on new cluster resource value
    // absoluteMaxCapacity now,  will be replaced with absoluteMaxAvailCapacity
    // during allocation
    setQueueResourceLimitsInfo(clusterResource);
    
    // Update metrics
    CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
        minimumAllocation, this, labelManager, null);

    // queue metrics are updated, more resource may be available
    // activate the pending applications if possible
    activateApplications();

    // Update application properties
    for (FiCaSchedulerApp application :
      orderingPolicy.getSchedulableEntities()) {
      synchronized (application) {
        computeUserLimitAndSetHeadroom(application, clusterResource,
            Resources.none(), RMNodeLabelsManager.NO_LABEL,
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
      }
    }
  }

  @VisibleForTesting
  public static class User {
    ResourceUsage userResourceUsage = new ResourceUsage();
    volatile Resource userResourceLimit = Resource.newInstance(0, 0);
    int pendingApplications = 0;
    int activeApplications = 0;

    public ResourceUsage getResourceUsage() {
      return userResourceUsage;
    }
    
    public Resource getUsed() {
      return userResourceUsage.getUsed();
    }
    
    public Resource getUsed(String label) {
      return userResourceUsage.getUsed(label);
    }

    public int getPendingApplications() {
      return pendingApplications;
    }

    public int getActiveApplications() {
      return activeApplications;
    }
    
    public Resource getConsumedAMResources() {
      return userResourceUsage.getAMUsed();
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

    public void assignContainer(Resource resource, String nodePartition) {
      userResourceUsage.incUsed(nodePartition, resource);
    }

    public void releaseContainer(Resource resource, String nodePartition) {
      userResourceUsage.decUsed(nodePartition, resource);
    }

    public Resource getUserResourceLimit() {
      return userResourceLimit;
    }

    public void setUserResourceLimit(Resource userResourceLimit) {
      this.userResourceLimit = userResourceLimit;
    }
  }

  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt attempt, RMContainer rmContainer) {
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }
    // Careful! Locking order is important! 
    synchronized (this) {
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource, attempt, rmContainer.getContainer()
          .getResource(), node.getPartition(), rmContainer);
    }
    getParent().recoverContainer(clusterResource, attempt, rmContainer);
  }
  
  /**
   * Obtain (read-only) collection of active applications.
   */
  public Collection<FiCaSchedulerApp> getApplications() {
    return orderingPolicy.getSchedulableEntities();
  }

  // return a single Resource capturing the overal amount of pending resources
  public synchronized Resource getTotalResourcePending() {
    Resource ret = BuilderUtils.newResource(0, 0);
    for (FiCaSchedulerApp f : 
      orderingPolicy.getSchedulableEntities()) {
      Resources.addTo(ret, f.getTotalPendingRequests());
    }
    return ret;
  }

  @Override
  public synchronized void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    for (FiCaSchedulerApp pendingApp : pendingApplications) {
      apps.add(pendingApp.getApplicationAttemptId());
    }
    for (FiCaSchedulerApp app : 
      orderingPolicy.getSchedulableEntities()) {
      apps.add(app.getApplicationAttemptId());
    }
  }

  @Override
  public void attachContainer(Resource clusterResource,
      FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource, application, rmContainer.getContainer()
          .getResource(), node.getPartition(), rmContainer);
      LOG.info("movedContainer" + " container=" + rmContainer.getContainer()
          + " resource=" + rmContainer.getContainer().getResource()
          + " queueMoveIn=" + this + " usedCapacity=" + getUsedCapacity()
          + " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + " used="
          + queueUsage.getUsed() + " cluster=" + clusterResource);
      // Inform the parent queue
      getParent().attachContainer(clusterResource, application, rmContainer);
    }
  }

  @Override
  public void detachContainer(Resource clusterResource,
      FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      releaseResource(clusterResource, application, rmContainer.getContainer()
          .getResource(), node.getPartition(), rmContainer);
      LOG.info("movedContainer" + " container=" + rmContainer.getContainer()
          + " resource=" + rmContainer.getContainer().getResource()
          + " queueMoveOut=" + this + " usedCapacity=" + getUsedCapacity()
          + " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + " used="
          + queueUsage.getUsed() + " cluster=" + clusterResource);
      // Inform the parent queue
      getParent().detachContainer(clusterResource, application, rmContainer);
    }
  }
  
  /**
   * return all ignored partition exclusivity RMContainers in the LeafQueue, this
   * will be used by preemption policy, and use of return
   * ignorePartitionExclusivityRMContainer should protected by LeafQueue
   * synchronized lock
   */
  public synchronized Map<String, TreeSet<RMContainer>>
      getIgnoreExclusivityRMContainers() {
    return ignorePartitionExclusivityRMContainers;
  }

  public void setCapacity(float capacity) {
    queueCapacities.setCapacity(capacity);
  }

  public void setAbsoluteCapacity(float absoluteCapacity) {
    queueCapacities.setAbsoluteCapacity(absoluteCapacity);
  }

  public void setMaxApplications(int maxApplications) {
    this.maxApplications = maxApplications;
  }
  
  public synchronized OrderingPolicy<FiCaSchedulerApp>
      getOrderingPolicy() {
    return orderingPolicy;
  }
  
  public synchronized void setOrderingPolicy(
      OrderingPolicy<FiCaSchedulerApp> orderingPolicy) {
   orderingPolicy.addAllSchedulableEntities(
     this.orderingPolicy.getSchedulableEntities()
     );
    this.orderingPolicy = orderingPolicy;
  }
  
  /*
   * Holds shared values used by all applications in
   * the queue to calculate headroom on demand
   */
  static class QueueResourceLimitsInfo {
    private Resource queueCurrentLimit;
    private Resource clusterResource;
    
    public void setQueueCurrentLimit(Resource currentLimit) {
      this.queueCurrentLimit = currentLimit;
    }
    
    public Resource getQueueCurrentLimit() {
      return queueCurrentLimit;
    }
    
    public void setClusterResource(Resource clusterResource) {
      this.clusterResource = clusterResource;
    }
    
    public Resource getClusterResource() {
      return clusterResource;
    }
  }
}
