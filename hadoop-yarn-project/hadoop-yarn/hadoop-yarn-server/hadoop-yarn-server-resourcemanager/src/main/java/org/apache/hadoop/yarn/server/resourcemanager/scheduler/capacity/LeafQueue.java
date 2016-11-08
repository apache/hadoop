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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedContainerChangeRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.AMState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.KillableContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.PlacementSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.PlacementSetUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicyForPendingApps;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.server.utils.Lock.NoLock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

@Private
@Unstable
public class LeafQueue extends AbstractCSQueue {
  private static final Log LOG = LogFactory.getLog(LeafQueue.class);

  private float absoluteUsedCapacity = 0.0f;
  private volatile int userLimit;
  private volatile float userLimitFactor;

  protected int maxApplications;
  protected volatile int maxApplicationsPerUser;
  
  private float maxAMResourcePerQueuePercent;

  private volatile int nodeLocalityDelay;
  private volatile boolean rackLocalityFullReset;

  Map<ApplicationAttemptId, FiCaSchedulerApp> applicationAttemptMap =
      new ConcurrentHashMap<>();

  private Priority defaultAppPriorityPerQueue;

  private final OrderingPolicy<FiCaSchedulerApp> pendingOrderingPolicy;

  private volatile float minimumAllocationFactor;

  private Map<String, User> users = new ConcurrentHashMap<>();

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private CapacitySchedulerContext scheduler;
  
  private final ActiveUsersManager activeUsersManager;

  // cache last cluster resource to compute actual capacity
  private Resource lastClusterResource = Resources.none();

  private final QueueResourceLimitsInfo queueResourceLimitsInfo =
      new QueueResourceLimitsInfo();

  private volatile ResourceLimits cachedResourceLimitsForHeadroom = null;

  private volatile OrderingPolicy<FiCaSchedulerApp> orderingPolicy = null;

  // Summation of consumed ratios for all users in queue
  private float totalUserConsumedRatio = 0;
  private UsageRatios qUsageRatios;

  // record all ignore partition exclusivityRMContainer, this will be used to do
  // preemption, key is the partition of the RMContainer allocated on
  private Map<String, TreeSet<RMContainer>> ignorePartitionExclusivityRMContainers =
      new ConcurrentHashMap<>();

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public LeafQueue(CapacitySchedulerContext cs,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);
    this.scheduler = cs;

    this.activeUsersManager = new ActiveUsersManager(metrics);

    // One time initialization is enough since it is static ordering policy
    this.pendingOrderingPolicy = new FifoOrderingPolicyForPendingApps();

    qUsageRatios = new UsageRatios();

    if(LOG.isDebugEnabled()) {
      LOG.debug("LeafQueue:" + " name=" + queueName
        + ", fullname=" + getQueuePath());
    }

    setupQueueConfigs(cs.getClusterResource());
  }

  protected void setupQueueConfigs(Resource clusterResource)
      throws IOException {
    try {
      writeLock.lock();
      super.setupQueueConfigs(clusterResource);

      this.lastClusterResource = clusterResource;

      this.cachedResourceLimitsForHeadroom = new ResourceLimits(
          clusterResource);

      // Initialize headroom info, also used for calculating application
      // master resource limits.  Since this happens during queue initialization
      // and all queues may not be realized yet, we'll use (optimistic)
      // absoluteMaxCapacity (it will be replaced with the more accurate
      // absoluteMaxAvailCapacity during headroom/userlimit/allocation events)
      setQueueResourceLimitsInfo(clusterResource);

      CapacitySchedulerConfiguration conf = csContext.getConfiguration();

      setOrderingPolicy(
          conf.<FiCaSchedulerApp>getOrderingPolicy(getQueuePath()));

      userLimit = conf.getUserLimit(getQueuePath());
      userLimitFactor = conf.getUserLimitFactor(getQueuePath());

      maxApplications = conf.getMaximumApplicationsPerQueue(getQueuePath());
      if (maxApplications < 0) {
        int maxSystemApps = conf.getMaximumSystemApplications();
        maxApplications =
            (int) (maxSystemApps * queueCapacities.getAbsoluteCapacity());
      }
      maxApplicationsPerUser = Math.min(maxApplications,
          (int) (maxApplications * (userLimit / 100.0f) * userLimitFactor));

      maxAMResourcePerQueuePercent =
          conf.getMaximumApplicationMasterResourcePerQueuePercent(
              getQueuePath());

      if (!SchedulerUtils.checkQueueLabelExpression(this.accessibleLabels,
          this.defaultLabelExpression, null)) {
        throw new IOException(
            "Invalid default label expression of " + " queue=" + getQueueName()
                + " doesn't have permission to access all labels "
                + "in default label expression. labelExpression of resource request="
                + (this.defaultLabelExpression == null ?
                "" :
                this.defaultLabelExpression) + ". Queue labels=" + (
                getAccessibleNodeLabels() == null ?
                    "" :
                    StringUtils
                        .join(getAccessibleNodeLabels().iterator(), ',')));
      }

      nodeLocalityDelay = conf.getNodeLocalityDelay();
      rackLocalityFullReset = conf.getRackLocalityFullReset();

      // re-init this since max allocation could have changed
      this.minimumAllocationFactor = Resources.ratio(resourceCalculator,
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

      defaultAppPriorityPerQueue = Priority.newInstance(
          conf.getDefaultApplicationPriorityConfPerQueue(getQueuePath()));

      LOG.info(
          "Initializing " + queueName + "\n" + "capacity = " + queueCapacities
              .getCapacity() + " [= (float) configuredCapacity / 100 ]" + "\n"
              + "absoluteCapacity = " + queueCapacities.getAbsoluteCapacity()
              + " [= parentAbsoluteCapacity * capacity ]" + "\n"
              + "maxCapacity = " + queueCapacities.getMaximumCapacity()
              + " [= configuredMaxCapacity ]" + "\n" + "absoluteMaxCapacity = "
              + queueCapacities.getAbsoluteMaximumCapacity()
              + " [= 1.0 maximumCapacity undefined, "
              + "(parentAbsoluteMaxCapacity * maximumCapacity) / 100 otherwise ]"
              + "\n" + "userLimit = " + userLimit + " [= configuredUserLimit ]"
              + "\n" + "userLimitFactor = " + userLimitFactor
              + " [= configuredUserLimitFactor ]" + "\n" + "maxApplications = "
              + maxApplications
              + " [= configuredMaximumSystemApplicationsPerQueue or"
              + " (int)(configuredMaximumSystemApplications * absoluteCapacity)]"
              + "\n" + "maxApplicationsPerUser = " + maxApplicationsPerUser
              + " [= (int)(maxApplications * (userLimit / 100.0f) * "
              + "userLimitFactor) ]" + "\n" + "usedCapacity = "
              + queueCapacities.getUsedCapacity() + " [= usedResourcesMemory / "
              + "(clusterResourceMemory * absoluteCapacity)]" + "\n"
              + "absoluteUsedCapacity = " + absoluteUsedCapacity
              + " [= usedResourcesMemory / clusterResourceMemory]" + "\n"
              + "maxAMResourcePerQueuePercent = " + maxAMResourcePerQueuePercent
              + " [= configuredMaximumAMResourcePercent ]" + "\n"
              + "minimumAllocationFactor = " + minimumAllocationFactor
              + " [= (float)(maximumAllocationMemory - minimumAllocationMemory) / "
              + "maximumAllocationMemory ]" + "\n" + "maximumAllocation = "
              + maximumAllocation + " [= configuredMaxAllocation ]" + "\n"
              + "numContainers = " + numContainers
              + " [= currentNumContainers ]" + "\n" + "state = " + state
              + " [= configuredState ]" + "\n" + "acls = " + aclsString
              + " [= configuredAcls ]" + "\n" + "nodeLocalityDelay = "
              + nodeLocalityDelay + "\n" + "labels=" + labelStrBuilder
              .toString() + "\n" + "reservationsContinueLooking = "
              + reservationsContinueLooking + "\n" + "preemptionDisabled = "
              + getPreemptionDisabled() + "\n" + "defaultAppPriorityPerQueue = "
              + defaultAppPriorityPerQueue);
    } finally {
      writeLock.unlock();
    }
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

  public int getMaxApplicationsPerUser() {
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
  @VisibleForTesting
  void setUserLimit(int userLimit) {
    this.userLimit = userLimit;
  }

  /**
   * Set user limit factor - used only for testing.
   * @param userLimitFactor new user limit factor
   */
  @VisibleForTesting
  void setUserLimitFactor(float userLimitFactor) {
    this.userLimitFactor = userLimitFactor;
  }

  @Override
  public int getNumApplications() {
    try {
      readLock.lock();
      return getNumPendingApplications() + getNumActiveApplications();
    } finally {
      readLock.unlock();
    }
  }

  public int getNumPendingApplications() {
    try {
      readLock.lock();
      return pendingOrderingPolicy.getNumSchedulableEntities();
    } finally {
      readLock.unlock();
    }
  }

  public int getNumActiveApplications() {
    try {
      readLock.lock();
      return orderingPolicy.getNumSchedulableEntities();
    } finally {
      readLock.unlock();
    }
  }

  @Private
  public int getNumPendingApplications(String user) {
    try {
      readLock.lock();
      User u = getUser(user);
      if (null == u) {
        return 0;
      }
      return u.getPendingApplications();
    } finally {
      readLock.unlock();
    }
  }

  @Private
  public int getNumActiveApplications(String user) {
    try {
      readLock.lock();
      User u = getUser(user);
      if (null == u) {
        return 0;
      }
      return u.getActiveApplications();
    } finally {
      readLock.unlock();
    }
  }

  @Private
  public int getUserLimit() {
    return userLimit;
  }

  @Private
  public float getUserLimitFactor() {
    return userLimitFactor;
  }

  @Override
  public QueueInfo getQueueInfo(
      boolean includeChildQueues, boolean recursive) {
    QueueInfo queueInfo = getQueueInfo();
    return queueInfo;
  }

  @Override
  public List<QueueUserACLInfo>
  getQueueUserAclInfo(UserGroupInformation user) {
    try {
      readLock.lock();
      QueueUserACLInfo userAclInfo = recordFactory.newRecordInstance(
          QueueUserACLInfo.class);
      List<QueueACL> operations = new ArrayList<>();
      for (QueueACL operation : QueueACL.values()) {
        if (hasAccess(operation, user)) {
          operations.add(operation);
        }
      }

      userAclInfo.setQueueName(getQueueName());
      userAclInfo.setUserAcls(operations);
      return Collections.singletonList(userAclInfo);
    } finally {
      readLock.unlock();
    }

  }

  public String toString() {
    try {
      readLock.lock();
      return queueName + ": " + "capacity=" + queueCapacities.getCapacity()
          + ", " + "absoluteCapacity=" + queueCapacities.getAbsoluteCapacity()
          + ", " + "usedResources=" + queueUsage.getUsed() + ", "
          + "usedCapacity=" + getUsedCapacity() + ", " + "absoluteUsedCapacity="
          + getAbsoluteUsedCapacity() + ", " + "numApps=" + getNumApplications()
          + ", " + "numContainers=" + getNumContainers();
    } finally {
      readLock.unlock();
    }

  }

  @VisibleForTesting
  public User getUser(String userName) {
    return users.get(userName);
  }

  // Get and add user if absent
  private User getUserAndAddIfAbsent(String userName) {
    try {
      writeLock.lock();
      User u = users.get(userName);
      if (null == u) {
        u = new User();
        users.put(userName, u);
      }
      return u;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * @return an ArrayList of UserInfo objects who are active in this queue
   */
  public ArrayList<UserInfo> getUsers() {
    try {
      readLock.lock();
      ArrayList<UserInfo> usersToReturn = new ArrayList<UserInfo>();
      for (Map.Entry<String, User> entry : users.entrySet()) {
        User user = entry.getValue();
        usersToReturn.add(
            new UserInfo(entry.getKey(), Resources.clone(user.getAllUsed()),
                user.getActiveApplications(), user.getPendingApplications(),
                Resources.clone(user.getConsumedAMResources()),
                Resources.clone(user.getUserResourceLimit()),
                user.getResourceUsage()));
      }
      return usersToReturn;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void reinitialize(
      CSQueue newlyParsedQueue, Resource clusterResource) 
  throws IOException {
    try {
      writeLock.lock();
      // Sanity check
      if (!(newlyParsedQueue instanceof LeafQueue) || !newlyParsedQueue
          .getQueuePath().equals(getQueuePath())) {
        throw new IOException(
            "Trying to reinitialize " + getQueuePath() + " from "
                + newlyParsedQueue.getQueuePath());
      }

      LeafQueue newlyParsedLeafQueue = (LeafQueue) newlyParsedQueue;

      // don't allow the maximum allocation to be decreased in size
      // since we have already told running AM's the size
      Resource oldMax = getMaximumAllocation();
      Resource newMax = newlyParsedLeafQueue.getMaximumAllocation();
      if (newMax.getMemorySize() < oldMax.getMemorySize()
          || newMax.getVirtualCores() < oldMax.getVirtualCores()) {
        throw new IOException("Trying to reinitialize " + getQueuePath()
            + " the maximum allocation size can not be decreased!"
            + " Current setting: " + oldMax + ", trying to set it to: "
            + newMax);
      }

      setupQueueConfigs(clusterResource);

      // queue metrics are updated, more resource may be available
      // activate the pending applications if possible
      activateApplications();

    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void submitApplicationAttempt(FiCaSchedulerApp application,
      String userName) {
    // Careful! Locking order is important!
    try {
      writeLock.lock();

      // TODO, should use getUser, use this method just to avoid UT failure
      // which is caused by wrong invoking order, will fix UT separately
      User user = getUserAndAddIfAbsent(userName);

      // Add the attempt to our data-structures
      addApplicationAttempt(application, user);
    } finally {
      writeLock.unlock();
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
    try {
      writeLock.lock();
      // Check if the queue is accepting jobs
      if (getState() != QueueState.RUNNING) {
        String msg = "Queue " + getQueuePath()
            + " is STOPPED. Cannot accept submission of application: "
            + applicationId;
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Check submission limits for queues
      if (getNumApplications() >= getMaxApplications()) {
        String msg =
            "Queue " + getQueuePath() + " already has " + getNumApplications()
                + " applications,"
                + " cannot accept submission of application: " + applicationId;
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Check submission limits for the user on this queue
      User user = getUserAndAddIfAbsent(userName);
      if (user.getTotalApplications() >= getMaxApplicationsPerUser()) {
        String msg = "Queue " + getQueuePath() + " already has " + user
            .getTotalApplications() + " applications from user " + userName
            + " cannot accept submission of application: " + applicationId;
        LOG.info(msg);
        throw new AccessControlException(msg);
      }
    } finally {
      writeLock.unlock();
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
  
  public Resource getAMResourceLimit() {
    return queueUsage.getAMLimit();
  }

  public Resource getAMResourceLimitPerPartition(String nodePartition) {
    return queueUsage.getAMLimit(nodePartition);
  }

  @VisibleForTesting
  public Resource calculateAndGetAMResourceLimit() {
    return calculateAndGetAMResourceLimitPerPartition(
        RMNodeLabelsManager.NO_LABEL);
  }

  @VisibleForTesting
  public Resource getUserAMResourceLimit() {
     return getUserAMResourceLimitPerPartition(RMNodeLabelsManager.NO_LABEL);
  }

  public Resource getUserAMResourceLimitPerPartition(
      String nodePartition) {
    try {
      readLock.lock();
      /*
       * The user am resource limit is based on the same approach as the user
       * limit (as it should represent a subset of that). This means that it uses
       * the absolute queue capacity (per partition) instead of the max and is
       * modified by the userlimit and the userlimit factor as is the userlimit
       */
      float effectiveUserLimit = Math.max(userLimit / 100.0f,
          1.0f / Math.max(getActiveUsersManager().getNumActiveUsers(), 1));

      Resource queuePartitionResource = Resources.multiplyAndNormalizeUp(
          resourceCalculator,
          labelManager.getResourceByLabel(nodePartition, lastClusterResource),
          queueCapacities.getAbsoluteCapacity(nodePartition),
          minimumAllocation);

      Resource userAMLimit = Resources.multiplyAndNormalizeUp(
          resourceCalculator, queuePartitionResource,
          queueCapacities.getMaxAMResourcePercentage(nodePartition)
              * effectiveUserLimit * userLimitFactor, minimumAllocation);
      return Resources.lessThanOrEqual(resourceCalculator, lastClusterResource,
          userAMLimit, getAMResourceLimitPerPartition(nodePartition)) ?
          userAMLimit :
          getAMResourceLimitPerPartition(nodePartition);
    } finally {
      readLock.unlock();
    }

  }

  public Resource calculateAndGetAMResourceLimitPerPartition(
      String nodePartition) {
    try {
      writeLock.lock();
      /*
       * For non-labeled partition, get the max value from resources currently
       * available to the queue and the absolute resources guaranteed for the
       * partition in the queue. For labeled partition, consider only the absolute
       * resources guaranteed. Multiply this value (based on labeled/
       * non-labeled), * with per-partition am-resource-percent to get the max am
       * resource limit for this queue and partition.
       */
      Resource queuePartitionResource = Resources.multiplyAndNormalizeUp(
          resourceCalculator,
          labelManager.getResourceByLabel(nodePartition, lastClusterResource),
          queueCapacities.getAbsoluteCapacity(nodePartition),
          minimumAllocation);

      Resource queueCurrentLimit = Resources.none();
      // For non-labeled partition, we need to consider the current queue
      // usage limit.
      if (nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
        synchronized (queueResourceLimitsInfo){
          queueCurrentLimit = queueResourceLimitsInfo.getQueueCurrentLimit();
        }
      }

      float amResourcePercent = queueCapacities.getMaxAMResourcePercentage(
          nodePartition);

      // Current usable resource for this queue and partition is the max of
      // queueCurrentLimit and queuePartitionResource.
      Resource queuePartitionUsableResource = Resources.max(resourceCalculator,
          lastClusterResource, queueCurrentLimit, queuePartitionResource);

      Resource amResouceLimit = Resources.multiplyAndNormalizeUp(
          resourceCalculator, queuePartitionUsableResource, amResourcePercent,
          minimumAllocation);

      metrics.setAMResouceLimit(amResouceLimit);
      queueUsage.setAMLimit(nodePartition, amResouceLimit);
      return amResouceLimit;
    } finally {
      writeLock.unlock();
    }
  }

  private void activateApplications() {
    try {
      writeLock.lock();
      // limit of allowed resource usage for application masters
      Map<String, Resource> userAmPartitionLimit =
          new HashMap<String, Resource>();

      // AM Resource Limit for accessible labels can be pre-calculated.
      // This will help in updating AMResourceLimit for all labels when queue
      // is initialized for the first time (when no applications are present).
      for (String nodePartition : getNodeLabelsForQueue()) {
        calculateAndGetAMResourceLimitPerPartition(nodePartition);
      }

      for (Iterator<FiCaSchedulerApp> fsApp =
           getPendingAppsOrderingPolicy().getAssignmentIterator();
           fsApp.hasNext(); ) {
        FiCaSchedulerApp application = fsApp.next();
        ApplicationId applicationId = application.getApplicationId();

        // Get the am-node-partition associated with each application
        // and calculate max-am resource limit for this partition.
        String partitionName = application.getAppAMNodePartitionName();

        Resource amLimit = getAMResourceLimitPerPartition(partitionName);
        // Verify whether we already calculated am-limit for this label.
        if (amLimit == null) {
          amLimit = calculateAndGetAMResourceLimitPerPartition(partitionName);
        }
        // Check am resource limit.
        Resource amIfStarted = Resources.add(
            application.getAMResource(partitionName),
            queueUsage.getAMUsed(partitionName));

        if (LOG.isDebugEnabled()) {
          LOG.debug("application " + application.getId() + " AMResource "
              + application.getAMResource(partitionName)
              + " maxAMResourcePerQueuePercent " + maxAMResourcePerQueuePercent
              + " amLimit " + amLimit + " lastClusterResource "
              + lastClusterResource + " amIfStarted " + amIfStarted
              + " AM node-partition name " + partitionName);
        }

        if (!Resources.lessThanOrEqual(resourceCalculator, lastClusterResource,
            amIfStarted, amLimit)) {
          if (getNumActiveApplications() < 1 || (Resources.lessThanOrEqual(
              resourceCalculator, lastClusterResource,
              queueUsage.getAMUsed(partitionName), Resources.none()))) {
            LOG.warn("maximum-am-resource-percent is insufficient to start a"
                + " single application in queue, it is likely set too low."
                + " skipping enforcement to allow at least one application"
                + " to start");
          } else{
            application.updateAMContainerDiagnostics(AMState.INACTIVATED,
                CSAMContainerLaunchDiagnosticsConstants.QUEUE_AM_RESOURCE_LIMIT_EXCEED);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Not activating application " + applicationId
                  + " as  amIfStarted: " + amIfStarted + " exceeds amLimit: "
                  + amLimit);
            }
            continue;
          }
        }

        // Check user am resource limit
        User user = getUser(application.getUser());
        Resource userAMLimit = userAmPartitionLimit.get(partitionName);

        // Verify whether we already calculated user-am-limit for this label.
        if (userAMLimit == null) {
          userAMLimit = getUserAMResourceLimitPerPartition(partitionName);
          userAmPartitionLimit.put(partitionName, userAMLimit);
        }

        Resource userAmIfStarted = Resources.add(
            application.getAMResource(partitionName),
            user.getConsumedAMResources(partitionName));

        if (!Resources.lessThanOrEqual(resourceCalculator, lastClusterResource,
            userAmIfStarted, userAMLimit)) {
          if (getNumActiveApplications() < 1 || (Resources.lessThanOrEqual(
              resourceCalculator, lastClusterResource,
              queueUsage.getAMUsed(partitionName), Resources.none()))) {
            LOG.warn("maximum-am-resource-percent is insufficient to start a"
                + " single application in queue for user, it is likely set too"
                + " low. skipping enforcement to allow at least one application"
                + " to start");
          } else{
            application.updateAMContainerDiagnostics(AMState.INACTIVATED,
                CSAMContainerLaunchDiagnosticsConstants.USER_AM_RESOURCE_LIMIT_EXCEED);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Not activating application " + applicationId
                  + " for user: " + user + " as userAmIfStarted: "
                  + userAmIfStarted + " exceeds userAmLimit: " + userAMLimit);
            }
            continue;
          }
        }
        user.activateApplication();
        orderingPolicy.addSchedulableEntity(application);
        application.updateAMContainerDiagnostics(AMState.ACTIVATED, null);

        queueUsage.incAMUsed(partitionName,
            application.getAMResource(partitionName));
        user.getResourceUsage().incAMUsed(partitionName,
            application.getAMResource(partitionName));
        user.getResourceUsage().setAMLimit(partitionName, userAMLimit);
        metrics.incAMUsed(application.getUser(),
            application.getAMResource(partitionName));
        metrics.setAMResouceLimitForUser(application.getUser(), userAMLimit);
        fsApp.remove();
        LOG.info("Application " + applicationId + " from user: " + application
            .getUser() + " activated in queue: " + getQueueName());
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  private void addApplicationAttempt(FiCaSchedulerApp application,
      User user) {
    try {
      writeLock.lock();
      // Accept
      user.submitApplication();
      getPendingAppsOrderingPolicy().addSchedulableEntity(application);
      applicationAttemptMap.put(application.getApplicationAttemptId(),
          application);

      // Activate applications
      if (Resources.greaterThan(resourceCalculator, lastClusterResource,
          lastClusterResource, Resources.none())) {
        activateApplications();
      } else {
        application.updateAMContainerDiagnostics(AMState.INACTIVATED,
            CSAMContainerLaunchDiagnosticsConstants.CLUSTER_RESOURCE_EMPTY);
        LOG.info("Skipping activateApplications for "
            + application.getApplicationAttemptId()
            + " since cluster resource is " + Resources.none());
      }

      LOG.info(
          "Application added -" + " appId: " + application.getApplicationId()
              + " user: " + application.getUser() + "," + " leaf-queue: "
              + getQueueName() + " #user-pending-applications: " + user
              .getPendingApplications() + " #user-active-applications: " + user
              .getActiveApplications() + " #queue-pending-applications: "
              + getNumPendingApplications() + " #queue-active-applications: "
              + getNumActiveApplications());
    } finally {
      writeLock.unlock();
    }
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
    removeApplicationAttempt(application, application.getUser());
    getParent().finishApplicationAttempt(application, queue);
  }

  private void removeApplicationAttempt(
      FiCaSchedulerApp application, String userName) {
    try {
      writeLock.lock();

      // TODO, should use getUser, use this method just to avoid UT failure
      // which is caused by wrong invoking order, will fix UT separately
      User user = getUserAndAddIfAbsent(userName);

      String partitionName = application.getAppAMNodePartitionName();
      boolean wasActive = orderingPolicy.removeSchedulableEntity(application);
      if (!wasActive) {
        pendingOrderingPolicy.removeSchedulableEntity(application);
      } else{
        queueUsage.decAMUsed(partitionName,
            application.getAMResource(partitionName));
        user.getResourceUsage().decAMUsed(partitionName,
            application.getAMResource(partitionName));
        metrics.decAMUsed(application.getUser(),
            application.getAMResource(partitionName));
      }
      applicationAttemptMap.remove(application.getApplicationAttemptId());

      user.finishApplication(wasActive);
      if (user.getTotalApplications() == 0) {
        users.remove(application.getUser());
      }

      // Check if we can activate more applications
      activateApplications();

      LOG.info(
          "Application removed -" + " appId: " + application.getApplicationId()
              + " user: " + application.getUser() + " queue: " + getQueueName()
              + " #user-pending-applications: " + user.getPendingApplications()
              + " #user-active-applications: " + user.getActiveApplications()
              + " #queue-pending-applications: " + getNumPendingApplications()
              + " #queue-active-applications: " + getNumActiveApplications());
    } finally {
      writeLock.unlock();
    }
  }

  private FiCaSchedulerApp getApplication(
      ApplicationAttemptId applicationAttemptId) {
    return applicationAttemptMap.get(applicationAttemptId);
  }

  private void setPreemptionAllowed(ResourceLimits limits, String nodePartition) {
    // Set preemption-allowed:
    // For leaf queue, only under-utilized queue is allowed to preempt resources from other queues
    float usedCapacity = queueCapacities.getAbsoluteUsedCapacity(nodePartition);
    float guaranteedCapacity = queueCapacities.getAbsoluteCapacity(nodePartition);
    limits.setIsAllowPreemption(usedCapacity < guaranteedCapacity);
  }

  private CSAssignment allocateFromReservedContainer(
      Resource clusterResource, PlacementSet<FiCaSchedulerNode> ps,
      ResourceLimits currentResourceLimits, SchedulingMode schedulingMode) {
    FiCaSchedulerNode node = PlacementSetUtils.getSingleNode(ps);
    if (null == node) {
      return null;
    }

    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      FiCaSchedulerApp application = getApplication(
          reservedContainer.getApplicationAttemptId());

      if (null != application) {
        ActivitiesLogger.APP.startAppAllocationRecording(activitiesManager,
            node.getNodeID(), SystemClock.getInstance().getTime(), application);
        CSAssignment assignment = application.assignContainers(clusterResource,
            ps, currentResourceLimits, schedulingMode, reservedContainer);
        return assignment;
      }
    }

    return null;
  }

  @Override
  public CSAssignment assignContainers(Resource clusterResource,
      PlacementSet<FiCaSchedulerNode> ps, ResourceLimits currentResourceLimits,
    SchedulingMode schedulingMode) {
    updateCurrentResourceLimits(currentResourceLimits, clusterResource);
    FiCaSchedulerNode node = PlacementSetUtils.getSingleNode(ps);

    if (LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: partition=" + ps.getPartition()
          + " #applications=" + orderingPolicy.getNumSchedulableEntities());
    }

    setPreemptionAllowed(currentResourceLimits, ps.getPartition());

    // Check for reserved resources, try to allocate reserved container first.
    CSAssignment assignment = allocateFromReservedContainer(clusterResource,
        ps, currentResourceLimits, schedulingMode);
    if (null != assignment) {
      return assignment;
    }

    // if our queue cannot access this node, just return
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY
        && !accessibleToPartition(ps.getPartition())) {
      ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
          getParent().getQueueName(), getQueueName(), ActivityState.REJECTED,
          ActivityDiagnosticConstant.NOT_ABLE_TO_ACCESS_PARTITION + ps
              .getPartition());
      return CSAssignment.NULL_ASSIGNMENT;
    }

    // Check if this queue need more resource, simply skip allocation if this
    // queue doesn't need more resources.
    if (!hasPendingResourceRequest(ps.getPartition(), clusterResource,
        schedulingMode)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip this queue=" + getQueuePath()
            + ", because it doesn't need more resource, schedulingMode="
            + schedulingMode.name() + " node-partition=" + ps.getPartition());
      }
      ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
          getParent().getQueueName(), getQueueName(), ActivityState.SKIPPED,
          ActivityDiagnosticConstant.QUEUE_DO_NOT_NEED_MORE_RESOURCE);
      return CSAssignment.NULL_ASSIGNMENT;
    }

    for (Iterator<FiCaSchedulerApp> assignmentIterator =
         orderingPolicy.getAssignmentIterator();
         assignmentIterator.hasNext(); ) {
      FiCaSchedulerApp application = assignmentIterator.next();

      ActivitiesLogger.APP.startAppAllocationRecording(activitiesManager,
          node.getNodeID(), SystemClock.getInstance().getTime(), application);

      // Check queue max-capacity limit
      if (!super.canAssignToThisQueue(clusterResource, ps.getPartition(),
          currentResourceLimits, application.getCurrentReservation(),
          schedulingMode)) {
        ActivitiesLogger.APP.recordRejectedAppActivityFromLeafQueue(
            activitiesManager, node, application, application.getPriority(),
            ActivityDiagnosticConstant.QUEUE_MAX_CAPACITY_LIMIT);
        ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
            getParent().getQueueName(), getQueueName(), ActivityState.SKIPPED,
            ActivityDiagnosticConstant.EMPTY);
        return CSAssignment.NULL_ASSIGNMENT;
      }

      Resource userLimit = computeUserLimitAndSetHeadroom(application,
          clusterResource, ps.getPartition(), schedulingMode);

      // Check user limit
      if (!canAssignToUser(clusterResource, application.getUser(), userLimit,
          application, ps.getPartition(), currentResourceLimits)) {
        application.updateAMContainerDiagnostics(AMState.ACTIVATED,
            "User capacity has reached its maximum limit.");
        ActivitiesLogger.APP.recordRejectedAppActivityFromLeafQueue(
            activitiesManager, node, application, application.getPriority(),
            ActivityDiagnosticConstant.USER_CAPACITY_MAXIMUM_LIMIT);
        continue;
      }

      // Try to schedule
      assignment = application.assignContainers(clusterResource,
          ps, currentResourceLimits, schedulingMode, null);

      if (LOG.isDebugEnabled()) {
        LOG.debug("post-assignContainers for application " + application
            .getApplicationId());
        application.showRequests();
      }

      // Did we schedule or reserve a container?
      Resource assigned = assignment.getResource();

      if (Resources.greaterThan(resourceCalculator, clusterResource, assigned,
          Resources.none())) {
        ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
            getParent().getQueueName(), getQueueName(),
            ActivityState.ACCEPTED, ActivityDiagnosticConstant.EMPTY);
        return assignment;
      } else if (assignment.getSkippedType()
          == CSAssignment.SkippedType.OTHER) {
        ActivitiesLogger.APP.finishSkippedAppAllocationRecording(
            activitiesManager, application.getApplicationId(),
            ActivityState.SKIPPED, ActivityDiagnosticConstant.EMPTY);
        application.updateNodeInfoForAMDiagnostics(node);
      } else if (assignment.getSkippedType()
          == CSAssignment.SkippedType.QUEUE_LIMIT) {
        return assignment;
      } else{
        // If we don't allocate anything, and it is not skipped by application,
        // we will return to respect FIFO of applications
        ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
            getParent().getQueueName(), getQueueName(), ActivityState.SKIPPED,
            ActivityDiagnosticConstant.RESPECT_FIFO);
        ActivitiesLogger.APP.finishSkippedAppAllocationRecording(
            activitiesManager, application.getApplicationId(),
            ActivityState.SKIPPED, ActivityDiagnosticConstant.EMPTY);
        return CSAssignment.NULL_ASSIGNMENT;
      }
    }
    ActivitiesLogger.QUEUE.recordQueueActivity(activitiesManager, node,
        getParent().getQueueName(), getQueueName(), ActivityState.SKIPPED,
        ActivityDiagnosticConstant.EMPTY);

    return CSAssignment.NULL_ASSIGNMENT;
  }

  @Override
  public boolean accept(Resource cluster,
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request) {
    ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode> allocation =
        request.getFirstAllocatedOrReservedContainer();
    SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> schedulerContainer =
        allocation.getAllocatedOrReservedContainer();

    // Do not check limits when allocation from a reserved container
    if (allocation.getAllocateFromReservedContainer() == null) {
      try {
        readLock.lock();
        FiCaSchedulerApp app =
            schedulerContainer.getSchedulerApplicationAttempt();
        String username = app.getUser();
        String p = schedulerContainer.getNodePartition();

        // check user-limit
        Resource userLimit = computeUserLimitAndSetHeadroom(app, cluster, p,
            allocation.getSchedulingMode());

        // Deduct resources that we can release
        Resource usedResource = Resources.clone(getUser(username).getUsed(p));
        Resources.subtractFrom(usedResource,
            request.getTotalReleasedResource());

        if (Resources.greaterThan(resourceCalculator, cluster, usedResource,
            userLimit)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Used resource=" + usedResource + " exceeded user-limit="
                + userLimit);
          }
          return false;
        }
      } finally {
        readLock.unlock();
      }
    }

    return super.accept(cluster, request);
  }

  private void internalReleaseContainer(Resource clusterResource,
      SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> schedulerContainer) {
    RMContainer rmContainer = schedulerContainer.getRmContainer();

    LeafQueue targetLeafQueue =
        schedulerContainer.getSchedulerApplicationAttempt().getCSLeafQueue();

    if (targetLeafQueue == this) {
      // When trying to preempt containers from the same queue
      if (rmContainer.hasIncreaseReservation()) {
        // Increased container reservation
        unreserveIncreasedContainer(clusterResource,
            schedulerContainer.getSchedulerApplicationAttempt(),
            schedulerContainer.getSchedulerNode(), rmContainer);
      } else if (rmContainer.getState() == RMContainerState.RESERVED) {
        // For other reserved containers
        // This is a reservation exchange, complete previous reserved container
        completedContainer(clusterResource,
            schedulerContainer.getSchedulerApplicationAttempt(),
            schedulerContainer.getSchedulerNode(), rmContainer, SchedulerUtils
                .createAbnormalContainerStatus(rmContainer.getContainerId(),
                    SchedulerUtils.UNRESERVED_CONTAINER),
            RMContainerEventType.RELEASED, null, false);
      }
    } else{
      // When trying to preempt containers from different queue -- this
      // is for lazy preemption feature (kill preemption candidate in scheduling
      // cycle).
      targetLeafQueue.completedContainer(clusterResource,
          schedulerContainer.getSchedulerApplicationAttempt(),
          schedulerContainer.getSchedulerNode(),
          schedulerContainer.getRmContainer(), SchedulerUtils
              .createPreemptedContainerStatus(rmContainer.getContainerId(),
                  SchedulerUtils.PREEMPTED_CONTAINER),
          RMContainerEventType.KILL, null, false);
    }
  }

  private void releaseContainers(Resource clusterResource,
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request) {
    for (SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> c : request
        .getContainersToRelease()) {
      internalReleaseContainer(clusterResource, c);
    }

    // Handle container reservation looking, or lazy preemption case:
    if (null != request.getContainersToAllocate() && !request
        .getContainersToAllocate().isEmpty()) {
      for (ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode> context : request
          .getContainersToAllocate()) {
        if (null != context.getToRelease()) {
          for (SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> c : context
              .getToRelease()) {
            internalReleaseContainer(clusterResource, c);
          }
        }
      }
    }
  }

  public void apply(Resource cluster,
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request) {
    // Do we need to call parent queue's apply?
    boolean applyToParentQueue = false;

    releaseContainers(cluster, request);

    try {
      writeLock.lock();

      if (request.anythingAllocatedOrReserved()) {
        ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode>
            allocation = request.getFirstAllocatedOrReservedContainer();
        SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode>
            schedulerContainer = allocation.getAllocatedOrReservedContainer();

        // Do not modify queue when allocation from reserved container
        if (allocation.getAllocateFromReservedContainer() == null) {
          // Only invoke apply() of ParentQueue when new allocation /
          // reservation happen.
          applyToParentQueue = true;
          // Book-keeping
          // Note: Update headroom to account for current allocation too...
          allocateResource(cluster,
              schedulerContainer.getSchedulerApplicationAttempt(),
              allocation.getAllocatedOrReservedResource(),
              schedulerContainer.getNodePartition(),
              schedulerContainer.getRmContainer(),
              allocation.isIncreasedAllocation());
          orderingPolicy.containerAllocated(
              schedulerContainer.getSchedulerApplicationAttempt(),
              schedulerContainer.getRmContainer());
        }

        // Update reserved resource
        if (Resources.greaterThan(resourceCalculator, cluster,
            request.getTotalReservedResource(), Resources.none())) {
          incReservedResource(schedulerContainer.getNodePartition(),
              request.getTotalReservedResource());
        }
      }
    } finally {
      writeLock.unlock();
    }

    if (parent != null && applyToParentQueue) {
      parent.apply(cluster, request);
    }
  }


  protected Resource getHeadroom(User user, Resource queueCurrentLimit,
      Resource clusterResource, FiCaSchedulerApp application) {
    return getHeadroom(user, queueCurrentLimit, clusterResource, application,
        RMNodeLabelsManager.NO_LABEL);
  }

  protected Resource getHeadroom(User user, Resource queueCurrentLimit,
      Resource clusterResource, FiCaSchedulerApp application,
      String partition) {
    return getHeadroom(user, queueCurrentLimit, clusterResource,
        computeUserLimit(application.getUser(), clusterResource, user,
            partition, SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY),
        partition);
  }

  private Resource getHeadroom(User user,
      Resource currentPartitionResourceLimit, Resource clusterResource,
      Resource userLimitResource, String partition) {
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
     * sum of queue max capacities of multiple queue's will be greater than the
     * actual capacity of a given partition, hence we need to ensure that the
     * headroom is not greater than the available resource for a given partition
     *
     * headroom = min (unused resourcelimit of a label, calculated headroom )
     */
    currentPartitionResourceLimit =
        partition.equals(RMNodeLabelsManager.NO_LABEL)
            ? currentPartitionResourceLimit
            : getQueueMaxResource(partition, clusterResource);

    Resource headroom = Resources.componentwiseMin(
        Resources.subtract(userLimitResource, user.getUsed(partition)),
        Resources.subtract(currentPartitionResourceLimit,
            queueUsage.getUsed(partition)));
    // Normalize it before return
    headroom =
        Resources.roundDown(resourceCalculator, headroom, minimumAllocation);

    //headroom = min (unused resourcelimit of a label, calculated headroom )
    Resource clusterPartitionResource =
        labelManager.getResourceByLabel(partition, clusterResource);
    Resource clusterFreePartitionResource =
        Resources.subtract(clusterPartitionResource,
            csContext.getClusterResourceUsage().getUsed(partition));
    headroom = Resources.min(resourceCalculator, clusterPartitionResource,
        clusterFreePartitionResource, headroom);
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

  // It doesn't necessarily to hold application's lock here.
  @Lock({LeafQueue.class})
  Resource computeUserLimitAndSetHeadroom(FiCaSchedulerApp application,
      Resource clusterResource, String nodePartition,
      SchedulingMode schedulingMode) {
    String user = application.getUser();
    User queueUser = getUser(user);

    // Compute user limit respect requested labels,
    // TODO, need consider headroom respect labels also
    Resource userLimit =
        computeUserLimit(application.getUser(), clusterResource, queueUser,
            nodePartition, schedulingMode);

    setQueueResourceLimitsInfo(clusterResource);

    Resource headroom =
        getHeadroom(queueUser, cachedResourceLimitsForHeadroom.getLimit(),
            clusterResource, userLimit, nodePartition);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Headroom calculation for user " + user + ": " + 
          " userLimit=" + userLimit + 
          " queueMaxAvailRes=" + cachedResourceLimitsForHeadroom.getLimit() +
          " consumed=" + queueUser.getUsed() + 
          " headroom=" + headroom);
    }
    
    CapacityHeadroomProvider headroomProvider = new CapacityHeadroomProvider(
      queueUser, this, application, queueResourceLimitsInfo);
    
    application.setHeadroomProvider(headroomProvider);

    metrics.setAvailableResourcesToUser(user, headroom);
    
    return userLimit;
  }
  
  @Lock(NoLock.class)
  public int getNodeLocalityDelay() {
    return nodeLocalityDelay;
  }

  @Lock(NoLock.class)
  public boolean getRackLocalityFullReset() {
    return rackLocalityFullReset;
  }

  @Lock(NoLock.class)
  private Resource computeUserLimit(String userName,
      Resource clusterResource, User user,
      String nodePartition, SchedulingMode schedulingMode) {
    Resource partitionResource = labelManager.getResourceByLabel(nodePartition,
        clusterResource);

    // What is our current capacity? 
    // * It is equal to the max(required, queue-capacity) if
    //   we're running below capacity. The 'max' ensures that jobs in queues
    //   with miniscule capacity (< 1 slot) make progress
    // * If we're running over capacity, then its
    //   (usedResources + required) (which extra resources we are allocating)
    Resource queueCapacity =
        Resources.multiplyAndNormalizeUp(resourceCalculator,
            partitionResource,
            queueCapacities.getAbsoluteCapacity(nodePartition),
            minimumAllocation);

    // Assume we have required resource equals to minimumAllocation, this can
    // make sure user limit can continuously increase till queueMaxResource
    // reached.
    Resource required = minimumAllocation;

    // Allow progress for queues with miniscule capacity
    queueCapacity =
        Resources.max(
            resourceCalculator, partitionResource,
            queueCapacity, 
            required);


    /* We want to base the userLimit calculation on
     * max(queueCapacity, usedResources+required). However, we want
     * usedResources to be based on the combined ratios of all the users in the
     * queue so we use consumedRatio to calculate such.
     * The calculation is dependent on how the resourceCalculator calculates the
     * ratio between two Resources. DRF Example: If usedResources is
     * greater than queueCapacity and users have the following [mem,cpu] usages:
     * User1: [10%,20%] - Dominant resource is 20%
     * User2: [30%,10%] - Dominant resource is 30%
     * Then total consumedRatio is then 20+30=50%. Yes, this value can be
     * larger than 100% but for the purposes of making sure all users are
     * getting their fair share, it works.
     */
    Resource consumed = Resources.multiplyAndNormalizeUp(resourceCalculator,
        partitionResource, qUsageRatios.getUsageRatio(nodePartition),
        minimumAllocation);
    Resource currentCapacity =
        Resources.lessThan(resourceCalculator, partitionResource, consumed,
            queueCapacity) ? queueCapacity : Resources.add(consumed, required);
    // Never allow a single user to take more than the 
    // queue's configured capacity * user-limit-factor.
    // Also, the queue's configured capacity should be higher than 
    // queue-hard-limit * ulMin
    
    final int activeUsers = activeUsersManager.getNumActiveUsers();
    
    // User limit resource is determined by:
    // max{currentCapacity / #activeUsers, currentCapacity *
    // user-limit-percentage%)
    Resource userLimitResource = Resources.max(
        resourceCalculator, partitionResource,
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
      maxUserLimit = partitionResource;
    }
    
    // Cap final user limit with maxUserLimit
    userLimitResource =
        Resources.roundUp(
            resourceCalculator, 
            Resources.min(
                resourceCalculator, partitionResource,
                  userLimitResource,
                  maxUserLimit
                ), 
            minimumAllocation);

    if (LOG.isDebugEnabled()) {
      LOG.debug("User limit computation for " + userName +
          " in queue " + getQueueName() +
          " userLimitPercent=" + userLimit +
          " userLimitFactor=" + userLimitFactor +
          " required: " + required +
          " consumed: " + consumed +
          " user-limit-resource: " + userLimitResource +
          " queueCapacity: " + queueCapacity +
          " qconsumed: " + queueUsage.getUsed() +
          " consumedRatio: " + totalUserConsumedRatio +
          " currentCapacity: " + currentCapacity +
          " activeUsers: " + activeUsers +
          " clusterCapacity: " + clusterResource +
          " resourceByLabel: " + partitionResource +
          " usageratio: " + qUsageRatios.getUsageRatio(nodePartition) +
          " Partition: " + nodePartition
      );
    }
    user.setUserResourceLimit(userLimitResource);
    return userLimitResource;
  }
  
  @Private
  protected boolean canAssignToUser(Resource clusterResource,
      String userName, Resource limit, FiCaSchedulerApp application,
      String nodePartition, ResourceLimits currentResourceLimits) {
    try {
      readLock.lock();
      User user = getUser(userName);

      currentResourceLimits.setAmountNeededUnreserve(Resources.none());

      // Note: We aren't considering the current request since there is a fixed
      // overhead of the AM, but it's a > check, not a >= check, so...
      if (Resources.greaterThan(resourceCalculator, clusterResource,
          user.getUsed(nodePartition), limit)) {
        // if enabled, check to see if could we potentially use this node instead
        // of a reserved node if the application has reserved containers
        if (this.reservationsContinueLooking && nodePartition.equals(
            CommonNodeLabelsManager.NO_LABEL)) {
          if (Resources.lessThanOrEqual(resourceCalculator, clusterResource,
              Resources.subtract(user.getUsed(),
                  application.getCurrentReservation()), limit)) {

            if (LOG.isDebugEnabled()) {
              LOG.debug("User " + userName + " in queue " + getQueueName()
                  + " will exceed limit based on reservations - "
                  + " consumed: " + user.getUsed() + " reserved: " + application
                  .getCurrentReservation() + " limit: " + limit);
            }
            Resource amountNeededToUnreserve = Resources.subtract(
                user.getUsed(nodePartition), limit);
            // we can only acquire a new container if we unreserve first to
            // respect user-limit
            currentResourceLimits.setAmountNeededUnreserve(
                amountNeededToUnreserve);
            return true;
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("User " + userName + " in queue " + getQueueName()
              + " will exceed limit - " + " consumed: " + user
              .getUsed(nodePartition) + " limit: " + limit);
        }
        return false;
      }
      return true;
    } finally {
      readLock.unlock();
    }
  }
  
  @Override
  public void unreserveIncreasedContainer(Resource clusterResource,
      FiCaSchedulerApp app, FiCaSchedulerNode node, RMContainer rmContainer) {
    boolean removed = false;
    Priority priority = null;

    try {
      writeLock.lock();
      if (rmContainer.getContainer() != null) {
        priority = rmContainer.getContainer().getPriority();
      }

      if (null != priority) {
        removed = app.unreserve(rmContainer.getAllocatedSchedulerKey(), node,
            rmContainer);
      }

      if (removed) {
        // Inform the ordering policy
        orderingPolicy.containerReleased(app, rmContainer);

        releaseResource(clusterResource, app, rmContainer.getReservedResource(),
            node.getPartition(), rmContainer, true);
      }
    } finally {
      writeLock.unlock();
    }

    if (removed) {
      getParent().unreserveIncreasedContainer(clusterResource, app, node,
          rmContainer);
    }
  }

  private void updateSchedulerHealthForCompletedContainer(
      RMContainer rmContainer, ContainerStatus containerStatus) {
    // Update SchedulerHealth for released / preempted container
    SchedulerHealth schedulerHealth = csContext.getSchedulerHealth();
    if (null == schedulerHealth) {
      // Only do update if we have schedulerHealth
      return;
    }

    if (containerStatus.getExitStatus() == ContainerExitStatus.PREEMPTED) {
      schedulerHealth.updatePreemption(Time.now(), rmContainer.getAllocatedNode(),
          rmContainer.getContainerId(), getQueuePath());
      schedulerHealth.updateSchedulerPreemptionCounts(1);
    } else {
      schedulerHealth.updateRelease(csContext.getLastNodeUpdateTime(),
          rmContainer.getAllocatedNode(), rmContainer.getContainerId(),
          getQueuePath());
    }
  }

  private float calculateUserUsageRatio(Resource clusterResource,
      String nodePartition) {
    try {
      writeLock.lock();
      Resource resourceByLabel = labelManager.getResourceByLabel(nodePartition,
          clusterResource);
      float consumed = 0;
      User user;
      for (Map.Entry<String, User> entry : users.entrySet()) {
        user = entry.getValue();
        consumed += user.resetAndUpdateUsageRatio(resourceCalculator,
            resourceByLabel, nodePartition);
      }
      return consumed;
    } finally {
      writeLock.unlock();
    }
  }

  private void recalculateQueueUsageRatio(Resource clusterResource,
      String nodePartition) {
    try {
      writeLock.lock();
      ResourceUsage queueResourceUsage = this.getQueueResourceUsage();

      if (nodePartition == null) {
        for (String partition : Sets.union(
            queueCapacities.getNodePartitionsSet(),
            queueResourceUsage.getNodePartitionsSet())) {
          qUsageRatios.setUsageRatio(partition,
              calculateUserUsageRatio(clusterResource, partition));
        }
      } else{
        qUsageRatios.setUsageRatio(nodePartition,
            calculateUserUsageRatio(clusterResource, nodePartition));
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void updateQueueUsageRatio(String nodePartition,
      float delta) {
    qUsageRatios.incUsageRatio(nodePartition, delta);
  }

  @Override
  public void completedContainer(Resource clusterResource, 
      FiCaSchedulerApp application, FiCaSchedulerNode node, RMContainer rmContainer, 
      ContainerStatus containerStatus, RMContainerEventType event, CSQueue childQueue,
      boolean sortQueues) {
    // Update SchedulerHealth for released / preempted container
    updateSchedulerHealthForCompletedContainer(rmContainer, containerStatus);

    if (application != null) {
      // unreserve container increase request if it previously reserved.
      if (rmContainer.hasIncreaseReservation()) {
        unreserveIncreasedContainer(clusterResource, application, node,
            rmContainer);
      }
      
      // Remove container increase request if it exists
      application.removeIncreaseRequest(node.getNodeID(),
          rmContainer.getAllocatedSchedulerKey(), rmContainer.getContainerId());

      boolean removed = false;

      // Careful! Locking order is important!
      try {
        writeLock.lock();
        Container container = rmContainer.getContainer();

        // Inform the application & the node
        // Note: It's safe to assume that all state changes to RMContainer
        // happen under scheduler's lock...
        // So, this is, in effect, a transaction across application & node
        if (rmContainer.getState() == RMContainerState.RESERVED) {
          removed = application.unreserve(rmContainer.getReservedSchedulerKey(),
              node, rmContainer);
        } else{
          removed = application.containerCompleted(rmContainer, containerStatus,
              event, node.getPartition());

          node.releaseContainer(container);
        }

        // Book-keeping
        if (removed) {

          // Inform the ordering policy
          orderingPolicy.containerReleased(application, rmContainer);

          releaseResource(clusterResource, application, container.getResource(),
              node.getPartition(), rmContainer, false);
        }
      } finally {
        writeLock.unlock();
      }


      if (removed) {
        // Inform the parent queue _outside_ of the leaf-queue lock
        getParent().completedContainer(clusterResource, application, node,
          rmContainer, null, event, this, sortQueues);
      }
    }

    // Notify PreemptionManager
    csContext.getPreemptionManager().removeKillableContainer(
        new KillableContainer(rmContainer, node.getPartition(), queueName));
  }

  void allocateResource(Resource clusterResource,
      SchedulerApplicationAttempt application, Resource resource,
      String nodePartition, RMContainer rmContainer,
      boolean isIncreasedAllocation) {
    try {
      writeLock.lock();
      super.allocateResource(clusterResource, resource, nodePartition,
          isIncreasedAllocation);
      Resource resourceByLabel = labelManager.getResourceByLabel(nodePartition,
          clusterResource);

      // handle ignore exclusivity container
      if (null != rmContainer && rmContainer.getNodeLabelExpression().equals(
          RMNodeLabelsManager.NO_LABEL) && !nodePartition.equals(
          RMNodeLabelsManager.NO_LABEL)) {
        TreeSet<RMContainer> rmContainers = null;
        if (null == (rmContainers = ignorePartitionExclusivityRMContainers.get(
            nodePartition))) {
          rmContainers = new TreeSet<>();
          ignorePartitionExclusivityRMContainers.put(nodePartition,
              rmContainers);
        }
        rmContainers.add(rmContainer);
      }

      // Update user metrics
      String userName = application.getUser();

      // TODO, should use getUser, use this method just to avoid UT failure
      // which is caused by wrong invoking order, will fix UT separately
      User user = getUserAndAddIfAbsent(userName);

      user.assignContainer(resource, nodePartition);

      // Update usage ratios
      updateQueueUsageRatio(nodePartition,
          user.updateUsageRatio(resourceCalculator, resourceByLabel,
              nodePartition));

      // Note this is a bit unconventional since it gets the object and modifies
      // it here, rather then using set routine
      Resources.subtractFrom(application.getHeadroom(), resource); // headroom
      metrics.setAvailableResourcesToUser(userName, application.getHeadroom());

      if (LOG.isDebugEnabled()) {
        LOG.debug(getQueueName() + " user=" + userName + " used=" + queueUsage
            .getUsed() + " numContainers=" + numContainers + " headroom = "
            + application.getHeadroom() + " user-resources=" + user.getUsed());
      }
    } finally {
      writeLock.unlock();
    }
  }

  void releaseResource(Resource clusterResource,
      FiCaSchedulerApp application, Resource resource, String nodePartition,
      RMContainer rmContainer, boolean isChangeResource) {
    try {
      writeLock.lock();
      super.releaseResource(clusterResource, resource, nodePartition,
          isChangeResource);
      Resource resourceByLabel = labelManager.getResourceByLabel(nodePartition,
          clusterResource);

      // handle ignore exclusivity container
      if (null != rmContainer && rmContainer.getNodeLabelExpression().equals(
          RMNodeLabelsManager.NO_LABEL) && !nodePartition.equals(
          RMNodeLabelsManager.NO_LABEL)) {
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
      User user = getUserAndAddIfAbsent(userName);
      user.releaseContainer(resource, nodePartition);

      // Update usage ratios
      updateQueueUsageRatio(nodePartition,
          user.updateUsageRatio(resourceCalculator, resourceByLabel,
              nodePartition));

      metrics.setAvailableResourcesToUser(userName, application.getHeadroom());

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            getQueueName() + " used=" + queueUsage.getUsed() + " numContainers="
                + numContainers + " user=" + userName + " user-resources="
                + user.getUsed());
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  private void updateCurrentResourceLimits(
      ResourceLimits currentResourceLimits, Resource clusterResource) {
    // TODO: need consider non-empty node labels when resource limits supports
    // node labels
    // Even if ParentQueue will set limits respect child's max queue capacity,
    // but when allocating reserved container, CapacityScheduler doesn't do
    // this. So need cap limits by queue's max capacity here.
    this.cachedResourceLimitsForHeadroom =
        new ResourceLimits(currentResourceLimits.getLimit());
    Resource queueMaxResource =
        Resources.multiplyAndNormalizeDown(resourceCalculator, labelManager
            .getResourceByLabel(RMNodeLabelsManager.NO_LABEL, clusterResource),
            queueCapacities
                .getAbsoluteMaximumCapacity(RMNodeLabelsManager.NO_LABEL),
            minimumAllocation);
    this.cachedResourceLimitsForHeadroom.setLimit(Resources.min(
        resourceCalculator, clusterResource, queueMaxResource,
        currentResourceLimits.getLimit()));
  }

  @Override
  public void updateClusterResource(Resource clusterResource,
      ResourceLimits currentResourceLimits) {
    try {
      writeLock.lock();
      updateCurrentResourceLimits(currentResourceLimits, clusterResource);
      lastClusterResource = clusterResource;

      // Update headroom info based on new cluster resource value
      // absoluteMaxCapacity now,  will be replaced with absoluteMaxAvailCapacity
      // during allocation
      setQueueResourceLimitsInfo(clusterResource);

      // Update user consumedRatios
      recalculateQueueUsageRatio(clusterResource, null);

      // Update metrics
      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          minimumAllocation, this, labelManager, null);

      // queue metrics are updated, more resource may be available
      // activate the pending applications if possible
      activateApplications();

      // Update application properties
      for (FiCaSchedulerApp application : orderingPolicy
          .getSchedulableEntities()) {
        computeUserLimitAndSetHeadroom(application, clusterResource,
            RMNodeLabelsManager.NO_LABEL,
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void incUsedResource(String nodeLabel, Resource resourceToInc,
      SchedulerApplicationAttempt application) {
    getUser(application.getUser()).getResourceUsage().incUsed(nodeLabel,
        resourceToInc);
    super.incUsedResource(nodeLabel, resourceToInc, application);
  }

  @Override
  public void decUsedResource(String nodeLabel, Resource resourceToDec,
      SchedulerApplicationAttempt application) {
    getUser(application.getUser()).getResourceUsage().decUsed(nodeLabel,
        resourceToDec);
    super.decUsedResource(nodeLabel, resourceToDec, application);
  }

  public void incAMUsedResource(String nodeLabel, Resource resourceToInc,
      SchedulerApplicationAttempt application) {
    getUser(application.getUser()).getResourceUsage().incAMUsed(nodeLabel,
        resourceToInc);
    // ResourceUsage has its own lock, no addition lock needs here.
    queueUsage.incAMUsed(nodeLabel, resourceToInc);
  }

  public void decAMUsedResource(String nodeLabel, Resource resourceToDec,
      SchedulerApplicationAttempt application) {
    getUser(application.getUser()).getResourceUsage().decAMUsed(nodeLabel,
        resourceToDec);
    // ResourceUsage has its own lock, no addition lock needs here.
    queueUsage.decAMUsed(nodeLabel, resourceToDec);
  }

  /*
   * Usage Ratio
   */
  static private class UsageRatios {
    private Map<String, Float> usageRatios;
    private ReadLock readLock;
    private WriteLock writeLock;

    public UsageRatios() {
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      readLock = lock.readLock();
      writeLock = lock.writeLock();
      usageRatios = new HashMap<String, Float>();
    }

    private void incUsageRatio(String label, float delta) {
      try {
        writeLock.lock();
        Float fl = usageRatios.get(label);
        if (null == fl) {
          fl = new Float(0.0);
        }
        fl += delta;
        usageRatios.put(label, new Float(fl));
      } finally {
        writeLock.unlock();
      }
    }

    float getUsageRatio(String label) {
      try {
        readLock.lock();
        Float f = usageRatios.get(label);
        if (null == f) {
          return 0.0f;
        }
        return f;
      } finally {
        readLock.unlock();
      }
    }

    private void setUsageRatio(String label, float ratio) {
      try {
        writeLock.lock();
        usageRatios.put(label, new Float(ratio));
      } finally {
        writeLock.unlock();
      }
    }
  }

  @VisibleForTesting
  public float getUsageRatio(String label) {
    return qUsageRatios.getUsageRatio(label);
  }

  @VisibleForTesting
  public static class User {
    ResourceUsage userResourceUsage = new ResourceUsage();
    volatile Resource userResourceLimit = Resource.newInstance(0, 0);
    volatile int pendingApplications = 0;
    volatile int activeApplications = 0;
    private UsageRatios userUsageRatios = new UsageRatios();
    private WriteLock writeLock;

    User() {
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      // Nobody uses read-lock now, will add it when necessary
      writeLock = lock.writeLock();
    }

    public ResourceUsage getResourceUsage() {
      return userResourceUsage;
    }
    
    public float resetAndUpdateUsageRatio(
        ResourceCalculator resourceCalculator,
        Resource resource, String nodePartition) {
      try {
        writeLock.lock();
        userUsageRatios.setUsageRatio(nodePartition, 0);
        return updateUsageRatio(resourceCalculator, resource, nodePartition);
      } finally {
        writeLock.unlock();
      }
    }

    public float updateUsageRatio(
        ResourceCalculator resourceCalculator,
        Resource resource, String nodePartition) {
      try {
        writeLock.lock();
        float delta;
        float newRatio = Resources.ratio(resourceCalculator,
            getUsed(nodePartition), resource);
        delta = newRatio - userUsageRatios.getUsageRatio(nodePartition);
        userUsageRatios.setUsageRatio(nodePartition, newRatio);
        return delta;
      } finally {
        writeLock.unlock();
      }
    }

    public Resource getUsed() {
      return userResourceUsage.getUsed();
    }

    public Resource getAllUsed() {
      return userResourceUsage.getAllUsed();
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

    public Resource getConsumedAMResources(String label) {
      return userResourceUsage.getAMUsed(label);
    }

    public int getTotalApplications() {
      return getPendingApplications() + getActiveApplications();
    }
    
    public void submitApplication() {
      try {
        writeLock.lock();
        ++pendingApplications;
      } finally {
        writeLock.unlock();
      }
    }
    
    public void activateApplication() {
      try {
        writeLock.lock();
        --pendingApplications;
        ++activeApplications;
      } finally {
        writeLock.unlock();
      }
    }

    public void finishApplication(boolean wasActive) {
      try {
        writeLock.lock();
        if (wasActive) {
          --activeApplications;
        } else{
          --pendingApplications;
        }
      } finally {
        writeLock.unlock();
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
    try {
      writeLock.lock();
      FiCaSchedulerNode node = scheduler.getNode(
          rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource, attempt,
          rmContainer.getContainer().getResource(), node.getPartition(),
          rmContainer, false);
    } finally {
      writeLock.unlock();
    }

    getParent().recoverContainer(clusterResource, attempt, rmContainer);
  }

  /**
   * Obtain (read-only) collection of pending applications.
   */
  public Collection<FiCaSchedulerApp> getPendingApplications() {
    return Collections.unmodifiableCollection(pendingOrderingPolicy
        .getSchedulableEntities());
  }

  /**
   * Obtain (read-only) collection of active applications.
   */
  public Collection<FiCaSchedulerApp> getApplications() {
    return Collections.unmodifiableCollection(orderingPolicy
        .getSchedulableEntities());
  }

  /**
   * Obtain (read-only) collection of all applications.
   */
  public Collection<FiCaSchedulerApp> getAllApplications() {
    Collection<FiCaSchedulerApp> apps = new HashSet<FiCaSchedulerApp>(
        pendingOrderingPolicy.getSchedulableEntities());
    apps.addAll(orderingPolicy.getSchedulableEntities());

    return Collections.unmodifiableCollection(apps);
  }

  // Consider the headroom for each user in the queue.
  // Total pending for the queue =
  //   sum(for each user(min((user's headroom), sum(user's pending requests))))
  //  NOTE: Used for calculating pedning resources in the preemption monitor.
  public Resource getTotalPendingResourcesConsideringUserLimit(
          Resource resources, String partition) {
    try {
      readLock.lock();
      Map<String, Resource> userNameToHeadroom =
          new HashMap<>();
      Resource pendingConsideringUserLimit = Resource.newInstance(0, 0);
      for (FiCaSchedulerApp app : getApplications()) {
        String userName = app.getUser();
        if (!userNameToHeadroom.containsKey(userName)) {
          User user = getUser(userName);
          Resource headroom = Resources.subtract(
              computeUserLimit(app.getUser(), resources, user, partition,
                  SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY),
              user.getUsed(partition));
          // Make sure headroom is not negative.
          headroom = Resources.componentwiseMax(headroom, Resources.none());
          userNameToHeadroom.put(userName, headroom);
        }
        Resource minpendingConsideringUserLimit = Resources.componentwiseMin(
            userNameToHeadroom.get(userName),
            app.getAppAttemptResourceUsage().getPending(partition));
        Resources.addTo(pendingConsideringUserLimit,
            minpendingConsideringUserLimit);
        Resources.subtractFrom(userNameToHeadroom.get(userName),
            minpendingConsideringUserLimit);
      }
      return pendingConsideringUserLimit;
    } finally {
      readLock.unlock();
    }

  }

  public synchronized Resource getUserLimitPerUser(String userName,
      Resource resources, String partition) {

    // Check user resource limit
    User user = getUser(userName);

    return computeUserLimit(userName, resources, user, partition,
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
  }

  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    try {
      readLock.lock();
      for (FiCaSchedulerApp pendingApp : pendingOrderingPolicy
          .getSchedulableEntities()) {
        apps.add(pendingApp.getApplicationAttemptId());
      }
      for (FiCaSchedulerApp app : orderingPolicy.getSchedulableEntities()) {
        apps.add(app.getApplicationAttemptId());
      }
    } finally {
      readLock.unlock();
    }

  }

  @Override
  public void attachContainer(Resource clusterResource,
      FiCaSchedulerApp application, RMContainer rmContainer) {
    if (application != null) {
      FiCaSchedulerNode node =
          scheduler.getNode(rmContainer.getContainer().getNodeId());
      allocateResource(clusterResource, application, rmContainer.getContainer()
          .getResource(), node.getPartition(), rmContainer, false);
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
          .getResource(), node.getPartition(), rmContainer, false);
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
   * @return all ignored partition exclusivity RMContainers in the LeafQueue,
   *         this will be used by preemption policy.
   */
  public Map<String, TreeSet<RMContainer>>
      getIgnoreExclusivityRMContainers() {
    Map<String, TreeSet<RMContainer>> clonedMap = new HashMap<>();
    try {
      readLock.lock();

      for (Map.Entry<String, TreeSet<RMContainer>> entry : ignorePartitionExclusivityRMContainers
          .entrySet()) {
        clonedMap.put(entry.getKey(), new TreeSet<>(entry.getValue()));
      }

      return clonedMap;

    } finally {
      readLock.unlock();
    }
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
  
  public OrderingPolicy<FiCaSchedulerApp>
      getOrderingPolicy() {
    return orderingPolicy;
  }
  
  void setOrderingPolicy(
      OrderingPolicy<FiCaSchedulerApp> orderingPolicy) {
    try {
      writeLock.lock();
      if (null != this.orderingPolicy) {
        orderingPolicy.addAllSchedulableEntities(
            this.orderingPolicy.getSchedulableEntities());
      }
      this.orderingPolicy = orderingPolicy;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public Priority getDefaultApplicationPriority() {
    return defaultAppPriorityPerQueue;
  }

  /**
   *
   * @param clusterResource Total cluster resource
   * @param decreaseRequest The decrease request
   * @param app The application of interest
   */
  @Override
  public void decreaseContainer(Resource clusterResource,
      SchedContainerChangeRequest decreaseRequest,
      FiCaSchedulerApp app) throws InvalidResourceRequestException {
    // If the container being decreased is reserved, we need to unreserve it
    // first.
    RMContainer rmContainer = decreaseRequest.getRMContainer();
    if (rmContainer.hasIncreaseReservation()) {
      unreserveIncreasedContainer(clusterResource, app,
          (FiCaSchedulerNode)decreaseRequest.getSchedulerNode(), rmContainer);
    }
    boolean resourceDecreased = false;
    Resource resourceBeforeDecrease;
    // Grab queue lock to avoid race condition when getting container resource

    try {
      writeLock.lock();
      // Make sure the decrease request is valid in terms of current resource
      // and target resource. This must be done under the leaf queue lock.
      // Throws exception if the check fails.
      RMServerUtils.checkSchedContainerChangeRequest(decreaseRequest, false);
      // Save resource before decrease for debug log
      resourceBeforeDecrease = Resources.clone(
          rmContainer.getAllocatedResource());
      // Do we have increase request for the same container? If so, remove it
      boolean hasIncreaseRequest = app.removeIncreaseRequest(
          decreaseRequest.getNodeId(),
          decreaseRequest.getRMContainer().getAllocatedSchedulerKey(),
          decreaseRequest.getContainerId());
      if (hasIncreaseRequest) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("While processing decrease requests, found an increase"
              + " request for the same container " + decreaseRequest
              .getContainerId() + ", removed the increase request");
        }
      }
      // Delta capacity is negative when it's a decrease request
      Resource absDelta = Resources.negate(decreaseRequest.getDeltaCapacity());
      if (Resources.equals(absDelta, Resources.none())) {
        // If delta capacity of this decrease request is 0, this decrease
        // request serves the purpose of cancelling an existing increase request
        // if any
        if (LOG.isDebugEnabled()) {
          LOG.debug("Decrease target resource equals to existing resource for"
              + " container:" + decreaseRequest.getContainerId()
              + " ignore this decrease request.");
        }
      } else{
        // Release the delta resource
        releaseResource(clusterResource, app, absDelta,
            decreaseRequest.getNodePartition(),
            decreaseRequest.getRMContainer(), true);
        // Notify application
        app.decreaseContainer(decreaseRequest);
        // Notify node
        decreaseRequest.getSchedulerNode().decreaseContainer(
            decreaseRequest.getContainerId(), absDelta);
        resourceDecreased = true;
      }
    } finally {
      writeLock.unlock();
    }

    if (resourceDecreased) {
      // Notify parent queue outside of leaf queue lock
      getParent().decreaseContainer(clusterResource, decreaseRequest, app);
      LOG.info("Application attempt " + app.getApplicationAttemptId()
          + " decreased container:" + decreaseRequest.getContainerId()
          + " from " + resourceBeforeDecrease + " to "
          + decreaseRequest.getTargetCapacity());
    }
  }

  public void updateApplicationPriority(SchedulerApplication<FiCaSchedulerApp> app,
      Priority newAppPriority) {
    try {
      writeLock.lock();
      FiCaSchedulerApp attempt = app.getCurrentAppAttempt();
      boolean isActive = orderingPolicy.removeSchedulableEntity(attempt);
      if (!isActive) {
        pendingOrderingPolicy.removeSchedulableEntity(attempt);
      }
      // Update new priority in SchedulerApplication
      attempt.setPriority(newAppPriority);

      if (isActive) {
        orderingPolicy.addSchedulableEntity(attempt);
      } else {
        pendingOrderingPolicy.addSchedulableEntity(attempt);
      }
    } finally {
      writeLock.unlock();
    }
  }

  public OrderingPolicy<FiCaSchedulerApp>
      getPendingAppsOrderingPolicy() {
    return pendingOrderingPolicy;
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
