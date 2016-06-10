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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.AMState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.KillableContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicyForPendingApps;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
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

  private volatile int nodeLocalityDelay;
  private volatile boolean rackLocalityFullReset;

  Map<ApplicationAttemptId, FiCaSchedulerApp> applicationAttemptMap =
      new HashMap<ApplicationAttemptId, FiCaSchedulerApp>();

  private Priority defaultAppPriorityPerQueue;

  private OrderingPolicy<FiCaSchedulerApp> pendingOrderingPolicy = null;

  private volatile float minimumAllocationFactor;

  private Map<String, User> users = new HashMap<String, User>();

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private CapacitySchedulerContext scheduler;
  
  private final ActiveUsersManager activeUsersManager;

  // cache last cluster resource to compute actual capacity
  private Resource lastClusterResource = Resources.none();
  
  private final QueueResourceLimitsInfo queueResourceLimitsInfo =
      new QueueResourceLimitsInfo();

  private volatile ResourceLimits cachedResourceLimitsForHeadroom = null;

  private OrderingPolicy<FiCaSchedulerApp> orderingPolicy = null;

  // record all ignore partition exclusivityRMContainer, this will be used to do
  // preemption, key is the partition of the RMContainer allocated on
  private Map<String, TreeSet<RMContainer>> ignorePartitionExclusivityRMContainers =
      new HashMap<>();

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public LeafQueue(CapacitySchedulerContext cs,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(cs, queueName, parent, old);
    this.scheduler = cs;

    this.activeUsersManager = new ActiveUsersManager(metrics); 

    // One time initialization is enough since it is static ordering policy
    this.pendingOrderingPolicy = new FifoOrderingPolicyForPendingApps();

    if(LOG.isDebugEnabled()) {
      LOG.debug("LeafQueue:" + " name=" + queueName
        + ", fullname=" + getQueuePath());
    }
    
    setupQueueConfigs(cs.getClusterResource());
  }

  protected synchronized void setupQueueConfigs(Resource clusterResource)
      throws IOException {
    super.setupQueueConfigs(clusterResource);
    
    this.lastClusterResource = clusterResource;
    
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
    maxApplicationsPerUser = Math.min(maxApplications,
        (int)(maxApplications * (userLimit / 100.0f) * userLimitFactor));
    
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
    rackLocalityFullReset = conf.getRackLocalityFullReset();

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

    defaultAppPriorityPerQueue = Priority.newInstance(conf
        .getDefaultApplicationPriorityConfPerQueue(getQueuePath()));

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
        "reservationsContinueLooking = " +
        reservationsContinueLooking + "\n" +
        "preemptionDisabled = " + getPreemptionDisabled() + "\n" +
        "defaultAppPriorityPerQueue = " + defaultAppPriorityPerQueue);
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
    return pendingOrderingPolicy.getNumSchedulableEntities();
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
  public QueueInfo getQueueInfo(
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
          .getAllUsed()), user.getActiveApplications(), user
          .getPendingApplications(), Resources.clone(user
          .getConsumedAMResources()), Resources.clone(user
          .getUserResourceLimit()), user.getResourceUsage()));
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
    if (newMax.getMemorySize() < oldMax.getMemorySize()
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
  
  public Resource getAMResourceLimit() {
    return queueUsage.getAMLimit();
  }

  public Resource getAMResourceLimitPerPartition(String nodePartition) {
    return queueUsage.getAMLimit(nodePartition);
  }

  public synchronized Resource calculateAndGetAMResourceLimit() {
    return calculateAndGetAMResourceLimitPerPartition(
        RMNodeLabelsManager.NO_LABEL);
  }

  @VisibleForTesting
  public synchronized Resource getUserAMResourceLimit() {
     return getUserAMResourceLimitPerPartition(RMNodeLabelsManager.NO_LABEL);
  }

  public synchronized Resource getUserAMResourceLimitPerPartition(
      String nodePartition) {
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
        queueCapacities.getAbsoluteCapacity(nodePartition), minimumAllocation);

    Resource userAMLimit = Resources.multiplyAndNormalizeUp(resourceCalculator,
        queuePartitionResource,
        queueCapacities.getMaxAMResourcePercentage(nodePartition)
            * effectiveUserLimit * userLimitFactor, minimumAllocation);
    return Resources.lessThanOrEqual(resourceCalculator, lastClusterResource,
        userAMLimit, getAMResourceLimitPerPartition(nodePartition))
        ? userAMLimit
        : getAMResourceLimitPerPartition(nodePartition);
  }

  public synchronized Resource calculateAndGetAMResourceLimitPerPartition(
      String nodePartition) {
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
        queueCapacities.getAbsoluteCapacity(nodePartition), minimumAllocation);

    Resource queueCurrentLimit = Resources.none();
    // For non-labeled partition, we need to consider the current queue
    // usage limit.
    if (nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      synchronized (queueResourceLimitsInfo) {
        queueCurrentLimit = queueResourceLimitsInfo.getQueueCurrentLimit();
      }
    }

    float amResourcePercent = queueCapacities
        .getMaxAMResourcePercentage(nodePartition);

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
  }

  private synchronized void activateApplications() {
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
        fsApp.hasNext();) {
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
        LOG.debug("application "+application.getId() +" AMResource "
            + application.getAMResource(partitionName)
            + " maxAMResourcePerQueuePercent " + maxAMResourcePerQueuePercent
            + " amLimit " + amLimit + " lastClusterResource "
            + lastClusterResource + " amIfStarted " + amIfStarted
            + " AM node-partition name " + partitionName);
      }

      if (!Resources.lessThanOrEqual(resourceCalculator, lastClusterResource,
          amIfStarted, amLimit)) {
        if (getNumActiveApplications() < 1
            || (Resources.lessThanOrEqual(resourceCalculator,
                lastClusterResource, queueUsage.getAMUsed(partitionName),
                Resources.none()))) {
          LOG.warn("maximum-am-resource-percent is insufficient to start a"
              + " single application in queue, it is likely set too low."
              + " skipping enforcement to allow at least one application"
              + " to start");
        } else {
          application.updateAMContainerDiagnostics(AMState.INACTIVATED,
              CSAMContainerLaunchDiagnosticsConstants.QUEUE_AM_RESOURCE_LIMIT_EXCEED);
          LOG.info("Not activating application " + applicationId
              + " as  amIfStarted: " + amIfStarted + " exceeds amLimit: "
              + amLimit);
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
        if (getNumActiveApplications() < 1
            || (Resources.lessThanOrEqual(resourceCalculator,
                lastClusterResource, queueUsage.getAMUsed(partitionName),
                Resources.none()))) {
          LOG.warn("maximum-am-resource-percent is insufficient to start a"
              + " single application in queue for user, it is likely set too"
              + " low. skipping enforcement to allow at least one application"
              + " to start");
        } else {
          application.updateAMContainerDiagnostics(AMState.INACTIVATED,
              CSAMContainerLaunchDiagnosticsConstants.USER_AM_RESOURCE_LIMIT_EXCEED);
          LOG.info("Not activating application " + applicationId
              + " for user: " + user + " as userAmIfStarted: "
              + userAmIfStarted + " exceeds userAmLimit: " + userAMLimit);
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
      LOG.info("Application " + applicationId + " from user: "
          + application.getUser() + " activated in queue: " + getQueueName());
    }
  }
  
  private synchronized void addApplicationAttempt(FiCaSchedulerApp application,
      User user) {
    // Accept 
    user.submitApplication();
    getPendingAppsOrderingPolicy().addSchedulableEntity(application);
    applicationAttemptMap.put(application.getApplicationAttemptId(), application);

    // Activate applications
    activateApplications();
    
    LOG.info("Application added -" +
        " appId: " + application.getApplicationId() +
        " user: " + application.getUser() + "," +
        " leaf-queue: " + getQueueName() +
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
    String partitionName = application.getAppAMNodePartitionName();
    boolean wasActive =
      orderingPolicy.removeSchedulableEntity(application);
    if (!wasActive) {
      pendingOrderingPolicy.removeSchedulableEntity(application);
    } else {
      queueUsage.decAMUsed(partitionName,
          application.getAMResource(partitionName));
      user.getResourceUsage().decAMUsed(partitionName,
          application.getAMResource(partitionName));
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
  
  private void handleExcessReservedContainer(Resource clusterResource,
      CSAssignment assignment, FiCaSchedulerNode node, FiCaSchedulerApp app) {
    if (assignment.getExcessReservation() != null) {
      RMContainer excessReservedContainer = assignment.getExcessReservation();
      
      if (excessReservedContainer.hasIncreaseReservation()) {
        unreserveIncreasedContainer(clusterResource,
            app, node, excessReservedContainer);
      } else {
        completedContainer(clusterResource, assignment.getApplication(),
            scheduler.getNode(excessReservedContainer.getAllocatedNode()),
            excessReservedContainer,
            SchedulerUtils.createAbnormalContainerStatus(
                excessReservedContainer.getContainerId(),
                SchedulerUtils.UNRESERVED_CONTAINER),
            RMContainerEventType.RELEASED, null, false);
      }

      assignment.setExcessReservation(null);
    }
  }

  private void killToPreemptContainers(Resource clusterResource,
      FiCaSchedulerNode node,
      CSAssignment assignment) {
    if (assignment.getContainersToKill() != null) {
      StringBuilder sb = new StringBuilder("Killing containers: [");

      for (RMContainer c : assignment.getContainersToKill()) {
        FiCaSchedulerApp application = csContext.getApplicationAttempt(
            c.getApplicationAttemptId());
        LeafQueue q = application.getCSLeafQueue();
        q.completedContainer(clusterResource, application, node, c, SchedulerUtils
                .createPreemptedContainerStatus(c.getContainerId(),
                    SchedulerUtils.PREEMPTED_CONTAINER), RMContainerEventType.KILL,
            null, false);
        sb.append("(container=" + c.getContainerId() + " resource=" + c
            .getAllocatedResource() + ")");
      }

      sb.append("] for container=" + assignment.getAssignmentInformation()
          .getFirstAllocatedOrReservedContainerId() + " resource=" + assignment
          .getResource());
      LOG.info(sb.toString());

    }
  }

  private void setPreemptionAllowed(ResourceLimits limits, String nodePartition) {
    // Set preemption-allowed:
    // For leaf queue, only under-utilized queue is allowed to preempt resources from other queues
    float usedCapacity = queueCapacities.getAbsoluteUsedCapacity(nodePartition);
    float guaranteedCapacity = queueCapacities.getAbsoluteCapacity(nodePartition);
    limits.setIsAllowPreemption(usedCapacity < guaranteedCapacity);
  }
  
  @Override
  public synchronized CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits currentResourceLimits,
      SchedulingMode schedulingMode) {
    updateCurrentResourceLimits(currentResourceLimits, clusterResource);

    if (LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getNodeName()
          + " #applications=" + orderingPolicy.getNumSchedulableEntities());
    }

    setPreemptionAllowed(currentResourceLimits, node.getPartition());

    // Check for reserved resources
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      FiCaSchedulerApp application =
          getApplication(reservedContainer.getApplicationAttemptId());
      synchronized (application) {
        CSAssignment assignment =
            application.assignContainers(clusterResource, node,
                currentResourceLimits, schedulingMode, reservedContainer);
        handleExcessReservedContainer(clusterResource, assignment, node,
            application);
        killToPreemptContainers(clusterResource, node, assignment);
        return assignment;
      }
    }

    // if our queue cannot access this node, just return
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY
        && !accessibleToPartition(node.getPartition())) {
      return CSAssignment.NULL_ASSIGNMENT;
    }

    // Check if this queue need more resource, simply skip allocation if this
    // queue doesn't need more resources.
    if (!hasPendingResourceRequest(node.getPartition(), clusterResource,
        schedulingMode)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip this queue=" + getQueuePath()
            + ", because it doesn't need more resource, schedulingMode="
            + schedulingMode.name() + " node-partition=" + node.getPartition());
      }
      return CSAssignment.NULL_ASSIGNMENT;
    }

    for (Iterator<FiCaSchedulerApp> assignmentIterator =
        orderingPolicy.getAssignmentIterator(); assignmentIterator.hasNext();) {
      FiCaSchedulerApp application = assignmentIterator.next();

      // Check queue max-capacity limit
      if (!super.canAssignToThisQueue(clusterResource, node.getPartition(),
          currentResourceLimits, application.getCurrentReservation(),
          schedulingMode)) {
        return CSAssignment.NULL_ASSIGNMENT;
      }
      
      Resource userLimit =
          computeUserLimitAndSetHeadroom(application, clusterResource,
              node.getPartition(), schedulingMode);

      // Check user limit
      if (!canAssignToUser(clusterResource, application.getUser(), userLimit,
          application, node.getPartition(), currentResourceLimits)) {
        application.updateAMContainerDiagnostics(AMState.ACTIVATED,
            "User capacity has reached its maximum limit.");
        continue;
      }

      // Try to schedule
      CSAssignment assignment =
          application.assignContainers(clusterResource, node,
              currentResourceLimits, schedulingMode, null);

      if (LOG.isDebugEnabled()) {
        LOG.debug("post-assignContainers for application "
            + application.getApplicationId());
        application.showRequests();
      }

      // Did we schedule or reserve a container?
      Resource assigned = assignment.getResource();
      
      handleExcessReservedContainer(clusterResource, assignment, node,
          application);
      killToPreemptContainers(clusterResource, node, assignment);

      if (Resources.greaterThan(resourceCalculator, clusterResource, assigned,
          Resources.none())) {
        // Get reserved or allocated container from application
        RMContainer reservedOrAllocatedRMContainer =
            application.getRMContainer(assignment.getAssignmentInformation()
                .getFirstAllocatedOrReservedContainerId());

        // Book-keeping
        // Note: Update headroom to account for current allocation too...
        allocateResource(clusterResource, application, assigned,
            node.getPartition(), reservedOrAllocatedRMContainer,
            assignment.isIncreasedAllocation());

        // Update reserved metrics
        Resource reservedRes = assignment.getAssignmentInformation()
            .getReserved();
        if (reservedRes != null && !reservedRes.equals(Resources.none())) {
          incReservedResource(node.getPartition(), reservedRes);
        }

        // Done
        return assignment;
      } else if (assignment.getSkipped()) {
        application.updateNodeInfoForAMDiagnostics(node);
      } else {
        // If we don't allocate anything, and it is not skipped by application,
        // we will return to respect FIFO of applications
        return CSAssignment.NULL_ASSIGNMENT;
      }
    }

    return CSAssignment.NULL_ASSIGNMENT;
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
        computeUserLimit(application, clusterResource, user, partition,
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY), partition);
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

  @Lock({LeafQueue.class, FiCaSchedulerApp.class})
  Resource computeUserLimitAndSetHeadroom(FiCaSchedulerApp application,
      Resource clusterResource, String nodePartition,
      SchedulingMode schedulingMode) {
    String user = application.getUser();
    User queueUser = getUser(user);

    // Compute user limit respect requested labels,
    // TODO, need consider headroom respect labels also
    Resource userLimit =
        computeUserLimit(application, clusterResource, queueUser,
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
  private Resource computeUserLimit(FiCaSchedulerApp application,
      Resource clusterResource, User user,
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

    // Assume we have required resource equals to minimumAllocation, this can
    // make sure user limit can continuously increase till queueMaxResource
    // reached.
    Resource required = minimumAllocation;

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
      String nodePartition, ResourceLimits currentResourceLimits) {
    User user = getUser(userName);

    currentResourceLimits.setAmountNeededUnreserve(Resources.none());

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
            Resources.subtract(user.getUsed(),
                application.getCurrentReservation()), limit)) {

          if (LOG.isDebugEnabled()) {
            LOG.debug("User " + userName + " in queue " + getQueueName()
                + " will exceed limit based on reservations - " + " consumed: "
                + user.getUsed() + " reserved: "
                + application.getCurrentReservation() + " limit: " + limit);
          }
          Resource amountNeededToUnreserve =
              Resources.subtract(user.getUsed(nodePartition), limit);
          // we can only acquire a new container if we unreserve first to
          // respect user-limit
          currentResourceLimits.setAmountNeededUnreserve(amountNeededToUnreserve);
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
  
  @Override
  public void unreserveIncreasedContainer(Resource clusterResource,
      FiCaSchedulerApp app, FiCaSchedulerNode node, RMContainer rmContainer) {
    boolean removed = false;
    Priority priority = null;
    
    synchronized (this) {
      if (rmContainer.getContainer() != null) {
        priority = rmContainer.getContainer().getPriority();
      }

      if (null != priority) {
        removed = app.unreserve(rmContainer.getContainer().getPriority(), node,
            rmContainer);
      }

      if (removed) {
        // Inform the ordering policy
        orderingPolicy.containerReleased(app, rmContainer);

        releaseResource(clusterResource, app, rmContainer.getReservedResource(),
            node.getPartition(), rmContainer, true);
      }
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
          rmContainer.getAllocatedPriority(), rmContainer.getContainerId());

      boolean removed = false;

      // Careful! Locking order is important!
      synchronized (this) {

        Container container = rmContainer.getContainer();

        // Inform the application & the node
        // Note: It's safe to assume that all state changes to RMContainer
        // happen under scheduler's lock... 
        // So, this is, in effect, a transaction across application & node
        if (rmContainer.getState() == RMContainerState.RESERVED) {
          removed = application.unreserve(rmContainer.getReservedPriority(),
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
          
          releaseResource(clusterResource, application, container.getResource(),
              node.getPartition(), rmContainer, false);
        }
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

  synchronized void allocateResource(Resource clusterResource,
      SchedulerApplicationAttempt application, Resource resource,
      String nodePartition, RMContainer rmContainer,
      boolean isIncreasedAllocation) {
    super.allocateResource(clusterResource, resource, nodePartition,
        isIncreasedAllocation);
    
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
      LOG.debug(getQueueName() +
          " user=" + userName +
          " used=" + queueUsage.getUsed() + " numContainers=" + numContainers +
          " headroom = " + application.getHeadroom() +
          " user-resources=" + user.getUsed()
          );
    }
  }

  synchronized void releaseResource(Resource clusterResource,
      FiCaSchedulerApp application, Resource resource, String nodePartition,
      RMContainer rmContainer, boolean isChangeResource) {
    super.releaseResource(clusterResource, resource, nodePartition,
        isChangeResource);
    
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

    if (LOG.isDebugEnabled()) {
      LOG.debug(getQueueName() +
          " used=" + queueUsage.getUsed() + " numContainers=" + numContainers +
          " user=" + userName + " user-resources=" + user.getUsed());
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
  public synchronized void updateClusterResource(Resource clusterResource,
      ResourceLimits currentResourceLimits) {
    updateCurrentResourceLimits(currentResourceLimits, clusterResource);
    lastClusterResource = clusterResource;
    
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
            RMNodeLabelsManager.NO_LABEL,
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
      }
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
          .getResource(), node.getPartition(), rmContainer, false);
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

  // Consider the headroom for each user in the queue.
  // Total pending for the queue =
  //   sum(for each user(min((user's headroom), sum(user's pending requests))))
  //  NOTE: Used for calculating pedning resources in the preemption monitor.
  public synchronized Resource getTotalPendingResourcesConsideringUserLimit(
          Resource resources, String partition) {
    Map<String, Resource> userNameToHeadroom = new HashMap<String, Resource>();
    Resource pendingConsideringUserLimit = Resource.newInstance(0, 0);
    for (FiCaSchedulerApp app : getApplications()) {
      String userName = app.getUser();
      if (!userNameToHeadroom.containsKey(userName)) {
        User user = getUser(userName);
        Resource headroom = Resources.subtract(
            computeUserLimit(app, resources, user, partition,
                SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY),
                user.getUsed(partition));
        // Make sure headroom is not negative.
        headroom = Resources.componentwiseMax(headroom, Resources.none());
        userNameToHeadroom.put(userName, headroom);
      }
      Resource minpendingConsideringUserLimit =
          Resources.componentwiseMin(userNameToHeadroom.get(userName),
                       app.getAppAttemptResourceUsage().getPending(partition));
      Resources.addTo(pendingConsideringUserLimit,
          minpendingConsideringUserLimit);
      Resources.subtractFrom(
          userNameToHeadroom.get(userName), minpendingConsideringUserLimit);
    }
    return pendingConsideringUserLimit;
  }

  @Override
  public synchronized void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    for (FiCaSchedulerApp pendingApp : pendingOrderingPolicy
        .getSchedulableEntities()) {
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
    if (null != this.orderingPolicy) {
      orderingPolicy.addAllSchedulableEntities(this.orderingPolicy
          .getSchedulableEntities());
    }
    this.orderingPolicy = orderingPolicy;
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
    synchronized (this) {
      // Make sure the decrease request is valid in terms of current resource
      // and target resource. This must be done under the leaf queue lock.
      // Throws exception if the check fails.
      RMServerUtils.checkSchedContainerChangeRequest(decreaseRequest, false);
      // Save resource before decrease for debug log
      resourceBeforeDecrease =
          Resources.clone(rmContainer.getAllocatedResource());
      // Do we have increase request for the same container? If so, remove it
      boolean hasIncreaseRequest =
          app.removeIncreaseRequest(decreaseRequest.getNodeId(),
              decreaseRequest.getPriority(), decreaseRequest.getContainerId());
      if (hasIncreaseRequest) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("While processing decrease requests, found an increase"
              + " request for the same container "
              + decreaseRequest.getContainerId()
              + ", removed the increase request");
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
      } else {
        // Release the delta resource
        releaseResource(clusterResource, app, absDelta,
            decreaseRequest.getNodePartition(),
            decreaseRequest.getRMContainer(),
            true);
        // Notify application
        app.decreaseContainer(decreaseRequest);
        // Notify node
        decreaseRequest.getSchedulerNode()
            .decreaseContainer(decreaseRequest.getContainerId(), absDelta);
        resourceDecreased = true;
      }
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

  public synchronized OrderingPolicy<FiCaSchedulerApp>
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
