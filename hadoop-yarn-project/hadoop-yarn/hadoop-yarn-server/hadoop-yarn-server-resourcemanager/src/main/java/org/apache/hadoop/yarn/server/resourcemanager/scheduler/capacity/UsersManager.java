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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

/**
 * {@link UsersManager} tracks users in the system and its respective data
 * structures.
 */
@Private
public class UsersManager implements AbstractUsersManager {

  private static final Log LOG = LogFactory.getLog(UsersManager.class);

  /*
   * Member declaration for UsersManager class.
   */
  private final LeafQueue lQueue;
  private final RMNodeLabelsManager labelManager;
  private final ResourceCalculator resourceCalculator;
  private final CapacitySchedulerContext scheduler;
  private Map<String, User> users = new ConcurrentHashMap<>();

  private ResourceUsage totalResUsageForActiveUsers = new ResourceUsage();
  private ResourceUsage totalResUsageForNonActiveUsers = new ResourceUsage();
  private Set<String> activeUsersSet = new HashSet<String>();
  private Set<String> nonActiveUsersSet = new HashSet<String>();

  // Summation of consumed ratios for all users in queue
  private UsageRatios qUsageRatios;

  // To detect whether there is a change in user count for every user-limit
  // calculation.
  private AtomicLong latestVersionOfUsersState = new AtomicLong(0);
  private Map<String, Map<SchedulingMode, Long>> localVersionOfActiveUsersState =
      new HashMap<String, Map<SchedulingMode, Long>>();
  private Map<String, Map<SchedulingMode, Long>> localVersionOfAllUsersState =
      new HashMap<String, Map<SchedulingMode, Long>>();

  private volatile int userLimit;
  private volatile float userLimitFactor;

  private WriteLock writeLock;
  private ReadLock readLock;

  private final QueueMetrics metrics;
  private AtomicInteger activeUsers = new AtomicInteger(0);
  private Map<String, Set<ApplicationId>> usersApplications =
      new HashMap<String, Set<ApplicationId>>();

  // Pre-computed list of user-limits.
  Map<String, Map<SchedulingMode, Resource>> preComputedActiveUserLimit = new ConcurrentHashMap<>();
  Map<String, Map<SchedulingMode, Resource>> preComputedAllUserLimit = new ConcurrentHashMap<>();

  private float activeUsersTimesWeights = 0.0f;
  private float allUsersTimesWeights = 0.0f;

  /**
   * UsageRatios will store the total used resources ratio across all users of
   * the queue.
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
        float usage = 0f;
        if (usageRatios.containsKey(label)) {
          usage = usageRatios.get(label);
        }
        usage += delta;
        usageRatios.put(label, usage);
      } finally {
        writeLock.unlock();
      }
    }

    private float getUsageRatio(String label) {
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
        usageRatios.put(label, ratio);
      } finally {
        writeLock.unlock();
      }
    }
  } /* End of UserRatios class */

  /**
   * User class stores all user related resource usage, application details.
   */
  @VisibleForTesting
  public static class User {
    ResourceUsage userResourceUsage = new ResourceUsage();
    String userName = null;
    volatile Resource userResourceLimit = Resource.newInstance(0, 0);
    private volatile AtomicInteger pendingApplications = new AtomicInteger(0);
    private volatile AtomicInteger activeApplications = new AtomicInteger(0);

    private UsageRatios userUsageRatios = new UsageRatios();
    private WriteLock writeLock;
    private float weight;

    public User(String name) {
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      // Nobody uses read-lock now, will add it when necessary
      writeLock = lock.writeLock();

      this.userName = name;
    }

    public ResourceUsage getResourceUsage() {
      return userResourceUsage;
    }

    public float setAndUpdateUsageRatio(ResourceCalculator resourceCalculator,
        Resource resource, String nodePartition) {
      try {
        writeLock.lock();
        userUsageRatios.setUsageRatio(nodePartition, 0);
        return updateUsageRatio(resourceCalculator, resource, nodePartition);
      } finally {
        writeLock.unlock();
      }
    }

    public float updateUsageRatio(ResourceCalculator resourceCalculator,
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
      return pendingApplications.get();
    }

    public int getActiveApplications() {
      return activeApplications.get();
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
      pendingApplications.incrementAndGet();
    }

    public void activateApplication() {
      pendingApplications.decrementAndGet();
      activeApplications.incrementAndGet();
    }

    public void finishApplication(boolean wasActive) {
      if (wasActive) {
        activeApplications.decrementAndGet();
      } else {
        pendingApplications.decrementAndGet();
      }
    }

    public Resource getUserResourceLimit() {
      return userResourceLimit;
    }

    public void setUserResourceLimit(Resource userResourceLimit) {
      this.userResourceLimit = userResourceLimit;
    }

    public String getUserName() {
      return this.userName;
    }

    @VisibleForTesting
    public void setResourceUsage(ResourceUsage resourceUsage) {
      this.userResourceUsage = resourceUsage;
    }

    /**
     * @return the weight
     */
    public float getWeight() {
      return weight;
    }

    /**
     * @param weight the weight to set
     */
    public void setWeight(float weight) {
      this.weight = weight;
    }
  } /* End of User class */

  /**
   * UsersManager Constructor.
   *
   * @param metrics
   *          Queue Metrics
   * @param lQueue
   *          Leaf Queue Object
   * @param labelManager
   *          Label Manager instance
   * @param scheduler
   *          Capacity Scheduler Context
   * @param resourceCalculator
   *          rc
   */
  public UsersManager(QueueMetrics metrics, LeafQueue lQueue,
      RMNodeLabelsManager labelManager, CapacitySchedulerContext scheduler,
      ResourceCalculator resourceCalculator) {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.lQueue = lQueue;
    this.scheduler = scheduler;
    this.labelManager = labelManager;
    this.resourceCalculator = resourceCalculator;
    this.qUsageRatios = new UsageRatios();
    this.metrics = metrics;

    this.writeLock = lock.writeLock();
    this.readLock = lock.readLock();
  }

  /**
   * Get configured user-limit.
   * @return user limit
   */
  public int getUserLimit() {
    return userLimit;
  }

  /**
   * Set configured user-limit.
   * @param userLimit user limit
   */
  public void setUserLimit(int userLimit) {
    this.userLimit = userLimit;
  }

  /**
   * Get configured user-limit factor.
   * @return user-limit factor
   */
  public float getUserLimitFactor() {
    return userLimitFactor;
  }

  /**
   * Set configured user-limit factor.
   * @param userLimitFactor User Limit factor.
   */
  public void setUserLimitFactor(float userLimitFactor) {
    this.userLimitFactor = userLimitFactor;
  }

  @VisibleForTesting
  public float getUsageRatio(String label) {
    return qUsageRatios.getUsageRatio(label);
  }

  /**
   * Force UsersManager to recompute userlimit.
   */
  public void userLimitNeedsRecompute() {

    // If latestVersionOfUsersState is negative due to overflow, ideally we need
    // to reset it. This method is invoked from UsersManager and LeafQueue and
    // all is happening within write/readLock. Below logic can help to set 0.
    try {
      writeLock.lock();

      long value = latestVersionOfUsersState.incrementAndGet();
      if (value < 0) {
        latestVersionOfUsersState.set(0);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /*
   * Get all users of queue.
   */
  public Map<String, User> getUsers() {
    return users;
  }

  /**
   * Get user object for given user name.
   *
   * @param userName
   *          User Name
   * @return User object
   */
  public User getUser(String userName) {
    return users.get(userName);
  }

  /**
   * Remove user.
   *
   * @param userName
   *          User Name
   */
  public void removeUser(String userName) {
    try {
      writeLock.lock();
      this.users.remove(userName);

      // Remove user from active/non-active list as well.
      activeUsersSet.remove(userName);
      nonActiveUsersSet.remove(userName);
      activeUsersTimesWeights = sumActiveUsersTimesWeights();
      allUsersTimesWeights = sumAllUsersTimesWeights();
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Get and add user if absent.
   *
   * @param userName
   *          User Name
   * @return User object
   */
  public User getUserAndAddIfAbsent(String userName) {
    try {
      writeLock.lock();
      User u = getUser(userName);
      if (null == u) {
        u = new User(userName);
        addUser(userName, u);

        // Add to nonActive list so that resourceUsage could be tracked
        if (!nonActiveUsersSet.contains(userName)) {
          nonActiveUsersSet.add(userName);
        }
      }
      return u;
    } finally {
      writeLock.unlock();
    }
  }

  /*
   * Add a new user
   */
  private void addUser(String userName, User user) {
    this.users.put(userName, user);
    user.setWeight(getUserWeightFromQueue(userName));
    allUsersTimesWeights = sumAllUsersTimesWeights();
  }

  /**
   * @return an ArrayList of UserInfo objects who are active in this queue
   */
  public ArrayList<UserInfo> getUsersInfo() {
    try {
      readLock.lock();
      ArrayList<UserInfo> usersToReturn = new ArrayList<UserInfo>();
      for (Map.Entry<String, User> entry : getUsers().entrySet()) {
        User user = entry.getValue();
        usersToReturn.add(
            new UserInfo(entry.getKey(), Resources.clone(user.getAllUsed()),
                user.getActiveApplications(), user.getPendingApplications(),
                Resources.clone(user.getConsumedAMResources()),
                Resources.clone(user.getUserResourceLimit()),
                user.getResourceUsage(), user.getWeight(),
                activeUsersSet.contains(user.userName)));
      }
      return usersToReturn;
    } finally {
      readLock.unlock();
    }
  }

  private float getUserWeightFromQueue(String userName) {
    Float weight = lQueue.getUserWeights().get(userName);
    return (weight == null) ? 1.0f : weight.floatValue();
  }

  /**
   * Get computed user-limit for all ACTIVE users in this queue. If cached data
   * is invalidated due to resource change, this method also enforce to
   * recompute user-limit.
   *
   * @param userName
   *          Name of user who has submitted one/more app to given queue.
   * @param clusterResource
   *          total cluster resource
   * @param nodePartition
   *          partition name
   * @param schedulingMode
   *          scheduling mode
   *          RESPECT_PARTITION_EXCLUSIVITY/IGNORE_PARTITION_EXCLUSIVITY
   * @return Computed User Limit
   */
  public Resource getComputedResourceLimitForActiveUsers(String userName,
      Resource clusterResource, String nodePartition,
      SchedulingMode schedulingMode) {

    Map<SchedulingMode, Resource> userLimitPerSchedulingMode = preComputedActiveUserLimit
        .get(nodePartition);

    try {
      writeLock.lock();
      if (isRecomputeNeeded(schedulingMode, nodePartition, true)) {
        // recompute
        userLimitPerSchedulingMode = reComputeUserLimits(userName,
            nodePartition, clusterResource, schedulingMode, true);

        // update user count to cache so that we can avoid recompute if no major
        // changes.
        setLocalVersionOfUsersState(nodePartition, schedulingMode, true);
      }
    } finally {
      writeLock.unlock();
    }

    Resource userLimitResource = userLimitPerSchedulingMode.get(schedulingMode);
    User user = getUser(userName);
    float weight = (user == null) ? 1.0f : user.getWeight();
    Resource userSpecificUserLimit =
        Resources.multiplyAndNormalizeDown(resourceCalculator,
            userLimitResource, weight, lQueue.getMinimumAllocation());

    if (user != null) {
      user.setUserResourceLimit(userSpecificUserLimit);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("userLimit is fetched. userLimit=" + userLimitResource
          + ", userSpecificUserLimit=" + userSpecificUserLimit
          + ", schedulingMode=" + schedulingMode
          + ", partition=" + nodePartition);
    }
    return userSpecificUserLimit;
  }

  /**
   * Get computed user-limit for all users in this queue. If cached data is
   * invalidated due to resource change, this method also enforce to recompute
   * user-limit.
   *
   * @param userName
   *          Name of user who has submitted one/more app to given queue.
   * @param clusterResource
   *          total cluster resource
   * @param nodePartition
   *          partition name
   * @param schedulingMode
   *          scheduling mode
   *          RESPECT_PARTITION_EXCLUSIVITY/IGNORE_PARTITION_EXCLUSIVITY
   * @return Computed User Limit
   */
  public Resource getComputedResourceLimitForAllUsers(String userName,
      Resource clusterResource, String nodePartition,
      SchedulingMode schedulingMode) {

    Map<SchedulingMode, Resource> userLimitPerSchedulingMode = preComputedAllUserLimit
        .get(nodePartition);

    try {
      writeLock.lock();
      if (isRecomputeNeeded(schedulingMode, nodePartition, false)) {
        // recompute
        userLimitPerSchedulingMode = reComputeUserLimits(userName,
            nodePartition, clusterResource, schedulingMode, false);

        // update user count to cache so that we can avoid recompute if no major
        // changes.
        setLocalVersionOfUsersState(nodePartition, schedulingMode, false);
      }
    } finally {
      writeLock.unlock();
    }

    Resource userLimitResource = userLimitPerSchedulingMode.get(schedulingMode);
    User user = getUser(userName);
    float weight = (user == null) ? 1.0f : user.getWeight();
    Resource userSpecificUserLimit =
        Resources.multiplyAndNormalizeDown(resourceCalculator,
            userLimitResource, weight, lQueue.getMinimumAllocation());

    if (LOG.isDebugEnabled()) {
      LOG.debug("userLimit is fetched. userLimit=" + userLimitResource
          + ", userSpecificUserLimit=" + userSpecificUserLimit
          + ", schedulingMode=" + schedulingMode
          + ", partition=" + nodePartition);
    }

    return userSpecificUserLimit;
  }

  /*
   * Recompute user-limit under following conditions: 1. cached user-limit does
   * not exist in local map. 2. Total User count doesn't match with local cached
   * version.
   */
  private boolean isRecomputeNeeded(SchedulingMode schedulingMode,
      String nodePartition, boolean isActive) {
    return (getLocalVersionOfUsersState(nodePartition, schedulingMode,
        isActive) != latestVersionOfUsersState.get());
  }

  /*
   * Set Local version of user count per label to invalidate cache if needed.
   */
  private void setLocalVersionOfUsersState(String nodePartition,
      SchedulingMode schedulingMode, boolean isActive) {
    try {
      writeLock.lock();
      Map<String, Map<SchedulingMode, Long>> localVersionOfUsersState = (isActive)
          ? localVersionOfActiveUsersState
          : localVersionOfAllUsersState;

      Map<SchedulingMode, Long> localVersion = localVersionOfUsersState
          .get(nodePartition);
      if (null == localVersion) {
        localVersion = new HashMap<SchedulingMode, Long>();
        localVersionOfUsersState.put(nodePartition, localVersion);
      }

      localVersion.put(schedulingMode, latestVersionOfUsersState.get());
    } finally {
      writeLock.unlock();
    }
  }

  /*
   * Get Local version of user count per label to invalidate cache if needed.
   */
  private long getLocalVersionOfUsersState(String nodePartition,
      SchedulingMode schedulingMode, boolean isActive) {
    try {
      this.readLock.lock();
      Map<String, Map<SchedulingMode, Long>> localVersionOfUsersState = (isActive)
          ? localVersionOfActiveUsersState
          : localVersionOfAllUsersState;

      if (!localVersionOfUsersState.containsKey(nodePartition)) {
        return -1;
      }

      Map<SchedulingMode, Long> localVersion = localVersionOfUsersState
          .get(nodePartition);
      if (!localVersion.containsKey(schedulingMode)) {
        return -1;
      }

      return localVersion.get(schedulingMode);
    } finally {
      readLock.unlock();
    }
  }

  private Map<SchedulingMode, Resource> reComputeUserLimits(String userName,
      String nodePartition, Resource clusterResource,
      SchedulingMode schedulingMode, boolean activeMode) {

    // preselect stored map as per active user-limit or all user computation.
    Map<String, Map<SchedulingMode, Resource>> computedMap = null;
    computedMap = (activeMode)
        ? preComputedActiveUserLimit
        : preComputedAllUserLimit;

    Map<SchedulingMode, Resource> userLimitPerSchedulingMode = computedMap
        .get(nodePartition);

    if (userLimitPerSchedulingMode == null) {
      userLimitPerSchedulingMode = new ConcurrentHashMap<>();
      computedMap.put(nodePartition, userLimitPerSchedulingMode);
    }

    // compute user-limit per scheduling mode.
    Resource computedUserLimit = computeUserLimit(userName, clusterResource,
        nodePartition, schedulingMode, activeMode);

    // update in local storage
    userLimitPerSchedulingMode.put(schedulingMode, computedUserLimit);

    return userLimitPerSchedulingMode;
  }

  private Resource computeUserLimit(String userName, Resource clusterResource,
      String nodePartition, SchedulingMode schedulingMode, boolean activeUser) {
    Resource partitionResource = labelManager.getResourceByLabel(nodePartition,
        clusterResource);

    /*
     * What is our current capacity?
     * * It is equal to the max(required, queue-capacity) if we're running
     * below capacity. The 'max' ensures that jobs in queues with miniscule
     * capacity (< 1 slot) make progress
     * * If we're running over capacity, then its (usedResources + required)
     * (which extra resources we are allocating)
     */
    Resource queueCapacity = lQueue.getEffectiveCapacity(nodePartition);

    /*
     * Assume we have required resource equals to minimumAllocation, this can
     * make sure user limit can continuously increase till queueMaxResource
     * reached.
     */
    Resource required = lQueue.getMinimumAllocation();

    // Allow progress for queues with miniscule capacity
    queueCapacity = Resources.max(resourceCalculator, partitionResource,
        queueCapacity, required);

    /*
     * We want to base the userLimit calculation on
     * max(queueCapacity, usedResources+required). However, we want
     * usedResources to be based on the combined ratios of all the users in the
     * queue so we use consumedRatio to calculate such.
     * The calculation is dependent on how the resourceCalculator calculates the
     * ratio between two Resources. DRF Example: If usedResources is greater
     * than queueCapacity and users have the following [mem,cpu] usages:
     *
     * User1: [10%,20%] - Dominant resource is 20%
     * User2: [30%,10%] - Dominant resource is 30%
     * Then total consumedRatio is then 20+30=50%. Yes, this value can be
     * larger than 100% but for the purposes of making sure all users are
     * getting their fair share, it works.
     */
    Resource consumed = Resources.multiplyAndNormalizeUp(resourceCalculator,
        partitionResource, getUsageRatio(nodePartition),
        lQueue.getMinimumAllocation());
    Resource currentCapacity = Resources.lessThan(resourceCalculator,
        partitionResource, consumed, queueCapacity)
            ? queueCapacity
            : Resources.add(consumed, required);

    /*
     * Never allow a single user to take more than the queue's configured
     * capacity * user-limit-factor. Also, the queue's configured capacity
     * should be higher than queue-hard-limit * ulMin
     */
    float usersSummedByWeight = activeUsersTimesWeights;
    Resource resourceUsed = Resources.add(
                            totalResUsageForActiveUsers.getUsed(nodePartition),
                            required);

    // For non-activeUser calculation, consider all users count.
    if (!activeUser) {
      resourceUsed = currentCapacity;
      usersSummedByWeight = allUsersTimesWeights;
    }

    /*
     * User limit resource is determined by: max(currentCapacity / #activeUsers,
     * currentCapacity * user-limit-percentage%)
     */
    Resource userLimitResource = Resources.max(resourceCalculator,
        partitionResource,
        Resources.divideAndCeil(resourceCalculator, resourceUsed,
            usersSummedByWeight),
        Resources.divideAndCeil(resourceCalculator,
            Resources.multiplyAndRoundDown(currentCapacity, getUserLimit()),
            100));

    // User limit is capped by maxUserLimit
    // - maxUserLimit = queueCapacity * user-limit-factor
    // (RESPECT_PARTITION_EXCLUSIVITY)
    // - maxUserLimit = total-partition-resource (IGNORE_PARTITION_EXCLUSIVITY)
    //
    // In IGNORE_PARTITION_EXCLUSIVITY mode, if a queue cannot access a
    // partition, its guaranteed resource on that partition is 0. And
    // user-limit-factor computation is based on queue's guaranteed capacity. So
    // we will not cap user-limit as well as used resource when doing
    // IGNORE_PARTITION_EXCLUSIVITY allocation.
    Resource maxUserLimit = Resources.none();
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY) {
      maxUserLimit = Resources.multiplyAndRoundDown(queueCapacity,
          getUserLimitFactor());
    } else if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      maxUserLimit = partitionResource;
    }

    // Cap final user limit with maxUserLimit
    userLimitResource = Resources
        .roundUp(resourceCalculator,
            Resources.min(resourceCalculator, partitionResource,
                userLimitResource, maxUserLimit),
            lQueue.getMinimumAllocation());

    if (LOG.isDebugEnabled()) {
      LOG.debug("User limit computation for " + userName
          + ",  in queue: " + lQueue.getQueueName()
          + ",  userLimitPercent=" + lQueue.getUserLimit()
          + ",  userLimitFactor=" + lQueue.getUserLimitFactor()
          + ",  required=" + required
          + ",  consumed=" + consumed
          + ",  user-limit-resource=" + userLimitResource
          + ",  queueCapacity=" + queueCapacity
          + ",  qconsumed=" + lQueue.getQueueResourceUsage().getUsed()
          + ",  currentCapacity=" + currentCapacity
          + ",  activeUsers=" + usersSummedByWeight
          + ",  clusterCapacity=" + clusterResource
          + ",  resourceByLabel=" + partitionResource
          + ",  usageratio=" + getUsageRatio(nodePartition)
          + ",  Partition=" + nodePartition
          + ",  resourceUsed=" + resourceUsed
          + ",  maxUserLimit=" + maxUserLimit
          + ",  userWeight=" + getUser(userName).getWeight()
      );
    }
    return userLimitResource;
  }

  /**
   * Update new usage ratio.
   *
   * @param partition
   *          Node partition
   * @param clusterResource
   *          Cluster Resource
   */
  public void updateUsageRatio(String partition, Resource clusterResource) {
    try {
      writeLock.lock();
      Resource resourceByLabel = labelManager.getResourceByLabel(partition,
          clusterResource);
      float consumed = 0;
      User user;
      for (Map.Entry<String, User> entry : getUsers().entrySet()) {
        user = entry.getValue();
        consumed += user.setAndUpdateUsageRatio(resourceCalculator,
            resourceByLabel, partition);
      }

      qUsageRatios.setUsageRatio(partition, consumed);
    } finally {
      writeLock.unlock();
    }
  }

  /*
   * Increment Queue Usage Ratio.
   */
  private void incQueueUsageRatio(String nodePartition, float delta) {
    qUsageRatios.incUsageRatio(nodePartition, delta);
  }

  @Override
  public void activateApplication(String user, ApplicationId applicationId) {
    try {
      this.writeLock.lock();

      Set<ApplicationId> userApps = usersApplications.get(user);
      if (userApps == null) {
        userApps = new HashSet<ApplicationId>();
        usersApplications.put(user, userApps);
        activeUsers.incrementAndGet();
        metrics.incrActiveUsers();

        // A user is added to active list. Invalidate user-limit cache.
        userLimitNeedsRecompute();
        updateActiveUsersResourceUsage(user);
        if (LOG.isDebugEnabled()) {
          LOG.debug("User " + user + " added to activeUsers, currently: "
              + activeUsers);
        }
      }
      if (userApps.add(applicationId)) {
        metrics.activateApp(user);
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public void deactivateApplication(String user, ApplicationId applicationId) {
    try {
      this.writeLock.lock();

      Set<ApplicationId> userApps = usersApplications.get(user);
      if (userApps != null) {
        if (userApps.remove(applicationId)) {
          metrics.deactivateApp(user);
        }
        if (userApps.isEmpty()) {
          usersApplications.remove(user);
          activeUsers.decrementAndGet();
          metrics.decrActiveUsers();

          // A user is removed from active list. Invalidate user-limit cache.
          userLimitNeedsRecompute();
          updateNonActiveUsersResourceUsage(user);
          if (LOG.isDebugEnabled()) {
            LOG.debug("User " + user + " removed from activeUsers, currently: "
                + activeUsers);
          }
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public int getNumActiveUsers() {
    return activeUsers.get();
  }

  float sumActiveUsersTimesWeights() {
    float count = 0.0f;
    try {
      this.readLock.lock();
      for (String u : activeUsersSet) {
        count += getUser(u).getWeight();
      }
      return count;
    } finally {
      this.readLock.unlock();
    }
  }

  float sumAllUsersTimesWeights() {
    float count = 0.0f;
    try {
      this.readLock.lock();
      for (String u : users.keySet()) {
        count += getUser(u).getWeight();
      }
      return count;
    } finally {
      this.readLock.unlock();
    }
  }

  private void updateActiveUsersResourceUsage(String userName) {
    try {
      this.writeLock.lock();

      // For UT case: We might need to add the user to users list.
      User user = getUserAndAddIfAbsent(userName);
      ResourceUsage resourceUsage = user.getResourceUsage();
      // If User is moved to active list, moved resource usage from non-active
      // to active list.
      if (nonActiveUsersSet.contains(userName)) {
        nonActiveUsersSet.remove(userName);
        activeUsersSet.add(userName);
        activeUsersTimesWeights = sumActiveUsersTimesWeights();

        // Update total resource usage of active and non-active after user
        // is moved from non-active to active.
        for (String partition : resourceUsage.getNodePartitionsSet()) {
          totalResUsageForNonActiveUsers.decUsed(partition,
              resourceUsage.getUsed(partition));
          totalResUsageForActiveUsers.incUsed(partition,
              resourceUsage.getUsed(partition));
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("User '" + userName
              + "' has become active. Hence move user to active list."
              + "Active users size = " + activeUsersSet.size()
              + "Non-active users size = " + nonActiveUsersSet.size()
              + "Total Resource usage for active users="
              + totalResUsageForActiveUsers.getAllUsed() + "."
              + "Total Resource usage for non-active users="
              + totalResUsageForNonActiveUsers.getAllUsed());
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  private void updateNonActiveUsersResourceUsage(String userName) {
    try {
      this.writeLock.lock();

      // For UT case: We might need to add the user to users list.
      User user = getUser(userName);
      if (user == null) return;

      ResourceUsage resourceUsage = user.getResourceUsage();
      // If User is moved to non-active list, moved resource usage from
      // non-active to active list.
      if (activeUsersSet.contains(userName)) {
        activeUsersSet.remove(userName);
        nonActiveUsersSet.add(userName);
        activeUsersTimesWeights = sumActiveUsersTimesWeights();

        // Update total resource usage of active and non-active after user is
        // moved from active to non-active.
        for (String partition : resourceUsage.getNodePartitionsSet()) {
          totalResUsageForActiveUsers.decUsed(partition,
              resourceUsage.getUsed(partition));
          totalResUsageForNonActiveUsers.incUsed(partition,
              resourceUsage.getUsed(partition));

          if (LOG.isDebugEnabled()) {
            LOG.debug("User '" + userName
                + "' has become non-active.Hence move user to non-active list."
                + "Active users size = " + activeUsersSet.size()
                + "Non-active users size = " + nonActiveUsersSet.size()
                + "Total Resource usage for active users="
                + totalResUsageForActiveUsers.getAllUsed() + "."
                + "Total Resource usage for non-active users="
                + totalResUsageForNonActiveUsers.getAllUsed());
          }
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  private ResourceUsage getTotalResourceUsagePerUser(String userName) {
    if (nonActiveUsersSet.contains(userName)) {
      return totalResUsageForNonActiveUsers;
    } else if (activeUsersSet.contains(userName)) {
      return totalResUsageForActiveUsers;
    } else {
      LOG.warn("User '" + userName
          + "' is not present in active/non-active. This is highly unlikely."
          + "We can consider this user in non-active list in this case.");
      return totalResUsageForNonActiveUsers;
    }
  }

  /**
   * During container allocate/release, ensure that all user specific data
   * structures are updated.
   *
   * @param userName
   *          Name of the user
   * @param resource
   *          Resource to increment/decrement
   * @param nodePartition
   *          Node label
   * @param isAllocate
   *          Indicate whether to allocate or release resource
   * @return user
   */
  public User updateUserResourceUsage(String userName, Resource resource,
      String nodePartition, boolean isAllocate) {
    try {
      this.writeLock.lock();

      // TODO, should use getUser, use this method just to avoid UT failure
      // which is caused by wrong invoking order, will fix UT separately
      User user = getUserAndAddIfAbsent(userName);

      // New container is allocated. Invalidate user-limit.
      updateResourceUsagePerUser(user, resource, nodePartition, isAllocate);

      userLimitNeedsRecompute();

      // Update usage ratios
      Resource resourceByLabel = labelManager.getResourceByLabel(nodePartition,
          scheduler.getClusterResource());
      incQueueUsageRatio(nodePartition, user.updateUsageRatio(
          resourceCalculator, resourceByLabel, nodePartition));

      return user;
    } finally {
      this.writeLock.unlock();
    }
  }

  private void updateResourceUsagePerUser(User user, Resource resource,
      String nodePartition, boolean isAllocate) {
    ResourceUsage totalResourceUsageForUsers = getTotalResourceUsagePerUser(
        user.userName);

    if (isAllocate) {
      user.getResourceUsage().incUsed(nodePartition, resource);
      totalResourceUsageForUsers.incUsed(nodePartition, resource);
    } else {
      user.getResourceUsage().decUsed(nodePartition, resource);
      totalResourceUsageForUsers.decUsed(nodePartition, resource);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "User resource is updated." + "Total Resource usage for active users="
              + totalResUsageForActiveUsers.getAllUsed() + "."
              + "Total Resource usage for non-active users="
              + totalResUsageForNonActiveUsers.getAllUsed());
    }
  }

  public void updateUserWeights() {
    try {
      this.writeLock.lock();
      for (Map.Entry<String, User> ue : users.entrySet()) {
        ue.getValue().setWeight(getUserWeightFromQueue(ue.getKey()));
      }
      activeUsersTimesWeights = sumActiveUsersTimesWeights();
      allUsersTimesWeights = sumAllUsersTimesWeights();
      userLimitNeedsRecompute();
    } finally {
      this.writeLock.unlock();
    }
  }
}
