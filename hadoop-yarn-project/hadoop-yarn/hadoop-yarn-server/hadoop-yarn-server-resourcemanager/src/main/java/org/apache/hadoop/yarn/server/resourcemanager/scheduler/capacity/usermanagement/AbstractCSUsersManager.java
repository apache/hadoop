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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.usermanagement;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserWeights;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractCSUsersManager implements AbstractUsersManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractCSUsersManager.class);

  final AbstractLeafQueue lQueue;
  final RMNodeLabelsManager labelManager;
  final ResourceCalculator resourceCalculator;
  final QueueMetrics metrics;

  final Map<String, AbstractCSUser> users = new ConcurrentHashMap<>();

  // Users are marked active if they have a pending resource request (container ask yet to be satisfied)
  // User & AM limits are computed based on total count of active users
  // Below counters are caches to return total active user count in getNumActiveUsers()
  final AtomicInteger activeUsers = new AtomicInteger(0);
  // If an AM is pending, its not considered in above count (because activateApplication() is not called yet)
  // https://issues.apache.org/jira/browse/YARN-4606
  // But total active users needs to consider such users too. This counter tracks users with only pending AMs
  // (users with both pending & active apps would get tracked in activeUsers variable)
  // The flow can be refactored to maintain just a single variable but using submitApplication() rather than activateApplication()
  private volatile int activeUsersWithOnlyPendingApps = 0;

  private volatile float userLimit;
  private volatile float userLimitFactor;

  AbstractCSUsersManager(QueueMetrics metrics, AbstractLeafQueue lQueue,
      RMNodeLabelsManager labelManager, ResourceCalculator resourceCalculator) {
    this.lQueue = lQueue;
    this.labelManager = labelManager;
    this.resourceCalculator = resourceCalculator;
    this.metrics = metrics;
  }

  public static AbstractCSUsersManager createUsersManager(QueueMetrics metrics, AbstractLeafQueue lQueue,
      RMNodeLabelsManager labelManager, ResourceCalculator resourceCalculator) {
    if (lQueue.getQueueContext().getConfiguration().isConcurrentSchedulerEnabled()) {
      return new ConcurrentUsersManager(metrics, lQueue, labelManager, resourceCalculator);
    } else {
      return new UsersManager(metrics, lQueue, labelManager, resourceCalculator);
    }
  }

  /**
   * Get configured user-limit.
   * @return user limit
   */
  public float getUserLimit() {
    return userLimit;
  }

  /**
   * Set configured user-limit.
   * @param userLimit user limit
   */
  public void setUserLimit(float userLimit) {
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

  /*
   * Get all users of queue. This is a weakly consistent view
   */
  public Set<String> getUserNames() {
    return users.keySet();
  }

  /**
   * Get user object for given user name.
   *
   * @param userName
   *          User Name
   * @return User object
   */
  public AbstractCSUser getUser(String userName) {
    return users.get(userName);
  }

  /**
   * This returns a weakly consistent view of number of active users
   * New users can be added or users moved from active to inactive (or vice versa) concurrently
   */
  @Override
  public int getNumActiveUsers() {
    return activeUsers.get() + activeUsersWithOnlyPendingApps;
  }

  /**
   * Can return an inconsistent snapshot of users info depending on the implementation
   * Users can be concurrently added / removed or users resource usage can change concurrently
   */
  public ArrayList<UserInfo> getUsersInfo() {
    ArrayList<UserInfo> usersToReturn = new ArrayList<>();
    for (Map.Entry<String, AbstractCSUser> entry : getUsers().entrySet()) {
      AbstractCSUser user = entry.getValue();
      usersToReturn.add(
          new UserInfo(user.getUserName(),
              Resources.clone(user.userResourceUsage.getAllUsed()),
              user.getActiveApplications(),
              user.getPendingApplications(),
              user.getConsumedAMResourcesCloned(),
              Resources.clone(user.getUserResourceLimit()),
              ResourceUsage.clone(user.userResourceUsage),
              this.getUserWeight(user.getUserName()),
              isActive(user)
          ));
    }
    return usersToReturn;
  }

  /**
   * Should be called whenever queue config is updated
   */
  public abstract void queueConfigUpdated();

  /**
   * Called when a new app is submitted
   * Also creates an user object if it wasn't existing before
   * This is different from activateApplication() which is called when an app has new resource requests
   */
  public abstract void submitApplication(String userName);

  /**
   * A submitted app is now being removed (finished / failed / killed)
   * App could have been active or inactive
   * This is different from deactivateApplication() which is called when an app has no more resource requests
   * This removes the user object if no more apps exist for the user
   */
  public abstract void removeApplication(String userName, boolean wasApplicationActive);

  /**
   * Get computed user-limit for all ACTIVE users in this queue. This could be older cached data
   * or accurate data based on implementation
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
  public abstract Resource getComputedResourceLimitForActiveUsers(String userName,
      Resource clusterResource, String nodePartition,
      SchedulingMode schedulingMode);

  /**
   * Get computed user-limit for all users in this queue. This could be older cached data
   * or accurate data based on implementation
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
  public abstract Resource getComputedResourceLimitForAllUsers(String userName,
      Resource clusterResource, String nodePartition,
      SchedulingMode schedulingMode);

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
   */
  public abstract void updateUserResourceUsage(String userName, Resource resource,
      Resource clusterResource,
      String nodePartition, boolean isAllocate);

  /**
   * Update new usage ratio.
   *
   * @param partition Node partition
   * @param clusterResource cluster resource
   */
  // TODO - remove this API
  public abstract void updateUsageRatio(String partition, Resource clusterResource);

  /**
   * Force UsersManager to recompute userlimit.
   */
  // TODO - remove this API
  public abstract void userLimitNeedsRecompute();

  abstract boolean isActive(AbstractCSUser user);

  // Get consumed resources for the given node partition (filtered based on active users or for all users)
  abstract Resource getConsumedResources(String label, boolean forActiveUsersOnly);

  // Get consumed resources for the given node partition considering dominant resource fairness
  abstract Resource getConsumedResourcesWithDRF(Resource partitionResource, String label);

  // Get sum total of user weights (filtered based on active users or for all users)
  abstract float getTotalUserWeight(boolean forActiveUsersOnly);

  // Do not make this public because clients can then modify the map and break the integrity of the package
  // Clients should use getUserNames() or we need to return an immutable map
  Map<String, AbstractCSUser> getUsers() {
    return users;
  }

  float getUserWeight(String userName) {
    return lQueue.getUserWeights().getByUser(userName);
  }

  Resource getUserSpecificUserLimit(AbstractCSUser user, Resource userLimitResource) {
    float weight = (user == null) ? UserWeights.DEFAULT_WEIGHT : this.getUserWeight(user.getUserName());
    return Resources.multiplyAndNormalizeDown(resourceCalculator,
            userLimitResource, weight, lQueue.getMinimumAllocation());
  }

  // Synchronization is expected from calling methods
  Resource computeUserLimit(Resource clusterResource,
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
    Resource originalCapacity = queueCapacity;

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
    Resource consumed = getConsumedResourcesWithDRF(partitionResource, nodePartition);
    Resource currentCapacity = Resources.lessThan(resourceCalculator,
        partitionResource, consumed, queueCapacity)
        ? queueCapacity
        : Resources.add(consumed, required);

    /*
     * Never allow a single user to take more than the queue's configured
     * capacity * user-limit-factor. Also, the queue's configured capacity
     * should be higher than queue-hard-limit * ulMin
     */
    float usersSummedByWeight = getTotalUserWeight(activeUser);
    Resource resourceUsed;
    if (activeUser) {
      resourceUsed = Resources.add(getConsumedResources(nodePartition, true), required);
    } else {
      // For non-activeUser calculation, consider all users count.
      resourceUsed = currentCapacity;
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
      if (getUserLimitFactor() == -1 ||
          originalCapacity.equals(Resources.none())) {
        // If user-limit-factor set to -1, we should disable user limit.
        //
        // Also prevent incorrect maxUserLimit due to low queueCapacity
        // Can happen if dynamic queue has capacity = 0%
        maxUserLimit = lQueue.
            getEffectiveMaxCapacityDown(
                nodePartition, lQueue.getMinimumAllocation());
      } else {
        maxUserLimit = Resources.multiplyAndRoundDown(queueCapacity,
            getUserLimitFactor());
      }
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
      LOG.debug("User limit computation "
          + ",  in queue: " + lQueue.getQueuePath()
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
          + ",  Partition=" + nodePartition
          + ",  resourceUsed=" + resourceUsed
          + ",  maxUserLimit=" + maxUserLimit
      );
    }
    return userLimitResource;
  }

  // Synchronization is expected from calling methods
  void computeNumActiveUsersWithOnlyPendingApps() {
    int numPendingUsers = 0;
    for (AbstractCSUser user : users.values()) {
      if ((user.getPendingApplications() > 0)
          && (user.getActiveApplications() <= 0)) {
        numPendingUsers++;
      }
    }
    activeUsersWithOnlyPendingApps = numPendingUsers;
  }

}