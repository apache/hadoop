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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * {@link UsersManager} tracks users in the system and its respective data
 * structures.
 */
// TODO - make UsersManager package private
public class UsersManager extends AbstractCSUsersManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(UsersManager.class);

  private ResourceUsage totalResUsageForActiveUsers = new ResourceUsage();
  private ResourceUsage totalResUsageForNonActiveUsers = new ResourceUsage();
  private Set<String> activeUsersSet = new HashSet<String>();
  private Set<String> nonActiveUsersSet = new HashSet<String>();

  // Summation of consumed ratios for all users in queue
  private UsageRatios qUsageRatios;

  // To detect whether there is a change in user count for every user-limit
  // calculation.
  private long latestVersionOfUsersState = 0;
  private Map<String, Map<SchedulingMode, Long>> localVersionOfActiveUsersState =
      new HashMap<String, Map<SchedulingMode, Long>>();
  private Map<String, Map<SchedulingMode, Long>> localVersionOfAllUsersState =
      new HashMap<String, Map<SchedulingMode, Long>>();

  private WriteLock writeLock;
  private ReadLock readLock;

  private Map<String, Set<ApplicationId>> usersApplications =
      new HashMap<String, Set<ApplicationId>>();

  // Pre-computed list of user-limits.
  @VisibleForTesting
  private Map<String, Map<SchedulingMode, Resource>> preComputedActiveUserLimit =
      new HashMap<>();
  @VisibleForTesting
  private Map<String, Map<SchedulingMode, Resource>> preComputedAllUserLimit =
      new HashMap<>();

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

    private UsageRatios() {
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      readLock = lock.readLock();
      writeLock = lock.writeLock();
      usageRatios = new HashMap<String, Float>();
    }

    private void incUsageRatio(String label, float delta) {
      writeLock.lock();
      try {
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
      readLock.lock();
      try {
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
      writeLock.lock();
      try {
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
  // TODO - make User private
  public static class User extends AbstractCSUser {

    private UsageRatios userUsageRatios = new UsageRatios();
    private WriteLock writeLock;

    private User(String name) {
      super(name);
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      // Nobody uses read-lock now, will add it when necessary
      writeLock = lock.writeLock();
    }

    private float setAndUpdateUsageRatio(ResourceCalculator resourceCalculator,
        Resource resource, String nodePartition) {
      writeLock.lock();
      try {
        userUsageRatios.setUsageRatio(nodePartition, 0);
        return updateUsageRatio(resourceCalculator, resource, nodePartition);
      } finally {
        writeLock.unlock();
      }
    }

    private float updateUsageRatio(ResourceCalculator resourceCalculator,
        Resource resource, String nodePartition) {
      writeLock.lock();
      try {
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
   * @param resourceCalculator
   *          rc
   */
  UsersManager(QueueMetrics metrics, AbstractLeafQueue lQueue,
      RMNodeLabelsManager labelManager, ResourceCalculator resourceCalculator) {
    super(metrics, lQueue, labelManager, resourceCalculator);
    this.qUsageRatios = new UsageRatios();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.writeLock = lock.writeLock();
    this.readLock = lock.readLock();
  }

  @Override
  public void submitApplication(String userName) {
    writeLock.lock();
    try {
      AbstractCSUser user = this.getUserAndAddIfAbsent(userName);
      user.submitApplication();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void removeApplication(String userName, boolean wasApplicationActive) {
    writeLock.lock();
    try {
      AbstractCSUser user = this.getUser(userName);
      if (user == null) {
        LOG.error("Remove application called without user " + userName + " present");
      } else {
        user.finishApplication(wasApplicationActive);
        if (user.getTotalApplications() == 0) {
          this.removeUser(userName);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  private float getUsageRatio(String label) {
    return qUsageRatios.getUsageRatio(label);
  }

  @Override
  public void userLimitNeedsRecompute() {

    // If latestVersionOfUsersState is negative due to overflow, ideally we need
    // to reset it. This method is invoked from UsersManager and LeafQueue and
    // all is happening within write/readLock. Below logic can help to set 0.
    writeLock.lock();
    try {

      long value = ++latestVersionOfUsersState;
      if (value < 0) {
        latestVersionOfUsersState = 0;
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  boolean isActive(AbstractCSUser user) {
    return activeUsersSet.contains(user.getUserName());
  }

  @Override
  Resource getConsumedResources(String label, boolean forActiveUsersOnly) {
    if (forActiveUsersOnly) {
      return totalResUsageForActiveUsers.getUsed(label);
    } else {
      return Resources.add(totalResUsageForActiveUsers.getUsed(label), totalResUsageForNonActiveUsers.getUsed(label));
    }
  }

  @Override
  float getTotalUserWeight(boolean forActiveUsersOnly) {
    return forActiveUsersOnly ? activeUsersTimesWeights : allUsersTimesWeights;
  }

  /**
   * Remove user.
   *
   * @param userName
   *          User Name
   */
  private void removeUser(String userName) {
    writeLock.lock();
    try {
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
  private AbstractCSUser getUserAndAddIfAbsent(String userName) {
    writeLock.lock();
    try {
      AbstractCSUser u = getUser(userName);
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
  private void addUser(String userName, AbstractCSUser user) {
    this.users.put(userName, user);
    allUsersTimesWeights = sumAllUsersTimesWeights();
  }

  /**
   * @return an ArrayList of UserInfo objects who are active in this queue
   */
  @Override
  public ArrayList<UserInfo> getUsersInfo() {
    readLock.lock();
    try {
      return super.getUsersInfo();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get computed user-limit for all ACTIVE users in this queue. If cached data
   * is invalidated due to resource change, this method also enforce to
   * recompute user-limit.
   */
  @Override
  public Resource getComputedResourceLimitForActiveUsers(String userName,
      Resource clusterResource, String nodePartition,
      SchedulingMode schedulingMode) {

    Map<SchedulingMode, Resource> userLimitPerSchedulingMode;

    writeLock.lock();
    try {
      userLimitPerSchedulingMode =
          preComputedActiveUserLimit.get(nodePartition);
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

    AbstractCSUser user = getUser(userName);
    Resource userSpecificUserLimit = getUserSpecificUserLimit(user, userLimitResource);

    if (user != null) {
      user.setUserResourceLimit(userSpecificUserLimit);
    }

    LOG.debug("userLimit is fetched for user={}. userLimit={}, userSpecificUserLimit={},"
        + " schedulingMode={}, partition={}, usageRatio={}", userName, userLimitResource,
        userSpecificUserLimit, schedulingMode, nodePartition, getUsageRatio(nodePartition));

    return userSpecificUserLimit;
  }

  /**
   * Get computed user-limit for all users in this queue. If cached data is
   * invalidated due to resource change, this method also enforce to recompute
   * user-limit.
   */
  @Override
  public Resource getComputedResourceLimitForAllUsers(String userName,
      Resource clusterResource, String nodePartition,
      SchedulingMode schedulingMode) {

    Map<SchedulingMode, Resource> userLimitPerSchedulingMode;

    writeLock.lock();
    try {
      userLimitPerSchedulingMode = preComputedAllUserLimit.get(nodePartition);
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
    AbstractCSUser user = getUser(userName);
    Resource userSpecificUserLimit = getUserSpecificUserLimit(user, userLimitResource);

    LOG.debug("userLimit is fetched for user={}. userLimit={}, userSpecificUserLimit={},"
            + " schedulingMode={}, partition={}, usageRatio={}", userName, userLimitResource,
        userSpecificUserLimit, schedulingMode, nodePartition, getUsageRatio(nodePartition));

    return userSpecificUserLimit;
  }

  // TODO - remove this API
  public long getLatestVersionOfUsersState() {
    readLock.lock();
    try {
      return latestVersionOfUsersState;
    } finally {
      readLock.unlock();
    }
  }

  /*
   * Recompute user-limit under following conditions: 1. cached user-limit does
   * not exist in local map. 2. Total User count doesn't match with local cached
   * version.
   */
  private boolean isRecomputeNeeded(SchedulingMode schedulingMode,
      String nodePartition, boolean isActive) {
    readLock.lock();
    try {
      return (getLocalVersionOfUsersState(nodePartition, schedulingMode,
          isActive) != latestVersionOfUsersState);
    } finally {
      readLock.unlock();
    }
  }

  /*
   * Set Local version of user count per label to invalidate cache if needed.
   */
  private void setLocalVersionOfUsersState(String nodePartition,
      SchedulingMode schedulingMode, boolean isActive) {
    writeLock.lock();
    try {
      Map<String, Map<SchedulingMode, Long>> localVersionOfUsersState = (isActive)
          ? localVersionOfActiveUsersState
          : localVersionOfAllUsersState;

      Map<SchedulingMode, Long> localVersion = localVersionOfUsersState
          .get(nodePartition);
      if (null == localVersion) {
        localVersion = new HashMap<SchedulingMode, Long>();
        localVersionOfUsersState.put(nodePartition, localVersion);
      }

      localVersion.put(schedulingMode, latestVersionOfUsersState);
    } finally {
      writeLock.unlock();
    }
  }

  /*
   * Get Local version of user count per label to invalidate cache if needed.
   */
  private long getLocalVersionOfUsersState(String nodePartition,
      SchedulingMode schedulingMode, boolean isActive) {
    this.readLock.lock();
    try {
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
    Resource computedUserLimit = computeUserLimit(clusterResource,
        nodePartition, schedulingMode, activeMode);

    // update in local storage
    userLimitPerSchedulingMode.put(schedulingMode, computedUserLimit);

    computeNumActiveUsersWithOnlyPendingApps();

    return userLimitPerSchedulingMode;
  }

  @Override
  public void updateUsageRatio(String partition, Resource clusterResource) {
    writeLock.lock();
    try {
      Resource resourceByLabel = labelManager.getResourceByLabel(partition,
          clusterResource);
      float consumed = 0;
      User user;
      for (Map.Entry<String, AbstractCSUser> entry : getUsers().entrySet()) {
        user = (User) entry.getValue();
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

    this.writeLock.lock();
    try {
      AbstractCSUser userDesc = getUser(user);
      if (userDesc != null && userDesc.getActiveApplications() <= 0) {
        return;
      }

      Set<ApplicationId> userApps = usersApplications.get(user);
      if (userApps == null) {
        userApps = new HashSet<ApplicationId>();
        usersApplications.put(user, userApps);
        activeUsers.incrementAndGet();
        metrics.incrActiveUsers();

        // A user is added to active list. Invalidate user-limit cache.
        userLimitNeedsRecompute();
        updateActiveUsersResourceUsage(user);
        LOG.debug("User {} added to activeUsers, currently: {}",
            user, activeUsers);
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

    this.writeLock.lock();
    try {
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
          LOG.debug("User {} removed from activeUsers, currently: {}",
              user, activeUsers);
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  private float sumActiveUsersTimesWeights() {
    float count = 0.0f;
    this.readLock.lock();
    try {
      for (String userName : activeUsersSet) {
        count += this.getUserWeight(userName);
      }
      return count;
    } finally {
      this.readLock.unlock();
    }
  }

  private float sumAllUsersTimesWeights() {
    float count = 0.0f;
    this.readLock.lock();
    try {
      for (String userName : users.keySet()) {
        count += this.getUserWeight(userName);
      }
      return count;
    } finally {
      this.readLock.unlock();
    }
  }

  private void updateActiveUsersResourceUsage(String userName) {
    this.writeLock.lock();
    try {
      AbstractCSUser user = getUser(userName);
      // If User is moved to active list, moved resource usage from non-active
      // to active list.
      if (nonActiveUsersSet.contains(userName)) {
        nonActiveUsersSet.remove(userName);
        activeUsersSet.add(userName);
        activeUsersTimesWeights = sumActiveUsersTimesWeights();

        // Update total resource usage of active and non-active after user
        // is moved from non-active to active.
        for (String partition : user.userResourceUsage.getExistingNodeLabels()) {
          totalResUsageForNonActiveUsers.decUsed(partition,
              user.getUsed(partition));
          totalResUsageForActiveUsers.incUsed(partition,
              user.getUsed(partition));
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
    this.writeLock.lock();
    try {

      AbstractCSUser user = getUser(userName);
      if (user == null) return;

      // If User is moved to non-active list, moved resource usage from
      // non-active to active list.
      if (activeUsersSet.contains(userName)) {
        activeUsersSet.remove(userName);
        nonActiveUsersSet.add(userName);
        activeUsersTimesWeights = sumActiveUsersTimesWeights();

        // Update total resource usage of active and non-active after user is
        // moved from active to non-active.
        for (String partition : user.userResourceUsage.getExistingNodeLabels()) {
          totalResUsageForActiveUsers.decUsed(partition,
              user.getUsed(partition));
          totalResUsageForNonActiveUsers.incUsed(partition,
              user.getUsed(partition));

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

  @Override
  public void updateUserResourceUsage(String userName, Resource resource,
      Resource clusterResource,
      String nodePartition, boolean isAllocate) {
    this.writeLock.lock();
    try {

      User user = (User) getUser(userName);

      // New container is allocated. Invalidate user-limit.
      updateResourceUsagePerUser(user, resource, nodePartition, isAllocate);

      userLimitNeedsRecompute();

      // Update usage ratios
      Resource resourceByLabel = labelManager.getResourceByLabel(nodePartition,
          clusterResource);
      incQueueUsageRatio(nodePartition, user.updateUsageRatio(
          resourceCalculator, resourceByLabel, nodePartition));

    } finally {
      this.writeLock.unlock();
    }
  }

  private void updateResourceUsagePerUser(AbstractCSUser user, Resource resource,
      String nodePartition, boolean isAllocate) {
    ResourceUsage totalResourceUsageForUsers = getTotalResourceUsagePerUser(
        user.getUserName());

    if (isAllocate) {
      user.userResourceUsage.incUsed(nodePartition, resource);
      totalResourceUsageForUsers.incUsed(nodePartition, resource);
    } else {
      user.userResourceUsage.decUsed(nodePartition, resource);
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

  @Override
  public void queueConfigUpdated() {
    this.writeLock.lock();
    try {
      activeUsersTimesWeights = sumActiveUsersTimesWeights();
      allUsersTimesWeights = sumAllUsersTimesWeights();
      userLimitNeedsRecompute();
    } finally {
      this.writeLock.unlock();
    }
  }

  @VisibleForTesting
  private void setUsageRatio(String label, float usage) {
    qUsageRatios.usageRatios.put(label, usage);
  }
}
