package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.usermanagement;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a weakly consistent view of user resource limits but high concurrent performance compared to UsersManager.
 * It provides user resource limits which are eventually consistent wrt containers
 * (allocation or release) / users (active or inactive)
 * These limits affect scheduling and preemption but at massive scale small variances in individual decisions
 * for a container are fine as long as the scheduler progresses in an acceptable way
 * (i.e - users are unconcerned about these variances)
 *
 * Internally
 * 1. Uses thread safe data structures to ensure data isn't corrupted & to ensure eventual consistency
 * 2. Also uses fine grained locks for key mutations like adding / removing users or marking a user active / inactive
 * 3. Container allocation, app submissions, getting user limits are lock free
 * 4. Background thread periodically (1 sec) computes & caches user limits
 *
 */
class ConcurrentUsersManager extends AbstractCSUsersManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConcurrentUsersManager.class);

  private static final Set<SchedulingMode> schedulingModes = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY, SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY))
  );

  // TODO - share threads in a global pool and enable a thread to compute user limits of multiple queues
  // If scheduler has too many queues, its not ideal to start a thread per queue
  private UserLimitComputationThread _userLimitComputationThread;

  // Pre-computed list of user-limits. nested hierarchy of node labels & scheduling modes
  private final Map<String, Map<SchedulingMode, Resource>> preComputedActiveUserLimit =
      new ConcurrentHashMap<>();
  private final Map<String, Map<SchedulingMode, Resource>> preComputedAllUserLimit =
      new ConcurrentHashMap<>();

  private static class ConcurrentUser extends AbstractCSUser {

    // lock is used to enable thread safe access to add / remove user & to mark a user active / inactive
    final ReentrantReadWriteLock lock;
    final AtomicBoolean isActive;
    final Set<ApplicationId> activeApps;

    private ConcurrentUser(String userName) {
      super(userName);
      lock = new ReentrantReadWriteLock();
      isActive = new AtomicBoolean(false);
      activeApps = ConcurrentHashMap.newKeySet();
    }
  }

  private static class UserLimitComputationThread extends Thread {
    private final ConcurrentUsersManager _usersManager;

    private UserLimitComputationThread(ConcurrentUsersManager usersManager) {
      this._usersManager = usersManager;
      setName("UserLimitComputationThread");
      setDaemon(true);
    }

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          this._usersManager.computeUserLimits();
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // keep interrupt signal
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          // Continue in case of exceptions
          LOG.error("Exception while computing user limits", e);
        }
      }
      LOG.warn("UserLimitComputationThread[" + getName() + "] exited!");
    }
  }

  ConcurrentUsersManager(QueueMetrics metrics, AbstractLeafQueue lQueue, RMNodeLabelsManager labelManager,
      ResourceCalculator resourceCalculator) {
    super(metrics, lQueue, labelManager, resourceCalculator);
  }

  @Override
  public void queueConfigUpdated() {
    // synchronize to ensure concurrent calls don't result in multiple user limit computation threads
    synchronized (this) {
      if (_userLimitComputationThread == null) {
        // this is required to ensure user limit cache is populated when the queue is created & users manager is instantiated (on 1st call to queueConfigUpdated)
        // this can't be called in constructor because node labels aren't available in queue object yet
        computeUserLimits();
        _userLimitComputationThread = new UserLimitComputationThread(this);
        _userLimitComputationThread.start();
      }
    }
  }

  // Use user level locks to avoid race conditions when user is concurrently getting added by multiple threads
  // or removed by another thread in removeApplication()
  // We use the user read lock when adding a user to users manager and a write lock to remove a user
  // This enables concurrent submission & removal of applications except when a new user is getting added or user is getting removed
  @Override
  public void submitApplication(String userName) {
    ConcurrentUser user = (ConcurrentUser) getUser(userName);
    if (user == null) {
      user = new ConcurrentUser(userName);
      // Taking a read lock ensures that user won't be removed from users manager after its inserted into users & before user.submitApplication() is called
      user.lock.readLock().lock();
      try {
        // Its possible that user has been added by a concurrent call
        // In such a case, recursively call submitApplication() again
        AbstractCSUser existingUser = this.users.putIfAbsent(userName, user);
        if (existingUser != null) {
          submitApplication(userName);
          // explicitly return for code clarity that nothing should execute
          return;
        } else {
          user.submitApplication();
        }
      } finally {
        user.lock.readLock().unlock();
      }
    } else {
      user.lock.readLock().lock();
      try {
        // Its possible that user has been removed / replaced before obtaining the lock
        // In such a case, recursively call submitApplication() again
        ConcurrentUser existingUser = (ConcurrentUser) getUser(userName);
        if (existingUser == null || !existingUser.equals(user)) {
          submitApplication(userName);
          // explicitly return for code clarity that nothing should execute
          return;
        } else {
          user.submitApplication();
        }
      } finally {
        user.lock.readLock().unlock();
      }
    }
  }

  // Use user level locks to avoid race conditions when user is concurrently getting removed by multiple threads
  // or added by another thread in submitApplication()
  // We use the user read lock when adding a user to users manager and a write lock to remove a user
  // This enables concurrent submission & removal of applications except when a new user is getting added or user is getting removed
  @Override
  public void removeApplication(String userName, boolean wasApplicationActive) {
    ConcurrentUser user = (ConcurrentUser) getUser(userName);
    if (user == null) {
      LOG.error("Remove application called without user " + userName + " present");
    } else {
      user.finishApplication(wasApplicationActive);
      if (user.getTotalApplications() == 0) {
        user.lock.writeLock().lock();
        try {
          // Verify no app got submitted concurrently
          if (user.getTotalApplications() == 0) {
            users.remove(userName);
          }
        } finally {
          user.lock.writeLock().unlock();
        }
      }
    }
  }

  /**
   * Returns cached resource limit for the user
   */
  @Override
  public Resource getComputedResourceLimitForActiveUsers(String userName, Resource clusterResource,
      String nodePartition, SchedulingMode schedulingMode) {
    Map<SchedulingMode, Resource> userLimitPerSchedulingMode = preComputedActiveUserLimit.get(nodePartition);
    if (userLimitPerSchedulingMode == null) {
      LOG.error("Active User limit not computed for node partition " + nodePartition);
      return null;
    }
    Resource userLimitResource = userLimitPerSchedulingMode.get(schedulingMode);
    if (userLimitResource == null) {
      LOG.error("Active User limit not computed for node partition " + nodePartition + " and scheduling mode " + schedulingMode);
      return null;
    }

    AbstractCSUser user = getUser(userName);
    Resource userSpecificUserLimit = getUserSpecificUserLimit(user, userLimitResource);

    if (user != null) {
      user.setUserResourceLimit(userSpecificUserLimit);
    }
    return userSpecificUserLimit;
  }

  /**
   * Returns cached resource limit for the user
   */
  @Override
  public Resource getComputedResourceLimitForAllUsers(String userName, Resource clusterResource, String nodePartition,
      SchedulingMode schedulingMode) {
    Map<SchedulingMode, Resource> userLimitPerSchedulingMode = preComputedAllUserLimit.get(nodePartition);
    if (userLimitPerSchedulingMode == null) {
      LOG.error("All User limit not computed for node partition " + nodePartition);
      return null;
    }
    Resource userLimitResource = userLimitPerSchedulingMode.get(schedulingMode);
    if (userLimitResource == null) {
      LOG.error("All User limit not computed for node partition " + nodePartition + " and scheduling mode " + schedulingMode);
      return null;
    }

    AbstractCSUser user = getUser(userName);
    return getUserSpecificUserLimit(user, userLimitResource);
  }

  @Override
  public void updateUserResourceUsage(String userName, Resource resource, Resource clusterResource,
      String nodePartition, boolean isAllocate) {
    AbstractCSUser user = getUser(userName);
    if (isAllocate) {
      user.userResourceUsage.incUsed(nodePartition, resource);
    } else {
      user.userResourceUsage.decUsed(nodePartition, resource);
    }
  }

  @Override
  public void updateUsageRatio(String partition, Resource clusterResource) {
    // no-op
  }

  @Override
  public void userLimitNeedsRecompute() {
    // no-op
  }

  @Override
  boolean isActive(AbstractCSUser user) {
    return (((ConcurrentUser) user).isActive).get();
  }

  @Override
  public void activateApplication(String user, ApplicationId applicationId) {
    ConcurrentUser userDesc = (ConcurrentUser) getUser(user);
    if (userDesc == null || userDesc.getActiveApplications() <= 0) {
      LOG.error("User " + user + " doesn't exist or hasn't submitted an application so appid " + applicationId + " can't be activated");
      return;
    }

    if (userDesc.activeApps.add(applicationId)) {
      metrics.activateApp(user);
    } else {
      LOG.warn("App id " + applicationId + " being activated multiple times");
      return;
    }

    // Read lock is used to ensure concurrent apps can proceed in a non blocking operation
    // It is blocked only when user is getting deactivated concurrently
    userDesc.lock.readLock().lock();
    try {
      if (userDesc.isActive.compareAndSet(false, true)) {
        activeUsers.incrementAndGet();
        metrics.incrActiveUsers();
      }
    } finally {
      userDesc.lock.readLock().unlock();
    }
  }

  @Override
  // TODO - validate client side ordering for activateApplication,deactivateApplication & user.finishApplication, user.activateApplication
  // TODO - validate expectation that clients will follow order submitApplication (once per app) -> multiple <activate, deactivate> calls
  // followed by removeApplication(once per app)
  public void deactivateApplication(String user, ApplicationId applicationId) {
    ConcurrentUser userDesc = (ConcurrentUser) getUser(user);
    if (userDesc == null) {
      LOG.error("User " + user + " doesn't exist so appid " + applicationId + " can't be deactivated");
      return;
    }

    if (userDesc.activeApps.remove(applicationId)) {
      metrics.deactivateApp(user);
    } else {
      LOG.warn("App id " + applicationId + " being deactivated multiple times or wasn't activated");
      return;
    }

    if (userDesc.activeApps.isEmpty()) {
      userDesc.lock.writeLock().lock();
      try {
        // This check is required to handle cases where apps were activated before obtaining the lock
        if (userDesc.activeApps.isEmpty()) {
          // This check is required to handle concurrent deactivateApplication calls
          if (userDesc.isActive.compareAndSet(true, false)) {
            activeUsers.decrementAndGet();
            metrics.decrActiveUsers();
          }
        }
      } finally {
        userDesc.lock.writeLock().unlock();
      }
    }
  }

  // It doesn't have atomic visibility for all users -
  // i.e User resources can get updated concurrently along with this method or users can be added / removed or marked active / inactive
  @Override
  Resource getConsumedResources(String label, boolean forActiveUsersOnly) {
    Resource consumed = Resource.newInstance(0, 0);
    for (AbstractCSUser user : getUsers().values()) {
      if (forActiveUsersOnly) {
        if (!isActive(user)) {
          continue;
        }
      }
      Resources.addTo(consumed, user.getUsed(label));
    }
    return consumed;
  }

  // It doesn't have atomic visibility for all users -
  // i.e User weights can get updated concurrently along with this method or users can be added / removed or marked active / inactive
  @Override
  float getTotalUserWeight(boolean forActiveUsersOnly) {
    float totalWeight = 0.0f;
    for (AbstractCSUser user : getUsers().values()) {
      if (forActiveUsersOnly) {
        if (!isActive(user)) {
          continue;
        }
      }
      totalWeight += this.getUserWeight(user.getUserName());
    }
    return totalWeight;
  }

  private void computeUserLimits() {
    Resource clusterResource = this.lQueue.getClusterResource();

    // or should we be using union of (queue.getQueueCapacities().getExistingNodeLabels(), queue.queueResourceUsage.getExistingNodeLabels())
    // similar to queue.recalculateQueueUsageRatio() ?
    for (String nodeLabel : this.lQueue.getAccessibleNodeLabels()) {
      if (!this.preComputedActiveUserLimit.containsKey(nodeLabel)) {
        this.preComputedActiveUserLimit.put(nodeLabel, new ConcurrentHashMap<>());
      }
      if (!this.preComputedAllUserLimit.containsKey(nodeLabel)) {
        this.preComputedAllUserLimit.put(nodeLabel, new ConcurrentHashMap<>());
      }
      for (SchedulingMode schedulingMode : schedulingModes) {
        Resource activeUserLimit = computeUserLimit(clusterResource, nodeLabel, schedulingMode, true);
        this.preComputedActiveUserLimit.get(nodeLabel).put(schedulingMode, activeUserLimit);

        Resource allUserLimit = computeUserLimit(clusterResource, nodeLabel, schedulingMode, false);
        this.preComputedAllUserLimit.get(nodeLabel).put(schedulingMode, allUserLimit);
      }
    }

    computeNumActiveUsersWithOnlyPendingApps();
  }

}
