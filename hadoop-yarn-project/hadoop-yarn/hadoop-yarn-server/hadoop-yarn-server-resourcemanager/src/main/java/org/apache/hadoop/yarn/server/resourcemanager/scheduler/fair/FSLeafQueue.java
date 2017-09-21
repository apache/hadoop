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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.util.resource.Resources.none;

@Private
@Unstable
public class FSLeafQueue extends FSQueue {
  private static final Log LOG = LogFactory.getLog(FSLeafQueue.class.getName());
  private static final List<FSQueue> EMPTY_LIST = Collections.emptyList();

  private FSContext context;

  // apps that are runnable
  private final List<FSAppAttempt> runnableApps = new ArrayList<>();
  private final List<FSAppAttempt> nonRunnableApps = new ArrayList<>();
  // get a lock with fair distribution for app list updates
  private final ReadWriteLock rwl = new ReentrantReadWriteLock(true);
  private final Lock readLock = rwl.readLock();
  private final Lock writeLock = rwl.writeLock();
  
  private Resource demand = Resources.createResource(0);
  
  // Variables used for preemption
  private long lastTimeAtMinShare;

  // Track the AM resource usage for this queue
  private Resource amResourceUsage;

  private final ActiveUsersManager activeUsersManager;

  public FSLeafQueue(String name, FairScheduler scheduler,
      FSParentQueue parent) {
    super(name, scheduler, parent);
    this.context = scheduler.getContext();
    this.lastTimeAtMinShare = scheduler.getClock().getTime();
    activeUsersManager = new ActiveUsersManager(getMetrics());
    amResourceUsage = Resource.newInstance(0, 0);
    getMetrics().setAMResourceUsage(amResourceUsage);
  }
  
  void addApp(FSAppAttempt app, boolean runnable) {
    writeLock.lock();
    try {
      if (runnable) {
        runnableApps.add(app);
      } else {
        nonRunnableApps.add(app);
      }
      incUsedResource(app.getResourceUsage());
    } finally {
      writeLock.unlock();
    }
  }
  
  /**
   * Removes the given app from this queue.
   * @return whether or not the app was runnable
   */
  boolean removeApp(FSAppAttempt app) {
    boolean runnable = false;

    // Remove app from runnable/nonRunnable list while holding the write lock
    writeLock.lock();
    try {
      runnable = runnableApps.remove(app);
      if (!runnable) {
        // removeNonRunnableApp acquires the write lock again, which is fine
        if (!removeNonRunnableApp(app)) {
          throw new IllegalStateException("Given app to remove " + app +
              " does not exist in queue " + this);
        }
      }
    } finally {
      writeLock.unlock();
    }

    // Update AM resource usage if needed. If isAMRunning is true, we're not
    // running an unmanaged AM.
    if (runnable && app.isAmRunning()) {
      Resources.subtractFrom(amResourceUsage, app.getAMResource());
      getMetrics().setAMResourceUsage(amResourceUsage);
    }

    decUsedResource(app.getResourceUsage());
    return runnable;
  }

  /**
   * Removes the given app if it is non-runnable and belongs to this queue
   * @return true if the app is removed, false otherwise
   */
  boolean removeNonRunnableApp(FSAppAttempt app) {
    writeLock.lock();
    try {
      return nonRunnableApps.remove(app);
    } finally {
      writeLock.unlock();
    }
  }

  boolean isRunnableApp(FSAppAttempt attempt) {
    readLock.lock();
    try {
      return runnableApps.contains(attempt);
    } finally {
      readLock.unlock();
    }
  }

  boolean isNonRunnableApp(FSAppAttempt attempt) {
    readLock.lock();
    try {
      return nonRunnableApps.contains(attempt);
    } finally {
      readLock.unlock();
    }
  }

  List<FSAppAttempt> getCopyOfNonRunnableAppSchedulables() {
    List<FSAppAttempt> appsToReturn = new ArrayList<>();
    readLock.lock();
    try {
      appsToReturn.addAll(nonRunnableApps);
    } finally {
      readLock.unlock();
    }
    return appsToReturn;
  }

  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    readLock.lock();
    try {
      for (FSAppAttempt appSched : runnableApps) {
        apps.add(appSched.getApplicationAttemptId());
      }
      for (FSAppAttempt appSched : nonRunnableApps) {
        apps.add(appSched.getApplicationAttemptId());
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  void updateInternal() {
    readLock.lock();
    try {
      policy.computeShares(runnableApps, getFairShare());
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Compute the extent of fairshare starvation for a set of apps.
   *
   * @param appsWithDemand apps to compute fairshare starvation for
   * @return aggregate fairshare starvation for all apps
   */
  private Resource updateStarvedAppsFairshare(
      TreeSet<FSAppAttempt> appsWithDemand) {
    Resource fairShareStarvation = Resources.clone(none());
    // Fetch apps with unmet demand sorted by fairshare starvation
    for (FSAppAttempt app : appsWithDemand) {
      Resource appStarvation = app.fairShareStarvation();
      if (!Resources.isNone(appStarvation))  {
        context.getStarvedApps().addStarvedApp(app);
        Resources.addTo(fairShareStarvation, appStarvation);
      } else {
        break;
      }
    }
    return fairShareStarvation;
  }

  /**
   * Distribute minshare starvation to a set of apps
   * @param appsWithDemand set of apps
   * @param minShareStarvation minshare starvation to distribute
   */
  private void updateStarvedAppsMinshare(
      final TreeSet<FSAppAttempt> appsWithDemand,
      final Resource minShareStarvation) {
    Resource pending = Resources.clone(minShareStarvation);

    // Keep adding apps to the starved list until the unmet demand goes over
    // the remaining minshare
    for (FSAppAttempt app : appsWithDemand) {
      if (!Resources.isNone(pending)) {
        Resource appMinShare = app.getPendingDemand();
        Resources.subtractFromNonNegative(
            appMinShare, app.getFairshareStarvation());

        if (Resources.greaterThan(policy.getResourceCalculator(),
            scheduler.getClusterResource(), appMinShare, pending)) {
          Resources.subtractFromNonNegative(appMinShare, pending);
          pending = none();
        } else {
          Resources.subtractFromNonNegative(pending, appMinShare);
        }
        app.setMinshareStarvation(appMinShare);
        context.getStarvedApps().addStarvedApp(app);
      } else {
        // Reset minshare starvation in case we had set it in a previous
        // iteration
        app.resetMinshareStarvation();
      }
    }
  }

  /**
   * Helper method to identify starved applications. This needs to be called
   * ONLY from {@link #updateInternal}, after the application shares
   * are updated.
   *
   * A queue can be starving due to fairshare or minshare.
   *
   * Minshare is defined only on the queue and not the applications.
   * Fairshare is defined for both the queue and the applications.
   *
   * If this queue is starved due to minshare, we need to identify the most
   * deserving apps if they themselves are not starved due to fairshare.
   *
   * If this queue is starving due to fairshare, there must be at least
   * one application that is starved. And, even if the queue is not
   * starved due to fairshare, there might still be starved applications.
   *
   * Caller does not need read/write lock on the leaf queue.
   */
  void updateStarvedApps() {
    // Fetch apps with pending demand
    TreeSet<FSAppAttempt> appsWithDemand = fetchAppsWithDemand(false);

    // Process apps with fairshare starvation
    Resource fairShareStarvation = updateStarvedAppsFairshare(appsWithDemand);

    // Compute extent of minshare starvation
    Resource minShareStarvation = minShareStarvation();

    // Compute minshare starvation that is not subsumed by fairshare starvation
    Resources.subtractFromNonNegative(minShareStarvation, fairShareStarvation);

    // Assign this minshare to apps with pending demand over fairshare
    updateStarvedAppsMinshare(appsWithDemand, minShareStarvation);
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  Resource getAmResourceUsage() {
    return amResourceUsage;
  }

  @Override
  public void updateDemand() {
    // Compute demand by iterating through apps in the queue
    // Limit demand to maxResources
    Resource tmpDemand = Resources.createResource(0);
    readLock.lock();
    try {
      for (FSAppAttempt sched : runnableApps) {
        sched.updateDemand();
        Resources.addTo(tmpDemand, sched.getDemand());
      }
      for (FSAppAttempt sched : nonRunnableApps) {
        sched.updateDemand();
        Resources.addTo(tmpDemand, sched.getDemand());
      }
    } finally {
      readLock.unlock();
    }
    // Cap demand to maxShare to limit allocation to maxShare
    demand = Resources.componentwiseMin(tmpDemand, getMaxShare());
    if (LOG.isDebugEnabled()) {
      LOG.debug("The updated demand for " + getName() + " is " + demand
          + "; the max is " + getMaxShare());
      LOG.debug("The updated fairshare for " + getName() + " is "
          + getFairShare());
    }
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    Resource assigned = none();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node " + node.getNodeName() + " offered to queue: " +
          getName() + " fairShare: " + getFairShare());
    }

    if (!assignContainerPreCheck(node)) {
      return assigned;
    }

    for (FSAppAttempt sched : fetchAppsWithDemand(true)) {
      if (SchedulerAppUtils.isPlaceBlacklisted(sched, node, LOG)) {
        continue;
      }
      assigned = sched.assignContainer(node);
      if (!assigned.equals(none())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigned container in queue:" + getName() + " " +
              "container:" + assigned);
        }
        break;
      }
    }
    return assigned;
  }

  /**
   * Fetch the subset of apps that have unmet demand. When used for
   * preemption-related code (as opposed to allocation), omits apps that
   * should not be checked for starvation.
   *
   * @param assignment whether the apps are for allocation containers, as
   *                   opposed to preemption calculations
   * @return Set of apps with unmet demand
   */
  private TreeSet<FSAppAttempt> fetchAppsWithDemand(boolean assignment) {
    TreeSet<FSAppAttempt> pendingForResourceApps =
        new TreeSet<>(policy.getComparator());
    readLock.lock();
    try {
      for (FSAppAttempt app : runnableApps) {
        if (!Resources.isNone(app.getPendingDemand()) &&
            (assignment || app.shouldCheckForStarvation())) {
          pendingForResourceApps.add(app);
        }
      }
    } finally {
      readLock.unlock();
    }
    return pendingForResourceApps;
  }

  @Override
  public List<FSQueue> getChildQueues() {
    return EMPTY_LIST;
  }
  
  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user) {
    QueueUserACLInfo userAclInfo =
      recordFactory.newRecordInstance(QueueUserACLInfo.class);
    List<QueueACL> operations = new ArrayList<>();
    for (QueueACL operation : QueueACL.values()) {
      if (hasAccess(operation, user)) {
        operations.add(operation);
      }
    }

    userAclInfo.setQueueName(getQueueName());
    userAclInfo.setUserAcls(operations);
    return Collections.singletonList(userAclInfo);
  }
  
  private void setLastTimeAtMinShare(long lastTimeAtMinShare) {
    this.lastTimeAtMinShare = lastTimeAtMinShare;
  }

  @Override
  public int getNumRunnableApps() {
    readLock.lock();
    try {
      return runnableApps.size();
    } finally {
      readLock.unlock();
    }
  }

  int getNumNonRunnableApps() {
    readLock.lock();
    try {
      return nonRunnableApps.size();
    } finally {
      readLock.unlock();
    }
  }

  public int getNumPendingApps() {
    int numPendingApps = 0;
    readLock.lock();
    try {
      for (FSAppAttempt attempt : runnableApps) {
        if (attempt.isPending()) {
          numPendingApps++;
        }
      }
      numPendingApps += nonRunnableApps.size();
    } finally {
      readLock.unlock();
    }
    return numPendingApps;
  }

  /**
   * TODO: Based on how frequently this is called, we might want to club
   * counting pending and active apps in the same method.
   */
  public int getNumActiveApps() {
    int numActiveApps = 0;
    readLock.lock();
    try {
      for (FSAppAttempt attempt : runnableApps) {
        if (!attempt.isPending()) {
          numActiveApps++;
        }
      }
    } finally {
      readLock.unlock();
    }
    return numActiveApps;
  }

  @Override
  public ActiveUsersManager getAbstractUsersManager() {
    return activeUsersManager;
  }

  /**
  * Compute the maximum resource AM can use. The value is the result of
  * multiplying FairShare and maxAMShare. If FairShare is zero, use
  * min(maxShare, available resource) instead to prevent zero value for
  * maximum AM resource since it forbids any job running in the queue.
  *
  * @return the maximum resource AM can use
  */
  private Resource computeMaxAMResource() {
    Resource maxResource = Resources.clone(getFairShare());
    if (maxResource.getMemorySize() == 0) {
      maxResource.setMemorySize(
          Math.min(scheduler.getRootQueueMetrics().getAvailableMB(),
                   getMaxShare().getMemorySize()));
    }

    if (maxResource.getVirtualCores() == 0) {
      maxResource.setVirtualCores(Math.min(
          scheduler.getRootQueueMetrics().getAvailableVirtualCores(),
          getMaxShare().getVirtualCores()));
    }

    // Round up to allow AM to run when there is only one vcore on the cluster
    return Resources.multiplyAndRoundUp(maxResource, maxAMShare);
  }

  /**
   * Check whether this queue can run the Application Master under the
   * maxAMShare limit.
   *
   * @param amResource resources required to run the AM
   * @return true if this queue can run
   */
  public boolean canRunAppAM(Resource amResource) {
    if (Math.abs(maxAMShare - -1.0f) < 0.0001) {
      return true;
    }

    Resource maxAMResource = computeMaxAMResource();
    getMetrics().setMaxAMShare(maxAMResource);
    Resource ifRunAMResource = Resources.add(amResourceUsage, amResource);
    return Resources.fitsIn(ifRunAMResource, maxAMResource);
  }

  void addAMResourceUsage(Resource amResource) {
    if (amResource != null) {
      Resources.addTo(amResourceUsage, amResource);
      getMetrics().setAMResourceUsage(amResourceUsage);
    }
  }

  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
    // TODO Auto-generated method stub
  }

  /**
   * Allows setting weight for a dynamically created queue.
   * Currently only used for reservation based queues.
   * @param weight queue weight
   */
  public void setWeights(float weight) {
    this.weights = new ResourceWeights(weight);
  }

  /**
   * Helper method to compute the amount of minshare starvation.
   *
   * @return the extent of minshare starvation
   */
  private Resource minShareStarvation() {
    // If demand < minshare, we should use demand to determine starvation
    Resource desiredShare = Resources.min(policy.getResourceCalculator(),
        scheduler.getClusterResource(), getMinShare(), getDemand());

    Resource starvation = Resources.subtract(desiredShare, getResourceUsage());
    boolean starved = !Resources.isNone(starvation);

    long now = scheduler.getClock().getTime();
    if (!starved) {
      // Record that the queue is not starved
      setLastTimeAtMinShare(now);
    }

    if (now - lastTimeAtMinShare < getMinSharePreemptionTimeout()) {
      // the queue is not starved for the preemption timeout
      starvation = Resources.clone(Resources.none());
    }

    return starvation;
  }

  /**
   * Helper method for tests to check if a queue is starved for minShare.
   * @return whether starved for minshare
   */
  @VisibleForTesting
  private boolean isStarvedForMinShare() {
    return !Resources.isNone(minShareStarvation());
  }

  /**
   * Helper method for tests to check if a queue is starved for fairshare.
   * @return whether starved for fairshare
   */
  @VisibleForTesting
  private boolean isStarvedForFairShare() {
    for (FSAppAttempt app : runnableApps) {
      if (app.isStarvedForFairShare()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Helper method for tests to check if a queue is starved.
   * @return whether starved for either minshare or fairshare
   */
  @VisibleForTesting
  boolean isStarved() {
    return isStarvedForMinShare() || isStarvedForFairShare();
  }

  @Override
  protected void dumpStateInternal(StringBuilder sb) {
    sb.append("{Name: " + getName() +
        ", Weight: " + weights +
        ", Policy: " + policy.getName() +
        ", FairShare: " + getFairShare() +
        ", SteadyFairShare: " + getSteadyFairShare() +
        ", MaxShare: " + getMaxShare() +
        ", MinShare: " + minShare +
        ", ResourceUsage: " + getResourceUsage() +
        ", Demand: " + getDemand() +
        ", Runnable: " + getNumRunnableApps() +
        ", NumPendingApps: " + getNumPendingApps() +
        ", NonRunnable: " + getNumNonRunnableApps() +
        ", MaxAMShare: " + maxAMShare +
        ", MaxAMResource: " + computeMaxAMResource() +
        ", AMResourceUsage: " + getAmResourceUsage() +
        ", LastTimeAtMinShare: " + lastTimeAtMinShare +
        "}");
  }
}
