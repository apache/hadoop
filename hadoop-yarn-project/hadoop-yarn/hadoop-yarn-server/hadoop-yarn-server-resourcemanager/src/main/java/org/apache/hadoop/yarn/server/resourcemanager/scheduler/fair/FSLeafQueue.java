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
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;

@Private
@Unstable
public class FSLeafQueue extends FSQueue {
  private static final Log LOG = LogFactory.getLog(
      FSLeafQueue.class.getName());
    
  private final List<AppSchedulable> runnableAppScheds = // apps that are runnable
      new ArrayList<AppSchedulable>();
  private final List<AppSchedulable> nonRunnableAppScheds =
      new ArrayList<AppSchedulable>();
  
  private Resource demand = Resources.createResource(0);
  
  // Variables used for preemption
  private long lastTimeAtMinShare;
  private long lastTimeAtHalfFairShare;
  
  private final ActiveUsersManager activeUsersManager;
  
  public FSLeafQueue(String name, FairScheduler scheduler,
      FSParentQueue parent) {
    super(name, scheduler, parent);
    this.lastTimeAtMinShare = scheduler.getClock().getTime();
    this.lastTimeAtHalfFairShare = scheduler.getClock().getTime();
    activeUsersManager = new ActiveUsersManager(getMetrics());
  }
  
  public void addApp(FSSchedulerApp app, boolean runnable) {
    AppSchedulable appSchedulable = new AppSchedulable(scheduler, app, this);
    app.setAppSchedulable(appSchedulable);
    if (runnable) {
      runnableAppScheds.add(appSchedulable);
    } else {
      nonRunnableAppScheds.add(appSchedulable);
    }
  }
  
  // for testing
  void addAppSchedulable(AppSchedulable appSched) {
    runnableAppScheds.add(appSched);
  }
  
  /**
   * Removes the given app from this queue.
   * @return whether or not the app was runnable
   */
  public boolean removeApp(FSSchedulerApp app) {
    if (runnableAppScheds.remove(app.getAppSchedulable())) {
      return true;
    } else if (nonRunnableAppScheds.remove(app.getAppSchedulable())) {
      return false;
    } else {
      throw new IllegalStateException("Given app to remove " + app +
          " does not exist in queue " + this);
    }
  }
  
  public Collection<AppSchedulable> getRunnableAppSchedulables() {
    return runnableAppScheds;
  }
  
  public List<AppSchedulable> getNonRunnableAppSchedulables() {
    return nonRunnableAppScheds;
  }
  
  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    for (AppSchedulable appSched : runnableAppScheds) {
      apps.add(appSched.getApp().getApplicationAttemptId());
    }
    for (AppSchedulable appSched : nonRunnableAppScheds) {
      apps.add(appSched.getApp().getApplicationAttemptId());
    }
  }

  @Override
  public void setPolicy(SchedulingPolicy policy)
      throws AllocationConfigurationException {
    if (!SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_LEAF)) {
      throwPolicyDoesnotApplyException(policy);
    }
    super.policy = policy;
  }
  
  @Override
  public void recomputeShares() {
    policy.computeShares(getRunnableAppSchedulables(), getFairShare());
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  @Override
  public Resource getResourceUsage() {
    Resource usage = Resources.createResource(0);
    for (AppSchedulable app : runnableAppScheds) {
      Resources.addTo(usage, app.getResourceUsage());
    }
    for (AppSchedulable app : nonRunnableAppScheds) {
      Resources.addTo(usage, app.getResourceUsage());
    }
    return usage;
  }

  @Override
  public void updateDemand() {
    // Compute demand by iterating through apps in the queue
    // Limit demand to maxResources
    Resource maxRes = scheduler.getAllocationConfiguration()
        .getMaxResources(getName());
    demand = Resources.createResource(0);
    for (AppSchedulable sched : runnableAppScheds) {
      if (Resources.equals(demand, maxRes)) {
        break;
      }
      updateDemandForApp(sched, maxRes);
    }
    for (AppSchedulable sched : nonRunnableAppScheds) {
      if (Resources.equals(demand, maxRes)) {
        break;
      }
      updateDemandForApp(sched, maxRes);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("The updated demand for " + getName() + " is " + demand
          + "; the max is " + maxRes);
    }
  }
  
  private void updateDemandForApp(AppSchedulable sched, Resource maxRes) {
    sched.updateDemand();
    Resource toAdd = sched.getDemand();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Counting resource from " + sched.getName() + " " + toAdd
          + "; Total resource consumption for " + getName() + " now "
          + demand);
    }
    demand = Resources.add(demand, toAdd);
    demand = Resources.componentwiseMin(demand, maxRes);
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    Resource assigned = Resources.none();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node " + node.getNodeName() + " offered to queue: " + getName());
    }

    if (!assignContainerPreCheck(node)) {
      return assigned;
    }

    Comparator<Schedulable> comparator = policy.getComparator();
    Collections.sort(runnableAppScheds, comparator);
    for (AppSchedulable sched : runnableAppScheds) {
      if (SchedulerAppUtils.isBlacklisted(sched.getApp(), node, LOG)) {
        continue;
      }

      assigned = sched.assignContainer(node);
      if (!assigned.equals(Resources.none())) {
        break;
      }
    }
    return assigned;
  }

  @Override
  public List<FSQueue> getChildQueues() {
    return new ArrayList<FSQueue>(1);
  }
  
  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user) {
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
  
  public long getLastTimeAtMinShare() {
    return lastTimeAtMinShare;
  }

  public void setLastTimeAtMinShare(long lastTimeAtMinShare) {
    this.lastTimeAtMinShare = lastTimeAtMinShare;
  }

  public long getLastTimeAtHalfFairShare() {
    return lastTimeAtHalfFairShare;
  }

  public void setLastTimeAtHalfFairShare(long lastTimeAtHalfFairShare) {
    this.lastTimeAtHalfFairShare = lastTimeAtHalfFairShare;
  }

  @Override
  public int getNumRunnableApps() {
    return runnableAppScheds.size();
  }
  
  @Override
  public ActiveUsersManager getActiveUsersManager() {
    return activeUsersManager;
  }
}
