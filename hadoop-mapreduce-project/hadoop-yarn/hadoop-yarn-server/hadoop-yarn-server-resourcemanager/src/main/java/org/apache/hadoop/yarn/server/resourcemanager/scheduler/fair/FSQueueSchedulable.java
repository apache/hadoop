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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

@Private
@Unstable
public class FSQueueSchedulable extends Schedulable implements Queue {
  public static final Log LOG = LogFactory.getLog(
      FSQueueSchedulable.class.getName());

  private FairScheduler scheduler;
  private FSQueue queue;
  private QueueManager queueMgr;
  private List<AppSchedulable> appScheds = new LinkedList<AppSchedulable>();
  private Resource demand = Resources.createResource(0);
  private QueueMetrics metrics;
  private RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  // Variables used for preemption
  long lastTimeAtMinShare;
  long lastTimeAtHalfFairShare;

  public FSQueueSchedulable(FairScheduler scheduler, FSQueue queue) {
    this.scheduler = scheduler;
    this.queue = queue;
    this.queueMgr = scheduler.getQueueManager();
    this.metrics = QueueMetrics.forQueue(this.getName(), null, true, scheduler.getConf());
    this.lastTimeAtMinShare = scheduler.getClock().getTime();
    this.lastTimeAtHalfFairShare = scheduler.getClock().getTime();
  }

  public void addApp(AppSchedulable app) {
    appScheds.add(app);
  }

  public void removeApp(FSSchedulerApp app) {
    for (Iterator<AppSchedulable> it = appScheds.iterator(); it.hasNext();) {
      AppSchedulable appSched = it.next();
      if (appSched.getApp() == app) {
        it.remove();
        break;
      }
    }
  }

  /**
   * Update demand by asking apps in the queue to update
   */
  @Override
  public void updateDemand() {
    demand = Resources.createResource(0);
    for (AppSchedulable sched: appScheds) {
      sched.updateDemand();
      Resource toAdd = sched.getDemand();
      LOG.debug("Counting resource from " + sched.getName() + " " + toAdd.toString());
      LOG.debug("Total resource consumption for " + this.getName() + " now " + demand.toString());
      demand = Resources.add(demand, toAdd);

    }
    // if demand exceeds the cap for this queue, limit to the max
    Resource maxRes = queueMgr.getMaxResources(queue.getName());
    if(Resources.greaterThan(demand, maxRes)) {
      demand = maxRes;
    }
  }

  /**
   * Distribute the queue's fair share among its jobs
   */
  @Override
  public void redistributeShare() {
    if (queue.getSchedulingMode() == SchedulingMode.FAIR) {
      SchedulingAlgorithms.computeFairShares(appScheds, getFairShare());
    } else {
      for (AppSchedulable sched: appScheds) {
        sched.setFairShare(Resources.createResource(0));
      }
    }
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  @Override
  public Resource getMinShare() {
    return queueMgr.getMinResources(queue.getName());
  }

  @Override
  public double getWeight() {
    return queueMgr.getQueueWeight(queue.getName());
  }

  @Override
  public long getStartTime() {
    return 0;
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node, boolean reserved) {
    LOG.debug("Node offered to queue: " + this.getName() + " reserved: " + reserved);
    // If this queue is over its limit, reject
    if (Resources.greaterThan(this.getResourceUsage(),
        queueMgr.getMaxResources(queue.getName()))) {
      return Resources.none();
    }

    // If this node already has reserved resources for an app, first try to
    // finish allocating resources for that app.
    if (reserved) {
      for (AppSchedulable sched : appScheds) {
        if (sched.getApp().getApplicationAttemptId() ==
            node.getReservedContainer().getApplicationAttemptId()) {
          return sched.assignContainer(node, reserved);
        }
      }
      return Resources.none(); // We should never get here
    }

    // Otherwise, chose app to schedule based on given policy (fair vs fifo).
    else {
      SchedulingMode mode = queue.getSchedulingMode();

      Comparator<Schedulable> comparator;
      if (mode == SchedulingMode.FIFO) {
        comparator = new SchedulingAlgorithms.FifoComparator();
      } else if (mode == SchedulingMode.FAIR) {
        comparator = new SchedulingAlgorithms.FairShareComparator();
      } else {
        throw new RuntimeException("Unsupported queue scheduling mode " + mode);
      }

      Collections.sort(appScheds, comparator);
      for (AppSchedulable sched: appScheds) {
        return sched.assignContainer(node, reserved);
      }

      return Resources.none();
    }

  }

  @Override
  public String getName() {
    return queue.getName();
  }

  FSQueue getQueue() {
    return queue;
  }

  public Collection<AppSchedulable> getAppSchedulables() {
    return appScheds;
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
  public QueueMetrics getMetrics() {
    return metrics;
  }

  @Override
  public Resource getResourceUsage() {
    Resource usage = Resources.createResource(0);
    for (AppSchedulable app : appScheds) {
      Resources.addTo(usage, app.getResourceUsage());
    }
    return usage;
  }

  @Override
  public Priority getPriority() {
    Priority p = recordFactory.newRecordInstance(Priority.class);
    p.setPriority(1);
    return p;
  }

  @Override
  public Map<QueueACL, AccessControlList> getQueueAcls() {
    Map<QueueACL, AccessControlList> acls = this.queueMgr.getQueueAcls(this.getName());
    return new HashMap<QueueACL, AccessControlList>(acls);
  }

  @Override
  public QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive) {
    QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    queueInfo.setQueueName(getQueueName());
    // TODO: we might change these queue metrics around a little bit
    // to match the semantics of the fair scheduler.
    queueInfo.setCapacity((float) getFairShare().getMemory() /
        scheduler.getClusterCapacity().getMemory());
    queueInfo.setCapacity((float) getResourceUsage().getMemory() /
        scheduler.getClusterCapacity().getMemory());

    queueInfo.setChildQueues(new ArrayList<QueueInfo>());
    queueInfo.setQueueState(QueueState.RUNNING);
    return queueInfo;
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user) {
    QueueUserACLInfo userAclInfo =
      recordFactory.newRecordInstance(QueueUserACLInfo.class);
    List<QueueACL> operations = new ArrayList<QueueACL>();
    for (QueueACL operation : QueueACL.values()) {
      Map<QueueACL, AccessControlList> acls = this.queueMgr.getQueueAcls(this.getName());
      if (acls.get(operation).isUserAllowed(user)) {
        operations.add(operation);
      }
    }

    userAclInfo.setQueueName(getQueueName());
    userAclInfo.setUserAcls(operations);
    return Collections.singletonList(userAclInfo);
  }

  @Override
  public String getQueueName() {
    return getName();
  }
}
