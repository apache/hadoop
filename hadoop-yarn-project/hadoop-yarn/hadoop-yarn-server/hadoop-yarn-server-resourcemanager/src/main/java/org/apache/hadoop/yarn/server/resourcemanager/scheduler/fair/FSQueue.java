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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

public abstract class FSQueue extends Schedulable implements Queue {
  private final String name;
  private final QueueManager queueMgr;
  private final FairScheduler scheduler;
  private final QueueMetrics metrics;
  
  protected final FSParentQueue parent;
  protected final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  
  public FSQueue(String name, QueueManager queueMgr, 
      FairScheduler scheduler, FSParentQueue parent) {
    this.name = name;
    this.queueMgr = queueMgr;
    this.scheduler = scheduler;
    this.metrics = QueueMetrics.forQueue(getName(), parent, true, scheduler.getConf());
    this.parent = parent;
  }
  
  public String getName() {
    return name;
  }
  
  @Override
  public String getQueueName() {
    return name;
  }
  
  @Override
  public double getWeight() {
    return queueMgr.getQueueWeight(getName());
  }
  
  @Override
  public Resource getMinShare() {
    return queueMgr.getMinResources(getName());
  }

  @Override
  public long getStartTime() {
    return 0;
  }

  @Override
  public Priority getPriority() {
    Priority p = recordFactory.newRecordInstance(Priority.class);
    p.setPriority(1);
    return p;
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
    
    ArrayList<QueueInfo> childQueueInfos = new ArrayList<QueueInfo>();
    if (includeChildQueues) {
      Collection<FSQueue> childQueues = getChildQueues();
      for (FSQueue child : childQueues) {
        childQueueInfos.add(child.getQueueInfo(recursive, recursive));
      }
    }
    queueInfo.setChildQueues(childQueueInfos);
    queueInfo.setQueueState(QueueState.RUNNING);
    return queueInfo;
  }
  
  @Override
  public Map<QueueACL, AccessControlList> getQueueAcls() {
    Map<QueueACL, AccessControlList> acls = queueMgr.getQueueAcls(getName());
    return new HashMap<QueueACL, AccessControlList>(acls);
  }
  
  @Override
  public QueueMetrics getMetrics() {
    return metrics;
  }
  
  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    // Check if the leaf-queue allows access
    if (queueMgr.getQueueAcls(getName()).get(acl).isUserAllowed(user)) {
      return true;
    }

    // Check if parent-queue allows access
    return parent != null && parent.hasAccess(acl, user);
  }
  
  /**
   * Recomputes the fair shares for all queues and applications
   * under this queue.
   */
  public abstract void recomputeFairShares();
  
  /**
   * Gets the children of this queue, if any.
   */
  public abstract Collection<FSQueue> getChildQueues();
}
