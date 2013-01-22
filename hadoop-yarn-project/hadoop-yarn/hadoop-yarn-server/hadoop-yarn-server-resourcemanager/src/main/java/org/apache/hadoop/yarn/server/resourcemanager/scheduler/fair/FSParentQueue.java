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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;

public class FSParentQueue extends FSQueue {
  private static final Log LOG = LogFactory.getLog(
      FSParentQueue.class.getName());


  private final List<FSQueue> childQueues = 
      new ArrayList<FSQueue>();
  private final QueueManager queueMgr;
  private Resource demand = Resources.createResource(0);
  
  public FSParentQueue(String name, QueueManager queueMgr, FairScheduler scheduler,
      FSParentQueue parent) {
    super(name, queueMgr, scheduler, parent);
    this.queueMgr = queueMgr;
  }
  
  public void addChildQueue(FSQueue child) {
    childQueues.add(child);
  }

  @Override
  public void recomputeFairShares() {
    SchedulingAlgorithms.computeFairShares(childQueues, getFairShare());
    for (FSQueue childQueue : childQueues) {
      childQueue.getMetrics().setAvailableResourcesToQueue(childQueue.getFairShare());
      childQueue.recomputeFairShares();
    }
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  @Override
  public Resource getResourceUsage() {
    Resource usage = Resources.createResource(0);
    for (FSQueue child : childQueues) {
      Resources.addTo(usage, child.getResourceUsage());
    }
    return usage;
  }

  @Override
  public void updateDemand() {
    // Compute demand by iterating through apps in the queue
    // Limit demand to maxResources
    Resource maxRes = queueMgr.getMaxResources(getName());
    demand = Resources.createResource(0);
    for (FSQueue childQueue : childQueues) {
      childQueue.updateDemand();
      Resource toAdd = childQueue.getDemand();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Counting resource from " + childQueue.getName() + " " + 
            toAdd + "; Total resource consumption for " + getName() +
            " now " + demand);
      }
      demand = Resources.add(demand, toAdd);
      if (Resources.greaterThanOrEqual(demand, maxRes)) {
        demand = maxRes;
        break;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("The updated demand for " + getName() + " is " + demand +
          "; the max is " + maxRes);
    }    
  }
  
  private synchronized QueueUserACLInfo getUserAclInfo(
      UserGroupInformation user) {
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
    return userAclInfo;
  }
  
  @Override
  public synchronized List<QueueUserACLInfo> getQueueUserAclInfo(
      UserGroupInformation user) {
    List<QueueUserACLInfo> userAcls = new ArrayList<QueueUserACLInfo>();
    
    // Add queue acls
    userAcls.add(getUserAclInfo(user));
    
    // Add children queue acls
    for (FSQueue child : childQueues) {
      userAcls.addAll(child.getQueueUserAclInfo(user));
    }
 
    return userAcls;
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node, boolean reserved) {
    throw new IllegalStateException(
        "Parent queue should not be assigned container");
  }

  @Override
  public Collection<FSQueue> getChildQueues() {
    return childQueues;
  }
}
