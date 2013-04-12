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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;


import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueueManager;

public class FairSchedulerQueueInfo {  
  private int fairShare;
  private int minShare;
  private int maxShare;
  private int clusterMaxMem;
  
  private int maxApps;
  
  private float fractionUsed;
  private float fractionFairShare;
  private float fractionMinShare;
  
  private Resource minResources;
  private Resource maxResources;
  private Resource usedResources;
  
  private String queueName;
  
  private Collection<FairSchedulerQueueInfo> childInfos;
  
  public FairSchedulerQueueInfo(FSQueue queue, FairScheduler scheduler) {
    QueueManager manager = scheduler.getQueueManager();
    
    queueName = queue.getName();
        
    Resource clusterMax = scheduler.getClusterCapacity();
    clusterMaxMem = clusterMax.getMemory();
    
    usedResources = queue.getResourceUsage();
    fractionUsed = (float)usedResources.getMemory() / clusterMaxMem;
    
    fairShare = queue.getFairShare().getMemory();
    minResources = queue.getMinShare();
    minShare = minResources.getMemory();
    maxResources = scheduler.getQueueManager().getMaxResources(queueName);
    if (maxResources.getMemory() > clusterMaxMem) {
      maxResources = Resources.createResource(clusterMaxMem);
    }
    maxShare = maxResources.getMemory();
    
    fractionFairShare = (float)fairShare / clusterMaxMem;
    fractionMinShare = (float)minShare / clusterMaxMem;
    
    maxApps = manager.getQueueMaxApps(queueName);
    
    Collection<FSQueue> childQueues = queue.getChildQueues();
    childInfos = new ArrayList<FairSchedulerQueueInfo>();
    for (FSQueue child : childQueues) {
      if (child instanceof FSLeafQueue) {
        childInfos.add(new FairSchedulerLeafQueueInfo((FSLeafQueue)child, scheduler));
      } else {
        childInfos.add(new FairSchedulerQueueInfo(child, scheduler));
      }
    }
  }
  
  /**
   * Returns the fair share as a fraction of the entire cluster capacity.
   */
  public float getFairShareFraction() {
    return fractionFairShare;
  }
  
  /**
   * Returns the fair share of this queue in megabytes.
   */
  public int getFairShare() {
    return fairShare;
  }
    
  public Resource getMinResources() {
    return minResources;
  }
  
  public Resource getMaxResources() {
    return maxResources;
  }
  
  public int getMaxApplications() {
    return maxApps;
  }
  
  public String getQueueName() {
    return queueName;
  }
  
  public Resource getUsedResources() {
    return usedResources;
  }
  
  /**
   * Returns the queue's min share in as a fraction of the entire
   * cluster capacity.
   */
  public float getMinShareFraction() {
    return fractionMinShare;
  }
  
  /**
   * Returns the memory used by this queue as a fraction of the entire 
   * cluster capacity.
   */
  public float getUsedFraction() {
    return fractionUsed;
  }
  
  /**
   * Returns the capacity of this queue as a fraction of the entire cluster 
   * capacity.
   */
  public float getMaxResourcesFraction() {
    return (float)maxShare / clusterMaxMem;
  }
  
  public Collection<FairSchedulerQueueInfo> getChildQueues() {
    return childInfos;
  }
}
