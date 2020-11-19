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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.resource.Resources;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlSeeAlso({FairSchedulerLeafQueueInfo.class})
public class FairSchedulerQueueInfo {  
  private int maxApps;
  
  @XmlTransient
  private float fractionMemUsed;
  @XmlTransient
  private float fractionMemSteadyFairShare;
  @XmlTransient
  private float fractionMemFairShare;
  @XmlTransient
  private float fractionMemMaxShare;
  
  private ResourceInfo minResources;
  private ResourceInfo maxResources;
  private ResourceInfo usedResources;
  private ResourceInfo amUsedResources;
  private ResourceInfo amMaxResources;
  private ResourceInfo demandResources;
  private ResourceInfo steadyFairResources;
  private ResourceInfo fairResources;
  private ResourceInfo clusterResources;
  private ResourceInfo reservedResources;
  private ResourceInfo maxContainerAllocation;

  private long pendingContainers;
  private long allocatedContainers;
  private long reservedContainers;

  private String queueName;
  private String schedulingPolicy;

  private boolean preemptable;

  private FairSchedulerQueueInfoList childQueues;

  public FairSchedulerQueueInfo() {
  }
  
  public FairSchedulerQueueInfo(FSQueue queue, FairScheduler scheduler) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    
    queueName = queue.getName();
    schedulingPolicy = queue.getPolicy().getName();
    
    clusterResources = new ResourceInfo(scheduler.getClusterResource());
    
    amUsedResources = new ResourceInfo(queue.getMetrics().getAMResourceUsage());
    amMaxResources = new ResourceInfo(queue.getMetrics().getMaxAMShare());
    usedResources = new ResourceInfo(queue.getResourceUsage());
    demandResources = new ResourceInfo(queue.getDemand());
    fractionMemUsed = (float)usedResources.getMemorySize() /
        clusterResources.getMemorySize();

    steadyFairResources = new ResourceInfo(queue.getSteadyFairShare());
    fairResources = new ResourceInfo(queue.getFairShare());
    minResources = new ResourceInfo(queue.getMinShare());
    maxResources = new ResourceInfo(
        Resources.componentwiseMin(queue.getMaxShare(),
            scheduler.getClusterResource()));
    maxContainerAllocation =
        new ResourceInfo(scheduler.getMaximumResourceCapability(queueName));
    reservedResources = new ResourceInfo(queue.getReservedResource());

    fractionMemSteadyFairShare =
        (float)steadyFairResources.getMemorySize() / clusterResources.getMemorySize();
    fractionMemFairShare = (float) fairResources.getMemorySize()
        / clusterResources.getMemorySize();
    fractionMemMaxShare = (float)maxResources.getMemorySize() / clusterResources.getMemorySize();
    
    maxApps = queue.getMaxRunningApps();

    allocatedContainers = queue.getMetrics().getAllocatedContainers();
    reservedContainers = queue.getMetrics().getReservedContainers();
    pendingContainers = queue.getMetrics().getPendingContainers();

    if (allocConf.isReservable(queueName) &&
        !allocConf.getShowReservationAsQueues(queueName)) {
      return;
    }

    preemptable = queue.isPreemptable();
    childQueues = getChildQueues(queue, scheduler);
  }

  public long getAllocatedContainers() {
    return allocatedContainers;
  }

  public long getPendingContainers() { return pendingContainers; }

  public long getReservedContainers() {
    return reservedContainers;
  }

  protected FairSchedulerQueueInfoList getChildQueues(FSQueue queue,
                                                      FairScheduler scheduler) {
    // Return null to omit 'childQueues' field from the return value of
    // REST API if it is empty. We omit the field to keep the consistency
    // with CapacitySchedulerQueueInfo, which omits 'queues' field if empty.
    Collection<FSQueue> children = queue.getChildQueues();
    if (children.isEmpty()) {
      return null;
    }
    FairSchedulerQueueInfoList list = new FairSchedulerQueueInfoList();
    for (FSQueue child : children) {
      if (child instanceof FSLeafQueue) {
        list.addToQueueInfoList(
            new FairSchedulerLeafQueueInfo((FSLeafQueue) child, scheduler));
      } else {
        list.addToQueueInfoList(
            new FairSchedulerQueueInfo(child, scheduler));
      }
    }
    return list;
  }
  
  /**
   * Returns the steady fair share as a fraction of the entire cluster capacity.
   */
  public float getSteadyFairShareMemoryFraction() {
    return fractionMemSteadyFairShare;
  }

  /**
   * Returns the fair share as a fraction of the entire cluster capacity.
   */
  public float getFairShareMemoryFraction() {
    return fractionMemFairShare;
  }

  /**
   * Returns the steady fair share of this queue in megabytes.
   */
  public ResourceInfo getSteadyFairShare() {
    return steadyFairResources;
  }

  /**
   * Returns the fair share of this queue in megabytes
   */
  public ResourceInfo getFairShare() {
    return fairResources;
  }

  public ResourceInfo getMinResources() {
    return minResources;
  }
  
  public ResourceInfo getMaxResources() {
    return maxResources;
  }

  public ResourceInfo getMaxContainerAllocation() {
    return maxContainerAllocation;
  }

  public ResourceInfo getReservedResources() {
    return reservedResources;
  }

  public int getMaxApplications() {
    return maxApps;
  }
  
  public String getQueueName() {
    return queueName;
  }
  
  public ResourceInfo getUsedResources() {
    return usedResources;
  }

  /**
   * @return the am used resource of this queue.
   */
  public ResourceInfo getAMUsedResources() {
    return amUsedResources;
  }

  /**
   * @return the am max resource of this queue.
   */
  public ResourceInfo getAMMaxResources() {
    return amMaxResources;
  }

  /**
   * @return the demand resource of this queue.
     */
  public ResourceInfo getDemandResources() {
    return demandResources;
  }

  /**
   * Returns the memory used by this queue as a fraction of the entire 
   * cluster capacity.
   */
  public float getUsedMemoryFraction() {
    return fractionMemUsed;
  }
  
  /**
   * Returns the capacity of this queue as a fraction of the entire cluster 
   * capacity.
   */
  public float getMaxResourcesFraction() {
    return fractionMemMaxShare;
  }
  
  /**
   * Returns the name of the scheduling policy used by this queue.
   */
  public String getSchedulingPolicy() {
    return schedulingPolicy;
  }

  public Collection<FairSchedulerQueueInfo> getChildQueues() {
    return childQueues != null ? childQueues.getQueueInfoList() :
        new ArrayList<FairSchedulerQueueInfo>();
  }

  public boolean isPreemptable() {
    return preemptable;
  }
}
