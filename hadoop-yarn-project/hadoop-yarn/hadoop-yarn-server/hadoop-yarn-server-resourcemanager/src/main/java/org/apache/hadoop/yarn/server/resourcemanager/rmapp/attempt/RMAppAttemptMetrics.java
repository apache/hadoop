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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.util.resource.Resources;

public class RMAppAttemptMetrics {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMAppAttemptMetrics.class);

  private ApplicationAttemptId attemptId = null;
  // preemption info
  private Resource resourcePreempted = Resource.newInstance(0, 0);
  // application headroom
  private volatile Resource applicationHeadroom = Resource.newInstance(0, 0);
  private AtomicInteger numNonAMContainersPreempted = new AtomicInteger(0);
  private AtomicBoolean isPreempted = new AtomicBoolean(false);
  
  private ReadLock readLock;
  private WriteLock writeLock;
  private Map<String, AtomicLong> resourceUsageMap = new ConcurrentHashMap<>();
  private Map<String, AtomicLong> preemptedResourceMap = new ConcurrentHashMap<>();
  private RMContext rmContext;

  private int[][] localityStatistics =
      new int[NodeType.values().length][NodeType.values().length];
  private volatile int totalAllocatedContainers;

  public RMAppAttemptMetrics(ApplicationAttemptId attemptId,
      RMContext rmContext) {
    this.attemptId = attemptId;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
    this.rmContext = rmContext;
  }

  public void updatePreemptionInfo(Resource resource, RMContainer container) {
    writeLock.lock();
    try {
      resourcePreempted = Resources.addTo(resourcePreempted, resource);
    } finally {
      writeLock.unlock();
    }

    if (!container.isAMContainer()) {
      // container got preempted is not a master container
      LOG.info(String.format(
        "Non-AM container preempted, current appAttemptId=%s, "
            + "containerId=%s, resource=%s", attemptId,
        container.getContainerId(), resource));
      numNonAMContainersPreempted.incrementAndGet();
    } else {
      // container got preempted is a master container
      LOG.info(String.format("AM container preempted, "
          + "current appAttemptId=%s, containerId=%s, resource=%s", attemptId,
        container.getContainerId(), resource));
      isPreempted.set(true);
    }
  }
  
  public Resource getResourcePreempted() {
    readLock.lock();
    try {
      return Resource.newInstance(resourcePreempted);
    } finally {
      readLock.unlock();
    }
  }

  public long getPreemptedMemory() {
    return preemptedResourceMap.get(ResourceInformation.MEMORY_MB.getName())
        .get();
  }

  public long getPreemptedVcore() {
    return preemptedResourceMap.get(ResourceInformation.VCORES.getName()).get();
  }

  public Map<String, Long> getPreemptedResourceSecondsMap() {
    return convertAtomicLongMaptoLongMap(preemptedResourceMap);
  }

  public int getNumNonAMContainersPreempted() {
    return numNonAMContainersPreempted.get();
  }
  
  public void setIsPreempted() {
    this.isPreempted.set(true);
  }
  
  public boolean getIsPreempted() {
    return this.isPreempted.get();
  }

  public AggregateAppResourceUsage getAggregateAppResourceUsage() {
    Map<String, Long> resourcesUsed =
        convertAtomicLongMaptoLongMap(resourceUsageMap);

    // Only add in the running containers if this is the active attempt.
    RMApp rmApp = rmContext.getRMApps().get(attemptId.getApplicationId());
    if (rmApp != null) {
      RMAppAttempt currentAttempt = rmApp.getCurrentAppAttempt();
      if (currentAttempt != null
          && currentAttempt.getAppAttemptId().equals(attemptId)) {
        ApplicationResourceUsageReport appResUsageReport =
            rmContext.getScheduler().getAppResourceUsageReport(attemptId);
        if (appResUsageReport != null) {
          Map<String, Long> tmp = appResUsageReport.getResourceSecondsMap();
          for (Map.Entry<String, Long> entry : tmp.entrySet()) {
            Long value = resourcesUsed.get(entry.getKey());
            if (value != null) {
              value += entry.getValue();
            } else {
              value = entry.getValue();
            }
            resourcesUsed.put(entry.getKey(), value);
          }
        }
      }
    }
    return new AggregateAppResourceUsage(resourcesUsed);
  }

  public void updateAggregateAppResourceUsage(Resource allocated,
      long deltaUsedMillis) {
    updateUsageMap(allocated, deltaUsedMillis, resourceUsageMap);
  }

  public void updateAggregatePreemptedAppResourceUsage(Resource allocated,
      long deltaUsedMillis) {
    updateUsageMap(allocated, deltaUsedMillis, preemptedResourceMap);
  }

  public void updateAggregateAppResourceUsage(
      Map<String, Long> resourceSecondsMap) {
    updateUsageMap(resourceSecondsMap, resourceUsageMap);
  }

  public void updateAggregatePreemptedAppResourceUsage(
      Map<String, Long> preemptedResourceSecondsMap) {
    updateUsageMap(preemptedResourceSecondsMap, preemptedResourceMap);
  }

  private void updateUsageMap(Resource allocated, long deltaUsedMillis,
      Map<String, AtomicLong> targetMap) {
    for (ResourceInformation entry : allocated.getResources()) {
      AtomicLong resourceUsed;
      if (!targetMap.containsKey(entry.getName())) {
        resourceUsed = new AtomicLong(0);
        targetMap.put(entry.getName(), resourceUsed);

      }
      resourceUsed = targetMap.get(entry.getName());
      resourceUsed.addAndGet((entry.getValue() * deltaUsedMillis)
          / DateUtils.MILLIS_PER_SECOND);
    }
  }

  private void updateUsageMap(Map<String, Long> sourceMap,
      Map<String, AtomicLong> targetMap) {
    for (Map.Entry<String, Long> entry : sourceMap.entrySet()) {
      AtomicLong resourceUsed;
      if (!targetMap.containsKey(entry.getKey())) {
        resourceUsed = new AtomicLong(0);
        targetMap.put(entry.getKey(), resourceUsed);

      }
      resourceUsed = targetMap.get(entry.getKey());
      resourceUsed.set(entry.getValue());
    }
  }

  private Map<String, Long> convertAtomicLongMaptoLongMap(
      Map<String, AtomicLong> source) {
    Map<String, Long> ret = new HashMap<>();
    for (Map.Entry<String, AtomicLong> entry : source.entrySet()) {
      ret.put(entry.getKey(), entry.getValue().get());
    }
    return ret;
  }

  public void incNumAllocatedContainers(NodeType containerType,
      NodeType requestType) {
    localityStatistics[containerType.getIndex()][requestType.getIndex()]++;
    totalAllocatedContainers++;
  }

  public int[][] getLocalityStatistics() {
    return this.localityStatistics;
  }

  public int getTotalAllocatedContainers() {
    return this.totalAllocatedContainers;
  }

  public Resource getApplicationAttemptHeadroom() {
    return Resource.newInstance(applicationHeadroom);
  }

  public void setApplicationAttemptHeadRoom(Resource headRoom) {
    this.applicationHeadroom = headRoom;
  }
}
