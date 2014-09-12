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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;

public class RMAppAttemptMetrics {
  private static final Log LOG = LogFactory.getLog(RMAppAttemptMetrics.class);

  private ApplicationAttemptId attemptId = null;
  // preemption info
  private Resource resourcePreempted = Resource.newInstance(0, 0);
  private AtomicInteger numNonAMContainersPreempted = new AtomicInteger(0);
  private AtomicBoolean isPreempted = new AtomicBoolean(false);
  
  private ReadLock readLock;
  private WriteLock writeLock;
  private AtomicLong finishedMemorySeconds = new AtomicLong(0);
  private AtomicLong finishedVcoreSeconds = new AtomicLong(0);
  private RMContext rmContext;

  public RMAppAttemptMetrics(ApplicationAttemptId attemptId,
      RMContext rmContext) {
    this.attemptId = attemptId;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
    this.rmContext = rmContext;
  }
  
  public void updatePreemptionInfo(Resource resource, RMContainer container) {
    try {
      writeLock.lock();
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
    try {
      readLock.lock();
      return resourcePreempted;
    } finally {
      readLock.unlock();
    }
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
    long memorySeconds = finishedMemorySeconds.get();
    long vcoreSeconds = finishedVcoreSeconds.get();

    // Only add in the running containers if this is the active attempt.
    RMAppAttempt currentAttempt = rmContext.getRMApps()
                   .get(attemptId.getApplicationId()).getCurrentAppAttempt();
    if (currentAttempt.getAppAttemptId().equals(attemptId)) {
      ApplicationResourceUsageReport appResUsageReport = rmContext
            .getScheduler().getAppResourceUsageReport(attemptId);
      if (appResUsageReport != null) {
        memorySeconds += appResUsageReport.getMemorySeconds();
        vcoreSeconds += appResUsageReport.getVcoreSeconds();
      }
    }
    return new AggregateAppResourceUsage(memorySeconds, vcoreSeconds);
  }

  public void updateAggregateAppResourceUsage(long finishedMemorySeconds,
                                        long finishedVcoreSeconds) {
    this.finishedMemorySeconds.addAndGet(finishedMemorySeconds);
    this.finishedVcoreSeconds.addAndGet(finishedVcoreSeconds);
  }
}
