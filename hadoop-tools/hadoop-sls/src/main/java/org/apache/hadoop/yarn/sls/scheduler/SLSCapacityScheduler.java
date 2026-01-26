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
package org.apache.hadoop.yarn.sls.scheduler;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
@Unstable
public class SLSCapacityScheduler extends CapacityScheduler implements
        SchedulerWrapper, Configurable {

  private final SLSSchedulerCommons schedulerCommons;
  private Configuration conf;
  private SLSRunner runner;
  private static final Logger LOG = LoggerFactory.getLogger(SLSCapacityScheduler.class);

  public SLSCapacityScheduler() {
    schedulerCommons = new SLSSchedulerCommons(this);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    super.setConf(conf);
    schedulerCommons.initMetrics(CapacityScheduler.class, conf);
  }

  @Override
  public Allocation allocate(ApplicationAttemptId attemptId,
      List<ResourceRequest> resourceRequests,
      List<SchedulingRequest> schedulingRequests, List<ContainerId> containerIds,
      List<String> blacklistAdditions, List<String> blacklistRemovals,
      ContainerUpdates updateRequests) {
    return schedulerCommons.allocate(attemptId, resourceRequests, schedulingRequests,
        containerIds, blacklistAdditions, blacklistRemovals, updateRequests);
  }

  @Override
  public Allocation allocatePropagated(ApplicationAttemptId attemptId,
      List<ResourceRequest> resourceRequests,
      List<SchedulingRequest> schedulingRequests,
      List<ContainerId> containerIds, List<String> blacklistAdditions,
      List<String> blacklistRemovals, ContainerUpdates updateRequests) {
    return super.allocate(attemptId, resourceRequests, schedulingRequests,
        containerIds, blacklistAdditions, blacklistRemovals, updateRequests);
  }

  @Override
  public boolean tryCommit(Resource cluster, ResourceCommitRequest r,
      boolean updatePending) {
    if (schedulerCommons.isMetricsON()) {
      boolean isSuccess = false;
      long startTimeNs = System.nanoTime();
      try {
        isSuccess = super.tryCommit(cluster, r, updatePending);
        return isSuccess;
      } finally {
        long elapsedNs = System.nanoTime() - startTimeNs;
        if (isSuccess) {
          getSchedulerMetrics().getSchedulerCommitSuccessTimer()
              .update(elapsedNs, TimeUnit.NANOSECONDS);
          getSchedulerMetrics().increaseSchedulerCommitSuccessCounter();
        } else {
          getSchedulerMetrics().getSchedulerCommitFailureTimer()
              .update(elapsedNs, TimeUnit.NANOSECONDS);
          getSchedulerMetrics().increaseSchedulerCommitFailureCounter();
        }
      }
    } else {
      return super.tryCommit(cluster, r, updatePending);
    }
  }

  @Override
  public void handle(SchedulerEvent schedulerEvent) {
    try {
      schedulerCommons.handle(schedulerEvent);
    } catch(Exception e) {
      LOG.error("Caught exception while handling scheduler event", e);
      throw e;
    }
  }

  @Override
  public void propagatedHandle(SchedulerEvent schedulerEvent) {
    super.handle(schedulerEvent);
  }

  @Override
  public void serviceStop() throws Exception {
    schedulerCommons.stopMetrics();
    super.serviceStop();
  }


  public String getRealQueueName(String queue) throws YarnException {
    if (getQueue(queue) == null) {
      throw new YarnException("Can't find the queue by the given name: " + queue
          + "! Please check if queue " + queue + " is in the allocation file.");
    }
    return getQueue(queue).getQueuePath();
  }

  public SchedulerMetrics getSchedulerMetrics() {
    return schedulerCommons.getSchedulerMetrics();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public Tracker getTracker() {
    return schedulerCommons.getTracker();
  }

  @Override
  public void setSLSRunner(SLSRunner runner) {
    this.runner = runner;
  }

  @Override
  public SLSRunner getSLSRunner() {
    return this.runner;
  }
}

