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

import com.codahale.metrics.Timer;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Private
@Unstable
public class SLSFairScheduler extends FairScheduler
    implements SchedulerWrapper, Configurable {
  private SchedulerMetrics schedulerMetrics;
  private boolean metricsON;
  private Tracker tracker;

  private Map<ContainerId, Resource> preemptionContainerMap =
      new ConcurrentHashMap<>();

  public SchedulerMetrics getSchedulerMetrics() {
    return schedulerMetrics;
  }

  public Tracker getTracker() {
    return tracker;
  }

  public SLSFairScheduler() {
    tracker = new Tracker();
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConfig(conf);

    metricsON = conf.getBoolean(SLSConfiguration.METRICS_SWITCH, true);
    if (metricsON) {
      try {
        schedulerMetrics = SchedulerMetrics.getInstance(conf,
            FairScheduler.class);
        schedulerMetrics.init(this, conf);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public Allocation allocate(ApplicationAttemptId attemptId,
      List<ResourceRequest> resourceRequests,
      List<SchedulingRequest> schedulingRequests, List<ContainerId> containerIds,
      List<String> blacklistAdditions, List<String> blacklistRemovals,
      ContainerUpdates updateRequests) {
    if (metricsON) {
      final Timer.Context context = schedulerMetrics.getSchedulerAllocateTimer()
          .time();
      Allocation allocation = null;
      try {
        allocation = super.allocate(attemptId, resourceRequests,
            schedulingRequests, containerIds,
            blacklistAdditions, blacklistRemovals, updateRequests);
        return allocation;
      } finally {
        context.stop();
        schedulerMetrics.increaseSchedulerAllocationCounter();
        try {
          updateQueueWithAllocateRequest(allocation, attemptId,
              resourceRequests, containerIds);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } else {
      return super.allocate(attemptId, resourceRequests, schedulingRequests,
          containerIds,
          blacklistAdditions, blacklistRemovals, updateRequests);
    }
  }

  @Override
  public void handle(SchedulerEvent schedulerEvent) {
    // metrics off
    if (!metricsON) {
      super.handle(schedulerEvent);
      return;
    }

    // metrics on
    if(!schedulerMetrics.isRunning()) {
      schedulerMetrics.setRunning(true);
    }

    Timer.Context handlerTimer = null;
    Timer.Context operationTimer = null;

    NodeUpdateSchedulerEventWrapper eventWrapper;
    try {
      if (schedulerEvent.getType() == SchedulerEventType.NODE_UPDATE
          && schedulerEvent instanceof NodeUpdateSchedulerEvent) {
        eventWrapper = new NodeUpdateSchedulerEventWrapper(
            (NodeUpdateSchedulerEvent)schedulerEvent);
        schedulerEvent = eventWrapper;
        updateQueueWithNodeUpdate(eventWrapper);
      } else if (
          schedulerEvent.getType() == SchedulerEventType.APP_ATTEMPT_REMOVED
          && schedulerEvent instanceof AppAttemptRemovedSchedulerEvent) {
        // check if having AM Container, update resource usage information
        AppAttemptRemovedSchedulerEvent appRemoveEvent =
            (AppAttemptRemovedSchedulerEvent) schedulerEvent;
        ApplicationAttemptId appAttemptId =
            appRemoveEvent.getApplicationAttemptID();
        String queueName = getSchedulerApp(appAttemptId).getQueue().getName();
        SchedulerAppReport app = getSchedulerAppInfo(appAttemptId);
        if (!app.getLiveContainers().isEmpty()) {  // have 0 or 1
          // should have one container which is AM container
          RMContainer rmc = app.getLiveContainers().iterator().next();
          schedulerMetrics.updateQueueMetricsByRelease(
              rmc.getContainer().getResource(), queueName);
        }
      }

      handlerTimer = schedulerMetrics.getSchedulerHandleTimer().time();
      operationTimer = schedulerMetrics.getSchedulerHandleTimer(
          schedulerEvent.getType()).time();

      super.handle(schedulerEvent);
    } finally {
      if (handlerTimer != null) {
        handlerTimer.stop();
      }
      if (operationTimer != null) {
        operationTimer.stop();
      }
      schedulerMetrics.increaseSchedulerHandleCounter(schedulerEvent.getType());

      if (schedulerEvent.getType() == SchedulerEventType.APP_ATTEMPT_REMOVED
          && schedulerEvent instanceof AppAttemptRemovedSchedulerEvent) {
        SLSRunner.decreaseRemainingApps();
      }
    }
  }

  private void updateQueueWithNodeUpdate(
      NodeUpdateSchedulerEventWrapper eventWrapper) {
    RMNodeWrapper node = (RMNodeWrapper) eventWrapper.getRMNode();
    List<UpdatedContainerInfo> containerList = node.getContainerUpdates();
    for (UpdatedContainerInfo info : containerList) {
      for (ContainerStatus status : info.getCompletedContainers()) {
        ContainerId containerId = status.getContainerId();
        SchedulerAppReport app = super.getSchedulerAppInfo(
            containerId.getApplicationAttemptId());

        if (app == null) {
          // this happens for the AM container
          // The app have already removed when the NM sends the release
          // information.
          continue;
        }

        int releasedMemory = 0, releasedVCores = 0;
        if (status.getExitStatus() == ContainerExitStatus.SUCCESS) {
          for (RMContainer rmc : app.getLiveContainers()) {
            if (rmc.getContainerId() == containerId) {
              Resource resource = rmc.getContainer().getResource();
              releasedMemory += resource.getMemorySize();
              releasedVCores += resource.getVirtualCores();
              break;
            }
          }
        } else if (status.getExitStatus() == ContainerExitStatus.ABORTED) {
          if (preemptionContainerMap.containsKey(containerId)) {
            Resource preResource = preemptionContainerMap.get(containerId);
            releasedMemory += preResource.getMemorySize();
            releasedVCores += preResource.getVirtualCores();
            preemptionContainerMap.remove(containerId);
          }
        }
        // update queue counters
        String queue = getSchedulerApp(containerId.getApplicationAttemptId()).
            getQueueName();
        schedulerMetrics.updateQueueMetricsByRelease(
            Resource.newInstance(releasedMemory, releasedVCores), queue);
      }
    }
  }

  private void updateQueueWithAllocateRequest(Allocation allocation,
      ApplicationAttemptId attemptId,
      List<ResourceRequest> resourceRequests,
      List<ContainerId> containerIds) throws IOException {
    // update queue information
    Resource pendingResource = Resources.createResource(0, 0);
    Resource allocatedResource = Resources.createResource(0, 0);
    // container requested
    for (ResourceRequest request : resourceRequests) {
      if (request.getResourceName().equals(ResourceRequest.ANY)) {
        Resources.addTo(pendingResource,
            Resources.multiply(request.getCapability(),
                request.getNumContainers()));
      }
    }
    // container allocated
    for (Container container : allocation.getContainers()) {
      Resources.addTo(allocatedResource, container.getResource());
      Resources.subtractFrom(pendingResource, container.getResource());
    }
    // container released from AM
    SchedulerAppReport report = super.getSchedulerAppInfo(attemptId);
    for (ContainerId containerId : containerIds) {
      Container container = null;
      for (RMContainer c : report.getLiveContainers()) {
        if (c.getContainerId().equals(containerId)) {
          container = c.getContainer();
          break;
        }
      }
      if (container != null) {
        // released allocated containers
        Resources.subtractFrom(allocatedResource, container.getResource());
      } else {
        for (RMContainer c : report.getReservedContainers()) {
          if (c.getContainerId().equals(containerId)) {
            container = c.getContainer();
            break;
          }
        }
        if (container != null) {
          // released reserved containers
          Resources.subtractFrom(pendingResource, container.getResource());
        }
      }
    }
    // containers released/preemption from scheduler
    Set<ContainerId> preemptionContainers = new HashSet<ContainerId>();
    if (allocation.getContainerPreemptions() != null) {
      preemptionContainers.addAll(allocation.getContainerPreemptions());
    }
    if (allocation.getStrictContainerPreemptions() != null) {
      preemptionContainers.addAll(allocation.getStrictContainerPreemptions());
    }
    if (!preemptionContainers.isEmpty()) {
      for (ContainerId containerId : preemptionContainers) {
        if (!preemptionContainerMap.containsKey(containerId)) {
          Container container = null;
          for (RMContainer c : report.getLiveContainers()) {
            if (c.getContainerId().equals(containerId)) {
              container = c.getContainer();
              break;
            }
          }
          if (container != null) {
            preemptionContainerMap.put(containerId, container.getResource());
          }
        }

      }
    }

    // update metrics
    String queueName = getSchedulerApp(attemptId).getQueueName();
    schedulerMetrics.updateQueueMetrics(pendingResource, allocatedResource,
        queueName);
  }

  @Override
  public void serviceStop() throws Exception {
    try {
      if (metricsON) {
        schedulerMetrics.tearDown();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    super.serviceStop();
  }

  public String getRealQueueName(String queue) throws YarnException {
    if (!getQueueManager().exists(queue)) {
      throw new YarnException("Can't find the queue by the given name: " + queue
          + "! Please check if queue " + queue + " is in the allocation file.");
    }
    return getQueueManager().getQueue(queue).getQueueName();
  }
}

