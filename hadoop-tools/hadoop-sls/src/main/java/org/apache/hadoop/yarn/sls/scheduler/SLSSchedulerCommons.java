/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.sls.scheduler;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SLSSchedulerCommons {
  private static final Logger LOG = LoggerFactory.getLogger(SLSSchedulerCommons.class);

  private final AbstractYarnScheduler<?, ?> scheduler;
  private boolean metricsON;
  private SchedulerMetrics schedulerMetrics;
  private final Map<ContainerId, Resource> preemptionContainerMap = new ConcurrentHashMap<>();
  private final Map<ApplicationAttemptId, String> appQueueMap = new ConcurrentHashMap<>();
  private final Tracker tracker;
  
  public SLSSchedulerCommons(AbstractYarnScheduler<?, ?> scheduler) {
    this.scheduler = scheduler;
    this.tracker = new Tracker();
  }

  public void initMetrics(Class<? extends AbstractYarnScheduler<?, ?>> schedulerClass, Configuration conf) {
    metricsON = conf.getBoolean(SLSConfiguration.METRICS_SWITCH, true);
    if (metricsON) {
      try {
        schedulerMetrics = SchedulerMetrics.getInstance(conf, schedulerClass);
        schedulerMetrics.init(scheduler, conf);
      } catch (Exception e) {
        LOG.error("Caught exception while initializing schedulerMetrics", e);
      }
    }
  }

  void stopMetrics() {
    try {
      if (metricsON) {
        schedulerMetrics.tearDown();
      }
    } catch (Exception e) {
      LOG.error("Caught exception while stopping service", e);
    }
  }
  
  public Allocation allocate(ApplicationAttemptId attemptId,
      List<ResourceRequest> resourceRequests,
      List<SchedulingRequest> schedulingRequests,
      List<ContainerId> containerIds,
      List<String> blacklistAdditions,
      List<String> blacklistRemovals,
      ContainerUpdates updateRequests) {
    if (metricsON) {
      final Timer.Context context = schedulerMetrics.getSchedulerAllocateTimer()
          .time();
      Allocation allocation = null;
      try {
        allocation = ((SchedulerWrapper)scheduler).allocatePropagated(
            attemptId, resourceRequests,
            schedulingRequests, containerIds,
            blacklistAdditions, blacklistRemovals, updateRequests);
        return allocation;
      } catch (Exception e) {
        LOG.error("Caught exception from allocate", e);
        throw e;
      } finally {
        context.stop();
        schedulerMetrics.increaseSchedulerAllocationCounter();
        try {
          updateQueueWithAllocateRequest(allocation, attemptId,
              resourceRequests, containerIds);
        } catch (IOException e) {
          LOG.error("Caught exception while executing finally block", e);
        }
      }
    } else {
      return ((SchedulerWrapper)scheduler).allocatePropagated(
          attemptId, resourceRequests, schedulingRequests,
          containerIds,
          blacklistAdditions, blacklistRemovals, updateRequests);
    }
  }

  private void updateQueueWithAllocateRequest(Allocation allocation,
      ApplicationAttemptId attemptId,
      List<ResourceRequest> resourceRequests,
      List<ContainerId> containerIds) throws IOException {
    // update queue information
    Resource pendingResource = Resources.createResource(0, 0);
    Resource allocatedResource = Resources.createResource(0, 0);
    String queueName = appQueueMap.get(attemptId);
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
    SchedulerAppReport report = scheduler.getSchedulerAppInfo(attemptId);
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
    Set<ContainerId> preemptionContainers = new HashSet<>();
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
    schedulerMetrics.updateQueueMetrics(pendingResource, allocatedResource,
        queueName);
  }

  public void handle(SchedulerEvent schedulerEvent) {
    SchedulerWrapper wrapper = (SchedulerWrapper) scheduler;
    if (!metricsON) {
      ((SchedulerWrapper)scheduler).propagatedHandle(schedulerEvent);
      return;
    }

    if (!schedulerMetrics.isRunning()) {
      schedulerMetrics.setRunning(true);
    }

    Timer.Context handlerTimer = null;
    Timer.Context operationTimer = null;

    NodeUpdateSchedulerEventWrapper eventWrapper;
    try {
      if (schedulerEvent.getType() == SchedulerEventType.NODE_UPDATE
          && schedulerEvent instanceof NodeUpdateSchedulerEvent) {
        eventWrapper = new NodeUpdateSchedulerEventWrapper(
            (NodeUpdateSchedulerEvent) schedulerEvent);
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
        String queue = appQueueMap.get(appAttemptId);
        SchedulerAppReport app = scheduler.getSchedulerAppInfo(appAttemptId);
        if (!app.getLiveContainers().isEmpty()) {  // have 0 or 1
          // should have one container which is AM container
          RMContainer rmc = app.getLiveContainers().iterator().next();
          schedulerMetrics.updateQueueMetricsByRelease(
              rmc.getContainer().getResource(), queue);
        }
      }

      handlerTimer = schedulerMetrics.getSchedulerHandleTimer().time();
      operationTimer = schedulerMetrics.getSchedulerHandleTimer(
          schedulerEvent.getType()).time();

      ((SchedulerWrapper)scheduler).propagatedHandle(schedulerEvent);
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
        wrapper.getSLSRunner().decreaseRemainingApps();
        AppAttemptRemovedSchedulerEvent appRemoveEvent =
            (AppAttemptRemovedSchedulerEvent) schedulerEvent;
        appQueueMap.remove(appRemoveEvent.getApplicationAttemptID());
        if (wrapper.getSLSRunner().getRemainingApps() == 0) {
          try {
            schedulerMetrics.tearDown();
            SLSRunner.exitSLSRunner();
          } catch (Exception e) {
            LOG.error("Scheduler Metrics failed to tear down.", e);
          }
        }
      } else if (schedulerEvent.getType() ==
          SchedulerEventType.APP_ATTEMPT_ADDED
          && schedulerEvent instanceof AppAttemptAddedSchedulerEvent) {
        AppAttemptAddedSchedulerEvent appAddEvent =
            (AppAttemptAddedSchedulerEvent) schedulerEvent;
        SchedulerApplication app =
            scheduler.getSchedulerApplications()
                .get(appAddEvent.getApplicationAttemptId().getApplicationId());
        appQueueMap.put(appAddEvent.getApplicationAttemptId(), app.getQueue()
            .getQueueName());
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
        SchedulerAppReport app = scheduler.getSchedulerAppInfo(
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
        String queue = appQueueMap.get(containerId.getApplicationAttemptId());
        schedulerMetrics.updateQueueMetricsByRelease(
            Resource.newInstance(releasedMemory, releasedVCores), queue);
      }
    }
  }
  
  public SchedulerMetrics getSchedulerMetrics() {
    return schedulerMetrics;
  }

  public boolean isMetricsON() {
    return metricsON;
  }

  public Tracker getTracker() {
    return tracker;
  }
}
