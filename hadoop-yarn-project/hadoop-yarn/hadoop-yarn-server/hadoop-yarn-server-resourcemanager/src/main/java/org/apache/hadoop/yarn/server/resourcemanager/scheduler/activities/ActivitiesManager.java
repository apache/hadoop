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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.util.SystemClock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.List;
import java.util.Set;
import java.util.*;
import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * A class to store node or application allocations.
 * It mainly contains operations for allocation start, add, update and finish.
 */
public class ActivitiesManager extends AbstractService {
  private static final Logger LOG =
      LoggerFactory.getLogger(ActivitiesManager.class);
  // An empty node ID, we use this variable as a placeholder
  // in the activity records when recording multiple nodes assignments.
  public static final NodeId EMPTY_NODE_ID = NodeId.newInstance("", 0);
  public static final char DIAGNOSTICS_DETAILS_SEPARATOR = '\n';
  public static final String EMPTY_DIAGNOSTICS = "";
  private ThreadLocal<Map<NodeId, List<NodeAllocation>>>
      recordingNodesAllocation;
  @VisibleForTesting
  ConcurrentMap<NodeId, List<NodeAllocation>> completedNodeAllocations;
  private Set<NodeId> activeRecordedNodes;
  private ConcurrentMap<ApplicationId, Long>
      recordingAppActivitiesUntilSpecifiedTime;
  private ThreadLocal<Map<ApplicationId, AppAllocation>>
      appsAllocation;
  @VisibleForTesting
  ConcurrentMap<ApplicationId, Queue<AppAllocation>> completedAppAllocations;
  private boolean recordNextAvailableNode = false;
  private List<NodeAllocation> lastAvailableNodeActivities = null;
  private Thread cleanUpThread;
  private long activitiesCleanupIntervalMs;
  private long schedulerActivitiesTTL;
  private long appActivitiesTTL;
  private volatile int appActivitiesMaxQueueLength;
  private int configuredAppActivitiesMaxQueueLength;
  private final RMContext rmContext;
  private volatile boolean stopped;
  private ThreadLocal<DiagnosticsCollectorManager> diagnosticCollectorManager;

  public ActivitiesManager(RMContext rmContext) {
    super(ActivitiesManager.class.getName());
    recordingNodesAllocation = ThreadLocal.withInitial(() -> new HashMap());
    completedNodeAllocations = new ConcurrentHashMap<>();
    appsAllocation = ThreadLocal.withInitial(() -> new HashMap());
    completedAppAllocations = new ConcurrentHashMap<>();
    activeRecordedNodes = Collections.newSetFromMap(new ConcurrentHashMap<>());
    recordingAppActivitiesUntilSpecifiedTime = new ConcurrentHashMap<>();
    diagnosticCollectorManager = ThreadLocal.withInitial(
        () -> new DiagnosticsCollectorManager(
            new GenericDiagnosticsCollector()));
    this.rmContext = rmContext;
    if (rmContext.getYarnConfiguration() != null) {
      setupConfForCleanup(rmContext.getYarnConfiguration());
    }
  }

  private void setupConfForCleanup(Configuration conf) {
    activitiesCleanupIntervalMs = conf.getLong(
        YarnConfiguration.RM_ACTIVITIES_MANAGER_CLEANUP_INTERVAL_MS,
        YarnConfiguration.
            DEFAULT_RM_ACTIVITIES_MANAGER_CLEANUP_INTERVAL_MS);
    schedulerActivitiesTTL = conf.getLong(
        YarnConfiguration.RM_ACTIVITIES_MANAGER_SCHEDULER_ACTIVITIES_TTL_MS,
        YarnConfiguration.
            DEFAULT_RM_ACTIVITIES_MANAGER_SCHEDULER_ACTIVITIES_TTL_MS);
    appActivitiesTTL = conf.getLong(
        YarnConfiguration.RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_TTL_MS,
        YarnConfiguration.
            DEFAULT_RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_TTL_MS);
    configuredAppActivitiesMaxQueueLength = conf.getInt(YarnConfiguration.
            RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_MAX_QUEUE_LENGTH,
        YarnConfiguration.
            DEFAULT_RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_MAX_QUEUE_LENGTH);
    appActivitiesMaxQueueLength = configuredAppActivitiesMaxQueueLength;
  }

  public AppActivitiesInfo getAppActivitiesInfo(ApplicationId applicationId,
      Set<Integer> requestPriorities, Set<Long> allocationRequestIds,
      RMWSConsts.ActivitiesGroupBy groupBy, int limit, boolean summarize,
      double maxTimeInSeconds) {
    RMApp app = rmContext.getRMApps().get(applicationId);
    if (app != null && app.getFinalApplicationStatus()
        == FinalApplicationStatus.UNDEFINED) {
      Queue<AppAllocation> curAllocations =
          completedAppAllocations.get(applicationId);
      List<AppAllocation> allocations = null;
      if (curAllocations != null) {
        if (CollectionUtils.isNotEmpty(requestPriorities) || CollectionUtils
            .isNotEmpty(allocationRequestIds)) {
          allocations = curAllocations.stream().map(e -> e
              .filterAllocationAttempts(requestPriorities,
                  allocationRequestIds))
              .filter(e -> !e.getAllocationAttempts().isEmpty())
              .collect(Collectors.toList());
        } else {
          allocations = new ArrayList(curAllocations);
        }
      }
      if (summarize && allocations != null) {
        AppAllocation summaryAppAllocation =
            getSummarizedAppAllocation(allocations, maxTimeInSeconds);
        if (summaryAppAllocation != null) {
          allocations = Lists.newArrayList(summaryAppAllocation);
        }
      }
      if (allocations != null && limit > 0 && limit < allocations.size()) {
        allocations =
            allocations.subList(allocations.size() - limit, allocations.size());
      }
      return new AppActivitiesInfo(allocations, applicationId, groupBy);
    } else {
      return new AppActivitiesInfo(
          "fail to get application activities after finished",
          applicationId.toString());
    }
  }

  /**
   * Get summarized app allocation from multiple allocations as follows:
   * 1. Collect latest allocation attempts on nodes to construct an allocation
   *    summary on nodes from multiple app allocations which are recorded a few
   *    seconds before the last allocation.
   * 2. Copy other fields from the last allocation.
   */
  private AppAllocation getSummarizedAppAllocation(
      List<AppAllocation> allocations, double maxTimeInSeconds) {
    if (allocations == null || allocations.isEmpty()) {
      return null;
    }
    long startTime = allocations.get(allocations.size() - 1).getTime()
        - (long) (maxTimeInSeconds * 1000);
    Map<String, ActivityNode> nodeActivities = new HashMap<>();
    for (int i = allocations.size() - 1; i >= 0; i--) {
      AppAllocation appAllocation = allocations.get(i);
      if (startTime > appAllocation.getTime()) {
        break;
      }
      List<ActivityNode> activityNodes = appAllocation.getAllocationAttempts();
      for (ActivityNode an : activityNodes) {
        nodeActivities.putIfAbsent(
            an.getRequestPriority() + "_" + an.getAllocationRequestId() + "_"
                + an.getNodeId(), an);
      }
    }
    AppAllocation lastAppAllocation = allocations.get(allocations.size() - 1);
    AppAllocation summarizedAppAllocation =
        new AppAllocation(lastAppAllocation.getPriority(), null,
            lastAppAllocation.getQueueName());
    summarizedAppAllocation.updateAppContainerStateAndTime(null,
        lastAppAllocation.getActivityState(), lastAppAllocation.getTime(),
        lastAppAllocation.getDiagnostic());
    summarizedAppAllocation
        .setAllocationAttempts(new ArrayList<>(nodeActivities.values()));
    return summarizedAppAllocation;
  }

  public ActivitiesInfo getActivitiesInfo(String nodeId,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    List<NodeAllocation> allocations;
    if (nodeId == null) {
      allocations = lastAvailableNodeActivities;
    } else {
      allocations = completedNodeAllocations.get(NodeId.fromString(nodeId));
    }
    return new ActivitiesInfo(allocations, nodeId, groupBy);
  }

  public void recordNextNodeUpdateActivities(String nodeId) {
    if (nodeId == null) {
      recordNextAvailableNode = true;
    } else {
      activeRecordedNodes.add(NodeId.fromString(nodeId));
    }
  }

  public void turnOnAppActivitiesRecording(ApplicationId applicationId,
      double maxTime) {
    long startTS = SystemClock.getInstance().getTime();
    long endTS = startTS + (long) (maxTime * 1000);
    recordingAppActivitiesUntilSpecifiedTime.put(applicationId, endTS);
  }

  private void dynamicallyUpdateAppActivitiesMaxQueueLengthIfNeeded() {
    if (rmContext.getRMNodes() == null) {
      return;
    }
    if (rmContext.getScheduler() instanceof CapacityScheduler) {
      CapacityScheduler cs = (CapacityScheduler) rmContext.getScheduler();
      if (!cs.isMultiNodePlacementEnabled()) {
        int numNodes = rmContext.getRMNodes().size();
        int newAppActivitiesMaxQueueLength;
        int numAsyncSchedulerThreads = cs.getNumAsyncSchedulerThreads();
        if (numAsyncSchedulerThreads > 0) {
          newAppActivitiesMaxQueueLength =
              Math.max(configuredAppActivitiesMaxQueueLength,
                  numNodes * numAsyncSchedulerThreads);
        } else {
          newAppActivitiesMaxQueueLength =
              Math.max(configuredAppActivitiesMaxQueueLength,
                  (int) (numNodes * 1.2));
        }
        if (appActivitiesMaxQueueLength != newAppActivitiesMaxQueueLength) {
          LOG.info("Update max queue length of app activities from {} to {},"
                  + " configured={}, numNodes={}, numAsyncSchedulerThreads={}"
                  + " when multi-node placement disabled.",
              appActivitiesMaxQueueLength, newAppActivitiesMaxQueueLength,
              configuredAppActivitiesMaxQueueLength, numNodes,
              numAsyncSchedulerThreads);
          appActivitiesMaxQueueLength = newAppActivitiesMaxQueueLength;
        }
      } else if (appActivitiesMaxQueueLength
          != configuredAppActivitiesMaxQueueLength) {
        LOG.info("Update max queue length of app activities from {} to {}"
                + " when multi-node placement enabled.",
            appActivitiesMaxQueueLength, configuredAppActivitiesMaxQueueLength);
        appActivitiesMaxQueueLength = configuredAppActivitiesMaxQueueLength;
      }
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    cleanUpThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          Iterator<Map.Entry<NodeId, List<NodeAllocation>>> ite =
              completedNodeAllocations.entrySet().iterator();
          long curTS = SystemClock.getInstance().getTime();
          while (ite.hasNext()) {
            Map.Entry<NodeId, List<NodeAllocation>> nodeAllocation = ite.next();
            List<NodeAllocation> allocations = nodeAllocation.getValue();
            if (allocations.size() > 0
                && curTS - allocations.get(0).getTimestamp()
                > schedulerActivitiesTTL) {
              ite.remove();
            }
          }

          Iterator<Map.Entry<ApplicationId, Queue<AppAllocation>>> iteApp =
              completedAppAllocations.entrySet().iterator();
          while (iteApp.hasNext()) {
            Map.Entry<ApplicationId, Queue<AppAllocation>> appAllocation =
                iteApp.next();
            RMApp rmApp = rmContext.getRMApps().get(appAllocation.getKey());
            if (rmApp == null || rmApp.getFinalApplicationStatus()
                != FinalApplicationStatus.UNDEFINED) {
              iteApp.remove();
            } else {
              Iterator<AppAllocation> appActivitiesIt =
                  appAllocation.getValue().iterator();
              while (appActivitiesIt.hasNext()) {
                if (curTS - appActivitiesIt.next().getTime()
                    > appActivitiesTTL) {
                  appActivitiesIt.remove();
                } else {
                  break;
                }
              }
              if (appAllocation.getValue().isEmpty()) {
                iteApp.remove();
                LOG.debug("Removed all expired activities from cache for {}.",
                    rmApp.getApplicationId());
              }
            }
          }

          LOG.debug("Remaining apps in app activities cache: {}",
              completedAppAllocations.keySet());
          // dynamically update max queue length of app activities if needed
          dynamicallyUpdateAppActivitiesMaxQueueLengthIfNeeded();
          try {
            Thread.sleep(activitiesCleanupIntervalMs);
          } catch (InterruptedException e) {
            LOG.info(getName() + " thread interrupted");
            break;
          }
        }
      }
    });
    cleanUpThread.setName("ActivitiesManager thread.");
    cleanUpThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (cleanUpThread != null) {
      cleanUpThread.interrupt();
      try {
        cleanUpThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted Exception while stopping", ie);
      }
    }
    super.serviceStop();
  }

  void startNodeUpdateRecording(NodeId nodeID) {
    if (recordNextAvailableNode) {
      recordNextNodeUpdateActivities(nodeID.toString());
    }
    // Removing from activeRecordedNodes immediately is to ensure that
    // activities will be recorded just once in multiple threads.
    if (activeRecordedNodes.remove(nodeID)) {
      List<NodeAllocation> nodeAllocation = new ArrayList<>();
      recordingNodesAllocation.get().put(nodeID, nodeAllocation);
      // enable diagnostic collector
      diagnosticCollectorManager.get().enable();
    }
  }

  void startAppAllocationRecording(NodeId nodeID, long currTS,
      SchedulerApplicationAttempt application) {
    ApplicationId applicationId = application.getApplicationId();

    Long turnOffTimestamp =
        recordingAppActivitiesUntilSpecifiedTime.get(applicationId);
    if (turnOffTimestamp != null) {
      if (turnOffTimestamp > currTS) {
        appsAllocation.get().put(applicationId,
            new AppAllocation(application.getPriority(), nodeID,
                application.getQueueName()));
        // enable diagnostic collector
        diagnosticCollectorManager.get().enable();
      } else {
        turnOffActivityMonitoringForApp(applicationId);
      }
    }
  }

  // Add queue, application or container activity into specific node allocation.
  void addSchedulingActivityForNode(NodeId nodeId, String parentName,
      String childName, Integer priority, ActivityState state,
      String diagnostic, ActivityLevel level, Long allocationRequestId) {
    if (shouldRecordThisNode(nodeId)) {
      NodeAllocation nodeAllocation = getCurrentNodeAllocation(nodeId);

      ResourceScheduler scheduler = this.rmContext.getScheduler();
      //Sorry about this :( Making sure CS short queue references are normalized
      if (scheduler instanceof CapacityScheduler) {
        CapacityScheduler cs = (CapacityScheduler)this.rmContext.getScheduler();
        parentName = cs.normalizeQueueName(parentName);
        childName  = cs.normalizeQueueName(childName);
      }

      nodeAllocation.addAllocationActivity(parentName, childName, priority,
          state, diagnostic, level, nodeId, allocationRequestId);
    }
  }

  // Add queue, application or container activity into specific application
  // allocation.
  void addSchedulingActivityForApp(ApplicationId applicationId,
      ContainerId containerId, Integer priority, ActivityState state,
      String diagnostic, ActivityLevel level, NodeId nodeId,
      Long allocationRequestId) {
    if (shouldRecordThisApp(applicationId)) {
      AppAllocation appAllocation = appsAllocation.get().get(applicationId);
      appAllocation.addAppAllocationActivity(containerId == null ?
          "Container-Id-Not-Assigned" :
          containerId.toString(), priority, state, diagnostic, level, nodeId,
          allocationRequestId);
    }
  }

  // Update container allocation meta status for this node allocation.
  // It updates general container status but not the detailed activity state
  // in updateActivityState.
  void updateAllocationFinalState(NodeId nodeID, ContainerId containerId,
      AllocationState containerState) {
    if (shouldRecordThisNode(nodeID)) {
      NodeAllocation nodeAllocation = getCurrentNodeAllocation(nodeID);
      nodeAllocation.updateContainerState(containerId, containerState);
    }
  }

  void finishAppAllocationRecording(ApplicationId applicationId,
      ContainerId containerId, ActivityState appState, String diagnostic) {
    if (shouldRecordThisApp(applicationId)) {
      long currTS = SystemClock.getInstance().getTime();
      AppAllocation appAllocation = appsAllocation.get().remove(applicationId);
      appAllocation.updateAppContainerStateAndTime(containerId, appState,
          currTS, diagnostic);

      Queue<AppAllocation> appAllocations =
          completedAppAllocations.get(applicationId);
      if (appAllocations == null) {
        appAllocations = new ConcurrentLinkedQueue<>();
        Queue<AppAllocation> curAppAllocations =
            completedAppAllocations.putIfAbsent(applicationId, appAllocations);
        if (curAppAllocations != null) {
          appAllocations = curAppAllocations;
        }
      }
      int curQueueLength = appAllocations.size();
      while (curQueueLength >= appActivitiesMaxQueueLength) {
        appAllocations.poll();
        --curQueueLength;
      }
      appAllocations.add(appAllocation);
      Long stopTime =
          recordingAppActivitiesUntilSpecifiedTime.get(applicationId);
      if (stopTime != null && stopTime <= currTS) {
        turnOffActivityMonitoringForApp(applicationId);
      }
    }
  }

  void finishNodeUpdateRecording(NodeId nodeID, String partition) {
    List<NodeAllocation> value = recordingNodesAllocation.get().get(nodeID);
    long timestamp = SystemClock.getInstance().getTime();

    if (value != null) {
      if (value.size() > 0) {
        lastAvailableNodeActivities = value;
        for (NodeAllocation allocation : lastAvailableNodeActivities) {
          allocation.transformToTree();
          allocation.setTimestamp(timestamp);
          allocation.setPartition(partition);
        }
        if (recordNextAvailableNode) {
          recordNextAvailableNode = false;
        }
      }

      if (shouldRecordThisNode(nodeID)) {
        recordingNodesAllocation.get().remove(nodeID);
        completedNodeAllocations.put(nodeID, value);
      }
    }
    // disable diagnostic collector
    diagnosticCollectorManager.get().disable();
  }

  boolean shouldRecordThisApp(ApplicationId applicationId) {
    if (recordingAppActivitiesUntilSpecifiedTime.isEmpty()
        || appsAllocation.get().isEmpty()) {
      return false;
    }
    return recordingAppActivitiesUntilSpecifiedTime.containsKey(applicationId)
        && appsAllocation.get().containsKey(applicationId);
  }

  boolean shouldRecordThisNode(NodeId nodeID) {
    return isRecordingMultiNodes() || recordingNodesAllocation.get()
        .containsKey(nodeID);
  }

  private NodeAllocation getCurrentNodeAllocation(NodeId nodeID) {
    NodeId recordingKey =
        isRecordingMultiNodes() ? EMPTY_NODE_ID : nodeID;
    List<NodeAllocation> nodeAllocations =
        recordingNodesAllocation.get().get(recordingKey);
    NodeAllocation nodeAllocation;
    // When this node has already stored allocation activities, get the
    // last allocation for this node.
    if (nodeAllocations.size() != 0) {
      nodeAllocation = nodeAllocations.get(nodeAllocations.size() - 1);
      // When final state in last allocation is not DEFAULT, it means
      // last allocation has finished. Create a new allocation for this node,
      // and add it to the allocation list. Return this new allocation.
      //
      // When final state in last allocation is DEFAULT,
      // it means last allocation has not finished. Just get last allocation.
      if (nodeAllocation.getFinalAllocationState() != AllocationState.DEFAULT) {
        nodeAllocation = new NodeAllocation(nodeID);
        nodeAllocations.add(nodeAllocation);
      }
    }
    // When this node has not stored allocation activities,
    // create a new allocation for this node, and add it to the allocation list.
    // Return this new allocation.
    else {
      nodeAllocation = new NodeAllocation(nodeID);
      nodeAllocations.add(nodeAllocation);
    }
    return nodeAllocation;
  }

  private void turnOffActivityMonitoringForApp(ApplicationId applicationId) {
    recordingAppActivitiesUntilSpecifiedTime.remove(applicationId);
  }

  public boolean isRecordingMultiNodes() {
    return recordingNodesAllocation.get().containsKey(EMPTY_NODE_ID);
  }

  /**
   * Get recording node id:
   * 1. node id of the input node if it is not null.
   * 2. EMPTY_NODE_ID if input node is null and activities manager is
   *    recording multi-nodes.
   * 3. null otherwise.
   * @param node - input node
   * @return recording nodeId
   */
  public NodeId getRecordingNodeId(SchedulerNode node) {
    if (node != null) {
      return node.getNodeID();
    } else if (isRecordingMultiNodes()) {
      return ActivitiesManager.EMPTY_NODE_ID;
    }
    return null;
  }

  /**
   * Class to manage the diagnostics collector.
   */
  public static class DiagnosticsCollectorManager {
    private boolean enabled = false;
    private DiagnosticsCollector gdc;

    public boolean isEnabled() {
      return enabled;
    }

    public void enable() {
      this.enabled = true;
    }

    public void disable() {
      this.enabled = false;
    }

    public DiagnosticsCollectorManager(DiagnosticsCollector gdc) {
      this.gdc = gdc;
    }

    public Optional<DiagnosticsCollector> getOptionalDiagnosticsCollector() {
      if (enabled) {
        return Optional.of(gdc);
      } else {
        return Optional.empty();
      }
    }
  }

  public Optional<DiagnosticsCollector> getOptionalDiagnosticsCollector() {
    return diagnosticCollectorManager.get().getOptionalDiagnosticsCollector();
  }

  public String getResourceDiagnostics(ResourceCalculator rc, Resource required,
      Resource available) {
    Optional<DiagnosticsCollector> dcOpt = getOptionalDiagnosticsCollector();
    if (dcOpt.isPresent()) {
      dcOpt.get().collectResourceDiagnostics(rc, required, available);
      return getDiagnostics(dcOpt.get());
    }
    return EMPTY_DIAGNOSTICS;
  }

  public static String getDiagnostics(Optional<DiagnosticsCollector> dcOpt) {
    if (dcOpt != null && dcOpt.isPresent()) {
      DiagnosticsCollector dc = dcOpt.get();
      if (dc != null && dc.getDiagnostics() != null) {
        return getDiagnostics(dc);
      }
    }
    return EMPTY_DIAGNOSTICS;
  }

  private static String getDiagnostics(DiagnosticsCollector dc) {
    StringBuilder sb = new StringBuilder();
    sb.append(", ").append(dc.getDiagnostics());
    if (dc.getDetails() != null) {
      sb.append(DIAGNOSTICS_DETAILS_SEPARATOR).append(dc.getDetails());
    }
    return sb.toString();
  }

  @VisibleForTesting
  public int getAppActivitiesMaxQueueLength() {
    return appActivitiesMaxQueueLength;
  }
}
