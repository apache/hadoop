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

import com.google.common.annotations.VisibleForTesting;
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
  private int timeThreshold = 600 * 1000;
  private final RMContext rmContext;
  private volatile boolean stopped;

  public ActivitiesManager(RMContext rmContext) {
    super(ActivitiesManager.class.getName());
    recordingNodesAllocation = ThreadLocal.withInitial(() -> new HashMap());
    completedNodeAllocations = new ConcurrentHashMap<>();
    appsAllocation = ThreadLocal.withInitial(() -> new HashMap());
    completedAppAllocations = new ConcurrentHashMap<>();
    activeRecordedNodes = Collections.newSetFromMap(new ConcurrentHashMap<>());
    recordingAppActivitiesUntilSpecifiedTime = new ConcurrentHashMap<>();
    this.rmContext = rmContext;
  }

  public AppActivitiesInfo getAppActivitiesInfo(ApplicationId applicationId) {
    RMApp app = rmContext.getRMApps().get(applicationId);
    if (app != null && app.getFinalApplicationStatus()
        == FinalApplicationStatus.UNDEFINED) {
      Queue<AppAllocation> curAllocations =
          completedAppAllocations.get(applicationId);
      List<AppAllocation> allocations = null;
      if (curAllocations != null) {
        allocations = new ArrayList(curAllocations);
      }
      return new AppActivitiesInfo(allocations, applicationId);
    } else {
      return new AppActivitiesInfo(
          "fail to get application activities after finished",
          applicationId.toString());
    }
  }

  public ActivitiesInfo getActivitiesInfo(String nodeId) {
    List<NodeAllocation> allocations;
    if (nodeId == null) {
      allocations = lastAvailableNodeActivities;
    } else {
      allocations = completedNodeAllocations.get(NodeId.fromString(nodeId));
    }
    return new ActivitiesInfo(allocations, nodeId);
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

  @Override
  protected void serviceStart() throws Exception {
    cleanUpThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          Iterator<Map.Entry<NodeId, List<NodeAllocation>>> ite =
              completedNodeAllocations.entrySet().iterator();
          while (ite.hasNext()) {
            Map.Entry<NodeId, List<NodeAllocation>> nodeAllocation = ite.next();
            List<NodeAllocation> allocations = nodeAllocation.getValue();
            long currTS = SystemClock.getInstance().getTime();
            if (allocations.size() > 0 && allocations.get(0).getTimeStamp()
                - currTS > timeThreshold) {
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
            }
          }

          try {
            Thread.sleep(5000);
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
      } else {
        turnOffActivityMonitoringForApp(applicationId);
      }
    }
  }

  // Add queue, application or container activity into specific node allocation.
  void addSchedulingActivityForNode(NodeId nodeId, String parentName,
      String childName, String priority, ActivityState state, String diagnostic,
      String type) {
    if (shouldRecordThisNode(nodeId)) {
      NodeAllocation nodeAllocation = getCurrentNodeAllocation(nodeId);
      nodeAllocation.addAllocationActivity(parentName, childName, priority,
          state, diagnostic, type);
    }
  }

  // Add queue, application or container activity into specific application
  // allocation.
  void addSchedulingActivityForApp(ApplicationId applicationId,
      ContainerId containerId, String priority, ActivityState state,
      String diagnostic, String type) {
    if (shouldRecordThisApp(applicationId)) {
      AppAllocation appAllocation = appsAllocation.get().get(applicationId);
      appAllocation.addAppAllocationActivity(containerId == null ?
          "Container-Id-Not-Assigned" :
          containerId.toString(), priority, state, diagnostic, type);
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
      if (appAllocations.size() == 1000) {
        appAllocations.poll();
      }
      appAllocations.add(appAllocation);
      Long stopTime =
          recordingAppActivitiesUntilSpecifiedTime.get(applicationId);
      if (stopTime != null && stopTime <= currTS) {
        turnOffActivityMonitoringForApp(applicationId);
      }
    }
  }

  void finishNodeUpdateRecording(NodeId nodeID) {
    List<NodeAllocation> value = recordingNodesAllocation.get().get(nodeID);
    long timeStamp = SystemClock.getInstance().getTime();

    if (value != null) {
      if (value.size() > 0) {
        lastAvailableNodeActivities = value;
        for (NodeAllocation allocation : lastAvailableNodeActivities) {
          allocation.transformToTree();
          allocation.setTimeStamp(timeStamp);
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
}
