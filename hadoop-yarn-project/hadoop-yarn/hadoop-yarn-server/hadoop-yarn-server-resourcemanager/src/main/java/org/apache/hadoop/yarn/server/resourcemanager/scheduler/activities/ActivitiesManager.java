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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.util.SystemClock;

import java.util.concurrent.ConcurrentHashMap;
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
  private static final Log LOG = LogFactory.getLog(ActivitiesManager.class);
  private ConcurrentMap<NodeId, List<NodeAllocation>> recordingNodesAllocation;
  private ConcurrentMap<NodeId, List<NodeAllocation>> completedNodeAllocations;
  private Set<NodeId> activeRecordedNodes;
  private ConcurrentMap<ApplicationId, Long>
      recordingAppActivitiesUntilSpecifiedTime;
  private ConcurrentMap<ApplicationId, AppAllocation> appsAllocation;
  private ConcurrentMap<ApplicationId, List<AppAllocation>>
      completedAppAllocations;
  private boolean recordNextAvailableNode = false;
  private List<NodeAllocation> lastAvailableNodeActivities = null;
  private Thread cleanUpThread;
  private int timeThreshold = 600 * 1000;
  private final RMContext rmContext;
  private volatile boolean stopped;

  public ActivitiesManager(RMContext rmContext) {
    super(ActivitiesManager.class.getName());
    recordingNodesAllocation = new ConcurrentHashMap<>();
    completedNodeAllocations = new ConcurrentHashMap<>();
    appsAllocation = new ConcurrentHashMap<>();
    completedAppAllocations = new ConcurrentHashMap<>();
    activeRecordedNodes = Collections.newSetFromMap(new ConcurrentHashMap<>());
    recordingAppActivitiesUntilSpecifiedTime = new ConcurrentHashMap<>();
    this.rmContext = rmContext;
  }

  public AppActivitiesInfo getAppActivitiesInfo(ApplicationId applicationId) {
    if (rmContext.getRMApps().get(applicationId).getFinalApplicationStatus()
        == FinalApplicationStatus.UNDEFINED) {
      List<AppAllocation> allocations = completedAppAllocations.get(
          applicationId);

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

          Iterator<Map.Entry<ApplicationId, List<AppAllocation>>> iteApp =
              completedAppAllocations.entrySet().iterator();
          while (iteApp.hasNext()) {
            Map.Entry<ApplicationId, List<AppAllocation>> appAllocation =
                iteApp.next();
            if (rmContext.getRMApps().get(appAllocation.getKey())
                .getFinalApplicationStatus()
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
    if (activeRecordedNodes.contains(nodeID)) {
      List<NodeAllocation> nodeAllocation = new ArrayList<>();
      recordingNodesAllocation.put(nodeID, nodeAllocation);
    }
  }

  void startAppAllocationRecording(NodeId nodeID, long currTS,
      SchedulerApplicationAttempt application) {
    ApplicationId applicationId = application.getApplicationId();

    if (recordingAppActivitiesUntilSpecifiedTime.containsKey(applicationId)
        && recordingAppActivitiesUntilSpecifiedTime.get(applicationId)
        > currTS) {
      appsAllocation.put(applicationId,
          new AppAllocation(application.getPriority(), nodeID,
              application.getQueueName()));
    }

    if (recordingAppActivitiesUntilSpecifiedTime.containsKey(applicationId)
        && recordingAppActivitiesUntilSpecifiedTime.get(applicationId)
        <= currTS) {
      turnOffActivityMonitoringForApp(applicationId);
    }
  }

  // Add queue, application or container activity into specific node allocation.
  void addSchedulingActivityForNode(NodeId nodeID, String parentName,
      String childName, String priority, ActivityState state, String diagnostic,
      String type) {
    if (shouldRecordThisNode(nodeID)) {
      NodeAllocation nodeAllocation = getCurrentNodeAllocation(nodeID);
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
      AppAllocation appAllocation = appsAllocation.get(applicationId);
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
      AppAllocation appAllocation = appsAllocation.remove(applicationId);
      appAllocation.updateAppContainerStateAndTime(containerId, appState,
          currTS, diagnostic);

      List<AppAllocation> appAllocations;
      if (completedAppAllocations.containsKey(applicationId)) {
        appAllocations = completedAppAllocations.get(applicationId);
      } else {
        appAllocations = new ArrayList<>();
        completedAppAllocations.put(applicationId, appAllocations);
      }
      if (appAllocations.size() == 1000) {
        appAllocations.remove(0);
      }
      appAllocations.add(appAllocation);

      if (recordingAppActivitiesUntilSpecifiedTime.get(applicationId)
          <= currTS) {
        turnOffActivityMonitoringForApp(applicationId);
      }
    }
  }

  void finishNodeUpdateRecording(NodeId nodeID) {
    List<NodeAllocation> value = recordingNodesAllocation.get(nodeID);
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
        recordingNodesAllocation.remove(nodeID);
        completedNodeAllocations.put(nodeID, value);
        stopRecordNodeUpdateActivities(nodeID);
      }
    }
  }

  boolean shouldRecordThisApp(ApplicationId applicationId) {
    return recordingAppActivitiesUntilSpecifiedTime.containsKey(applicationId)
        && appsAllocation.containsKey(applicationId);
  }

  boolean shouldRecordThisNode(NodeId nodeID) {
    return activeRecordedNodes.contains(nodeID) && recordingNodesAllocation
        .containsKey(nodeID);
  }

  private NodeAllocation getCurrentNodeAllocation(NodeId nodeID) {
    List<NodeAllocation> nodeAllocations = recordingNodesAllocation.get(nodeID);
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

  private void stopRecordNodeUpdateActivities(NodeId nodeId) {
    activeRecordedNodes.remove(nodeId);
  }

  private void turnOffActivityMonitoringForApp(ApplicationId applicationId) {
    recordingAppActivitiesUntilSpecifiedTime.remove(applicationId);
  }
}
