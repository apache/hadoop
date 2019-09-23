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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import java.util.function.Supplier;

/**
 * Utility for logging scheduler activities
 */
public class ActivitiesLogger {
  private static final Logger LOG =
      LoggerFactory.getLogger(ActivitiesLogger.class);

  /**
   * Methods for recording activities from an app
   */
  public static class APP {

    /*
     * Record skipped application activity when no container allocated /
     * reserved / re-reserved. Scheduler will look at following applications
     * within the same leaf queue.
     */
    public static void recordSkippedAppActivityWithoutAllocation(
        ActivitiesManager activitiesManager, SchedulerNode node,
        SchedulerApplicationAttempt application,
        SchedulerRequestKey requestKey,
        String diagnostic, ActivityLevel level) {
      recordAppActivityWithoutAllocation(activitiesManager, node, application,
          requestKey, diagnostic, ActivityState.SKIPPED, level);
    }

    /*
     * Record application activity when rejected because of queue maximum
     * capacity or user limit.
     */
    public static void recordRejectedAppActivityFromLeafQueue(
        ActivitiesManager activitiesManager, SchedulerNode node,
        SchedulerApplicationAttempt application, Priority priority,
        String diagnostic) {
      if (activitiesManager == null) {
        return;
      }
      NodeId nodeId = getRecordingNodeId(activitiesManager, node);
      if (activitiesManager.shouldRecordThisNode(nodeId)) {
        recordActivity(activitiesManager, nodeId, application.getQueueName(),
            application.getApplicationId().toString(), priority,
            ActivityState.REJECTED, diagnostic, ActivityLevel.APP);
      }
      finishSkippedAppAllocationRecording(activitiesManager,
          application.getApplicationId(), ActivityState.REJECTED, diagnostic);
    }

    /*
     * Record application activity when no container allocated /
     * reserved / re-reserved. Scheduler will look at following applications
     * within the same leaf queue.
     */
    public static void recordAppActivityWithoutAllocation(
        ActivitiesManager activitiesManager, SchedulerNode node,
        SchedulerApplicationAttempt application,
        SchedulerRequestKey schedulerKey,
        String diagnostic, ActivityState appState, ActivityLevel level) {
      if (activitiesManager == null) {
        return;
      }
      NodeId nodeId = getRecordingNodeId(activitiesManager, node);
      if (activitiesManager.shouldRecordThisNode(nodeId)) {
        String requestName = null;
        Integer priority = null;
        Long allocationRequestId = null;
        if (level == ActivityLevel.NODE || level == ActivityLevel.REQUEST) {
          if (schedulerKey == null) {
            LOG.warn("Request key should not be null at " + level + " level.");
            return;
          }
          priority = getPriority(schedulerKey);
          allocationRequestId = schedulerKey.getAllocationRequestId();
          requestName = getRequestName(priority, allocationRequestId);
        }
        switch (level) {
        case NODE:
          recordSchedulerActivityAtNodeLevel(activitiesManager, application,
              requestName, priority, allocationRequestId, null, nodeId,
              appState, diagnostic);
          break;
        case REQUEST:
          recordSchedulerActivityAtRequestLevel(activitiesManager, application,
              requestName, priority, allocationRequestId, nodeId, appState,
              diagnostic);
          break;
        case APP:
          recordSchedulerActivityAtAppLevel(activitiesManager, application,
              nodeId, appState, diagnostic);
          break;
        default:
          LOG.warn("Doesn't handle app activities at " + level + " level.");
          break;
        }
      }
      // Add application-container activity into specific application allocation
      // Under this condition, it fails to allocate a container to this
      // application, so containerId is null.
      if (activitiesManager.shouldRecordThisApp(
          application.getApplicationId())) {
        activitiesManager.addSchedulingActivityForApp(
            application.getApplicationId(), null,
            getPriority(schedulerKey), appState,
            diagnostic, level, nodeId,
            schedulerKey == null ?
                null : schedulerKey.getAllocationRequestId());
      }
    }

    /*
     * Record application activity when container allocated / reserved /
     * re-reserved
     */
    public static void recordAppActivityWithAllocation(
        ActivitiesManager activitiesManager, SchedulerNode node,
        SchedulerApplicationAttempt application, RMContainer updatedContainer,
        ActivityState activityState) {
      if (activitiesManager == null) {
        return;
      }
      NodeId nodeId = getRecordingNodeId(activitiesManager, node);
      if (nodeId == null || nodeId == ActivitiesManager.EMPTY_NODE_ID) {
        nodeId = updatedContainer.getNodeId();
      }
      if (activitiesManager.shouldRecordThisNode(nodeId)) {
        Integer containerPriority =
            updatedContainer.getContainer().getPriority().getPriority();
        Long allocationRequestId =
            updatedContainer.getContainer().getAllocationRequestId();
        String requestName =
            getRequestName(containerPriority, allocationRequestId);
        // Add node,request,app level activities into scheduler activities.
        recordSchedulerActivityAtNodeLevel(activitiesManager, application,
            requestName, containerPriority, allocationRequestId,
            updatedContainer.getContainer().toString(), nodeId, activityState,
            ActivityDiagnosticConstant.EMPTY);
      }
      // Add application-container activity into specific application allocation
      if (activitiesManager.shouldRecordThisApp(
          application.getApplicationId())) {
        activitiesManager.addSchedulingActivityForApp(
            application.getApplicationId(),
            updatedContainer.getContainerId(),
            updatedContainer.getContainer().getPriority().getPriority(),
            activityState, ActivityDiagnosticConstant.EMPTY,
            ActivityLevel.NODE, nodeId,
            updatedContainer.getContainer().getAllocationRequestId());
      }
    }

    @SuppressWarnings("parameternumber")
    private static void recordSchedulerActivityAtNodeLevel(
        ActivitiesManager activitiesManager, SchedulerApplicationAttempt app,
        String requestName, Integer priority, Long allocationRequestId,
        String containerId, NodeId nodeId, ActivityState state,
        String diagnostic) {
      activitiesManager
          .addSchedulingActivityForNode(nodeId, requestName, containerId, null,
              state, diagnostic, ActivityLevel.NODE, null);
      // Record request level activity additionally.
      recordSchedulerActivityAtRequestLevel(activitiesManager, app, requestName,
          priority, allocationRequestId, nodeId, state,
          ActivityDiagnosticConstant.EMPTY);
    }

    @SuppressWarnings("parameternumber")
    private static void recordSchedulerActivityAtRequestLevel(
        ActivitiesManager activitiesManager, SchedulerApplicationAttempt app,
        String requestName, Integer priority, Long allocationRequestId,
        NodeId nodeId, ActivityState state, String diagnostic) {
      activitiesManager.addSchedulingActivityForNode(nodeId,
          app.getApplicationId().toString(), requestName, priority,
          state, diagnostic, ActivityLevel.REQUEST,
          allocationRequestId);
      // Record app level activity additionally.
      recordSchedulerActivityAtAppLevel(activitiesManager, app, nodeId, state,
          ActivityDiagnosticConstant.EMPTY);
    }

    private static void recordSchedulerActivityAtAppLevel(
        ActivitiesManager activitiesManager, SchedulerApplicationAttempt app,
        NodeId nodeId, ActivityState state, String diagnostic) {
      activitiesManager.addSchedulingActivityForNode(nodeId, app.getQueueName(),
          app.getApplicationId().toString(), app.getPriority().getPriority(),
          state, diagnostic, ActivityLevel.APP, null);
    }

    /*
     * Invoked when scheduler starts to look at this application within one node
     * update.
     */
    public static void startAppAllocationRecording(
        ActivitiesManager activitiesManager, FiCaSchedulerNode node,
        long currentTime,
        SchedulerApplicationAttempt application) {
      if (activitiesManager == null) {
        return;
      }
      NodeId nodeId = getRecordingNodeId(activitiesManager, node);
      activitiesManager
          .startAppAllocationRecording(nodeId, currentTime,
              application);
    }

    /*
     * Invoked when scheduler finishes looking at this application within one
     * node update, and the app has any container allocated/reserved during
     * this allocation.
     */
    public static void finishAllocatedAppAllocationRecording(
        ActivitiesManager activitiesManager, ApplicationId applicationId,
        ContainerId containerId, ActivityState containerState,
        String diagnostic) {
      if (activitiesManager == null) {
        return;
      }

      if (activitiesManager.shouldRecordThisApp(applicationId)) {
        activitiesManager.finishAppAllocationRecording(applicationId,
            containerId, containerState, diagnostic);
      }
    }

    /*
     * Invoked when scheduler finishes looking at this application within one
     * node update, and the app DOESN'T have any container allocated/reserved
     * during this allocation.
     */
    public static void finishSkippedAppAllocationRecording(
        ActivitiesManager activitiesManager, ApplicationId applicationId,
        ActivityState containerState, String diagnostic) {
      finishAllocatedAppAllocationRecording(activitiesManager, applicationId,
          null, containerState, diagnostic);
    }
  }

  /**
   * Methods for recording activities from a queue
   */
  public static class QUEUE {
    /*
     * Record activities of a queue
     */
    public static void recordQueueActivity(ActivitiesManager activitiesManager,
        SchedulerNode node, String parentQueueName, String queueName,
        ActivityState state, String diagnostic) {
      recordQueueActivity(activitiesManager, node, parentQueueName, queueName,
          state, () -> diagnostic);
    }

    public static void recordQueueActivity(ActivitiesManager activitiesManager,
        SchedulerNode node, String parentQueueName, String queueName,
        ActivityState state, Supplier<String> diagnosticSupplier) {
      if (activitiesManager == null) {
        return;
      }
      NodeId nodeId = getRecordingNodeId(activitiesManager, node);
      if (activitiesManager.shouldRecordThisNode(nodeId)) {
        recordActivity(activitiesManager, nodeId, parentQueueName, queueName,
            null, state, diagnosticSupplier.get(), ActivityLevel.QUEUE);
      }
    }
  }

  /**
   * Methods for recording overall activities from one node update
   */
  public static class NODE {

    /*
     * Invoked when node allocation finishes, and there's NO container
     * allocated or reserved during the allocation
     */
    public static void finishSkippedNodeAllocation(
        ActivitiesManager activitiesManager, SchedulerNode node) {
      finishAllocatedNodeAllocation(activitiesManager, node, null,
          AllocationState.SKIPPED);
    }

    /*
     * Invoked when node allocation finishes, and there's any container
     * allocated or reserved during the allocation
     */
    public static void finishAllocatedNodeAllocation(
        ActivitiesManager activitiesManager, SchedulerNode node,
        ContainerId containerId, AllocationState containerState) {
      NodeId nodeId = getRecordingNodeId(activitiesManager, node);
      if (nodeId == null) {
        return;
      }
      if (activitiesManager.shouldRecordThisNode(nodeId)) {
        activitiesManager.updateAllocationFinalState(nodeId,
            containerId, containerState);
      }
    }

    /*
     * Invoked when node heartbeat finishes
     */
    public static void finishNodeUpdateRecording(
        ActivitiesManager activitiesManager, NodeId nodeID, String partition) {
      if (activitiesManager == null) {
        return;
      }
      activitiesManager.finishNodeUpdateRecording(nodeID, partition);
    }

    /*
     * Invoked when node heartbeat starts
     */
    public static void startNodeUpdateRecording(
        ActivitiesManager activitiesManager, NodeId nodeID) {
      if (activitiesManager == null) {
        return;
      }
      activitiesManager.startNodeUpdateRecording(nodeID);
    }
  }

  // Add queue, application or container activity into specific node allocation.
  private static void recordActivity(ActivitiesManager activitiesManager,
      NodeId nodeId, String parentName, String childName, Priority priority,
      ActivityState state, String diagnostic, ActivityLevel level) {
    activitiesManager.addSchedulingActivityForNode(nodeId, parentName,
        childName, priority != null ? priority.getPriority() : null, state,
        diagnostic, level, null);
  }

  private static NodeId getRecordingNodeId(ActivitiesManager activitiesManager,
      SchedulerNode node) {
    return activitiesManager == null ? null :
        activitiesManager.getRecordingNodeId(node);
  }

  private static String getRequestName(Integer priority,
      Long allocationRequestId) {
    return "request_"
        + (priority == null ? "" : priority)
        + "_" + (allocationRequestId == null ? "" : allocationRequestId);
  }

  private static Integer getPriority(SchedulerRequestKey schedulerKey) {
    Priority priority = schedulerKey == null ?
        null : schedulerKey.getPriority();
    return priority == null ? null : priority.getPriority();
  }
}
