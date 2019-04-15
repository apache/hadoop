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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;

import java.util.ArrayList;
import java.util.List;

/*
 * It contains allocation information for one application within a period of
 * time.
 * Each application allocation may have several allocation attempts.
 */
public class AppAllocation {
  private Priority priority = null;
  private NodeId nodeId;
  private ContainerId containerId = null;
  private ActivityState appState = null;
  private String diagnostic = null;
  private String queueName = null;
  private List<ActivityNode> allocationAttempts;
  private long timestamp;

  public AppAllocation(Priority priority, NodeId nodeId, String queueName) {
    this.priority = priority;
    this.nodeId = nodeId;
    this.allocationAttempts = new ArrayList<>();
    this.queueName = queueName;
  }

  public void updateAppContainerStateAndTime(ContainerId containerId,
      ActivityState appState, long ts, String diagnostic) {
    this.timestamp = ts;
    this.containerId = containerId;
    this.appState = appState;
    this.diagnostic = diagnostic;
  }

  public void addAppAllocationActivity(String containerId, String priority,
      ActivityState state, String diagnostic, String type) {
    ActivityNode container = new ActivityNode(containerId, null, priority,
        state, diagnostic, type);
    this.allocationAttempts.add(container);
    if (state == ActivityState.REJECTED) {
      this.appState = ActivityState.SKIPPED;
    } else {
      this.appState = state;
    }
  }

  public String getNodeId() {
    return nodeId == null ? null : nodeId.toString();
  }

  public String getQueueName() {
    return queueName;
  }

  public ActivityState getAppState() {
    return appState;
  }

  public String getPriority() {
    if (priority == null) {
      return null;
    }
    return priority.toString();
  }

  public String getContainerId() {
    if (containerId == null) {
      return null;
    }
    return containerId.toString();
  }

  public String getDiagnostic() {
    return diagnostic;
  }

  public long getTime() {
    return this.timestamp;
  }

  public List<ActivityNode> getAllocationAttempts() {
    return allocationAttempts;
  }
}
