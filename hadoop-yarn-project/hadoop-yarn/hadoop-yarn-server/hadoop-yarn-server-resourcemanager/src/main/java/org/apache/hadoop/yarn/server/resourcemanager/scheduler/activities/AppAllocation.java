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

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * It contains allocation information for one application within a period of
 * time.
 * Each application allocation may have several allocation attempts.
 */
public class AppAllocation {
  private Priority priority;
  private NodeId nodeId;
  private ContainerId containerId;
  private ActivityState activityState;
  private String diagnostic;
  private String queueName;
  private List<ActivityNode> allocationAttempts;
  private long timestamp;

  public AppAllocation(Priority priority, NodeId nodeId, String queueName) {
    this.priority = priority;
    this.nodeId = nodeId;
    this.allocationAttempts = new ArrayList<>();
    this.queueName = queueName;
  }

  public void updateAppContainerStateAndTime(ContainerId cId,
      ActivityState appState, long ts, String diagnostic) {
    this.timestamp = ts;
    this.containerId = cId;
    this.activityState = appState;
    this.diagnostic = diagnostic;
  }

  public void addAppAllocationActivity(String cId, Integer reqPriority,
      ActivityState state, String diagnose, ActivityLevel level, NodeId nId,
      Long allocationRequestId) {
    ActivityNode container = new ActivityNode(cId, null, reqPriority,
        state, diagnose, level, nId, allocationRequestId);
    this.allocationAttempts.add(container);
    if (state == ActivityState.REJECTED) {
      this.activityState = ActivityState.SKIPPED;
    } else {
      this.activityState = state;
    }
  }

  public String getNodeId() {
    return nodeId == null ? null : nodeId.toString();
  }

  public String getQueueName() {
    return queueName;
  }

  public ActivityState getActivityState() {
    return activityState;
  }

  public Priority getPriority() {
    return priority;
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

  public AppAllocation filterAllocationAttempts(Set<Integer> requestPriorities,
      Set<Long> allocationRequestIds) {
    AppAllocation appAllocation =
        new AppAllocation(this.priority, this.nodeId, this.queueName);
    appAllocation.activityState = this.activityState;
    appAllocation.containerId = this.containerId;
    appAllocation.timestamp = this.timestamp;
    appAllocation.diagnostic = this.diagnostic;
    Predicate<ActivityNode> predicate = (e) ->
        (CollectionUtils.isEmpty(requestPriorities) || requestPriorities
            .contains(e.getRequestPriority())) && (
            CollectionUtils.isEmpty(allocationRequestIds)
                || allocationRequestIds.contains(e.getAllocationRequestId()));
    appAllocation.allocationAttempts =
        this.allocationAttempts.stream().filter(predicate)
            .collect(Collectors.toList());
    return appAllocation;
  }

  public void setAllocationAttempts(List<ActivityNode> allocationAttempts) {
    this.allocationAttempts = allocationAttempts;
  }
}
