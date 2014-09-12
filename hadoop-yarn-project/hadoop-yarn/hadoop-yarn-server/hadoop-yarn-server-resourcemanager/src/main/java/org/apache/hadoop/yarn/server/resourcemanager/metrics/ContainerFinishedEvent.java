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

package org.apache.hadoop.yarn.server.resourcemanager.metrics;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;

public class ContainerFinishedEvent extends SystemMetricsEvent {

  private ContainerId containerId;
  private String diagnosticsInfo;
  private int containerExitStatus;
  private ContainerState state;

  public ContainerFinishedEvent(
      ContainerId containerId,
      String diagnosticsInfo,
      int containerExitStatus,
      ContainerState state,
      long finishedTime) {
    super(SystemMetricsEventType.CONTAINER_FINISHED, finishedTime);
    this.containerId = containerId;
    this.diagnosticsInfo = diagnosticsInfo;
    this.containerExitStatus = containerExitStatus;
    this.state = state;
  }

  @Override
  public int hashCode() {
    return containerId.getApplicationAttemptId().getApplicationId().hashCode();
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }

  public int getContainerExitStatus() {
    return containerExitStatus;
  }

  public ContainerState getContainerState() {
    return state;
  }

}
