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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;

public class AppAttemptFinishedEvent extends
    SystemMetricsEvent {

  private ApplicationAttemptId appAttemptId;
  private String trackingUrl;
  private String originalTrackingUrl;
  private String diagnosticsInfo;
  private FinalApplicationStatus appStatus;
  private YarnApplicationAttemptState state;
  private ContainerId masterContainerId;

  public AppAttemptFinishedEvent(
      ApplicationAttemptId appAttemptId,
      String trackingUrl,
      String originalTrackingUrl,
      String diagnosticsInfo,
      FinalApplicationStatus appStatus,
      YarnApplicationAttemptState state,
      long finishedTime,
      ContainerId masterContainerId) {
    super(SystemMetricsEventType.APP_ATTEMPT_FINISHED, finishedTime);
    this.appAttemptId = appAttemptId;
    // This is the tracking URL after the application attempt is finished
    this.trackingUrl = trackingUrl;
    this.originalTrackingUrl = originalTrackingUrl;
    this.diagnosticsInfo = diagnosticsInfo;
    this.appStatus = appStatus;
    this.state = state;
    this.masterContainerId = masterContainerId;
  }

  @Override
  public int hashCode() {
    return appAttemptId.getApplicationId().hashCode();
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return appAttemptId;
  }

  public String getTrackingUrl() {
    return trackingUrl;
  }

  public String getOriginalTrackingURL() {
    return originalTrackingUrl;
  }

  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }

  public FinalApplicationStatus getFinalApplicationStatus() {
    return appStatus;
  }

  public YarnApplicationAttemptState getYarnApplicationAttemptState() {
    return state;
  }

  public ContainerId getMasterContainerId() {
    return masterContainerId;
  }

}
