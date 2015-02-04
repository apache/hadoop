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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;

public class ApplicationFinishedEvent extends
    SystemMetricsEvent {

  private ApplicationId appId;;
  private String diagnosticsInfo;
  private FinalApplicationStatus appStatus;
  private YarnApplicationState state;
  private ApplicationAttemptId latestAppAttemptId;
  private RMAppMetrics appMetrics;

  public ApplicationFinishedEvent(
      ApplicationId appId,
      String diagnosticsInfo,
      FinalApplicationStatus appStatus,
      YarnApplicationState state,
      ApplicationAttemptId latestAppAttemptId,
      long finishedTime,
      RMAppMetrics appMetrics) {
    super(SystemMetricsEventType.APP_FINISHED, finishedTime);
    this.appId = appId;
    this.diagnosticsInfo = diagnosticsInfo;
    this.appStatus = appStatus;
    this.latestAppAttemptId = latestAppAttemptId;
    this.state = state;
    this.appMetrics=appMetrics;
  }

  @Override
  public int hashCode() {
    return appId.hashCode();
  }

  public ApplicationId getApplicationId() {
    return appId;
  }

  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }

  public FinalApplicationStatus getFinalApplicationStatus() {
    return appStatus;
  }

  public YarnApplicationState getYarnApplicationState() {
    return state;
  }

  public ApplicationAttemptId getLatestApplicationAttemptId() {
    return latestAppAttemptId;
  }

  public RMAppMetrics getAppMetrics() {
    return appMetrics;
  }
}
