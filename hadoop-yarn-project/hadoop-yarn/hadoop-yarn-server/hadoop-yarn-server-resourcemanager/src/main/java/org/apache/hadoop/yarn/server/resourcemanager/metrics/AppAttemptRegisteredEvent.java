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

public class AppAttemptRegisteredEvent extends
    SystemMetricsEvent {

  private ApplicationAttemptId appAttemptId;
  private String host;
  private int rpcPort;
  private String trackingUrl;
  private String originalTrackingUrl;
  private ContainerId masterContainerId;

  public AppAttemptRegisteredEvent(
      ApplicationAttemptId appAttemptId,
      String host,
      int rpcPort,
      String trackingUrl,
      String originalTrackingUrl,
      ContainerId masterContainerId,
      long registeredTime) {
    super(SystemMetricsEventType.APP_ATTEMPT_REGISTERED, registeredTime);
    this.appAttemptId = appAttemptId;
    this.host = host;
    this.rpcPort = rpcPort;
    // This is the tracking URL after the application attempt is registered
    this.trackingUrl = trackingUrl;
    this.originalTrackingUrl = originalTrackingUrl;
    this.masterContainerId = masterContainerId;
  }

  @Override
  public int hashCode() {
    return appAttemptId.getApplicationId().hashCode();
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return appAttemptId;
  }

  public String getHost() {
    return host;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public String getTrackingUrl() {
    return trackingUrl;
  }

  public String getOriginalTrackingURL() {
    return originalTrackingUrl;
  }

  public ContainerId getMasterContainerId() {
    return masterContainerId;
  }

}
