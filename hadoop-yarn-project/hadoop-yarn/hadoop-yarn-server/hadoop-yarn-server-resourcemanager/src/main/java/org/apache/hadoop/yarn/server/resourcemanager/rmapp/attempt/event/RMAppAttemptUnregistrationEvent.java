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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

public class RMAppAttemptUnregistrationEvent extends RMAppAttemptEvent {

  private final String finalTrackingUrl;
  private final FinalApplicationStatus finalStatus;

  public RMAppAttemptUnregistrationEvent(ApplicationAttemptId appAttemptId,
      String trackingUrl, FinalApplicationStatus finalStatus,
      String diagnostics) {
    super(appAttemptId, RMAppAttemptEventType.UNREGISTERED, diagnostics);
    this.finalTrackingUrl = trackingUrl;
    this.finalStatus = finalStatus;
  }

  public String getFinalTrackingUrl() {
    return this.finalTrackingUrl;
  }

  public FinalApplicationStatus getFinalApplicationStatus() {
    return this.finalStatus;
  }

}
