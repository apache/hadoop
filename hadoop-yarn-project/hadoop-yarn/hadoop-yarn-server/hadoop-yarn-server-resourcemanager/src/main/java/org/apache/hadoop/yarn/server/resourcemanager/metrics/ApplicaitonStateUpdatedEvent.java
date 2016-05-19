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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/**
 * When the state of this application has been changed, RM would sent
 * this event to inform Timeline Server for keeping the Application state
 * consistent.
 */
public class ApplicaitonStateUpdatedEvent extends SystemMetricsEvent{
  private ApplicationId appId;
  private YarnApplicationState appState;

  public ApplicaitonStateUpdatedEvent(ApplicationId appliocationId,
      YarnApplicationState state, long updatedTime) {
    super(SystemMetricsEventType.APP_STATE_UPDATED, updatedTime);
    this.appId = appliocationId;
    this.appState = state;
  }

  public ApplicationId getApplicationId() {
    return appId;
  }

  public YarnApplicationState getAppState() {
    return appState;
  }
}
