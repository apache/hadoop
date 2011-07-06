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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationState;

public class AMFinishEvent extends ApplicationEvent {
  private final ApplicationState finalState;
  private final String trackingUrl;
  private final String diagnostics;

  public AMFinishEvent(ApplicationId applicationId,
      ApplicationState finalState, String trackingUrl, String diagnostics) {
    super(ApplicationEventType.FINISH, applicationId);
    this.finalState = finalState;
    this.trackingUrl = trackingUrl;
    this.diagnostics = diagnostics;
  }

  public ApplicationState getFinalApplicationState() {
    return this.finalState;
  }

  public String getTrackingUrl() {
    return this.trackingUrl;
  }

  public String getDiagnostics() {
    return this.diagnostics;
  }
}