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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;

public class AppRemovedSchedulerEvent extends SchedulerEvent {

  private final ApplicationId applicationId;
  private final RMAppState finalState;
  private final FinalApplicationStatus finalApplicationStatus;

  public AppRemovedSchedulerEvent(ApplicationId applicationId,
      RMAppState finalState, FinalApplicationStatus finalApplicationStatus) {
    super(SchedulerEventType.APP_REMOVED);
    this.applicationId = applicationId;
    this.finalState = finalState;
    this.finalApplicationStatus = finalApplicationStatus;
  }

  public ApplicationId getApplicationID() {
    return this.applicationId;
  }

  public RMAppState getFinalState() {
    return this.finalState;
  }

  public FinalApplicationStatus getFinalApplicationStatus() {
    return finalApplicationStatus;
  }
}
