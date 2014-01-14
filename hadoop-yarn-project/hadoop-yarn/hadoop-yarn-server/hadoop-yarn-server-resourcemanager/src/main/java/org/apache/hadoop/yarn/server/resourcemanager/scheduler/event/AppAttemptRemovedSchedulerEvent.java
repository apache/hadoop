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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;

public class AppAttemptRemovedSchedulerEvent extends SchedulerEvent {

  private final ApplicationAttemptId applicationAttemptId;
  private final RMAppAttemptState finalAttemptState;
  private final boolean keepContainersAcrossAppAttempts;

  public AppAttemptRemovedSchedulerEvent(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState finalAttemptState, boolean keepContainers) {
    super(SchedulerEventType.APP_ATTEMPT_REMOVED);
    this.applicationAttemptId = applicationAttemptId;
    this.finalAttemptState = finalAttemptState;
    this.keepContainersAcrossAppAttempts = keepContainers;
  }

  public ApplicationAttemptId getApplicationAttemptID() {
    return this.applicationAttemptId;
  }

  public RMAppAttemptState getFinalAttemptState() {
    return this.finalAttemptState;
  }

  public boolean getKeepContainersAcrossAppAttempts() {
    return this.keepContainersAcrossAppAttempts;
  }
}
