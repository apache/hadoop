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

import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/**
 * Interface used to publish app/container events to timelineservice.
 */
public interface SystemMetricsPublisher {

  void appCreated(RMApp app, long createdTime);

  void appLaunched(RMApp app, long launchTime);

  void appACLsUpdated(RMApp app, String appViewACLs, long updatedTime);

  void appUpdated(RMApp app, long updatedTime);

  void appStateUpdated(RMApp app, YarnApplicationState appState,
      long updatedTime);

  void appFinished(RMApp app, RMAppState state, long finishedTime);

  void appAttemptRegistered(RMAppAttempt appAttempt, long registeredTime);

  void appAttemptFinished(RMAppAttempt appAttempt,
      RMAppAttemptState appAttemtpState, RMApp app, long finishedTime);

  void containerCreated(RMContainer container, long createdTime);

  void containerFinished(RMContainer container, long finishedTime);
}
