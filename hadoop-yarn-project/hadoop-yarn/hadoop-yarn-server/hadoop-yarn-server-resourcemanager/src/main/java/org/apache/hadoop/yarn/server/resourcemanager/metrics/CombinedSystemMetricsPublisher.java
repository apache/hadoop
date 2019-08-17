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

import java.util.Collection;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * A metrics publisher that can publish for a collection of publishers.
 */
public class CombinedSystemMetricsPublisher implements SystemMetricsPublisher {
  private Collection<SystemMetricsPublisher> publishers;

  public CombinedSystemMetricsPublisher(Collection<SystemMetricsPublisher>
      publishers) {
    this.publishers = publishers;
  }

  @Override
  public void appCreated(RMApp app, long createdTime) {
    for (SystemMetricsPublisher publisher : this.publishers) {
      publisher.appCreated(app, createdTime);
    }
  }

  @Override public void appLaunched(RMApp app, long launchTime) {
    for (SystemMetricsPublisher publisher : this.publishers) {
      publisher.appLaunched(app, launchTime);
    }
  }

  @Override
  public void appACLsUpdated(RMApp app, String appViewACLs, long updatedTime) {
    for (SystemMetricsPublisher publisher : this.publishers) {
      publisher.appACLsUpdated(app, appViewACLs, updatedTime);
    }
  }

  @Override
  public void appUpdated(RMApp app, long updatedTime) {
    for (SystemMetricsPublisher publisher : this.publishers) {
      publisher.appUpdated(app, updatedTime);
    }
  }

  @Override
  public void appStateUpdated(RMApp app, YarnApplicationState appState,
      long updatedTime) {
    for (SystemMetricsPublisher publisher : this.publishers) {
      publisher.appStateUpdated(app, appState, updatedTime);
    }
  }

  @Override
  public void appFinished(RMApp app, RMAppState state, long finishedTime) {
    for (SystemMetricsPublisher publisher : this.publishers) {
      publisher.appFinished(app, state, finishedTime);
    }
  }

  @Override
  public void appAttemptRegistered(RMAppAttempt appAttempt,
      long registeredTime) {
    for (SystemMetricsPublisher publisher : this.publishers) {
      publisher.appAttemptRegistered(appAttempt, registeredTime);
    }
  }

  @Override
  public void appAttemptFinished(RMAppAttempt appAttempt,
      RMAppAttemptState appAttemtpState, RMApp app, long finishedTime) {
    for (SystemMetricsPublisher publisher : this.publishers) {
      publisher.appAttemptFinished(appAttempt, appAttemtpState, app,
          finishedTime);
    }
  }

  @Override
  public void containerCreated(RMContainer container, long createdTime) {
    for (SystemMetricsPublisher publisher : this.publishers) {
      publisher.containerCreated(container, createdTime);
    }
  }

  @Override
  public void containerFinished(RMContainer container, long finishedTime) {
    for (SystemMetricsPublisher publisher : this.publishers) {
      publisher.containerFinished(container, finishedTime);
    }
  }
}

