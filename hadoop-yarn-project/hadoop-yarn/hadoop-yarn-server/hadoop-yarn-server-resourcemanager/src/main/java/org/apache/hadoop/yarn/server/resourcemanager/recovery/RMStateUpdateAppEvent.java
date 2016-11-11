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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;

import com.google.common.util.concurrent.SettableFuture;

public class RMStateUpdateAppEvent extends RMStateStoreEvent {
  private final ApplicationStateData appState;
  // After application state is updated in state store,
  // should notify back to application or not
  private boolean notifyApplication;
  private SettableFuture<Object> future;

  public RMStateUpdateAppEvent(ApplicationStateData appState) {
    super(RMStateStoreEventType.UPDATE_APP);
    this.appState = appState;
    this.notifyApplication = true;
    this.future = null;
  }

  public RMStateUpdateAppEvent(ApplicationStateData appState, boolean notifyApp,
      SettableFuture<Object> future) {
    super(RMStateStoreEventType.UPDATE_APP);
    this.appState = appState;
    this.notifyApplication = notifyApp;
    this.future = future;
  }

  public ApplicationStateData getAppState() {
    return appState;
  }

  public boolean isNotifyApplication() {
    return notifyApplication;
  }

  public SettableFuture<Object> getResult() {
    return future;
  }
}
