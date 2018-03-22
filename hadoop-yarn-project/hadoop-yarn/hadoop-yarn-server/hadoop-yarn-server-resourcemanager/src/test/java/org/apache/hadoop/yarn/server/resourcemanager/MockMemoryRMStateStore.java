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
package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;

import com.google.common.annotations.VisibleForTesting;

/**
 * Test helper for MemoryRMStateStore will make sure the event.
 */
public class MockMemoryRMStateStore extends MemoryRMStateStore {

  private Map<ApplicationId, ApplicationSubmissionContext> appSubCtxtCopy =
      new HashMap<ApplicationId, ApplicationSubmissionContext>();

  @SuppressWarnings("rawtypes")
  @Override
  protected EventHandler getRMStateStoreEventHandler() {
    return rmStateStoreEventHandler;
  }

  @Override
  public synchronized RMState loadState() throws Exception {

    RMState cloneState = super.loadState();

    for(Entry<ApplicationId, ApplicationStateData> state :
        cloneState.getApplicationState().entrySet()) {
      ApplicationStateData oldStateData = state.getValue();
      oldStateData.setApplicationSubmissionContext(
          this.appSubCtxtCopy.get(state.getKey()));
      cloneState.getApplicationState().put(state.getKey(), oldStateData);
    }
    return cloneState;
  }

  @Override
  public synchronized void storeApplicationStateInternal(
      ApplicationId appId, ApplicationStateData appState)
      throws Exception {
    // Clone Application Submission Context
    this.cloneAppSubmissionContext(appState);
    super.storeApplicationStateInternal(appId, appState);
  }

  @Override
  public synchronized void updateApplicationStateInternal(
      ApplicationId appId, ApplicationStateData appState)
      throws Exception {
    // Clone Application Submission Context
    this.cloneAppSubmissionContext(appState);
    super.updateApplicationStateInternal(appId, appState);
  }

  /**
   * Clone Application Submission Context and Store in Map for
   * later use.
   *
   * @param appState
   */
  private void cloneAppSubmissionContext(ApplicationStateData appState) {
    ApplicationSubmissionContext oldAppSubCtxt =
        appState.getApplicationSubmissionContext();
    ApplicationSubmissionContext context =
        ApplicationSubmissionContext.newInstance(
            oldAppSubCtxt.getApplicationId(),
            oldAppSubCtxt.getApplicationName(),
            oldAppSubCtxt.getQueue(),
            oldAppSubCtxt.getPriority(),
            oldAppSubCtxt.getAMContainerSpec(),
            oldAppSubCtxt.getUnmanagedAM(),
            oldAppSubCtxt.getCancelTokensWhenComplete(),
            oldAppSubCtxt.getMaxAppAttempts(),
            oldAppSubCtxt.getResource()
            );
    context.setAttemptFailuresValidityInterval(
        oldAppSubCtxt.getAttemptFailuresValidityInterval());
    context.setKeepContainersAcrossApplicationAttempts(
        oldAppSubCtxt.getKeepContainersAcrossApplicationAttempts());
    context.setAMContainerResourceRequests(
        oldAppSubCtxt.getAMContainerResourceRequests());
    context.setLogAggregationContext(oldAppSubCtxt.getLogAggregationContext());
    context.setApplicationType(oldAppSubCtxt.getApplicationType());
    this.appSubCtxtCopy.put(oldAppSubCtxt.getApplicationId(), context);
  }

  /**
   * Traverse each app state and replace cloned app sub context
   * into the state.
   *
   * @param actualState
   * @return actualState
   */
  @VisibleForTesting
  public RMState reloadStateWithClonedAppSubCtxt(RMState actualState) {
    for(Entry<ApplicationId, ApplicationStateData> state :
        actualState.getApplicationState().entrySet()) {
      ApplicationStateData oldStateData = state.getValue();
      oldStateData.setApplicationSubmissionContext(
          this.appSubCtxtCopy.get(state.getKey()));
      actualState.getApplicationState().put(state.getKey(),
          oldStateData);
    }
    return actualState;
  }
}