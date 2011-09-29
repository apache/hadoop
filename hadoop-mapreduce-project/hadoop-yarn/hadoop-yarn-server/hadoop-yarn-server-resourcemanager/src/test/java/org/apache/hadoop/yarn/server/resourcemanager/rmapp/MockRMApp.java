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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;

public class MockRMApp implements RMApp {
  static final int DT = 1000000; // ms

  String user = MockApps.newUserName();
  String name = MockApps.newAppName();
  String queue = MockApps.newQueue();
  long start = System.currentTimeMillis() - (int) (Math.random() * DT);
  long finish = 0;
  RMAppState state = RMAppState.NEW;
  int failCount = 0;
  ApplicationId id;
  String url = null;
  StringBuilder diagnostics = new StringBuilder();
  RMAppAttempt attempt;

  public MockRMApp(int newid, long time, RMAppState newState) {
    finish = time;
    id = MockApps.newAppID(newid);
    state = newState;
  }

  public MockRMApp(int newid, long time, RMAppState newState, String userName) {
    this(newid, time, newState);
    user = userName;
  }

  public MockRMApp(int newid, long time, RMAppState newState, String userName, String diag) {
    this(newid, time, newState, userName);
    this.diagnostics = new StringBuilder(diag);
  }

  @Override
  public ApplicationId getApplicationId() {
    return id;
  }

  @Override
  public RMAppState getState() {
    return state;
  }

  public void setState(RMAppState state) {
    this.state = state;
  }

  @Override
  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  @Override
  public float getProgress() {
    return (float) 0.0;
  }

  @Override
  public RMAppAttempt getRMAppAttempt(ApplicationAttemptId appAttemptId) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  @Override
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public RMAppAttempt getCurrentAppAttempt() {
    return attempt;
  }

  public void setCurrentAppAttempt(RMAppAttempt attempt) {
    this.attempt = attempt;
  }

  @Override
  public ApplicationReport createAndGetApplicationReport() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public ApplicationStore getApplicationStore() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long getFinishTime() {
    return finish;
  }

  public void setFinishTime(long time) {
    this.finish = time;
  }

  @Override
  public long getStartTime() {
    return start;
  }

  public void setStartTime(long time) {
    this.start = time;
  }

  @Override
  public String getTrackingUrl() {
    return url;
  }

  public void setTrackingUrl(String url) {
    this.url = url;
  }

  @Override
  public StringBuilder getDiagnostics() {
    return diagnostics;
  }

  public void setDiagnostics(String diag) {
    this.diagnostics  = new StringBuilder(diag);
  }

  public void handle(RMAppEvent event) {
  }

  @Override
  public String getAMFinalState() {
    return "UNKNOWN";
  };

}
