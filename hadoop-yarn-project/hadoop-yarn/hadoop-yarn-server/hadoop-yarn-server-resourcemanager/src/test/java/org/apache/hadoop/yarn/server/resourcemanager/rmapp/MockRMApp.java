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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeUpdateType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

public class MockRMApp implements RMApp {
  static final int DT = 1000000; // ms

  String user = MockApps.newUserName();
  String name = MockApps.newAppName();
  String queue = MockApps.newQueue();
  long start = System.currentTimeMillis() - (int) (Math.random() * DT);
  long submit = start - (int) (Math.random() * DT);
  long finish = 0;
  RMAppState state = RMAppState.NEW;
  int failCount = 0;
  ApplicationId id;
  String url = null;
  String oUrl = null;
  StringBuilder diagnostics = new StringBuilder();
  RMAppAttempt attempt;
  int maxAppAttempts = 1;
  List<ResourceRequest> amReqs;

  public MockRMApp(int newid, long time, RMAppState newState) {
    finish = time;
    id = MockApps.newAppID(newid);
    state = newState;
    amReqs = Collections.singletonList(ResourceRequest.newInstance(
        Priority.UNDEFINED, "0.0.0.0", Resource.newInstance(0, 0), 1));
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
  public ApplicationSubmissionContext getApplicationSubmissionContext() {
    return new ApplicationSubmissionContextPBImpl();
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
  public Map<ApplicationAttemptId, RMAppAttempt> getAppAttempts() {
    Map<ApplicationAttemptId, RMAppAttempt> attempts =
      new LinkedHashMap<ApplicationAttemptId, RMAppAttempt>();
    if(attempt != null) {
      attempts.put(attempt.getAppAttemptId(), attempt);
    }
    return attempts;
  }

  @Override
  public RMAppAttempt getCurrentAppAttempt() {
    return attempt;
  }

  public void setCurrentAppAttempt(RMAppAttempt attempt) {
    this.attempt = attempt;
  }

  @Override
  public ApplicationReport createAndGetApplicationReport(
      String clientUserName, boolean allowAccess) {
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

  @Override
  public long getSubmitTime() {
    return submit;
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
  public String getOriginalTrackingUrl() {
    return oUrl;
  }

  public void setOriginalTrackingUrl(String oUrl) {
    this.oUrl = oUrl;
  }

  @Override
  public StringBuilder getDiagnostics() {
    return diagnostics;
  }

  public void setDiagnostics(String diag) {
    this.diagnostics  = new StringBuilder(diag);
  }

  @Override
  public int getMaxAppAttempts() {
    return maxAppAttempts;
  }

  public void setNumMaxRetries(int maxAppAttempts) {
    this.maxAppAttempts = maxAppAttempts;
  }

  @Override
  public void handle(RMAppEvent event) {
  }

  @Override
  public FinalApplicationStatus getFinalApplicationStatus() {
    return FinalApplicationStatus.UNDEFINED;
  }

  @Override
  public int pullRMNodeUpdates(Map<RMNode, NodeUpdateType> updatedNodes) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getApplicationType() {
    return YarnConfiguration.DEFAULT_APPLICATION_TYPE;
  }

  @Override
  public Set<String> getApplicationTags() {
    return null;
  }

  @Override
  public boolean isAppFinalStateStored() {
    return true;
  }

  @Override
  public YarnApplicationState createApplicationState() {
    return null;
  }

  @Override
  public Set<NodeId> getRanNodes() {
    return null;
  }
  
  public Resource getResourcePreempted() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public RMAppMetrics getRMAppMetrics() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public ReservationId getReservationId() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  @Override
  public List<ResourceRequest> getAMResourceRequests() {
    return this.amReqs;
  }

  @Override
  public Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public LogAggregationStatus getLogAggregationStatusForAppReport() {
    return null;
  }

  @Override
  public String getAmNodeLabelExpression() {
    return null;
  }

  @Override
  public String getAppNodeLabelExpression() {
    return null;
  }

  public CallerContext getCallerContext() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public AppCollectorData getCollectorData() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<ApplicationTimeoutType, Long> getApplicationTimeouts() {
    return Collections.emptyMap();
  }

  @Override
  public Priority getApplicationPriority() {
    return null;
  }

  @Override
  public boolean isAppInCompletedStates() {
    return false;
  }

  @Override
  public CollectorInfo getCollectorInfo() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
