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

package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationTimeout;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.proto.YarnProtos.AppTimeoutsMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationResourceUsageReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationTimeoutProto;
import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationStateProto;

import com.google.protobuf.TextFormat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Private
@Unstable
public class ApplicationReportPBImpl extends ApplicationReport {
  ApplicationReportProto proto = ApplicationReportProto.getDefaultInstance();
  ApplicationReportProto.Builder builder = null;
  boolean viaProto = false;

  private ApplicationId applicationId;
  private ApplicationAttemptId currentApplicationAttemptId;
  private Token clientToAMToken = null;
  private Token amRmToken = null;
  private Set<String> applicationTags = null;
  private Priority priority = null;
  private Map<ApplicationTimeoutType, ApplicationTimeout> applicationTimeouts = null;

  public ApplicationReportPBImpl() {
    builder = ApplicationReportProto.newBuilder();
  }

  public ApplicationReportPBImpl(ApplicationReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ApplicationId getApplicationId() {
    if (this.applicationId != null) {
      return this.applicationId;
    }

    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }

  public void setApplicationResourceUsageReport(ApplicationResourceUsageReport appInfo) {
    maybeInitBuilder();
    if (appInfo == null) {
      builder.clearAppResourceUsage();
      return;
    }
    builder.setAppResourceUsage(convertToProtoFormat(appInfo));
  }
  
  @Override
  public ApplicationAttemptId getCurrentApplicationAttemptId() {
    if (this.currentApplicationAttemptId != null) {
      return this.currentApplicationAttemptId;
    }

    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasCurrentApplicationAttemptId()) {
      return null;
    }
    this.currentApplicationAttemptId = convertFromProtoFormat(p.getCurrentApplicationAttemptId());
    return this.currentApplicationAttemptId;
  }

  @Override
  public ApplicationResourceUsageReport getApplicationResourceUsageReport() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAppResourceUsage()) {
      return null;
    }
    return convertFromProtoFormat(p.getAppResourceUsage());
  }

  @Override
  public String getTrackingUrl() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTrackingUrl()) {
      return null;
    }
    return p.getTrackingUrl();
  }

  @Override
  public String getOriginalTrackingUrl() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasOriginalTrackingUrl()) {
      return null;
    }
    return p.getOriginalTrackingUrl();
  }
  
  @Override
  public String getName() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasName()) {
      return null;
    }
    return p.getName();
  }

  @Override
  public String getQueue() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasQueue()) {
      return null;
    }
    return p.getQueue();
  }

  @Override
  public YarnApplicationState getYarnApplicationState() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasYarnApplicationState()) {
      return null;
    }
    return convertFromProtoFormat(p.getYarnApplicationState());
  }

  @Override
  public String getHost() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHost()) {
      return null;
    }
    return (p.getHost());
  }

  @Override
  public int getRpcPort() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getRpcPort());
  }

  @Override
  public Token getClientToAMToken() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.clientToAMToken != null) {
      return this.clientToAMToken;
    }
    if (!p.hasClientToAmToken()) {
      return null;
    }
    this.clientToAMToken = convertFromProtoFormat(p.getClientToAmToken());
    return this.clientToAMToken;
  }

  @Override
  public String getUser() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUser()) {
      return null;
    }
    return p.getUser();
  }

  @Override
  public String getDiagnostics() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnostics()) {
      return null;
    }
    return p.getDiagnostics();
  }

  @Override
  public long getStartTime() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getStartTime();
  }

  @Override
  public long getLaunchTime() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getLaunchTime();
  }

  @Override
  public void setLaunchTime(long launchTime) {
    maybeInitBuilder();
    builder.setLaunchTime(launchTime);
  }
  @Override
  public long getFinishTime() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getFinishTime();
  }

  @Override
  public FinalApplicationStatus getFinalApplicationStatus() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasFinalApplicationStatus()) {
      return null;
    }	
    return convertFromProtoFormat(p.getFinalApplicationStatus());
  }

  @Override
  public float getProgress() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getProgress();
  }

  @Override
  public String getApplicationType() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationType()) {
      return null;
    }
    return p.getApplicationType();
  }

  @Override
  public Token getAMRMToken() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (amRmToken != null) {
      return amRmToken;
    }
    if (!p.hasAmRmToken()) {
      return null;
    }
    amRmToken = convertFromProtoFormat(p.getAmRmToken());
    return amRmToken;
  }

  private void initApplicationTags() {
    if (this.applicationTags != null) {
      return;
    }
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    this.applicationTags = new HashSet<String>();
    this.applicationTags.addAll(p.getApplicationTagsList());
  }

  @Override
  public Set<String> getApplicationTags() {
    initApplicationTags();
    return this.applicationTags;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null)
      builder.clearApplicationId();
    this.applicationId = applicationId;
  }

  @Override
  public void setCurrentApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
    maybeInitBuilder();
    if (applicationAttemptId == null)
      builder.clearCurrentApplicationAttemptId();
    this.currentApplicationAttemptId = applicationAttemptId;
  }

  @Override
  public void setTrackingUrl(String url) {
    maybeInitBuilder();
    if (url == null) {
      builder.clearTrackingUrl();
      return;
    }
    builder.setTrackingUrl(url);
  }
  
  @Override
  public void setOriginalTrackingUrl(String url) {
    maybeInitBuilder();
    if (url == null) {
      builder.clearOriginalTrackingUrl();
      return;
    }
    builder.setOriginalTrackingUrl(url);
  }

  @Override
  public void setName(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearName();
      return;
    }
    builder.setName(name);
  }

  @Override
  public void setQueue(String queue) {
    maybeInitBuilder();
    if (queue == null) {
      builder.clearQueue();
      return;
    }
    builder.setQueue(queue);
  }

  @Override
  public void setYarnApplicationState(YarnApplicationState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearYarnApplicationState();
      return;
    }
    builder.setYarnApplicationState(convertToProtoFormat(state));
  }

  @Override
  public void setHost(String host) {
    maybeInitBuilder();
    if (host == null) {
      builder.clearHost();
      return;
    }
    builder.setHost((host));
  }

  @Override
  public void setRpcPort(int rpcPort) {
    maybeInitBuilder();
    builder.setRpcPort((rpcPort));
  }

  @Override
  public void setClientToAMToken(Token clientToAMToken) {
    maybeInitBuilder();
    if (clientToAMToken == null) 
      builder.clearClientToAmToken();
    this.clientToAMToken = clientToAMToken;
  }

  @Override
  public void setUser(String user) {
    maybeInitBuilder();
    if (user == null) {
      builder.clearUser();
      return;
    }
    builder.setUser((user));
  }
  
  @Override
  public void setApplicationType(String applicationType) {
    maybeInitBuilder();
    if (applicationType == null) {
      builder.clearApplicationType();
      return;
    }
    builder.setApplicationType((applicationType));
  }

  @Override
  public void setApplicationTags(Set<String> tags) {
    maybeInitBuilder();
    if (tags == null || tags.isEmpty()) {
      builder.clearApplicationTags();
    }
    this.applicationTags = tags;
  }

  @Override
  public void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    if (diagnostics == null) {
      builder.clearDiagnostics();
      return;
    }
    builder.setDiagnostics(diagnostics);
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime(startTime);
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime(finishTime);
  }

  @Override
  public void setFinalApplicationStatus(FinalApplicationStatus finishState) {
    maybeInitBuilder();
    if (finishState == null) {
      builder.clearFinalApplicationStatus();
      return;
    }
    builder.setFinalApplicationStatus(convertToProtoFormat(finishState));
  }

  @Override
  public void setProgress(float progress) {
    maybeInitBuilder();
    builder.setProgress(progress);
  }

  @Override
  public void setAMRMToken(Token amRmToken) {
    maybeInitBuilder();
    if (amRmToken == null) {
      builder.clearAmRmToken();
    }
    this.amRmToken = amRmToken;
  }

  public ApplicationReportProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null
        && !((ApplicationIdPBImpl) this.applicationId).getProto().equals(
            builder.getApplicationId())) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
    if (this.currentApplicationAttemptId != null
        && !((ApplicationAttemptIdPBImpl) this.currentApplicationAttemptId).getProto().equals(
            builder.getCurrentApplicationAttemptId())) {
      builder.setCurrentApplicationAttemptId(convertToProtoFormat(this.currentApplicationAttemptId));
    }
    if (this.clientToAMToken != null
        && !((TokenPBImpl) this.clientToAMToken).getProto().equals(
            builder.getClientToAmToken())) {
      builder.setClientToAmToken(convertToProtoFormat(this.clientToAMToken));
    }
    if (this.amRmToken != null
      && !((TokenPBImpl) this.amRmToken).getProto().equals(
      builder.getAmRmToken())) {
      builder.setAmRmToken(convertToProtoFormat(this.amRmToken));
    }
    if (this.applicationTags != null && !this.applicationTags.isEmpty()) {
      builder.clearApplicationTags();
      builder.addAllApplicationTags(this.applicationTags);
    }
    if (this.priority != null
        && !((PriorityPBImpl) this.priority).getProto().equals(
            builder.getPriority())) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
    if (this.applicationTimeouts != null) {
      addApplicationTimeouts();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationReportProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }
  
  private ApplicationAttemptIdProto convertToProtoFormat(ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl) t).getProto();
  }

  private ApplicationResourceUsageReport convertFromProtoFormat(ApplicationResourceUsageReportProto s) {
    return ProtoUtils.convertFromProtoFormat(s);
  }

  private ApplicationResourceUsageReportProto convertToProtoFormat(ApplicationResourceUsageReport s) {
    return ProtoUtils.convertToProtoFormat(s);
  }

  private ApplicationIdPBImpl convertFromProtoFormat(
      ApplicationIdProto applicationId) {
    return new ApplicationIdPBImpl(applicationId);
  }
  
  private ApplicationAttemptIdPBImpl convertFromProtoFormat(
      ApplicationAttemptIdProto applicationAttemptId) {
    return new ApplicationAttemptIdPBImpl(applicationAttemptId);
  }

  private YarnApplicationState convertFromProtoFormat(YarnApplicationStateProto s) {
    return ProtoUtils.convertFromProtoFormat(s);
  }

  private YarnApplicationStateProto convertToProtoFormat(YarnApplicationState s) {
    return ProtoUtils.convertToProtoFormat(s);
  }

  private FinalApplicationStatus convertFromProtoFormat(FinalApplicationStatusProto s) {
    return ProtoUtils.convertFromProtoFormat(s);
  }

  private FinalApplicationStatusProto convertToProtoFormat(FinalApplicationStatus s) {
    return ProtoUtils.convertToProtoFormat(s);
  }

  private TokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl)t).getProto();
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl)t).getProto();
  }

  @Override
  public LogAggregationStatus getLogAggregationStatus() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasLogAggregationStatus()) {
      return null;
    }
    return convertFromProtoFormat(p.getLogAggregationStatus());
  }

  @Override
  public void setLogAggregationStatus(
      LogAggregationStatus logAggregationStatus) {
    maybeInitBuilder();
    if (logAggregationStatus == null) {
      builder.clearLogAggregationStatus();
      return;
    }
    builder.setLogAggregationStatus(
        convertToProtoFormat(logAggregationStatus));
  }

  private LogAggregationStatus convertFromProtoFormat(
      LogAggregationStatusProto s) {
    return ProtoUtils.convertFromProtoFormat(s);
  }

  private LogAggregationStatusProto
      convertToProtoFormat(LogAggregationStatus s) {
    return ProtoUtils.convertToProtoFormat(s);
  }

  @Override
  public boolean isUnmanagedApp() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getUnmanagedApplication();
  }

  @Override
  public void setUnmanagedApp(boolean unmanagedApplication) {
    maybeInitBuilder();
    builder.setUnmanagedApplication(unmanagedApplication);
  }

  @Override
  public Priority getPriority() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.priority != null) {
      return this.priority;
    }
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }

  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null)
      builder.clearPriority();
    this.priority = priority;
  }

  @Override
  public String getAppNodeLabelExpression() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAppNodeLabelExpression()) {
      return null;
    }
    return p.getAppNodeLabelExpression();
  }

  @Override
  public void setAppNodeLabelExpression(String appNodeLabelExpression) {
    maybeInitBuilder();
    if (appNodeLabelExpression == null) {
      builder.clearAppNodeLabelExpression();
      return;
    }
    builder.setAppNodeLabelExpression((appNodeLabelExpression));
  }

  @Override
  public String getAmNodeLabelExpression() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAmNodeLabelExpression()) {
      return null;
    }
    return p.getAmNodeLabelExpression();
  }

  @Override
  public void setAmNodeLabelExpression(String amNodeLabelExpression) {
    maybeInitBuilder();
    if (amNodeLabelExpression == null) {
      builder.clearAmNodeLabelExpression();
      return;
    }
    builder.setAmNodeLabelExpression((amNodeLabelExpression));
  }

  @Override
  public Map<ApplicationTimeoutType, ApplicationTimeout> getApplicationTimeouts() {
    initApplicationTimeout();
    return this.applicationTimeouts;
  }

  @Override
  public void setApplicationTimeouts(
      Map<ApplicationTimeoutType, ApplicationTimeout> timeouts) {
    if (timeouts == null) {
      return;
    }
    initApplicationTimeout();
    this.applicationTimeouts.clear();
    this.applicationTimeouts.putAll(timeouts);
  }

  private void initApplicationTimeout() {
    if (this.applicationTimeouts != null) {
      return;
    }
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    List<AppTimeoutsMapProto> lists = p.getAppTimeoutsList();
    this.applicationTimeouts =
        new HashMap<ApplicationTimeoutType, ApplicationTimeout>(lists.size());
    for (AppTimeoutsMapProto timeoutProto : lists) {
      this.applicationTimeouts.put(
          ProtoUtils
              .convertFromProtoFormat(timeoutProto.getApplicationTimeoutType()),
          convertFromProtoFormat(timeoutProto.getApplicationTimeout()));
    }
  }

  private ApplicationTimeoutPBImpl convertFromProtoFormat(
      ApplicationTimeoutProto p) {
    return new ApplicationTimeoutPBImpl(p);
  }

  private ApplicationTimeoutProto convertToProtoFormat(ApplicationTimeout t) {
    return ((ApplicationTimeoutPBImpl) t).getProto();
  }

  private void addApplicationTimeouts() {
    maybeInitBuilder();
    builder.clearAppTimeouts();
    if (applicationTimeouts == null) {
      return;
    }
    Iterable<? extends AppTimeoutsMapProto> values =
        new Iterable<AppTimeoutsMapProto>() {

          @Override
          public Iterator<AppTimeoutsMapProto> iterator() {
            return new Iterator<AppTimeoutsMapProto>() {
              private Iterator<ApplicationTimeoutType> iterator =
                  applicationTimeouts.keySet().iterator();

              @Override
              public boolean hasNext() {
                return iterator.hasNext();
              }

              @Override
              public AppTimeoutsMapProto next() {
                ApplicationTimeoutType key = iterator.next();
                return AppTimeoutsMapProto.newBuilder()
                    .setApplicationTimeout(
                        convertToProtoFormat(applicationTimeouts.get(key)))
                    .setApplicationTimeoutType(
                        ProtoUtils.convertToProtoFormat(key))
                    .build();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        };
    this.builder.addAllAppTimeouts(values);
  }

}
