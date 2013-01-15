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

import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ClientToken;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationResourceUsageReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationStateProto;
import org.apache.hadoop.yarn.util.ProtoUtils;

public class ApplicationReportPBImpl extends ProtoBase<ApplicationReportProto>
implements ApplicationReport {
  ApplicationReportProto proto = ApplicationReportProto.getDefaultInstance();
  ApplicationReportProto.Builder builder = null;
  boolean viaProto = false;

  private ApplicationId applicationId;
  private ApplicationAttemptId currentApplicationAttemptId;
  private ClientToken clientToken = null;

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
  public ClientToken getClientToken() {
    ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.clientToken != null) {
      return this.clientToken;
    }
    if (!p.hasClientToken()) {
      return null;
    }
    this.clientToken = convertFromProtoFormat(p.getClientToken());
    return this.clientToken;
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
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null)
      builder.clearStatus();
    this.applicationId = applicationId;
  }

  @Override
  public void setCurrentApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
    maybeInitBuilder();
    if (applicationId == null)
      builder.clearStatus();
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
  public void setClientToken(ClientToken clientToken) {
    maybeInitBuilder();
    if (clientToken == null) 
      builder.clearClientToken();
    this.clientToken = clientToken;
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
  public ApplicationReportProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
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
    if (this.clientToken != null
        && !((ClientTokenPBImpl) this.clientToken).getProto().equals(
            builder.getClientToken())) {
      builder.setClientToken(convertToProtoFormat(this.clientToken));
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

  private ClientTokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new ClientTokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(ClientToken t) {
    return ((ClientTokenPBImpl)t).getProto();
  }
}
