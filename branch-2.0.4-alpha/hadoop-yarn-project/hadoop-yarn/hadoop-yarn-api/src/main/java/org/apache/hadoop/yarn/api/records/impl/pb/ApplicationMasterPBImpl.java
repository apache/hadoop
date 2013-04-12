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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ClientToken;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationMasterProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationMasterProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationStateProto;
import org.apache.hadoop.yarn.util.ProtoUtils;

public class ApplicationMasterPBImpl extends ProtoBase<ApplicationMasterProto>
    implements ApplicationMaster {
  ApplicationMasterProto proto = ApplicationMasterProto.getDefaultInstance();
  ApplicationMasterProto.Builder builder = null;
  boolean viaProto = false;

  private ApplicationId applicationId = null;
  private ApplicationStatus applicationStatus = null;
  private ClientToken clientToken = null;

  public ApplicationMasterPBImpl() {
    builder = ApplicationMasterProto.newBuilder();
  }

  public ApplicationMasterPBImpl(ApplicationMasterProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ApplicationMasterProto getProto() {

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

    if (this.applicationStatus != null
        && !((ApplicationStatusPBImpl) this.applicationStatus).getProto()
          .equals(builder.getStatus())) {
      builder.setStatus(convertToProtoFormat(this.applicationStatus));
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
      builder = ApplicationMasterProto.newBuilder(proto);
    }
    viaProto = false;
  }


  @Override
  public YarnApplicationState getState() {
    ApplicationMasterProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState()) {
      return null;
    }
    return convertFromProtoFormat(p.getState());
  }

  @Override
  public void setState(YarnApplicationState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearState();
      return;
    }
    builder.setState(convertToProtoFormat(state));
  }
  @Override
  public String getHost() {
    ApplicationMasterProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHost()) {
      return null;
    }
    return (p.getHost());
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
  public ApplicationId getApplicationId() {
    ApplicationMasterProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationId != null) {
      return applicationId;
    } // Else via proto
    if (!p.hasApplicationId()) {
      return null;
    }
    applicationId = convertFromProtoFormat(p.getApplicationId());

    return applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null)
      builder.clearApplicationId();
    this.applicationId = applicationId;

  }
  @Override
  public int getRpcPort() {
    ApplicationMasterProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getRpcPort());
  }

  @Override
  public void setRpcPort(int rpcPort) {
    maybeInitBuilder();
    builder.setRpcPort((rpcPort));
  }
  @Override
  public String getTrackingUrl() {
    ApplicationMasterProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getTrackingUrl());
  }

  @Override
  public void setTrackingUrl(String url) {
    maybeInitBuilder();
    builder.setTrackingUrl(url);
  }
  @Override
  public ApplicationStatus getStatus() {
    ApplicationMasterProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationStatus != null) {
      return this.applicationStatus;
    } // Else via proto
    if (!p.hasStatus()) {
      return null;
    }
    this.applicationStatus = convertFromProtoFormat(p.getStatus());

    return this.applicationStatus;
  }

  @Override
  public void setStatus(ApplicationStatus status) {
    maybeInitBuilder();
    if (status == null)
      builder.clearStatus();
    this.applicationStatus = status;

  }

  @Override
  public ClientToken getClientToken() {
    ApplicationMasterProtoOrBuilder p = viaProto ? proto : builder;
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
  public void setClientToken(ClientToken clientToken) {
    maybeInitBuilder();
    if (clientToken == null) 
      builder.clearClientToken();
    this.clientToken = clientToken;
  }

  @Override
  public int getAMFailCount() {
    ApplicationMasterProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getAmFailCount());
  }

  @Override
  public int getContainerCount() {
    ApplicationMasterProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getContainerCount());
  }

  @Override
  public void setAMFailCount(int amFailCount) {
    maybeInitBuilder();
    builder.setAmFailCount(amFailCount);
  }

  @Override
  public void setContainerCount(int containerCount) {
    maybeInitBuilder();
    builder.setContainerCount(containerCount);
  }

  @Override
  public String getDiagnostics() {
    ApplicationMasterProtoOrBuilder p = viaProto ? proto : builder;
    return p.getDiagnostics();
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

  private YarnApplicationStateProto convertToProtoFormat(YarnApplicationState e) {
    return ProtoUtils.convertToProtoFormat(e);
  }

  private YarnApplicationState convertFromProtoFormat(YarnApplicationStateProto e) {
    return ProtoUtils.convertFromProtoFormat(e);
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }

  private ApplicationStatusPBImpl convertFromProtoFormat(ApplicationStatusProto p) {
    return new ApplicationStatusPBImpl(p);
  }

  private ApplicationStatusProto convertToProtoFormat(ApplicationStatus t) {
    return ((ApplicationStatusPBImpl)t).getProto();
  }

  private ClientTokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new ClientTokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(ClientToken t) {
    return ((ClientTokenPBImpl)t).getProto();
  }
}
