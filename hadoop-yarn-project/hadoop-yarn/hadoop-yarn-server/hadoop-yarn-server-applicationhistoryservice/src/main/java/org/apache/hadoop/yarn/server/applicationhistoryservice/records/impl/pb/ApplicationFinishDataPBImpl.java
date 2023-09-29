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

package org.apache.hadoop.yarn.server.applicationhistoryservice.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationFinishDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationFinishDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationStateProto;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationFinishData;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

public class ApplicationFinishDataPBImpl extends ApplicationFinishData {

  ApplicationFinishDataProto proto = ApplicationFinishDataProto
    .getDefaultInstance();
  ApplicationFinishDataProto.Builder builder = null;
  boolean viaProto = false;

  private ApplicationId applicationId;

  public ApplicationFinishDataPBImpl() {
    builder = ApplicationFinishDataProto.newBuilder();
  }

  public ApplicationFinishDataPBImpl(ApplicationFinishDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ApplicationId getApplicationId() {
    if (this.applicationId != null) {
      return this.applicationId;
    }
    ApplicationFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null) {
      builder.clearApplicationId();
    }
    this.applicationId = applicationId;
  }

  @Override
  public long getFinishTime() {
    ApplicationFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getFinishTime();
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime(finishTime);
  }

  @Override
  public String getDiagnosticsInfo() {
    ApplicationFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnosticsInfo()) {
      return null;
    }
    return p.getDiagnosticsInfo();
  }

  @Override
  public void setDiagnosticsInfo(String diagnosticsInfo) {
    maybeInitBuilder();
    if (diagnosticsInfo == null) {
      builder.clearDiagnosticsInfo();
      return;
    }
    builder.setDiagnosticsInfo(diagnosticsInfo);
  }

  @Override
  public FinalApplicationStatus getFinalApplicationStatus() {
    ApplicationFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasFinalApplicationStatus()) {
      return null;
    }
    return convertFromProtoFormat(p.getFinalApplicationStatus());
  }

  @Override
  public void setFinalApplicationStatus(
      FinalApplicationStatus finalApplicationStatus) {
    maybeInitBuilder();
    if (finalApplicationStatus == null) {
      builder.clearFinalApplicationStatus();
      return;
    }
    builder
      .setFinalApplicationStatus(convertToProtoFormat(finalApplicationStatus));
  }

  @Override
  public YarnApplicationState getYarnApplicationState() {
    ApplicationFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasYarnApplicationState()) {
      return null;
    }
    return convertFromProtoFormat(p.getYarnApplicationState());
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

  public ApplicationFinishDataProto getProto() {
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
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationFinishDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId applicationId) {
    return ((ApplicationIdPBImpl) applicationId).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(
      ApplicationIdProto applicationId) {
    return new ApplicationIdPBImpl(applicationId);
  }

  private FinalApplicationStatus convertFromProtoFormat(
      FinalApplicationStatusProto finalApplicationStatus) {
    return ProtoUtils.convertFromProtoFormat(finalApplicationStatus);
  }

  private FinalApplicationStatusProto convertToProtoFormat(
      FinalApplicationStatus finalApplicationStatus) {
    return ProtoUtils.convertToProtoFormat(finalApplicationStatus);
  }

  private YarnApplicationStateProto convertToProtoFormat(
      YarnApplicationState state) {
    return ProtoUtils.convertToProtoFormat(state);
  }

  private YarnApplicationState convertFromProtoFormat(
      YarnApplicationStateProto yarnApplicationState) {
    return ProtoUtils.convertFromProtoFormat(yarnApplicationState);
  }

}
