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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationAttemptStateProto;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptFinishData;

import com.google.protobuf.TextFormat;

public class ApplicationAttemptFinishDataPBImpl extends
    ApplicationAttemptFinishData {

  ApplicationAttemptFinishDataProto proto = ApplicationAttemptFinishDataProto
    .getDefaultInstance();
  ApplicationAttemptFinishDataProto.Builder builder = null;
  boolean viaProto = false;

  public ApplicationAttemptFinishDataPBImpl() {
    builder = ApplicationAttemptFinishDataProto.newBuilder();
  }

  public ApplicationAttemptFinishDataPBImpl(
      ApplicationAttemptFinishDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private ApplicationAttemptId applicationAttemptId;

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    if (this.applicationAttemptId != null) {
      return this.applicationAttemptId;
    }
    ApplicationAttemptFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationAttemptId()) {
      return null;
    }
    this.applicationAttemptId =
        convertFromProtoFormat(p.getApplicationAttemptId());
    return this.applicationAttemptId;
  }

  @Override
  public void
      setApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
    maybeInitBuilder();
    if (applicationAttemptId == null) {
      builder.clearApplicationAttemptId();
    }
    this.applicationAttemptId = applicationAttemptId;
  }

  @Override
  public String getTrackingURL() {
    ApplicationAttemptFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTrackingUrl()) {
      return null;
    }
    return p.getTrackingUrl();
  }

  @Override
  public void setTrackingURL(String trackingURL) {
    maybeInitBuilder();
    if (trackingURL == null) {
      builder.clearTrackingUrl();
      return;
    }
    builder.setTrackingUrl(trackingURL);
  }

  @Override
  public String getDiagnosticsInfo() {
    ApplicationAttemptFinishDataProtoOrBuilder p = viaProto ? proto : builder;
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
    ApplicationAttemptFinishDataProtoOrBuilder p = viaProto ? proto : builder;
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
  public YarnApplicationAttemptState getYarnApplicationAttemptState() {
    ApplicationAttemptFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasYarnApplicationAttemptState()) {
      return null;
    }
    return convertFromProtoFormat(p.getYarnApplicationAttemptState());
  }

  @Override
  public void setYarnApplicationAttemptState(YarnApplicationAttemptState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearYarnApplicationAttemptState();
      return;
    }
    builder.setYarnApplicationAttemptState(convertToProtoFormat(state));
  }

  public ApplicationAttemptFinishDataProto getProto() {
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
    if (this.applicationAttemptId != null
        && !((ApplicationAttemptIdPBImpl) this.applicationAttemptId).getProto()
          .equals(builder.getApplicationAttemptId())) {
      builder
        .setApplicationAttemptId(convertToProtoFormat(this.applicationAttemptId));
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
      builder = ApplicationAttemptFinishDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(
      ApplicationAttemptIdProto applicationAttemptId) {
    return new ApplicationAttemptIdPBImpl(applicationAttemptId);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(
      ApplicationAttemptId applicationAttemptId) {
    return ((ApplicationAttemptIdPBImpl) applicationAttemptId).getProto();
  }

  private FinalApplicationStatus convertFromProtoFormat(
      FinalApplicationStatusProto finalApplicationStatus) {
    return ProtoUtils.convertFromProtoFormat(finalApplicationStatus);
  }

  private FinalApplicationStatusProto convertToProtoFormat(
      FinalApplicationStatus finalApplicationStatus) {
    return ProtoUtils.convertToProtoFormat(finalApplicationStatus);
  }

  private YarnApplicationAttemptStateProto convertToProtoFormat(
      YarnApplicationAttemptState state) {
    return ProtoUtils.convertToProtoFormat(state);
  }

  private YarnApplicationAttemptState convertFromProtoFormat(
      YarnApplicationAttemptStateProto yarnApplicationAttemptState) {
    return ProtoUtils.convertFromProtoFormat(yarnApplicationAttemptState);
  }

}
