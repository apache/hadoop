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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptReportProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationAttemptStateProto;

import com.google.protobuf.TextFormat;

public class ApplicationAttemptReportPBImpl extends ApplicationAttemptReport {
  ApplicationAttemptReportProto proto = ApplicationAttemptReportProto
    .getDefaultInstance();
  ApplicationAttemptReportProto.Builder builder = null;
  boolean viaProto = false;

  private ApplicationAttemptId ApplicationAttemptId;
  private ContainerId amContainerId;

  public ApplicationAttemptReportPBImpl() {
    builder = ApplicationAttemptReportProto.newBuilder();
  }

  public ApplicationAttemptReportPBImpl(ApplicationAttemptReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    if (this.ApplicationAttemptId != null) {
      return this.ApplicationAttemptId;
    }

    ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationAttemptId()) {
      return null;
    }
    this.ApplicationAttemptId =
        convertFromProtoFormat(p.getApplicationAttemptId());
    return this.ApplicationAttemptId;
  }

  @Override
  public String getHost() {
    ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHost()) {
      return null;
    }
    return p.getHost();
  }

  @Override
  public int getRpcPort() {
    ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getRpcPort();
  }

  @Override
  public String getTrackingUrl() {
    ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTrackingUrl()) {
      return null;
    }
    return p.getTrackingUrl();
  }

  @Override
  public String getOriginalTrackingUrl() {
    ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasOriginalTrackingUrl()) {
      return null;
    }
    return p.getOriginalTrackingUrl();
  }

  @Override
  public String getDiagnostics() {
    ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnostics()) {
      return null;
    }
    return p.getDiagnostics();
  }

  @Override
  public YarnApplicationAttemptState getYarnApplicationAttemptState() {
    ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
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

  private YarnApplicationAttemptStateProto convertToProtoFormat(
      YarnApplicationAttemptState state) {
    return ProtoUtils.convertToProtoFormat(state);
  }

  private YarnApplicationAttemptState convertFromProtoFormat(
      YarnApplicationAttemptStateProto yarnApplicationAttemptState) {
    return ProtoUtils.convertFromProtoFormat(yarnApplicationAttemptState);
  }

  @Override
  public void
      setApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
    maybeInitBuilder();
    if (applicationAttemptId == null)
      builder.clearApplicationAttemptId();
    this.ApplicationAttemptId = applicationAttemptId;
  }

  @Override
  public void setHost(String host) {
    maybeInitBuilder();
    if (host == null) {
      builder.clearHost();
      return;
    }
    builder.setHost(host);
  }

  @Override
  public void setRpcPort(int rpcPort) {
    maybeInitBuilder();
    builder.setRpcPort(rpcPort);
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
  public void setOriginalTrackingUrl(String oUrl) {
    maybeInitBuilder();
    if (oUrl == null) {
      builder.clearOriginalTrackingUrl();
      return;
    }
    builder.setOriginalTrackingUrl(oUrl);
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

  public ApplicationAttemptReportProto getProto() {
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

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationAttemptReportProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.ApplicationAttemptId != null
        && !((ApplicationAttemptIdPBImpl) this.ApplicationAttemptId).getProto()
          .equals(builder.getApplicationAttemptId())) {
      builder
        .setApplicationAttemptId(convertToProtoFormat(this.ApplicationAttemptId));
    }

    if (this.amContainerId != null
        && !((ContainerIdPBImpl) this.amContainerId).getProto().equals(
          builder.getAmContainerId())) {
      builder.setAmContainerId(convertToProtoFormat(this.amContainerId));
    }
  }

  private ContainerIdProto convertToProtoFormat(ContainerId amContainerId) {
    return ((ContainerIdPBImpl) amContainerId).getProto();
  }

  private ContainerIdPBImpl convertFromProtoFormat(
      ContainerIdProto amContainerId) {
    return new ContainerIdPBImpl(amContainerId);
  }

  private ApplicationAttemptIdProto
      convertToProtoFormat(ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl) t).getProto();
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(
      ApplicationAttemptIdProto applicationAttemptId) {
    return new ApplicationAttemptIdPBImpl(applicationAttemptId);
  }

  @Override
  public ContainerId getAMContainerId() {
    if (this.amContainerId != null) {
      return this.amContainerId;
    }

    ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAmContainerId()) {
      return null;
    }
    this.amContainerId = convertFromProtoFormat(p.getAmContainerId());
    return this.amContainerId;
  }

  @Override
  public void setAMContainerId(ContainerId amContainerId) {
    maybeInitBuilder();
    if (amContainerId == null)
      builder.clearAmContainerId();
    this.amContainerId = amContainerId;
  }
}
