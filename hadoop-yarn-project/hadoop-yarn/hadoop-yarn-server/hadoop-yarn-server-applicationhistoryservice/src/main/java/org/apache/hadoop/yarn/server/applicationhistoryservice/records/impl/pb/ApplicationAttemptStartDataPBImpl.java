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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationAttemptStartDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationAttemptStartDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptStartData;

import com.google.protobuf.TextFormat;

public class ApplicationAttemptStartDataPBImpl extends
    ApplicationAttemptStartData {

  ApplicationAttemptStartDataProto proto = ApplicationAttemptStartDataProto
    .getDefaultInstance();
  ApplicationAttemptStartDataProto.Builder builder = null;
  boolean viaProto = false;

  public ApplicationAttemptStartDataPBImpl() {
    builder = ApplicationAttemptStartDataProto.newBuilder();
  }

  public ApplicationAttemptStartDataPBImpl(
      ApplicationAttemptStartDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private ApplicationAttemptId applicationAttemptId;
  private ContainerId masterContainerId;

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    if (this.applicationAttemptId != null) {
      return this.applicationAttemptId;
    }
    ApplicationAttemptStartDataProtoOrBuilder p = viaProto ? proto : builder;
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
  public String getHost() {
    ApplicationAttemptStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHost()) {
      return null;
    }
    return p.getHost();
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
  public int getRPCPort() {
    ApplicationAttemptStartDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getRpcPort();
  }

  @Override
  public void setRPCPort(int rpcPort) {
    maybeInitBuilder();
    builder.setRpcPort(rpcPort);
  }

  @Override
  public ContainerId getMasterContainerId() {
    if (this.masterContainerId != null) {
      return this.masterContainerId;
    }
    ApplicationAttemptStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationAttemptId()) {
      return null;
    }
    this.masterContainerId = convertFromProtoFormat(p.getMasterContainerId());
    return this.masterContainerId;
  }

  @Override
  public void setMasterContainerId(ContainerId masterContainerId) {
    maybeInitBuilder();
    if (masterContainerId == null) {
      builder.clearMasterContainerId();
    }
    this.masterContainerId = masterContainerId;
  }

  public ApplicationAttemptStartDataProto getProto() {
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
    if (this.masterContainerId != null
        && !((ContainerIdPBImpl) this.masterContainerId).getProto().equals(
          builder.getMasterContainerId())) {
      builder
        .setMasterContainerId(convertToProtoFormat(this.masterContainerId));
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
      builder = ApplicationAttemptStartDataProto.newBuilder(proto);
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

  private ContainerIdPBImpl
      convertFromProtoFormat(ContainerIdProto containerId) {
    return new ContainerIdPBImpl(containerId);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId masterContainerId) {
    return ((ContainerIdPBImpl) masterContainerId).getProto();
  }

}
