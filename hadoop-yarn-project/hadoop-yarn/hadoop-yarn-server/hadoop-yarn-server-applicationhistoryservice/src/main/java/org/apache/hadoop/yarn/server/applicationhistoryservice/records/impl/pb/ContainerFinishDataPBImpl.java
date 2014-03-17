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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ContainerFinishDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ContainerFinishDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStateProto;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerFinishData;

import com.google.protobuf.TextFormat;

public class ContainerFinishDataPBImpl extends ContainerFinishData {

  ContainerFinishDataProto proto = ContainerFinishDataProto
    .getDefaultInstance();
  ContainerFinishDataProto.Builder builder = null;
  boolean viaProto = false;

  private ContainerId containerId;

  public ContainerFinishDataPBImpl() {
    builder = ContainerFinishDataProto.newBuilder();
  }

  public ContainerFinishDataPBImpl(ContainerFinishDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ContainerId getContainerId() {
    if (this.containerId != null) {
      return this.containerId;
    }
    ContainerFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId = convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) {
      builder.clearContainerId();
    }
    this.containerId = containerId;
  }

  @Override
  public long getFinishTime() {
    ContainerFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getFinishTime();
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime(finishTime);
  }

  @Override
  public String getDiagnosticsInfo() {
    ContainerFinishDataProtoOrBuilder p = viaProto ? proto : builder;
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
  public int getContainerExitStatus() {
    ContainerFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getContainerExitStatus();
  }

  @Override
  public ContainerState getContainerState() {
    ContainerFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerState()) {
      return null;
    }
    return convertFromProtoFormat(p.getContainerState());
  }

  @Override
  public void setContainerState(ContainerState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearContainerState();
      return;
    }
    builder.setContainerState(convertToProtoFormat(state));
  }

  @Override
  public void setContainerExitStatus(int containerExitStatus) {
    maybeInitBuilder();
    builder.setContainerExitStatus(containerExitStatus);
  }

  public ContainerFinishDataProto getProto() {
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
    if (this.containerId != null
        && !((ContainerIdPBImpl) this.containerId).getProto().equals(
          builder.getContainerId())) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
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
      builder = ContainerFinishDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private ContainerIdProto convertToProtoFormat(ContainerId containerId) {
    return ((ContainerIdPBImpl) containerId).getProto();
  }

  private ContainerIdPBImpl
      convertFromProtoFormat(ContainerIdProto containerId) {
    return new ContainerIdPBImpl(containerId);
  }

  private ContainerStateProto convertToProtoFormat(ContainerState state) {
    return ProtoUtils.convertToProtoFormat(state);
  }

  private ContainerState convertFromProtoFormat(
      ContainerStateProto containerState) {
    return ProtoUtils.convertFromProtoFormat(containerState);
  }

}
