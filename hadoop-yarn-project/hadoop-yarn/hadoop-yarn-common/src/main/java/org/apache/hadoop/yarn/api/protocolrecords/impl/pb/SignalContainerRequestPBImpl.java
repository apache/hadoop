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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SignalContainerCommandProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SignalContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SignalContainerRequestProtoOrBuilder;


public class SignalContainerRequestPBImpl
    extends SignalContainerRequest {
  SignalContainerRequestProto proto =
      SignalContainerRequestProto.getDefaultInstance();
  SignalContainerRequestProto.Builder builder = null;
  boolean viaProto = false;

  private ContainerId containerId;
  private SignalContainerCommand command = null;

  private static SignalContainerCommand convertFromProtoFormat(
      SignalContainerCommandProto p) {
    return SignalContainerCommand.valueOf(p.name());
  }

  private static SignalContainerCommandProto convertToProtoFormat(
      SignalContainerCommand p) {
    return SignalContainerCommandProto.valueOf(p.name());
  }

  public SignalContainerRequestPBImpl() {
    builder = SignalContainerRequestProto.newBuilder();
  }

  public SignalContainerRequestPBImpl(SignalContainerRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public SignalContainerRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.containerId != null) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }

    if (this.command != null) {
      builder.setCommand(convertToProtoFormat(this.command));
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
      builder = SignalContainerRequestProto.newBuilder(proto);
    }
    viaProto = false;
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

  @Override
  public ContainerId getContainerId() {
    SignalContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerId != null) {
      return this.containerId;
    }
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

  private void initCommand() {
    if (this.command != null) {
      return;
    }
    SignalContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if(p.hasCommand()) {
      this.command = convertFromProtoFormat(p.getCommand());
    }
  }

  @Override
  public SignalContainerCommand getCommand() {
    initCommand();
    return command;
  }

  @Override
  public void setCommand(SignalContainerCommand command) {
    maybeInitBuilder();
    if (command == null) {
      builder.clearCommand();
    }
    this.command = command;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }

}