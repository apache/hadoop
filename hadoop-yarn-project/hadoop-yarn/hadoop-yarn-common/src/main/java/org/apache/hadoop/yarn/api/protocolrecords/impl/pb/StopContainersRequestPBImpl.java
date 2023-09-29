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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainersRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainersRequestProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class StopContainersRequestPBImpl extends StopContainersRequest {
  StopContainersRequestProto proto = StopContainersRequestProto
    .getDefaultInstance();
  StopContainersRequestProto.Builder builder = null;
  boolean viaProto = false;

  private List<ContainerId> containerIds = null;

  public StopContainersRequestPBImpl() {
    builder = StopContainersRequestProto.newBuilder();
  }

  public StopContainersRequestPBImpl(StopContainersRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public StopContainersRequestProto getProto() {
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
    if (this.containerIds != null) {
      addLocalContainerIdsToProto();
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
      builder = StopContainersRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addLocalContainerIdsToProto() {
    maybeInitBuilder();
    builder.clearContainerId();
    if (this.containerIds == null)
      return;
    List<ContainerIdProto> protoList = new ArrayList<ContainerIdProto>();
    for (ContainerId id : containerIds) {
      protoList.add(convertToProtoFormat(id));
    }
    builder.addAllContainerId(protoList);
  }

  private void initLocalContainerIds() {
    if (this.containerIds != null) {
      return;
    }
    StopContainersRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerIdProto> containerIds = p.getContainerIdList();
    this.containerIds = new ArrayList<ContainerId>();
    for (ContainerIdProto id : containerIds) {
      this.containerIds.add(convertFromProtoFormat(id));
    }
  }

  @Override
  public List<ContainerId> getContainerIds() {
    initLocalContainerIds();
    return this.containerIds;
  }

  @Override
  public void setContainerIds(List<ContainerId> containerIds) {
    maybeInitBuilder();
    if (containerIds == null)
      builder.clearContainerId();
    this.containerIds = containerIds;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }
}
