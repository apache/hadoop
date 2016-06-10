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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;


import org.apache.hadoop.yarn.api.protocolrecords.impl.pb
    .RegisterApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords
    .DistSchedRegisterResponse;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DistSchedRegisterResponsePBImpl extends DistSchedRegisterResponse {

  YarnServerCommonServiceProtos.DistSchedRegisterResponseProto proto =
      YarnServerCommonServiceProtos.DistSchedRegisterResponseProto.getDefaultInstance();
  YarnServerCommonServiceProtos.DistSchedRegisterResponseProto.Builder builder = null;
  boolean viaProto = false;

  private Resource maxAllocatableCapability;
  private Resource minAllocatableCapability;
  private Resource incrAllocatableCapability;
  private List<NodeId> nodesForScheduling;
  private RegisterApplicationMasterResponse registerApplicationMasterResponse;

  public DistSchedRegisterResponsePBImpl() {
    builder = YarnServerCommonServiceProtos.DistSchedRegisterResponseProto.newBuilder();
  }

  public DistSchedRegisterResponsePBImpl(YarnServerCommonServiceProtos.DistSchedRegisterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public YarnServerCommonServiceProtos.DistSchedRegisterResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnServerCommonServiceProtos.DistSchedRegisterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.nodesForScheduling != null) {
      builder.clearNodesForScheduling();
      Iterable<YarnProtos.NodeIdProto> iterable =
          getNodeIdProtoIterable(this.nodesForScheduling);
      builder.addAllNodesForScheduling(iterable);
    }
    if (this.maxAllocatableCapability != null) {
      builder.setMaxAllocCapability(
          ProtoUtils.convertToProtoFormat(this.maxAllocatableCapability));
    }
    if (this.minAllocatableCapability != null) {
      builder.setMinAllocCapability(
          ProtoUtils.convertToProtoFormat(this.minAllocatableCapability));
    }
    if (this.incrAllocatableCapability != null) {
      builder.setIncrAllocCapability(
          ProtoUtils.convertToProtoFormat(this.incrAllocatableCapability));
    }
    if (this.registerApplicationMasterResponse != null) {
      builder.setRegisterResponse(
          ((RegisterApplicationMasterResponsePBImpl)
              this.registerApplicationMasterResponse).getProto());
    }
  }

  @Override
  public void setRegisterResponse(RegisterApplicationMasterResponse resp) {
    maybeInitBuilder();
    if(registerApplicationMasterResponse == null) {
      builder.clearRegisterResponse();
    }
    this.registerApplicationMasterResponse = resp;
  }

  @Override
  public RegisterApplicationMasterResponse getRegisterResponse() {
    if (this.registerApplicationMasterResponse != null) {
      return this.registerApplicationMasterResponse;
    }

    YarnServerCommonServiceProtos.DistSchedRegisterResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasRegisterResponse()) {
      return null;
    }

    this.registerApplicationMasterResponse =
        new RegisterApplicationMasterResponsePBImpl(p.getRegisterResponse());
    return this.registerApplicationMasterResponse;
  }

  @Override
  public void setMaxAllocatableCapabilty(Resource maxResource) {
    maybeInitBuilder();
    if(maxAllocatableCapability == null) {
      builder.clearMaxAllocCapability();
    }
    this.maxAllocatableCapability = maxResource;
  }

  @Override
  public Resource getMaxAllocatableCapabilty() {
    if (this.maxAllocatableCapability != null) {
      return this.maxAllocatableCapability;
    }

    YarnServerCommonServiceProtos.DistSchedRegisterResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMaxAllocCapability()) {
      return null;
    }

    this.maxAllocatableCapability =
        ProtoUtils.convertFromProtoFormat(p.getMaxAllocCapability());
    return this.maxAllocatableCapability;
  }

  @Override
  public void setMinAllocatableCapabilty(Resource minResource) {
    maybeInitBuilder();
    if(minAllocatableCapability == null) {
      builder.clearMinAllocCapability();
    }
    this.minAllocatableCapability = minResource;
  }

  @Override
  public Resource getMinAllocatableCapabilty() {
    if (this.minAllocatableCapability != null) {
      return this.minAllocatableCapability;
    }

    YarnServerCommonServiceProtos.DistSchedRegisterResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMinAllocCapability()) {
      return null;
    }

    this.minAllocatableCapability =
        ProtoUtils.convertFromProtoFormat(p.getMinAllocCapability());
    return this.minAllocatableCapability;
  }

  @Override
  public void setIncrAllocatableCapabilty(Resource incrResource) {
    maybeInitBuilder();
    if(incrAllocatableCapability == null) {
      builder.clearIncrAllocCapability();
    }
    this.incrAllocatableCapability = incrResource;
  }

  @Override
  public Resource getIncrAllocatableCapabilty() {
    if (this.incrAllocatableCapability != null) {
      return this.incrAllocatableCapability;
    }

    YarnServerCommonServiceProtos.DistSchedRegisterResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasIncrAllocCapability()) {
      return null;
    }

    this.incrAllocatableCapability =
        ProtoUtils.convertFromProtoFormat(p.getIncrAllocCapability());
    return this.incrAllocatableCapability;
  }

  @Override
  public void setContainerTokenExpiryInterval(int interval) {
    maybeInitBuilder();
    builder.setContainerTokenExpiryInterval(interval);
  }

  @Override
  public int getContainerTokenExpiryInterval() {
    YarnServerCommonServiceProtos.DistSchedRegisterResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerTokenExpiryInterval()) {
      return 0;
    }
    return p.getContainerTokenExpiryInterval();
  }

  @Override
  public void setContainerIdStart(long containerIdStart) {
    maybeInitBuilder();
    builder.setContainerIdStart(containerIdStart);
  }

  @Override
  public long getContainerIdStart() {
    YarnServerCommonServiceProtos.DistSchedRegisterResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerIdStart()) {
      return 0;
    }
    return p.getContainerIdStart();
  }


  @Override
  public void setNodesForScheduling(List<NodeId> nodesForScheduling) {
    maybeInitBuilder();
    if (nodesForScheduling == null || nodesForScheduling.isEmpty()) {
      if (this.nodesForScheduling != null) {
        this.nodesForScheduling.clear();
      }
      builder.clearNodesForScheduling();
      return;
    }
    this.nodesForScheduling = new ArrayList<>();
    this.nodesForScheduling.addAll(nodesForScheduling);
  }

  @Override
  public List<NodeId> getNodesForScheduling() {
    if (nodesForScheduling != null) {
      return nodesForScheduling;
    }
    initLocalNodesForSchedulingList();
    return nodesForScheduling;
  }

  private synchronized void initLocalNodesForSchedulingList() {
    YarnServerCommonServiceProtos.DistSchedRegisterResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<YarnProtos.NodeIdProto> list = p.getNodesForSchedulingList();
    nodesForScheduling = new ArrayList<>();
    if (list != null) {
      for (YarnProtos.NodeIdProto t : list) {
        nodesForScheduling.add(ProtoUtils.convertFromProtoFormat(t));
      }
    }
  }
  private synchronized Iterable<YarnProtos.NodeIdProto> getNodeIdProtoIterable(
      final List<NodeId> nodeList) {
    maybeInitBuilder();
    return new Iterable<YarnProtos.NodeIdProto>() {
      @Override
      public synchronized Iterator<YarnProtos.NodeIdProto> iterator() {
        return new Iterator<YarnProtos.NodeIdProto>() {

          Iterator<NodeId> iter = nodeList.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public YarnProtos.NodeIdProto next() {
            return ProtoUtils.convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}
