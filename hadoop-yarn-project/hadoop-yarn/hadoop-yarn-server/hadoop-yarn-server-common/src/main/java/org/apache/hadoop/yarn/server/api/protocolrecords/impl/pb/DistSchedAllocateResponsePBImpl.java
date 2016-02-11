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

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;

import org.apache.hadoop.yarn.api.protocolrecords.impl.pb
    .AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords
    .DistSchedAllocateResponse;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DistSchedAllocateResponsePBImpl extends DistSchedAllocateResponse {

  YarnServerCommonServiceProtos.DistSchedAllocateResponseProto proto =
      YarnServerCommonServiceProtos.DistSchedAllocateResponseProto.getDefaultInstance();
  YarnServerCommonServiceProtos.DistSchedAllocateResponseProto.Builder builder = null;
  boolean viaProto = false;

  private AllocateResponse allocateResponse;
  private List<NodeId> nodesForScheduling;

  public DistSchedAllocateResponsePBImpl() {
    builder = YarnServerCommonServiceProtos.DistSchedAllocateResponseProto.newBuilder();
  }

  public DistSchedAllocateResponsePBImpl(YarnServerCommonServiceProtos.DistSchedAllocateResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public YarnServerCommonServiceProtos.DistSchedAllocateResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnServerCommonServiceProtos.DistSchedAllocateResponseProto.newBuilder(proto);
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
    if (this.allocateResponse != null) {
      builder.setAllocateResponse(
          ((AllocateResponsePBImpl)this.allocateResponse).getProto());
    }
  }
  @Override
  public void setAllocateResponse(AllocateResponse response) {
    maybeInitBuilder();
    if(allocateResponse == null) {
      builder.clearAllocateResponse();
    }
    this.allocateResponse = response;
  }

  @Override
  public AllocateResponse getAllocateResponse() {
    if (this.allocateResponse != null) {
      return this.allocateResponse;
    }

    YarnServerCommonServiceProtos.DistSchedAllocateResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasAllocateResponse()) {
      return null;
    }

    this.allocateResponse =
        new AllocateResponsePBImpl(p.getAllocateResponse());
    return this.allocateResponse;
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
    YarnServerCommonServiceProtos.DistSchedAllocateResponseProtoOrBuilder p =
        viaProto ? proto : builder;
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

}
