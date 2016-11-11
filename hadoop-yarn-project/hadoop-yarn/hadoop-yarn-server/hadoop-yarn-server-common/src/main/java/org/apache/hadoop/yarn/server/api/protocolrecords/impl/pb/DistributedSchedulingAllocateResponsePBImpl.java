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

import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RemoteNodeProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;


import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link DistributedSchedulingAllocateResponse}.
 */
public class DistributedSchedulingAllocateResponsePBImpl extends
    DistributedSchedulingAllocateResponse {

  YarnServerCommonServiceProtos.DistributedSchedulingAllocateResponseProto
      proto = YarnServerCommonServiceProtos.
          DistributedSchedulingAllocateResponseProto.getDefaultInstance();
  YarnServerCommonServiceProtos.DistributedSchedulingAllocateResponseProto.
      Builder builder = null;
  boolean viaProto = false;

  private AllocateResponse allocateResponse;
  private List<RemoteNode> nodesForScheduling;

  public DistributedSchedulingAllocateResponsePBImpl() {
    builder = YarnServerCommonServiceProtos.
        DistributedSchedulingAllocateResponseProto.newBuilder();
  }

  public DistributedSchedulingAllocateResponsePBImpl(
      YarnServerCommonServiceProtos.
      DistributedSchedulingAllocateResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public YarnServerCommonServiceProtos.
      DistributedSchedulingAllocateResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnServerCommonServiceProtos.
          DistributedSchedulingAllocateResponseProto.newBuilder(proto);
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
      Iterable<YarnServerCommonServiceProtos.RemoteNodeProto> iterable =
          getNodeIdProtoIterable(this.nodesForScheduling);
      builder.addAllNodesForScheduling(iterable);
    }
    if (this.allocateResponse != null) {
      builder.setAllocateResponse(
          ((AllocateResponsePBImpl) this.allocateResponse).getProto());
    }
  }

  @Override
  public void setAllocateResponse(AllocateResponse response) {
    maybeInitBuilder();
    if (allocateResponse == null) {
      builder.clearAllocateResponse();
    }
    this.allocateResponse = response;
  }

  @Override
  public AllocateResponse getAllocateResponse() {
    if (this.allocateResponse != null) {
      return this.allocateResponse;
    }

    YarnServerCommonServiceProtos.
        DistributedSchedulingAllocateResponseProtoOrBuilder p =
            viaProto ? proto : builder;
    if (!p.hasAllocateResponse()) {
      return null;
    }

    this.allocateResponse = new AllocateResponsePBImpl(p.getAllocateResponse());
    return this.allocateResponse;
  }

  @Override
  public void setNodesForScheduling(List<RemoteNode> nodesForScheduling) {
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
  public List<RemoteNode> getNodesForScheduling() {
    if (nodesForScheduling != null) {
      return nodesForScheduling;
    }
    initLocalNodesForSchedulingList();
    return nodesForScheduling;
  }

  private synchronized void initLocalNodesForSchedulingList() {
    YarnServerCommonServiceProtos.
        DistributedSchedulingAllocateResponseProtoOrBuilder p =
            viaProto ? proto : builder;
    List<YarnServerCommonServiceProtos.RemoteNodeProto> list =
        p.getNodesForSchedulingList();
    nodesForScheduling = new ArrayList<>();
    if (list != null) {
      for (YarnServerCommonServiceProtos.RemoteNodeProto t : list) {
        nodesForScheduling.add(new RemoteNodePBImpl(t));
      }
    }
  }

  private synchronized Iterable<RemoteNodeProto> getNodeIdProtoIterable(
      final List<RemoteNode> nodeList) {
    maybeInitBuilder();
    return new Iterable<RemoteNodeProto>() {
      @Override
      public synchronized Iterator<RemoteNodeProto> iterator() {
        return new Iterator<RemoteNodeProto>() {

          Iterator<RemoteNode> iter = nodeList.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public RemoteNodeProto next() {
            return ((RemoteNodePBImpl)iter.next()).getProto();
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
