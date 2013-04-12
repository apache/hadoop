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


import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.HeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.impl.pb.HeartbeatResponsePBImpl;


    
public class NodeHeartbeatResponsePBImpl extends ProtoBase<NodeHeartbeatResponseProto> implements NodeHeartbeatResponse {
  NodeHeartbeatResponseProto proto = NodeHeartbeatResponseProto.getDefaultInstance();
  NodeHeartbeatResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private HeartbeatResponse heartbeatResponse = null;
  
  
  public NodeHeartbeatResponsePBImpl() {
    builder = NodeHeartbeatResponseProto.newBuilder();
  }

  public NodeHeartbeatResponsePBImpl(NodeHeartbeatResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public NodeHeartbeatResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.heartbeatResponse != null) {
      builder.setHeartbeatResponse(convertToProtoFormat(this.heartbeatResponse));
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
      builder = NodeHeartbeatResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public HeartbeatResponse getHeartbeatResponse() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.heartbeatResponse != null) {
      return this.heartbeatResponse;
    }
    if (!p.hasHeartbeatResponse()) {
      return null;
    }
    this.heartbeatResponse = convertFromProtoFormat(p.getHeartbeatResponse());
    return this.heartbeatResponse;
  }

  @Override
  public void setHeartbeatResponse(HeartbeatResponse heartbeatResponse) {
    maybeInitBuilder();
    if (heartbeatResponse == null) 
      builder.clearHeartbeatResponse();
    this.heartbeatResponse = heartbeatResponse;
  }

  private HeartbeatResponsePBImpl convertFromProtoFormat(HeartbeatResponseProto p) {
    return new HeartbeatResponsePBImpl(p);
  }

  private HeartbeatResponseProto convertToProtoFormat(HeartbeatResponse t) {
    return ((HeartbeatResponsePBImpl)t).getProto();
  }



}  
