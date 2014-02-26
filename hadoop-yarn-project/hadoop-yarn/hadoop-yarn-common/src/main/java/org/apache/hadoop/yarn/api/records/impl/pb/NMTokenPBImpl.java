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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.NMTokenProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.NMTokenProtoOrBuilder;

@Private
@Unstable
public class NMTokenPBImpl extends NMToken{
  
  NMTokenProto proto = NMTokenProto.getDefaultInstance();
  NMTokenProto.Builder builder = null;
  boolean viaProto = false;

  private Token token = null;
  private NodeId nodeId = null;

  public NMTokenPBImpl() {
    builder = NMTokenProto.newBuilder();
  }
  
  public NMTokenPBImpl(NMTokenProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized NodeId getNodeId() {
    NMTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeId != null) {
      return nodeId;
    }
    if (!p.hasNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getNodeId()); 
    return nodeId;
  }

  @Override
  public synchronized void setNodeId(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null) {
      builder.clearNodeId();
    }
    this.nodeId = nodeId;
  }

  @Override
  public synchronized Token getToken() {
    NMTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (this.token != null) {
      return this.token;
    }
    if (!p.hasToken()) {
      return null;
    }
    this.token = convertFromProtoFormat(p.getToken()); 
    return token;
  }

  @Override
  public synchronized void setToken(Token token) {
    maybeInitBuilder();
    if (token == null) {
      builder.clearToken();
    }
    this.token = token;
  }

  public synchronized NMTokenProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.nodeId != null) {
      builder.setNodeId(convertToProtoFormat(nodeId));
    }
    if (this.token != null) {
      builder.setToken(convertToProtoFormat(token));
    }
  }
  
  private synchronized void maybeInitBuilder() {
    if(viaProto || builder == null) {
      builder = NMTokenProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  private synchronized NodeId convertFromProtoFormat(NodeIdProto p) {
    return new NodeIdPBImpl(p);
  }
  
  private synchronized NodeIdProto convertToProtoFormat(NodeId nodeId) {
    return ((NodeIdPBImpl)nodeId).getProto();
  }
  
  private synchronized TokenProto convertToProtoFormat(Token token) {
    return ((TokenPBImpl)token).getProto();
  }
  
  private synchronized Token convertFromProtoFormat(TokenProto proto) {
    return new TokenPBImpl(proto);
  }
}
