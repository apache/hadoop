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


import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProtoOrBuilder;


    
public class NodeIdPBImpl extends NodeId {
  NodeIdProto proto = NodeIdProto.getDefaultInstance();
  NodeIdProto.Builder builder = null;
  boolean viaProto = false;
  
  public NodeIdPBImpl() {
    builder = NodeIdProto.newBuilder();
  }

  public NodeIdPBImpl(NodeIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized NodeIdProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  @Override
  public synchronized String getHost() {
    NodeIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getHost());
  }

  @Override
  public synchronized void setHost(String host) {
    maybeInitBuilder();
    builder.setHost((host));
  }

  @Override
  public synchronized int getPort() {
    NodeIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getPort());
  }

  @Override
  public synchronized void setPort(int port) {
    maybeInitBuilder();
    builder.setPort((port));
  }
}  
