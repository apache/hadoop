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
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProtoOrBuilder;


    
public class NodeIdPBImpl extends ProtoBase<NodeIdProto> implements NodeId {
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
  
  public NodeIdProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  @Override
  public String getHost() {
    NodeIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getHost());
  }

  @Override
  public void setHost(String host) {
    maybeInitBuilder();
    builder.setHost((host));
  }

  @Override
  public int getPort() {
    NodeIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getPort());
  }

  @Override
  public void setPort(int port) {
    maybeInitBuilder();
    builder.setPort((port));
  }

  @Override
  public String toString() {
    return this.getHost() + ":" + this.getPort();
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    String host = this.getHost();
    result = prime * result + ((host == null) ? 0 : host.hashCode());
    result = prime * result + this.getPort();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    NodeIdPBImpl other = (NodeIdPBImpl) obj;
    String host = this.getHost();
    String otherHost = other.getHost();
    if (host == null) {
      if (otherHost != null)
        return false;
    } else if (!host.equals(otherHost))
      return false;
    if (this.getPort() != other.getPort())
      return false;
    return true;
  }

  @Override
  public int compareTo(NodeId other) {
    int hostCompare = this.getHost().compareTo(other.getHost());
    if (hostCompare == 0) {
      if (this.getPort() > other.getPort()) {
        return 1;
      } else if (this.getPort() < other.getPort()) {
        return -1;
      }
      return 0;
    }
    return hostCompare;
  }

}  
