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

import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProtoOrBuilder;

public class NodeLabelPBImpl extends NodeLabel {
  NodeLabelProto proto = 
      NodeLabelProto.getDefaultInstance();
  NodeLabelProto.Builder builder = null;
  boolean viaProto = false;
  
  public NodeLabelPBImpl() {
    builder = NodeLabelProto.newBuilder();
  }

  public NodeLabelPBImpl(NodeLabelProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public NodeLabelProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    proto = builder.build();
    viaProto = true;
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
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeLabelProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public String getName() {
    NodeLabelProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasName()) {
      return null;
    }
    return (p.getName());
  }

  @Override
  public void setName(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearName();
      return;
    }
    builder.setName(name);
  }

  @Override
  public boolean isExclusive() {
    NodeLabelProtoOrBuilder p = viaProto ? proto : builder;
    return p.getIsExclusive();
  }

  @Override
  public void setExclusivity(boolean isExclusive) {
    maybeInitBuilder();
    builder.setIsExclusive(isExclusive);
  }

}
