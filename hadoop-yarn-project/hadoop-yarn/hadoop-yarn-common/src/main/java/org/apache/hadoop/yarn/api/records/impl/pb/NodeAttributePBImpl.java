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

import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeTypeProto;

public class NodeAttributePBImpl extends NodeAttribute {
  private NodeAttributeProto proto = NodeAttributeProto.getDefaultInstance();
  private NodeAttributeProto.Builder builder = null;
  private boolean viaProto = false;

  public NodeAttributePBImpl() {
    builder = NodeAttributeProto.newBuilder();
  }

  public NodeAttributePBImpl(NodeAttributeProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public NodeAttributeProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeAttributeProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getAttributeName() {
    NodeAttributeProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAttributeName()) {
      return null;
    }
    return p.getAttributeName();
  }

  @Override
  public void setAttributeName(String attributeName) {
    maybeInitBuilder();
    if(attributeName == null) {
      builder.clearAttributeName();
      return;
    }
    builder.setAttributeName(attributeName);
  }

  @Override
  public String getAttributeValue() {
    NodeAttributeProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAttributeValue()) {
      return null;
    }
    return p.getAttributeValue();
  }

  @Override
  public void setAttributeValue(String attributeValue) {
    maybeInitBuilder();
    if(attributeValue == null) {
      builder.clearAttributeValue();
      return;
    }
    builder.setAttributeValue(attributeValue);
  }

  @Override
  public NodeAttributeType getAttributeType() {
    NodeAttributeProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAttributeType()) {
      return null;
    }
    return convertFromProtoFormat(p.getAttributeType());
  }

  @Override
  public void setAttributeType(NodeAttributeType attributeType) {
    maybeInitBuilder();
    if (attributeType == null) {
      builder.clearAttributeType();
      return;
    }
    builder.setAttributeType(convertToProtoFormat(attributeType));
  }

  private NodeAttributeTypeProto convertToProtoFormat(
      NodeAttributeType attributeType) {
    return NodeAttributeTypeProto.valueOf(attributeType.name());
  }

  private NodeAttributeType convertFromProtoFormat(
      NodeAttributeTypeProto containerState) {
    return NodeAttributeType.valueOf(containerState.name());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((getAttributePrefix() == null) ? 0
        : getAttributePrefix().hashCode());
    result = prime * result
        + ((getAttributeName() == null) ? 0 : getAttributeName().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (obj instanceof NodeAttribute) {
      NodeAttribute other = (NodeAttribute) obj;
      if (!compare(getAttributePrefix(), other.getAttributePrefix())) {
        return false;
      }
      if (!compare(getAttributeName(), other.getAttributeName())) {
        return false;
      }
      return true;
    }
    return false;
  }

  private static boolean compare(Object left, Object right) {
    if (left == null) {
      return right == null;
    } else {
      return left.equals(right);
    }
  }

  @Override
  public String getAttributePrefix() {
    NodeAttributeProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAttributePrefix()) {
      return null;
    }
    return p.getAttributePrefix();
  }

  @Override
  public void setAttributePrefix(String attributePrefix) {
    maybeInitBuilder();
    if(attributePrefix == null) {
      builder.clearAttributePrefix();
      return;
    }
    builder.setAttributePrefix(attributePrefix);
  }

  @Override
  public String toString() {
    return "Prefix-" + getAttributePrefix() + " :Name-" + getAttributeName()
        + ":Value-" + getAttributeValue() + ":Type-" + getAttributeType();
  }
}
