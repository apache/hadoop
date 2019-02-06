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

import org.apache.hadoop.yarn.api.records.NodeToAttributeValue;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeToAttributeValueProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeToAttributeValueProtoOrBuilder;

/**
 * PB Implementation for NodeToAttributeValue.
 *
 */
public class NodeToAttributeValuePBImpl extends NodeToAttributeValue {
  private NodeToAttributeValueProto proto =
      NodeToAttributeValueProto.getDefaultInstance();
  private NodeToAttributeValueProto.Builder builder = null;
  private boolean viaProto = false;

  public NodeToAttributeValuePBImpl() {
    builder = NodeToAttributeValueProto.newBuilder();
  }

  public NodeToAttributeValuePBImpl(NodeToAttributeValueProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public NodeToAttributeValueProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeToAttributeValueProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getAttributeValue() {
    NodeToAttributeValueProtoOrBuilder p = viaProto ? proto : builder;
    return p.getAttributeValue();
  }

  @Override
  public void setAttributeValue(String attributeValue) {
    maybeInitBuilder();
    if (attributeValue == null) {
      builder.clearAttributeValue();
      return;
    }
    builder.setAttributeValue(attributeValue);
  }

  @Override
  public String getHostname() {
    NodeToAttributeValueProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHostname()) {
      return null;
    }
    return p.getHostname();
  }

  @Override
  public void setHostname(String hostname) {
    maybeInitBuilder();
    if (hostname == null) {
      builder.clearHostname();
      return;
    }
    builder.setHostname(hostname);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((getAttributeValue() == null) ? 0 : getAttributeValue().hashCode());
    result = prime * result
        + ((getHostname() == null) ? 0 : getHostname().hashCode());
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
    if (obj instanceof NodeToAttributeValue) {
      NodeToAttributeValue other = (NodeToAttributeValue) obj;
      if (!compare(getAttributeValue(), other.getAttributeValue())) {
        return false;
      }
      if (!compare(getHostname(), other.getHostname())) {
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
  public String toString() {
    return "Name-" + getHostname() + " : Attribute Value-"
        + getAttributeValue();
  }
}
