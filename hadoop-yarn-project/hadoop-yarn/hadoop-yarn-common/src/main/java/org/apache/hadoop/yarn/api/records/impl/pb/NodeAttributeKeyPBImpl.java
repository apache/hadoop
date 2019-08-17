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
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeKeyProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeKeyProtoOrBuilder;

/**
 * Implementation for NodeAttributeKey.
 *
 */
@Private
@Unstable
public class NodeAttributeKeyPBImpl extends NodeAttributeKey {
  private NodeAttributeKeyProto proto =
      NodeAttributeKeyProto.getDefaultInstance();
  private NodeAttributeKeyProto.Builder builder = null;
  private boolean viaProto = false;

  public NodeAttributeKeyPBImpl() {
    builder = NodeAttributeKeyProto.newBuilder();
  }

  public NodeAttributeKeyPBImpl(NodeAttributeKeyProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public NodeAttributeKeyProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeAttributeKeyProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getAttributePrefix() {
    NodeAttributeKeyProtoOrBuilder p = viaProto ? proto : builder;
    return p.getAttributePrefix();
  }

  @Override
  public void setAttributePrefix(String attributePrefix) {
    maybeInitBuilder();
    if (attributePrefix == null) {
      builder.clearAttributePrefix();
      return;
    }
    builder.setAttributePrefix(attributePrefix);
  }

  @Override
  public String getAttributeName() {
    NodeAttributeKeyProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAttributeName()) {
      return null;
    }
    return p.getAttributeName();
  }

  @Override
  public void setAttributeName(String attributeName) {
    maybeInitBuilder();
    if (attributeName == null) {
      builder.clearAttributeName();
      return;
    }
    builder.setAttributeName(attributeName);
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
    if (obj instanceof NodeAttributeKey) {
      NodeAttributeKey other = (NodeAttributeKey) obj;
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
  public String toString() {
    return "Prefix-" + getAttributePrefix() + " :Name-" + getAttributeName();
  }
}