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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeAttributePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeToAttributesProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeToAttributesProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;

/**
 * Proto class for Node to attributes mapping.
 */
public class NodeToAttributesPBImpl extends NodeToAttributes {
  private NodeToAttributesProto proto =
      NodeToAttributesProto.getDefaultInstance();
  private NodeToAttributesProto.Builder builder = null;
  private boolean viaProto = false;

  private List<NodeAttribute> nodeAttributes = null;

  public NodeToAttributesPBImpl() {
    builder = NodeToAttributesProto.newBuilder();
  }

  public NodeToAttributesPBImpl(NodeToAttributesProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    if (this.nodeAttributes != null) {
      for (NodeAttribute nodeAttribute : nodeAttributes) {
        builder.addNodeAttributes(
            ((NodeAttributePBImpl) nodeAttribute).getProto());
      }
    }
    proto = builder.build();
    viaProto = true;
  }

  public NodeToAttributesProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeToAttributesProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getNode() {
    NodeToAttributesProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNode()) {
      return null;
    }
    return p.getNode();
  }

  @Override
  public void setNode(String node) {
    maybeInitBuilder();
    builder.setNode(node);
  }

  private void initNodeAttributes() {
    if (this.nodeAttributes != null) {
      return;
    }

    NodeToAttributesProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeAttributeProto> nodeAttributesProtoList =
        p.getNodeAttributesList();
    List<NodeAttribute> attributes = new ArrayList<>();
    if (nodeAttributesProtoList == null
        || nodeAttributesProtoList.size() == 0) {
      this.nodeAttributes = attributes;
      return;
    }
    for (NodeAttributeProto nodeAttributeProto : nodeAttributesProtoList) {
      attributes.add(new NodeAttributePBImpl(nodeAttributeProto));
    }
    this.nodeAttributes = attributes;
  }

  @Override
  public List<NodeAttribute> getNodeAttributes() {
    initNodeAttributes();
    return this.nodeAttributes;
  }

  @Override
  public void setNodeAttributes(List<NodeAttribute> attributes) {
    if (nodeAttributes == null) {
      nodeAttributes = new ArrayList<>();
    }
    nodeAttributes.clear();
    nodeAttributes.addAll(attributes);
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (obj instanceof NodeToAttributes) {
      NodeToAttributes other = (NodeToAttributes) obj;
      if (getNodeAttributes() == null) {
        if (other.getNodeAttributes() != null) {
          return false;
        }
      } else if (!getNodeAttributes().containsAll(other.getNodeAttributes())) {
        return false;
      }

      if (getNode() == null) {
        if (other.getNode() != null) {
          return false;
        }
      } else if (!getNode().equals(other.getNode())) {
        return false;
      }

      return true;
    }
    return false;
  }
}
