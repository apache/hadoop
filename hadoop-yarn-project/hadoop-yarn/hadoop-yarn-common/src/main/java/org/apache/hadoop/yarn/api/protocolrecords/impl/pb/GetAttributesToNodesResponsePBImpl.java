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
package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesResponse;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeToAttributeValue;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeAttributeKeyPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeToAttributeValuePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.AttributeToNodesProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeKeyProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeToAttributeValueProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAttributesToNodesResponseProto;

/**
 * Attributes to nodes response.
 */
@Private
@Unstable
public class GetAttributesToNodesResponsePBImpl
    extends GetAttributesToNodesResponse {

  private GetAttributesToNodesResponseProto proto =
      GetAttributesToNodesResponseProto.getDefaultInstance();
  private GetAttributesToNodesResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private Map<NodeAttributeKey, List<NodeToAttributeValue>> attributesToNodes;

  public GetAttributesToNodesResponsePBImpl() {
    this.builder = GetAttributesToNodesResponseProto.newBuilder();
  }

  public GetAttributesToNodesResponsePBImpl(
      GetAttributesToNodesResponseProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  private void initAttributesToNodes() {
    if (this.attributesToNodes != null) {
      return;
    }
    YarnServiceProtos.GetAttributesToNodesResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    List<AttributeToNodesProto> list = p.getAttributesToNodesList();
    this.attributesToNodes = new HashMap<>();

    for (AttributeToNodesProto c : list) {
      List<NodeToAttributeValueProto> nodeValueMapList =
          c.getNodeValueMapList();
      List<NodeToAttributeValue> nodeToAttributeValue = new ArrayList<>();
      for (NodeToAttributeValueProto valueProto : nodeValueMapList) {
        nodeToAttributeValue.add(convertFromProtoFormat(valueProto));
      }
      if (!nodeToAttributeValue.isEmpty()) {
        this.attributesToNodes.put(convertFromProtoFormat(c.getNodeAttribute()),
            nodeToAttributeValue);
      }
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetAttributesToNodesResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addAttributesToNodesToProto() {
    maybeInitBuilder();
    builder.clearAttributesToNodes();
    if (attributesToNodes == null) {
      return;
    }
    Iterable<AttributeToNodesProto> iterable =
        () -> new Iterator<AttributeToNodesProto>() {

          private Iterator<Map.Entry<NodeAttributeKey,
              List<NodeToAttributeValue>>> iter = attributesToNodes.entrySet()
              .iterator();

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public AttributeToNodesProto next() {
            Map.Entry<NodeAttributeKey, List<NodeToAttributeValue>> attrToNodes
                      = iter.next();

            AttributeToNodesProto.Builder attrToNodesBuilder =
                AttributeToNodesProto.newBuilder().setNodeAttribute(
                    convertToProtoFormat(attrToNodes.getKey()));
            for (NodeToAttributeValue hostToAttrVal : attrToNodes.getValue()) {
              attrToNodesBuilder
                  .addNodeValueMap(convertToProtoFormat(hostToAttrVal));
            }

            return attrToNodesBuilder.build();
          }

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
        };
    builder.addAllAttributesToNodes(iterable);
  }

  private NodeAttributeKey convertFromProtoFormat(NodeAttributeKeyProto p) {
    return new NodeAttributeKeyPBImpl(p);
  }

  private NodeAttributeKeyProto convertToProtoFormat(NodeAttributeKey t) {
    return ((NodeAttributeKeyPBImpl) t).getProto();
  }

  private NodeToAttributeValue convertFromProtoFormat(
      NodeToAttributeValueProto p) {
    return new NodeToAttributeValuePBImpl(p);
  }

  private NodeToAttributeValueProto convertToProtoFormat(
      NodeToAttributeValue t) {
    return ((NodeToAttributeValuePBImpl) t).getProto();
  }

  private void mergeLocalToBuilder() {
    if (this.attributesToNodes != null) {
      addAttributesToNodesToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  public GetAttributesToNodesResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    assert false : "hashCode not designed";
    return 0;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public void setAttributeToNodes(
      Map<NodeAttributeKey, List<NodeToAttributeValue>> map) {
    initAttributesToNodes();
    attributesToNodes.clear();
    attributesToNodes.putAll(map);
  }

  @Override
  public Map<NodeAttributeKey,
      List<NodeToAttributeValue>> getAttributesToNodes() {
    initAttributesToNodes();
    return this.attributesToNodes;
  }
}
