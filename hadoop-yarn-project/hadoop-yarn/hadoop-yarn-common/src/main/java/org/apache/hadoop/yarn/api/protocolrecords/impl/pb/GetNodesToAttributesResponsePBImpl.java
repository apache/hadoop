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

import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesResponse;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeAttributePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Nodes to attributes request response.
 */
public class GetNodesToAttributesResponsePBImpl
    extends GetNodesToAttributesResponse {

  private YarnServiceProtos.GetNodesToAttributesResponseProto proto =
      YarnServiceProtos.GetNodesToAttributesResponseProto.getDefaultInstance();
  private YarnServiceProtos.GetNodesToAttributesResponseProto.Builder builder =
      null;
  private boolean viaProto = false;

  private Map<String, Set<NodeAttribute>> nodesToAttributes;

  public GetNodesToAttributesResponsePBImpl() {
    this.builder =
        YarnServiceProtos.GetNodesToAttributesResponseProto.newBuilder();
  }

  public GetNodesToAttributesResponsePBImpl(
      YarnServiceProtos.GetNodesToAttributesResponseProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  private void initNodesToAttributes() {
    if (this.nodesToAttributes != null) {
      return;
    }
    YarnServiceProtos.GetNodesToAttributesResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    List<YarnProtos.NodeToAttributesProto> list = p.getNodesToAttributesList();
    this.nodesToAttributes = new HashMap<>();
    for (YarnProtos.NodeToAttributesProto c : list) {
      HashSet<NodeAttribute> attributes = new HashSet<>();
      for (YarnProtos.NodeAttributeProto nodeAttrProto : c
          .getNodeAttributesList()) {
        attributes.add(new NodeAttributePBImpl(nodeAttrProto));
      }
      nodesToAttributes.put(c.getNode(), attributes);
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder =
          YarnServiceProtos.GetNodesToAttributesResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addNodesToAttributesToProto() {
    maybeInitBuilder();
    builder.clearNodesToAttributes();
    if (nodesToAttributes == null) {
      return;
    }
    Iterable<YarnProtos.NodeToAttributesProto> iterable =
        () -> new Iterator<YarnProtos.NodeToAttributesProto>() {

          private Iterator<Map.Entry<String, Set<NodeAttribute>>> iter =
              nodesToAttributes.entrySet().iterator();

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public YarnProtos.NodeToAttributesProto next() {
            Map.Entry<String, Set<NodeAttribute>> now = iter.next();
            Set<YarnProtos.NodeAttributeProto> protoSet = new HashSet<>();
            for (NodeAttribute nodeAttribute : now.getValue()) {
              protoSet.add(convertToProtoFormat(nodeAttribute));
            }
            return YarnProtos.NodeToAttributesProto.newBuilder()
                .setNode(now.getKey()).addAllNodeAttributes(protoSet).build();
          }

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
        };
    builder.addAllNodesToAttributes(iterable);
  }

  private NodeAttributePBImpl convertFromProtoFormat(
      YarnProtos.NodeAttributeProto p) {
    return new NodeAttributePBImpl(p);
  }

  private YarnProtos.NodeAttributeProto convertToProtoFormat(NodeAttribute t) {
    return ((NodeAttributePBImpl) t).getProto();
  }

  private void mergeLocalToBuilder() {
    if (this.nodesToAttributes != null) {
      addNodesToAttributesToProto();
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

  public YarnServiceProtos.GetNodesToAttributesResponseProto getProto() {
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
  public void setNodeToAttributes(Map<String, Set<NodeAttribute>> map) {
    initNodesToAttributes();
    nodesToAttributes.clear();
    nodesToAttributes.putAll(map);
  }

  @Override
  public Map<String, Set<NodeAttribute>> getNodeToAttributes() {
    initNodesToAttributes();
    return nodesToAttributes;
  }
}
