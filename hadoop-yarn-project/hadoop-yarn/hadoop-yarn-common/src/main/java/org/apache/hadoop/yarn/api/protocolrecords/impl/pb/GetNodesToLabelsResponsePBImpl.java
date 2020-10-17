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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeLabelPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdToLabelsProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNodesToLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNodesToLabelsResponseProtoOrBuilder;

import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;

public class GetNodesToLabelsResponsePBImpl extends
    GetNodesToLabelsResponse {
  GetNodesToLabelsResponseProto proto = GetNodesToLabelsResponseProto
      .getDefaultInstance();
  GetNodesToLabelsResponseProto.Builder builder = null;
  boolean viaProto = false;

  private Map<NodeId, Set<String>> nodeToLabels;

  public GetNodesToLabelsResponsePBImpl() {
    this.builder = GetNodesToLabelsResponseProto.newBuilder();
  }

  public GetNodesToLabelsResponsePBImpl(GetNodesToLabelsResponseProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  private void initNodeToLabels() {
    if (this.nodeToLabels != null) {
      return;
    }
    GetNodesToLabelsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeIdToLabelsProto> list = p.getNodeToLabelsList();
    this.nodeToLabels = new HashMap<NodeId, Set<String>>();

    for (NodeIdToLabelsProto c : list) {
      this.nodeToLabels.put(new NodeIdPBImpl(c.getNodeId()),
          Sets.newHashSet(c.getNodeLabelsList()));
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetNodesToLabelsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addNodeToLabelsToProto() {
    maybeInitBuilder();
    builder.clearNodeToLabels();
    if (nodeToLabels == null) {
      return;
    }
    Iterable<NodeIdToLabelsProto> iterable =
        new Iterable<NodeIdToLabelsProto>() {
          @Override
          public Iterator<NodeIdToLabelsProto> iterator() {
            return new Iterator<NodeIdToLabelsProto>() {

              Iterator<Entry<NodeId, Set<String>>> iter = nodeToLabels
                  .entrySet().iterator();

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public NodeIdToLabelsProto next() {
                Entry<NodeId, Set<String>> now = iter.next();
                return NodeIdToLabelsProto.newBuilder()
                    .setNodeId(convertToProtoFormat(now.getKey()))
                    .addAllNodeLabels(now.getValue()).build();
              }

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }
            };
          }
        };
    builder.addAllNodeToLabels(iterable);
  }

  private void mergeLocalToBuilder() {
    if (this.nodeToLabels != null) {
      addNodeToLabelsToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  public GetNodesToLabelsResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public Map<NodeId, Set<String>> getNodeToLabels() {
    initNodeToLabels();
    return this.nodeToLabels;
  }

  @Override
  public void setNodeToLabels(Map<NodeId, Set<String>> map) {
    initNodeToLabels();
    nodeToLabels.clear();
    nodeToLabels.putAll(map);
  }
  
  private NodeIdProto convertToProtoFormat(NodeId t) {
    return ((NodeIdPBImpl)t).getProto();
  }

  @Override
  public int hashCode() {
    assert false : "hashCode not designed";
    return 0;
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
}
