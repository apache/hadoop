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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdToLabelsProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;

import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;

public class ReplaceLabelsOnNodeRequestPBImpl extends
    ReplaceLabelsOnNodeRequest {
  ReplaceLabelsOnNodeRequestProto proto = ReplaceLabelsOnNodeRequestProto
      .getDefaultInstance();
  ReplaceLabelsOnNodeRequestProto.Builder builder = null;
  boolean viaProto = false;

  private Map<NodeId, Set<String>> nodeIdToLabels;

  public ReplaceLabelsOnNodeRequestPBImpl() {
    this.builder = ReplaceLabelsOnNodeRequestProto.newBuilder();
  }

  public ReplaceLabelsOnNodeRequestPBImpl(ReplaceLabelsOnNodeRequestProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  private void initNodeToLabels() {
    if (this.nodeIdToLabels != null) {
      return;
    }
    ReplaceLabelsOnNodeRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeIdToLabelsProto> list = p.getNodeToLabelsList();
    this.nodeIdToLabels = new HashMap<NodeId, Set<String>>();

    for (NodeIdToLabelsProto c : list) {
      this.nodeIdToLabels.put(new NodeIdPBImpl(c.getNodeId()),
          Sets.newHashSet(c.getNodeLabelsList()));
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ReplaceLabelsOnNodeRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addNodeToLabelsToProto() {
    maybeInitBuilder();
    builder.clearNodeToLabels();
    if (nodeIdToLabels == null) {
      return;
    }
    Iterable<NodeIdToLabelsProto> iterable =
        new Iterable<NodeIdToLabelsProto>() {
          @Override
          public Iterator<NodeIdToLabelsProto> iterator() {
            return new Iterator<NodeIdToLabelsProto>() {

              Iterator<Entry<NodeId, Set<String>>> iter = nodeIdToLabels
                  .entrySet().iterator();

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public NodeIdToLabelsProto next() {
                Entry<NodeId, Set<String>> now = iter.next();
                return NodeIdToLabelsProto.newBuilder()
                    .setNodeId(convertToProtoFormat(now.getKey())).clearNodeLabels()
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
    if (this.nodeIdToLabels != null) {
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

  public ReplaceLabelsOnNodeRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public Map<NodeId, Set<String>> getNodeToLabels() {
    initNodeToLabels();
    return this.nodeIdToLabels;
  }

  @Override
  public void setNodeToLabels(Map<NodeId, Set<String>> map) {
    initNodeToLabels();
    nodeIdToLabels.clear();
    nodeIdToLabels.putAll(map);
  }

  @Override
  public boolean getFailOnUnknownNodes() {
    ReplaceLabelsOnNodeRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getFailOnUnknownNodes();
  }

  @Override
  public void setFailOnUnknownNodes(boolean failOnUnknownNodes) {
    maybeInitBuilder();
    builder.setFailOnUnknownNodes(failOnUnknownNodes);
  }

  private NodeIdProto convertToProtoFormat(NodeId t) {
    return ((NodeIdPBImpl) t).getProto();
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
