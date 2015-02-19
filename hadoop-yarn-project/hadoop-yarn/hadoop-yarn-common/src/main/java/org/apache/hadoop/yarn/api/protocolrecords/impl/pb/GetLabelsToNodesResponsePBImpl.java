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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;

import org.apache.hadoop.yarn.proto.YarnProtos.LabelsToNodeIdsProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLabelsToNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLabelsToNodesResponseProtoOrBuilder;

public class GetLabelsToNodesResponsePBImpl extends
  GetLabelsToNodesResponse {
  GetLabelsToNodesResponseProto proto = GetLabelsToNodesResponseProto
     .getDefaultInstance();
  GetLabelsToNodesResponseProto.Builder builder = null;
  boolean viaProto = false;

  private Map<String, Set<NodeId>> labelsToNodes;

  public GetLabelsToNodesResponsePBImpl() {
    this.builder = GetLabelsToNodesResponseProto.newBuilder();
  }

  public GetLabelsToNodesResponsePBImpl(GetLabelsToNodesResponseProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  private void initLabelsToNodes() {
    if (this.labelsToNodes != null) {
      return;
    }
    GetLabelsToNodesResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<LabelsToNodeIdsProto> list = p.getLabelsToNodesList();
    this.labelsToNodes = new HashMap<String, Set<NodeId>>();

    for (LabelsToNodeIdsProto c : list) {
      Set<NodeId> setNodes = new HashSet<NodeId>();
      for(NodeIdProto n : c.getNodeIdList()) {
        NodeId node = new NodeIdPBImpl(n);
        setNodes.add(node);
      }
      if(!setNodes.isEmpty()) {
        this.labelsToNodes.put(c.getNodeLabels(), setNodes);
      }
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetLabelsToNodesResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addLabelsToNodesToProto() {
    maybeInitBuilder();
    builder.clearLabelsToNodes();
    if (labelsToNodes == null) {
      return;
    }
    Iterable<LabelsToNodeIdsProto> iterable =
        new Iterable<LabelsToNodeIdsProto>() {
          @Override
          public Iterator<LabelsToNodeIdsProto> iterator() {
            return new Iterator<LabelsToNodeIdsProto>() {

              Iterator<Entry<String, Set<NodeId>>> iter =
                  labelsToNodes.entrySet().iterator();

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public LabelsToNodeIdsProto next() {
                Entry<String, Set<NodeId>> now = iter.next();
                Set<NodeIdProto> nodeProtoSet = new HashSet<NodeIdProto>();
                for(NodeId n : now.getValue()) {
                  nodeProtoSet.add(convertToProtoFormat(n));
                }
                return LabelsToNodeIdsProto.newBuilder()
                    .setNodeLabels(now.getKey()).addAllNodeId(nodeProtoSet)
                    .build();
              }

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }
            };
          }
        };
    builder.addAllLabelsToNodes(iterable);
  }

  private void mergeLocalToBuilder() {
    if (this.labelsToNodes != null) {
      addLabelsToNodesToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  public GetLabelsToNodesResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
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

  @Override
  @Public
  @Evolving
  public void setLabelsToNodes(Map<String, Set<NodeId>> map) {
    initLabelsToNodes();
    labelsToNodes.clear();
    labelsToNodes.putAll(map);
  }

  @Override
  @Public
  @Evolving
  public Map<String, Set<NodeId>> getLabelsToNodes() {
    initLabelsToNodes();
    return this.labelsToNodes;
  }
}