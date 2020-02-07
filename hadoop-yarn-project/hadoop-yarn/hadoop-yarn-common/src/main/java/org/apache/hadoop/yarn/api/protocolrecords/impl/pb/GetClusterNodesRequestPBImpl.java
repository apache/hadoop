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

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeStateProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesRequestProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class GetClusterNodesRequestPBImpl extends GetClusterNodesRequest {

  GetClusterNodesRequestProto proto = GetClusterNodesRequestProto.getDefaultInstance();
  GetClusterNodesRequestProto.Builder builder = null;
  boolean viaProto = false;

  private EnumSet<NodeState> states = null;
  
  public GetClusterNodesRequestPBImpl() {
    builder = GetClusterNodesRequestProto.newBuilder();
  }

  public GetClusterNodesRequestPBImpl(GetClusterNodesRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetClusterNodesRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  @Override
  public EnumSet<NodeState> getNodeStates() {
    initNodeStates();
    return this.states;
  }
  
  @Override
  public void setNodeStates(final EnumSet<NodeState> states) {
    initNodeStates();
    this.states.clear();
    if (states == null) {
      return;
    }
    this.states.addAll(states);
  }
  
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetClusterNodesRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  private void mergeLocalToBuilder() {
    if (this.states != null) {
      maybeInitBuilder();
      builder.clearNodeStates();
      Iterable<NodeStateProto> iterable = new Iterable<NodeStateProto>() {
        @Override
        public Iterator<NodeStateProto> iterator() {
          return new Iterator<NodeStateProto>() {

            Iterator<NodeState> iter = states.iterator();

            @Override
            public boolean hasNext() {
              return iter.hasNext();
            }

            @Override
            public NodeStateProto next() {
              return ProtoUtils.convertToProtoFormat(iter.next());
            }

            @Override
            public void remove() {
              throw new UnsupportedOperationException();

            }
          };

        }
      };
      builder.addAllNodeStates(iterable);
    }
  }
  
  private void initNodeStates() {
    if (this.states != null) {
      return;
    }
    GetClusterNodesRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeStateProto> list = p.getNodeStatesList();
    this.states = EnumSet.noneOf(NodeState.class);

    for (NodeStateProto c : list) {
      this.states.add(ProtoUtils.convertFromProtoFormat(c));
    }
  }
  
  @Override
  public int hashCode() {
    return getProto().hashCode();
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
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}
