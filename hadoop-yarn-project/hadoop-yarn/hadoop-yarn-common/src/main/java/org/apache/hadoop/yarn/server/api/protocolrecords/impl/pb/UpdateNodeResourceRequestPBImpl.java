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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceOptionPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeResourceMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceOptionProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;

public class UpdateNodeResourceRequestPBImpl extends UpdateNodeResourceRequest {

  UpdateNodeResourceRequestProto proto = UpdateNodeResourceRequestProto.getDefaultInstance();
  UpdateNodeResourceRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  Map<NodeId, ResourceOption> nodeResourceMap = null;
  
  public UpdateNodeResourceRequestPBImpl() {
    builder = UpdateNodeResourceRequestProto.newBuilder();
  }

  public UpdateNodeResourceRequestPBImpl(UpdateNodeResourceRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public Map<NodeId, ResourceOption> getNodeResourceMap() {
    initNodeResourceMap();
    return this.nodeResourceMap;
  }

  @Override
  public void setNodeResourceMap(Map<NodeId, ResourceOption> nodeResourceMap) {
    if (nodeResourceMap == null) {
      return;
    }
    initNodeResourceMap();
    this.nodeResourceMap.clear();
    this.nodeResourceMap.putAll(nodeResourceMap);
  }

  public UpdateNodeResourceRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private void mergeLocalToBuilder() {
    if (this.nodeResourceMap != null) {
      addNodeResourceMap();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void initNodeResourceMap() {
    if (this.nodeResourceMap != null) {
      return;
    }
    UpdateNodeResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeResourceMapProto> list = p.getNodeResourceMapList();
    this.nodeResourceMap = new HashMap<NodeId, ResourceOption>(list
        .size());
    for (NodeResourceMapProto nodeResourceProto : list) {
      this.nodeResourceMap.put(convertFromProtoFormat(nodeResourceProto.getNodeId()), 
          convertFromProtoFormat(nodeResourceProto.getResourceOption()));
    }
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UpdateNodeResourceRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  private NodeIdProto convertToProtoFormat(NodeId nodeId) {
    return ((NodeIdPBImpl)nodeId).getProto();
  }
  
  private NodeId convertFromProtoFormat(NodeIdProto proto) {
    return new NodeIdPBImpl(proto);
  }
  
  private ResourceOptionPBImpl convertFromProtoFormat(ResourceOptionProto c) {
    return new ResourceOptionPBImpl(c);
  }
  
  private ResourceOptionProto convertToProtoFormat(ResourceOption c) {
    return ((ResourceOptionPBImpl)c).getProto();
  }
  
  private void addNodeResourceMap() {
    maybeInitBuilder();
    builder.clearNodeResourceMap();
    if (nodeResourceMap == null) {
      return;
    }
    Iterable<? extends NodeResourceMapProto> values
        = new Iterable<NodeResourceMapProto>() {

      @Override
      public Iterator<NodeResourceMapProto> iterator() {
        return new Iterator<NodeResourceMapProto>() {
          Iterator<NodeId> nodeIterator = nodeResourceMap
              .keySet().iterator();

          @Override
          public boolean hasNext() {
            return nodeIterator.hasNext();
          }

          @Override
          public NodeResourceMapProto next() {
            NodeId nodeId = nodeIterator.next();
            return NodeResourceMapProto.newBuilder().setNodeId(
                convertToProtoFormat(nodeId)).setResourceOption(
                convertToProtoFormat(nodeResourceMap.get(nodeId))).build();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    this.builder.addAllNodeResourceMap(values);
  }
  
}
