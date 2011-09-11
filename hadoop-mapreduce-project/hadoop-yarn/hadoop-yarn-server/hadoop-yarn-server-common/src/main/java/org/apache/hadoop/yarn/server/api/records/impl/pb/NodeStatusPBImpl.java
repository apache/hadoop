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

package org.apache.hadoop.yarn.server.api.records.impl.pb;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerStatusPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeHealthStatusPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeHealthStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
    
public class NodeStatusPBImpl extends ProtoBase<NodeStatusProto> implements NodeStatus {
  NodeStatusProto proto = NodeStatusProto.getDefaultInstance();
  NodeStatusProto.Builder builder = null;
  boolean viaProto = false;
  
  private NodeId nodeId = null;
  private List<ContainerStatus> containers = null;
  private NodeHealthStatus nodeHealthStatus = null;
  
  public NodeStatusPBImpl() {
    builder = NodeStatusProto.newBuilder();
  }

  public NodeStatusPBImpl(NodeStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public NodeStatusProto getProto() {

      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.nodeId != null) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
    if (this.containers != null) {
      addContainersToProto();
    }
    if (this.nodeHealthStatus != null) {
      builder.setNodeHealthStatus(convertToProtoFormat(this.nodeHealthStatus));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  private void addContainersToProto() {
    maybeInitBuilder();
    builder.clearContainersStatuses();
    if (containers == null)
      return;
    Iterable<ContainerStatusProto> iterable = new Iterable<ContainerStatusProto>() {
      @Override
      public Iterator<ContainerStatusProto> iterator() {
        return new Iterator<ContainerStatusProto>() {
  
          Iterator<ContainerStatus> iter = containers.iterator();
  
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
  
          @Override
          public ContainerStatusProto next() {
            return convertToProtoFormat(iter.next());
          }
  
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
  
          }
        };
  
      }
    };
    builder.addAllContainersStatuses(iterable);
  }

  @Override
  public int getResponseId() {
    NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getResponseId();
  }
  @Override
  public void setResponseId(int responseId) {
    maybeInitBuilder();
    builder.setResponseId(responseId);
  }
  @Override
  public NodeId getNodeId() {
    NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeId != null) {
      return this.nodeId;
    }
    if (!p.hasNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getNodeId());
    
    return this.nodeId;
  }
  @Override
  public void setNodeId(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null)
      builder.clearNodeId();
    this.nodeId = nodeId;
    
  }
  
  @Override
  public List<ContainerStatus> getContainersStatuses() {
    initContainers();
    return this.containers;
  }

  @Override
  public void setContainersStatuses(List<ContainerStatus> containers) {
    if (containers == null) {
      builder.clearContainersStatuses();
    }
    this.containers = containers;
  }

  private void initContainers() {
    if (this.containers != null) {
      return;
    }
    NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerStatusProto> list = p.getContainersStatusesList();
    this.containers = new ArrayList<ContainerStatus>();

    for (ContainerStatusProto c : list) {
      this.containers.add(convertFromProtoFormat(c));
    }
    
  }
  
  @Override
  public NodeHealthStatus getNodeHealthStatus() {
    NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (nodeHealthStatus != null) {
      return nodeHealthStatus;
    }
    if (!p.hasNodeHealthStatus()) {
      return null;
    }
    nodeHealthStatus = convertFromProtoFormat(p.getNodeHealthStatus());
    return nodeHealthStatus;
  }

  @Override
  public void setNodeHealthStatus(NodeHealthStatus healthStatus) {
    maybeInitBuilder();
    if (healthStatus == null) {
      builder.clearNodeHealthStatus();
    }
    this.nodeHealthStatus = healthStatus;
  }

  private NodeIdProto convertToProtoFormat(NodeId nodeId) {
    return ((NodeIdPBImpl)nodeId).getProto();
  }
  
  private NodeId convertFromProtoFormat(NodeIdProto proto) {
    return new NodeIdPBImpl(proto);
  }

  private NodeHealthStatusProto convertToProtoFormat(
      NodeHealthStatus healthStatus) {
    return ((NodeHealthStatusPBImpl) healthStatus).getProto();
  }

  private NodeHealthStatus convertFromProtoFormat(NodeHealthStatusProto proto) {
    return new NodeHealthStatusPBImpl(proto);
  }

  private ContainerStatusPBImpl convertFromProtoFormat(ContainerStatusProto c) {
    return new ContainerStatusPBImpl(c);
  }
  
  private ContainerStatusProto convertToProtoFormat(ContainerStatus c) {
    return ((ContainerStatusPBImpl)c).getProto();
  }
}  
