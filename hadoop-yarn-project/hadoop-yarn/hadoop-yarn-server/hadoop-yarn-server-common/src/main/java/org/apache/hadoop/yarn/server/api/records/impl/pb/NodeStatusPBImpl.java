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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerStatusPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeHealthStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
    

public class NodeStatusPBImpl extends NodeStatus {
  NodeStatusProto proto = NodeStatusProto.getDefaultInstance();
  NodeStatusProto.Builder builder = null;
  boolean viaProto = false;
  
  private NodeId nodeId = null;
  private List<ContainerStatus> containers = null;
  private NodeHealthStatus nodeHealthStatus = null;
  private List<ApplicationId> keepAliveApplications = null;
  
  public NodeStatusPBImpl() {
    builder = NodeStatusProto.newBuilder();
  }

  public NodeStatusPBImpl(NodeStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized NodeStatusProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.nodeId != null) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
    if (this.containers != null) {
      addContainersToProto();
    }
    if (this.nodeHealthStatus != null) {
      builder.setNodeHealthStatus(convertToProtoFormat(this.nodeHealthStatus));
    }
    if (this.keepAliveApplications != null) {
      addKeepAliveApplicationsToProto();
    }
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  private synchronized void addContainersToProto() {
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
  
  private synchronized void addKeepAliveApplicationsToProto() {
    maybeInitBuilder();
    builder.clearKeepAliveApplications();
    if (keepAliveApplications == null)
      return;
    Iterable<ApplicationIdProto> iterable = new Iterable<ApplicationIdProto>() {
      @Override
      public Iterator<ApplicationIdProto> iterator() {
        return new Iterator<ApplicationIdProto>() {
  
          Iterator<ApplicationId> iter = keepAliveApplications.iterator();
  
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
  
          @Override
          public ApplicationIdProto next() {
            return convertToProtoFormat(iter.next());
          }
  
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
  
          }
        };
  
      }
    };
    builder.addAllKeepAliveApplications(iterable);
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
  public synchronized int getResponseId() {
    NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getResponseId();
  }
  @Override
  public synchronized void setResponseId(int responseId) {
    maybeInitBuilder();
    builder.setResponseId(responseId);
  }
  @Override
  public synchronized NodeId getNodeId() {
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
  public synchronized void setNodeId(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null)
      builder.clearNodeId();
    this.nodeId = nodeId;
    
  }
  
  @Override
  public synchronized List<ContainerStatus> getContainersStatuses() {
    initContainers();
    return this.containers;
  }

  @Override
  public synchronized void setContainersStatuses(
      List<ContainerStatus> containers) {
    if (containers == null) {
      builder.clearContainersStatuses();
    }
    this.containers = containers;
  }
  
  @Override
  public synchronized List<ApplicationId> getKeepAliveApplications() {
    initKeepAliveApplications();
    return this.keepAliveApplications;
  }
  
  @Override
  public synchronized void setKeepAliveApplications(List<ApplicationId> appIds) {
    if (appIds == null) {
      builder.clearKeepAliveApplications();
    }
    this.keepAliveApplications = appIds;
  }

  private synchronized void initContainers() {
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
  
  private synchronized void initKeepAliveApplications() {
    if (this.keepAliveApplications != null) {
      return;
    }
    NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationIdProto> list = p.getKeepAliveApplicationsList();
    this.keepAliveApplications = new ArrayList<ApplicationId>();

    for (ApplicationIdProto c : list) {
      this.keepAliveApplications.add(convertFromProtoFormat(c));
    }
    
  }
  
  @Override
  public synchronized NodeHealthStatus getNodeHealthStatus() {
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
  public synchronized void setNodeHealthStatus(NodeHealthStatus healthStatus) {
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
  
  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto c) {
    return new ApplicationIdPBImpl(c);
  }
  
  private ApplicationIdProto convertToProtoFormat(ApplicationId c) {
    return ((ApplicationIdPBImpl)c).getProto();
  }
}
