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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerStatusPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeReportPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.PreemptionMessageProto;

    
public class AllocateResponsePBImpl extends ProtoBase<AllocateResponseProto>
    implements AllocateResponse {
  AllocateResponseProto proto = AllocateResponseProto.getDefaultInstance();
  AllocateResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  Resource limit;

  private List<Container> allocatedContainers = null;
  private List<ContainerStatus> completedContainersStatuses = null;

  private List<NodeReport> updatedNodes = null;
  private PreemptionMessage preempt;
  
  
  public AllocateResponsePBImpl() {
    builder = AllocateResponseProto.newBuilder();
  }

  public AllocateResponsePBImpl(AllocateResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized AllocateResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.allocatedContainers != null) {
      builder.clearAllocatedContainers();
      Iterable<ContainerProto> iterable =
          getProtoIterable(this.allocatedContainers);
      builder.addAllAllocatedContainers(iterable);
    }
    if (this.completedContainersStatuses != null) {
      builder.clearCompletedContainerStatuses();
      Iterable<ContainerStatusProto> iterable =
          getContainerStatusProtoIterable(this.completedContainersStatuses);
      builder.addAllCompletedContainerStatuses(iterable);
    }
    if (this.updatedNodes != null) {
      builder.clearUpdatedNodes();
      Iterable<NodeReportProto> iterable =
          getNodeReportProtoIterable(this.updatedNodes);
      builder.addAllUpdatedNodes(iterable);
    }
    if (this.limit != null) {
      builder.setLimit(convertToProtoFormat(this.limit));
    }
    if (this.preempt != null) {
      builder.setPreempt(convertToProtoFormat(this.preempt));
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
      builder = AllocateResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  @Override
  public synchronized boolean getReboot() {
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getReboot());
  }

  @Override
  public synchronized void setReboot(boolean reboot) {
    maybeInitBuilder();
    builder.setReboot((reboot));
  }

  @Override
  public synchronized int getResponseId() {
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getResponseId());
  }

  @Override
  public synchronized void setResponseId(int responseId) {
    maybeInitBuilder();
    builder.setResponseId((responseId));
  }

  @Override
  public synchronized Resource getAvailableResources() {
    if (this.limit != null) {
      return this.limit;
    }

    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasLimit()) {
      return null;
    }
    this.limit = convertFromProtoFormat(p.getLimit());
    return this.limit;
  }

  @Override
  public synchronized void setAvailableResources(Resource limit) {
    maybeInitBuilder();
    if (limit == null)
      builder.clearLimit();
    this.limit = limit;
  }

  @Override
  public synchronized List<NodeReport> getUpdatedNodes() {
    initLocalNewNodeReportList();
    return this.updatedNodes;
  }
  @Override
  public synchronized void setUpdatedNodes(
      final List<NodeReport> updatedNodes) {
    if (updatedNodes == null) {
      this.updatedNodes.clear();
      return;
    }
    this.updatedNodes = new ArrayList<NodeReport>(updatedNodes.size());
    this.updatedNodes.addAll(updatedNodes);
  }

  @Override
  public synchronized List<Container> getAllocatedContainers() {
    initLocalNewContainerList();
    return this.allocatedContainers;
  }

  @Override
  public synchronized void setAllocatedContainers(
      final List<Container> containers) {
    if (containers == null)
      return;
    // this looks like a bug because it results in append and not set
    initLocalNewContainerList();
    allocatedContainers.addAll(containers);
  }

  //// Finished containers
  @Override
  public synchronized List<ContainerStatus> getCompletedContainersStatuses() {
    initLocalFinishedContainerList();
    return this.completedContainersStatuses;
  }

  @Override
  public synchronized void setCompletedContainersStatuses(
      final List<ContainerStatus> containers) {
    if (containers == null)
      return;
    initLocalFinishedContainerList();
    completedContainersStatuses.addAll(containers);
  }

  @Override
  public synchronized int getNumClusterNodes() {
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    return p.getNumClusterNodes();
  }

  @Override
  public synchronized void setNumClusterNodes(int numNodes) {
    maybeInitBuilder();
    builder.setNumClusterNodes(numNodes);
  }

  @Override
  public synchronized PreemptionMessage getPreemptionMessage() {
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.preempt != null) {
      return this.preempt;
    }
    if (!p.hasPreempt()) {
      return null;
    }
    this.preempt = convertFromProtoFormat(p.getPreempt());
    return this.preempt;
  }

  @Override
  public synchronized void setPreemptionMessage(PreemptionMessage preempt) {
    maybeInitBuilder();
    if (null == preempt) {
      builder.clearPreempt();
    }
    this.preempt = preempt;
  }

  // Once this is called. updatedNodes will never be null - until a getProto is
  // called.
  private synchronized void initLocalNewNodeReportList() {
    if (this.updatedNodes != null) {
      return;
    }
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeReportProto> list = p.getUpdatedNodesList();
    updatedNodes = new ArrayList<NodeReport>(list.size());

    for (NodeReportProto n : list) {
      updatedNodes.add(convertFromProtoFormat(n));
    }
  }

  // Once this is called. containerList will never be null - until a getProto
  // is called.
  private synchronized void initLocalNewContainerList() {
    if (this.allocatedContainers != null) {
      return;
    }
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerProto> list = p.getAllocatedContainersList();
    allocatedContainers = new ArrayList<Container>();

    for (ContainerProto c : list) {
      allocatedContainers.add(convertFromProtoFormat(c));
    }
  }

  private synchronized Iterable<ContainerProto> getProtoIterable(
      final List<Container> newContainersList) {
    maybeInitBuilder();
    return new Iterable<ContainerProto>() {
      @Override
      public synchronized Iterator<ContainerProto> iterator() {
        return new Iterator<ContainerProto>() {

          Iterator<Container> iter = newContainersList.iterator();

          @Override
          public synchronized boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public synchronized ContainerProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public synchronized void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
  }

  private synchronized Iterable<ContainerStatusProto>
  getContainerStatusProtoIterable(
      final List<ContainerStatus> newContainersList) {
    maybeInitBuilder();
    return new Iterable<ContainerStatusProto>() {
      @Override
      public synchronized Iterator<ContainerStatusProto> iterator() {
        return new Iterator<ContainerStatusProto>() {

          Iterator<ContainerStatus> iter = newContainersList.iterator();

          @Override
          public synchronized boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public synchronized ContainerStatusProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public synchronized void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
  }
  
  private synchronized Iterable<NodeReportProto>
  getNodeReportProtoIterable(
      final List<NodeReport> newNodeReportsList) {
    maybeInitBuilder();
    return new Iterable<NodeReportProto>() {
      @Override
      public synchronized Iterator<NodeReportProto> iterator() {
        return new Iterator<NodeReportProto>() {

          Iterator<NodeReport> iter = newNodeReportsList.iterator();

          @Override
          public synchronized boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public synchronized NodeReportProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public synchronized void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
  }

  // Once this is called. containerList will never be null - until a getProto
  // is called.
  private synchronized void initLocalFinishedContainerList() {
    if (this.completedContainersStatuses != null) {
      return;
    }
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerStatusProto> list = p.getCompletedContainerStatusesList();
    completedContainersStatuses = new ArrayList<ContainerStatus>();

    for (ContainerStatusProto c : list) {
      completedContainersStatuses.add(convertFromProtoFormat(c));
    }
  }

  private synchronized NodeReportPBImpl convertFromProtoFormat(
      NodeReportProto p) {
    return new NodeReportPBImpl(p);
  }

  private synchronized NodeReportProto convertToProtoFormat(NodeReport t) {
    return ((NodeReportPBImpl)t).getProto();
  }

  private synchronized ContainerPBImpl convertFromProtoFormat(
      ContainerProto p) {
    return new ContainerPBImpl(p);
  }

  private synchronized ContainerProto convertToProtoFormat(Container t) {
    return ((ContainerPBImpl)t).getProto();
  }

  private synchronized ContainerStatusPBImpl convertFromProtoFormat(
      ContainerStatusProto p) {
    return new ContainerStatusPBImpl(p);
  }

  private synchronized ContainerStatusProto convertToProtoFormat(
      ContainerStatus t) {
    return ((ContainerStatusPBImpl)t).getProto();
  }

  private synchronized ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private synchronized ResourceProto convertToProtoFormat(Resource r) {
    return ((ResourcePBImpl) r).getProto();
  }

  private synchronized PreemptionMessagePBImpl convertFromProtoFormat(PreemptionMessageProto p) {
    return new PreemptionMessagePBImpl(p);
  }

  private synchronized PreemptionMessageProto convertToProtoFormat(PreemptionMessage r) {
    return ((PreemptionMessagePBImpl)r).getProto();
  }
}  
