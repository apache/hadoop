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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.api.records.impl.pb.CollectorInfoPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerStatusPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NMTokenPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeReportPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PreemptionMessagePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.RejectedSchedulingRequestPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.UpdatedContainerPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.CollectorInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PreemptionMessageProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.NMTokenProto;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class AllocateResponsePBImpl extends AllocateResponse {
  AllocateResponseProto proto = AllocateResponseProto.getDefaultInstance();
  AllocateResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  Resource limit;

  private List<Container> allocatedContainers = null;
  private List<Container> containersFromPreviousAttempts = null;
  private List<NMToken> nmTokens = null;
  private List<ContainerStatus> completedContainersStatuses = null;
  private List<UpdatedContainer> updatedContainers = null;

  private List<NodeReport> updatedNodes = null;
  private List<UpdateContainerError> updateErrors = null;
  private List<RejectedSchedulingRequest> rejectedRequests = null;
  private PreemptionMessage preempt;
  private Token amrmToken = null;
  private Priority appPriority = null;
  private CollectorInfo collectorInfo = null;

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

  private synchronized void mergeLocalToBuilder() {
    if (this.allocatedContainers != null) {
      builder.clearAllocatedContainers();
      Iterable<ContainerProto> iterable =
          getContainerProtoIterable(this.allocatedContainers);
      builder.addAllAllocatedContainers(iterable);
    }
    if (nmTokens != null) {
      builder.clearNmTokens();
      Iterable<NMTokenProto> iterable = getTokenProtoIterable(nmTokens);
      builder.addAllNmTokens(iterable);
    }
    if (this.completedContainersStatuses != null) {
      builder.clearCompletedContainerStatuses();
      Iterable<ContainerStatusProto> iterable =
          getContainerStatusProtoIterable(this.completedContainersStatuses);
      builder.addAllCompletedContainerStatuses(iterable);
    }
    if (this.rejectedRequests != null) {
      builder.clearRejectedSchedulingRequests();
      Iterable<YarnProtos.RejectedSchedulingRequestProto> iterable =
          getRejectedSchedulingRequestsProtoIterable(
              this.rejectedRequests);
      builder.addAllRejectedSchedulingRequests(iterable);
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
    if (this.updatedContainers != null) {
      builder.clearUpdatedContainers();
      Iterable<YarnServiceProtos.UpdatedContainerProto> iterable =
          getUpdatedContainerProtoIterable(this.updatedContainers);
      builder.addAllUpdatedContainers(iterable);
    }
    if (this.updateErrors != null) {
      builder.clearUpdateErrors();
      Iterable<YarnServiceProtos.UpdateContainerErrorProto> iterable =
          getUpdateErrorsIterable(this.updateErrors);
      builder.addAllUpdateErrors(iterable);
    }
    if (this.amrmToken != null) {
      builder.setAmRmToken(convertToProtoFormat(this.amrmToken));
    }
    if (this.collectorInfo != null) {
      builder.setCollectorInfo(convertToProtoFormat(this.collectorInfo));
    }
    if (this.appPriority != null) {
      builder.setApplicationPriority(convertToProtoFormat(this.appPriority));
    }
    if (this.containersFromPreviousAttempts != null) {
      builder.clearContainersFromPreviousAttempts();
      Iterable<ContainerProto> iterable =
          getContainerProtoIterable(this.containersFromPreviousAttempts);
      builder.addAllContainersFromPreviousAttempts(iterable);
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
  public synchronized AMCommand getAMCommand() {
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAMCommand()) {
      return null;
    }
    return ProtoUtils.convertFromProtoFormat(p.getAMCommand());
  }

  @Override
  public synchronized void setAMCommand(AMCommand command) {
    maybeInitBuilder();
    if (command == null) {
      builder.clearAMCommand();
      return;
    }
    builder.setAMCommand(ProtoUtils.convertToProtoFormat(command));
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
  public synchronized List<UpdateContainerError> getUpdateErrors() {
    initLocalUpdateErrorsList();
    return this.updateErrors;
  }

  @Override
  public synchronized void setUpdateErrors(
      List<UpdateContainerError> updateErrors) {
    if (updateErrors == null) {
      if (this.updateErrors != null) {
        this.updateErrors.clear();
      }
      return;
    }
    this.updateErrors = new ArrayList<>(
        updateErrors.size());
    this.updateErrors.addAll(updateErrors);
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
    initLocalNewContainerList();
    allocatedContainers.clear();
    allocatedContainers.addAll(containers);
  }

  @Override
  public synchronized List<UpdatedContainer> getUpdatedContainers() {
    initLocalUpdatedContainerList();
    return this.updatedContainers;
  }

  @Override
  public synchronized void setUpdatedContainers(
      final List<UpdatedContainer> containers) {
    if (containers == null)
      return;
    initLocalUpdatedContainerList();
    updatedContainers.clear();
    updatedContainers.addAll(containers);
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
    completedContainersStatuses.clear();
    completedContainersStatuses.addAll(containers);
  }

  @Override
  public synchronized void setNMTokens(List<NMToken> nmTokens) {
    maybeInitBuilder();
    if (nmTokens == null || nmTokens.isEmpty()) {
      if (this.nmTokens != null) {
        this.nmTokens.clear();
      }
      builder.clearNmTokens();
      return;
    }
    // Implementing it as an append rather than set for consistency
    initLocalNewNMTokenList();
    this.nmTokens.addAll(nmTokens);
  }

  @Override
  public synchronized List<NMToken> getNMTokens() {
    initLocalNewNMTokenList();
    return nmTokens;
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

  @Override
  public synchronized Token getAMRMToken() {
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (amrmToken != null) {
      return amrmToken;
    }
    if (!p.hasAmRmToken()) {
      return null;
    }
    this.amrmToken = convertFromProtoFormat(p.getAmRmToken());
    return amrmToken;
  }

  @Override
  public synchronized void setAMRMToken(Token amRMToken) {
    maybeInitBuilder();
    if (amRMToken == null) {
      builder.clearAmRmToken();
    }
    this.amrmToken = amRMToken;
  }


  @Override
  public synchronized CollectorInfo getCollectorInfo() {
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.collectorInfo != null) {
      return this.collectorInfo;
    }
    if (!p.hasCollectorInfo()) {
      return null;
    }
    this.collectorInfo = convertFromProtoFormat(p.getCollectorInfo());
    return this.collectorInfo;
  }

  @Override
  public synchronized void setCollectorInfo(CollectorInfo info) {
    maybeInitBuilder();
    if (info == null) {
      builder.clearCollectorInfo();
    }
    this.collectorInfo = info;
  }

  @Override
  public synchronized Priority getApplicationPriority() {
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.appPriority != null) {
      return this.appPriority;
    }
    if (!p.hasApplicationPriority()) {
      return null;
    }
    this.appPriority = convertFromProtoFormat(p.getApplicationPriority());
    return this.appPriority;
  }

  @Override
  public synchronized void setApplicationPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null)
      builder.clearApplicationPriority();
    this.appPriority = priority;
  }

  @Override
  public synchronized List<Container> getContainersFromPreviousAttempts() {
    initContainersFromPreviousAttemptsList();
    return this.containersFromPreviousAttempts;
  }

  @Override
  public synchronized void setContainersFromPreviousAttempts(
      final List<Container> containers) {
    if (containers == null) {
      return;
    }
    initContainersFromPreviousAttemptsList();
    containersFromPreviousAttempts.clear();
    containersFromPreviousAttempts.addAll(containers);
  }

  @Override
  public synchronized List<RejectedSchedulingRequest>
      getRejectedSchedulingRequests() {
    initRejectedRequestsList();
    return this.rejectedRequests;
  }

  @Override
  public synchronized void setRejectedSchedulingRequests(
      List<RejectedSchedulingRequest> rejectedReqs) {
    if (rejectedReqs == null) {
      return;
    }
    initRejectedRequestsList();
    this.rejectedRequests.clear();
    this.rejectedRequests.addAll(rejectedReqs);
  }

  private synchronized void initLocalUpdatedContainerList() {
    if (this.updatedContainers != null) {
      return;
    }
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<YarnServiceProtos.UpdatedContainerProto> list =
        p.getUpdatedContainersList();
    updatedContainers = new ArrayList<>();

    for (YarnServiceProtos.UpdatedContainerProto c : list) {
      updatedContainers.add(convertFromProtoFormat(c));
    }
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

  private synchronized void initContainersFromPreviousAttemptsList() {
    if (this.containersFromPreviousAttempts != null) {
      return;
    }
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerProto> list = p.getContainersFromPreviousAttemptsList();
    containersFromPreviousAttempts = new ArrayList<>();

    for (ContainerProto c : list) {
      containersFromPreviousAttempts.add(convertFromProtoFormat(c));
    }
  }

  private synchronized void initRejectedRequestsList() {
    if (this.rejectedRequests != null) {
      return;
    }
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<YarnProtos.RejectedSchedulingRequestProto> list =
        p.getRejectedSchedulingRequestsList();
    rejectedRequests = new ArrayList<>();

    for (YarnProtos.RejectedSchedulingRequestProto c : list) {
      rejectedRequests.add(convertFromProtoFormat(c));
    }
  }

  private synchronized void initLocalNewNMTokenList() {
    if (nmTokens != null) {
      return;
    }
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<NMTokenProto> list = p.getNmTokensList();
    nmTokens = new ArrayList<NMToken>();
    for (NMTokenProto t : list) {
      nmTokens.add(convertFromProtoFormat(t));
    }
  }

  private synchronized void initLocalUpdateErrorsList() {
    if (updateErrors != null) {
      return;
    }
    AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<YarnServiceProtos.UpdateContainerErrorProto> list =
        p.getUpdateErrorsList();
    this.updateErrors = new ArrayList<UpdateContainerError>();
    for (YarnServiceProtos.UpdateContainerErrorProto t : list) {
      updateErrors.add(ProtoUtils.convertFromProtoFormat(t));
    }
  }

  private synchronized Iterable<YarnServiceProtos.UpdateContainerErrorProto>
      getUpdateErrorsIterable(
      final List<UpdateContainerError> updateErrorsList) {
    maybeInitBuilder();
    return new Iterable<YarnServiceProtos.UpdateContainerErrorProto>() {
      @Override
      public synchronized Iterator<YarnServiceProtos
          .UpdateContainerErrorProto> iterator() {
        return new Iterator<YarnServiceProtos.UpdateContainerErrorProto>() {

          private Iterator<UpdateContainerError> iter =
              updateErrorsList.iterator();

          @Override
          public synchronized boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public synchronized YarnServiceProtos.UpdateContainerErrorProto
              next() {
            return ProtoUtils.convertToProtoFormat(iter.next());
          }

          @Override
          public synchronized void remove() {
            throw new UnsupportedOperationException();
          }
        };

      }
    };
  }

  private synchronized Iterable<ContainerProto> getContainerProtoIterable(
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

  private synchronized Iterable<YarnServiceProtos.UpdatedContainerProto>
        getUpdatedContainerProtoIterable(
      final List<UpdatedContainer> newUpdatedContainersList) {
    maybeInitBuilder();
    return new Iterable<YarnServiceProtos.UpdatedContainerProto>() {
      @Override
      public synchronized Iterator<YarnServiceProtos.UpdatedContainerProto>
          iterator() {
        return new Iterator<YarnServiceProtos.UpdatedContainerProto>() {

          private Iterator<UpdatedContainer> iter =
              newUpdatedContainersList.iterator();

          @Override
          public synchronized boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public synchronized YarnServiceProtos.UpdatedContainerProto next() {
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

  private synchronized Iterable<NMTokenProto> getTokenProtoIterable(
      final List<NMToken> nmTokenList) {
    maybeInitBuilder();
    return new Iterable<NMTokenProto>() {
      @Override
      public synchronized Iterator<NMTokenProto> iterator() {
        return new Iterator<NMTokenProto>() {

          Iterator<NMToken> iter = nmTokenList.iterator();
          
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
          
          @Override
          public NMTokenProto next() {
            return convertToProtoFormat(iter.next());
          }
          
          @Override
          public void remove() {
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

  private synchronized Iterable<YarnProtos.RejectedSchedulingRequestProto>
      getRejectedSchedulingRequestsProtoIterable(
      final List<RejectedSchedulingRequest> rejectedReqsList) {
    maybeInitBuilder();
    return new Iterable<YarnProtos.RejectedSchedulingRequestProto>() {
      @Override
      public Iterator<YarnProtos.RejectedSchedulingRequestProto> iterator() {
        return new Iterator<YarnProtos.RejectedSchedulingRequestProto>() {

          private Iterator<RejectedSchedulingRequest> iter =
              rejectedReqsList.iterator();

          @Override
          public synchronized boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public synchronized YarnProtos.RejectedSchedulingRequestProto next() {
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

  private synchronized CollectorInfoPBImpl convertFromProtoFormat(
      CollectorInfoProto p) {
    return new CollectorInfoPBImpl(p);
  }

  private synchronized CollectorInfoProto convertToProtoFormat(
      CollectorInfo t) {
    return ((CollectorInfoPBImpl)t).getProto();
  }

  private synchronized ContainerPBImpl convertFromProtoFormat(
      ContainerProto p) {
    return new ContainerPBImpl(p);
  }
  
  private synchronized ContainerProto convertToProtoFormat(
      Container t) {
    return ((ContainerPBImpl)t).getProto();
  }

  private synchronized UpdatedContainerPBImpl convertFromProtoFormat(
      YarnServiceProtos.UpdatedContainerProto p) {
    return new UpdatedContainerPBImpl(p);
  }

  private synchronized YarnServiceProtos.UpdatedContainerProto
      convertToProtoFormat(UpdatedContainer t) {
    return ((UpdatedContainerPBImpl)t).getProto();
  }

  private synchronized ContainerStatusPBImpl convertFromProtoFormat(
      ContainerStatusProto p) {
    return new ContainerStatusPBImpl(p);
  }

  private synchronized ContainerStatusProto convertToProtoFormat(
      ContainerStatus t) {
    return ((ContainerStatusPBImpl)t).getProto();
  }

  private synchronized RejectedSchedulingRequestPBImpl convertFromProtoFormat(
      YarnProtos.RejectedSchedulingRequestProto p) {
    return new RejectedSchedulingRequestPBImpl(p);
  }

  private synchronized YarnProtos.RejectedSchedulingRequestProto
      convertToProtoFormat(RejectedSchedulingRequest t) {
    return ((RejectedSchedulingRequestPBImpl)t).getProto();
  }

  private synchronized ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private synchronized ResourceProto convertToProtoFormat(Resource r) {
    return ProtoUtils.convertToProtoFormat(r);
  }

  private synchronized PreemptionMessagePBImpl convertFromProtoFormat(PreemptionMessageProto p) {
    return new PreemptionMessagePBImpl(p);
  }

  private synchronized PreemptionMessageProto convertToProtoFormat(PreemptionMessage r) {
    return ((PreemptionMessagePBImpl)r).getProto();
  }
  
  private synchronized NMTokenProto convertToProtoFormat(NMToken token) {
    return ((NMTokenPBImpl)token).getProto();
  }
  
  private synchronized NMToken convertFromProtoFormat(NMTokenProto proto) {
    return new NMTokenPBImpl(proto);
  }

  private TokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl)t).getProto();
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl)t).getProto();
  }
}
