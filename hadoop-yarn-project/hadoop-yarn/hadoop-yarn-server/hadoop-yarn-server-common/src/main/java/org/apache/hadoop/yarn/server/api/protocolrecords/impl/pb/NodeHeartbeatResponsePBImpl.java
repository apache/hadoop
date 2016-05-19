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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SignalContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ContainerQueuingLimitProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SignalContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeActionProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SystemCredentialsForAppsProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.impl.pb.ContainerQueuingLimitPBImpl;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;


public class NodeHeartbeatResponsePBImpl extends
    ProtoBase<NodeHeartbeatResponseProto> implements NodeHeartbeatResponse {
  NodeHeartbeatResponseProto proto = NodeHeartbeatResponseProto.getDefaultInstance();
  NodeHeartbeatResponseProto.Builder builder = null;
  boolean viaProto = false;

  private List<ContainerId> containersToCleanup = null;
  private List<ContainerId> containersToBeRemovedFromNM = null;
  private List<ApplicationId> applicationsToCleanup = null;
  private Map<ApplicationId, ByteBuffer> systemCredentials = null;
  private Resource resource = null;

  private MasterKey containerTokenMasterKey = null;
  private MasterKey nmTokenMasterKey = null;
  private ContainerQueuingLimit containerQueuingLimit = null;
  private List<Container> containersToDecrease = null;
  private List<SignalContainerRequest> containersToSignal = null;

  public NodeHeartbeatResponsePBImpl() {
    builder = NodeHeartbeatResponseProto.newBuilder();
  }

  public NodeHeartbeatResponsePBImpl(NodeHeartbeatResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public NodeHeartbeatResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.containersToCleanup != null) {
      addContainersToCleanupToProto();
    }
    if (this.applicationsToCleanup != null) {
      addApplicationsToCleanupToProto();
    }
    if (this.containersToBeRemovedFromNM != null) {
      addContainersToBeRemovedFromNMToProto();
    }
    if (this.containerTokenMasterKey != null) {
      builder.setContainerTokenMasterKey(
          convertToProtoFormat(this.containerTokenMasterKey));
    }
    if (this.nmTokenMasterKey != null) {
      builder.setNmTokenMasterKey(
          convertToProtoFormat(this.nmTokenMasterKey));
    }
    if (this.containerQueuingLimit != null) {
      builder.setContainerQueuingLimit(
          convertToProtoFormat(this.containerQueuingLimit));
    }
    if (this.systemCredentials != null) {
      addSystemCredentialsToProto();
    }
    if (this.containersToDecrease != null) {
      addContainersToDecreaseToProto();
    }
    if (this.containersToSignal != null) {
      addContainersToSignalToProto();
    }
    if (this.resource != null) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
  }

  private void addSystemCredentialsToProto() {
    maybeInitBuilder();
    builder.clearSystemCredentialsForApps();
    for (Map.Entry<ApplicationId, ByteBuffer> entry : systemCredentials.entrySet()) {
      builder.addSystemCredentialsForApps(SystemCredentialsForAppsProto.newBuilder()
        .setAppId(convertToProtoFormat(entry.getKey()))
        .setCredentialsForApp(ProtoUtils.convertToProtoFormat(
            entry.getValue().duplicate())));
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
      builder = NodeHeartbeatResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }


  @Override
  public int getResponseId() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getResponseId());
  }

  @Override
  public void setResponseId(int responseId) {
    maybeInitBuilder();
    builder.setResponseId((responseId));
  }

  @Override
  public Resource getResource() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resource != null) {
      return this.resource;
    }
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public void setResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null) {
      builder.clearResource();
    }
    this.resource = resource;
  }

  @Override
  public MasterKey getContainerTokenMasterKey() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerTokenMasterKey != null) {
      return this.containerTokenMasterKey;
    }
    if (!p.hasContainerTokenMasterKey()) {
      return null;
    }
    this.containerTokenMasterKey =
        convertFromProtoFormat(p.getContainerTokenMasterKey());
    return this.containerTokenMasterKey;
  }

  @Override
  public void setContainerTokenMasterKey(MasterKey masterKey) {
    maybeInitBuilder();
    if (masterKey == null)
      builder.clearContainerTokenMasterKey();
    this.containerTokenMasterKey = masterKey;
  }

  @Override
  public MasterKey getNMTokenMasterKey() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nmTokenMasterKey != null) {
      return this.nmTokenMasterKey;
    }
    if (!p.hasNmTokenMasterKey()) {
      return null;
    }
    this.nmTokenMasterKey =
        convertFromProtoFormat(p.getNmTokenMasterKey());
    return this.nmTokenMasterKey;
  }

  @Override
  public void setNMTokenMasterKey(MasterKey masterKey) {
    maybeInitBuilder();
    if (masterKey == null)
      builder.clearNmTokenMasterKey();
    this.nmTokenMasterKey = masterKey;
  }

  @Override
  public ContainerQueuingLimit getContainerQueuingLimit() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerQueuingLimit != null) {
      return this.containerQueuingLimit;
    }
    if (!p.hasContainerQueuingLimit()) {
      return null;
    }
    this.containerQueuingLimit =
        convertFromProtoFormat(p.getContainerQueuingLimit());
    return this.containerQueuingLimit;
  }

  @Override
  public void setContainerQueuingLimit(ContainerQueuingLimit
      containerQueuingLimit) {
    maybeInitBuilder();
    if (containerQueuingLimit == null) {
      builder.clearContainerQueuingLimit();
    }
    this.containerQueuingLimit = containerQueuingLimit;
  }

  @Override
  public NodeAction getNodeAction() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeAction()) {
      return null;
    }
    return (convertFromProtoFormat(p.getNodeAction()));
  }

  @Override
  public void setNodeAction(NodeAction nodeAction) {
    maybeInitBuilder();
    if (nodeAction == null) {
      builder.clearNodeAction();
      return;
    }
    builder.setNodeAction(convertToProtoFormat(nodeAction));
  }

  @Override
  public String getDiagnosticsMessage() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnosticsMessage()) {
      return null;
    }
    return p.getDiagnosticsMessage();
  }

  @Override
  public void setDiagnosticsMessage(String diagnosticsMessage) {
    maybeInitBuilder();
    if (diagnosticsMessage == null) {
      builder.clearDiagnosticsMessage();
      return;
    }
    builder.setDiagnosticsMessage((diagnosticsMessage));
  }

  @Override
  public List<ContainerId> getContainersToCleanup() {
    initContainersToCleanup();
    return this.containersToCleanup;
  }

  @Override
  public List<ContainerId> getContainersToBeRemovedFromNM() {
    initContainersToBeRemovedFromNM();
    return this.containersToBeRemovedFromNM;
  }

  private void initContainersToCleanup() {
    if (this.containersToCleanup != null) {
      return;
    }
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerIdProto> list = p.getContainersToCleanupList();
    this.containersToCleanup = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.containersToCleanup.add(convertFromProtoFormat(c));
    }
  }

  private void initContainersToBeRemovedFromNM() {
    if (this.containersToBeRemovedFromNM != null) {
      return;
    }
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerIdProto> list = p.getContainersToBeRemovedFromNmList();
    this.containersToBeRemovedFromNM = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.containersToBeRemovedFromNM.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public void addAllContainersToCleanup(
      final List<ContainerId> containersToCleanup) {
    if (containersToCleanup == null)
      return;
    initContainersToCleanup();
    this.containersToCleanup.addAll(containersToCleanup);
  }

  @Override
  public void
      addContainersToBeRemovedFromNM(final List<ContainerId> containers) {
    if (containers == null)
      return;
    initContainersToBeRemovedFromNM();
    this.containersToBeRemovedFromNM.addAll(containers);
  }

  private void addContainersToCleanupToProto() {
    maybeInitBuilder();
    builder.clearContainersToCleanup();
    if (containersToCleanup == null)
      return;
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {

      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {

          Iterator<ContainerId> iter = containersToCleanup.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ContainerIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllContainersToCleanup(iterable);
  }

  private void addContainersToBeRemovedFromNMToProto() {
    maybeInitBuilder();
    builder.clearContainersToBeRemovedFromNm();
    if (containersToBeRemovedFromNM == null)
      return;
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {

      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {

          Iterator<ContainerId> iter = containersToBeRemovedFromNM.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ContainerIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllContainersToBeRemovedFromNm(iterable);
  }

  @Override
  public List<ApplicationId> getApplicationsToCleanup() {
    initApplicationsToCleanup();
    return this.applicationsToCleanup;
  }

  private void initApplicationsToCleanup() {
    if (this.applicationsToCleanup != null) {
      return;
    }
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationIdProto> list = p.getApplicationsToCleanupList();
    this.applicationsToCleanup = new ArrayList<ApplicationId>();

    for (ApplicationIdProto c : list) {
      this.applicationsToCleanup.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public void addAllApplicationsToCleanup(
      final List<ApplicationId> applicationsToCleanup) {
    if (applicationsToCleanup == null)
      return;
    initApplicationsToCleanup();
    this.applicationsToCleanup.addAll(applicationsToCleanup);
  }

  private void addApplicationsToCleanupToProto() {
    maybeInitBuilder();
    builder.clearApplicationsToCleanup();
    if (applicationsToCleanup == null)
      return;
    Iterable<ApplicationIdProto> iterable = new Iterable<ApplicationIdProto>() {

      @Override
      public Iterator<ApplicationIdProto> iterator() {
        return new Iterator<ApplicationIdProto>() {

          Iterator<ApplicationId> iter = applicationsToCleanup.iterator();

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
    builder.addAllApplicationsToCleanup(iterable);
  }

  private void initContainersToDecrease() {
    if (this.containersToDecrease != null) {
      return;
    }
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerProto> list = p.getContainersToDecreaseList();
    this.containersToDecrease = new ArrayList<>();

    for (ContainerProto c : list) {
      this.containersToDecrease.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public List<Container> getContainersToDecrease() {
    initContainersToDecrease();
    return this.containersToDecrease;
  }

  @Override
  public void addAllContainersToDecrease(
      final Collection<Container> containersToDecrease) {
    if (containersToDecrease == null) {
      return;
    }
    initContainersToDecrease();
    this.containersToDecrease.addAll(containersToDecrease);
  }

  private void addContainersToDecreaseToProto() {
    maybeInitBuilder();
    builder.clearContainersToDecrease();
    if (this.containersToDecrease == null) {
      return;
    }
    Iterable<ContainerProto> iterable = new
        Iterable<ContainerProto>() {
      @Override
      public Iterator<ContainerProto> iterator() {
        return new Iterator<ContainerProto>() {
          private Iterator<Container> iter = containersToDecrease.iterator();
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
          @Override
          public ContainerProto next() {
            return convertToProtoFormat(iter.next());
          }
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    builder.addAllContainersToDecrease(iterable);
  }

  @Override
  public Map<ApplicationId, ByteBuffer> getSystemCredentialsForApps() {
    if (this.systemCredentials != null) {
      return this.systemCredentials;
    }
    initSystemCredentials();
    return systemCredentials;
  }

  private void initSystemCredentials() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<SystemCredentialsForAppsProto> list = p.getSystemCredentialsForAppsList();
    this.systemCredentials = new HashMap<ApplicationId, ByteBuffer> ();
    for (SystemCredentialsForAppsProto c : list) {
      ApplicationId appId = convertFromProtoFormat(c.getAppId());
      ByteBuffer byteBuffer = ProtoUtils.convertFromProtoFormat(c.getCredentialsForApp());
      this.systemCredentials.put(appId, byteBuffer);
    }
  }

  @Override
  public void setSystemCredentialsForApps(
      Map<ApplicationId, ByteBuffer> systemCredentials) {
    if (systemCredentials == null || systemCredentials.isEmpty()) {
      return;
    }
    maybeInitBuilder();
    this.systemCredentials = new HashMap<ApplicationId, ByteBuffer>();
    this.systemCredentials.putAll(systemCredentials);
  }

  @Override
  public long getNextHeartBeatInterval() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNextHeartBeatInterval());
  }

  @Override
  public void setNextHeartBeatInterval(long nextHeartBeatInterval) {
    maybeInitBuilder();
    builder.setNextHeartBeatInterval(nextHeartBeatInterval);
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ((ResourcePBImpl)t).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }

  private NodeAction convertFromProtoFormat(NodeActionProto p) {
    return NodeAction.valueOf(p.name());
  }

  private NodeActionProto convertToProtoFormat(NodeAction t) {
    return NodeActionProto.valueOf(t.name());
  }

  private MasterKeyPBImpl convertFromProtoFormat(MasterKeyProto p) {
    return new MasterKeyPBImpl(p);
  }

  private MasterKeyProto convertToProtoFormat(MasterKey t) {
    return ((MasterKeyPBImpl) t).getProto();
  }

  private ContainerPBImpl convertFromProtoFormat(ContainerProto p) {
    return new ContainerPBImpl(p);
  }

  private ContainerProto convertToProtoFormat(Container t) {
    return ((ContainerPBImpl) t).getProto();
  }

  @Override
  public boolean getAreNodeLabelsAcceptedByRM() {
    NodeHeartbeatResponseProtoOrBuilder p =
        this.viaProto ? this.proto : this.builder;
    return p.getAreNodeLabelsAcceptedByRM();
  }

  @Override
  public void setAreNodeLabelsAcceptedByRM(boolean areNodeLabelsAcceptedByRM) {
    maybeInitBuilder();
    this.builder.setAreNodeLabelsAcceptedByRM(areNodeLabelsAcceptedByRM);
  }

  @Override
  public List<SignalContainerRequest> getContainersToSignalList() {
    initContainersToSignal();
    return this.containersToSignal;
  }

  private void initContainersToSignal() {
    if (this.containersToSignal != null) {
      return;
    }
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<SignalContainerRequestProto> list = p.getContainersToSignalList();
    this.containersToSignal = new ArrayList<SignalContainerRequest>();

    for (SignalContainerRequestProto c : list) {
      this.containersToSignal.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public void addAllContainersToSignal(
      final List<SignalContainerRequest> containersToSignal) {
    if (containersToSignal == null)
      return;
    initContainersToSignal();
    this.containersToSignal.addAll(containersToSignal);
  }

  private void addContainersToSignalToProto() {
    maybeInitBuilder();
    builder.clearContainersToSignal();
    if (containersToSignal == null)
      return;

    Iterable<SignalContainerRequestProto> iterable =
        new Iterable<SignalContainerRequestProto>() {
          @Override
          public Iterator<SignalContainerRequestProto> iterator() {
            return new Iterator<SignalContainerRequestProto>() {
              Iterator<SignalContainerRequest> iter = containersToSignal.iterator();
              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public SignalContainerRequestProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        };
    builder.addAllContainersToSignal(iterable);
  }

  private ContainerQueuingLimit convertFromProtoFormat(
      ContainerQueuingLimitProto p) {
    return new ContainerQueuingLimitPBImpl(p);
  }

  private ContainerQueuingLimitProto convertToProtoFormat(
      ContainerQueuingLimit c) {
    return ((ContainerQueuingLimitPBImpl)c).getProto();
  }

  private SignalContainerRequestPBImpl convertFromProtoFormat(
      SignalContainerRequestProto p) {
    return new SignalContainerRequestPBImpl(p);
  }

  private SignalContainerRequestProto convertToProtoFormat(
      SignalContainerRequest t) {
    return ((SignalContainerRequestPBImpl)t).getProto();
  }
}

