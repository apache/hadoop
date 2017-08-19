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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeLabelPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.AppCollectorDataProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeLabelsProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeLabelsProto.Builder;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.api.records.impl.pb.NodeStatusPBImpl;

public class NodeHeartbeatRequestPBImpl extends NodeHeartbeatRequest {
  NodeHeartbeatRequestProto proto = NodeHeartbeatRequestProto.getDefaultInstance();
  NodeHeartbeatRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private NodeStatus nodeStatus = null;
  private MasterKey lastKnownContainerTokenMasterKey = null;
  private MasterKey lastKnownNMTokenMasterKey = null;
  private Set<NodeLabel> labels = null;
  private List<LogAggregationReport> logAggregationReportsForApps = null;

  private Map<ApplicationId, AppCollectorData> registeringCollectors = null;

  public NodeHeartbeatRequestPBImpl() {
    builder = NodeHeartbeatRequestProto.newBuilder();
  }

  public NodeHeartbeatRequestPBImpl(NodeHeartbeatRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public NodeHeartbeatRequestProto getProto() {
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

  private void mergeLocalToBuilder() {
    if (this.nodeStatus != null) {
      builder.setNodeStatus(convertToProtoFormat(this.nodeStatus));
    }
    if (this.lastKnownContainerTokenMasterKey != null) {
      builder.setLastKnownContainerTokenMasterKey(
          convertToProtoFormat(this.lastKnownContainerTokenMasterKey));
    }
    if (this.lastKnownNMTokenMasterKey != null) {
      builder.setLastKnownNmTokenMasterKey(
          convertToProtoFormat(this.lastKnownNMTokenMasterKey));
    }
    if (this.labels != null) {
      builder.clearNodeLabels();
      Builder newBuilder = NodeLabelsProto.newBuilder();
      for (NodeLabel label : labels) {
        newBuilder.addNodeLabels(convertToProtoFormat(label));
      }
      builder.setNodeLabels(newBuilder.build());
    }
    if (this.logAggregationReportsForApps != null) {
      addLogAggregationStatusForAppsToProto();
    }
    if (this.registeringCollectors != null) {
      addRegisteringCollectorsToProto();
    }
  }

  private void addLogAggregationStatusForAppsToProto() {
    maybeInitBuilder();
    builder.clearLogAggregationReportsForApps();
    if (this.logAggregationReportsForApps == null) {
      return;
    }
    Iterable<LogAggregationReportProto> it =
        new Iterable<LogAggregationReportProto>() {
          @Override
          public Iterator<LogAggregationReportProto> iterator() {
            return new Iterator<LogAggregationReportProto>() {
              private Iterator<LogAggregationReport> iter =
                  logAggregationReportsForApps.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public LogAggregationReportProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        };
    builder.addAllLogAggregationReportsForApps(it);
  }

  private LogAggregationReportProto convertToProtoFormat(
      LogAggregationReport value) {
    return ((LogAggregationReportPBImpl) value).getProto();
  }

  private void addRegisteringCollectorsToProto() {
    maybeInitBuilder();
    builder.clearRegisteringCollectors();
    for (Map.Entry<ApplicationId, AppCollectorData> entry :
        registeringCollectors.entrySet()) {
      AppCollectorData data = entry.getValue();
      AppCollectorDataProto.Builder appCollectorDataBuilder =
          AppCollectorDataProto.newBuilder()
              .setAppId(convertToProtoFormat(entry.getKey()))
              .setAppCollectorAddr(data.getCollectorAddr())
              .setRmIdentifier(data.getRMIdentifier())
              .setVersion(data.getVersion());
      if (data.getCollectorToken() != null) {
        appCollectorDataBuilder.setAppCollectorToken(
            convertToProtoFormat(data.getCollectorToken()));
      }
      builder.addRegisteringCollectors(appCollectorDataBuilder);
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
      builder = NodeHeartbeatRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public NodeStatus getNodeStatus() {
    NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeStatus != null) {
      return this.nodeStatus;
    }
    if (!p.hasNodeStatus()) {
      return null;
    }
    this.nodeStatus = convertFromProtoFormat(p.getNodeStatus());
    return this.nodeStatus;
  }

  @Override
  public void setNodeStatus(NodeStatus nodeStatus) {
    maybeInitBuilder();
    if (nodeStatus == null) 
      builder.clearNodeStatus();
    this.nodeStatus = nodeStatus;
  }

  @Override
  public MasterKey getLastKnownContainerTokenMasterKey() {
    NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.lastKnownContainerTokenMasterKey != null) {
      return this.lastKnownContainerTokenMasterKey;
    }
    if (!p.hasLastKnownContainerTokenMasterKey()) {
      return null;
    }
    this.lastKnownContainerTokenMasterKey =
        convertFromProtoFormat(p.getLastKnownContainerTokenMasterKey());
    return this.lastKnownContainerTokenMasterKey;
  }

  @Override
  public void setLastKnownContainerTokenMasterKey(MasterKey masterKey) {
    maybeInitBuilder();
    if (masterKey == null) 
      builder.clearLastKnownContainerTokenMasterKey();
    this.lastKnownContainerTokenMasterKey = masterKey;
  }

  @Override
  public MasterKey getLastKnownNMTokenMasterKey() {
    NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.lastKnownNMTokenMasterKey != null) {
      return this.lastKnownNMTokenMasterKey;
    }
    if (!p.hasLastKnownNmTokenMasterKey()) {
      return null;
    }
    this.lastKnownNMTokenMasterKey =
        convertFromProtoFormat(p.getLastKnownNmTokenMasterKey());
    return this.lastKnownNMTokenMasterKey;
  }

  @Override
  public void setLastKnownNMTokenMasterKey(MasterKey masterKey) {
    maybeInitBuilder();
    if (masterKey == null) 
      builder.clearLastKnownNmTokenMasterKey();
    this.lastKnownNMTokenMasterKey = masterKey;
  }

  @Override
  public Map<ApplicationId, AppCollectorData> getRegisteringCollectors() {
    if (this.registeringCollectors != null) {
      return this.registeringCollectors;
    }
    initRegisteredCollectors();
    return registeringCollectors;
  }

  private void initRegisteredCollectors() {
    NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<AppCollectorDataProto> list = p.getRegisteringCollectorsList();
    if (!list.isEmpty()) {
      this.registeringCollectors = new HashMap<>();
      for (AppCollectorDataProto c : list) {
        ApplicationId appId = convertFromProtoFormat(c.getAppId());
        Token collectorToken = null;
        if (c.hasAppCollectorToken()){
          collectorToken = convertFromProtoFormat(c.getAppCollectorToken());
        }
        AppCollectorData data = AppCollectorData.newInstance(appId,
            c.getAppCollectorAddr(), c.getRmIdentifier(), c.getVersion(),
            collectorToken);
        this.registeringCollectors.put(appId, data);
      }
    }
  }

  @Override
  public void setRegisteringCollectors(
      Map<ApplicationId, AppCollectorData> registeredCollectors) {
    if (registeredCollectors == null || registeredCollectors.isEmpty()) {
      return;
    }
    maybeInitBuilder();
    this.registeringCollectors = new HashMap<>();
    this.registeringCollectors.putAll(registeredCollectors);
  }

  private NodeStatusPBImpl convertFromProtoFormat(NodeStatusProto p) {
    return new NodeStatusPBImpl(p);
  }

  private NodeStatusProto convertToProtoFormat(NodeStatus t) {
    return ((NodeStatusPBImpl)t).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }

  private MasterKeyPBImpl convertFromProtoFormat(MasterKeyProto p) {
    return new MasterKeyPBImpl(p);
  }

  private MasterKeyProto convertToProtoFormat(MasterKey t) {
    return ((MasterKeyPBImpl)t).getProto();
  }

  private TokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl) t).getProto();
  }

  @Override
  public Set<NodeLabel> getNodeLabels() {
    initNodeLabels();
    return this.labels;
  }

  @Override
  public void setNodeLabels(Set<NodeLabel> nodeLabels) {
    maybeInitBuilder();
    builder.clearNodeLabels();
    this.labels = nodeLabels;
  }
  
  private void initNodeLabels() {
    if (this.labels != null) {
      return;
    }
    NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeLabels()) {
      labels = null;
      return;
    }
    NodeLabelsProto nodeLabels = p.getNodeLabels();
    labels = new HashSet<NodeLabel>();
    for(NodeLabelProto nlp : nodeLabels.getNodeLabelsList()) {
      labels.add(convertFromProtoFormat(nlp));
    }
  }

  private NodeLabelPBImpl convertFromProtoFormat(NodeLabelProto p) {
    return new NodeLabelPBImpl(p);
  }

  private NodeLabelProto convertToProtoFormat(NodeLabel t) {
    return ((NodeLabelPBImpl)t).getProto();
  }

  @Override
  public List<LogAggregationReport> getLogAggregationReportsForApps() {
    if (this.logAggregationReportsForApps != null) {
      return this.logAggregationReportsForApps;
    }
    initLogAggregationReportsForApps();
    return logAggregationReportsForApps;
  }

  private void initLogAggregationReportsForApps() {
    NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<LogAggregationReportProto> list =
        p.getLogAggregationReportsForAppsList();
    this.logAggregationReportsForApps = new ArrayList<LogAggregationReport>();
    for (LogAggregationReportProto c : list) {
      this.logAggregationReportsForApps.add(convertFromProtoFormat(c));
    }
  }

  private LogAggregationReport convertFromProtoFormat(
      LogAggregationReportProto logAggregationReport) {
    return new LogAggregationReportPBImpl(logAggregationReport);
  }

  @Override
  public void setLogAggregationReportsForApps(
      List<LogAggregationReport> logAggregationStatusForApps) {
    if(logAggregationStatusForApps == null) {
      builder.clearLogAggregationReportsForApps();
    }
    this.logAggregationReportsForApps = logAggregationStatusForApps;
  }
}  
