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


import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeActionProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;


    
public class RegisterNodeManagerResponsePBImpl extends ProtoBase<RegisterNodeManagerResponseProto> implements RegisterNodeManagerResponse {
  RegisterNodeManagerResponseProto proto = RegisterNodeManagerResponseProto.getDefaultInstance();
  RegisterNodeManagerResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private MasterKey containerTokenMasterKey = null;
  private MasterKey nmTokenMasterKey = null;
  
  private boolean rebuild = false;
  
  public RegisterNodeManagerResponsePBImpl() {
    builder = RegisterNodeManagerResponseProto.newBuilder();
  }

  public RegisterNodeManagerResponsePBImpl(RegisterNodeManagerResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RegisterNodeManagerResponseProto getProto() {
    if (rebuild)
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.containerTokenMasterKey != null) {
      builder.setContainerTokenMasterKey(
          convertToProtoFormat(this.containerTokenMasterKey));
    }
    if (this.nmTokenMasterKey != null) {
      builder.setNmTokenMasterKey(
          convertToProtoFormat(this.nmTokenMasterKey));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    rebuild = false;
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterNodeManagerResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public MasterKey getContainerTokenMasterKey() {
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
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
    rebuild = true;
  }

  @Override
  public MasterKey getNMTokenMasterKey() {
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
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
    rebuild = true;
  }

  @Override
  public String getDiagnosticsMessage() {
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
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
  public String getRMVersion() {
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasRmVersion()) {
      return null;
    }
    return p.getRmVersion();
  }

  @Override
  public void setRMVersion(String rmVersion) {
    maybeInitBuilder();
    if (rmVersion == null) {
      builder.clearRmIdentifier();
      return;
    }
    builder.setRmVersion(rmVersion);
  }

  @Override
  public NodeAction getNodeAction() {
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasNodeAction()) {
      return null;
    }
    return convertFromProtoFormat(p.getNodeAction());
  }

  @Override
  public void setNodeAction(NodeAction nodeAction) {
    maybeInitBuilder();
    if (nodeAction == null) {
      builder.clearNodeAction();
    } else {
      builder.setNodeAction(convertToProtoFormat(nodeAction));
    }
    rebuild = true;
  }

  @Override
  public long getRMIdentifier() {
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getRmIdentifier());
  }

  @Override
  public void setRMIdentifier(long rmIdentifier) {
    maybeInitBuilder();
    builder.setRmIdentifier(rmIdentifier);
  }

  private NodeAction convertFromProtoFormat(NodeActionProto p) {
    return  NodeAction.valueOf(p.name());
  }

  private NodeActionProto convertToProtoFormat(NodeAction t) {
    return NodeActionProto.valueOf(t.name());
  }

  private MasterKeyPBImpl convertFromProtoFormat(MasterKeyProto p) {
    return new MasterKeyPBImpl(p);
  }

  private MasterKeyProto convertToProtoFormat(MasterKey t) {
    return ((MasterKeyPBImpl)t).getProto();
  }
}  
