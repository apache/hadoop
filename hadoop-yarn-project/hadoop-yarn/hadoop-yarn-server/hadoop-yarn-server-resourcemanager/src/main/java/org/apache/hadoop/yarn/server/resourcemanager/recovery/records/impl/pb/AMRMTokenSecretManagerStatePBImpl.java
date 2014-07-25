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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb;

import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;

public class AMRMTokenSecretManagerStatePBImpl extends AMRMTokenSecretManagerState{
  AMRMTokenSecretManagerStateProto proto =
      AMRMTokenSecretManagerStateProto.getDefaultInstance();
  AMRMTokenSecretManagerStateProto.Builder builder = null;
  boolean viaProto = false;

  private MasterKey currentMasterKey = null;
  private MasterKey nextMasterKey = null;

  public AMRMTokenSecretManagerStatePBImpl() {
    builder = AMRMTokenSecretManagerStateProto.newBuilder();
  }

  public AMRMTokenSecretManagerStatePBImpl(AMRMTokenSecretManagerStateProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public AMRMTokenSecretManagerStateProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.currentMasterKey != null) {
      builder.setCurrentMasterKey(convertToProtoFormat(this.currentMasterKey));
    }
    if (this.nextMasterKey != null) {
      builder.setNextMasterKey(convertToProtoFormat(this.nextMasterKey));
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
      builder = AMRMTokenSecretManagerStateProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public MasterKey getCurrentMasterKey() {
    AMRMTokenSecretManagerStateProtoOrBuilder p = viaProto ? proto : builder;
    if (this.currentMasterKey != null) {
      return this.currentMasterKey;
    }
    if (!p.hasCurrentMasterKey()) {
      return null;
    }
    this.currentMasterKey = convertFromProtoFormat(p.getCurrentMasterKey());
    return this.currentMasterKey;
  }

  @Override
  public void setCurrentMasterKey(MasterKey currentMasterKey) {
    maybeInitBuilder();
    if (currentMasterKey == null)
      builder.clearCurrentMasterKey();
    this.currentMasterKey = currentMasterKey;
  }

  @Override
  public MasterKey getNextMasterKey() {
    AMRMTokenSecretManagerStateProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nextMasterKey != null) {
      return this.nextMasterKey;
    }
    if (!p.hasNextMasterKey()) {
      return null;
    }
    this.nextMasterKey = convertFromProtoFormat(p.getNextMasterKey());
    return this.nextMasterKey;
  }

  @Override
  public void setNextMasterKey(MasterKey nextMasterKey) {
    maybeInitBuilder();
    if (nextMasterKey == null)
      builder.clearNextMasterKey();
    this.nextMasterKey = nextMasterKey;
  }

  private MasterKeyProto convertToProtoFormat(MasterKey t) {
    return ((MasterKeyPBImpl) t).getProto();
  }

  private MasterKeyPBImpl convertFromProtoFormat(MasterKeyProto p) {
    return new MasterKeyPBImpl(p);
  }
}
