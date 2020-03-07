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
package org.apache.hadoop.hdfs.server.federation.store.records.impl.pb;

import java.io.IOException;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipRecordProto.Builder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipRecordProtoOrBuilder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipStatsRecordProto;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationProtocolPBTranslator;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipStats;

import org.apache.hadoop.thirdparty.protobuf.Message;

/**
 * Protobuf implementation of the MembershipState record.
 */
public class MembershipStatePBImpl extends MembershipState implements PBRecord {

  private FederationProtocolPBTranslator<NamenodeMembershipRecordProto, Builder,
      NamenodeMembershipRecordProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<NamenodeMembershipRecordProto,
              Builder, NamenodeMembershipRecordProtoOrBuilder>(
                  NamenodeMembershipRecordProto.class);

  public MembershipStatePBImpl() {
  }

  public MembershipStatePBImpl(NamenodeMembershipRecordProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public NamenodeMembershipRecordProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Message proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void readInstance(String base64String) throws IOException {
    this.translator.readInstance(base64String);
  }

  @Override
  public void setRouterId(String routerId) {
    Builder builder = this.translator.getBuilder();
    if (routerId == null) {
      builder.clearRouterId();
    } else {
      builder.setRouterId(routerId);
    }
  }

  @Override
  public void setNameserviceId(String nameserviceId) {
    Builder builder = this.translator.getBuilder();
    if (nameserviceId == null) {
      builder.clearNameserviceId();
    } else {
      builder.setNameserviceId(nameserviceId);
    }
  }

  @Override
  public void setNamenodeId(String namenodeId) {
    Builder builder = this.translator.getBuilder();
    if (namenodeId == null) {
      builder.clearNamenodeId();
    } else {
      builder.setNamenodeId(namenodeId);
    }
  }

  @Override
  public void setWebAddress(String webAddress) {
    Builder builder = this.translator.getBuilder();
    if (webAddress == null) {
      builder.clearWebAddress();
    } else {
      builder.setWebAddress(webAddress);
    }
  }

  @Override
  public void setRpcAddress(String rpcAddress) {
    Builder builder = this.translator.getBuilder();
    if (rpcAddress == null) {
      builder.clearRpcAddress();
    } else {
      builder.setRpcAddress(rpcAddress);
    }
  }

  @Override
  public void setServiceAddress(String serviceAddress) {
    this.translator.getBuilder().setServiceAddress(serviceAddress);
  }

  @Override
  public void setLifelineAddress(String lifelineAddress) {
    Builder builder = this.translator.getBuilder();
    if (lifelineAddress == null) {
      builder.clearLifelineAddress();
    } else {
      builder.setLifelineAddress(lifelineAddress);
    }
  }

  @Override
  public void setIsSafeMode(boolean isSafeMode) {
    Builder builder = this.translator.getBuilder();
    builder.setIsSafeMode(isSafeMode);
  }

  @Override
  public void setClusterId(String clusterId) {
    Builder builder = this.translator.getBuilder();
    if (clusterId == null) {
      builder.clearClusterId();
    } else {
      builder.setClusterId(clusterId);
    }
  }

  @Override
  public void setBlockPoolId(String blockPoolId) {
    Builder builder = this.translator.getBuilder();
    if (blockPoolId == null) {
      builder.clearBlockPoolId();
    } else {
      builder.setBlockPoolId(blockPoolId);
    }
  }

  @Override
  public void setState(FederationNamenodeServiceState state) {
    Builder builder = this.translator.getBuilder();
    if (state == null) {
      builder.clearState();
    } else {
      builder.setState(state.toString());
    }
  }

  @Override
  public void setWebScheme(String webScheme) {
    Builder builder = this.translator.getBuilder();
    if (webScheme == null) {
      builder.clearWebScheme();
    } else {
      builder.setWebScheme(webScheme);
    }
  }

  @Override
  public String getRouterId() {
    NamenodeMembershipRecordProtoOrBuilder proto =
        this.translator.getProtoOrBuilder();
    if (!proto.hasRouterId()) {
      return null;
    }
    return proto.getRouterId();
  }

  @Override
  public String getNameserviceId() {
    NamenodeMembershipRecordProtoOrBuilder proto =
        this.translator.getProtoOrBuilder();
    if (!proto.hasNameserviceId()) {
      return null;
    }
    return this.translator.getProtoOrBuilder().getNameserviceId();
  }

  @Override
  public String getNamenodeId() {
    NamenodeMembershipRecordProtoOrBuilder proto =
        this.translator.getProtoOrBuilder();
    if (!proto.hasNamenodeId()) {
      return null;
    }
    return this.translator.getProtoOrBuilder().getNamenodeId();
  }

  @Override
  public String getClusterId() {
    NamenodeMembershipRecordProtoOrBuilder proto =
        this.translator.getProtoOrBuilder();
    if (!proto.hasClusterId()) {
      return null;
    }
    return this.translator.getProtoOrBuilder().getClusterId();
  }

  @Override
  public String getBlockPoolId() {
    NamenodeMembershipRecordProtoOrBuilder proto =
        this.translator.getProtoOrBuilder();
    if (!proto.hasBlockPoolId()) {
      return null;
    }
    return this.translator.getProtoOrBuilder().getBlockPoolId();
  }

  @Override
  public String getRpcAddress() {
    NamenodeMembershipRecordProtoOrBuilder proto =
        this.translator.getProtoOrBuilder();
    if (!proto.hasRpcAddress()) {
      return null;
    }
    return this.translator.getProtoOrBuilder().getRpcAddress();
  }

  @Override
  public String getServiceAddress() {
    NamenodeMembershipRecordProtoOrBuilder proto =
        this.translator.getProtoOrBuilder();
    if (!proto.hasServiceAddress()) {
      return null;
    }
    return this.translator.getProtoOrBuilder().getServiceAddress();
  }

  @Override
  public String getWebAddress() {
    NamenodeMembershipRecordProtoOrBuilder proto =
        this.translator.getProtoOrBuilder();
    if (!proto.hasWebAddress()) {
      return null;
    }
    return this.translator.getProtoOrBuilder().getWebAddress();
  }

  @Override
  public String getLifelineAddress() {
    NamenodeMembershipRecordProtoOrBuilder proto =
        this.translator.getProtoOrBuilder();
    if (!proto.hasLifelineAddress()) {
      return null;
    }
    return this.translator.getProtoOrBuilder().getLifelineAddress();
  }

  @Override
  public boolean getIsSafeMode() {
    return this.translator.getProtoOrBuilder().getIsSafeMode();
  }

  @Override
  public FederationNamenodeServiceState getState() {
    FederationNamenodeServiceState ret =
        FederationNamenodeServiceState.UNAVAILABLE;
    NamenodeMembershipRecordProtoOrBuilder proto =
        this.translator.getProtoOrBuilder();
    if (!proto.hasState()) {
      return null;
    }
    try {
      ret = FederationNamenodeServiceState.valueOf(proto.getState());
    } catch (IllegalArgumentException e) {
      // Ignore this error
    }
    return ret;
  }

  @Override
  public String getWebScheme() {
    NamenodeMembershipRecordProtoOrBuilder proto =
            this.translator.getProtoOrBuilder();
    if (!proto.hasWebScheme()) {
      return null;
    }
    return this.translator.getProtoOrBuilder().getWebScheme();
  }

  @Override
  public void setStats(MembershipStats stats) {
    if (stats instanceof MembershipStatsPBImpl) {
      MembershipStatsPBImpl statsPB = (MembershipStatsPBImpl)stats;
      NamenodeMembershipStatsRecordProto statsProto =
          (NamenodeMembershipStatsRecordProto)statsPB.getProto();
      this.translator.getBuilder().setStats(statsProto);
    }
  }

  @Override
  public MembershipStats getStats() {
    NamenodeMembershipStatsRecordProto statsProto =
        this.translator.getProtoOrBuilder().getStats();
    MembershipStats stats =
        StateStoreSerializer.newRecord(MembershipStats.class);
    if (stats instanceof MembershipStatsPBImpl) {
      MembershipStatsPBImpl statsPB = (MembershipStatsPBImpl)stats;
      statsPB.setProto(statsProto);
      return statsPB;
    } else {
      throw new IllegalArgumentException(
          "Cannot get stats for the membership");
    }
  }

  @Override
  public void setLastContact(long contact) {
    this.translator.getBuilder().setLastContact(contact);
  }

  @Override
  public long getLastContact() {
    return this.translator.getProtoOrBuilder().getLastContact();
  }

  @Override
  public void setDateModified(long time) {
    if (getState() != FederationNamenodeServiceState.EXPIRED) {
      this.translator.getBuilder().setDateModified(time);
    }
  }

  @Override
  public long getDateModified() {
    return this.translator.getProtoOrBuilder().getDateModified();
  }

  @Override
  public void setDateCreated(long time) {
    this.translator.getBuilder().setDateCreated(time);
  }

  @Override
  public long getDateCreated() {
    return this.translator.getProtoOrBuilder().getDateCreated();
  }
}