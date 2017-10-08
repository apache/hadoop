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
package org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb;

import java.io.IOException;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeHeartbeatRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeHeartbeatRequestProto.Builder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeHeartbeatRequestProtoOrBuilder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipRecordProto;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.MembershipStatePBImpl;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

import com.google.protobuf.Message;

/**
 * Protobuf implementation of the state store API object
 * NamenodeHeartbeatRequest.
 */
public class NamenodeHeartbeatRequestPBImpl
    extends NamenodeHeartbeatRequest implements PBRecord {

  private FederationProtocolPBTranslator<NamenodeHeartbeatRequestProto, Builder,
      NamenodeHeartbeatRequestProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<NamenodeHeartbeatRequestProto,
              Builder,
              NamenodeHeartbeatRequestProtoOrBuilder>(
                  NamenodeHeartbeatRequestProto.class);

  public NamenodeHeartbeatRequestPBImpl() {
  }

  @Override
  public NamenodeHeartbeatRequestProto getProto() {
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
  public MembershipState getNamenodeMembership() throws IOException {
    NamenodeMembershipRecordProto membershipProto =
        this.translator.getProtoOrBuilder().getNamenodeMembership();
    MembershipState membership =
        StateStoreSerializer.newRecord(MembershipState.class);
    if (membership instanceof MembershipStatePBImpl) {
      MembershipStatePBImpl membershipPB = (MembershipStatePBImpl)membership;
      membershipPB.setProto(membershipProto);
      return membershipPB;
    } else {
      throw new IOException("Cannot get membership from request");
    }
  }

  @Override
  public void setNamenodeMembership(MembershipState membership)
      throws IOException {
    if (membership instanceof MembershipStatePBImpl) {
      MembershipStatePBImpl membershipPB = (MembershipStatePBImpl)membership;
      NamenodeMembershipRecordProto membershipProto =
          (NamenodeMembershipRecordProto)membershipPB.getProto();
      this.translator.getBuilder().setNamenodeMembership(membershipProto);
    } else {
      throw new IOException("Cannot set mount table entry");
    }
  }
}