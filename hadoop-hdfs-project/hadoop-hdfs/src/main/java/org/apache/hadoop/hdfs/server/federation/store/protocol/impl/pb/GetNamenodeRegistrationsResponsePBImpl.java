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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetNamenodeRegistrationsResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetNamenodeRegistrationsResponseProtoOrBuilder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipRecordProto;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.MembershipStatePBImpl;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

import com.google.protobuf.Message;

/**
 * Protobuf implementation of the state store API object
 * GetNamenodeRegistrationsResponse.
 */
public class GetNamenodeRegistrationsResponsePBImpl
    extends GetNamenodeRegistrationsResponse implements PBRecord {

  private FederationProtocolPBTranslator<GetNamenodeRegistrationsResponseProto,
      GetNamenodeRegistrationsResponseProto.Builder,
      GetNamenodeRegistrationsResponseProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<
              GetNamenodeRegistrationsResponseProto,
              GetNamenodeRegistrationsResponseProto.Builder,
              GetNamenodeRegistrationsResponseProtoOrBuilder>(
                  GetNamenodeRegistrationsResponseProto.class);

  public GetNamenodeRegistrationsResponsePBImpl() {
  }

  public GetNamenodeRegistrationsResponsePBImpl(
      GetNamenodeRegistrationsResponseProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public GetNamenodeRegistrationsResponseProto getProto() {
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
  public List<MembershipState> getNamenodeMemberships()
      throws IOException {

    List<MembershipState> ret = new ArrayList<MembershipState>();
    List<NamenodeMembershipRecordProto> memberships =
        this.translator.getProtoOrBuilder().getNamenodeMembershipsList();
    for (NamenodeMembershipRecordProto memberProto : memberships) {
      MembershipState membership = new MembershipStatePBImpl(memberProto);
      ret.add(membership);
    }

    return ret;
  }

  @Override
  public void setNamenodeMemberships(List<MembershipState> records)
      throws IOException {
    for (MembershipState member : records) {
      if (member instanceof MembershipStatePBImpl) {
        MembershipStatePBImpl memberPB = (MembershipStatePBImpl)member;
        this.translator.getBuilder().addNamenodeMemberships(
            memberPB.getProto());
      }
    }
  }
}