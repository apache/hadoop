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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.UpdateNamenodeRegistrationRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.UpdateNamenodeRegistrationRequestProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateNamenodeRegistrationRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

import com.google.protobuf.Message;

/**
 * Protobuf implementation of the state store API object
 * OverrideNamenodeRegistrationRequest.
 */
public class UpdateNamenodeRegistrationRequestPBImpl
    extends UpdateNamenodeRegistrationRequest implements PBRecord {

  private FederationProtocolPBTranslator<
      UpdateNamenodeRegistrationRequestProto,
      UpdateNamenodeRegistrationRequestProto.Builder,
      UpdateNamenodeRegistrationRequestProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<
              UpdateNamenodeRegistrationRequestProto,
              UpdateNamenodeRegistrationRequestProto.Builder,
              UpdateNamenodeRegistrationRequestProtoOrBuilder>(
                  UpdateNamenodeRegistrationRequestProto.class);

  public UpdateNamenodeRegistrationRequestPBImpl() {
  }

  @Override
  public UpdateNamenodeRegistrationRequestProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Message protocol) {
    this.translator.setProto(protocol);
  }

  @Override
  public void readInstance(String base64String) throws IOException {
    this.translator.readInstance(base64String);
  }

  @Override
  public String getNameserviceId() {
    return this.translator.getProtoOrBuilder().getNameserviceId();
  }

  @Override
  public String getNamenodeId() {
    return this.translator.getProtoOrBuilder().getNamenodeId();
  }

  @Override
  public FederationNamenodeServiceState getState() {
    return FederationNamenodeServiceState
        .valueOf(this.translator.getProtoOrBuilder().getState());
  }

  @Override
  public void setNameserviceId(String nsId) {
    this.translator.getBuilder().setNameserviceId(nsId);
  }

  @Override
  public void setNamenodeId(String nnId) {
    this.translator.getBuilder().setNamenodeId(nnId);
  }

  @Override
  public void setState(FederationNamenodeServiceState state) {
    this.translator.getBuilder().setState(state.toString());
  }
}