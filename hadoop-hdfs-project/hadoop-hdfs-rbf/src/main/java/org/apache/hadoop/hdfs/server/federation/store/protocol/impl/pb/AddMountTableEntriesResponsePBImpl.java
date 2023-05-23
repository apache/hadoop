/*
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
import java.util.List;

import org.apache.hadoop.thirdparty.protobuf.Message;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntriesResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntriesResponseProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

/**
 * Protobuf implementation of the state store API object addMountTableEntriesResponse.
 */
public class AddMountTableEntriesResponsePBImpl
    extends AddMountTableEntriesResponse implements PBRecord {

  private final FederationProtocolPBTranslator<AddMountTableEntriesResponseProto,
      AddMountTableEntriesResponseProto.Builder,
      AddMountTableEntriesResponseProtoOrBuilder> translator =
      new FederationProtocolPBTranslator<>(AddMountTableEntriesResponseProto.class);

  public AddMountTableEntriesResponsePBImpl() {
  }

  public AddMountTableEntriesResponsePBImpl(
      AddMountTableEntriesResponseProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public AddMountTableEntriesResponseProto getProto() {
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
  public boolean getStatus() {
    return this.translator.getProtoOrBuilder().getStatus();
  }

  @Override
  public List<String> getFailedRecordsKeys() {
    return this.translator.getProtoOrBuilder().getFailedEntriesKeysList();
  }

  @Override
  public void setStatus(boolean result) {
    this.translator.getBuilder().setStatus(result);
  }

  @Override
  public void setFailedRecordsKeys(List<String> failedRecordsKeys) {
    this.translator.getBuilder().addAllFailedEntriesKeys(failedRecordsKeys);
  }
}