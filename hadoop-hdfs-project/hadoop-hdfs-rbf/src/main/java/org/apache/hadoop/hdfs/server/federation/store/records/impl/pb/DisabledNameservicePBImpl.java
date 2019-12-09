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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.DisabledNameserviceRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.DisabledNameserviceRecordProto.Builder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.DisabledNameserviceRecordProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationProtocolPBTranslator;
import org.apache.hadoop.hdfs.server.federation.store.records.DisabledNameservice;

import com.google.protobuf.Message;

/**
 * Protobuf implementation of the {@link DisabledNameservice} record.
 */
public class DisabledNameservicePBImpl extends DisabledNameservice
    implements PBRecord {

  private FederationProtocolPBTranslator<DisabledNameserviceRecordProto,
      Builder, DisabledNameserviceRecordProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<
              DisabledNameserviceRecordProto, Builder,
              DisabledNameserviceRecordProtoOrBuilder>(
                  DisabledNameserviceRecordProto.class);

  public DisabledNameservicePBImpl() {
  }

  public DisabledNameservicePBImpl(
      DisabledNameserviceRecordProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public DisabledNameserviceRecordProto getProto() {
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
  public String getNameserviceId() {
    return this.translator.getProtoOrBuilder().getNameServiceId();
  }

  @Override
  public void setNameserviceId(String nameServiceId) {
    this.translator.getBuilder().setNameServiceId(nameServiceId);
  }

  @Override
  public void setDateModified(long time) {
    this.translator.getBuilder().setDateModified(time);
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
