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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.EnableNameserviceRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.EnableNameserviceRequestProto.Builder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.EnableNameserviceRequestProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnableNameserviceRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

import org.apache.hadoop.thirdparty.protobuf.Message;

/**
 * Protobuf implementation of the state store API object
 * EnableNameserviceRequest.
 */
public class EnableNameserviceRequestPBImpl extends EnableNameserviceRequest
    implements PBRecord {

  private FederationProtocolPBTranslator<EnableNameserviceRequestProto,
      Builder, EnableNameserviceRequestProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<>(
              EnableNameserviceRequestProto.class);

  public EnableNameserviceRequestPBImpl() {
  }

  public EnableNameserviceRequestPBImpl(EnableNameserviceRequestProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public EnableNameserviceRequestProto getProto() {
    return translator.build();
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
  public String getNameServiceId() {
    return this.translator.getProtoOrBuilder().getNameServiceId();
  }

  @Override
  public void setNameServiceId(String nsId) {
    this.translator.getBuilder().setNameServiceId(nsId);
  }
}
