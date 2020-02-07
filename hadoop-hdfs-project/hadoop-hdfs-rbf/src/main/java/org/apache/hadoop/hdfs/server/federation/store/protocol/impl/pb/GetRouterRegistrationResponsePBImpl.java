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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRouterRegistrationResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRouterRegistrationResponseProto.Builder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRouterRegistrationResponseProtoOrBuilder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RouterRecordProto;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.RouterStatePBImpl;

import org.apache.hadoop.thirdparty.protobuf.Message;

/**
 * Protobuf implementation of the state store API object
 * GetRouterRegistrationResponse.
 */
public class GetRouterRegistrationResponsePBImpl
    extends GetRouterRegistrationResponse implements PBRecord {

  private FederationProtocolPBTranslator<GetRouterRegistrationResponseProto,
      Builder, GetRouterRegistrationResponseProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<
              GetRouterRegistrationResponseProto, Builder,
              GetRouterRegistrationResponseProtoOrBuilder>(
                  GetRouterRegistrationResponseProto.class);

  public GetRouterRegistrationResponsePBImpl() {
  }

  @Override
  public GetRouterRegistrationResponseProto getProto() {
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
  public RouterState getRouter() {
    RouterRecordProto proto = this.translator.getProtoOrBuilder().getRouter();
    return new RouterStatePBImpl(proto);
  }

  @Override
  public void setRouter(RouterState router) {
    if (router instanceof RouterStatePBImpl) {
      RouterStatePBImpl routerPB = (RouterStatePBImpl)router;
      RouterRecordProto routerProto = routerPB.getProto();
      this.translator.getBuilder().setRouter(routerProto);
    }
  }
}