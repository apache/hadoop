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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRouterRegistrationsResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRouterRegistrationsResponseProto.Builder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRouterRegistrationsResponseProtoOrBuilder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RouterRecordProto;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRouterRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.RouterStatePBImpl;

import org.apache.hadoop.thirdparty.protobuf.Message;

/**
 * Protobuf implementation of the state store API object
 * GetRouterRegistrationsResponse.
 */
public class GetRouterRegistrationsResponsePBImpl
    extends GetRouterRegistrationsResponse implements PBRecord {

  private FederationProtocolPBTranslator<GetRouterRegistrationsResponseProto,
      Builder, GetRouterRegistrationsResponseProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<
              GetRouterRegistrationsResponseProto, Builder,
              GetRouterRegistrationsResponseProtoOrBuilder>(
                  GetRouterRegistrationsResponseProto.class);

  public GetRouterRegistrationsResponsePBImpl() {

  }

  @Override
  public GetRouterRegistrationsResponseProto getProto() {
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
  public List<RouterState> getRouters() throws IOException {

    List<RouterState> ret = new ArrayList<RouterState>();
    List<RouterRecordProto> memberships =
        this.translator.getProtoOrBuilder().getRoutersList();
    for (RouterRecordProto memberProto : memberships) {
      RouterState membership = new RouterStatePBImpl(memberProto);
      ret.add(membership);
    }
    return ret;
  }

  @Override
  public void setRouters(List<RouterState> records) throws IOException {

    this.translator.getBuilder().clearRouters();
    for (RouterState router : records) {
      if (router instanceof RouterStatePBImpl) {
        RouterStatePBImpl routerPB = (RouterStatePBImpl) router;
        this.translator.getBuilder().addRouters(routerPB.getProto());
      }
    }
  }

  @Override
  public long getTimestamp() {
    return this.translator.getProtoOrBuilder().getTimestamp();
  }

  @Override
  public void setTimestamp(long time) {
    this.translator.getBuilder().setTimestamp(time);
  }
}