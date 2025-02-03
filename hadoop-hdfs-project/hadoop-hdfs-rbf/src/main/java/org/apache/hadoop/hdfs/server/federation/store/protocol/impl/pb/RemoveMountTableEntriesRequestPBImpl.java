/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoveMountTableEntriesRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoveMountTableEntriesRequestProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

/**
 * Protobuf implementation of the state store API object
 * RemoveMountTableEntriesRequest.
 */
public class RemoveMountTableEntriesRequestPBImpl extends RemoveMountTableEntriesRequest
    implements PBRecord {

  private FederationProtocolPBTranslator<RemoveMountTableEntriesRequestProto, RemoveMountTableEntriesRequestProto.Builder, RemoveMountTableEntriesRequestProtoOrBuilder>
      translator =
      new FederationProtocolPBTranslator<RemoveMountTableEntriesRequestProto, RemoveMountTableEntriesRequestProto.Builder, RemoveMountTableEntriesRequestProtoOrBuilder>(
          RemoveMountTableEntriesRequestProto.class);

  public RemoveMountTableEntriesRequestPBImpl() {
  }

  public RemoveMountTableEntriesRequestPBImpl(RemoveMountTableEntriesRequestProto proto) {
    this.setProto(proto);
  }

  @Override
  public RemoveMountTableEntriesRequestProto getProto() {
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
  public List<String> getSrcPaths() {
    return this.translator.getProtoOrBuilder().getSrcPathsList();
  }

  @Override
  public void setSrcPaths(List<String> paths) {
    this.translator.getBuilder().addAllSrcPaths(paths);
  }
}