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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.LeaveSafeModeResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.LeaveSafeModeResponseProto.Builder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.LeaveSafeModeResponseProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.LeaveSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

import com.google.protobuf.Message;

/**
 * Protobuf implementation of the state store API object
 * LeaveSafeModeResponse.
 */
public class LeaveSafeModeResponsePBImpl extends LeaveSafeModeResponse
    implements PBRecord {

  private FederationProtocolPBTranslator<LeaveSafeModeResponseProto,
      Builder, LeaveSafeModeResponseProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<>(
              LeaveSafeModeResponseProto.class);

  public LeaveSafeModeResponsePBImpl() {
  }

  public LeaveSafeModeResponsePBImpl(LeaveSafeModeResponseProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public LeaveSafeModeResponseProto getProto() {
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
  public boolean getStatus() {
    return this.translator.getProtoOrBuilder().getStatus();
  }

  @Override
  public void setStatus(boolean result) {
    this.translator.getBuilder().setStatus(result);
  }
}
