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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntryRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntryRequestProtoOrBuilder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.MountTableRecordProto;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.MountTablePBImpl;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

import org.apache.hadoop.thirdparty.protobuf.Message;

/**
 * Protobuf implementation of the state store API object
 * AddMountTableEntryRequest.
 */
public class AddMountTableEntryRequestPBImpl
    extends AddMountTableEntryRequest implements PBRecord {

  private FederationProtocolPBTranslator<AddMountTableEntryRequestProto,
      AddMountTableEntryRequestProto.Builder,
      AddMountTableEntryRequestProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<AddMountTableEntryRequestProto,
              AddMountTableEntryRequestProto.Builder,
              AddMountTableEntryRequestProtoOrBuilder>(
                  AddMountTableEntryRequestProto.class);

  public AddMountTableEntryRequestPBImpl() {
  }

  public AddMountTableEntryRequestPBImpl(AddMountTableEntryRequestProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public AddMountTableEntryRequestProto getProto() {
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
  public MountTable getEntry() {
    MountTableRecordProto entryProto =
        this.translator.getProtoOrBuilder().getEntry();
    return new MountTablePBImpl(entryProto);
  }

  @Override
  public void setEntry(MountTable mount) {
    if (mount instanceof MountTablePBImpl) {
      MountTablePBImpl mountPB = (MountTablePBImpl)mount;
      MountTableRecordProto mountProto = mountPB.getProto();
      translator.getBuilder().setEntry(mountProto);
    }
  }
}