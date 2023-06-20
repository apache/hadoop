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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.thirdparty.protobuf.Message;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntriesRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntriesRequestProtoOrBuilder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.MountTableRecordProto;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.MountTablePBImpl;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

/**
 * Protobuf implementation of the state store API object addMountTableEntriesRequest.
 */
public class AddMountTableEntriesRequestPBImpl
    extends AddMountTableEntriesRequest implements PBRecord {

  private final FederationProtocolPBTranslator<AddMountTableEntriesRequestProto,
      AddMountTableEntriesRequestProto.Builder,
      AddMountTableEntriesRequestProtoOrBuilder> translator =
      new FederationProtocolPBTranslator<>(AddMountTableEntriesRequestProto.class);

  public AddMountTableEntriesRequestPBImpl() {
  }

  public AddMountTableEntriesRequestPBImpl(AddMountTableEntriesRequestProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public AddMountTableEntriesRequestProto getProto() {
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
  public List<MountTable> getEntries() {
    List<MountTableRecordProto> entryProto = this.translator.getProtoOrBuilder().getEntryList();
    if (entryProto == null) {
      return null;
    }
    List<MountTable> mountTables = new ArrayList<>();
    entryProto.forEach(e -> mountTables.add(new MountTablePBImpl(e)));
    return mountTables;
  }

  @Override
  public void setEntries(List<MountTable> mountTables) {
    for (MountTable mountTable : mountTables) {
      if (mountTable instanceof MountTablePBImpl) {
        MountTablePBImpl mountPB = (MountTablePBImpl) mountTable;
        MountTableRecordProto mountProto = mountPB.getProto();
        translator.getBuilder().addEntry(mountProto);
      }
    }
  }
}