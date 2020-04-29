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

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.MountTableRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.UpdateMountTableEntryRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.UpdateMountTableEntryRequestProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.MountTablePBImpl;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

import org.apache.hadoop.thirdparty.protobuf.Message;

/**
 * Protobuf implementation of the state store API object
 * UpdateMountTableEntryRequest.
 */
public class UpdateMountTableEntryRequestPBImpl
    extends UpdateMountTableEntryRequest implements PBRecord {

  private FederationProtocolPBTranslator<UpdateMountTableEntryRequestProto,
      UpdateMountTableEntryRequestProto.Builder,
      UpdateMountTableEntryRequestProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<UpdateMountTableEntryRequestProto,
              UpdateMountTableEntryRequestProto.Builder,
              UpdateMountTableEntryRequestProtoOrBuilder>(
                  UpdateMountTableEntryRequestProto.class);

  public UpdateMountTableEntryRequestPBImpl() {
  }

  public UpdateMountTableEntryRequestPBImpl(
      UpdateMountTableEntryRequestProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public UpdateMountTableEntryRequestProto getProto() {
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
  public MountTable getEntry() throws IOException {
    MountTableRecordProto statsProto =
        this.translator.getProtoOrBuilder().getEntry();
    MountTable stats = StateStoreSerializer.newRecord(MountTable.class);
    if (stats instanceof MountTablePBImpl) {
      MountTablePBImpl entryPB = (MountTablePBImpl)stats;
      entryPB.setProto(statsProto);
      return entryPB;
    } else {
      throw new IOException("Cannot get stats for the membership");
    }
  }

  @Override
  public void setEntry(MountTable mount) throws IOException {
    if (mount instanceof MountTablePBImpl) {
      MountTablePBImpl mountPB = (MountTablePBImpl)mount;
      MountTableRecordProto mountProto =
          (MountTableRecordProto)mountPB.getProto();
      this.translator.getBuilder().setEntry(mountProto);
    } else {
      throw new IOException("Cannot set mount table entry");
    }
  }
}