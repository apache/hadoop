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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.thirdparty.protobuf.Message;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoveMountTableEntriesResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoveMountTableEntriesResponseProto.Builder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoveMountTableEntriesResponseProtoOrBuilder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoveMountTableEntryFailureProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoveMountTableEntryFailureReasonProto;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

/**
 * Protobuf implementation of the state store API object
 * RemoveMountTableEntriesResponse.
 */
public class RemoveMountTableEntriesResponsePBImpl extends RemoveMountTableEntriesResponse
    implements PBRecord {

  private FederationProtocolPBTranslator<RemoveMountTableEntriesResponseProto, Builder,
      RemoveMountTableEntriesResponseProtoOrBuilder> translator
          = new FederationProtocolPBTranslator<>(RemoveMountTableEntriesResponseProto.class);

  public RemoveMountTableEntriesResponsePBImpl() {
  }

  public RemoveMountTableEntriesResponsePBImpl(RemoveMountTableEntriesResponseProto proto) {
    this.setProto(proto);
  }

  private static FailureReason convert(
      RemoveMountTableEntryFailureReasonProto proto) {
    return switch (proto) {
      case NONEXISTENT_MOUNT_POINT -> FailureReason.NONEXISTENT_MOUNT_POINT;
      case DRIVER_FAILURE -> FailureReason.DRIVER_FAILURE;
      case ACCESS_DENIED -> FailureReason.ACCESS_DENIED;
      default -> FailureReason.UNKNOWN_FAILURE;
    };
  }

  private static RemoveMountTableEntryFailureReasonProto convert(
      FailureReason reason) {
    return switch (reason) {
      case NONEXISTENT_MOUNT_POINT ->
          RemoveMountTableEntryFailureReasonProto.NONEXISTENT_MOUNT_POINT;
      case DRIVER_FAILURE -> RemoveMountTableEntryFailureReasonProto.DRIVER_FAILURE;
      case ACCESS_DENIED -> RemoveMountTableEntryFailureReasonProto.ACCESS_DENIED;
      default -> RemoveMountTableEntryFailureReasonProto.UNKNOWN_FAILURE;
    };
  }

  @Override
  public RemoveMountTableEntriesResponseProto getProto() {
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
  public List<EntryFailure> getFailedEntries() {
    List<EntryFailure> list = new ArrayList<>();
    for (RemoveMountTableEntryFailureProto p : this.translator.getProtoOrBuilder()
        .getFailedEntriesList()) {
      list.add(new EntryFailure(p.getSrcPath(), convert(p.getReason())));
    }
    return list;
  }

  @Override
  public void setStatus(boolean result) {
    this.translator.getBuilder().setStatus(result);
  }

  @Override
  public void setFailedEntries(List<EntryFailure> failedEntries) {
    Builder b = translator.getBuilder();
    b.clearFailedEntries();
    for (EntryFailure entry : failedEntries) {
      b.addFailedEntries(RemoveMountTableEntryFailureProto.newBuilder()
          .setSrcPath(entry.getSrcPath())
          .setReason(convert(entry.getReason()))
          .build());
    }
  }
}
