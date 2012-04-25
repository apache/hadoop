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
package org.apache.hadoop.hdfs.protocolPB;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.proto.JournalSyncProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalSyncProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalSyncProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link JournalSyncProtocolPB} to the 
 * {@link JournalSyncProtocol} server implementation.
 */
@InterfaceAudience.Private
public class JournalSyncProtocolServerSideTranslatorPB implements JournalSyncProtocolPB {
  /** Server side implementation to delegate the requests to */
  private final JournalSyncProtocol impl;

  public JournalSyncProtocolServerSideTranslatorPB(JournalSyncProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GetEditLogManifestResponseProto getEditLogManifest(
      RpcController unused, GetEditLogManifestRequestProto request)
      throws ServiceException {
    RemoteEditLogManifest manifest;
    try {
      JournalInfo journalInfo = PBHelper.convert(request.getJournalInfo());
      manifest = impl.getEditLogManifest(journalInfo, request.getSinceTxId());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetEditLogManifestResponseProto.newBuilder()
        .setManifest(PBHelper.convert(manifest)).build();
  }
}
