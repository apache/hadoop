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


package org.apache.hadoop.hdfs.qjournal.protocolPB;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

/**
 * This class is the client side translator to translate the requests made on
 * {@link InterQJournalProtocol} interfaces to the RPC server implementing
 * {@link InterQJournalProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class InterQJournalProtocolTranslatorPB implements ProtocolMetaInterface,
    InterQJournalProtocol, Closeable {

  /* RpcController is not used and hence is set to null. */
  private final static RpcController NULL_CONTROLLER = null;
  private final InterQJournalProtocolPB rpcProxy;

  public InterQJournalProtocolTranslatorPB(InterQJournalProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }


  @Override
  public GetEditLogManifestResponseProto getEditLogManifestFromJournal(
      String jid, String nameServiceId, long sinceTxId, boolean inProgressOk)
      throws IOException {
    GetEditLogManifestRequestProto.Builder req;
    req = GetEditLogManifestRequestProto.newBuilder()
        .setJid(convertJournalId(jid))
        .setSinceTxId(sinceTxId)
        .setInProgressOk(inProgressOk);
    if (nameServiceId !=null) {
      req.setNameServiceId(nameServiceId);
    }
    return ipc(() -> rpcProxy.getEditLogManifestFromJournal(NULL_CONTROLLER,
        req.build()));
  }

  private QJournalProtocolProtos.JournalIdProto convertJournalId(String jid) {
    return QJournalProtocolProtos.JournalIdProto.newBuilder()
        .setIdentifier(jid)
        .build();
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        InterQJournalProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(InterQJournalProtocolPB.class), methodName);
  }
}
