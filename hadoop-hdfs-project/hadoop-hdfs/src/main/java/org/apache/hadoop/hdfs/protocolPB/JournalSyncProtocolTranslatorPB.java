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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalSyncProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalSyncProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is the client side translator to translate the requests made on
 * {@link JournalSyncProtocol} interfaces to the RPC server implementing
 * {@link JournalSyncProtocolPB}.
 */
@InterfaceAudience.Private
public class JournalSyncProtocolTranslatorPB implements ProtocolMetaInterface,
    JournalSyncProtocol, Closeable {
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final JournalSyncProtocolPB rpcProxy;
  
  public JournalSyncProtocolTranslatorPB(JournalSyncProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }
  
  @Override
  public RemoteEditLogManifest getEditLogManifest(JournalInfo journalInfo, long sinceTxId)
      throws IOException {
    JournalInfoProto journalInfoProto = PBHelper.convert(journalInfo);
    GetEditLogManifestRequestProto req = GetEditLogManifestRequestProto
        .newBuilder().setJournalInfo(journalInfoProto).setSinceTxId(sinceTxId).build();
    
    try {
      return PBHelper.convert(rpcProxy.getEditLogManifest(NULL_CONTROLLER, req)
          .getManifest());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy, JournalSyncProtocolPB.class,
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(JournalSyncProtocolPB.class), methodName);
  }
}