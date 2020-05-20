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
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.FenceRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.FenceResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.server.protocol.FenceResponse;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * This class is the client side translator to translate the requests made on
 * {@link JournalProtocol} interfaces to the RPC server implementing
 * {@link JournalProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class JournalProtocolTranslatorPB implements ProtocolMetaInterface,
    JournalProtocol, Closeable {
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final JournalProtocolPB rpcProxy;
  
  public JournalProtocolTranslatorPB(JournalProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public void journal(JournalInfo journalInfo, long epoch, long firstTxnId,
      int numTxns, byte[] records) throws IOException {
    JournalRequestProto req = JournalRequestProto.newBuilder()
        .setJournalInfo(PBHelper.convert(journalInfo))
        .setEpoch(epoch)
        .setFirstTxnId(firstTxnId)
        .setNumTxns(numTxns)
        .setRecords(PBHelperClient.getByteString(records))
        .build();
    try {
      rpcProxy.journal(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void startLogSegment(JournalInfo journalInfo, long epoch, long txid)
      throws IOException {
    StartLogSegmentRequestProto req = StartLogSegmentRequestProto.newBuilder()
        .setJournalInfo(PBHelper.convert(journalInfo))
        .setEpoch(epoch)
        .setTxid(txid)
        .build();
    try {
      rpcProxy.startLogSegment(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
  
  @Override
  public FenceResponse fence(JournalInfo journalInfo, long epoch,
      String fencerInfo) throws IOException {
    FenceRequestProto req = FenceRequestProto.newBuilder().setEpoch(epoch)
        .setJournalInfo(PBHelper.convert(journalInfo)).build();
    try {
      FenceResponseProto resp = rpcProxy.fence(NULL_CONTROLLER, req);
      return new FenceResponse(resp.getPreviousEpoch(),
          resp.getLastTransactionId(), resp.getInSync());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy, JournalProtocolPB.class,
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(JournalProtocolPB.class), methodName);
  }
}
