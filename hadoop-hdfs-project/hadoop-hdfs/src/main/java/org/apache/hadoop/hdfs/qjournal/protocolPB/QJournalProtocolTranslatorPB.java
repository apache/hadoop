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

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.AcceptRecoveryRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.CanRollBackRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.CanRollBackResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DiscardSegmentsRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoFinalizeRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoPreUpgradeRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoRollbackRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoUpgradeRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FinalizeLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FormatRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalCTimeRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalCTimeResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.IsFormattedRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.IsFormattedResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalIdProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PurgeLogsRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.RequestInfoProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is the client side translator to translate the requests made on
 * {@link JournalProtocol} interfaces to the RPC server implementing
 * {@link JournalProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class QJournalProtocolTranslatorPB implements ProtocolMetaInterface,
    QJournalProtocol, Closeable {
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final QJournalProtocolPB rpcProxy;
  
  public QJournalProtocolTranslatorPB(QJournalProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }


  @Override
  public boolean isFormatted(String journalId,
                             String nameServiceId) throws IOException {
    try {
      IsFormattedRequestProto.Builder req = IsFormattedRequestProto.newBuilder()
          .setJid(convertJournalId(journalId));
      if (nameServiceId != null) {
        req.setNameServiceId(nameServiceId);
      }

      IsFormattedResponseProto resp = rpcProxy.isFormatted(
          NULL_CONTROLLER, req.build());
      return resp.getIsFormatted();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public GetJournalStateResponseProto getJournalState(String jid,
                                                      String nameServiceId)
      throws IOException {
    try {
      GetJournalStateRequestProto.Builder req = GetJournalStateRequestProto
          .newBuilder()
          .setJid(convertJournalId(jid));
      if (nameServiceId != null) {
        req.setNameServiceId(nameServiceId);
      }
      return rpcProxy.getJournalState(NULL_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private JournalIdProto convertJournalId(String jid) {
    return JournalIdProto.newBuilder()
        .setIdentifier(jid)
        .build();
  }
  
  @Override
  public void format(String jid,
                     String nameServiceId,
                     NamespaceInfo nsInfo,
                     boolean force) throws IOException {
    try {
      FormatRequestProto.Builder req = FormatRequestProto.newBuilder()
          .setJid(convertJournalId(jid))
          .setNsInfo(PBHelper.convert(nsInfo))
          .setForce(force);
      if(nameServiceId != null) {
        req.setNameServiceId(nameServiceId);
      }

      rpcProxy.format(NULL_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public NewEpochResponseProto newEpoch(String jid,
                                        String nameServiceId,
                                        NamespaceInfo nsInfo,
                                        long epoch) throws IOException {
    try {
      NewEpochRequestProto.Builder req = NewEpochRequestProto.newBuilder()
          .setJid(convertJournalId(jid))
          .setNsInfo(PBHelper.convert(nsInfo))
          .setEpoch(epoch);

      if(nameServiceId != null) {
        req.setNameServiceId(nameServiceId);
      }

      return rpcProxy.newEpoch(NULL_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void journal(RequestInfo reqInfo,
      long segmentTxId, long firstTxnId, int numTxns,
      byte[] records) throws IOException {
    JournalRequestProto req = JournalRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setSegmentTxnId(segmentTxId)
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
  public void heartbeat(RequestInfo reqInfo) throws IOException {
    try {
      rpcProxy.heartbeat(NULL_CONTROLLER, HeartbeatRequestProto.newBuilder()
            .setReqInfo(convert(reqInfo))
            .build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private QJournalProtocolProtos.RequestInfoProto convert(
      RequestInfo reqInfo) {
    RequestInfoProto.Builder builder = RequestInfoProto.newBuilder()
        .setJournalId(convertJournalId(reqInfo.getJournalId()))
        .setEpoch(reqInfo.getEpoch())
        .setIpcSerialNumber(reqInfo.getIpcSerialNumber());
    if (reqInfo.hasCommittedTxId()) {
      builder.setCommittedTxId(reqInfo.getCommittedTxId());
    }
    if(reqInfo.getNameServiceId() != null) {
      builder.setNameServiceId(reqInfo.getNameServiceId());
    }
    return builder.build();
  }

  @Override
  public void startLogSegment(RequestInfo reqInfo, long txid, int layoutVersion)
      throws IOException {
    StartLogSegmentRequestProto req = StartLogSegmentRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setTxid(txid).setLayoutVersion(layoutVersion)
        .build();
    try {
      rpcProxy.startLogSegment(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
  
  @Override
  public void finalizeLogSegment(RequestInfo reqInfo, long startTxId,
      long endTxId) throws IOException {
    FinalizeLogSegmentRequestProto req =
        FinalizeLogSegmentRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setStartTxId(startTxId)
        .setEndTxId(endTxId)
        .build();
    try {
      rpcProxy.finalizeLogSegment(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
  
  @Override
  public void purgeLogsOlderThan(RequestInfo reqInfo, long minTxIdToKeep)
      throws IOException {
    PurgeLogsRequestProto req = PurgeLogsRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setMinTxIdToKeep(minTxIdToKeep)
        .build();
    try {
      rpcProxy.purgeLogs(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public GetEditLogManifestResponseProto getEditLogManifest(
      String jid, String nameServiceId,
       long sinceTxId, boolean inProgressOk) throws IOException {
    try {
      GetEditLogManifestRequestProto.Builder req;
      req = GetEditLogManifestRequestProto.newBuilder()
          .setJid(convertJournalId(jid))
          .setSinceTxId(sinceTxId)
          .setInProgressOk(inProgressOk);
      if (nameServiceId !=null) {
        req.setNameServiceId(nameServiceId);
      }
      return rpcProxy.getEditLogManifest(NULL_CONTROLLER,
          req.build()
          );
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public PrepareRecoveryResponseProto prepareRecovery(RequestInfo reqInfo,
      long segmentTxId) throws IOException {
    try {
      return rpcProxy.prepareRecovery(NULL_CONTROLLER,
          PrepareRecoveryRequestProto.newBuilder()
            .setReqInfo(convert(reqInfo))
            .setSegmentTxId(segmentTxId)
            .build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void acceptRecovery(RequestInfo reqInfo,
      SegmentStateProto stateToAccept, URL fromUrl) throws IOException {
    try {
      rpcProxy.acceptRecovery(NULL_CONTROLLER,
          AcceptRecoveryRequestProto.newBuilder()
            .setReqInfo(convert(reqInfo))
            .setStateToAccept(stateToAccept)
            .setFromURL(fromUrl.toExternalForm())
            .build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        QJournalProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(QJournalProtocolPB.class), methodName);
  }

  @Override
  public void doPreUpgrade(String jid) throws IOException {
    try {
      DoPreUpgradeRequestProto.Builder req;
      req = DoPreUpgradeRequestProto.newBuilder()
          .setJid(convertJournalId(jid));
      rpcProxy.doPreUpgrade(NULL_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void doUpgrade(String journalId, StorageInfo sInfo) throws IOException {
    try {
      rpcProxy.doUpgrade(NULL_CONTROLLER,
          DoUpgradeRequestProto.newBuilder()
            .setJid(convertJournalId(journalId))
            .setSInfo(PBHelper.convert(sInfo))
            .build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
  
  @Override
  public void doFinalize(String jid, String nameServiceId) throws IOException {
    try {
      DoFinalizeRequestProto.Builder req = DoFinalizeRequestProto
          .newBuilder()
          .setJid(convertJournalId(jid));
      if (nameServiceId != null) {
        req.setNameServiceId(nameServiceId);
      }
      rpcProxy.doFinalize(NULL_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public Boolean canRollBack(String journalId,
                             String nameServiceId,
                             StorageInfo storage,
                             StorageInfo prevStorage,
                             int targetLayoutVersion) throws IOException {
    try {
      CanRollBackRequestProto.Builder req = CanRollBackRequestProto.newBuilder()
          .setJid(convertJournalId(journalId))
          .setStorage(PBHelper.convert(storage))
          .setPrevStorage(PBHelper.convert(prevStorage))
          .setTargetLayoutVersion(targetLayoutVersion);
      if (nameServiceId != null) {
        req.setNameServiceId(nameServiceId);
      }
      CanRollBackResponseProto response = rpcProxy.canRollBack(
          NULL_CONTROLLER, req.build());
      return response.getCanRollBack();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void doRollback(String journalId,
                         String nameServiceId) throws IOException {
    try {
      DoRollbackRequestProto.Builder req = DoRollbackRequestProto.newBuilder()
          .setJid(convertJournalId(journalId));

      if (nameServiceId != null) {
        req.setNameserviceId(nameServiceId);
      }
      rpcProxy.doRollback(NULL_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void discardSegments(String journalId,
                              String nameServiceId,
                              long startTxId)
      throws IOException {
    try {
      DiscardSegmentsRequestProto.Builder req = DiscardSegmentsRequestProto
          .newBuilder()
          .setJid(convertJournalId(journalId)).setStartTxId(startTxId);

      if (nameServiceId != null) {
        req.setNameServiceId(nameServiceId);
      }
      rpcProxy.discardSegments(NULL_CONTROLLER, req.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public Long getJournalCTime(String journalId,
                              String nameServiceId) throws IOException {
    try {

      GetJournalCTimeRequestProto.Builder req = GetJournalCTimeRequestProto
          .newBuilder()
          .setJid(convertJournalId(journalId));
      if(nameServiceId !=null) {
        req.setNameServiceId(nameServiceId);
      }
      GetJournalCTimeResponseProto response = rpcProxy.getJournalCTime(
          NULL_CONTROLLER, req.build());
      return response.getResultCTime();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

}
