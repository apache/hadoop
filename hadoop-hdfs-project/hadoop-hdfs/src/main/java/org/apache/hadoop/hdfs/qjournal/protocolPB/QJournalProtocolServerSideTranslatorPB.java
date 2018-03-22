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

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.AcceptRecoveryRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.AcceptRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.CanRollBackRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.CanRollBackResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DiscardSegmentsRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DiscardSegmentsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoFinalizeRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoFinalizeResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoPreUpgradeRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoPreUpgradeResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoRollbackRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoRollbackResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoUpgradeRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoUpgradeResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FinalizeLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FinalizeLogSegmentResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FormatRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FormatResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalCTimeRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalCTimeResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.HeartbeatResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.IsFormattedRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.IsFormattedResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalIdProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PurgeLogsRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PurgeLogsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.StartLogSegmentResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link JournalProtocolPB} to the 
 * {@link JournalProtocol} server implementation.
 */
@InterfaceAudience.Private
public class QJournalProtocolServerSideTranslatorPB implements QJournalProtocolPB {
  /** Server side implementation to delegate the requests to */
  private final QJournalProtocol impl;

  private final static JournalResponseProto VOID_JOURNAL_RESPONSE =
  JournalResponseProto.newBuilder().build();

  private final static StartLogSegmentResponseProto
  VOID_START_LOG_SEGMENT_RESPONSE =
      StartLogSegmentResponseProto.newBuilder().build();

  public QJournalProtocolServerSideTranslatorPB(QJournalProtocol impl) {
    this.impl = impl;
  }

  
  @Override
  public IsFormattedResponseProto isFormatted(RpcController controller,
      IsFormattedRequestProto request) throws ServiceException {
    try {
      boolean ret = impl.isFormatted(
          convert(request.getJid()),
          request.hasNameServiceId() ? request.getNameServiceId() : null);
      return IsFormattedResponseProto.newBuilder()
          .setIsFormatted(ret)
          .build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }


  @Override
  public GetJournalStateResponseProto getJournalState(RpcController controller,
      GetJournalStateRequestProto request) throws ServiceException {
    try {
      return impl.getJournalState(
          convert(request.getJid()),
          request.hasNameServiceId() ? request.getNameServiceId() : null);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  private String convert(JournalIdProto jid) {
    return jid.getIdentifier();
  }

  @Override
  public NewEpochResponseProto newEpoch(RpcController controller,
      NewEpochRequestProto request) throws ServiceException {
    try {
      return impl.newEpoch(
          request.getJid().getIdentifier(),
          request.hasNameServiceId() ? request.getNameServiceId() : null,
          PBHelper.convert(request.getNsInfo()),
          request.getEpoch());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  public FormatResponseProto format(RpcController controller,
      FormatRequestProto request) throws ServiceException {
    try {
      impl.format(request.getJid().getIdentifier(),
          request.hasNameServiceId() ? request.getNameServiceId() : null,
          PBHelper.convert(request.getNsInfo()));
      return FormatResponseProto.getDefaultInstance();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }
  
  /** @see JournalProtocol#journal */
  @Override
  public JournalResponseProto journal(RpcController unused,
      JournalRequestProto req) throws ServiceException {
    try {
      impl.journal(convert(req.getReqInfo()),
          req.getSegmentTxnId(), req.getFirstTxnId(),
          req.getNumTxns(), req.getRecords().toByteArray());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_JOURNAL_RESPONSE;
  }

  /** @see JournalProtocol#heartbeat */
  @Override
  public HeartbeatResponseProto heartbeat(RpcController controller,
      HeartbeatRequestProto req) throws ServiceException {
    try {
      impl.heartbeat(convert(req.getReqInfo()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return HeartbeatResponseProto.getDefaultInstance();
  }

  /** @see JournalProtocol#startLogSegment */
  @Override
  public StartLogSegmentResponseProto startLogSegment(RpcController controller,
      StartLogSegmentRequestProto req) throws ServiceException {
    try {
      int layoutVersion = req.hasLayoutVersion() ? req.getLayoutVersion()
          : NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION;
      impl.startLogSegment(convert(req.getReqInfo()), req.getTxid(),
          layoutVersion);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_START_LOG_SEGMENT_RESPONSE;
  }
  
  @Override
  public FinalizeLogSegmentResponseProto finalizeLogSegment(
      RpcController controller, FinalizeLogSegmentRequestProto req)
      throws ServiceException {
    try {
      impl.finalizeLogSegment(convert(req.getReqInfo()),
          req.getStartTxId(), req.getEndTxId());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return FinalizeLogSegmentResponseProto.newBuilder().build();
  }
  
  @Override
  public PurgeLogsResponseProto purgeLogs(RpcController controller,
      PurgeLogsRequestProto req) throws ServiceException {
    try {
      impl.purgeLogsOlderThan(convert(req.getReqInfo()),
          req.getMinTxIdToKeep());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return PurgeLogsResponseProto.getDefaultInstance();
  }

  @Override
  public GetEditLogManifestResponseProto getEditLogManifest(
      RpcController controller, GetEditLogManifestRequestProto request)
      throws ServiceException {
    try {
      return impl.getEditLogManifest(
          request.getJid().getIdentifier(),
          request.hasNameServiceId() ? request.getNameServiceId() : null,
          request.getSinceTxId(),
          request.getInProgressOk());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }


  @Override
  public PrepareRecoveryResponseProto prepareRecovery(RpcController controller,
      PrepareRecoveryRequestProto request) throws ServiceException {
    try {
      return impl.prepareRecovery(convert(request.getReqInfo()),
          request.getSegmentTxId());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public AcceptRecoveryResponseProto acceptRecovery(RpcController controller,
      AcceptRecoveryRequestProto request) throws ServiceException {
    try {
      impl.acceptRecovery(convert(request.getReqInfo()),
          request.getStateToAccept(),
          new URL(request.getFromURL()));
      return AcceptRecoveryResponseProto.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  
  private RequestInfo convert(
      QJournalProtocolProtos.RequestInfoProto reqInfo) {
    return new RequestInfo(
        reqInfo.getJournalId().getIdentifier(),
        reqInfo.hasNameServiceId() ?
            reqInfo.getNameServiceId() : null,
        reqInfo.getEpoch(),
        reqInfo.getIpcSerialNumber(),
        reqInfo.hasCommittedTxId() ?
          reqInfo.getCommittedTxId() : HdfsServerConstants.INVALID_TXID);
  }


  @Override
  public DoPreUpgradeResponseProto doPreUpgrade(RpcController controller,
      DoPreUpgradeRequestProto request) throws ServiceException {
    try {
      impl.doPreUpgrade(convert(request.getJid()));
      return DoPreUpgradeResponseProto.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DoUpgradeResponseProto doUpgrade(RpcController controller,
      DoUpgradeRequestProto request) throws ServiceException {
    StorageInfo si = PBHelper.convert(request.getSInfo(), NodeType.JOURNAL_NODE);
    try {
      impl.doUpgrade(convert(request.getJid()), si);
      return DoUpgradeResponseProto.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DoFinalizeResponseProto doFinalize(RpcController controller,
      DoFinalizeRequestProto request) throws ServiceException {
    try {
      impl.doFinalize(convert(request.getJid()),
          request.hasNameServiceId() ? request.getNameServiceId() : null);
      return DoFinalizeResponseProto.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CanRollBackResponseProto canRollBack(RpcController controller,
      CanRollBackRequestProto request) throws ServiceException {
    try {
      StorageInfo si = PBHelper.convert(request.getStorage(), NodeType.JOURNAL_NODE);
      Boolean result = impl.canRollBack(convert(request.getJid()),
          request.hasNameServiceId() ? request.getNameServiceId() : null,
          si,
          PBHelper.convert(request.getPrevStorage(), NodeType.JOURNAL_NODE),
          request.getTargetLayoutVersion());
      return CanRollBackResponseProto.newBuilder()
          .setCanRollBack(result)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DoRollbackResponseProto doRollback(RpcController controller, DoRollbackRequestProto request)
      throws ServiceException {
    try {
      impl.doRollback(convert(request.getJid()), request.getNameserviceId());
      return DoRollbackResponseProto.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DiscardSegmentsResponseProto discardSegments(
      RpcController controller, DiscardSegmentsRequestProto request)
      throws ServiceException {
    try {
      impl.discardSegments(convert(request.getJid()),
          request.hasNameServiceId() ? request.getNameServiceId() : null,
          request.getStartTxId());
      return DiscardSegmentsResponseProto.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetJournalCTimeResponseProto getJournalCTime(RpcController controller,
      GetJournalCTimeRequestProto request) throws ServiceException {
    try {
      Long resultCTime = impl.getJournalCTime(convert(request.getJid()),
          request.getNameServiceId());
      return GetJournalCTimeResponseProto.newBuilder()
          .setResultCTime(resultCTime)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
