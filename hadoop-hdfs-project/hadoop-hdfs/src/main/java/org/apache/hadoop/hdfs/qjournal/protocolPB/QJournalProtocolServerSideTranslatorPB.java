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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FinalizeLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FinalizeLogSegmentResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FormatRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FormatResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
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
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.AcceptRecoveryRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.AcceptRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PurgeLogsRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PurgeLogsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.StartLogSegmentResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.net.URL;

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
          convert(request.getJid()));
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
          convert(request.getJid()));
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
      impl.startLogSegment(convert(req.getReqInfo()),
          req.getTxid());
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
          request.getSinceTxId());
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
        reqInfo.getEpoch(),
        reqInfo.getIpcSerialNumber(),
        reqInfo.hasCommittedTxId() ?
          reqInfo.getCommittedTxId() : HdfsConstants.INVALID_TXID);
  }
}
