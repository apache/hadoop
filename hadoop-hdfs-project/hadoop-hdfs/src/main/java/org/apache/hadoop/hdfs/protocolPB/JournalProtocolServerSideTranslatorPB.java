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
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.FenceRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.FenceResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.StartLogSegmentResponseProto;
import org.apache.hadoop.hdfs.server.protocol.FenceResponse;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link JournalProtocolPB} to the 
 * {@link JournalProtocol} server implementation.
 */
@InterfaceAudience.Private
public class JournalProtocolServerSideTranslatorPB implements JournalProtocolPB {
  /** Server side implementation to delegate the requests to */
  private final JournalProtocol impl;

  private final static JournalResponseProto VOID_JOURNAL_RESPONSE = 
  JournalResponseProto.newBuilder().build();

  private final static StartLogSegmentResponseProto
  VOID_START_LOG_SEGMENT_RESPONSE =
      StartLogSegmentResponseProto.newBuilder().build();

  public JournalProtocolServerSideTranslatorPB(JournalProtocol impl) {
    this.impl = impl;
  }

  /** @see JournalProtocol#journal */
  @Override
  public JournalResponseProto journal(RpcController unused,
      JournalRequestProto req) throws ServiceException {
    try {
      impl.journal(PBHelper.convert(req.getJournalInfo()), req.getEpoch(),
          req.getFirstTxnId(), req.getNumTxns(), req.getRecords().toByteArray());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_JOURNAL_RESPONSE;
  }

  /** @see JournalProtocol#startLogSegment */
  @Override
  public StartLogSegmentResponseProto startLogSegment(RpcController controller,
      StartLogSegmentRequestProto req) throws ServiceException {
    try {
      impl.startLogSegment(PBHelper.convert(req.getJournalInfo()),
          req.getEpoch(), req.getTxid());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_START_LOG_SEGMENT_RESPONSE;
  }

  @Override
  public FenceResponseProto fence(RpcController controller,
      FenceRequestProto req) throws ServiceException {
    try {
      FenceResponse resp = impl.fence(PBHelper.convert(req.getJournalInfo()), req.getEpoch(),
          req.getFencerInfo());
      return FenceResponseProto.newBuilder().setInSync(resp.isInSync())
          .setLastTransactionId(resp.getLastTransactionId())
          .setPreviousEpoch(resp.getPreviousEpoch()).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
