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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocolProtos.GetEditLogManifestFromJournalRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocolProtos.GetEditLogManifestFromJournalResponseProto;

import java.io.IOException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link InterQJournalProtocolPB} to the
 * {@link InterQJournalProtocol} server implementation.
 */
@InterfaceAudience.Private
public class InterQJournalProtocolServerSideTranslatorPB implements
    InterQJournalProtocolPB{

  /* Server side implementation to delegate the requests to. */
  private final InterQJournalProtocol impl;

  public InterQJournalProtocolServerSideTranslatorPB(InterQJournalProtocol
                                                         impl) {
    this.impl = impl;
  }

  @Override
  public GetEditLogManifestFromJournalResponseProto
      getEditLogManifestFromJournal(RpcController controller,
                                    GetEditLogManifestFromJournalRequestProto
                                        request) throws ServiceException {
    try {
      return impl.getEditLogManifestFromJournal(
          request.getJid().getIdentifier(),
          request.hasNameServiceId() ? request.getNameServiceId() : null,
          request.getSinceTxId(),
          request.getInProgressOk());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
