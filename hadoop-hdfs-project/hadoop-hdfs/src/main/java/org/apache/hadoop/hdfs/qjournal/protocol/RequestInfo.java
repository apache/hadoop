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
package org.apache.hadoop.hdfs.qjournal.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

@InterfaceAudience.Private
public class RequestInfo {
  private final String jid;
  private long epoch;
  private long ipcSerialNumber;
  private final long committedTxId;
  private final String nameServiceId;
  
  public RequestInfo(String jid, String nameServiceId,
                     long epoch, long ipcSerialNumber,
                     long committedTxId) {
    this.jid = jid;
    this.nameServiceId = nameServiceId;
    this.epoch = epoch;
    this.ipcSerialNumber = ipcSerialNumber;
    this.committedTxId = committedTxId;
  }

  public String getNameServiceId() {
    return nameServiceId;
  }

  public long getEpoch() {
    return epoch;
  }

  public void setEpoch(long epoch) {
    this.epoch = epoch;
  }
  
  public String getJournalId() {
    return jid;
  }

  public long getIpcSerialNumber() {
    return ipcSerialNumber;
  }

  public void setIpcSerialNumber(long ipcSerialNumber) {
    this.ipcSerialNumber = ipcSerialNumber;
  }

  public long getCommittedTxId() {
    return committedTxId;
  }

  public boolean hasCommittedTxId() {
    return (committedTxId != HdfsServerConstants.INVALID_TXID);
  }
}
