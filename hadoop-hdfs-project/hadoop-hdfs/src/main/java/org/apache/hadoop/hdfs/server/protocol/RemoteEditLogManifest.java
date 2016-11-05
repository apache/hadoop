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
package org.apache.hadoop.hdfs.server.protocol;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

/**
 * An enumeration of logs available on a remote NameNode.
 */
public class RemoteEditLogManifest {

  private List<RemoteEditLog> logs;

  private long committedTxnId = HdfsServerConstants.INVALID_TXID;

  public RemoteEditLogManifest() {
  }

  public RemoteEditLogManifest(List<RemoteEditLog> logs) {
    this(logs, HdfsServerConstants.INVALID_TXID);
  }

  public RemoteEditLogManifest(List<RemoteEditLog> logs, long committedTxnId) {
    this.logs = logs;
    this.committedTxnId = committedTxnId;
    checkState();
  }
  
  
  /**
   * Check that the logs are non-overlapping sequences of transactions,
   * in sorted order. They do not need to be contiguous.
   * @throws IllegalStateException if incorrect
   */
  private void checkState()  {
    Preconditions.checkNotNull(logs);

    RemoteEditLog prev = null;
    for (RemoteEditLog log : logs) {
      if (prev != null) {
        if (log.getStartTxId() <= prev.getEndTxId()) {
          throw new IllegalStateException(
              "Invalid log manifest (log " + log + " overlaps " + prev + ")\n"
              + this);
        }
      }
      prev = log;
    }
  }
  
  public List<RemoteEditLog> getLogs() {
    return Collections.unmodifiableList(logs);
  }

  public long getCommittedTxnId() {
    return committedTxnId;
  }

  @Override
  public String toString() {
    return "[" + Joiner.on(", ").join(logs) + "]" + " CommittedTxId: "
        + committedTxnId;
  }
}
