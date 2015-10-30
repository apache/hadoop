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

import com.google.common.base.Function;
import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

public class RemoteEditLog implements Comparable<RemoteEditLog> {
  private long startTxId = HdfsServerConstants.INVALID_TXID;
  private long endTxId = HdfsServerConstants.INVALID_TXID;
  private boolean isInProgress = false;
  
  public RemoteEditLog() {
  }

  public RemoteEditLog(long startTxId, long endTxId) {
    this.startTxId = startTxId;
    this.endTxId = endTxId;
    this.isInProgress = (endTxId == HdfsServerConstants.INVALID_TXID);
  }
  
  public RemoteEditLog(long startTxId, long endTxId, boolean inProgress) {
    this.startTxId = startTxId;
    this.endTxId = endTxId;
    this.isInProgress = inProgress;
  }

  public long getStartTxId() {
    return startTxId;
  }

  public long getEndTxId() {
    return endTxId;
  }

  public boolean isInProgress() {
    return isInProgress;
  }

  @Override
  public String toString() {
    if (!isInProgress) {
      return "[" + startTxId + "," + endTxId + "]";
    } else {
      return "[" + startTxId + "-? (in-progress)]";
    }
  }
  
  @Override
  public int compareTo(RemoteEditLog log) {
    return ComparisonChain.start()
      .compare(startTxId, log.startTxId)
      .compare(endTxId, log.endTxId)
      .result();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RemoteEditLog)) return false;
    return this.compareTo((RemoteEditLog)o) == 0;
  }
  
  @Override
  public int hashCode() {
    return (int) (startTxId * endTxId);
  }
  
  /**
   * Guava <code>Function</code> which applies {@link #getStartTxId()} 
   */
  public static final Function<RemoteEditLog, Long> GET_START_TXID =
    new Function<RemoteEditLog, Long>() {
      @Override
      public Long apply(RemoteEditLog log) {
        if (null == log) {
          return HdfsServerConstants.INVALID_TXID;
        }
        return log.getStartTxId();
      }
    };
}
