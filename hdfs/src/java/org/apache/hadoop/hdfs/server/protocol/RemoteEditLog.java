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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.io.Writable;

public class RemoteEditLog implements Writable {
  private long startTxId = FSConstants.INVALID_TXID;
  private long endTxId = FSConstants.INVALID_TXID;
  
  public RemoteEditLog() {
  }

  public RemoteEditLog(long startTxId, long endTxId) {
    this.startTxId = startTxId;
    this.endTxId = endTxId;
  }

  public long getStartTxId() {
    return startTxId;
  }

  public long getEndTxId() {
    return endTxId;
  }
    
  @Override
  public String toString() {
    return "[" + startTxId + "," + endTxId + "]";
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(startTxId);
    out.writeLong(endTxId);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    startTxId = in.readLong();
    endTxId = in.readLong();
  }

}
