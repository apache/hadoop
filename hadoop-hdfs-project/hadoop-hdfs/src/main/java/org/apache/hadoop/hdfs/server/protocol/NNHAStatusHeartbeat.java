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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NNHAStatusHeartbeat implements Writable {

  private State state;
  private long txid = HdfsConstants.INVALID_TXID;
  
  public NNHAStatusHeartbeat() {
  }
  
  public NNHAStatusHeartbeat(State state, long txid) {
    this.state = state;
    this.txid = txid;
  }

  public State getState() {
    return state;
  }
  
  public long getTxId() {
    return txid;
  }
  
  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeEnum(out, state);
    out.writeLong(txid);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    state = WritableUtils.readEnum(in, State.class);
    txid = in.readLong();
  }

  @InterfaceAudience.Private
  public enum State {
    ACTIVE,
    STANDBY;
  }
}
