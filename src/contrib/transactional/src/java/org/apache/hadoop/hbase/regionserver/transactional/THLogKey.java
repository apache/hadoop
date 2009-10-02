/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver.transactional;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.regionserver.HLogKey;

public class THLogKey extends HLogKey {

  /** Type of Transactional op going into the HLot
   *  
   */
  public enum TrxOp {
    /** A standard operation that is transactional. KV holds the op. */
    OP((byte)2), 
    /** A transaction was committed. */
    COMMIT((byte)3), 
    /** A transaction was aborted. */
    ABORT((byte)4);
    
    private final byte opCode;
    
    private TrxOp(byte opCode) {
      this.opCode = opCode;
    }

    public static TrxOp fromByte(byte opCode) {
      for (TrxOp op : TrxOp.values()) {
        if (op.opCode == opCode) {
          return op;
        }
      }
      return null;
    }

  }

  private byte transactionOp = -1;
  private long transactionId = -1;
  
  public THLogKey() {
    // For Writable
  }
  
  public THLogKey(byte[] regionName, byte[] tablename, long logSeqNum, long now) {
    super(regionName, tablename, logSeqNum, now);
   }
  
  public THLogKey(byte[] regionName, byte[] tablename, long logSeqNum, long now, TrxOp op, long transactionId) {
    super(regionName, tablename, logSeqNum, now);
    this.transactionOp = op.opCode;
    this.transactionId = transactionId;
  }

  public TrxOp getTrxOp() {
    return TrxOp.fromByte(this.transactionOp);
  }
  
  public long getTransactionId() {
    return this.transactionId;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeByte(transactionOp);
    out.writeLong(transactionId);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
   super.readFields(in);
   this.transactionOp = in.readByte();
   this.transactionId = in.readLong();
  }
}
