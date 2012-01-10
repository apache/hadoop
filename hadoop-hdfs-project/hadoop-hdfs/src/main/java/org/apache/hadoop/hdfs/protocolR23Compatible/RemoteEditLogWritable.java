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
package org.apache.hadoop.hdfs.protocolR23Compatible;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

public class RemoteEditLogWritable implements Writable  {
  private long startTxId;
  private long endTxId;
  
  static { // register a ctor
    WritableFactories.setFactory(RemoteEditLogWritable.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new RemoteEditLogWritable();
          }
        });
  }
  
  public RemoteEditLogWritable() {
  }

  public RemoteEditLogWritable(long startTxId, long endTxId) {
    this.startTxId = startTxId;
    this.endTxId = endTxId;
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

  public static RemoteEditLogWritable convert(RemoteEditLog log) {
    return new RemoteEditLogWritable(log.getStartTxId(), log.getEndTxId());
  }

  public RemoteEditLog convert() {
    return new RemoteEditLog(startTxId, endTxId);
  }
}
