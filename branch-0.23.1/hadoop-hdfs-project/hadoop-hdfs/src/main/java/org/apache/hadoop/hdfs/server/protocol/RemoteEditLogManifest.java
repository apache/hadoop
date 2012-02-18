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
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * An enumeration of logs available on a remote NameNode.
 */
public class RemoteEditLogManifest implements Writable {

  private List<RemoteEditLog> logs;
  
  public RemoteEditLogManifest() {
  }
  
  public RemoteEditLogManifest(List<RemoteEditLog> logs) {
    this.logs = logs;
    checkState();
  }
  
  
  /**
   * Check that the logs are contiguous and non-overlapping
   * sequences of transactions, in sorted order
   * @throws IllegalStateException if incorrect
   */
  private void checkState()  {
    Preconditions.checkNotNull(logs);
    
    RemoteEditLog prev = null;
    for (RemoteEditLog log : logs) {
      if (prev != null) {
        if (log.getStartTxId() != prev.getEndTxId() + 1) {
          throw new IllegalStateException("Invalid log manifest:" + this);
        }
      }
      
      prev = log;
    }
  }
  
  public List<RemoteEditLog> getLogs() {
    return Collections.unmodifiableList(logs);
  }


  
  @Override
  public String toString() {
    return "[" + Joiner.on(", ").join(logs) + "]";
  }
  
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(logs.size());
    for (RemoteEditLog log : logs) {
      log.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numLogs = in.readInt();
    logs = Lists.newArrayList();
    for (int i = 0; i < numLogs; i++) {
      RemoteEditLog log = new RemoteEditLog();
      log.readFields(in);
      logs.add(log);
    }
    checkState();
  }
}
