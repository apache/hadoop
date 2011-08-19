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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Replica recovery information.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReplicaRecoveryInfo extends Block {
  private ReplicaState originalState;

  public ReplicaRecoveryInfo() {
  }

  public ReplicaRecoveryInfo(long blockId, long diskLen, long gs, ReplicaState rState) {
    set(blockId, diskLen, gs);
    originalState = rState;
  }

  public ReplicaState getOriginalReplicaState() {
    return originalState;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (ReplicaRecoveryInfo.class,
       new WritableFactory() {
         public Writable newInstance() { return new ReplicaRecoveryInfo(); }
       });
  }

 @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    originalState = ReplicaState.read(in); 
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    originalState.write(out);
  }
}
