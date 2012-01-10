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

package org.apache.hadoop.hdfs.server.protocolR23Compatible;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocolR23Compatible.BlockWritable;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Replica recovery information.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReplicaRecoveryInfoWritable implements Writable {
  private int originalState;
  private BlockWritable block;

  public ReplicaRecoveryInfoWritable() {
  }

  public ReplicaRecoveryInfoWritable(long blockId, long diskLen, long gs,
      ReplicaState rState) {
    block = new BlockWritable(blockId, diskLen, gs);
    originalState = rState.getValue();
  }

  // /////////////////////////////////////////
  // Writable
  // /////////////////////////////////////////
  static { // register a ctor
    WritableFactories.setFactory(ReplicaRecoveryInfoWritable.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new ReplicaRecoveryInfoWritable();
          }
        });
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    block = new BlockWritable();
    block.readFields(in);
    originalState = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    block.write(out);
    out.writeInt(originalState);
  }

  public static ReplicaRecoveryInfoWritable convert(ReplicaRecoveryInfo rrInfo) {
    return new ReplicaRecoveryInfoWritable(rrInfo.getBlockId(),
        rrInfo.getNumBytes(), rrInfo.getGenerationStamp(),
        rrInfo.getOriginalReplicaState());
  }

  public ReplicaRecoveryInfo convert() {
    return new ReplicaRecoveryInfo(block.getBlockId(), block.getNumBytes(),
        block.getGenerationStamp(), ReplicaState.getState(originalState));
  }
}
