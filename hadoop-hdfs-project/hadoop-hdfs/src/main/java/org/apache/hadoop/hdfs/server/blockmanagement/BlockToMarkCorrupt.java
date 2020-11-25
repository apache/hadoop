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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplicasMap.Reason;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * BlockToMarkCorrupt is used to build the "toCorrupt" list, which is a
 * list of blocks that should be considered corrupt due to a block report.
 */
class BlockToMarkCorrupt {
  /** The corrupted block in a datanode. */
  private final Block corrupted;
  /** The corresponding block stored in the BlockManager. */
  private final BlockInfo stored;
  /** The reason to mark corrupt. */
  private final String reason;
  /** The reason code to be stored */
  private final CorruptReplicasMap.Reason reasonCode;

  BlockToMarkCorrupt(Block corrupted, BlockInfo stored, String reason,
      CorruptReplicasMap.Reason reasonCode) {
    Preconditions.checkNotNull(corrupted, "corrupted is null");
    Preconditions.checkNotNull(stored, "stored is null");

    this.corrupted = corrupted;
    this.stored = stored;
    this.reason = reason;
    this.reasonCode = reasonCode;
  }

  BlockToMarkCorrupt(Block corrupted, BlockInfo stored, long gs, String reason,
      CorruptReplicasMap.Reason reasonCode) {
    this(corrupted, stored, reason, reasonCode);
    //the corrupted block in datanode has a different generation stamp
    this.corrupted.setGenerationStamp(gs);
  }

  public boolean isCorruptedDuringWrite() {
    return stored.getGenerationStamp() > corrupted.getGenerationStamp();
  }

  public Block getCorrupted() {
    return corrupted;
  }

  public BlockInfo getStored() {
    return stored;
  }

  public String getReason() {
    return reason;
  }

  public Reason getReasonCode() {
    return reasonCode;
  }

  @Override
  public String toString() {
    return corrupted + "("
        + (corrupted == stored ? "same as stored": "stored=" + stored) + ")";
  }
}
