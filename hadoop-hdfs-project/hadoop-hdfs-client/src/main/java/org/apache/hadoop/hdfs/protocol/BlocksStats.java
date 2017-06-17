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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Get statistics pertaining to blocks of type {@link BlockType#CONTIGUOUS}
 * in the filesystem.
 * <p>
 * @see ClientProtocol#getBlocksStats()
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class BlocksStats {
  private final long lowRedundancyBlocksStat;
  private final long corruptBlocksStat;
  private final long missingBlocksStat;
  private final long missingReplicationOneBlocksStat;
  private final long bytesInFutureBlocksStat;
  private final long pendingDeletionBlocksStat;

  public BlocksStats(long lowRedundancyBlocksStat,
      long corruptBlocksStat, long missingBlocksStat,
      long missingReplicationOneBlocksStat, long bytesInFutureBlocksStat,
      long pendingDeletionBlocksStat) {
    this.lowRedundancyBlocksStat = lowRedundancyBlocksStat;
    this.corruptBlocksStat = corruptBlocksStat;
    this.missingBlocksStat = missingBlocksStat;
    this.missingReplicationOneBlocksStat = missingReplicationOneBlocksStat;
    this.bytesInFutureBlocksStat = bytesInFutureBlocksStat;
    this.pendingDeletionBlocksStat = pendingDeletionBlocksStat;
  }

  public long getLowRedundancyBlocksStat() {
    return lowRedundancyBlocksStat;
  }

  public long getCorruptBlocksStat() {
    return corruptBlocksStat;
  }

  public long getMissingReplicaBlocksStat() {
    return missingBlocksStat;
  }

  public long getMissingReplicationOneBlocksStat() {
    return missingReplicationOneBlocksStat;
  }

  public long getBytesInFutureBlocksStat() {
    return bytesInFutureBlocksStat;
  }

  public long getPendingDeletionBlocksStat() {
    return pendingDeletionBlocksStat;
  }

  @Override
  public String toString() {
    StringBuilder statsBuilder = new StringBuilder();
    statsBuilder.append("ReplicatedBlocksStats=[")
        .append("LowRedundancyBlocks=").append(getLowRedundancyBlocksStat())
        .append(", CorruptBlocks=").append(getCorruptBlocksStat())
        .append(", MissingReplicaBlocks=").append(getMissingReplicaBlocksStat())
        .append(", MissingReplicationOneBlocks=").append(
            getMissingReplicationOneBlocksStat())
        .append(", BytesInFutureBlocks=").append(getBytesInFutureBlocksStat())
        .append(", PendingDeletionBlocks=").append(
            getPendingDeletionBlocksStat())
        .append("]");
    return statsBuilder.toString();
  }
}
