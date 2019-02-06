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
 * @see ClientProtocol#getReplicatedBlockStats()
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ReplicatedBlockStats {
  private final long lowRedundancyBlocks;
  private final long corruptBlocks;
  private final long missingBlocks;
  private final long missingReplicationOneBlocks;
  private final long bytesInFutureBlocks;
  private final long pendingDeletionBlocks;
  private final Long highestPriorityLowRedundancyBlocks;

  public ReplicatedBlockStats(long lowRedundancyBlocks,
      long corruptBlocks, long missingBlocks,
      long missingReplicationOneBlocks, long bytesInFutureBlocks,
      long pendingDeletionBlocks) {
    this(lowRedundancyBlocks, corruptBlocks, missingBlocks,
        missingReplicationOneBlocks, bytesInFutureBlocks, pendingDeletionBlocks,
        null);
  }

  public ReplicatedBlockStats(long lowRedundancyBlocks,
      long corruptBlocks, long missingBlocks,
      long missingReplicationOneBlocks, long bytesInFutureBlocks,
      long pendingDeletionBlocks, Long highestPriorityLowRedundancyBlocks) {
    this.lowRedundancyBlocks = lowRedundancyBlocks;
    this.corruptBlocks = corruptBlocks;
    this.missingBlocks = missingBlocks;
    this.missingReplicationOneBlocks = missingReplicationOneBlocks;
    this.bytesInFutureBlocks = bytesInFutureBlocks;
    this.pendingDeletionBlocks = pendingDeletionBlocks;
    this.highestPriorityLowRedundancyBlocks
        = highestPriorityLowRedundancyBlocks;
  }

  public long getLowRedundancyBlocks() {
    return lowRedundancyBlocks;
  }

  public long getCorruptBlocks() {
    return corruptBlocks;
  }

  public long getMissingReplicaBlocks() {
    return missingBlocks;
  }

  public long getMissingReplicationOneBlocks() {
    return missingReplicationOneBlocks;
  }

  public long getBytesInFutureBlocks() {
    return bytesInFutureBlocks;
  }

  public long getPendingDeletionBlocks() {
    return pendingDeletionBlocks;
  }

  public boolean hasHighestPriorityLowRedundancyBlocks() {
    return getHighestPriorityLowRedundancyBlocks() != null;
  }

  public Long getHighestPriorityLowRedundancyBlocks(){
    return highestPriorityLowRedundancyBlocks;
  }

  @Override
  public String toString() {
    StringBuilder statsBuilder = new StringBuilder();
    statsBuilder.append("ReplicatedBlockStats=[")
        .append("LowRedundancyBlocks=").append(getLowRedundancyBlocks())
        .append(", CorruptBlocks=").append(getCorruptBlocks())
        .append(", MissingReplicaBlocks=").append(getMissingReplicaBlocks())
        .append(", MissingReplicationOneBlocks=").append(
            getMissingReplicationOneBlocks())
        .append(", BytesInFutureBlocks=").append(getBytesInFutureBlocks())
        .append(", PendingDeletionBlocks=").append(
            getPendingDeletionBlocks());
    if (hasHighestPriorityLowRedundancyBlocks()) {
        statsBuilder.append(", HighestPriorityLowRedundancyBlocks=").append(
            getHighestPriorityLowRedundancyBlocks());
    }
    statsBuilder.append("]");
    return statsBuilder.toString();
  }
}
