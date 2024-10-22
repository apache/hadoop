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

import java.util.Collection;

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
  private final long badlyDistributedBlocks;
  private final Long highestPriorityLowRedundancyBlocks;

  public ReplicatedBlockStats(long lowRedundancyBlocks,
      long corruptBlocks, long missingBlocks,
      long missingReplicationOneBlocks, long bytesInFutureBlocks,
      long pendingDeletionBlocks, long badlyDistributedBlocks) {
    this(lowRedundancyBlocks, corruptBlocks, missingBlocks,
        missingReplicationOneBlocks, bytesInFutureBlocks, pendingDeletionBlocks,
        badlyDistributedBlocks, null);
  }

  public ReplicatedBlockStats(long lowRedundancyBlocks,
      long corruptBlocks, long missingBlocks,
      long missingReplicationOneBlocks, long bytesInFutureBlocks,
      long pendingDeletionBlocks, long badlyDistributedBlocks,
      Long highestPriorityLowRedundancyBlocks) {
    this.lowRedundancyBlocks = lowRedundancyBlocks;
    this.corruptBlocks = corruptBlocks;
    this.missingBlocks = missingBlocks;
    this.missingReplicationOneBlocks = missingReplicationOneBlocks;
    this.bytesInFutureBlocks = bytesInFutureBlocks;
    this.pendingDeletionBlocks = pendingDeletionBlocks;
    this.badlyDistributedBlocks = badlyDistributedBlocks;
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

  public long getBadlyDistributedBlocks() {
    return badlyDistributedBlocks;
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
            getPendingDeletionBlocks())
        .append(" , badlyDistributedBlocks=").append(getBadlyDistributedBlocks());
    if (hasHighestPriorityLowRedundancyBlocks()) {
        statsBuilder.append(", HighestPriorityLowRedundancyBlocks=").append(
            getHighestPriorityLowRedundancyBlocks());
    }
    statsBuilder.append("]");
    return statsBuilder.toString();
  }

  /**
   * Merge the multiple ReplicatedBlockStats.
   * @param stats Collection of stats to merge.
   * @return A new ReplicatedBlockStats merging all the input ones
   */
  public static ReplicatedBlockStats merge(
      Collection<ReplicatedBlockStats> stats) {
    long lowRedundancyBlocks = 0;
    long corruptBlocks = 0;
    long missingBlocks = 0;
    long missingReplicationOneBlocks = 0;
    long bytesInFutureBlocks = 0;
    long pendingDeletionBlocks = 0;
    long badlyDistributedBlocks = 0;
    long highestPriorityLowRedundancyBlocks = 0;
    boolean hasHighestPriorityLowRedundancyBlocks = false;

    // long's range is large enough that we don't need to consider overflow
    for (ReplicatedBlockStats stat : stats) {
      lowRedundancyBlocks += stat.getLowRedundancyBlocks();
      corruptBlocks += stat.getCorruptBlocks();
      missingBlocks += stat.getMissingReplicaBlocks();
      missingReplicationOneBlocks += stat.getMissingReplicationOneBlocks();
      bytesInFutureBlocks += stat.getBytesInFutureBlocks();
      pendingDeletionBlocks += stat.getPendingDeletionBlocks();
      badlyDistributedBlocks += stat.getBadlyDistributedBlocks();
      if (stat.hasHighestPriorityLowRedundancyBlocks()) {
        hasHighestPriorityLowRedundancyBlocks = true;
        highestPriorityLowRedundancyBlocks +=
            stat.getHighestPriorityLowRedundancyBlocks();
      }
    }
    if (hasHighestPriorityLowRedundancyBlocks) {
      return new ReplicatedBlockStats(lowRedundancyBlocks, corruptBlocks,
          missingBlocks, missingReplicationOneBlocks, bytesInFutureBlocks,
          pendingDeletionBlocks, badlyDistributedBlocks, highestPriorityLowRedundancyBlocks);
    }
    return new ReplicatedBlockStats(lowRedundancyBlocks, corruptBlocks,
        missingBlocks, missingReplicationOneBlocks, bytesInFutureBlocks,
        pendingDeletionBlocks, badlyDistributedBlocks);
  }
}
