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

import java.util.Collection;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Get statistics pertaining to blocks of type {@link BlockType#STRIPED}
 * in the filesystem.
 * <p>
 * @see ClientProtocol#getECBlockGroupStats()
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ECBlockGroupStats {
  private final long lowRedundancyBlockGroups;
  private final long corruptBlockGroups;
  private final long missingBlockGroups;
  private final long bytesInFutureBlockGroups;
  private final long pendingDeletionBlocks;
  private final Long highestPriorityLowRedundancyBlocks;

  public ECBlockGroupStats(long lowRedundancyBlockGroups,
      long corruptBlockGroups, long missingBlockGroups,
      long bytesInFutureBlockGroups, long pendingDeletionBlocks) {
    this(lowRedundancyBlockGroups, corruptBlockGroups, missingBlockGroups,
        bytesInFutureBlockGroups, pendingDeletionBlocks, null);
  }

  public ECBlockGroupStats(long lowRedundancyBlockGroups,
      long corruptBlockGroups, long missingBlockGroups,
      long bytesInFutureBlockGroups, long pendingDeletionBlocks,
      Long highestPriorityLowRedundancyBlocks) {
    this.lowRedundancyBlockGroups = lowRedundancyBlockGroups;
    this.corruptBlockGroups = corruptBlockGroups;
    this.missingBlockGroups = missingBlockGroups;
    this.bytesInFutureBlockGroups = bytesInFutureBlockGroups;
    this.pendingDeletionBlocks = pendingDeletionBlocks;
    this.highestPriorityLowRedundancyBlocks
        = highestPriorityLowRedundancyBlocks;
  }

  public long getBytesInFutureBlockGroups() {
    return bytesInFutureBlockGroups;
  }

  public long getCorruptBlockGroups() {
    return corruptBlockGroups;
  }

  public long getLowRedundancyBlockGroups() {
    return lowRedundancyBlockGroups;
  }

  public long getMissingBlockGroups() {
    return missingBlockGroups;
  }

  public long getPendingDeletionBlocks() {
    return pendingDeletionBlocks;
  }

  public boolean hasHighestPriorityLowRedundancyBlocks() {
    return getHighestPriorityLowRedundancyBlocks() != null;
  }

  public Long getHighestPriorityLowRedundancyBlocks() {
    return highestPriorityLowRedundancyBlocks;
  }

  @Override
  public String toString() {
    StringBuilder statsBuilder = new StringBuilder();
    statsBuilder.append("ECBlockGroupStats=[")
        .append("LowRedundancyBlockGroups=").append(
            getLowRedundancyBlockGroups())
        .append(", CorruptBlockGroups=").append(getCorruptBlockGroups())
        .append(", MissingBlockGroups=").append(getMissingBlockGroups())
        .append(", BytesInFutureBlockGroups=").append(
            getBytesInFutureBlockGroups())
        .append(", PendingDeletionBlocks=").append(
            getPendingDeletionBlocks());
    if (hasHighestPriorityLowRedundancyBlocks()) {
      statsBuilder.append(", HighestPriorityLowRedundancyBlocks=")
          .append(getHighestPriorityLowRedundancyBlocks());
    }
    statsBuilder.append("]");
    return statsBuilder.toString();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(lowRedundancyBlockGroups)
        .append(corruptBlockGroups)
        .append(missingBlockGroups)
        .append(bytesInFutureBlockGroups)
        .append(pendingDeletionBlocks)
        .append(highestPriorityLowRedundancyBlocks)
        .toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ECBlockGroupStats other = (ECBlockGroupStats)o;
    return new EqualsBuilder()
        .append(lowRedundancyBlockGroups, other.lowRedundancyBlockGroups)
        .append(corruptBlockGroups, other.corruptBlockGroups)
        .append(missingBlockGroups, other.missingBlockGroups)
        .append(bytesInFutureBlockGroups, other.bytesInFutureBlockGroups)
        .append(pendingDeletionBlocks, other.pendingDeletionBlocks)
        .append(highestPriorityLowRedundancyBlocks,
            other.highestPriorityLowRedundancyBlocks)
        .isEquals();
  }

  /**
   * Merge the multiple ECBlockGroupStats.
   * @param stats Collection of stats to merge.
   * @return A new ECBlockGroupStats merging all the input ones
   */
  public static ECBlockGroupStats merge(Collection<ECBlockGroupStats> stats) {
    long lowRedundancyBlockGroups = 0;
    long corruptBlockGroups = 0;
    long missingBlockGroups = 0;
    long bytesInFutureBlockGroups = 0;
    long pendingDeletionBlocks = 0;
    long highestPriorityLowRedundancyBlocks = 0;
    boolean hasHighestPriorityLowRedundancyBlocks = false;

    for (ECBlockGroupStats stat : stats) {
      lowRedundancyBlockGroups += stat.getLowRedundancyBlockGroups();
      corruptBlockGroups += stat.getCorruptBlockGroups();
      missingBlockGroups += stat.getMissingBlockGroups();
      bytesInFutureBlockGroups += stat.getBytesInFutureBlockGroups();
      pendingDeletionBlocks += stat.getPendingDeletionBlocks();
      if (stat.hasHighestPriorityLowRedundancyBlocks()) {
        hasHighestPriorityLowRedundancyBlocks = true;
        highestPriorityLowRedundancyBlocks +=
            stat.getHighestPriorityLowRedundancyBlocks();
      }
    }
    if (hasHighestPriorityLowRedundancyBlocks) {
      return new ECBlockGroupStats(lowRedundancyBlockGroups, corruptBlockGroups,
          missingBlockGroups, bytesInFutureBlockGroups, pendingDeletionBlocks,
          highestPriorityLowRedundancyBlocks);
    }
    return new ECBlockGroupStats(lowRedundancyBlockGroups, corruptBlockGroups,
        missingBlockGroups, bytesInFutureBlockGroups, pendingDeletionBlocks);
  }
}
