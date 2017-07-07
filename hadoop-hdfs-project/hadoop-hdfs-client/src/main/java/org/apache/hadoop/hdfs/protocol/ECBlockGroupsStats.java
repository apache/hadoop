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
 * Get statistics pertaining to blocks of type {@link BlockType#STRIPED}
 * in the filesystem.
 * <p>
 * @see ClientProtocol#getECBlockGroupsStats()
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ECBlockGroupsStats {
  private final long lowRedundancyBlockGroupsStat;
  private final long corruptBlockGroupsStat;
  private final long missingBlockGroupsStat;
  private final long bytesInFutureBlockGroupsStat;
  private final long pendingDeletionBlockGroupsStat;

  public ECBlockGroupsStats(long lowRedundancyBlockGroupsStat, long
      corruptBlockGroupsStat, long missingBlockGroupsStat, long
      bytesInFutureBlockGroupsStat, long pendingDeletionBlockGroupsStat) {
    this.lowRedundancyBlockGroupsStat = lowRedundancyBlockGroupsStat;
    this.corruptBlockGroupsStat = corruptBlockGroupsStat;
    this.missingBlockGroupsStat = missingBlockGroupsStat;
    this.bytesInFutureBlockGroupsStat = bytesInFutureBlockGroupsStat;
    this.pendingDeletionBlockGroupsStat = pendingDeletionBlockGroupsStat;
  }

  public long getBytesInFutureBlockGroupsStat() {
    return bytesInFutureBlockGroupsStat;
  }

  public long getCorruptBlockGroupsStat() {
    return corruptBlockGroupsStat;
  }

  public long getLowRedundancyBlockGroupsStat() {
    return lowRedundancyBlockGroupsStat;
  }

  public long getMissingBlockGroupsStat() {
    return missingBlockGroupsStat;
  }

  public long getPendingDeletionBlockGroupsStat() {
    return pendingDeletionBlockGroupsStat;
  }

  @Override
  public String toString() {
    StringBuilder statsBuilder = new StringBuilder();
    statsBuilder.append("ECBlockGroupsStats=[")
        .append("LowRedundancyBlockGroups=").append(
            getLowRedundancyBlockGroupsStat())
        .append(", CorruptBlockGroups=").append(getCorruptBlockGroupsStat())
        .append(", MissingBlockGroups=").append(getMissingBlockGroupsStat())
        .append(", BytesInFutureBlockGroups=").append(
            getBytesInFutureBlockGroupsStat())
        .append(", PendingDeletionBlockGroups=").append(
            getPendingDeletionBlockGroupsStat())
        .append("]");
    return statsBuilder.toString();
  }
}
