/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * A class that allows a DataNode to communicate information about all
 * its disks that appear to be slow.
 *
 * The wire representation of this structure is a list of
 * SlowDiskReportProto messages.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class SlowDiskReports {
  /**
   * A map from the DataNode Disk's BasePath to its mean metadata op latency,
   * mean read io latency and mean write io latency.
   *
   * The NameNode must not attempt to interpret the mean latencies
   * beyond exposing them as a diagnostic. e.g. metrics. Also, comparing
   * latencies across reports from different DataNodes may not be not
   * meaningful and must be avoided.
   */
  @Nonnull
  private final Map<String, Map<DiskOp, Double>> slowDisks;

  /**
   * An object representing a SlowDiskReports with no entries. Should
   * be used instead of null or creating new objects when there are
   * no slow peers to report.
   */
  public static final SlowDiskReports EMPTY_REPORT =
      new SlowDiskReports(ImmutableMap.of());

  private SlowDiskReports(Map<String, Map<DiskOp, Double>> slowDisks) {
    this.slowDisks = slowDisks;
  }

  public static SlowDiskReports create(
      @Nullable Map<String, Map<DiskOp, Double>> slowDisks) {
    if (slowDisks == null || slowDisks.isEmpty()) {
      return EMPTY_REPORT;
    }
    return new SlowDiskReports(slowDisks);
  }

  public Map<String, Map<DiskOp, Double>> getSlowDisks() {
    return slowDisks;
  }

  public boolean haveSlowDisks() {
    return slowDisks.size() > 0;
  }

  /**
   * Return true if the two objects represent the same set slow disk
   * entries. Primarily for unit testing convenience.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof SlowDiskReports)) {
      return false;
    }

    SlowDiskReports that = (SlowDiskReports) o;

    if (this.slowDisks.size() != that.slowDisks.size()) {
      return false;
    }

    if (!this.slowDisks.keySet().containsAll(that.slowDisks.keySet()) ||
        !that.slowDisks.keySet().containsAll(this.slowDisks.keySet())) {
      return false;
    }

    boolean areEqual;
    for (Map.Entry<String, Map<DiskOp, Double>> entry : this.slowDisks
        .entrySet()) {
      if (!entry.getValue().equals(that.slowDisks.get(entry.getKey()))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    return slowDisks.hashCode();
  }

  /**
   * Lists the types of operations on which disk latencies are measured.
   */
  public enum DiskOp {
    METADATA("MetadataOp"),
    READ("ReadIO"),
    WRITE("WriteIO");

    private final String value;

    DiskOp(final String v) {
      this.value = v;
    }

    @Override
    public String toString() {
      return value;
    }

    public static DiskOp fromValue(final String value) {
      for (DiskOp as : DiskOp.values()) {
        if (as.value.equals(value)) {
          return as;
        }
      }
      return null;
    }
  }
}
