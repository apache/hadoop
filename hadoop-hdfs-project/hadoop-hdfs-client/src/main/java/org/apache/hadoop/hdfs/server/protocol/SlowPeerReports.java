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

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * A class that allows a DataNode to communicate information about all
 * its peer DataNodes that appear to be slow.
 *
 * The wire representation of this structure is a list of
 * SlowPeerReportProto messages.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class SlowPeerReports {
  /**
   * A map from the DataNode's DataNodeUUID to its aggregate latency
   * as seen by the reporting node.
   *
   * The exact choice of the aggregate is opaque to the NameNode but it
   * should be chosen consistently by all DataNodes in the cluster.
   * Examples of aggregates are 90th percentile (good) and mean (not so
   * good).
   *
   * The NameNode must not attempt to interpret the aggregate latencies
   * beyond exposing them as a diagnostic. e.g. metrics. Also, comparing
   * latencies across reports from different DataNodes may not be not
   * meaningful and must be avoided.
   */
  @Nonnull
  private final Map<String, Double> slowPeers;

  /**
   * An object representing a SlowPeerReports with no entries. Should
   * be used instead of null or creating new objects when there are
   * no slow peers to report.
   */
  public static final SlowPeerReports EMPTY_REPORT =
      new SlowPeerReports(ImmutableMap.of());

  private SlowPeerReports(Map<String, Double> slowPeers) {
    this.slowPeers = slowPeers;
  }

  public static SlowPeerReports create(
      @Nullable Map<String, Double> slowPeers) {
    if (slowPeers == null || slowPeers.isEmpty()) {
      return EMPTY_REPORT;
    }
    return new SlowPeerReports(slowPeers);
  }

  public Map<String, Double> getSlowPeers() {
    return slowPeers;
  }

  public boolean haveSlowPeers() {
    return slowPeers.size() > 0;
  }

  /**
   * Return true if the two objects represent the same set slow peer
   * entries. Primarily for unit testing convenience.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof SlowPeerReports)) {
      return false;
    }

    SlowPeerReports that = (SlowPeerReports) o;

    return slowPeers.equals(that.slowPeers);
  }

  @Override
  public int hashCode() {
    return slowPeers.hashCode();
  }
}
