/*
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

package org.apache.hadoop.fs.statistics;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.util.JsonSerialization;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToString;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.aggregateMaps;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.snapshotMap;

/**
 * Snapshot of statistics from a different source.
 * <p></p>
 * It is serializable so that frameworks which can use java serialization
 * to propagate data (Spark, Flink...) can send the statistics
 * back. For this reason, TreeMaps are explicitly used as field types,
 * even though IDEs can recommend use of Map instead.
 * <p></p>
 * It is annotated for correct serializations with jackson2.
 */
@SuppressWarnings("CollectionDeclaredAsConcreteClass")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class IOStatisticsSnapshot
    implements IOStatistics, Serializable, IOStatisticsAggregator {

  private static final long serialVersionUID = -1762522703841538084L;

  /**
   * Counters.
   */
  @JsonProperty
  private TreeMap<String, Long> counters;

  /**
   * Gauges.
   */
  @JsonProperty
  private TreeMap<String, Long> gauges;

  /**
   * Minimum values.
   */
  @JsonProperty
  private TreeMap<String, Long> minimums;

  /**
   * Maximum values.
   */
  @JsonProperty
  private TreeMap<String, Long> maximums;

  /**
   * mean statistics. The JSON key is all lower case..
   */
  @JsonProperty("meanstatistics")
  private TreeMap<String, MeanStatistic> meanStatistics;

  /**
   * Construct.
   */
  public IOStatisticsSnapshot() {
    createMaps();
  }

  /**
   * Construct, taking a snapshot of the source statistics data
   * if the source is non-null.
   * If the source is null, the empty maps are created
   * @param source statistics source. Nullable.
   */
  public IOStatisticsSnapshot(IOStatistics source) {
    if (source != null) {
      snapshot(source);
    } else {
      createMaps();
    }
  }

  /**
   * Create the maps.
   */
  private void createMaps() {
    counters = new TreeMap<>();
    gauges = new TreeMap<>();
    minimums = new TreeMap<>();
    maximums = new TreeMap<>();
    meanStatistics = new TreeMap<>();
  }

  /**
   * Clear all the maps.
   */
  public void clear() {
    counters.clear();
    gauges.clear();
    minimums.clear();
    maximums.clear();
    meanStatistics.clear();
  }

  /**
   * Take a snapshot.
   * @param source statistics source.
   */
  public void snapshot(IOStatistics source) {
    checkNotNull(source);
    counters = snapshotMap(source.counters());
    gauges = snapshotMap(source.gauges());
    minimums = snapshotMap(source.minimums());
    maximums = snapshotMap(source.maximums());
    meanStatistics = snapshotMap(source.meanStatistics(),
        MeanStatistic::copy);
  }

  @Override
  public boolean aggregate(@Nullable IOStatistics statistics) {
    if (statistics == null) {
      return false;
    }
    aggregateMaps(counters, statistics.counters(),
        IOStatisticsBinding::aggregateCounters,
        IOStatisticsBinding::passthroughFn);
    aggregateMaps(gauges, statistics.gauges(),
        IOStatisticsBinding::aggregateGauges,
        IOStatisticsBinding::passthroughFn);
    aggregateMaps(minimums, statistics.minimums(),
        IOStatisticsBinding::aggregateMinimums,
        IOStatisticsBinding::passthroughFn);
    aggregateMaps(maximums, statistics.maximums(),
        IOStatisticsBinding::aggregateMaximums,
        IOStatisticsBinding::passthroughFn);
    aggregateMaps(meanStatistics, statistics.meanStatistics(),
        IOStatisticsBinding::aggregateMeanStatistics, MeanStatistic::copy);
    return true;
  }

  @Override
  public Map<String, Long> counters() {
    return counters;
  }

  @Override
  public Map<String, Long> gauges() {
    return gauges;
  }

  @Override
  public Map<String, Long> minimums() {
    return minimums;
  }

  @Override
  public Map<String, Long> maximums() {
    return maximums;
  }

  @Override
  public Map<String, MeanStatistic> meanStatistics() {
    return meanStatistics;
  }

  @Override
  public String toString() {
    return ioStatisticsToString(this);
  }

  /**
   * Get a JSON serializer for this class.
   * @return a serializer.
   */
  public static JsonSerialization<IOStatisticsSnapshot> serializer() {
    return new JsonSerialization<>(IOStatisticsSnapshot.class, false, true);
  }

}
