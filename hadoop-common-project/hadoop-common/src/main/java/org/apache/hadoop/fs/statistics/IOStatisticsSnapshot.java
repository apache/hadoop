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

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.util.JsonSerialization;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.iostatisticsToString;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.aggregateMaps;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.snapshotMap;

/**
 * Snapshot of statistics from a different source.
 * <p></p>
 * It is serializable so that frameworks which can use java serialization
 * to propagate data (Spark, Flink...) can send the statistics
 * back.
 * <p></p>
 * It is annotated for correct serializations with jackson2.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class IOStatisticsSnapshot
    implements IOStatistics, Serializable {

  private static final long serialVersionUID = -1762522703841538084L;

  @JsonProperty
  private TreeMap<String, Long> counters;

  @JsonProperty
  private TreeMap<String, Long> gauges;

  @JsonProperty
  private TreeMap<String, Long> minimums;

  @JsonProperty
  private TreeMap<String, Long> maximums;

  @JsonProperty
  private TreeMap<String, MeanStatistic> meanStatistics;

  /**
   * Construct.
   */
  public IOStatisticsSnapshot() {
    counters = new TreeMap<>();
    gauges = new TreeMap<>();
    minimums = new TreeMap<>();
    maximums = new TreeMap<>();
    meanStatistics = new TreeMap<>();
  }

  /**
   * Construct.
   * @param source statistics source.
   */
  public IOStatisticsSnapshot(IOStatistics source) {
    snapshot(source);
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

  /**
   * Produce an aggregate snapshot.
   * @param source statistics source.
   */
  public void aggregate(IOStatistics source) {
    checkNotNull(source);
    aggregateMaps(counters, source.counters(),
        IOStatisticsBinding::aggregateCounters);
    aggregateMaps(gauges, source.gauges(),
        IOStatisticsBinding::aggregateGauges);
    aggregateMaps(minimums, source.minimums(),
        IOStatisticsBinding::aggregateMinimums);
    aggregateMaps(maximums, source.maximums(),
        IOStatisticsBinding::aggregateMaximums);
    aggregateMaps(meanStatistics, source.meanStatistics(),
        IOStatisticsBinding::aggregateMeanStatistics);
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
    return iostatisticsToString(this);
  }

  /**
   * Get a JSON serializer for this class.
   * @return a serializer.
   */
  public static JsonSerialization<IOStatisticsSnapshot> serializer() {
    return new JsonSerialization<>(IOStatisticsSnapshot.class, false, true);
  }

}
