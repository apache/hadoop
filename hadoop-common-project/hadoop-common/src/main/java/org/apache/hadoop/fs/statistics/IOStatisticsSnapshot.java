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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.util.JsonSerialization;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToString;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.aggregateMaps;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.snapshotMap;

/**
 * Snapshot of statistics from a different source.
 * <p>
 * It is serializable so that frameworks which can use java serialization
 * to propagate data (Spark, Flink...) can send the statistics
 * back. For this reason, TreeMaps are explicitly used as field types,
 * even though IDEs can recommend use of Map instead.
 * For security reasons, untrusted java object streams should never be
 * deserialized. If for some reason this is required, use
 * {@link #requiredSerializationClasses()} to get the list of classes
 * used when deserializing instances of this object.
 * <p>
 * <p>
 * It is annotated for correct serializations with jackson2.
 * </p>
 */
@SuppressWarnings("CollectionDeclaredAsConcreteClass")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class IOStatisticsSnapshot
    implements IOStatistics, Serializable, IOStatisticsAggregator {

  private static final long serialVersionUID = -1762522703841538084L;

  /**
   * List of chasses needed to deserialize.
   */
  private static final Class[] DESERIALIZATION_CLASSES = {
      IOStatisticsSnapshot.class,
      TreeMap.class,
      Long.class,
      MeanStatistic.class,
  };

  /**
   * Counters.
   */
  @JsonProperty
  private transient Map<String, Long> counters;

  /**
   * Gauges.
   */
  @JsonProperty
  private transient Map<String, Long> gauges;

  /**
   * Minimum values.
   */
  @JsonProperty
  private transient Map<String, Long> minimums;

  /**
   * Maximum values.
   */
  @JsonProperty
  private transient Map<String, Long> maximums;

  /**
   * mean statistics. The JSON key is all lower case..
   */
  @JsonProperty("meanstatistics")
  private transient Map<String, MeanStatistic> meanStatistics;

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
  private synchronized void createMaps() {
    counters = new ConcurrentHashMap<>();
    gauges = new ConcurrentHashMap<>();
    minimums = new ConcurrentHashMap<>();
    maximums = new ConcurrentHashMap<>();
    meanStatistics = new ConcurrentHashMap<>();
  }

  /**
   * Clear all the maps.
   */
  public synchronized void clear() {
    counters.clear();
    gauges.clear();
    minimums.clear();
    maximums.clear();
    meanStatistics.clear();
  }

  /**
   * Take a snapshot.
   *
   * This completely overwrites the map data with the statistics
   * from the source.
   * @param source statistics source.
   */
  public synchronized void snapshot(IOStatistics source) {
    checkNotNull(source);
    counters = snapshotMap(source.counters());
    gauges = snapshotMap(source.gauges());
    minimums = snapshotMap(source.minimums());
    maximums = snapshotMap(source.maximums());
    meanStatistics = snapshotMap(source.meanStatistics(),
        MeanStatistic::copy);
  }

  /**
   * Aggregate the current statistics with the
   * source reference passed in.
   *
   * The operation is synchronized.
   * @param source source; may be null
   * @return true if a merge took place.
   */
  @Override
  public synchronized boolean aggregate(
      @Nullable IOStatistics source) {
    if (source == null) {
      return false;
    }
    aggregateMaps(counters, source.counters(),
        IOStatisticsBinding::aggregateCounters,
        IOStatisticsBinding::passthroughFn);
    aggregateMaps(gauges, source.gauges(),
        IOStatisticsBinding::aggregateGauges,
        IOStatisticsBinding::passthroughFn);
    aggregateMaps(minimums, source.minimums(),
        IOStatisticsBinding::aggregateMinimums,
        IOStatisticsBinding::passthroughFn);
    aggregateMaps(maximums, source.maximums(),
        IOStatisticsBinding::aggregateMaximums,
        IOStatisticsBinding::passthroughFn);
    aggregateMaps(meanStatistics, source.meanStatistics(),
        IOStatisticsBinding::aggregateMeanStatistics, MeanStatistic::copy);
    return true;
  }

  @Override
  public synchronized Map<String, Long> counters() {
    return counters;
  }

  @Override
  public synchronized Map<String, Long> gauges() {
    return gauges;
  }

  @Override
  public synchronized Map<String, Long> minimums() {
    return minimums;
  }

  @Override
  public synchronized Map<String, Long> maximums() {
    return maximums;
  }

  @Override
  public synchronized Map<String, MeanStatistic> meanStatistics() {
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

  /**
   * Serialize by converting each map to a TreeMap, and saving that
   * to the stream.
   */
  private synchronized void writeObject(ObjectOutputStream s)
      throws IOException {
    // Write out the core
    s.defaultWriteObject();
    s.writeObject(new TreeMap<String, Long>(counters));
    s.writeObject(new TreeMap<String, Long>(gauges));
    s.writeObject(new TreeMap<String, Long>(minimums));
    s.writeObject(new TreeMap<String, Long>(maximums));
    s.writeObject(new TreeMap<String, MeanStatistic>(meanStatistics));
  }

  /**
   * Deserialize by loading each TreeMap, and building concurrent
   * hash maps from them.
   */
  private void readObject(final ObjectInputStream s)
      throws IOException, ClassNotFoundException {
    // read in core
    s.defaultReadObject();
    // and rebuild a concurrent hashmap from every serialized tree map
    // read back from the stream.
    counters = new ConcurrentHashMap<>(
        (TreeMap<String, Long>) s.readObject());
    gauges = new ConcurrentHashMap<>(
        (TreeMap<String, Long>) s.readObject());
    minimums = new ConcurrentHashMap<>(
        (TreeMap<String, Long>) s.readObject());
    maximums = new ConcurrentHashMap<>(
        (TreeMap<String, Long>) s.readObject());
    meanStatistics = new ConcurrentHashMap<>(
        (TreeMap<String, MeanStatistic>) s.readObject());
  }

  /**
   * What classes are needed to deserialize this class?
   * Needed to securely unmarshall this from untrusted sources.
   * @return a list of required classes to deserialize the data.
   */
  public static List<Class> requiredSerializationClasses() {
    return Arrays.stream(DESERIALIZATION_CLASSES)
        .collect(Collectors.toList());
  }

}
