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

package org.apache.hadoop.io.wrappedio;

import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.util.functional.FunctionRaisingIOE;
import org.apache.hadoop.util.functional.Tuples;

import static org.apache.hadoop.fs.statistics.IOStatisticsContext.getCurrentIOStatisticsContext;
import static org.apache.hadoop.fs.statistics.IOStatisticsContext.setThreadIOStatisticsContext;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.util.functional.FunctionalIO.uncheckIOExceptions;

/**
 * Reflection-friendly access to IOStatistics APIs.
 * All {@code Serializable} arguments/return values are actually
 * {@code IOStatisticsSource} instances; passing in the wrong value
 * will raise IllegalArgumentExceptions.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class WrappedStatistics {

  private WrappedStatistics() {
  }

  /**
   * Probe for an object being an instance of {@code IOStatisticsSource}.
   * @param object object to probe
   * @return true if the object is the right type.
   */
  public static boolean isIOStatisticsSource(Object object) {
    return object instanceof IOStatisticsSource;
  }

  /**
   * Probe for an object being an instance of {@code IOStatistics}.
   * @param object object to probe
   * @return true if the object is the right type.
   */
  public static boolean isIOStatistics(Object object) {
    return object instanceof IOStatistics;
  }

  /**
   * Probe for an object being an instance of {@code IOStatisticsSnapshot}.
   * @param object object to probe
   * @return true if the object is the right type.
   */
  public static boolean isIOStatisticsSnapshot(Serializable object) {
    return object instanceof IOStatisticsSnapshot;
  }

  /**
   * Aggregate an existing {@link IOStatisticsSnapshot} with
   * the supplied statistics.
   * @param snapshot snapshot to update
   * @param statistics IOStatistics to add
   * @return true if the snapshot was updated.
   * @throws IllegalArgumentException if the {@code statistics} argument is not
   * null but not an instance of IOStatistics, or if  {@code snapshot} is invalid.
   */
  public static boolean iostatisticsSnapshot_aggregate(
      Serializable snapshot, @Nullable Object statistics) {

    requireIOStatisticsSnapshot(snapshot);
    if (statistics == null) {
      return false;
    }
    checkArgument(statistics instanceof IOStatistics,
        "Not an IOStatistics instance: %s", statistics);

    final IOStatistics sourceStats = (IOStatistics) statistics;
    return applyToIOStatisticsSnapshot(snapshot, s ->
        s.aggregate(sourceStats));
  }

  /**
   * Create a new {@link IOStatisticsSnapshot} instance.
   * @return an empty IOStatisticsSnapshot.
   */
  public static Serializable iostatisticsSnapshot_create() {
    return iostatisticsSnapshot_create(null);
  }

  /**
   * Create a new {@link IOStatisticsSnapshot} instance.
   * @param source optional source statistics
   * @return an IOStatisticsSnapshot.
   * @throws ClassCastException if the {@code source} is not null and not an IOStatistics instance
   */
  public static Serializable iostatisticsSnapshot_create(@Nullable Object source) {
    return new IOStatisticsSnapshot((IOStatistics) source);
  }

  /**
   * Load IOStatisticsSnapshot from a Hadoop filesystem.
   * @param fs filesystem
   * @param path path
   * @return the loaded snapshot
   * @throws UncheckedIOException Any IO exception.
   */
  public static Serializable iostatisticsSnapshot_load(
      FileSystem fs,
      Path path) {
    return uncheckIOExceptions(() ->
        IOStatisticsSnapshot.serializer().load(fs, path));
  }

  /**
   * Extract the IOStatistics from an object in a serializable form.
   * @param source source object, may be null/not a statistics source/instance
   * @return {@link IOStatisticsSnapshot} or null if the object is null/doesn't have statistics
   */
  public static Serializable iostatisticsSnapshot_retrieve(@Nullable Object source) {
    IOStatistics stats = retrieveIOStatistics(source);
    if (stats == null) {
      return null;
    }
    return iostatisticsSnapshot_create(stats);
  }

  /**
   * Save IOStatisticsSnapshot to a Hadoop filesystem as a JSON file.
   * @param snapshot statistics
   * @param fs filesystem
   * @param path path
   * @param overwrite should any existing file be overwritten?
   * @throws UncheckedIOException Any IO exception.
   */
  public static void iostatisticsSnapshot_save(
      @Nullable Serializable snapshot,
      FileSystem fs,
      Path path,
      boolean overwrite) {
    applyToIOStatisticsSnapshot(snapshot, s -> {
      IOStatisticsSnapshot.serializer().save(fs, path, s, overwrite);
      return null;
    });
  }

  /**
   * Save IOStatisticsSnapshot to a JSON string.
   * @param snapshot statistics; may be null or of an incompatible type
   * @return JSON string value
   * @throws UncheckedIOException Any IO/jackson exception.
   * @throws IllegalArgumentException if the supplied class is not a snapshot
   */
  public static String iostatisticsSnapshot_toJsonString(@Nullable Serializable snapshot) {

    return applyToIOStatisticsSnapshot(snapshot,
        IOStatisticsSnapshot.serializer()::toJson);
  }

  /**
   * Load IOStatisticsSnapshot from a JSON string.
   * @param json JSON string value.
   * @return deserialized snapshot.
   * @throws UncheckedIOException Any IO/jackson exception.
   */
  public static Serializable iostatisticsSnapshot_fromJsonString(
      final String json) {
    return uncheckIOExceptions(() ->
        IOStatisticsSnapshot.serializer().fromJson(json));
  }

  /**
   * Get the counters of an IOStatisticsSnapshot.
   * @param source source of statistics.
   * @return the map of counters.
   */
  public static Map<String, Long> iostatistics_counters(
      Serializable source) {
    return applyToIOStatisticsSnapshot(source, IOStatisticsSnapshot::counters);
  }

  /**
   * Get the gauges of an IOStatisticsSnapshot.
   * @param source source of statistics.
   * @return the map of gauges.
   */
  public static Map<String, Long> iostatistics_gauges(
      Serializable source) {
    return applyToIOStatisticsSnapshot(source, IOStatisticsSnapshot::gauges);
  }

  /**
   * Get the minimums of an IOStatisticsSnapshot.
   * @param source source of statistics.
   * @return the map of minimums.
   */
  public static Map<String, Long> iostatistics_minimums(
      Serializable source) {
    return applyToIOStatisticsSnapshot(source, IOStatisticsSnapshot::minimums);
  }

  /**
   * Get the maximums of an IOStatisticsSnapshot.
   * @param source source of statistics.
   * @return the map of maximums.
   */
  public static Map<String, Long> iostatistics_maximums(
      Serializable source) {
    return applyToIOStatisticsSnapshot(source, IOStatisticsSnapshot::maximums);
  }

  /**
   * Get the means of an IOStatisticsSnapshot.
   * Each value in the map is the (sample, sum) tuple of the values;
   * the mean is then calculated by dividing sum/sample wherever sample count is non-zero.
   * @param source source of statistics.
   * @return a map of mean key to (sample, sum) tuples.
   */
  public static Map<String, Map.Entry<Long, Long>> iostatistics_means(
      Serializable source) {
    return applyToIOStatisticsSnapshot(source, stats -> {
      Map<String, Map.Entry<Long, Long>> map = new HashMap<>();
      stats.meanStatistics().forEach((k, v) ->
          map.put(k, Tuples.pair(v.getSamples(), v.getSum())));
      return map;
    });
  }

  /**
   * Get the context's {@link IOStatisticsContext} which
   * implements {@link IOStatisticsSource}.
   * This is either a thread-local value or a global empty context.
   * @return instance of {@link IOStatisticsContext}.
   */
  public static Object iostatisticsContext_getCurrent() {
    return getCurrentIOStatisticsContext();
  }

  /**
   * Set the IOStatisticsContext for the current thread.
   * @param statisticsContext IOStatistics context instance for the
   * current thread. If null, the context is reset.
   */
  public static void iostatisticsContext_setThreadIOStatisticsContext(
      @Nullable Object statisticsContext) {
    setThreadIOStatisticsContext((IOStatisticsContext) statisticsContext);
  }

  /**
   * Static probe to check if the thread-level IO statistics enabled.
   * @return true if the thread-level IO statistics are enabled.
   */
  public static boolean iostatisticsContext_enabled() {
    return IOStatisticsContext.enabled();
  }

  /**
   * Reset the context's IOStatistics.
   * {@link IOStatisticsContext#reset()}
   */
  public static void iostatisticsContext_reset() {
    getCurrentIOStatisticsContext().reset();
  }

  /**
   * Take a snapshot of the context IOStatistics.
   * {@link IOStatisticsContext#snapshot()}
   * @return an instance of {@link IOStatisticsSnapshot}.
   */
  public static Serializable iostatisticsContext_snapshot() {
    return getCurrentIOStatisticsContext().snapshot();
  }

  /**
   * Aggregate into the IOStatistics context the statistics passed in via
   * IOStatistics/source parameter.
   * <p>
   * Returns false if the source is null or does not contain any statistics.
   * @param source implementation of {@link IOStatisticsSource} or {@link IOStatistics}
   * @return true if the the source object was aggregated.
   */
  public static boolean iostatisticsContext_aggregate(Object source) {
    IOStatistics stats = retrieveIOStatistics(source);
    if (stats != null) {
      getCurrentIOStatisticsContext().getAggregator().aggregate(stats);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Convert IOStatistics to a string form, with all the metrics sorted
   * and empty value stripped.
   * @param statistics A statistics instance; may be null
   * @return string value or the empty string if null
   */
  public static String iostatistics_toPrettyString(@Nullable Object statistics) {
    return statistics == null
        ? ""
        : ioStatisticsToPrettyString((IOStatistics) statistics);
  }

  /**
   * Apply a function to an object which may be an IOStatisticsSnapshot.
   * @param <T> return type
   * @param source statistics snapshot
   * @param fun function to invoke if {@code source} is valid.
   * @return the applied value
   * @throws UncheckedIOException Any IO exception.
   * @throws IllegalArgumentException if the supplied class is not a snapshot
   */
  public static <T> T applyToIOStatisticsSnapshot(
      Serializable source,
      FunctionRaisingIOE<IOStatisticsSnapshot, T> fun) {

    return fun.unchecked(requireIOStatisticsSnapshot(source));
  }

  /**
   * Require the parameter to be an instance of {@link IOStatisticsSnapshot}.
   * @param snapshot object to validate
   * @return cast value
   * @throws IllegalArgumentException if the supplied class is not a snapshot
   */
  private static IOStatisticsSnapshot requireIOStatisticsSnapshot(final Serializable snapshot) {
    checkArgument(snapshot instanceof IOStatisticsSnapshot,
        "Not an IOStatisticsSnapshot %s", snapshot);
    return (IOStatisticsSnapshot) snapshot;
  }
}
