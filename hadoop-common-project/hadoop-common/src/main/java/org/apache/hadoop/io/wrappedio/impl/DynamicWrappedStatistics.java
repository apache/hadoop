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

package org.apache.hadoop.io.wrappedio.impl;

import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.util.dynamic.DynMethods;

import static org.apache.hadoop.util.dynamic.BindingUtils.available;
import static org.apache.hadoop.util.dynamic.BindingUtils.checkAvailable;
import static org.apache.hadoop.util.dynamic.BindingUtils.loadClass;
import static org.apache.hadoop.util.dynamic.BindingUtils.loadStaticMethod;

/**
 * The wrapped IOStatistics methods in {@code WrappedStatistics},
 * dynamically loaded.
 * This is suitable for copy-and-paste into other libraries which have some
 * version of the Parquet DynMethods classes already present.
 */
public final class DynamicWrappedStatistics {

  /**
   * Classname of the wrapped statistics class: {@value}.
   */
  public static final String WRAPPED_STATISTICS_CLASSNAME =
      "org.apache.hadoop.io.wrappedio.WrappedStatistics";

  /**
   * Method name: {@value}.
   */
  public static final String IS_IOSTATISTICS_SOURCE = "isIOStatisticsSource";

  /**
   * Method name: {@value}.
   */
  public static final String IS_IOSTATISTICS = "isIOStatistics";

  /**
   * Method name: {@value}.
   */
  public static final String IS_IOSTATISTICS_SNAPSHOT = "isIOStatisticsSnapshot";

  /**
   * IOStatisticsContext method: {@value}.
   */
  public static final String IOSTATISTICS_CONTEXT_AGGREGATE = "iostatisticsContext_aggregate";

  /**
   * IOStatisticsContext method: {@value}.
   */
  public static final String IOSTATISTICS_CONTEXT_ENABLED = "iostatisticsContext_enabled";

  /**
   * IOStatisticsContext method: {@value}.
   */
  public static final String IOSTATISTICS_CONTEXT_GET_CURRENT = "iostatisticsContext_getCurrent";

  /**
   * IOStatisticsContext method: {@value}.
   */
  public static final String IOSTATISTICS_CONTEXT_SET_THREAD_CONTEXT =
      "iostatisticsContext_setThreadIOStatisticsContext";

  /**
   * IOStatisticsContext method: {@value}.
   */
  public static final String IOSTATISTICS_CONTEXT_RESET = "iostatisticsContext_reset";

  /**
   * IOStatisticsContext method: {@value}.
   */
  public static final String IOSTATISTICS_CONTEXT_SNAPSHOT = "iostatisticsContext_snapshot";


  /**
   * Method name: {@value}.
   */
  public static final String IOSTATISTICS_SNAPSHOT_AGGREGATE = "iostatisticsSnapshot_aggregate";

  /**
   * Method name: {@value}.
   */
  public static final String IOSTATISTICS_SNAPSHOT_CREATE = "iostatisticsSnapshot_create";

  /**
   * Method name: {@value}.
   */
  public static final String IOSTATISTICS_SNAPSHOT_FROM_JSON_STRING =
      "iostatisticsSnapshot_fromJsonString";

  /**
   * Method name: {@value}.
   */
  public static final String IOSTATISTICS_SNAPSHOT_LOAD = "iostatisticsSnapshot_load";

  /**
   * Method name: {@value}.
   */
  public static final String IOSTATISTICS_SNAPSHOT_RETRIEVE = "iostatisticsSnapshot_retrieve";

  /**
   * Method name: {@value}.
   */
  public static final String IOSTATISTICS_SNAPSHOT_SAVE = "iostatisticsSnapshot_save";

  /**
   * Method name: {@value}.
   */
  public static final String IOSTATISTICS_SNAPSHOT_TO_JSON_STRING =
      "iostatisticsSnapshot_toJsonString";

  /**
   * Method name: {@value}.
   */
  public static final String IOSTATISTICS_TO_PRETTY_STRING =
      "iostatistics_toPrettyString";

  /**
   * Method name: {@value}.
   */
  public static final String IOSTATISTICS_COUNTERS = "iostatistics_counters";

  /**
   * Method name: {@value}.
   */
  public static final String IOSTATISTICS_GAUGES = "iostatistics_gauges";

  /**
   * Method name: {@value}.
   */
  public static final String IOSTATISTICS_MINIMUMS = "iostatistics_minimums";

  /**
   * Method name: {@value}.
   */
  public static final String IOSTATISTICS_MAXIMUMS = "iostatistics_maximums";

  /**
   * Method name: {@value}.
   */
  public static final String IOSTATISTICS_MEANS = "iostatistics_means";

  /**
   * Was wrapped IO loaded?
   * In the hadoop codebase, this is true.
   * But in other libraries it may not always be true...this
   * field is used to assist copy-and-paste adoption.
   */
  private final boolean loaded;

  /*
   IOStatisticsContext methods.
   */
  private final DynMethods.UnboundMethod iostatisticsContextAggregateMethod;

  private final DynMethods.UnboundMethod iostatisticsContextEnabledMethod;

  private final DynMethods.UnboundMethod iostatisticsContextGetCurrentMethod;

  private final DynMethods.UnboundMethod iostatisticsContextResetMethod;

  private final DynMethods.UnboundMethod iostatisticsContextSetThreadContextMethod;

  private final DynMethods.UnboundMethod iostatisticsContextSnapshotMethod;

  private final DynMethods.UnboundMethod iostatisticsSnapshotAggregateMethod;

  private final DynMethods.UnboundMethod iostatisticsSnapshotCreateMethod;

  private final DynMethods.UnboundMethod iostatisticsSnapshotCreateWithSourceMethod;

  private final DynMethods.UnboundMethod iostatisticsSnapshotLoadMethod;

  private final DynMethods.UnboundMethod iostatisticsSnapshotFromJsonStringMethod;

  private final DynMethods.UnboundMethod iostatisticsSnapshotRetrieveMethod;

  private final DynMethods.UnboundMethod iostatisticsSnapshotSaveMethod;

  private final DynMethods.UnboundMethod iostatisticsToPrettyStringMethod;

  private final DynMethods.UnboundMethod iostatisticsSnapshotToJsonStringMethod;

  private final DynMethods.UnboundMethod iostatisticsCountersMethod;

  private final DynMethods.UnboundMethod iostatisticsGaugesMethod;

  private final DynMethods.UnboundMethod iostatisticsMinimumsMethod;

  private final DynMethods.UnboundMethod iostatisticsMaximumsMethod;

  private final DynMethods.UnboundMethod iostatisticsMeansMethod;

  private final DynMethods.UnboundMethod isIOStatisticsSourceMethod;

  private final DynMethods.UnboundMethod isIOStatisticsMethod;

  private final DynMethods.UnboundMethod isIOStatisticsSnapshotMethod;


  public DynamicWrappedStatistics() {
    this(WRAPPED_STATISTICS_CLASSNAME);
  }

  public DynamicWrappedStatistics(String classname) {

    // wrap the real class.
    Class<?> wrappedClass = loadClass(classname);

    loaded = wrappedClass != null;

    // instanceof checks
    isIOStatisticsSourceMethod = loadStaticMethod(wrappedClass,
        Boolean.class, IS_IOSTATISTICS_SOURCE, Object.class);
    isIOStatisticsMethod = loadStaticMethod(wrappedClass,
        Boolean.class, IS_IOSTATISTICS, Object.class);
    isIOStatisticsSnapshotMethod = loadStaticMethod(wrappedClass,
        Boolean.class, IS_IOSTATISTICS_SNAPSHOT, Serializable.class);

    // IOStatisticsContext operations
    iostatisticsContextAggregateMethod = loadStaticMethod(wrappedClass,
        Boolean.class, IOSTATISTICS_CONTEXT_AGGREGATE, Object.class);
    iostatisticsContextEnabledMethod = loadStaticMethod(wrappedClass,
        Boolean.class, IOSTATISTICS_CONTEXT_ENABLED);
    iostatisticsContextGetCurrentMethod = loadStaticMethod(wrappedClass,
        Object.class, IOSTATISTICS_CONTEXT_GET_CURRENT);
    iostatisticsContextResetMethod = loadStaticMethod(wrappedClass,
        Void.class, IOSTATISTICS_CONTEXT_RESET);
    iostatisticsContextSetThreadContextMethod = loadStaticMethod(wrappedClass,
        Void.class, IOSTATISTICS_CONTEXT_SET_THREAD_CONTEXT, Object.class);
    iostatisticsContextSnapshotMethod = loadStaticMethod(wrappedClass,
        Serializable.class, IOSTATISTICS_CONTEXT_SNAPSHOT);

    // IOStatistics Snapshot operations

    iostatisticsSnapshotAggregateMethod =
        loadStaticMethod(wrappedClass,
            Boolean.class,
            IOSTATISTICS_SNAPSHOT_AGGREGATE,
            Serializable.class,
            Object.class);

    iostatisticsSnapshotCreateMethod =
        loadStaticMethod(wrappedClass,
            Serializable.class,
            IOSTATISTICS_SNAPSHOT_CREATE);

    iostatisticsSnapshotCreateWithSourceMethod =
        loadStaticMethod(wrappedClass,
            Serializable.class,
            IOSTATISTICS_SNAPSHOT_CREATE,
            Object.class);

    iostatisticsSnapshotFromJsonStringMethod =
        loadStaticMethod(wrappedClass,
            Serializable.class,
            IOSTATISTICS_SNAPSHOT_FROM_JSON_STRING,
            String.class);

    iostatisticsSnapshotToJsonStringMethod =
        loadStaticMethod(wrappedClass,
            String.class,
            IOSTATISTICS_SNAPSHOT_TO_JSON_STRING,
            Serializable.class);

    iostatisticsSnapshotRetrieveMethod =
        loadStaticMethod(wrappedClass,
            Serializable.class,
            IOSTATISTICS_SNAPSHOT_RETRIEVE,
            Object.class);

    iostatisticsSnapshotLoadMethod =
        loadStaticMethod(wrappedClass,
            Serializable.class,
            IOSTATISTICS_SNAPSHOT_LOAD,
            FileSystem.class,
            Path.class);

    iostatisticsSnapshotSaveMethod =
        loadStaticMethod(wrappedClass,
            Void.class,
            IOSTATISTICS_SNAPSHOT_SAVE,
            Serializable.class,
            FileSystem.class,
            Path.class,
            boolean.class);  // note: not Boolean.class

    // getting contents of snapshots
    iostatisticsCountersMethod =
        loadStaticMethod(wrappedClass,
            Map.class,
            IOSTATISTICS_COUNTERS,
            Serializable.class);
    iostatisticsGaugesMethod =
        loadStaticMethod(wrappedClass,
            Map.class,
            IOSTATISTICS_GAUGES,
            Serializable.class);
    iostatisticsMinimumsMethod =
        loadStaticMethod(wrappedClass,
            Map.class,
            IOSTATISTICS_MINIMUMS,
            Serializable.class);
    iostatisticsMaximumsMethod =
        loadStaticMethod(wrappedClass,
            Map.class,
            IOSTATISTICS_MAXIMUMS,
            Serializable.class);
    iostatisticsMeansMethod =
        loadStaticMethod(wrappedClass,
            Map.class,
            IOSTATISTICS_MEANS,
            Serializable.class);

    // stringification

    iostatisticsToPrettyStringMethod =
        loadStaticMethod(wrappedClass,
            String.class,
            IOSTATISTICS_TO_PRETTY_STRING,
            Object.class);

  }

  /**
   * Is the wrapped statistics class loaded?
   * @return true if the wrappedIO class was found and loaded.
   */
  public boolean loaded() {
    return loaded;
  }

  /**
   * Are the core IOStatistics methods and classes available.
   * @return true if the relevant methods are loaded.
   */
  public boolean ioStatisticsAvailable() {
    return available(iostatisticsSnapshotCreateMethod);
  }

  /**
   * Are the IOStatisticsContext methods and classes available?
   * @return true if the relevant methods are loaded.
   */
  public boolean ioStatisticsContextAvailable() {
    return available(iostatisticsContextEnabledMethod);
  }

  /**
   * Require a IOStatistics to be available.
   * @throws UnsupportedOperationException if the method was not found.
   */
  private void checkIoStatisticsAvailable() {
    checkAvailable(iostatisticsSnapshotCreateMethod);
  }

  /**
   * Require IOStatisticsContext methods to be available.
   * @throws UnsupportedOperationException if the classes/methods were not found
   */
  private void checkIoStatisticsContextAvailable() {
    checkAvailable(iostatisticsContextEnabledMethod);
  }

  /**
   * Probe for an object being an instance of {@code IOStatisticsSource}.
   * @param object object to probe
   * @return true if the object is the right type, false if the classes
   * were not found or the object is null/of a different type
   */
  public boolean isIOStatisticsSource(Object object) {
    return ioStatisticsAvailable()
        && (boolean) isIOStatisticsSourceMethod.invoke(null, object);
  }

  /**
   * Probe for an object being an instance of {@code IOStatisticsSource}.
   * @param object object to probe
   * @return true if the object is the right type, false if the classes
   * were not found or the object is null/of a different type
   */
  public boolean isIOStatistics(Object object) {
    return ioStatisticsAvailable()
        && (boolean) isIOStatisticsMethod.invoke(null, object);
  }

  /**
   * Probe for an object being an instance of {@code IOStatisticsSnapshot}.
   * @param object object to probe
   * @return true if the object is the right type, false if the classes
   * were not found or the object is null/of a different type
   */
  public boolean isIOStatisticsSnapshot(Serializable object) {
    return ioStatisticsAvailable()
        && (boolean) isIOStatisticsSnapshotMethod.invoke(null, object);
  }

  /**
   * Probe to check if the thread-level IO statistics enabled.
   * If the relevant classes and methods were not found, returns false
   * @return true if the IOStatisticsContext API was found
   * and is enabled.
   */
  public boolean iostatisticsContext_enabled() {
    return ioStatisticsAvailable()
        && (boolean) iostatisticsContextEnabledMethod.invoke(null);
  }

  /**
   * Get the context's {@code IOStatisticsContext} which
   * implements {@code IOStatisticsSource}.
   * This is either a thread-local value or a global empty context.
   * @return instance of {@code IOStatisticsContext}.
   * @throws UnsupportedOperationException if the IOStatisticsContext API was not found
   */
  public Object iostatisticsContext_getCurrent()
      throws UnsupportedOperationException {
    checkIoStatisticsContextAvailable();
    return iostatisticsContextGetCurrentMethod.invoke(null);
  }

  /**
   * Set the IOStatisticsContext for the current thread.
   * @param statisticsContext IOStatistics context instance for the
   * current thread. If null, the context is reset.
   * @throws UnsupportedOperationException if the IOStatisticsContext API was not found
   */
  public void iostatisticsContext_setThreadIOStatisticsContext(
      @Nullable Object statisticsContext) throws UnsupportedOperationException {
    checkIoStatisticsContextAvailable();
    iostatisticsContextSetThreadContextMethod.invoke(null, statisticsContext);
  }

  /**
   * Reset the context's IOStatistics.
   * {@code IOStatisticsContext#reset()}
   * @throws UnsupportedOperationException if the IOStatisticsContext API was not found
   */
  public void iostatisticsContext_reset()
      throws UnsupportedOperationException {
    checkIoStatisticsContextAvailable();
    iostatisticsContextResetMethod.invoke(null);
  }

  /**
   * Take a snapshot of the context IOStatistics.
   * {@code IOStatisticsContext#snapshot()}
   * @return an instance of {@code IOStatisticsSnapshot}.
   * @throws UnsupportedOperationException if the IOStatisticsContext API was not found
   */
  public Serializable iostatisticsContext_snapshot()
      throws UnsupportedOperationException {
    checkIoStatisticsContextAvailable();
    return iostatisticsContextSnapshotMethod.invoke(null);
  }
  /**
   * Aggregate into the IOStatistics context the statistics passed in via
   * IOStatistics/source parameter.
   * <p>
   * Returns false if the source is null or does not contain any statistics.
   * @param source implementation of {@link IOStatisticsSource} or {@link IOStatistics}
   * @return true if the the source object was aggregated.
   */
  public boolean iostatisticsContext_aggregate(Object source) {
    checkIoStatisticsContextAvailable();
    return iostatisticsContextAggregateMethod.invoke(null, source);
  }

  /**
   * Aggregate an existing {@code IOStatisticsSnapshot} with
   * the supplied statistics.
   * @param snapshot snapshot to update
   * @param statistics IOStatistics to add
   * @return true if the snapshot was updated.
   * @throws IllegalArgumentException if the {@code statistics} argument is not
   * null but not an instance of IOStatistics, or if  {@code snapshot} is invalid.
   * @throws UnsupportedOperationException if the IOStatistics classes were not found
   */
  public boolean iostatisticsSnapshot_aggregate(
      Serializable snapshot, @Nullable Object statistics)
      throws UnsupportedOperationException {
    checkIoStatisticsAvailable();
    return iostatisticsSnapshotAggregateMethod.invoke(null, snapshot, statistics);
  }

  /**
   * Create a new {@code IOStatisticsSnapshot} instance.
   * @return an empty IOStatisticsSnapshot.
   * @throws UnsupportedOperationException if the IOStatistics classes were not found
   */
  public Serializable iostatisticsSnapshot_create()
      throws UnsupportedOperationException {
    checkIoStatisticsAvailable();
    return iostatisticsSnapshotCreateMethod.invoke(null);
  }

  /**
   * Create a new {@code IOStatisticsSnapshot} instance.
   * @param source optional source statistics
   * @return an IOStatisticsSnapshot.
   * @throws ClassCastException if the {@code source} is not valid.
   * @throws UnsupportedOperationException if the IOStatistics classes were not found
   */
  public Serializable iostatisticsSnapshot_create(
      @Nullable Object source)
      throws UnsupportedOperationException, ClassCastException {
    checkIoStatisticsAvailable();
    return iostatisticsSnapshotCreateWithSourceMethod.invoke(null, source);
  }

  /**
   * Save IOStatisticsSnapshot to a JSON string.
   * @param snapshot statistics; may be null or of an incompatible type
   * @return JSON string value or null if source is not an IOStatisticsSnapshot
   * @throws UncheckedIOException Any IO/jackson exception.
   * @throws UnsupportedOperationException if the IOStatistics classes were not found
   */
  public String iostatisticsSnapshot_toJsonString(@Nullable Serializable snapshot)
      throws UncheckedIOException, UnsupportedOperationException {
    checkIoStatisticsAvailable();
    return iostatisticsSnapshotToJsonStringMethod.invoke(null, snapshot);
  }

  /**
   * Load IOStatisticsSnapshot from a JSON string.
   * @param json JSON string value.
   * @return deserialized snapshot.
   * @throws UncheckedIOException Any IO/jackson exception.
   * @throws UnsupportedOperationException if the IOStatistics classes were not found
   */
  public Serializable iostatisticsSnapshot_fromJsonString(
      final String json) throws UncheckedIOException, UnsupportedOperationException {
    checkIoStatisticsAvailable();
    return iostatisticsSnapshotFromJsonStringMethod.invoke(null, json);
  }

  /**
   * Load IOStatisticsSnapshot from a Hadoop filesystem.
   * @param fs filesystem
   * @param path path
   * @return the loaded snapshot
   * @throws UncheckedIOException Any IO exception.
   * @throws UnsupportedOperationException if the IOStatistics classes were not found
   */
  public Serializable iostatisticsSnapshot_load(
      FileSystem fs,
      Path path) throws UncheckedIOException, UnsupportedOperationException {
    checkIoStatisticsAvailable();
    return iostatisticsSnapshotLoadMethod.invoke(null, fs, path);
  }

  /**
   * Extract the IOStatistics from an object in a serializable form.
   * @param source source object, may be null/not a statistics source/instance
   * @return {@code IOStatisticsSnapshot} or null if the object is null/doesn't have statistics
   * @throws UnsupportedOperationException if the IOStatistics classes were not found
   */
  public Serializable iostatisticsSnapshot_retrieve(@Nullable Object source)
      throws UnsupportedOperationException {
    checkIoStatisticsAvailable();
    return iostatisticsSnapshotRetrieveMethod.invoke(null, source);
  }

  /**
   * Save IOStatisticsSnapshot to a Hadoop filesystem as a JSON file.
   * @param snapshot statistics
   * @param fs filesystem
   * @param path path
   * @param overwrite should any existing file be overwritten?
   * @throws UncheckedIOException Any IO exception.
   * @throws UnsupportedOperationException if the IOStatistics classes were not found
   */
  public void iostatisticsSnapshot_save(
      @Nullable Serializable snapshot,
      FileSystem fs,
      Path path,
      boolean overwrite) throws UncheckedIOException, UnsupportedOperationException {

    checkIoStatisticsAvailable();
    iostatisticsSnapshotSaveMethod.invoke(null, snapshot, fs, path, overwrite);
  }

  /**
   * Get the counters of an IOStatisticsSnapshot.
   * @param source source of statistics.
   * @return the map of counters.
   */
  public Map<String, Long> iostatistics_counters(
      Serializable source) {
    return iostatisticsCountersMethod.invoke(null, source);
  }

  /**
   * Get the gauges of an IOStatisticsSnapshot.
   * @param source source of statistics.
   * @return the map of gauges.
   */
  public Map<String, Long> iostatistics_gauges(
      Serializable source) {
    return iostatisticsGaugesMethod.invoke(null, source);

  }

  /**
   * Get the minimums of an IOStatisticsSnapshot.
   * @param source source of statistics.
   * @return the map of minimums.
   */
  public Map<String, Long> iostatistics_minimums(
      Serializable source) {
    return iostatisticsMinimumsMethod.invoke(null, source);
  }

  /**
   * Get the maximums of an IOStatisticsSnapshot.
   * @param source source of statistics.
   * @return the map of maximums.
   */
  public Map<String, Long> iostatistics_maximums(
      Serializable source) {
    return iostatisticsMaximumsMethod.invoke(null, source);
  }

  /**
   * Get the means of an IOStatisticsSnapshot.
   * Each value in the map is the (sample, sum) tuple of the values;
   * the mean is then calculated by dividing sum/sample wherever sample is non-zero.
   * @param source source of statistics.
   * @return a map of mean key to (sample, sum) tuples.
   */
  public Map<String, Map.Entry<Long, Long>> iostatistics_means(
      Serializable source) {
    return iostatisticsMeansMethod.invoke(null, source);
  }

  /**
   * Convert IOStatistics to a string form, with all the metrics sorted
   * and empty value stripped.
   * @param statistics A statistics instance.
   * @return string value or the empty string if null
   * @throws UnsupportedOperationException if the IOStatistics classes were not found
   */
  public String iostatistics_toPrettyString(Object statistics) {
    checkIoStatisticsAvailable();
    return iostatisticsToPrettyStringMethod.invoke(null, statistics);
  }

  @Override
  public String toString() {
    return "DynamicWrappedStatistics{" +
        "ioStatisticsAvailable =" + ioStatisticsAvailable() +
        ", ioStatisticsContextAvailable =" + ioStatisticsContextAvailable() +
        '}';
  }
}
