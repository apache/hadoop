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

package org.apache.hadoop.fs.statistics.impl;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.MeanStatistic;
import org.apache.hadoop.util.functional.CallableRaisingIOE;
import org.apache.hadoop.util.functional.ConsumerRaisingIOE;
import org.apache.hadoop.util.functional.FunctionRaisingIOE;
import org.apache.hadoop.util.functional.InvocationRaisingIOE;

import static org.apache.hadoop.fs.statistics.IOStatistics.MIN_UNSET_VALUE;
import static org.apache.hadoop.fs.statistics.impl.StubDurationTracker.STUB_DURATION_TRACKER;

/**
 * Support for implementing IOStatistics interfaces.
 */
public final class IOStatisticsBinding {

  /** Pattern used for each entry. */
  public static final String ENTRY_PATTERN = "(%s=%s)";

  /** String to return when a source is null. */
  @VisibleForTesting
  public static final String NULL_SOURCE = "()";

  private IOStatisticsBinding() {
  }

  /**
   * Create  IOStatistics from a storage statistics instance.
   *
   * This will be updated as the storage statistics change.
   * @param storageStatistics source data.
   * @return an IO statistics source.
   */
  public static IOStatistics fromStorageStatistics(
      StorageStatistics storageStatistics) {
    DynamicIOStatisticsBuilder builder = dynamicIOStatistics();
    Iterator<StorageStatistics.LongStatistic> it = storageStatistics
        .getLongStatistics();
    while (it.hasNext()) {
      StorageStatistics.LongStatistic next = it.next();
      builder.withLongFunctionCounter(next.getName(),
          k -> storageStatistics.getLong(k));
    }
    return builder.build();
  }

  /**
   * Create a builder for dynamic IO Statistics.
   * @return a builder to be completed.
   */
  public static DynamicIOStatisticsBuilder dynamicIOStatistics() {
    return new DynamicIOStatisticsBuilder();
  }

  /**
   * Get the shared instance of the immutable empty statistics
   * object.
   * @return an empty statistics object.
   */
  public static IOStatistics emptyStatistics() {
    return EmptyIOStatistics.getInstance();
  }

  /**
   * Take an IOStatistics instance and wrap it in a source.
   * @param statistics statistics.
   * @return a source which will return the values
   */
  public static IOStatisticsSource wrap(IOStatistics statistics) {
    return new SourceWrappedStatistics(statistics);
  }

  /**
   * Create a builder for an {@link IOStatisticsStore}.
   *
   * @return a builder instance.
   */
  public static IOStatisticsStoreBuilder iostatisticsStore() {
    return new IOStatisticsStoreBuilderImpl();
  }

  /**
   * Convert an entry to the string format used in logging.
   *
   * @param entry entry to evaluate
   * @param <E> entry type
   * @return formatted string
   */
  public static <E> String entryToString(
      final Map.Entry<String, E> entry) {
    return entryToString(entry.getKey(), entry.getValue());
  }

  /**
   * Convert entry values to the string format used in logging.
   *
   * @param name statistic name
   * @param value stat value
   * @return formatted string
   */
  public static <E> String entryToString(
      final String name, final E value) {
    return String.format(
        ENTRY_PATTERN,
        name,
        value);
  }

  /**
   * Copy into the dest map all the source entries.
   * The destination is cleared first.
   * @param <E> entry type
   * @param dest destination of the copy
   * @param source source
   * @param copyFn function to copy entries
   * @return the destination.
   */
  private static <E> Map<String, E> copyMap(
      Map<String, E> dest,
      Map<String, E> source,
      Function<E, E> copyFn) {
    // we have to clone the values so that they aren't
    // bound to the original values
    dest.clear();
    source.entrySet()
        .forEach(entry ->
            dest.put(entry.getKey(), copyFn.apply(entry.getValue())));
    return dest;
  }

  /**
   * A passthrough copy operation suitable for immutable
   * types, including numbers.
   * @param src source object
   * @return the source object
   */
  public static <E extends Serializable> E passthroughFn(E src) {
    return src;
  }

  /**
   * Take a snapshot of a supplied map, where the copy option simply
   * uses the existing value.
   *
   * For this to be safe, the map must refer to immutable objects.
   * @param source source map
   * @param <E> type of values.
   * @return a new map referencing the same values.
   */
  public static <E extends Serializable> Map<String, E> snapshotMap(
      Map<String, E> source) {
    return snapshotMap(source,
        IOStatisticsBinding::passthroughFn);
  }

  /**
   * Take a snapshot of a supplied map, using the copy function
   * to replicate the source values.
   * @param source source map
   * @param copyFn function to copy the value
   * @param <E> type of values.
   * @return a concurrent hash map referencing the same values.
   */
  public static <E extends Serializable>
      ConcurrentHashMap<String, E> snapshotMap(
          Map<String, E> source,
          Function<E, E> copyFn) {
    ConcurrentHashMap<String, E> dest = new ConcurrentHashMap<>();
    copyMap(dest, source, copyFn);
    return dest;
  }

  /**
   * Aggregate two maps so that the destination.
   * @param <E> type of values
   * @param dest destination map.
   * @param other other map
   * @param aggregateFn function to aggregate the values.
   * @param copyFn function to copy the value
   */
  public static <E> void aggregateMaps(
      Map<String, E> dest,
      Map<String, E> other,
      BiFunction<E, E, E> aggregateFn,
      Function<E, E> copyFn) {
    // scan through the other hand map; copy
    // any values not in the left map,
    // aggregate those for which there is already
    // an entry
    other.entrySet().forEach(entry -> {
      String key = entry.getKey();
      E rVal = entry.getValue();
      E lVal = dest.get(key);
      if (lVal == null) {
        dest.put(key, copyFn.apply(rVal));
      } else {
        dest.put(key, aggregateFn.apply(lVal, rVal));
      }
    });
  }

  /**
   * Aggregate two counters.
   * @param l left value
   * @param r right value
   * @return the aggregate value
   */
  public static Long aggregateCounters(Long l, Long r) {
    return Math.max(l, 0) + Math.max(r, 0);
  }

  /**
   * Add two gauges.
   * @param l left value
   * @param r right value
   * @return aggregate value
   */
  public static Long aggregateGauges(Long l, Long r) {
    return l + r;
  }


  /**
   * Aggregate two minimum values.
   * @param l left
   * @param r right
   * @return the new minimum.
   */
  public static Long aggregateMinimums(Long l, Long r) {
    if (l == MIN_UNSET_VALUE) {
      return r;
    } else if (r == MIN_UNSET_VALUE) {
      return l;
    } else {
      return Math.min(l, r);
    }
  }

  /**
   * Aggregate two maximum values.
   * @param l left
   * @param r right
   * @return the new minimum.
   */
  public static Long aggregateMaximums(Long l, Long r) {
    if (l == MIN_UNSET_VALUE) {
      return r;
    } else if (r == MIN_UNSET_VALUE) {
      return l;
    } else {
      return Math.max(l, r);
    }
  }

  /**
   * Aggregate the mean statistics.
   * This returns a new instance.
   * @param l left value
   * @param r right value
   * @return aggregate value
   */
  public static MeanStatistic aggregateMeanStatistics(
      MeanStatistic l, MeanStatistic r) {
    MeanStatistic res = l.copy();
    res.add(r);
    return res;
  }

  /**
   * Update a maximum value tracked in an atomic long.
   * This is thread safe -it uses compareAndSet to ensure
   * that Thread T1 whose sample is greater than the current
   * value never overwrites an update from thread T2 whose
   * sample was also higher -and which completed first.
   * @param dest destination for all changes.
   * @param sample sample to update.
   */
  public static void maybeUpdateMaximum(AtomicLong dest, long sample) {
    boolean done;
    do {
      long current = dest.get();
      if (sample > current) {
        done = dest.compareAndSet(current, sample);
      } else {
        done = true;
      }
    } while (!done);
  }

  /**
   * Update a maximum value tracked in an atomic long.
   * This is thread safe -it uses compareAndSet to ensure
   * that Thread T1 whose sample is greater than the current
   * value never overwrites an update from thread T2 whose
   * sample was also higher -and which completed first.
   * @param dest destination for all changes.
   * @param sample sample to update.
   */
  public static void maybeUpdateMinimum(AtomicLong dest, long sample) {
    boolean done;
    do {
      long current = dest.get();
      if (current == MIN_UNSET_VALUE || sample < current) {
        done = dest.compareAndSet(current, sample);
      } else {
        done = true;
      }
    } while (!done);
  }

  /**
   * Given an IOException raising function/lambda expression,
   * return a new one which wraps the inner and tracks
   * the duration of the operation, including whether
   * it passes/fails.
   * @param factory factory of duration trackers
   * @param statistic statistic key
   * @param inputFn input function
   * @param <A> type of argument to the input function.
   * @param <B> return type.
   * @return a new function which tracks duration and failure.
   */
  public static <A, B> FunctionRaisingIOE<A, B> trackFunctionDuration(
      @Nullable DurationTrackerFactory factory,
      String statistic,
      FunctionRaisingIOE<A, B> inputFn) {
    return (x) -> {
      // create the tracker outside try-with-resources so
      // that failures can be set in the catcher.
      DurationTracker tracker = createTracker(factory, statistic);
      try {
        // exec the input function and return its value
        return inputFn.apply(x);
      } catch (IOException | RuntimeException e) {
        // input function failed: note it
        tracker.failed();
        // and rethrow
        throw e;
      } finally {
        // update the tracker.
        // this is called after the catch() call would have
        // set the failed flag.
        tracker.close();
      }
    };
  }

  /**
   * Given a java function/lambda expression,
   * return a new one which wraps the inner and tracks
   * the duration of the operation, including whether
   * it passes/fails.
   * @param factory factory of duration trackers
   * @param statistic statistic key
   * @param inputFn input function
   * @param <A> type of argument to the input function.
   * @param <B> return type.
   * @return a new function which tracks duration and failure.
   */
  public static <A, B> Function<A, B> trackJavaFunctionDuration(
      @Nullable DurationTrackerFactory factory,
      String statistic,
      Function<A, B> inputFn) {
    return (x) -> {
      // create the tracker outside try-with-resources so
      // that failures can be set in the catcher.
      DurationTracker tracker = createTracker(factory, statistic);
      try {
        // exec the input function and return its value
        return inputFn.apply(x);
      } catch (RuntimeException e) {
        // input function failed: note it
        tracker.failed();
        // and rethrow
        throw e;
      } finally {
        // update the tracker.
        // this is called after the catch() call would have
        // set the failed flag.
        tracker.close();
      }
    };
  }

  /**
   * Given an IOException raising callable/lambda expression,
   * execute it and update the relevant statistic.
   * @param factory factory of duration trackers
   * @param statistic statistic key
   * @param input input callable.
   * @param <B> return type.
   * @return the result of the operation.
   */
  public static <B> B trackDuration(
      DurationTrackerFactory factory,
      String statistic,
      CallableRaisingIOE<B> input) throws IOException {
    return trackDurationOfOperation(factory, statistic, input).apply();
  }

  /**
   * Given an IOException raising callable/lambda expression,
   * execute it and update the relevant statistic.
   * @param factory factory of duration trackers
   * @param statistic statistic key
   * @param input input callable.
   */
  public static void trackDurationOfInvocation(
      DurationTrackerFactory factory,
      String statistic,
      InvocationRaisingIOE input) throws IOException {

    // create the tracker outside try-with-resources so
    // that failures can be set in the catcher.
    DurationTracker tracker = createTracker(factory, statistic);
    try {
      // exec the input function and return its value
      input.apply();
    } catch (IOException | RuntimeException e) {
      // input function failed: note it
      tracker.failed();
      // and rethrow
      throw e;
    } finally {
      // update the tracker.
      // this is called after the catch() call would have
      // set the failed flag.
      tracker.close();
    }
  }

  /**
   * Given an IOException raising callable/lambda expression,
   * return a new one which wraps the inner and tracks
   * the duration of the operation, including whether
   * it passes/fails.
   * @param factory factory of duration trackers
   * @param statistic statistic key
   * @param input input callable.
   * @param <B> return type.
   * @return a new callable which tracks duration and failure.
   */
  public static <B> CallableRaisingIOE<B> trackDurationOfOperation(
      @Nullable DurationTrackerFactory factory,
      String statistic,
      CallableRaisingIOE<B> input) {
    return () -> {
      // create the tracker outside try-with-resources so
      // that failures can be set in the catcher.
      DurationTracker tracker = createTracker(factory, statistic);
      try {
        // exec the input function and return its value
        return input.apply();
      } catch (IOException | RuntimeException e) {
        // input function failed: note it
        tracker.failed();
        // and rethrow
        throw e;
      } finally {
        // update the tracker.
        // this is called after the catch() call would have
        // set the failed flag.
        tracker.close();
      }
    };
  }

  /**
   * Given an IOException raising Consumer,
   * return a new one which wraps the inner and tracks
   * the duration of the operation, including whether
   * it passes/fails.
   * @param factory factory of duration trackers
   * @param statistic statistic key
   * @param input input callable.
   * @param <B> return type.
   * @return a new consumer which tracks duration and failure.
   */
  public static <B> ConsumerRaisingIOE<B> trackDurationConsumer(
      @Nullable DurationTrackerFactory factory,
      String statistic,
      ConsumerRaisingIOE<B> input) {
    return (B t) -> {
      // create the tracker outside try-with-resources so
      // that failures can be set in the catcher.
      DurationTracker tracker = createTracker(factory, statistic);
      try {
        // exec the input function and return its value
        input.accept(t);
      } catch (IOException | RuntimeException e) {
        // input function failed: note it
        tracker.failed();
        // and rethrow
        throw e;
      } finally {
        // update the tracker.
        // this is called after the catch() call would have
        // set the failed flag.
        tracker.close();
      }
    };
  }

  /**
   * Given a callable/lambda expression,
   * return a new one which wraps the inner and tracks
   * the duration of the operation, including whether
   * it passes/fails.
   * @param factory factory of duration trackers
   * @param statistic statistic key
   * @param input input callable.
   * @param <B> return type.
   * @return a new callable which tracks duration and failure.
   */
  public static <B> Callable<B> trackDurationOfCallable(
      @Nullable DurationTrackerFactory factory,
      String statistic,
      Callable<B> input) {
    return () -> {
      // create the tracker outside try-with-resources so
      // that failures can be set in the catcher.
      DurationTracker tracker = createTracker(factory, statistic);
      try {
        // exec the input function and return its value
        return input.call();
      } catch (RuntimeException e) {
        // input function failed: note it
        tracker.failed();
        // and rethrow
        throw e;
      } finally {
        // update the tracker.
        // this is called after any catch() call will have
        // set the failed flag.
        tracker.close();
      }
    };
  }

  /**
   * Create the tracker. If the factory is null, a stub
   * tracker is returned.
   * @param factory tracker factory
   * @param statistic statistic to track
   * @return a duration tracker.
   */
  private static DurationTracker createTracker(
      @Nullable final DurationTrackerFactory factory,
      final String statistic) {
    return factory != null
        ? factory.trackDuration(statistic)
        : STUB_DURATION_TRACKER;
  }

  /**
   * Create a DurationTrackerFactory which aggregates the tracking
   * of two other factories.
   * @param first first tracker factory
   * @param second second tracker factory
   * @return a factory
   */
  public static DurationTrackerFactory pairedTrackerFactory(
      final DurationTrackerFactory first,
      final DurationTrackerFactory second) {
    return new PairedDurationTrackerFactory(first, second);
  }

  /**
   * Publish the IOStatistics as a set of storage statistics.
   * This is dynamic.
   * @param name storage statistics name.
   * @param scheme FS scheme; may be null.
   * @param source IOStatistics source.
   * @return a dynamic storage statistics object.
   */
  public static StorageStatistics publishAsStorageStatistics(
      String name, String scheme, IOStatistics source) {
    return new StorageStatisticsFromIOStatistics(name, scheme, source);
  }
}
