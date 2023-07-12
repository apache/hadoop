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

package org.apache.hadoop.fs.s3a;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.impl.WeakRefMetricsSource;
import org.apache.hadoop.fs.s3a.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.s3a.statistics.ChangeTrackerStatistics;
import org.apache.hadoop.fs.s3a.statistics.CommitterStatistics;
import org.apache.hadoop.fs.s3a.statistics.CountersAndGauges;
import org.apache.hadoop.fs.s3a.statistics.DelegationTokenStatistics;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.s3a.statistics.StatisticTypeEnum;
import org.apache.hadoop.fs.s3a.statistics.impl.AbstractS3AStatisticsSource;
import org.apache.hadoop.fs.s3a.statistics.impl.CountingChangeTracker;
import org.apache.hadoop.fs.statistics.impl.ForwardingIOStatisticsStore;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStoreBuilder;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricStringBuilder;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;

import java.io.Closeable;
import java.net.URI;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.fs.s3a.Constants.STREAM_READ_GAUGE_INPUT_POLICY;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.demandStringifyIOStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_EXECUTOR_ACQUIRED;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_UNBUFFERED;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;
import static org.apache.hadoop.fs.s3a.Statistic.*;

/**
 * Instrumentation of S3A.
 * <p>
 * History
 * </p>
 * <ol>
 *   <li>
 *    HADOOP-13028. Initial implementation.
 *    Derived from the {@code AzureFileSystemInstrumentation}.
 *   </li>
 *   <li>
 *    Broadly (and directly) used in S3A.
 *    The use of direct references causes "problems" in mocking tests.
 *   </li>
 *   <li>
 *     HADOOP-16830. IOStatistics. Move to an interface and implementation
 *     design for the different inner classes.
 *   </li>
 * </ol>
 * <p>
 * Counters and metrics are generally addressed in code by their name or
 * {@link Statistic} key. There <i>may</i> be some Statistics which do
 * not have an entry here. To avoid attempts to access such counters failing,
 * the operations to increment/query metric values are designed to handle
 * lookup failures.
 * </p>
 * <p>
 *   S3AFileSystem StorageStatistics are dynamically derived from
 *   the IOStatistics.
 * </p>
 * <p>
 *   The toString() operation includes the entire IOStatistics when this
 *   class's log is set to DEBUG. This keeps the logs somewhat manageable
 *   on normal runs, but allows for more reporting.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AInstrumentation implements Closeable, MetricsSource,
    CountersAndGauges, IOStatisticsSource {
  private static final Logger LOG = LoggerFactory.getLogger(
      S3AInstrumentation.class);

  private static final String METRICS_SOURCE_BASENAME = "S3AMetrics";

  /**
   * {@value} The name of the s3a-specific metrics
   * system instance used for s3a metrics.
   */
  public static final String METRICS_SYSTEM_NAME = "s3a-file-system";

  /**
   * {@value} Currently all s3a metrics are placed in a single
   * "context". Distinct contexts may be used in the future.
   */
  public static final String CONTEXT = "s3aFileSystem";

  /**
   * {@value} The name of a field added to metrics
   * records that uniquely identifies a specific FileSystem instance.
   */
  public static final String METRIC_TAG_FILESYSTEM_ID = "s3aFileSystemId";

  /**
   * {@value} The name of a field added to metrics records
   * that indicates the hostname portion of the FS URL.
   */
  public static final String METRIC_TAG_BUCKET = "bucket";

  // metricsSystemLock must be used to synchronize modifications to
  // metricsSystem and the following counters.
  private static final Object METRICS_SYSTEM_LOCK = new Object();
  private static MetricsSystem metricsSystem = null;
  private static int metricsSourceNameCounter = 0;
  private static int metricsSourceActiveCounter = 0;

  private final DurationTrackerFactory durationTrackerFactory;

  /**
   * Weak reference so there's no back reference to the instrumentation.
   */
  private WeakRefMetricsSource metricsSourceReference;

  private final MetricsRegistry registry =
      new MetricsRegistry("s3aFileSystem").setContext(CONTEXT);
  private final MutableQuantiles throttleRateQuantile;

  /**
   * This is the IOStatistics store for the S3AFileSystem
   * instance.
   * It is not kept in sync with the rest of the S3A instrumentation.
   * Most inner statistics implementation classes only update this
   * store when it is pushed back, such as as in close().
   */
  private final IOStatisticsStore instanceIOStatistics;

  /**
   * Construct the instrumentation for a filesystem.
   * @param name URI of filesystem.
   */
  public S3AInstrumentation(URI name) {
    UUID fileSystemInstanceId = UUID.randomUUID();
    registry.tag(METRIC_TAG_FILESYSTEM_ID,
        "A unique identifier for the instance",
        fileSystemInstanceId.toString());
    registry.tag(METRIC_TAG_BUCKET, "Hostname from the FS URL", name.getHost());

    // now set up the instance IOStatistics.
    // create the builder
    IOStatisticsStoreBuilder storeBuilder = iostatisticsStore();

    // declare all counter statistics
    EnumSet.allOf(Statistic.class).stream()
        .filter(statistic ->
            statistic.getType() == StatisticTypeEnum.TYPE_COUNTER)
        .forEach(stat -> {
          counter(stat);
          storeBuilder.withCounters(stat.getSymbol());
        });
    // declare all gauge statistics
    EnumSet.allOf(Statistic.class).stream()
        .filter(statistic ->
            statistic.getType() == StatisticTypeEnum.TYPE_GAUGE)
        .forEach(stat -> {
          gauge(stat);
          storeBuilder.withGauges(stat.getSymbol());
        });

    // and durations
    EnumSet.allOf(Statistic.class).stream()
        .filter(statistic ->
            statistic.getType() == StatisticTypeEnum.TYPE_DURATION)
        .forEach(stat -> {
          duration(stat);
          storeBuilder.withDurationTracking(stat.getSymbol());
        });

    //todo need a config for the quantiles interval?
    int interval = 1;
    throttleRateQuantile = quantiles(STORE_IO_THROTTLE_RATE,
        "events", "frequency (Hz)", interval);

    // register with Hadoop metrics
    registerAsMetricsSource(name);

    // and build the IO Statistics
    instanceIOStatistics = storeBuilder.build();

    // duration track metrics (Success/failure) and IOStatistics.
    durationTrackerFactory = IOStatisticsBinding.pairedTrackerFactory(
        instanceIOStatistics,
        new MetricDurationTrackerFactory());
  }

  /**
   * Get the current metrics system; demand creating.
   * @return a metric system, creating if need be.
   */
  @VisibleForTesting
  static MetricsSystem getMetricsSystem() {
    synchronized (METRICS_SYSTEM_LOCK) {
      if (metricsSystem == null) {
        metricsSystem = new MetricsSystemImpl();
        metricsSystem.init(METRICS_SYSTEM_NAME);
        LOG.debug("Metrics system inited {}", metricsSystem);
      }
    }
    return metricsSystem;
  }

  /**
   * Does the instrumentation have a metrics system?
   * @return true if the metrics system is present.
   */
  @VisibleForTesting
  static boolean hasMetricSystem() {
    return metricsSystem != null;
  }

  /**
   * Register this instance as a metrics source via a weak reference.
   * @param name s3a:// URI for the associated FileSystem instance
   */
  private void registerAsMetricsSource(URI name) {
    int number;
    synchronized(METRICS_SYSTEM_LOCK) {
      getMetricsSystem();

      metricsSourceActiveCounter++;
      number = ++metricsSourceNameCounter;
    }
    String msName = METRICS_SOURCE_BASENAME + number;
    String metricsSourceName = msName + "-" + name.getHost();
    metricsSourceReference = new WeakRefMetricsSource(metricsSourceName, this);
    metricsSystem.register(metricsSourceName, "", metricsSourceReference);
  }

  /**
   * Create a counter in the registry.
   * @param name counter name
   * @param desc counter description
   * @return a new counter
   */
  protected final MutableCounterLong counter(String name, String desc) {
    return registry.newCounter(name, desc, 0L);
  }

  /**
   * Create a counter in the registry.
   * @param op statistic to count
   * @return a new counter
   */
  protected final MutableCounterLong counter(Statistic op) {
    return counter(op.getSymbol(), op.getDescription());
  }

  /**
   * Registering a duration adds the success and failure counters.
   * @param op statistic to track
   */
  protected final void duration(Statistic op) {
    counter(op.getSymbol(), op.getDescription());
    counter(op.getSymbol() + SUFFIX_FAILURES, op.getDescription());
  }

  /**
   * Create a gauge in the registry.
   * @param name name gauge name
   * @param desc description
   * @return the gauge
   */
  protected final MutableGaugeLong gauge(String name, String desc) {
    return registry.newGauge(name, desc, 0L);
  }

  /**
   * Create a gauge in the registry.
   * @param op statistic to count
   * @return the gauge
   */
  protected final MutableGaugeLong gauge(Statistic op) {
    return gauge(op.getSymbol(), op.getDescription());
  }

  /**
   * Create a quantiles in the registry.
   * @param op  statistic to collect
   * @param sampleName sample name of the quantiles
   * @param valueName value name of the quantiles
   * @param interval interval of the quantiles in seconds
   * @return the created quantiles metric
   */
  protected final MutableQuantiles quantiles(Statistic op,
      String sampleName,
      String valueName,
      int interval) {
    return registry.newQuantiles(op.getSymbol(), op.getDescription(),
        sampleName, valueName, interval);
  }

  /**
   * Get the metrics registry.
   * @return the registry
   */
  public MetricsRegistry getRegistry() {
    return registry;
  }

  /**
   * Dump all the metrics to a string.
   * @param prefix prefix before every entry
   * @param separator separator between name and value
   * @param suffix suffix
   * @param all get all the metrics even if the values are not changed.
   * @return a string dump of the metrics
   */
  public String dump(String prefix,
      String separator,
      String suffix,
      boolean all) {
    MetricStringBuilder metricBuilder = new MetricStringBuilder(null,
        prefix,
        separator, suffix);
    registry.snapshot(metricBuilder, all);
    return metricBuilder.toString();
  }

  /**
   * Get the value of a counter.
   * @param statistic the operation
   * @return its value, or 0 if not found.
   */
  public long getCounterValue(Statistic statistic) {
    return getCounterValue(statistic.getSymbol());
  }

  /**
   * Get the value of a counter.
   * If the counter is null, return 0.
   * @param name the name of the counter
   * @return its value.
   */
  public long getCounterValue(String name) {
    MutableCounterLong counter = lookupCounter(name);
    return counter == null ? 0 : counter.value();
  }

  /**
   * Lookup a counter by name. Return null if it is not known.
   * @param name counter name
   * @return the counter
   * @throws IllegalStateException if the metric is not a counter
   */
  private MutableCounterLong lookupCounter(String name) {
    MutableMetric metric = lookupMetric(name);
    if (metric == null) {
      return null;
    }
    if (!(metric instanceof MutableCounterLong)) {
      throw new IllegalStateException("Metric " + name
          + " is not a MutableCounterLong: " + metric
          + " (type: " + metric.getClass() +")");
    }
    return (MutableCounterLong) metric;
  }

  /**
   * Look up a gauge.
   * @param name gauge name
   * @return the gauge or null
   * @throws ClassCastException if the metric is not a Gauge.
   */
  public MutableGaugeLong lookupGauge(String name) {
    MutableMetric metric = lookupMetric(name);
    if (metric == null) {
      LOG.debug("No gauge {}", name);
    }
    return (MutableGaugeLong) metric;
  }

  /**
   * Look up a quantiles.
   * @param name quantiles name
   * @return the quantiles or null
   * @throws ClassCastException if the metric is not a Quantiles.
   */
  public MutableQuantiles lookupQuantiles(String name) {
    MutableMetric metric = lookupMetric(name);
    if (metric == null) {
      LOG.debug("No quantiles {}", name);
    }
    return (MutableQuantiles) metric;
  }

  /**
   * Look up a metric from both the registered set and the lighter weight
   * stream entries.
   * @param name metric name
   * @return the metric or null
   */
  public MutableMetric lookupMetric(String name) {
    MutableMetric metric = getRegistry().get(name);
    return metric;
  }

  /**
   * Get the instance IO Statistics.
   * @return statistics.
   */
  @Override
  public IOStatisticsStore getIOStatistics() {
    return instanceIOStatistics;
  }

  /**
   * Get the duration tracker factory.
   * @return duration tracking for the instrumentation.
   */
  public DurationTrackerFactory getDurationTrackerFactory() {
    return durationTrackerFactory;
  }

  /**
   * The duration tracker updates the metrics with the count
   * and IOStatistics will full duration information.
   * @param key statistic key prefix
   * @param count  #of times to increment the matching counter in this
   * operation.
   * @return a duration tracker.
   */
  @Override
  public DurationTracker trackDuration(final String key, final long count) {
    return durationTrackerFactory.trackDuration(key, count);
  }

  /**
   * Create an IOStatistics store which updates FS metrics
   * as well as IOStatistics.
   * @return instance of the store.
   */
  public IOStatisticsStore createMetricsUpdatingStore() {
    return new MetricsUpdatingIOStatisticsStore();
  }

  /**
   * String representation. Includes the IOStatistics
   * when logging is at DEBUG.
   * @return a string form.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3AInstrumentation{");
    if (LOG.isDebugEnabled()) {
      sb.append("instanceIOStatistics=").append(instanceIOStatistics);
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Indicate that S3A created a file.
   */
  public void fileCreated() {
    incrementCounter(FILES_CREATED, 1);
  }

  /**
   * Indicate that S3A deleted one or more files.
   * @param count number of files.
   */
  public void fileDeleted(int count) {
    incrementCounter(FILES_DELETED, count);
  }

  /**
   * Indicate that fake directory request was made.
   * @param count number of directory entries included in the delete request.
   */
  public void fakeDirsDeleted(int count) {
    incrementCounter(FAKE_DIRECTORIES_DELETED, count);
  }

  /**
   * Indicate that S3A created a directory.
   */
  public void directoryCreated() {
    incrementCounter(DIRECTORIES_CREATED, 1);
  }

  /**
   * Indicate that S3A just deleted a directory.
   */
  public void directoryDeleted() {
    incrementCounter(DIRECTORIES_DELETED, 1);
  }

  /**
   * Indicate that S3A copied some files within the store.
   *
   * @param files number of files
   * @param size total size in bytes
   */
  public void filesCopied(int files, long size) {
    incrementCounter(FILES_COPIED, files);
    incrementCounter(FILES_COPIED_BYTES, size);
  }

  /**
   * Note that an error was ignored.
   */
  public void errorIgnored() {
    incrementCounter(IGNORED_ERRORS, 1);
  }

  /**
   * Increments a mutable counter and the matching
   * instance IOStatistics counter.
   * No-op if the counter is not defined, or the count == 0.
   * @param op operation
   * @param count increment value
   */
  public void incrementCounter(Statistic op, long count) {
    incrementNamedCounter(op.getSymbol(), count);
  }

  /**
   * Increments a mutable counter and the matching
   * instance IOStatistics counter.
   * No-op if the counter is not defined, or the count == 0.
   * @param name counter name
   * @param count increment value
   * @return the updated value or, if the counter is unknown: 0
   */
  private long incrementNamedCounter(final String name,
      final long count) {
    if (count != 0) {
      incrementMutableCounter(name, count);
      return instanceIOStatistics.incrementCounter(name, count);
    } else {
      return 0;
    }
  }

  /**
   * Increments a Mutable counter.
   * No-op if not a positive integer.
   * @param name counter name.
   * @param count increment value
   */
  private void incrementMutableCounter(final String name, final long count) {
    if (count > 0) {
      MutableCounterLong counter = lookupCounter(name);
      if (counter != null) {
        counter.incr(count);
      }
    }
  }

  /**
   * Add a value to a quantiles statistic. No-op if the quantile
   * isn't found.
   * @param op operation to look up.
   * @param value value to add.
   * @throws ClassCastException if the metric is not a Quantiles.
   */
  public void addValueToQuantiles(Statistic op, long value) {
    MutableQuantiles quantiles = lookupQuantiles(op.getSymbol());
    if (quantiles != null) {
      quantiles.add(value);
    }
  }

  /**
   * Increments a mutable counter and the matching
   * instance IOStatistics counter with the value of
   * the atomic long.
   * No-op if the counter is not defined, or the count == 0.
   * @param op operation
   * @param count atomic long containing value
   */
  public void incrementCounter(Statistic op, AtomicLong count) {
    incrementCounter(op, count.get());
  }

  /**
   * Increment a specific gauge.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   * @throws ClassCastException if the metric is of the wrong type
   */
  public void incrementGauge(Statistic op, long count) {
    MutableGaugeLong gauge = lookupGauge(op.getSymbol());
    if (gauge != null) {
      gauge.incr(count);
    } else {
      LOG.debug("No Gauge: "+ op);
    }
  }

  /**
   * Decrement a specific gauge.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   * @throws ClassCastException if the metric is of the wrong type
   */
  public void decrementGauge(Statistic op, long count) {
    MutableGaugeLong gauge = lookupGauge(op.getSymbol());
    if (gauge != null) {
      gauge.decr(count);
    } else {
      LOG.debug("No Gauge: {}", op);
    }
  }

  /**
   * Add the duration as a timed statistic, deriving
   * statistic name from the operation symbol and the outcome.
   * @param op operation
   * @param success was the operation a success?
   * @param duration how long did it take
   */
  @Override
  public void recordDuration(final Statistic op,
      final boolean success,
      final Duration duration) {
    String name = op.getSymbol()
        + (success ? "" : SUFFIX_FAILURES);
    instanceIOStatistics.addTimedOperation(name, duration);
  }

  /**
   * Create a stream input statistics instance.
   * @return the new instance
   * @param filesystemStatistics FS Statistics to update in close().
   */
  public S3AInputStreamStatistics newInputStreamStatistics(
      @Nullable final FileSystem.Statistics filesystemStatistics) {
    return new InputStreamStatistics(filesystemStatistics);
  }

  /**
   * Create a new instance of the committer statistics.
   * @return a new committer statistics instance
   */
  public CommitterStatistics newCommitterStatistics() {
    return new CommitterStatisticsImpl();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(registry.info().name()), true);
  }

  /**
   * if registered with the metrics, return the
   * name of the source.
   * @return the name of the metrics, or null if this instance is not bonded.
   */
  public String getMetricSourceName() {
    return metricsSourceReference != null
        ? metricsSourceReference.getName()
        : null;
  }

  public void close() {
    if (metricsSourceReference != null) {
      // get the name
      String name = metricsSourceReference.getName();
      LOG.debug("Unregistering metrics for {}", name);
      // then set to null so a second close() is a noop here.
      metricsSourceReference = null;
      synchronized (METRICS_SYSTEM_LOCK) {
        // it is critical to close each quantile, as they start a scheduled
        // task in a shared thread pool.
        if (metricsSystem == null) {
          LOG.debug("there is no metric system to unregister {} from", name);
          return;
        }
        throttleRateQuantile.stop();

        metricsSystem.unregisterSource(name);
        metricsSourceActiveCounter--;
        int activeSources = metricsSourceActiveCounter;
        if (activeSources == 0) {
          LOG.debug("Shutting down metrics publisher");
          metricsSystem.publishMetricsNow();
          metricsSystem.shutdown();
          metricsSystem = null;
        }
      }
    }
  }

  /**
   * A duration tracker which updates a mutable counter with a metric.
   * The metric is updated with the count on start; after a failure
   * the failures count is incremented by one.
   */
  private final class MetricUpdatingDurationTracker
      implements DurationTracker {

    private final String symbol;

    private boolean failed;

    private MetricUpdatingDurationTracker(
        final String symbol,
        final long count) {
      this.symbol = symbol;
      incrementMutableCounter(symbol, count);
    }

    @Override
    public void failed() {
      failed = true;
    }

    /**
     * Close: on failure increment any mutable counter of
     * failures.
     */
    @Override
    public void close() {
      if (failed) {
        incrementMutableCounter(symbol + SUFFIX_FAILURES, 1);
      }
    }
  }

  /**
   * Duration Tracker Factory for updating metrics.
   */
  private final class MetricDurationTrackerFactory
      implements DurationTrackerFactory {

    @Override
    public DurationTracker trackDuration(final String key, final long count) {
      return new MetricUpdatingDurationTracker(key, count);
    }

  }

  /**
   * Statistics updated by an S3AInputStream during its actual operation.
   * <p>
   * When {@code unbuffer()} is called, the changed numbers are propagated
   * to the S3AFileSystem metrics.
   * </p>
   * <p>
   * When {@code close()} is called, the final set of numbers are propagated
   * to the S3AFileSystem metrics.
   * </p>
   * The {@link FileSystem.Statistics} statistics passed in are also
   * updated. This ensures that whichever thread calls close() gets the
   * total count of bytes read, even if any work is done in other
   * threads.
   *
   */
  private final class InputStreamStatistics
      extends AbstractS3AStatisticsSource
      implements S3AInputStreamStatistics {

    /**
     * Distance used when incrementing FS stats.
     */
    private static final int DISTANCE = 5;

    /**
     * FS statistics for the thread creating the stream.
     */
    private final FileSystem.Statistics filesystemStatistics;

    /**
     * The statistics from the last merge.
     */
    private IOStatisticsSnapshot mergedStats;

    /*
    The core counters are extracted to atomic longs for slightly
    faster resolution on the critical paths, especially single byte
    reads and the like.
     */
    private final AtomicLong aborted;
    private final AtomicLong backwardSeekOperations;
    private final AtomicLong bytesBackwardsOnSeek;
    private final AtomicLong bytesDiscardedInAbort;
    /** Bytes read by the application. */
    private final AtomicLong bytesRead;
    private final AtomicLong bytesDiscardedInClose;
    private final AtomicLong bytesDiscardedOnSeek;
    private final AtomicLong bytesSkippedOnSeek;
    private final AtomicLong closed;
    private final AtomicLong forwardSeekOperations;
    private final AtomicLong openOperations;
    private final AtomicLong readExceptions;
    private final AtomicLong readsIncomplete;
    private final AtomicLong readOperations;
    private final AtomicLong readFullyOperations;
    private final AtomicLong seekOperations;
    private final AtomicLong readVectoredOperations;
    private final AtomicLong bytesDiscardedInVectoredIO;
    private final AtomicLong readVectoredIncomingRanges;
    private final AtomicLong readVectoredCombinedRanges;

    /** Bytes read by the application and any when draining streams . */
    private final AtomicLong totalBytesRead;

    /**
     * Instantiate.
     * @param filesystemStatistics FS Statistics to update in close().
     */
    private InputStreamStatistics(
        @Nullable FileSystem.Statistics filesystemStatistics) {
      this.filesystemStatistics = filesystemStatistics;
      IOStatisticsStore st = iostatisticsStore()
          .withCounters(
              StreamStatisticNames.STREAM_READ_ABORTED,
              StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_ABORT,
              StreamStatisticNames.STREAM_READ_CLOSED,
              StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_CLOSE,
              StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS,
              StreamStatisticNames.STREAM_READ_OPENED,
              StreamStatisticNames.STREAM_READ_BYTES,
              StreamStatisticNames.STREAM_READ_EXCEPTIONS,
              StreamStatisticNames.STREAM_READ_FULLY_OPERATIONS,
              StreamStatisticNames.STREAM_READ_OPERATIONS,
              StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE,
              StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS,
              StreamStatisticNames.STREAM_READ_SEEK_POLICY_CHANGED,
              StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS,
              StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS,
              StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS,
              StreamStatisticNames.STREAM_READ_SEEK_BYTES_DISCARDED,
              StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED,
              StreamStatisticNames.STREAM_READ_TOTAL_BYTES,
              StreamStatisticNames.STREAM_READ_UNBUFFERED,
              StreamStatisticNames.STREAM_READ_VECTORED_COMBINED_RANGES,
              StreamStatisticNames.STREAM_READ_VECTORED_INCOMING_RANGES,
              StreamStatisticNames.STREAM_READ_VECTORED_OPERATIONS,
              StreamStatisticNames.STREAM_READ_VECTORED_READ_BYTES_DISCARDED,
              StreamStatisticNames.STREAM_READ_VERSION_MISMATCHES)
          .withGauges(STREAM_READ_GAUGE_INPUT_POLICY,
              STREAM_READ_BLOCKS_IN_FILE_CACHE.getSymbol(),
              STREAM_READ_ACTIVE_PREFETCH_OPERATIONS.getSymbol(),
              STREAM_READ_ACTIVE_MEMORY_IN_USE.getSymbol()
              )
          .withDurationTracking(ACTION_HTTP_GET_REQUEST,
              ACTION_EXECUTOR_ACQUIRED,
              StoreStatisticNames.ACTION_FILE_OPENED,
              StreamStatisticNames.STREAM_READ_REMOTE_STREAM_ABORTED,
              StreamStatisticNames.STREAM_READ_REMOTE_STREAM_DRAINED,
              StreamStatisticNames.STREAM_READ_PREFETCH_OPERATIONS,
              StreamStatisticNames.STREAM_READ_REMOTE_BLOCK_READ,
              StreamStatisticNames.STREAM_READ_BLOCK_ACQUIRE_AND_READ)
          .build();
      setIOStatistics(st);
      aborted = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_ABORTED);
      backwardSeekOperations = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS);
      bytesBackwardsOnSeek = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS);
      bytesDiscardedInAbort = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_ABORT);
      bytesRead = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_BYTES);
      bytesDiscardedInClose = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_CLOSE);
      bytesDiscardedOnSeek = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_SEEK_BYTES_DISCARDED);
      bytesSkippedOnSeek = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED);
      closed = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_CLOSED);
      forwardSeekOperations = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS);
      openOperations = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_OPENED);
      readExceptions = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_EXCEPTIONS);
      readsIncomplete = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE);
      readOperations = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_OPERATIONS);
      readVectoredOperations = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_VECTORED_OPERATIONS);
      bytesDiscardedInVectoredIO =  st.getCounterReference(
              StreamStatisticNames.STREAM_READ_VECTORED_READ_BYTES_DISCARDED);
      readVectoredIncomingRanges = st.getCounterReference(
              StreamStatisticNames.STREAM_READ_VECTORED_INCOMING_RANGES);
      readVectoredCombinedRanges = st.getCounterReference(
              StreamStatisticNames.STREAM_READ_VECTORED_COMBINED_RANGES);
      readFullyOperations = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_FULLY_OPERATIONS);
      seekOperations = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS);
      totalBytesRead = st.getCounterReference(
          StreamStatisticNames.STREAM_READ_TOTAL_BYTES);
      setIOStatistics(st);
      // create initial snapshot of merged statistics
      mergedStats = snapshotIOStatistics(st);
    }

    /**
     * Increment a named counter by one.
     * @param name counter name
     * @return the new value
     */
    private long increment(String name) {
      return increment(name, 1);
    }

    /**
     * Increment a named counter by a given value.
     * @param name counter name
     * @param value value to increment by.
     * @return the new value
     */
    private long increment(String name, long value) {
      return incCounter(name, value);
    }

    /**
     * Increment the Statistic gauge and the local IOStatistics
     * equivalent.
     * @param statistic statistic
     * @param v value.
     * @return local IOStatistic value
     */
    private long incAllGauges(Statistic statistic, long v) {
      incrementGauge(statistic, v);
      return incGauge(statistic.getSymbol(), v);
    }

    /**
     * {@inheritDoc}.
     * Increments the number of seek operations,
     * and backward seek operations.
     * The offset is inverted and used as the increment
     * of {@link #bytesBackwardsOnSeek}.
     */
    @Override
    public void seekBackwards(long negativeOffset) {
      seekOperations.incrementAndGet();
      backwardSeekOperations.incrementAndGet();
      bytesBackwardsOnSeek.addAndGet(-negativeOffset);
    }

    /**
     * {@inheritDoc}.
     * Increment the number of seek and forward seek
     * operations, as well as counters of bytes skipped
     * and bytes read in seek, where appropriate.
     * Bytes read in seek are also added to the totalBytesRead
     * counter.
     */
    @Override
    public void seekForwards(final long skipped,
        long bytesReadInSeek) {
      seekOperations.incrementAndGet();
      forwardSeekOperations.incrementAndGet();
      if (skipped > 0) {
        bytesSkippedOnSeek.addAndGet(skipped);
      }
      if (bytesReadInSeek > 0) {
        bytesDiscardedOnSeek.addAndGet(bytesReadInSeek);
        totalBytesRead.addAndGet(bytesReadInSeek);
      }
    }

    /**
     * {@inheritDoc}.
     * Use {@code getAnIncrement()} on {@link #openOperations}
     * so that on invocation 1 it returns 0.
     * The caller will know that this is the first invocation.
     */
    @Override
    public long streamOpened() {
      return openOperations.getAndIncrement();
    }

    /**
     * {@inheritDoc}.
     * If the connection was aborted, increment {@link #aborted}
     * and add the byte's remaining count to {@link #bytesDiscardedInAbort}.
     * If not aborted, increment {@link #closed} and
     * then {@link #bytesDiscardedInClose} and {@link #totalBytesRead}
     * with the bytes remaining value.
     */
    @Override
    public void streamClose(boolean abortedConnection,
        long remainingInCurrentRequest) {
      if (abortedConnection) {
        // the connection was aborted.
        // update the counter of abort() calls and bytes discarded
        aborted.incrementAndGet();
        bytesDiscardedInAbort.addAndGet(remainingInCurrentRequest);
      } else {
        // connection closed, possibly draining the stream of surplus
        // bytes.
        closed.incrementAndGet();
        bytesDiscardedInClose.addAndGet(remainingInCurrentRequest);
        totalBytesRead.addAndGet(remainingInCurrentRequest);
        filesystemStatistics.incrementBytesRead(remainingInCurrentRequest);
      }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void readException() {
      readExceptions.incrementAndGet();
    }

    /**
     * {@inheritDoc}.
     * If the byte counter is positive, increment bytesRead and totalBytesRead.
     */
    @Override
    public void bytesRead(long bytes) {
      if (bytes > 0) {
        bytesRead.addAndGet(bytes);
        totalBytesRead.addAndGet(bytes);
      }
    }

    @Override
    public void readOperationStarted(long pos, long len) {
      readOperations.incrementAndGet();
    }

    @Override
    public void readFullyOperationStarted(long pos, long len) {
      readFullyOperations.incrementAndGet();
    }

    /**
     * {@inheritDoc}.
     * If more data was requested than was actually returned, this
     * was an incomplete read. Increment {@link #readsIncomplete}.
     */
    @Override
    public void readOperationCompleted(int requested, int actual) {
      if (requested > actual) {
        readsIncomplete.incrementAndGet();
      }
    }

    @Override
    public void readVectoredOperationStarted(int numIncomingRanges,
                                             int numCombinedRanges) {
      readVectoredIncomingRanges.addAndGet(numIncomingRanges);
      readVectoredCombinedRanges.addAndGet(numCombinedRanges);
      readVectoredOperations.incrementAndGet();
    }

    @Override
    public void readVectoredBytesDiscarded(int discarded) {
      bytesDiscardedInVectoredIO.addAndGet(discarded);
    }

    @Override
    public void executorAcquired(Duration timeInQueue) {
      // update the duration fields in the IOStatistics.
      localIOStatistics().addTimedOperation(ACTION_EXECUTOR_ACQUIRED, timeInQueue);
    }

    /**
     * {@code close()} merges the stream statistics into the filesystem's
     * instrumentation instance.
     */
    @Override
    public void close() {
      increment(StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS);
      merge(true);
    }

    /**
     * {@inheritDoc}.
     * As well as incrementing the {@code STREAM_READ_SEEK_POLICY_CHANGED}
     * counter, the
     * {@code STREAM_READ_GAUGE_INPUT_POLICY} gauge is set to the new value.
     *
     */
    @Override
    public void inputPolicySet(int updatedPolicy) {
      increment(StreamStatisticNames.STREAM_READ_SEEK_POLICY_CHANGED);
      localIOStatistics().setGauge(STREAM_READ_GAUGE_INPUT_POLICY,
          updatedPolicy);
    }

    /**
     * Get the inner class's IO Statistics. This is
     * needed to avoid findbugs warnings about ambiguity.
     * @return the Input Stream's statistics.
     */
    private IOStatisticsStore localIOStatistics() {
      return InputStreamStatistics.super.getIOStatistics();
    }

    /**
     * The change tracker increments {@code versionMismatches} on any
     * mismatch.
     * @return change tracking.
     */
    @Override
    public ChangeTrackerStatistics getChangeTrackerStatistics() {
      return new CountingChangeTracker(
          localIOStatistics().getCounterReference(
              StreamStatisticNames.STREAM_READ_VERSION_MISMATCHES));
    }

    /**
     * String operator describes all the current statistics.
     * <b>Important: there are no guarantees as to the stability
     * of this value.</b>
     * @return the current values of the stream statistics.
     */
    @Override
    @InterfaceStability.Unstable
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "StreamStatistics{");
      sb.append(IOStatisticsLogging.ioStatisticsToString(
          localIOStatistics()));
      sb.append('}');
      return sb.toString();
    }

    /**
     * {@inheritDoc}
     * Increment the counter {@code STREAM_READ_UNBUFFERED}
     * and then merge the current set of statistics into the
     * FileSystem's statistics through {@link #merge(boolean)}.
     */
    @Override
    public void unbuffered() {
      increment(STREAM_READ_UNBUFFERED);
      merge(false);
    }

    /**
     * Merge the statistics into the filesystem's instrumentation instance.
     * <p>
     *   If the merge is invoked because the stream has been closed,
     *   then all statistics are merged, and the filesystem
     *   statistics of {@link #filesystemStatistics} updated
     *   with the bytes read values.
     * </p>
     * <p>
     *   Whichever thread close()d the stream will have its counters
     *   updated.
     * </p>
     * <p>
     *   If the merge is due to an unbuffer() call, the change in all
     *   counters since the last merge will be pushed to the Instrumentation's
     *   counters.
     * </p>
     *
     * @param isClosed is this merge invoked because the stream is closed?
     */
    private void merge(boolean isClosed) {

      IOStatisticsStore ioStatistics = localIOStatistics();
      LOG.debug("Merging statistics into FS statistics in {}: {}",
          (isClosed ? "close()" : "unbuffer()"),
          demandStringifyIOStatistics(ioStatistics));
      promoteInputStreamCountersToMetrics();
      mergedStats = snapshotIOStatistics(localIOStatistics());

      if (isClosed) {
        // stream is being closed.
        // merge in all the IOStatistics
        S3AInstrumentation.this.getIOStatistics().aggregate(ioStatistics);

        // increment the filesystem statistics for this thread.
        if (filesystemStatistics != null) {
          long t = getTotalBytesRead();
          filesystemStatistics.incrementBytesReadByDistance(DISTANCE, t);
        }
      }
    }

    /**
     * Propagate a counter from the instance-level statistics
     * to the S3A instrumentation, subtracting the previous merged value.
     * @param name statistic to promote
     */
    void promoteIOCounter(String name) {
      incrementMutableCounter(name,
          lookupCounterValue(name)
              - mergedStats.counters().get(name));
    }

    /**
     * Merge in the statistics of a single input stream into
     * the filesystem-wide metrics counters.
     * This does not update the FS IOStatistics values.
     */
    private void promoteInputStreamCountersToMetrics() {
      // iterate through all the counters
      localIOStatistics().counters()
          .keySet().stream()
          .forEach(e -> promoteIOCounter(e));
    }

    @Override
    public long getCloseOperations() {
      return lookupCounterValue(
          StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS);
    }

    @Override
    public long getClosed() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_CLOSED);
    }

    @Override
    public long getAborted() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_ABORTED);
    }

    @Override
    public long getForwardSeekOperations() {
      return lookupCounterValue(
          StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS);
    }

    @Override
    public long getBackwardSeekOperations() {
      return lookupCounterValue(
          StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS);
    }

    @Override
    public long getBytesRead() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_BYTES);
    }

    @Override
    public long getTotalBytesRead() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_TOTAL_BYTES);
    }

    @Override
    public long getBytesSkippedOnSeek() {
      return lookupCounterValue(
          StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED);
    }

    @Override
    public long getBytesBackwardsOnSeek() {
      return lookupCounterValue(
          StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS);
    }

    @Override
    public long getBytesReadInClose() {
      return lookupCounterValue(
          StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_CLOSE);
    }

    @Override
    public long getBytesDiscardedInAbort() {
      return lookupCounterValue(
          StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_ABORT);
    }

    @Override
    public long getOpenOperations() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_OPENED);
    }

    @Override
    public long getSeekOperations() {
      return lookupCounterValue(
          StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS);
    }

    @Override
    public long getReadExceptions() {
      return lookupCounterValue(
          StreamStatisticNames.STREAM_READ_EXCEPTIONS);
    }

    @Override
    public long getReadOperations() {
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_OPERATIONS);
    }

    @Override
    public long getReadFullyOperations() {
      return lookupCounterValue(
          StreamStatisticNames.STREAM_READ_FULLY_OPERATIONS);
    }

    @Override
    public long getReadsIncomplete() {
      return lookupCounterValue(
          StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE);
    }

    @Override
    public long getPolicySetCount() {
      return lookupCounterValue(
          StreamStatisticNames.STREAM_READ_SEEK_POLICY_CHANGED);
    }

    @Override
    public long getVersionMismatches() {
      return lookupCounterValue(
          StreamStatisticNames.STREAM_READ_VERSION_MISMATCHES);
    }

    @Override
    public long getInputPolicy() {
      return localIOStatistics().gauges()
          .get(STREAM_READ_GAUGE_INPUT_POLICY);
    }

    @Override
    public DurationTracker initiateGetRequest() {
      return trackDuration(ACTION_HTTP_GET_REQUEST);
    }

    @Override
    public DurationTracker initiateInnerStreamClose(final boolean abort) {
      return trackDuration(abort
          ? StreamStatisticNames.STREAM_READ_REMOTE_STREAM_ABORTED
          : StreamStatisticNames.STREAM_READ_REMOTE_STREAM_DRAINED);
    }

    @Override
    public DurationTracker prefetchOperationStarted() {
      incAllGauges(STREAM_READ_ACTIVE_PREFETCH_OPERATIONS, 1);
      return trackDuration(StreamStatisticNames.STREAM_READ_PREFETCH_OPERATIONS);
    }

    @Override
    public void blockAddedToFileCache() {
      incAllGauges(STREAM_READ_BLOCKS_IN_FILE_CACHE, 1);
    }

    @Override
    public void blockRemovedFromFileCache() {
      incAllGauges(STREAM_READ_BLOCKS_IN_FILE_CACHE, -1);
    }

    @Override
    public void prefetchOperationCompleted() {
      incAllGauges(STREAM_READ_ACTIVE_PREFETCH_OPERATIONS, -1);
    }

    @Override
    public void memoryAllocated(int size) {
      incAllGauges(STREAM_READ_ACTIVE_MEMORY_IN_USE, size);
    }

    @Override
    public void memoryFreed(int size) {
      incAllGauges(STREAM_READ_ACTIVE_MEMORY_IN_USE, -size);
    }
  }

  /**
   * Create a stream output statistics instance.
   * @param filesystemStatistics thread-local FS statistics.
   * @return the new instance
   */
  public BlockOutputStreamStatistics newOutputStreamStatistics(
      FileSystem.Statistics filesystemStatistics) {
    return new OutputStreamStatistics(filesystemStatistics);
  }

  /**
   * Merge in the statistics of a single output stream into
   * the filesystem-wide statistics.
   * @param source stream statistics
   */
  private void mergeOutputStreamStatistics(
      OutputStreamStatistics source) {
    incrementCounter(STREAM_WRITE_TOTAL_TIME, source.totalUploadDuration());
    incrementCounter(STREAM_WRITE_QUEUE_DURATION, source.queueDuration);
    incrementCounter(STREAM_WRITE_TOTAL_DATA, source.bytesUploaded);
    incrementCounter(STREAM_WRITE_BLOCK_UPLOADS,
        source.blockUploadsCompleted);
    incrementCounter(STREAM_WRITE_EXCEPTIONS,
        source.lookupCounterValue(
            StreamStatisticNames.STREAM_WRITE_EXCEPTIONS));

    // merge in all the IOStatistics
    final IOStatisticsStore sourceIOStatistics = source.getIOStatistics();
    this.getIOStatistics().aggregate(sourceIOStatistics);

    // propagate any extra values into the FS-level stats;
    incrementMutableCounter(
        COMMITTER_MAGIC_MARKER_PUT.getSymbol(),
        sourceIOStatistics.counters().get(COMMITTER_MAGIC_MARKER_PUT.getSymbol()));

  }

  /**
   * Statistics updated by an output stream during its actual operation.
   * <p>
   * Some of these stats are propagated to any passed in
   * {@link FileSystem.Statistics} instance; this is done
   * in close() for better cross-thread accounting.
   * </p>
   * <p>
   *   Some of the collected statistics are not directly served via
   *   IOStatistics.
   *   They are added to the instrumentation IOStatistics and metric counters
   *   during the {@link #mergeOutputStreamStatistics(OutputStreamStatistics)}
   *   operation.
   * </p>
   */
  private final class OutputStreamStatistics
      extends AbstractS3AStatisticsSource
      implements BlockOutputStreamStatistics {

    private final AtomicLong blocksActive = new AtomicLong(0);
    private final AtomicLong blockUploadsCompleted = new AtomicLong(0);

    private final AtomicLong bytesWritten;
    private final AtomicLong bytesUploaded;
    private final AtomicLong transferDuration = new AtomicLong(0);
    private final AtomicLong queueDuration = new AtomicLong(0);
    private final AtomicInteger blocksAllocated = new AtomicInteger(0);
    private final AtomicInteger blocksReleased = new AtomicInteger(0);

    private final FileSystem.Statistics filesystemStatistics;

    /**
     * Instantiate.
     * @param filesystemStatistics FS Statistics to update in close().
     */
    private OutputStreamStatistics(
        @Nullable FileSystem.Statistics filesystemStatistics) {
      this.filesystemStatistics = filesystemStatistics;
      IOStatisticsStore st = iostatisticsStore()
          .withCounters(
              STREAM_WRITE_BLOCK_UPLOADS.getSymbol(),
              STREAM_WRITE_BYTES.getSymbol(),
              STREAM_WRITE_EXCEPTIONS.getSymbol(),
              STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS.getSymbol(),
              STREAM_WRITE_QUEUE_DURATION.getSymbol(),
              STREAM_WRITE_TOTAL_DATA.getSymbol(),
              STREAM_WRITE_TOTAL_TIME.getSymbol(),
              INVOCATION_HFLUSH.getSymbol(),
              INVOCATION_HSYNC.getSymbol())
          .withGauges(
              STREAM_WRITE_BLOCK_UPLOADS_PENDING.getSymbol(),
              STREAM_WRITE_BLOCK_UPLOADS_BYTES_PENDING.getSymbol())
          .withDurationTracking(
              ACTION_EXECUTOR_ACQUIRED,
              COMMITTER_MAGIC_MARKER_PUT.getSymbol(),
              INVOCATION_ABORT.getSymbol(),
              MULTIPART_UPLOAD_COMPLETED.getSymbol(),
              MULTIPART_UPLOAD_PART_PUT.getSymbol(),
              OBJECT_MULTIPART_UPLOAD_ABORTED.getSymbol(),
              OBJECT_MULTIPART_UPLOAD_INITIATED.getSymbol(),
              OBJECT_PUT_REQUESTS.getSymbol())
          .build();
      setIOStatistics(st);
      // these are extracted to avoid lookups on heavily used counters.
      bytesUploaded = st.getCounterReference(
          STREAM_WRITE_TOTAL_DATA.getSymbol());
      bytesWritten = st.getCounterReference(
          StreamStatisticNames.STREAM_WRITE_BYTES);
    }

    /**
     * Increment the Statistic gauge and the local IOStatistics
     * equivalent.
     * @param statistic statistic
     * @param v value.
     * @return local IOStatistic value
     */
    private long incAllGauges(Statistic statistic, long v) {
      incrementGauge(statistic, v);
      return incGauge(statistic.getSymbol(), v);
    }

    @Override
    public void blockAllocated() {
      blocksAllocated.incrementAndGet();
    }

    @Override
    public void blockReleased() {
      blocksReleased.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     * Increments the counter of block uplaods, and the gauges
     * of block uploads pending (1) and the bytes pending (blockSize).
     */
    @Override
    public void blockUploadQueued(long blockSize) {
      incCounter(StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS);
      incAllGauges(STREAM_WRITE_BLOCK_UPLOADS_PENDING, 1);
      incAllGauges(STREAM_WRITE_BLOCK_UPLOADS_BYTES_PENDING, blockSize);
    }

    /**
     * {@inheritDoc}
     * Update {@link #queueDuration} with queue duration, decrement
     * {@code STREAM_WRITE_BLOCK_UPLOADS_PENDING} gauge and increment
     * {@code STREAM_WRITE_BLOCK_UPLOADS_ACTIVE}.
     */
    @Override
    public void blockUploadStarted(Duration timeInQueue, long blockSize) {
      // the local counter is used in toString reporting.
      queueDuration.addAndGet(timeInQueue.toMillis());
      // update the duration fields in the IOStatistics.
      localIOStatistics().addTimedOperation(
          ACTION_EXECUTOR_ACQUIRED,
          timeInQueue);
      incAllGauges(STREAM_WRITE_BLOCK_UPLOADS_PENDING, -1);
      incAllGauges(STREAM_WRITE_BLOCK_UPLOADS_ACTIVE, 1);
    }

    /**
     * Get the inner class's IO Statistics. This is
     * needed to avoid findbugs warnings about ambiguity.
     * @return the Input Stream's statistics.
     */
    private IOStatisticsStore localIOStatistics() {
      return OutputStreamStatistics.super.getIOStatistics();
    }

    /**
     * {@inheritDoc}
     * Increment the transfer duration; decrement the
     * {@code STREAM_WRITE_BLOCK_UPLOADS_ACTIVE} gauge.
     */
    @Override
    public void blockUploadCompleted(
        Duration timeSinceUploadStarted,
        long blockSize) {
      transferDuration.addAndGet(timeSinceUploadStarted.toMillis());
      incAllGauges(STREAM_WRITE_BLOCK_UPLOADS_ACTIVE, -1);
      blockUploadsCompleted.incrementAndGet();
    }

    /**
     *  A block upload has failed.
     *  A final transfer completed event is still expected, so this
     *  does not decrement any gauges.
     */
    @Override
    public void blockUploadFailed(
        Duration timeSinceUploadStarted,
        long blockSize) {
      incCounter(StreamStatisticNames.STREAM_WRITE_EXCEPTIONS);
    }

    /**
     * Intermediate report of bytes uploaded.
     * Increment counters of bytes upload, reduce the counter and
     * gauge of pending bytes.;
     * @param byteCount bytes uploaded
     */
    @Override
    public void bytesTransferred(long byteCount) {
      bytesUploaded.addAndGet(byteCount);
      incAllGauges(STREAM_WRITE_BLOCK_UPLOADS_BYTES_PENDING, -byteCount);
    }

    @Override
    public void exceptionInMultipartComplete(int count) {
      if (count > 0) {
        incCounter(
            STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS.getSymbol(),
            count);
      }
    }

    @Override
    public void exceptionInMultipartAbort() {
      incCounter(
          STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS.getSymbol());
    }

    @Override
    public long getBytesPendingUpload() {
      return lookupGaugeValue(
          STREAM_WRITE_BLOCK_UPLOADS_BYTES_PENDING.getSymbol());
    }

    @Override
    public void commitUploaded(long size) {
      incrementCounter(COMMITTER_BYTES_UPLOADED, size);
    }

    @Override
    public void hflushInvoked() {
      incCounter(INVOCATION_HFLUSH.getSymbol(), 1);
    }

    @Override
    public void hsyncInvoked() {
      incCounter(INVOCATION_HSYNC.getSymbol(), 1);
    }

    @Override
    public void close() {
      if (getBytesPendingUpload() > 0) {
        LOG.warn("Closing output stream statistics while data is still marked" +
            " as pending upload in {}", this);
      }
      mergeOutputStreamStatistics(this);
      // and patch the FS statistics.
      // provided the stream is closed in the worker thread, this will
      // ensure that the thread-specific worker stats are updated.
      if (filesystemStatistics != null) {
        filesystemStatistics.incrementBytesWritten(bytesUploaded.get());
      }
    }

    /**
     * What is the effective bandwidth of this stream's write.
     * @return the bytes uploaded divided by the total duration.
     */
    private double effectiveBandwidth() {
      double duration = totalUploadDuration() / 1000.0;
      return duration > 0 ?
          (bytesUploaded.get() / duration) : 0;
    }

    /**
     * Total of time spend uploading bytes.
     * @return the transfer duration plus queue duration.
     */
    private long totalUploadDuration() {
      return queueDuration.get() + transferDuration.get();
    }

    @Override
    public int getBlocksAllocated() {
      return blocksAllocated.get();
    }

    @Override
    public int getBlocksReleased() {
      return blocksReleased.get();
    }

    /**
     * Get counters of blocks actively allocated; may be inaccurate
     * if the numbers change during the (non-synchronized) calculation.
     * @return the number of actively allocated blocks.
     */
    @Override
    public int getBlocksActivelyAllocated() {
      return blocksAllocated.get() - blocksReleased.get();
    }

    /**
     * Record bytes written.
     * @param count number of bytes
     */
    @Override
    public void writeBytes(long count) {
      bytesWritten.addAndGet(count);
    }

    /**
     * Get the current count of bytes written.
     * @return the counter value.
     */
    @Override
    public long getBytesWritten() {
      return bytesWritten.get();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "OutputStreamStatistics{");
      sb.append(localIOStatistics().toString());
      sb.append(", blocksActive=").append(blocksActive);
      sb.append(", blockUploadsCompleted=").append(blockUploadsCompleted);
      sb.append(", blocksAllocated=").append(blocksAllocated);
      sb.append(", blocksReleased=").append(blocksReleased);
      sb.append(", blocksActivelyAllocated=")
          .append(getBlocksActivelyAllocated());
      sb.append(", transferDuration=").append(transferDuration).append(" ms");
      sb.append(", totalUploadDuration=").append(totalUploadDuration())
          .append(" ms");
      sb.append(", effectiveBandwidth=").append(effectiveBandwidth())
          .append(" bytes/s");
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Instrumentation exported to S3A Committers.
   * The S3AInstrumentation metrics and
   * {@link #instanceIOStatistics} are updated continuously.
   */
  private final class CommitterStatisticsImpl
      extends AbstractS3AStatisticsSource
      implements CommitterStatistics {

    private CommitterStatisticsImpl() {
      IOStatisticsStore st = iostatisticsStore()
          .withCounters(
              COMMITTER_BYTES_COMMITTED.getSymbol(),
              COMMITTER_BYTES_UPLOADED.getSymbol(),
              COMMITTER_COMMITS_CREATED.getSymbol(),
              COMMITTER_COMMITS_ABORTED.getSymbol(),
              COMMITTER_COMMITS_COMPLETED.getSymbol(),
              COMMITTER_COMMITS_FAILED.getSymbol(),
              COMMITTER_COMMITS_REVERTED.getSymbol(),
              COMMITTER_JOBS_FAILED.getSymbol(),
              COMMITTER_JOBS_SUCCEEDED.getSymbol(),
              COMMITTER_TASKS_FAILED.getSymbol(),
              COMMITTER_TASKS_SUCCEEDED.getSymbol())
          .withDurationTracking(
              COMMITTER_COMMIT_JOB.getSymbol(),
              COMMITTER_LOAD_SINGLE_PENDING_FILE.getSymbol(),
              COMMITTER_MATERIALIZE_FILE.getSymbol(),
              COMMITTER_STAGE_FILE_UPLOAD.getSymbol(),
              OBJECT_PUT_REQUESTS.getSymbol())
          .build();
      setIOStatistics(st);
    }

    /**
     * Increment both the local counter and the S3AInstrumentation counters.
     * @param stat statistic
     * @param value value
     * @return the new value
     */
    private long increment(Statistic stat, long value) {
      incrementCounter(stat, value);
      return incCounter(stat.getSymbol(), value);
    }

    /** A commit has been created. */
    @Override
    public void commitCreated() {
      increment(COMMITTER_COMMITS_CREATED, 1);
    }

    @Override
    public void commitUploaded(long size) {
      increment(COMMITTER_BYTES_UPLOADED, size);
    }

    @Override
    public void commitCompleted(long size) {
      increment(COMMITTER_COMMITS_COMPLETED, 1);
      increment(COMMITTER_BYTES_COMMITTED, size);
    }

    @Override
    public void commitAborted() {
      increment(COMMITTER_COMMITS_ABORTED, 1);
    }

    @Override
    public void commitReverted() {
      increment(COMMITTER_COMMITS_REVERTED, 1);
    }

    @Override
    public void commitFailed() {
      increment(COMMITTER_COMMITS_FAILED, 1);
    }

    @Override
    public void taskCompleted(boolean success) {
      increment(success
              ? COMMITTER_TASKS_SUCCEEDED
              : COMMITTER_TASKS_FAILED,
          1);
    }

    @Override
    public void jobCompleted(boolean success) {
      increment(success
              ? COMMITTER_JOBS_SUCCEEDED
              : COMMITTER_JOBS_FAILED,
          1);
    }

  }

  /**
   * Create a delegation token statistics instance.
   * @return an instance of delegation token statistics
   */
  public DelegationTokenStatistics newDelegationTokenStatistics() {
    return new DelegationTokenStatisticsImpl();
  }

  /**
   * Instrumentation exported to S3A Delegation Token support.
   * The {@link #tokenIssued()} call is a no-op;
   * This statistics class doesn't collect any local statistics.
   * Instead it directly updates the S3A Instrumentation.
   */
  private final class DelegationTokenStatisticsImpl implements
      DelegationTokenStatistics {

    private DelegationTokenStatisticsImpl() {
    }

    @Override
    public void tokenIssued() {
    }

    @Override
    public DurationTracker trackDuration(final String key, final long count) {
      return getDurationTrackerFactory()
          .trackDuration(key, count);
    }
  }

  /**
   * Copy all the metrics to a map of (name, long-value).
   * @return a map of the metrics
   */
  public Map<String, Long> toMap() {
    MetricsToMap metricBuilder = new MetricsToMap(null);
    registry.snapshot(metricBuilder, true);
    return metricBuilder.getMap();
  }

  /**
   * Convert all metrics to a map.
   */
  private static class MetricsToMap extends MetricsRecordBuilder {
    private final MetricsCollector parent;
    private final Map<String, Long> map =
        new HashMap<>();

    MetricsToMap(MetricsCollector parent) {
      this.parent = parent;
    }

    @Override
    public MetricsRecordBuilder tag(MetricsInfo info, String value) {
      return this;
    }

    @Override
    public MetricsRecordBuilder add(MetricsTag tag) {
      return this;
    }

    @Override
    public MetricsRecordBuilder add(AbstractMetric metric) {
      return this;
    }

    @Override
    public MetricsRecordBuilder setContext(String value) {
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo info, int value) {
      return tuple(info, value);
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo info, long value) {
      return tuple(info, value);
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, int value) {
      return tuple(info, value);
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, long value) {
      return tuple(info, value);
    }

    public MetricsToMap tuple(MetricsInfo info, long value) {
      return tuple(info.name(), value);
    }

    public MetricsToMap tuple(String name, long value) {
      map.put(name, value);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, float value) {
      return tuple(info, (long) value);
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, double value) {
      return tuple(info, (long) value);
    }

    @Override
    public MetricsCollector parent() {
      return parent;
    }

    /**
     * Get the map.
     * @return the map of metrics
     */
    public Map<String, Long> getMap() {
      return map;
    }
  }

  /**
   * An IOStatisticsStore which updates metrics on calls to
   * {@link #incrementCounter(String, long)}.
   * This helps keeps FS metrics and IOStats in sync.
   * Duration tracking methods are forwarded to
   * the S3A Instrumentation duration tracker, which will
   * update the instance IOStatistics.
   */
  private final class MetricsUpdatingIOStatisticsStore
      extends ForwardingIOStatisticsStore {

    private MetricsUpdatingIOStatisticsStore() {
      super(S3AInstrumentation.this.getIOStatistics());
    }

    /**
     * Incrementing the counter also implements the metric alongside
     * the IOStatistics value.
     * @param key counter key
     * @param value increment value.
     * @return the value in the wrapped IOStatistics.
     */
    @Override
    public long incrementCounter(final String key, final long value) {
      incrementMutableCounter(key, value);
      return super.incrementCounter(key, value);
    }

    @Override
    public DurationTracker trackDuration(final String key, final long count) {
      return S3AInstrumentation.this.trackDuration(key, count);
    }

    @Override
    public DurationTracker trackDuration(final String key) {
      return S3AInstrumentation.this.trackDuration(key);
    }
  }
}
