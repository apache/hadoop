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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.impl.statistics.AbstractS3AStatisticsSource;
import org.apache.hadoop.fs.s3a.impl.statistics.ChangeTrackerStatistics;
import org.apache.hadoop.fs.s3a.impl.statistics.CommitterStatistics;
import org.apache.hadoop.fs.s3a.impl.statistics.CountersAndGauges;
import org.apache.hadoop.fs.s3a.impl.statistics.CountingChangeTracker;
import org.apache.hadoop.fs.s3a.impl.statistics.DelegationTokenStatistics;
import org.apache.hadoop.fs.s3a.impl.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.s3a.impl.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.s3a.s3guard.MetastoreInstrumentation;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.fs.s3a.Constants.STREAM_READ_GAUGE_INPUT_POLICY;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.demandStringifyIOStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;
import static org.apache.hadoop.fs.s3a.Statistic.*;

/**
 * Instrumentation of S3a.
 * <p></p>
 * History
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
 * <p></p>
 * Counters and metrics are generally addressed in code by their name or
 * {@link Statistic} key. There <i>may</i> be some Statistics which do
 * not have an entry here. To avoid attempts to access such counters failing,
 * the operations to increment/query metric values are designed to handle
 * lookup failures.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AInstrumentation implements Closeable, MetricsSource,
    CountersAndGauges {
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

  private String metricsSourceName;

  private final MetricsRegistry registry =
      new MetricsRegistry("s3aFileSystem").setContext(CONTEXT);
  private final MutableCounterLong ignoredErrors;
  private final MutableQuantiles putLatencyQuantile;
  private final MutableQuantiles throttleRateQuantile;
  private final MutableQuantiles s3GuardThrottleRateQuantile;
  private final MutableCounterLong numberOfFilesCreated;
  private final MutableCounterLong numberOfFilesCopied;
  private final MutableCounterLong bytesOfFilesCopied;
  private final MutableCounterLong numberOfFilesDeleted;
  private final MutableCounterLong numberOfFakeDirectoryDeletes;
  private final MutableCounterLong numberOfDirectoriesCreated;
  private final MutableCounterLong numberOfDirectoriesDeleted;

  /** Instantiate this without caring whether or not S3Guard is enabled. */
  private final S3GuardInstrumentation s3GuardInstrumentation
      = new S3GuardInstrumentation();

  /**
   * All the counters to create.
   */
  private static final Statistic[] COUNTERS_TO_CREATE = {
      INVOCATION_COPY_FROM_LOCAL_FILE,
      INVOCATION_CREATE,
      INVOCATION_CREATE_NON_RECURSIVE,
      INVOCATION_DELETE,
      INVOCATION_EXISTS,
      INVOCATION_GET_DELEGATION_TOKEN,
      INVOCATION_GET_FILE_CHECKSUM,
      INVOCATION_GET_FILE_STATUS,
      INVOCATION_GLOB_STATUS,
      INVOCATION_IS_DIRECTORY,
      INVOCATION_IS_FILE,
      INVOCATION_LIST_FILES,
      INVOCATION_LIST_LOCATED_STATUS,
      INVOCATION_LIST_STATUS,
      INVOCATION_MKDIRS,
      INVOCATION_OPEN,
      INVOCATION_RENAME,

      OBJECT_COPY_REQUESTS,
      OBJECT_DELETE_REQUESTS,
      OBJECT_LIST_REQUESTS,
      OBJECT_CONTINUE_LIST_REQUESTS,
      OBJECT_METADATA_REQUESTS,
      OBJECT_MULTIPART_UPLOAD_ABORTED,
      OBJECT_PUT_BYTES,
      OBJECT_PUT_REQUESTS,
      OBJECT_PUT_REQUESTS_COMPLETED,
      OBJECT_SELECT_REQUESTS,

      STREAM_READ_ABORTED,
      STREAM_READ_BYTES_DISCARDED_ABORT,
      STREAM_READ_CLOSED,
      STREAM_READ_CLOSE_BYTES_READ,
      STREAM_READ_CLOSE_OPERATIONS,
      STREAM_READ_OPENED,
      STREAM_READ_BYTES,
      STREAM_READ_EXCEPTIONS,
      STREAM_READ_FULLY_OPERATIONS,
      STREAM_READ_OPERATIONS,
      STREAM_READ_OPERATIONS_INCOMPLETE,
      STREAM_READ_SEEK_OPERATIONS,
      STREAM_READ_SEEK_POLICY_CHANGED,
      STREAM_READ_SEEK_BACKWARD_OPERATIONS,
      STREAM_READ_SEEK_FORWARD_OPERATIONS,
      STREAM_READ_SEEK_BYTES_BACKWARDS,
      STREAM_READ_SEEK_BYTES_READ,
      STREAM_READ_SEEK_BYTES_SKIPPED,
      STREAM_READ_TOTAL_BYTES,
      STREAM_READ_VERSION_MISMATCHES,

      STREAM_WRITE_FAILURES,
      STREAM_WRITE_BLOCK_UPLOADS,
      STREAM_WRITE_BLOCK_UPLOADS_COMMITTED,
      STREAM_WRITE_BLOCK_UPLOADS_ABORTED,
      STREAM_WRITE_TOTAL_TIME,
      STREAM_WRITE_TOTAL_DATA,

      COMMITTER_COMMITS_CREATED,
      COMMITTER_COMMITS_COMPLETED,
      COMMITTER_JOBS_SUCCEEDED,
      COMMITTER_JOBS_FAILED,
      COMMITTER_TASKS_SUCCEEDED,
      COMMITTER_TASKS_FAILED,
      COMMITTER_BYTES_COMMITTED,
      COMMITTER_BYTES_UPLOADED,
      COMMITTER_COMMITS_FAILED,
      COMMITTER_COMMITS_ABORTED,
      COMMITTER_COMMITS_REVERTED,
      COMMITTER_MAGIC_FILES_CREATED,

      S3GUARD_METADATASTORE_PUT_PATH_REQUEST,
      S3GUARD_METADATASTORE_INITIALIZATION,
      S3GUARD_METADATASTORE_RECORD_DELETES,
      S3GUARD_METADATASTORE_RECORD_READS,
      S3GUARD_METADATASTORE_RECORD_WRITES,
      S3GUARD_METADATASTORE_RETRY,
      S3GUARD_METADATASTORE_THROTTLED,
      S3GUARD_METADATASTORE_AUTHORITATIVE_DIRECTORIES_UPDATED,

      STORE_IO_THROTTLED,
      STORE_IO_REQUEST,
      STORE_IO_RETRY,

      DELEGATION_TOKENS_ISSUED,
      FILES_DELETE_REJECTED,
      MULTIPART_INSTANTIATED,
      MULTIPART_PART_PUT,
      MULTIPART_PART_PUT_BYTES,
      MULTIPART_UPLOAD_ABORTED,
      MULTIPART_UPLOAD_ABORT_UNDER_PATH_INVOKED,
      MULTIPART_UPLOAD_COMPLETED,
      MULTIPART_UPLOAD_STARTED
  };

  private static final Statistic[] GAUGES_TO_CREATE = {
      OBJECT_PUT_REQUESTS_ACTIVE,
      OBJECT_PUT_BYTES_PENDING,
      STREAM_WRITE_BLOCK_UPLOADS_ACTIVE,
      STREAM_WRITE_BLOCK_UPLOADS_PENDING,
      STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING,
  };

  public S3AInstrumentation(URI name) {
    UUID fileSystemInstanceId = UUID.randomUUID();
    registry.tag(METRIC_TAG_FILESYSTEM_ID,
        "A unique identifier for the instance",
        fileSystemInstanceId.toString());
    registry.tag(METRIC_TAG_BUCKET, "Hostname from the FS URL", name.getHost());
    numberOfFilesCreated = counter(FILES_CREATED);
    numberOfFilesCopied = counter(FILES_COPIED);
    bytesOfFilesCopied = counter(FILES_COPIED_BYTES);
    numberOfFilesDeleted = counter(FILES_DELETED);
    numberOfFakeDirectoryDeletes = counter(FAKE_DIRECTORIES_DELETED);
    numberOfDirectoriesCreated = counter(DIRECTORIES_CREATED);
    numberOfDirectoriesDeleted = counter(DIRECTORIES_DELETED);
    ignoredErrors = counter(IGNORED_ERRORS);
    for (Statistic statistic : COUNTERS_TO_CREATE) {
      counter(statistic);
    }
    for (Statistic statistic : GAUGES_TO_CREATE) {
      gauge(statistic.getSymbol(), statistic.getDescription());
    }
    //todo need a config for the quantiles interval?
    int interval = 1;
    putLatencyQuantile = quantiles(S3GUARD_METADATASTORE_PUT_PATH_LATENCY,
        "ops", "latency", interval);
    s3GuardThrottleRateQuantile = quantiles(S3GUARD_METADATASTORE_THROTTLE_RATE,
        "events", "frequency (Hz)", interval);
    throttleRateQuantile = quantiles(STORE_IO_THROTTLE_RATE,
        "events", "frequency (Hz)", interval);

    registerAsMetricsSource(name);
  }

  @VisibleForTesting
  public MetricsSystem getMetricsSystem() {
    synchronized (METRICS_SYSTEM_LOCK) {
      if (metricsSystem == null) {
        metricsSystem = new MetricsSystemImpl();
        metricsSystem.init(METRICS_SYSTEM_NAME);
      }
    }
    return metricsSystem;
  }

  /**
   * Register this instance as a metrics source.
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
    metricsSourceName = msName + "-" + name.getHost();
    metricsSystem.register(metricsSourceName, "", this);
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
   * Create a gauge in the registry.
   * @param name name gauge name
   * @param desc description
   * @return the gauge
   */
  protected final MutableGaugeLong gauge(String name, String desc) {
    return registry.newGauge(name, desc, 0L);
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
          + " is not a MutableCounterLong: " + metric);
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
   * Indicate that S3A created a file.
   */
  public void fileCreated() {
    numberOfFilesCreated.incr();
  }

  /**
   * Indicate that S3A deleted one or more files.
   * @param count number of files.
   */
  public void fileDeleted(int count) {
    numberOfFilesDeleted.incr(count);
  }

  /**
   * Indicate that fake directory request was made.
   * @param count number of directory entries included in the delete request.
   */
  public void fakeDirsDeleted(int count) {
    numberOfFakeDirectoryDeletes.incr(count);
  }

  /**
   * Indicate that S3A created a directory.
   */
  public void directoryCreated() {
    numberOfDirectoriesCreated.incr();
  }

  /**
   * Indicate that S3A just deleted a directory.
   */
  public void directoryDeleted() {
    numberOfDirectoriesDeleted.incr();
  }

  /**
   * Indicate that S3A copied some files within the store.
   *
   * @param files number of files
   * @param size total size in bytes
   */
  public void filesCopied(int files, long size) {
    numberOfFilesCopied.incr(files);
    bytesOfFilesCopied.incr(size);
  }

  /**
   * Note that an error was ignored.
   */
  public void errorIgnored() {
    ignoredErrors.incr();
  }

  /**
   * Increment a specific counter.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   */
  public void incrementCounter(Statistic op, long count) {
    incrementNamedCounter(op.getSymbol(), count);
  }

  private void incrementNamedCounter(final String name, final long count) {
    MutableCounterLong counter = lookupCounter(name);
    if (counter != null) {
      counter.incr(count);
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
   * Increment a specific counter.
   * No-op if not defined.
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
   * Create a stream input statistics instance.
   * @return the new instance
   * @param filesystemStatistics FS Stats.
   */
  public S3AInputStreamStatistics newInputStreamStatistics(
      final FileSystem.Statistics filesystemStatistics) {
    return new InputStreamStatisticsImpl(filesystemStatistics);
  }

  /**
   * Create a MetastoreInstrumentation instrumentation instance.
   * There's likely to be at most one instance of this per FS instance.
   * @return the S3Guard instrumentation point.
   */
  public MetastoreInstrumentation getS3GuardInstrumentation() {
    return s3GuardInstrumentation;
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

  public void close() {
    synchronized (METRICS_SYSTEM_LOCK) {
      // it is critical to close each quantile, as they start a scheduled
      // task in a shared thread pool.
      putLatencyQuantile.stop();
      throttleRateQuantile.stop();
      s3GuardThrottleRateQuantile.stop();
      metricsSystem.unregisterSource(metricsSourceName);
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

  /**
   * Statistics updated by an S3AInputStream during its actual operation.
   * <p></p>
   * When {@code unbuffer()} is called, the changed numbers are propagated
   * to the S3AFileSystem metrics.
   * <p></p>
   * When {@code close()} is called, the final set of numbers are propagated
   * to the S3AFileSystem metrics.
   * The {@link FileSystem.Statistics} statistics passed in are also
   * updated. This ensures that whichever thread calls close() gets the
   * total count of bytes read, even if any work is done in other
   * threads.
   * 
   */
  private final class InputStreamStatisticsImpl
      extends AbstractS3AStatisticsSource
      implements S3AInputStreamStatistics {

    /**
     * Distance used when incrementing FS stats.
     */
    private static final int DISTANCE = 5;

    private final FileSystem.Statistics filesystemStatistics;

    /**
     * The statistics from the last merge.
     */
    private IOStatisticsSnapshot mergedStats;

    private InputStreamStatisticsImpl(
        FileSystem.Statistics filesystemStatistics) {
      this.filesystemStatistics = filesystemStatistics;
      IOStatisticsStore st = iostatisticsStore()
          .withCounters(
              StreamStatisticNames.STREAM_READ_ABORTED,
              StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_ABORT,
              StreamStatisticNames.STREAM_READ_CLOSED,
              StreamStatisticNames.STREAM_READ_CLOSE_BYTES_READ,
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
              StreamStatisticNames.STREAM_READ_SEEK_BYTES_READ,
              StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED,
              StreamStatisticNames.STREAM_READ_TOTAL_BYTES,
              StreamStatisticNames.STREAM_READ_VERSION_MISMATCHES)
          .withGauges(STREAM_READ_GAUGE_INPUT_POLICY)
          .withDurationTracking(OP_HTTP_GET_REQUEST)
          .build();
      setIOStatistics(st);
      mergedStats = snapshotIOStatistics(st);
    }

    private long increment(String name) {
      return incCounter(name);
    }

    private long increment(String name, long value) {
      return incCounter(name, value);
    }

    /**
     * Seek backwards, incrementing the seek and backward seek counters.
     * @param negativeOffset how far was the seek?
     * This is expected to be negative.
     */
    @Override
    public void seekBackwards(long negativeOffset) {
      increment(StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS);
      increment(StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS);
      increment(StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS,
          -negativeOffset);
    }

    /**
     * Record a forward seek, adding a seek operation, a forward
     * seek operation, and any bytes skipped.
     * @param bytesForward
     * @param bytesRead number of bytes skipped by reading from the stream.
 * If the seek was implemented by a close + reopen, set this to zero.
     */
    @Override
    public void seekForwards(final long bytesForward,
        long bytesRead) {
      increment(StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS);
      increment(StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS);
      increment(StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED,
          bytesForward);
      if (bytesRead > 0) {
        increment(StreamStatisticNames.STREAM_READ_SEEK_BYTES_READ,
            bytesRead);
        increment(StreamStatisticNames.STREAM_READ_TOTAL_BYTES,
            bytesRead);
      }
    }

    /**
     * The inner stream was opened.
     * @return the previous count
     */
    @Override
    public long streamOpened() {
      return increment(StreamStatisticNames.STREAM_READ_OPENED);
    }

    /**
     * The inner stream was closed.
     * @param abortedConnection flag to indicate the stream was aborted,
     * rather than closed cleanly
     * @param remainingInCurrentRequest the number of bytes remaining in
     * the current request.
     */
    @Override
    public void streamClose(boolean abortedConnection,
        long remainingInCurrentRequest) {
      if (abortedConnection) {
        increment(StreamStatisticNames.STREAM_READ_ABORTED);
        increment(StreamStatisticNames.STREAM_READ_BYTES_DISCARDED_ABORT,
            remainingInCurrentRequest);
      } else {
        increment(StreamStatisticNames.STREAM_READ_CLOSED);
        increment(StreamStatisticNames.STREAM_READ_CLOSE_BYTES_READ,
            remainingInCurrentRequest);
        increment(StreamStatisticNames.STREAM_READ_TOTAL_BYTES,
            remainingInCurrentRequest);
      }
    }

    /**
     * An ignored stream read exception was received.
     */
    @Override
    public void readException() {
      increment(StreamStatisticNames.STREAM_READ_EXCEPTIONS);
    }

    /**
     * Increment the bytes read counter by the number of bytes;
     * no-op if the argument is negative.
     * @param bytes number of bytes read
     */
    @Override
    public void bytesRead(long bytes) {
      if (bytes > 0) {
        increment(StreamStatisticNames.STREAM_READ_BYTES, bytes);
        increment(StreamStatisticNames.STREAM_READ_TOTAL_BYTES, bytes);
      }
    }

    /**
     * A {@code read(byte[] buf, int off, int len)} operation has started.
     * @param pos starting position of the read
     * @param len length of bytes to read
     */
    @Override
    public void readOperationStarted(long pos, long len) {
      increment(StreamStatisticNames.STREAM_READ_OPERATIONS);
    }

    /**
     * A {@code PositionedRead.read(position, buffer, offset, length)}
     * operation has just started.
     * @param pos starting position of the read
     * @param len length of bytes to read
     */
    @Override
    public void readFullyOperationStarted(long pos, long len) {
      increment(StreamStatisticNames.STREAM_READ_FULLY_OPERATIONS);
    }

    /**
     * A read operation has completed.
     * @param requested number of requested bytes
     * @param actual the actual number of bytes
     */
    @Override
    public void readOperationCompleted(int requested, int actual) {
      if (requested > actual) {
        increment(StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE);
      }
    }

    /**
     * Close triggers the merge of statistics into the filesystem's
     * instrumentation instance.
     */
    @Override
    public void close() {
      merge(true);
    }

    /**
     * The input policy has been switched.
     * @param updatedPolicy enum value of new policy.
     */
    @Override
    public void inputPolicySet(int updatedPolicy) {
      increment(StreamStatisticNames.STREAM_READ_SEEK_POLICY_CHANGED);
      getIOStatistics().setGauge(STREAM_READ_GAUGE_INPUT_POLICY, updatedPolicy);
    }

    /**
     * The change tracker increments {@code versionMismatches} on any
     * mismatch.
     * @return change tracking.
     */
    @Override
    public ChangeTrackerStatistics getChangeTrackerStatistics() {
      return new CountingChangeTracker(
          getIOStatistics().getCounterReference(
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
          getIOStatistics()));
      sb.append('}');
      return sb.toString();
    }

    /**
     * Merge the statistics into the filesystem's instrumentation instance.
     * Takes a diff between the current version of the stats and the
     * version of the stats when merge was last called, and merges the diff
     * into the instrumentation instance. Used to periodically merge the
     * stats into the fs-wide stats.
     * <p></p>
     * <b>Behavior is undefined if called on a closed instance.</b>
     */
    @Override
    public void merge(boolean isClosed) {

      LOG.debug("Merging statistics into FS statistics in {}: {}",
          (isClosed ? "close()" : "unbuffer()"),
          demandStringifyIOStatistics(getIOStatistics()));
      mergeInputStreamStatistics();
      mergedStats = snapshotIOStatistics(getIOStatistics());

      if (isClosed) {
        // stream is being closed.
        // increment the filesystem statistics for this thread.
        if (filesystemStatistics != null) {
          long t = getTotalBytesRead();
          filesystemStatistics.incrementBytesRead(t);
          filesystemStatistics.incrementBytesReadByDistance(DISTANCE, t);
        }
      }
    }

    /**
     * Propagate a counter from the instance-level statistics
     * to the S3A instrumentation, subtracting the previous marged value.
     * @param name statistic to promote
     */
    void promoteIOCounter(String name) {
      incrementNamedCounter(name,
          lookupCounterValue(name)
              - mergedStats.counters().get(name));
    }

    /**
     * Merge in the statistics of a single input stream into
     * the filesystem-wide statistics.
     */
    private void mergeInputStreamStatistics() {
      // iterate through all the counters
      getIOStatistics().counters()
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
          StreamStatisticNames.STREAM_READ_CLOSE_BYTES_READ);
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
      return lookupCounterValue(StreamStatisticNames.STREAM_READ_CLOSED);
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
      return getIOStatistics().gauges().get(STREAM_READ_GAUGE_INPUT_POLICY);
    }

    @Override
    public void getRequestCompleted(final Duration duration) {
      getIOStatistics().addTimedOperation(OP_HTTP_GET_REQUEST, duration);
    }
  }

  /**
   * Create a stream output statistics instance.
   * @return the new instance
   */
  public BlockOutputStreamStatistics newOutputStreamStatistics(
      FileSystem.Statistics filesystemStatistics) {
    return new BlockOutputStreamStatisticsImpl(filesystemStatistics);
  }

  /**
   * Merge in the statistics of a single output stream into
   * the filesystem-wide statistics.
   * @param source stream statistics
   */
  private void mergeOutputStreamStatistics(
      BlockOutputStreamStatisticsImpl source) {
    incrementCounter(STREAM_WRITE_TOTAL_TIME, source.totalUploadDuration());
    incrementCounter(STREAM_WRITE_QUEUE_DURATION, source.queueDuration);
    incrementCounter(STREAM_WRITE_TOTAL_DATA, source.bytesUploaded);
    incrementCounter(STREAM_WRITE_BLOCK_UPLOADS,
        source.blockUploadsCompleted);
    incrementCounter(STREAM_WRITE_FAILURES,
        source.lookupCounterValue(
            StreamStatisticNames.STREAM_WRITE_EXCEPTIONS));
  }

  /**
   * Statistics updated by an output stream during its actual operation.
   * <p>
   * Some of these stats are propagated to any passed in
   * {@link FileSystem.Statistics} instance; this is only done
   * in close() for better cross-thread accounting.
   */
  private final class BlockOutputStreamStatisticsImpl
      extends AbstractS3AStatisticsSource
      implements BlockOutputStreamStatistics {

    private final AtomicLong blocksInQueue = new AtomicLong(0);
    private final AtomicLong blocksActive = new AtomicLong(0);
    private final AtomicLong blockUploadsCompleted = new AtomicLong(0);
    private final AtomicLong bytesPendingUpload = new AtomicLong(0);

    private final AtomicLong bytesWritten;
    private final AtomicLong bytesUploaded;
    private final AtomicLong transferDuration = new AtomicLong(0);
    private final AtomicLong queueDuration = new AtomicLong(0);
    private final AtomicInteger blocksAllocated = new AtomicInteger(0);
    private final AtomicInteger blocksReleased = new AtomicInteger(0);

    private final FileSystem.Statistics filesystemStatistics;

    private BlockOutputStreamStatisticsImpl(
        @Nullable FileSystem.Statistics filesystemStatistics) {
      this.filesystemStatistics = filesystemStatistics;
      IOStatisticsStore st = iostatisticsStore()
          .withCounters(
              StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS,
              StreamStatisticNames.STREAM_WRITE_BYTES,
              StreamStatisticNames.STREAM_WRITE_EXCEPTIONS,
              StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING,
              STREAM_WRITE_TOTAL_TIME.getSymbol(),
              STREAM_WRITE_QUEUE_DURATION.getSymbol(),
              STREAM_WRITE_TOTAL_DATA.getSymbol(),
              STREAM_WRITE_FAILURES.getSymbol(),
              STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS.getSymbol()
              )
          .withGauges(
              STREAM_WRITE_BLOCK_UPLOADS_PENDING.getSymbol(),
              STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING.getSymbol())
          .build();
      setIOStatistics(st);
      // these are extracted to avoid lookups on heavily used counters.
      bytesUploaded = st.getCounterReference(
          STREAM_WRITE_TOTAL_DATA.getSymbol());
      bytesWritten = st.getCounterReference(
          StreamStatisticNames.STREAM_WRITE_BYTES);
    }

    /**
     * Increment the Statistic gauge and the local IOStats
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
     * A block has been allocated.
     */
    @Override
    public void blockAllocated() {
      blocksAllocated.incrementAndGet();
    }

    /**
     * A block has been released.
     */
    @Override
    public void blockReleased() {
      blocksReleased.incrementAndGet();
    }

    /**
     * Block is queued for upload.
     */
    @Override
    public void blockUploadQueued(int blockSize) {
      incCounter(StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS);

      blocksInQueue.incrementAndGet();
      bytesPendingUpload.addAndGet(blockSize);
      incAllGauges(STREAM_WRITE_BLOCK_UPLOADS_PENDING, 1);
      incAllGauges(STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING, blockSize);
    }

    /** Queued block has been scheduled for upload. */
    @Override
    public void blockUploadStarted(long duration, int blockSize) {
      queueDuration.addAndGet(duration);
      blocksInQueue.decrementAndGet();
      incAllGauges(STREAM_WRITE_BLOCK_UPLOADS_PENDING, -1);
      incAllGauges(STREAM_WRITE_BLOCK_UPLOADS_ACTIVE, 1);
    }

    /** A block upload has completed. */
    @Override
    public void blockUploadCompleted(long duration, int blockSize) {
      transferDuration.addAndGet(duration);
      incAllGauges(STREAM_WRITE_BLOCK_UPLOADS_ACTIVE, -1);
      blockUploadsCompleted.incrementAndGet();
    }

    /**
     *  A block upload has failed.
     *  A final transfer completed event is still expected, so this
     *  does not decrement the active block counter.
     */
    @Override
    public void blockUploadFailed(long duration, int blockSize) {
      incCounter(StreamStatisticNames.STREAM_WRITE_EXCEPTIONS);
    }

    /** Intermediate report of bytes uploaded. */
    @Override
    public void bytesTransferred(long byteCount) {
      bytesUploaded.addAndGet(byteCount);
      bytesPendingUpload.addAndGet(-byteCount);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING, -byteCount);
    }

    /**
     * Note exception in a multipart complete.
     * @param count count of exceptions
     */
    @Override
    public void exceptionInMultipartComplete(int count) {
      if (count > 0) {
        incCounter(
            STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS.getSymbol(),
            count);
      }
    }

    /**
     * Note an exception in a multipart abort.
     */
    @Override
    public void exceptionInMultipartAbort() {
      incCounter(
          STREAM_WRITE_EXCEPTIONS_COMPLETING_UPLOADS.getSymbol());
    }

    /**
     * Get the number of bytes pending upload.
     * @return the number of bytes in the pending upload state.
     */
    @Override
    public long getBytesPendingUpload() {
      return bytesPendingUpload.get();
    }

    /**
     * Data has been uploaded to be committed in a subsequent operation;
     * to be called at the end of the write.
     * @param size size in bytes
     */
    @Override
    public void commitUploaded(long size) {
      incrementCounter(COMMITTER_BYTES_UPLOADED, size);
    }

    /**
     * Output stream has closed.
     * Trigger merge in of all statistics not updated during operation.
     */
    @Override
    public void close() {
      if (bytesPendingUpload.get() > 0) {
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

    long averageQueueTime() {
      long l = getCounterValue(StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS);
      return l > 0 ?
          (queueDuration.get() / l) : 0;
    }

    double effectiveBandwidth() {
      double duration = totalUploadDuration() / 1000.0;
      return duration > 0 ?
          (bytesUploaded.get() / duration) : 0;
    }

    long totalUploadDuration() {
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
      sb.append(getIOStatistics().toString());
      sb.append(", blocksInQueue=").append(blocksInQueue);
      sb.append(", blocksActive=").append(blocksActive);
      sb.append(", blockUploadsCompleted=").append(blockUploadsCompleted);
      sb.append(", bytesPendingUpload=").append(bytesPendingUpload);
      sb.append(", bytesUploaded=").append(bytesUploaded);
      sb.append(", bytesWritten=").append(bytesWritten);
      sb.append(", blocksAllocated=").append(blocksAllocated);
      sb.append(", blocksReleased=").append(blocksReleased);
      sb.append(", blocksActivelyAllocated=")
          .append(getBlocksActivelyAllocated());
      sb.append(", transferDuration=").append(transferDuration).append(" ms");
      sb.append(", queueDuration=").append(queueDuration).append(" ms");
      sb.append(", averageQueueTime=").append(averageQueueTime()).append(" ms");
      sb.append(", totalUploadDuration=").append(totalUploadDuration())
          .append(" ms");
      sb.append(", effectiveBandwidth=").append(effectiveBandwidth())
          .append(" bytes/s");
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Instrumentation exported to S3Guard.
   */
  private final class S3GuardInstrumentation
      implements MetastoreInstrumentation {

    @Override
    public void initialized() {
      incrementCounter(S3GUARD_METADATASTORE_INITIALIZATION, 1);
    }

    @Override
    public void storeClosed() {

    }

    @Override
    public void throttled() {
      // counters are incremented by owner.
    }

    @Override
    public void retrying() {
      // counters are incremented by owner.
    }

    @Override
    public void recordsDeleted(int count) {
      incrementCounter(S3GUARD_METADATASTORE_RECORD_DELETES, count);
    }

    @Override
    public void recordsRead(int count) {
      incrementCounter(S3GUARD_METADATASTORE_RECORD_READS, count);
    }

    /**
     * records have been written (including deleted).
     * @param count number of records written.
     */
    @Override
    public void recordsWritten(int count) {
      incrementCounter(S3GUARD_METADATASTORE_RECORD_WRITES, count);
    }

    @Override
    public void directoryMarkedAuthoritative() {
      incrementCounter(S3GUARD_METADATASTORE_AUTHORITATIVE_DIRECTORIES_UPDATED,
          1);
    }

    @Override
    public void entryAdded(final long durationNanos) {
      addValueToQuantiles(
          S3GUARD_METADATASTORE_PUT_PATH_LATENCY,
          durationNanos);
      incrementCounter(S3GUARD_METADATASTORE_PUT_PATH_REQUEST, 1);
    }

  }

  /**
   * Instrumentation exported to S3A Committers.
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
          .build();
      setIOStatistics(st);
    }

    private long increment(Statistic stat, long value) {
      incrementCounter(stat, value);
      return incCounter(stat.getSymbol(), value);
    }

    /** A commit has been created. */
    @Override
    public void commitCreated() {
      increment(COMMITTER_COMMITS_CREATED, 1);
    }

    /**
     * Data has been uploaded to be committed in a subsequent operation.
     * @param size size in bytes
     */
    @Override
    public void commitUploaded(long size) {
      increment(COMMITTER_BYTES_UPLOADED, size);
    }

    /**
     * A commit has been completed.
     * @param size size in bytes
     */
    @Override
    public void commitCompleted(long size) {
      increment(COMMITTER_COMMITS_COMPLETED, 1);
      increment(COMMITTER_BYTES_COMMITTED, size);
    }

    /** A commit has been aborted. */
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
      increment(
          success ? COMMITTER_TASKS_SUCCEEDED
              : COMMITTER_TASKS_FAILED,
          1);
    }

    @Override
    public void jobCompleted(boolean success) {
      increment(
          success ? COMMITTER_JOBS_SUCCEEDED
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
   */
  private final class DelegationTokenStatisticsImpl implements
      DelegationTokenStatistics {

    private DelegationTokenStatisticsImpl() {
    }

    /** A token has been issued. */
    @Override
    public void tokenIssued() {
      incrementCounter(DELEGATION_TOKENS_ISSUED, 1);
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
        new HashMap<>(COUNTERS_TO_CREATE.length * 2);

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

}
