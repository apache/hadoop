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
import org.apache.hadoop.fs.s3a.impl.statistics.ChangeTrackerStatistics;
import org.apache.hadoop.fs.s3a.impl.statistics.CommitterStatistics;
import org.apache.hadoop.fs.s3a.impl.statistics.CountingChangeTracker;
import org.apache.hadoop.fs.s3a.impl.statistics.DelegationTokenStatistics;
import org.apache.hadoop.fs.s3a.impl.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.s3a.impl.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.s3a.s3guard.MetastoreInstrumentation;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.fs.statistics.impl.DynamicIOStatisticsBuilder;
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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsImplementationSupport.createDynamicIOStatistics;
import static org.apache.hadoop.fs.s3a.Statistic.*;

/**
 * Instrumentation of S3a.
 * Derived from the {@code AzureFileSystemInstrumentation}.
 *
 * Counters and metrics are generally addressed in code by their name or
 * {@link Statistic} key. There <i>may</i> be some Statistics which do
 * not have an entry here. To avoid attempts to access such counters failing,
 * the operations to increment/query metric values are designed to handle
 * lookup failures.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AInstrumentation implements Closeable, MetricsSource {
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
  private static Object metricsSystemLock = new Object();
  private static MetricsSystem metricsSystem = null;
  private static int metricsSourceNameCounter = 0;
  private static int metricsSourceActiveCounter = 0;

  private String metricsSourceName;

  private final MetricsRegistry registry =
      new MetricsRegistry("s3aFileSystem").setContext(CONTEXT);
  private final MutableCounterLong streamOpenOperations;
  private final MutableCounterLong streamCloseOperations;
  private final MutableCounterLong streamClosed;
  private final MutableCounterLong streamAborted;
  private final MutableCounterLong streamSeekOperations;
  private final MutableCounterLong streamReadExceptions;
  private final MutableCounterLong streamForwardSeekOperations;
  private final MutableCounterLong streamBackwardSeekOperations;
  private final MutableCounterLong streamBytesSkippedOnSeek;
  private final MutableCounterLong streamBytesBackwardsOnSeek;
  private final MutableCounterLong streamBytesRead;
  private final MutableCounterLong streamReadOperations;
  private final MutableCounterLong streamReadFullyOperations;
  private final MutableCounterLong streamReadsIncomplete;
  private final MutableCounterLong streamBytesReadInClose;
  private final MutableCounterLong streamBytesDiscardedInAbort;
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
      DELEGATION_TOKENS_ISSUED,
      FILES_DELETE_REJECTED
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
    streamOpenOperations = counter(STREAM_OPENED);
    streamCloseOperations = counter(STREAM_CLOSE_OPERATIONS);
    streamClosed = counter(STREAM_CLOSED);
    streamAborted = counter(STREAM_ABORTED);
    streamSeekOperations = counter(STREAM_SEEK_OPERATIONS);
    streamReadExceptions = counter(STREAM_READ_EXCEPTIONS);
    streamForwardSeekOperations =
        counter(STREAM_FORWARD_SEEK_OPERATIONS);
    streamBackwardSeekOperations =
        counter(STREAM_BACKWARD_SEEK_OPERATIONS);
    streamBytesSkippedOnSeek = counter(STREAM_SEEK_BYTES_SKIPPED);
    streamBytesBackwardsOnSeek =
        counter(STREAM_SEEK_BYTES_BACKWARDS);
    streamBytesRead = counter(STREAM_SEEK_BYTES_READ);
    streamReadOperations = counter(STREAM_READ_OPERATIONS);
    streamReadFullyOperations =
        counter(STREAM_READ_FULLY_OPERATIONS);
    streamReadsIncomplete =
        counter(STREAM_READ_OPERATIONS_INCOMPLETE);
    streamBytesReadInClose = counter(STREAM_CLOSE_BYTES_READ);
    streamBytesDiscardedInAbort = counter(STREAM_ABORT_BYTES_DISCARDED);
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
    synchronized (metricsSystemLock) {
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
    synchronized(metricsSystemLock) {
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
    MutableCounterLong counter = lookupCounter(op.getSymbol());
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

  /**
   * Merge in the statistics of a single input stream into
   * the filesystem-wide statistics.
   * @param statistics stream statistics
   */
  private void mergeInputStreamStatistics(InputStreamStatisticsImpl statistics) {
    streamOpenOperations.incr(statistics.openOperations);
    streamCloseOperations.incr(statistics.closeOperations);
    streamClosed.incr(statistics.closed);
    streamAborted.incr(statistics.aborted);
    streamSeekOperations.incr(statistics.seekOperations);
    streamReadExceptions.incr(statistics.readExceptions);
    streamForwardSeekOperations.incr(statistics.forwardSeekOperations);
    streamBytesSkippedOnSeek.incr(statistics.bytesSkippedOnSeek);
    streamBackwardSeekOperations.incr(statistics.backwardSeekOperations);
    streamBytesBackwardsOnSeek.incr(statistics.bytesBackwardsOnSeek);
    streamBytesRead.incr(statistics.bytesRead);
    streamReadOperations.incr(statistics.readOperations);
    streamReadFullyOperations.incr(statistics.readFullyOperations);
    streamReadsIncomplete.incr(statistics.readsIncomplete);
    streamBytesReadInClose.incr(statistics.bytesReadInClose);
    streamBytesDiscardedInAbort.incr(statistics.bytesDiscardedInAbort);
    incrementCounter(STREAM_READ_VERSION_MISMATCHES,
        statistics.versionMismatches.get());
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(registry.info().name()), true);
  }

  public void close() {
    synchronized (metricsSystemLock) {
      // it is critical to close each quantile, as they start a scheduled
      // task in a shared thread pool.
      putLatencyQuantile.stop();
      throttleRateQuantile.stop();
      s3GuardThrottleRateQuantile.stop();
      metricsSystem.unregisterSource(metricsSourceName);
      int activeSources = --metricsSourceActiveCounter;
      if (activeSources == 0) {
        LOG.debug("Shutting down metrics publisher");
        metricsSystem.publishMetricsNow();
        metricsSystem.shutdown();
        metricsSystem = null;
      }
    }
  }

  /**
   * Statistics updated by an input stream during its actual operation.
   * These counters are marked as volatile so that IOStatistics on the stream
   * will get the latest values.
   * They are only to be incremented within synchronized blocks.
   */
  private final class InputStreamStatisticsImpl implements
      S3AInputStreamStatistics {

    /**
     * Distance used when incrementing FS stats.
     */
    private static final int DISTANCE = 5;

    private final FileSystem.Statistics filesystemStatistics;

    public volatile long openOperations;
    public volatile long closeOperations;
    public volatile long closed;
    public volatile long aborted;
    public volatile long seekOperations;
    public volatile long readExceptions;
    public volatile long forwardSeekOperations;
    public volatile long backwardSeekOperations;
    public volatile long bytesRead;
    public volatile long bytesSkippedOnSeek;
    public volatile long bytesBackwardsOnSeek;
    public volatile long readOperations;
    public volatile long readFullyOperations;
    public volatile long readsIncomplete;
    public volatile long bytesReadInClose;
    public volatile long bytesDiscardedInAbort;
    public volatile long policySetCount;
    public volatile long inputPolicy;
    /** This is atomic so that it can be passed as a reference. */
    private final AtomicLong versionMismatches = new AtomicLong(0);
    private InputStreamStatisticsImpl mergedStats;

    private InputStreamStatisticsImpl(FileSystem.Statistics filesystemStatistics) {

      this.filesystemStatistics = filesystemStatistics;
    }

    /**
     * Seek backwards, incrementing the seek and backward seek counters.
     * @param negativeOffset how far was the seek?
     * This is expected to be negative.
     */
    @Override
    public void seekBackwards(long negativeOffset) {
      seekOperations++;
      backwardSeekOperations++;
      bytesBackwardsOnSeek -= negativeOffset;
    }

    /**
     * Record a forward seek, adding a seek operation, a forward
     * seek operation, and any bytes skipped.
     * @param skipped number of bytes skipped by reading from the stream.
     * If the seek was implemented by a close + reopen, set this to zero.
     */
    @Override
    public void seekForwards(long skipped) {
      seekOperations++;
      forwardSeekOperations++;
      if (skipped > 0) {
        bytesSkippedOnSeek += skipped;
      }
    }

    /**
     * The inner stream was opened.
     * @return the previous count
     */
    @Override
    public long streamOpened() {
      long count = openOperations;
      openOperations++;
      return count;
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
      closeOperations++;
      if (abortedConnection) {
        this.aborted++;
        bytesDiscardedInAbort += remainingInCurrentRequest;
      } else {
        closed++;
        bytesReadInClose += remainingInCurrentRequest;
      }
    }

    /**
     * An ignored stream read exception was received.
     */
    @Override
    public void readException() {
      readExceptions++;
    }

    /**
     * Increment the bytes read counter by the number of bytes;
     * no-op if the argument is negative.
     * @param bytes number of bytes read
     */
    @Override
    public void bytesRead(long bytes) {
      if (bytes > 0) {
        bytesRead += bytes;
      }
    }

    /**
     * A {@code read(byte[] buf, int off, int len)} operation has started.
     * @param pos starting position of the read
     * @param len length of bytes to read
     */
    @Override
    public void readOperationStarted(long pos, long len) {
      readOperations++;
    }

    /**
     * A {@code PositionedRead.read(position, buffer, offset, length)}
     * operation has just started.
     * @param pos starting position of the read
     * @param len length of bytes to read
     */
    @Override
    public void readFullyOperationStarted(long pos, long len) {
      readFullyOperations++;
    }

    /**
     * A read operation has completed.
     * @param requested number of requested bytes
     * @param actual the actual number of bytes
     */
    @Override
    public void readOperationCompleted(int requested, int actual) {
      if (requested > actual) {
        readsIncomplete++;
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
      policySetCount++;
      inputPolicy = updatedPolicy;
    }

    /**
     * The change tracker increments {@code versionMismatches} on any
     * mismatch.
     * @return change tracking.
     */
    @Override
    public ChangeTrackerStatistics getChangeTrackerStatistics() {
      return new CountingChangeTracker(versionMismatches);
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
      sb.append("OpenOperations=").append(openOperations);
      sb.append(", CloseOperations=").append(closeOperations);
      sb.append(", Closed=").append(closed);
      sb.append(", Aborted=").append(aborted);
      sb.append(", SeekOperations=").append(seekOperations);
      sb.append(", ReadExceptions=").append(readExceptions);
      sb.append(", ForwardSeekOperations=")
          .append(forwardSeekOperations);
      sb.append(", BackwardSeekOperations=")
          .append(backwardSeekOperations);
      sb.append(", BytesSkippedOnSeek=").append(bytesSkippedOnSeek);
      sb.append(", BytesBackwardsOnSeek=").append(bytesBackwardsOnSeek);
      sb.append(", BytesRead=").append(bytesRead);
      sb.append(", BytesRead excluding skipped=")
          .append(bytesRead - bytesSkippedOnSeek);
      sb.append(", ReadOperations=").append(readOperations);
      sb.append(", ReadFullyOperations=").append(readFullyOperations);
      sb.append(", ReadsIncomplete=").append(readsIncomplete);
      sb.append(", BytesReadInClose=").append(bytesReadInClose);
      sb.append(", BytesDiscardedInAbort=").append(bytesDiscardedInAbort);
      sb.append(", InputPolicy=").append(inputPolicy);
      sb.append(", InputPolicySetCount=").append(policySetCount);
      sb.append(", versionMismatches=").append(versionMismatches.get());
      sb.append('}');
      return sb.toString();
    }

    /**
     * Merge the statistics into the filesystem's instrumentation instance.
     * Takes a diff between the current version of the stats and the
     * version of the stats when merge was last called, and merges the diff
     * into the instrumentation instance. Used to periodically merge the
     * stats into the fs-wide stats. <b>Behavior is undefined if called on a
     * closed instance.</b>
     */
    @Override
    public void merge(boolean isClosed) {
      if (mergedStats != null) {
        mergeInputStreamStatistics(diff(mergedStats));
      } else {
        mergeInputStreamStatistics(this);
      }
      // If stats are closed, no need to create another copy
      if (!isClosed) {
        mergedStats = copy();
      } else {
        if (filesystemStatistics != null) {
          // increment the read counter
          filesystemStatistics.incrementBytesReadByDistance(DISTANCE,
              bytesRead + bytesReadInClose);
        }
      }
    }

    /**
     * Returns a diff between this {@link InputStreamStatisticsImpl} instance and
     * the given {@link InputStreamStatisticsImpl} instance.
     */
    private InputStreamStatisticsImpl diff(InputStreamStatisticsImpl inputStats) {
      InputStreamStatisticsImpl diff = new InputStreamStatisticsImpl(filesystemStatistics);
      diff.openOperations = openOperations - inputStats.openOperations;
      diff.closeOperations = closeOperations - inputStats.closeOperations;
      diff.closed = closed - inputStats.closed;
      diff.aborted = aborted - inputStats.aborted;
      diff.seekOperations = seekOperations - inputStats.seekOperations;
      diff.readExceptions = readExceptions - inputStats.readExceptions;
      diff.forwardSeekOperations =
              forwardSeekOperations - inputStats.forwardSeekOperations;
      diff.backwardSeekOperations =
              backwardSeekOperations - inputStats.backwardSeekOperations;
      diff.bytesRead = bytesRead - inputStats.bytesRead;
      diff.bytesSkippedOnSeek =
              bytesSkippedOnSeek - inputStats.bytesSkippedOnSeek;
      diff.bytesBackwardsOnSeek =
              bytesBackwardsOnSeek - inputStats.bytesBackwardsOnSeek;
      diff.readOperations = readOperations - inputStats.readOperations;
      diff.readFullyOperations =
              readFullyOperations - inputStats.readFullyOperations;
      diff.readsIncomplete = readsIncomplete - inputStats.readsIncomplete;
      diff.bytesReadInClose = bytesReadInClose - inputStats.bytesReadInClose;
      diff.bytesDiscardedInAbort =
              bytesDiscardedInAbort - inputStats.bytesDiscardedInAbort;
      diff.policySetCount = policySetCount - inputStats.policySetCount;
      diff.inputPolicy = inputPolicy - inputStats.inputPolicy;
      diff.versionMismatches.set(versionMismatches.longValue() -
              inputStats.versionMismatches.longValue());
      return diff;
    }

    /**
     * Returns a new {@link InputStreamStatisticsImpl} instance with all the same
     * values as this {@link InputStreamStatisticsImpl}.
     */
    private InputStreamStatisticsImpl copy() {
      InputStreamStatisticsImpl copy = new InputStreamStatisticsImpl(filesystemStatistics);
      copy.openOperations = openOperations;
      copy.closeOperations = closeOperations;
      copy.closed = closed;
      copy.aborted = aborted;
      copy.seekOperations = seekOperations;
      copy.readExceptions = readExceptions;
      copy.forwardSeekOperations = forwardSeekOperations;
      copy.backwardSeekOperations = backwardSeekOperations;
      copy.bytesRead = bytesRead;
      copy.bytesSkippedOnSeek = bytesSkippedOnSeek;
      copy.bytesBackwardsOnSeek = bytesBackwardsOnSeek;
      copy.readOperations = readOperations;
      copy.readFullyOperations = readFullyOperations;
      copy.readsIncomplete = readsIncomplete;
      copy.bytesReadInClose = bytesReadInClose;
      copy.bytesDiscardedInAbort = bytesDiscardedInAbort;
      copy.policySetCount = policySetCount;
      copy.inputPolicy = inputPolicy;
      return copy;
    }

    /**
     * Convert to an IOStatistics source which is
     * dynamically updated.
     * @return statistics
     */
    @Override
    public IOStatistics createIOStatistics() {
      DynamicIOStatisticsBuilder builder
          = createDynamicIOStatistics();

      builder.add(StreamStatisticNames.STREAM_CLOSED,
          k -> closed);
      builder.add(StreamStatisticNames.STREAM_CLOSE_OPERATIONS,
          k -> closeOperations);
      builder.add(StreamStatisticNames.STREAM_OPENED,
          k -> openOperations);
      builder.add(StreamStatisticNames.STREAM_READ_EXCEPTIONS,
          k -> readExceptions);
      builder.add(StreamStatisticNames.STREAM_READ_FULLY_OPERATIONS,
          k -> readFullyOperations);
      builder.add(StreamStatisticNames.STREAM_READ_OPERATIONS,
          k -> readOperations);
      builder.add(StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE,
          k -> readsIncomplete);
      builder.add(StreamStatisticNames.STREAM_OPENED,
          k -> openOperations);
      builder.add(StreamStatisticNames.STREAM_ABORT_BYTES_DISCARDED,
          k -> bytesDiscardedInAbort);
      return builder.build();
    }

    @Override
    public long getCloseOperations() {
      return closeOperations;
    }

    @Override
    public long getClosed() {
      return closed;
    }

    @Override
    public long getAborted() {
      return aborted;
    }

    @Override
    public long getForwardSeekOperations() {
      return forwardSeekOperations;
    }

    @Override
    public long getBackwardSeekOperations() {
      return backwardSeekOperations;
    }

    @Override
    public long getBytesRead() {
      return bytesRead;
    }

    @Override
    public long getBytesSkippedOnSeek() {
      return bytesSkippedOnSeek;
    }

    @Override
    public long getBytesBackwardsOnSeek() {
      return bytesBackwardsOnSeek;
    }

    @Override
    public long getBytesReadInClose() {
      return bytesReadInClose;
    }

    @Override
    public long getBytesDiscardedInAbort() {
      return bytesDiscardedInAbort;
    }

    @Override
    public long getOpenOperations() {
      return openOperations;
    }

    @Override
    public long getSeekOperations() {
      return seekOperations;
    }

    @Override
    public long getReadExceptions() {
      return readExceptions;
    }

    @Override
    public long getReadOperations() {
      return readOperations;
    }

    @Override
    public long getReadFullyOperations() {
      return readFullyOperations;
    }

    @Override
    public long getReadsIncomplete() {
      return readsIncomplete;
    }

    @Override
    public long getPolicySetCount() {
      return policySetCount;
    }

    @Override
    public long getVersionMismatches() {
      return versionMismatches.get();
    }

    @Override
    public long getInputPolicy() {
      return inputPolicy;
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
  private void mergeOutputStreamStatistics(BlockOutputStreamStatisticsImpl source) {
    incrementCounter(STREAM_WRITE_TOTAL_TIME, source.totalUploadDuration());
    incrementCounter(STREAM_WRITE_QUEUE_DURATION, source.queueDuration);
    incrementCounter(STREAM_WRITE_TOTAL_DATA, source.bytesUploaded);
    incrementCounter(STREAM_WRITE_BLOCK_UPLOADS,
        source.blockUploadsCompleted);
    incrementCounter(STREAM_WRITE_FAILURES, source.blockUploadsFailed);
  }

  /**
   * Statistics updated by an output stream during its actual operation.
   * Some of these stats are propagated to any passed in
   * {@link FileSystem.Statistics} instance; this is only done
   * in close() for better cross-thread accounting.
   */
  private final class BlockOutputStreamStatisticsImpl implements
      BlockOutputStreamStatistics {
    private final AtomicLong blocksSubmitted = new AtomicLong(0);
    private final AtomicLong blocksInQueue = new AtomicLong(0);
    private final AtomicLong blocksActive = new AtomicLong(0);
    private final AtomicLong blockUploadsCompleted = new AtomicLong(0);
    private final AtomicLong blockUploadsFailed = new AtomicLong(0);
    private final AtomicLong bytesPendingUpload = new AtomicLong(0);

    private final AtomicLong bytesUploaded = new AtomicLong(0);
    private final AtomicLong transferDuration = new AtomicLong(0);
    private final AtomicLong queueDuration = new AtomicLong(0);
    private final AtomicLong exceptionsInMultipartFinalize = new AtomicLong(0);
    private final AtomicInteger blocksAllocated = new AtomicInteger(0);
    private final AtomicInteger blocksReleased = new AtomicInteger(0);

    private final FileSystem.Statistics filesystemStatistics;

    private BlockOutputStreamStatisticsImpl(
        @Nullable FileSystem.Statistics filesystemStatistics) {
      this.filesystemStatistics = filesystemStatistics;
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
      blocksSubmitted.incrementAndGet();
      blocksInQueue.incrementAndGet();
      bytesPendingUpload.addAndGet(blockSize);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_PENDING, 1);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING, blockSize);
    }

    /** Queued block has been scheduled for upload. */
    @Override
    public void blockUploadStarted(long duration, int blockSize) {
      queueDuration.addAndGet(duration);
      blocksInQueue.decrementAndGet();
      blocksActive.incrementAndGet();
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_PENDING, -1);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_ACTIVE, 1);
    }

    /** A block upload has completed. */
    @Override
    public void blockUploadCompleted(long duration, int blockSize) {
      transferDuration.addAndGet(duration);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_ACTIVE, -1);
      blocksActive.decrementAndGet();
      blockUploadsCompleted.incrementAndGet();
    }

    /**
     *  A block upload has failed.
     *  A final transfer completed event is still expected, so this
     *  does not decrement the active block counter.
     */
    @Override
    public void blockUploadFailed(long duration, int blockSize) {
      blockUploadsFailed.incrementAndGet();
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
        exceptionsInMultipartFinalize.addAndGet(count);
      }
    }

    /**
     * Note an exception in a multipart abort.
     */
    @Override
    public void exceptionInMultipartAbort() {
      exceptionsInMultipartFinalize.incrementAndGet();
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
      return blocksSubmitted.get() > 0 ?
          (queueDuration.get() / blocksSubmitted.get()) : 0;
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
     * Get counters of blocks actively allocated; my be inaccurate
     * if the numbers change during the (non-synchronized) calculation.
     * @return the number of actively allocated blocks.
     */
    @Override
    public int getBlocksActivelyAllocated() {
      return blocksAllocated.get() - blocksReleased.get();
    }


    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "OutputStreamStatistics{");
      sb.append("blocksSubmitted=").append(blocksSubmitted);
      sb.append(", blocksInQueue=").append(blocksInQueue);
      sb.append(", blocksActive=").append(blocksActive);
      sb.append(", blockUploadsCompleted=").append(blockUploadsCompleted);
      sb.append(", blockUploadsFailed=").append(blockUploadsFailed);
      sb.append(", bytesPendingUpload=").append(bytesPendingUpload);
      sb.append(", bytesUploaded=").append(bytesUploaded);
      sb.append(", blocksAllocated=").append(blocksAllocated);
      sb.append(", blocksReleased=").append(blocksReleased);
      sb.append(", blocksActivelyAllocated=").append(getBlocksActivelyAllocated());
      sb.append(", exceptionsInMultipartFinalize=").append(
          exceptionsInMultipartFinalize);
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

    /**
     * Convert to an IOStatistics source which is
     * dynamically updated.
     * @return statistics
     */
    @Override
    public IOStatistics createIOStatistics() {
      DynamicIOStatisticsBuilder builder = createDynamicIOStatistics();

      builder.add(StreamStatisticNames.STREAM_WRITE_BLOCK_UPLOADS,
          blocksSubmitted);
      builder.add(StreamStatisticNames.STREAM_WRITE_TOTAL_DATA,
          bytesUploaded);
      builder.add(StreamStatisticNames.STREAM_WRITE_FAILURES,
          blockUploadsFailed);
      return builder.build();
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
  private final class CommitterStatisticsImpl implements CommitterStatistics {

    /** A commit has been created. */
    @Override
    public void commitCreated() {
      incrementCounter(COMMITTER_COMMITS_CREATED, 1);
    }

    /**
     * Data has been uploaded to be committed in a subsequent operation.
     * @param size size in bytes
     */
    @Override
    public void commitUploaded(long size) {
      incrementCounter(COMMITTER_BYTES_UPLOADED, size);
    }

    /**
     * A commit has been completed.
     * @param size size in bytes
     */
    @Override
    public void commitCompleted(long size) {
      incrementCounter(COMMITTER_COMMITS_COMPLETED, 1);
      incrementCounter(COMMITTER_BYTES_COMMITTED, size);
    }

    /** A commit has been aborted. */
    @Override
    public void commitAborted() {
      incrementCounter(COMMITTER_COMMITS_ABORTED, 1);
    }

    @Override
    public void commitReverted() {
      incrementCounter(COMMITTER_COMMITS_REVERTED, 1);
    }

    @Override
    public void commitFailed() {
      incrementCounter(COMMITTER_COMMITS_FAILED, 1);
    }

    @Override
    public void taskCompleted(boolean success) {
      incrementCounter(
          success ? COMMITTER_TASKS_SUCCEEDED
              : COMMITTER_TASKS_FAILED,
          1);
    }

    @Override
    public void jobCompleted(boolean success) {
      incrementCounter(
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
