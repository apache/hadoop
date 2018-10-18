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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem.Statistics;
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
   * {@value #METRICS_SYSTEM_NAME} The name of the s3a-specific metrics
   * system instance used for s3a metrics.
   */
  public static final String METRICS_SYSTEM_NAME = "s3a-file-system";

  /**
   * {@value #CONTEXT} Currently all s3a metrics are placed in a single
   * "context". Distinct contexts may be used in the future.
   */
  public static final String CONTEXT = "s3aFileSystem";

  /**
   * {@value #METRIC_TAG_FILESYSTEM_ID} The name of a field added to metrics
   * records that uniquely identifies a specific FileSystem instance.
   */
  public static final String METRIC_TAG_FILESYSTEM_ID = "s3aFileSystemId";

  /**
   * {@value #METRIC_TAG_BUCKET} The name of a field added to metrics records
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
      S3GUARD_METADATASTORE_RETRY,
      S3GUARD_METADATASTORE_THROTTLED,
      STORE_IO_THROTTLED
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
    quantiles(S3GUARD_METADATASTORE_PUT_PATH_LATENCY,
        "ops", "latency", interval);
    quantiles(S3GUARD_METADATASTORE_THROTTLE_RATE,
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
   */
  InputStreamStatistics newInputStreamStatistics() {
    return new InputStreamStatistics();
  }

  /**
   * Create a S3Guard instrumentation instance.
   * There's likely to be at most one instance of this per FS instance.
   * @return the S3Guard instrumentation point.
   */
  public S3GuardInstrumentation getS3GuardInstrumentation() {
    return s3GuardInstrumentation;
  }

  /**
   * Create a new instance of the committer statistics.
   * @return a new committer statistics instance
   */
  CommitterStatistics newCommitterStatistics() {
    return new CommitterStatistics();
  }

  /**
   * Merge in the statistics of a single input stream into
   * the filesystem-wide statistics.
   * @param statistics stream statistics
   */
  private void mergeInputStreamStatistics(InputStreamStatistics statistics) {
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
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(registry.info().name()), true);
  }

  public void close() {
    synchronized (metricsSystemLock) {
      metricsSystem.unregisterSource(metricsSourceName);
      int activeSources = --metricsSourceActiveCounter;
      if (activeSources == 0) {
        metricsSystem.publishMetricsNow();
        metricsSystem.shutdown();
        metricsSystem = null;
      }
    }
  }

  /**
   * Statistics updated by an input stream during its actual operation.
   * These counters not thread-safe and are for use in a single instance
   * of a stream.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public final class InputStreamStatistics implements AutoCloseable {
    public long openOperations;
    public long closeOperations;
    public long closed;
    public long aborted;
    public long seekOperations;
    public long readExceptions;
    public long forwardSeekOperations;
    public long backwardSeekOperations;
    public long bytesRead;
    public long bytesSkippedOnSeek;
    public long bytesBackwardsOnSeek;
    public long readOperations;
    public long readFullyOperations;
    public long readsIncomplete;
    public long bytesReadInClose;
    public long bytesDiscardedInAbort;
    public long policySetCount;
    public long inputPolicy;

    private InputStreamStatistics() {
    }

    /**
     * Seek backwards, incrementing the seek and backward seek counters.
     * @param negativeOffset how far was the seek?
     * This is expected to be negative.
     */
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
    public void readException() {
      readExceptions++;
    }

    /**
     * Increment the bytes read counter by the number of bytes;
     * no-op if the argument is negative.
     * @param bytes number of bytes read
     */
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
    public void readOperationStarted(long pos, long len) {
      readOperations++;
    }

    /**
     * A {@code PositionedRead.read(position, buffer, offset, length)}
     * operation has just started.
     * @param pos starting position of the read
     * @param len length of bytes to read
     */
    public void readFullyOperationStarted(long pos, long len) {
      readFullyOperations++;
    }

    /**
     * A read operation has completed.
     * @param requested number of requested bytes
     * @param actual the actual number of bytes
     */
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
      mergeInputStreamStatistics(this);
    }

    /**
     * The input policy has been switched.
     * @param updatedPolicy enum value of new policy.
     */
    public void inputPolicySet(int updatedPolicy) {
      policySetCount++;
      inputPolicy = updatedPolicy;
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
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Create a stream output statistics instance.
   * @return the new instance
   */
  OutputStreamStatistics newOutputStreamStatistics(Statistics statistics) {
    return new OutputStreamStatistics(statistics);
  }

  /**
   * Merge in the statistics of a single output stream into
   * the filesystem-wide statistics.
   * @param statistics stream statistics
   */
  private void mergeOutputStreamStatistics(OutputStreamStatistics statistics) {
    incrementCounter(STREAM_WRITE_TOTAL_TIME, statistics.totalUploadDuration());
    incrementCounter(STREAM_WRITE_QUEUE_DURATION, statistics.queueDuration);
    incrementCounter(STREAM_WRITE_TOTAL_DATA, statistics.bytesUploaded);
    incrementCounter(STREAM_WRITE_BLOCK_UPLOADS,
        statistics.blockUploadsCompleted);
  }

  /**
   * Statistics updated by an output stream during its actual operation.
   * Some of these stats may be relayed. However, as block upload is
   * spans multiple
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public final class OutputStreamStatistics implements Closeable {
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

    private Statistics statistics;

    public OutputStreamStatistics(Statistics statistics){
      this.statistics = statistics;
    }

    /**
     * A block has been allocated.
     */
    void blockAllocated() {
      blocksAllocated.incrementAndGet();
    }

    /**
     * A block has been released.
     */
    void blockReleased() {
      blocksReleased.incrementAndGet();
    }

    /**
     * Block is queued for upload.
     */
    void blockUploadQueued(int blockSize) {
      blocksSubmitted.incrementAndGet();
      blocksInQueue.incrementAndGet();
      bytesPendingUpload.addAndGet(blockSize);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_PENDING, 1);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING, blockSize);
    }

    /** Queued block has been scheduled for upload. */
    void blockUploadStarted(long duration, int blockSize) {
      queueDuration.addAndGet(duration);
      blocksInQueue.decrementAndGet();
      blocksActive.incrementAndGet();
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_PENDING, -1);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_ACTIVE, 1);
    }

    /** A block upload has completed. */
    void blockUploadCompleted(long duration, int blockSize) {
      this.transferDuration.addAndGet(duration);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_ACTIVE, -1);
      blocksActive.decrementAndGet();
      blockUploadsCompleted.incrementAndGet();
    }

    /**
     *  A block upload has failed.
     *  A final transfer completed event is still expected, so this
     *  does not decrement the active block counter.
     */
    void blockUploadFailed(long duration, int blockSize) {
      blockUploadsFailed.incrementAndGet();
    }

    /** Intermediate report of bytes uploaded. */
    void bytesTransferred(long byteCount) {
      bytesUploaded.addAndGet(byteCount);
      statistics.incrementBytesWritten(byteCount);
      bytesPendingUpload.addAndGet(-byteCount);
      incrementGauge(STREAM_WRITE_BLOCK_UPLOADS_DATA_PENDING, -byteCount);
    }

    /**
     * Note exception in a multipart complete.
     * @param count count of exceptions
     */
    void exceptionInMultipartComplete(int count) {
      if (count > 0) {
        exceptionsInMultipartFinalize.addAndGet(count);
      }
    }

    /**
     * Note an exception in a multipart abort.
     */
    void exceptionInMultipartAbort() {
      exceptionsInMultipartFinalize.incrementAndGet();
    }

    /**
     * Get the number of bytes pending upload.
     * @return the number of bytes in the pending upload state.
     */
    public long getBytesPendingUpload() {
      return bytesPendingUpload.get();
    }

    /**
     * Data has been uploaded to be committed in a subsequent operation;
     * to be called at the end of the write.
     * @param size size in bytes
     */
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

    public int blocksAllocated() {
      return blocksAllocated.get();
    }

    public int blocksReleased() {
      return blocksReleased.get();
    }

    /**
     * Get counters of blocks actively allocated; my be inaccurate
     * if the numbers change during the (non-synchronized) calculation.
     * @return the number of actively allocated blocks.
     */
    public int blocksActivelyAllocated() {
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
      sb.append(", blocksActivelyAllocated=").append(blocksActivelyAllocated());
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
  }

  /**
   * Instrumentation exported to S3Guard.
   */
  public final class S3GuardInstrumentation {

    /** Initialized event. */
    public void initialized() {
      incrementCounter(S3GUARD_METADATASTORE_INITIALIZATION, 1);
    }

    public void storeClosed() {

    }

    /**
     * Throttled request.
     */
    public void throttled() {
      // counters are incremented by owner.
    }

    /**
     * S3Guard is retrying after a (retryable) failure.
     */
    public void retrying() {
      // counters are incremented by owner.
    }
  }

  /**
   * Instrumentation exported to S3Guard Committers.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public final class CommitterStatistics {

    /** A commit has been created. */
    public void commitCreated() {
      incrementCounter(COMMITTER_COMMITS_CREATED, 1);
    }

    /**
     * Data has been uploaded to be committed in a subsequent operation.
     * @param size size in bytes
     */
    public void commitUploaded(long size) {
      incrementCounter(COMMITTER_BYTES_UPLOADED, size);
    }

    /**
     * A commit has been completed.
     * @param size size in bytes
     */
    public void commitCompleted(long size) {
      incrementCounter(COMMITTER_COMMITS_COMPLETED, 1);
      incrementCounter(COMMITTER_BYTES_COMMITTED, size);
    }

    /** A commit has been aborted. */
    public void commitAborted() {
      incrementCounter(COMMITTER_COMMITS_ABORTED, 1);
    }

    public void commitReverted() {
      incrementCounter(COMMITTER_COMMITS_REVERTED, 1);
    }

    public void commitFailed() {
      incrementCounter(COMMITTER_COMMITS_FAILED, 1);
    }

    public void taskCompleted(boolean success) {
      incrementCounter(
          success ? COMMITTER_TASKS_SUCCEEDED
              : COMMITTER_TASKS_FAILED,
          1);
    }

    public void jobCompleted(boolean success) {
      incrementCounter(
          success ? COMMITTER_JOBS_SUCCEEDED
              : COMMITTER_JOBS_FAILED,
          1);
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
