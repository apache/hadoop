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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricStringBuilder;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Instrumentation of S3a.
 * Derived from the {@code AzureFileSystemInstrumentation}
 */
@Metrics(about = "Metrics for S3a", context = "S3AFileSystem")
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AInstrumentation {
  public static final String CONTEXT = "S3AFileSystem";

  public static final String STREAM_OPENED = "streamOpened";
  public static final String STREAM_CLOSE_OPERATIONS = "streamCloseOperations";
  public static final String STREAM_CLOSED = "streamClosed";
  public static final String STREAM_ABORTED = "streamAborted";
  public static final String STREAM_READ_EXCEPTIONS = "streamReadExceptions";
  public static final String STREAM_SEEK_OPERATIONS = "streamSeekOperations";
  public static final String STREAM_FORWARD_SEEK_OPERATIONS
      = "streamForwardSeekOperations";
  public static final String STREAM_BACKWARD_SEEK_OPERATIONS
      = "streamBackwardSeekOperations";
  public static final String STREAM_SEEK_BYTES_SKIPPED =
      "streamBytesSkippedOnSeek";
  public static final String STREAM_SEEK_BYTES_BACKWARDS =
      "streamBytesBackwardsOnSeek";
  public static final String STREAM_SEEK_BYTES_READ = "streamBytesRead";
  public static final String STREAM_READ_OPERATIONS = "streamReadOperations";
  public static final String STREAM_READ_FULLY_OPERATIONS
      = "streamReadFullyOperations";
  public static final String STREAM_READ_OPERATIONS_INCOMPLETE
      = "streamReadOperationsIncomplete";
  public static final String FILES_CREATED = "files_created";
  public static final String FILES_COPIED = "files_copied";
  public static final String FILES_COPIED_BYTES = "files_copied_bytes";
  public static final String FILES_DELETED = "files_deleted";
  public static final String DIRECTORIES_CREATED = "directories_created";
  public static final String DIRECTORIES_DELETED = "directories_deleted";
  public static final String IGNORED_ERRORS = "ignored_errors";
  private final MetricsRegistry registry =
      new MetricsRegistry("S3AFileSystem").setContext(CONTEXT);
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
  private final MutableCounterLong ignoredErrors;

  private final MutableCounterLong numberOfFilesCreated;
  private final MutableCounterLong numberOfFilesCopied;
  private final MutableCounterLong bytesOfFilesCopied;
  private final MutableCounterLong numberOfFilesDeleted;
  private final MutableCounterLong numberOfDirectoriesCreated;
  private final MutableCounterLong numberOfDirectoriesDeleted;
  private final Map<String, MutableCounterLong> streamMetrics = new HashMap<>();

  public S3AInstrumentation(URI name) {
    UUID fileSystemInstanceId = UUID.randomUUID();
    registry.tag("FileSystemId",
        "A unique identifier for the FS ",
        fileSystemInstanceId.toString() + "-" + name.getHost());
    registry.tag("fsURI",
        "URI of this filesystem",
        name.toString());
    streamOpenOperations = streamCounter(STREAM_OPENED,
        "Total count of times an input stream to object store was opened");
    streamCloseOperations = streamCounter(STREAM_CLOSE_OPERATIONS,
        "Total count of times an attempt to close a data stream was made");
    streamClosed = streamCounter(STREAM_CLOSED,
        "Count of times the TCP stream was closed");
    streamAborted = streamCounter(STREAM_ABORTED,
        "Count of times the TCP stream was aborted");
    streamSeekOperations = streamCounter(STREAM_SEEK_OPERATIONS,
        "Number of seek operations invoked on input streams");
    streamReadExceptions = streamCounter(STREAM_READ_EXCEPTIONS,
        "Number of read exceptions caught and attempted to recovered from");
    streamForwardSeekOperations = streamCounter(STREAM_FORWARD_SEEK_OPERATIONS,
        "Number of executed seek operations which went forward in a stream");
    streamBackwardSeekOperations = streamCounter(
        STREAM_BACKWARD_SEEK_OPERATIONS,
        "Number of executed seek operations which went backwards in a stream");
    streamBytesSkippedOnSeek = streamCounter(STREAM_SEEK_BYTES_SKIPPED,
        "Count of bytes skipped during forward seek operations");
    streamBytesBackwardsOnSeek = streamCounter(STREAM_SEEK_BYTES_BACKWARDS,
        "Count of bytes moved backwards during seek operations");
    streamBytesRead = streamCounter(STREAM_SEEK_BYTES_READ,
        "Count of bytes read during seek() in stream operations");
    streamReadOperations = streamCounter(STREAM_READ_OPERATIONS,
        "Count of read() operations in streams");
    streamReadFullyOperations = streamCounter(STREAM_READ_FULLY_OPERATIONS,
        "Count of readFully() operations in streams");
    streamReadsIncomplete = streamCounter(STREAM_READ_OPERATIONS_INCOMPLETE,
        "Count of incomplete read() operations in streams");

    numberOfFilesCreated = counter(FILES_CREATED,
            "Total number of files created through the object store.");
    numberOfFilesCopied = counter(FILES_COPIED,
            "Total number of files copied within the object store.");
    bytesOfFilesCopied = counter(FILES_COPIED_BYTES,
            "Total number of bytes copied within the object store.");
    numberOfFilesDeleted = counter(FILES_DELETED,
            "Total number of files deleted through from the object store.");
    numberOfDirectoriesCreated = counter(DIRECTORIES_CREATED,
        "Total number of directories created through the object store.");
    numberOfDirectoriesDeleted = counter(DIRECTORIES_DELETED,
        "Total number of directories deleted through the object store.");
    ignoredErrors = counter(IGNORED_ERRORS,
        "Total number of errors caught and ingored.");
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
   * Create a counter in the stream map: these are unregistered in the public
   * metrics.
   * @param name counter name
   * @param desc counter description
   * @return a new counter
   */
  protected final MutableCounterLong streamCounter(String name, String desc) {
    MutableCounterLong counter = new MutableCounterLong(
        Interns.info(name, desc), 0L);
    streamMetrics.put(name, counter);
    return counter;
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
    for (Map.Entry<String, MutableCounterLong> entry:
        streamMetrics.entrySet()) {
      metricBuilder.tuple(entry.getKey(),
          Long.toString(entry.getValue().value()));
    }
    return metricBuilder.toString();
  }

  /**
   * Indicate that S3A created a file.
   */
  public void fileCreated() {
    numberOfFilesCreated.incr();
  }

  /**
   * Indicate that S3A deleted one or more file.s
   * @param count number of files.
   */
  public void fileDeleted(int count) {
    numberOfFilesDeleted.incr(count);
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
   * Create a stream input statistics instance.
   * @return the new instance
   */
  InputStreamStatistics newInputStreamStatistics() {
    return new InputStreamStatistics();
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
     */
    public void streamOpened() {
      openOperations++;
    }

    /**
     * The inner stream was closed.
     * @param abortedConnection flag to indicate the stream was aborted,
     * rather than closed cleanly
     */
    public void streamClose(boolean abortedConnection) {
      closeOperations++;
      if (abortedConnection) {
        this.aborted++;
      } else {
        closed++;
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
      sb.append('}');
      return sb.toString();
    }
  }
}
