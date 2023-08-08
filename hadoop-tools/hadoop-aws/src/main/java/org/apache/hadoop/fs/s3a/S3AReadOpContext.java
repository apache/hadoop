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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.prefetch.ExecutorServiceFuturePool;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.store.audit.AuditSpan;

import javax.annotation.Nullable;

import org.apache.hadoop.util.Preconditions;

import static java.util.Objects.requireNonNull;

/**
 * Read-specific operation context struct.
 */
public class S3AReadOpContext extends S3AOpContext {

  /**
   * Path of read.
   */
  private final Path path;

  /**
   * Initial input policy of the stream.
   */
  private S3AInputPolicy inputPolicy;

  /**
   * How to detect and deal with the object being updated during read.
   */
  private ChangeDetectionPolicy changeDetectionPolicy;

  /**
   * Readahead for GET operations/skip, etc.
   */
  private long readahead;

  private AuditSpan auditSpan;

  /**
   * Threshold for stream reads to switch to
   * asynchronous draining.
   */
  private long asyncDrainThreshold;

  /**
   * Vectored IO context for vectored read api
   * in {@code S3AInputStream#readVectored(List, IntFunction)}.
   */
  private final VectoredIOContext vectoredIOContext;

  /** Thread-level IOStatistics aggregator. **/
  private final IOStatisticsAggregator ioStatisticsAggregator;

  // S3 reads are prefetched asynchronously using this future pool.
  private ExecutorServiceFuturePool futurePool;

  // Size in bytes of a single prefetch block.
  private final int prefetchBlockSize;

  // Size of prefetch queue (in number of blocks).
  private final int prefetchBlockCount;

  /**
   * Instantiate.
   * @param path path of read
   * @param invoker invoker for normal retries.
   * @param stats Filesystem statistics (may be null)
   * @param instrumentation statistics context
   * @param dstFileStatus target file status
   * @param vectoredIOContext context for vectored read operation.
   * @param ioStatisticsAggregator IOStatistics aggregator for each thread.
   * @param futurePool the ExecutorServiceFuturePool instance used by async prefetches.
   * @param prefetchBlockSize the size (in number of bytes) of each prefetched block.
   * @param prefetchBlockCount maximum number of prefetched blocks.
   */
  public S3AReadOpContext(
      final Path path,
      Invoker invoker,
      @Nullable FileSystem.Statistics stats,
      S3AStatisticsContext instrumentation,
      FileStatus dstFileStatus,
      VectoredIOContext vectoredIOContext,
      IOStatisticsAggregator ioStatisticsAggregator,
      ExecutorServiceFuturePool futurePool,
      int prefetchBlockSize,
      int prefetchBlockCount) {

    super(invoker, stats, instrumentation,
        dstFileStatus);
    this.path = requireNonNull(path);
    this.vectoredIOContext = requireNonNull(vectoredIOContext, "vectoredIOContext");
    this.ioStatisticsAggregator = ioStatisticsAggregator;
    this.futurePool = futurePool;
    Preconditions.checkArgument(
        prefetchBlockSize > 0, "invalid prefetchBlockSize %d", prefetchBlockSize);
    this.prefetchBlockSize = prefetchBlockSize;
    Preconditions.checkArgument(
        prefetchBlockCount > 0, "invalid prefetchBlockCount %d", prefetchBlockCount);
    this.prefetchBlockCount = prefetchBlockCount;
  }

  /**
   * validate the context.
   * @return a read operation context ready for use.
   */
  public S3AReadOpContext build() {
    requireNonNull(inputPolicy, "inputPolicy");
    requireNonNull(changeDetectionPolicy, "changeDetectionPolicy");
    requireNonNull(auditSpan, "auditSpan");
    requireNonNull(inputPolicy, "inputPolicy");
    Preconditions.checkArgument(readahead >= 0,
        "invalid readahead %d", readahead);
    Preconditions.checkArgument(asyncDrainThreshold >= 0,
        "invalid drainThreshold %d", asyncDrainThreshold);
    requireNonNull(ioStatisticsAggregator, "ioStatisticsAggregator");
    return this;
  }

  /**
   * Get invoker to use for read operations.
   * @return invoker to use for read codepaths
   */
  public Invoker getReadInvoker() {
    return invoker;
  }

  /**
   * Get the path of this read.
   * @return path.
   */
  public Path getPath() {
    return path;
  }

  /**
   * Get the IO policy.
   * @return the initial input policy.
   */
  public S3AInputPolicy getInputPolicy() {
    return inputPolicy;
  }

  public ChangeDetectionPolicy getChangeDetectionPolicy() {
    return changeDetectionPolicy;
  }

  /**
   * Get the readahead for this operation.
   * @return a value {@literal >=} 0
   */
  public long getReadahead() {
    return readahead;
  }

  /**
   * Get the audit which was active when the file was opened.
   * @return active span
   */
  public AuditSpan getAuditSpan() {
    return auditSpan;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public S3AReadOpContext withInputPolicy(final S3AInputPolicy value) {
    inputPolicy = value;
    return this;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public S3AReadOpContext withChangeDetectionPolicy(
      final ChangeDetectionPolicy value) {
    changeDetectionPolicy = value;
    return this;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public S3AReadOpContext withReadahead(final long value) {
    readahead = value;
    return this;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public S3AReadOpContext withAuditSpan(final AuditSpan value) {
    auditSpan = value;
    return this;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return the builder
   */
  public S3AReadOpContext withAsyncDrainThreshold(final long value) {
    asyncDrainThreshold = value;
    return this;
  }

  public long getAsyncDrainThreshold() {
    return asyncDrainThreshold;
  }

  /**
   * Get Vectored IO context for this this read op.
   * @return vectored IO context.
   */
  public VectoredIOContext getVectoredIOContext() {
    return vectoredIOContext;
  }

  /**
   * Return the IOStatistics aggregator.
   *
   * @return instance of IOStatisticsAggregator.
   */
  public IOStatisticsAggregator getIOStatisticsAggregator() {
    return ioStatisticsAggregator;
  }

  /**
   * Gets the {@code ExecutorServiceFuturePool} used for asynchronous prefetches.
   *
   * @return the {@code ExecutorServiceFuturePool} used for asynchronous prefetches.
   */
  public ExecutorServiceFuturePool getFuturePool() {
    return this.futurePool;
  }

  /**
   * Gets the size in bytes of a single prefetch block.
   *
   * @return the size in bytes of a single prefetch block.
   */
  public int getPrefetchBlockSize() {
    return this.prefetchBlockSize;
  }

  /**
   * Gets the size of prefetch queue (in number of blocks).
   *
   * @return the size of prefetch queue (in number of blocks).
   */
  public int getPrefetchBlockCount() {
    return this.prefetchBlockCount;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3AReadOpContext{");
    sb.append("path=").append(path);
    sb.append(", inputPolicy=").append(inputPolicy);
    sb.append(", readahead=").append(readahead);
    sb.append(", changeDetectionPolicy=").append(changeDetectionPolicy);
    sb.append('}');
    return sb.toString();
  }
}
