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

package org.apache.hadoop.fs.s3a.impl.statistics;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.s3guard.MetastoreInstrumentation;

/**
 * An S3A statistics context which is bonded to a
 * S3AInstrumentation instance -inevitably that of an S3AFileSystem
 * instance.
 * An interface is used to bind to the relevant fields, rather
 * than have them passed in the constructor because some
 * production code, specifically, DelegateToFileSystem,
 * patches the protected field after initialization.
 *
 * All operations are passed through directly to that class.
 *
 * If an instance of FileSystem.Statistics is passed in, it
 * will be used whenever input stream statistics are created -
 * However, Internally always increments the statistics in the
 * current thread.
 * As a result, cross-thread IO will under-report.
 *
 * This is addressed through the stream statistics classes
 * only updating the stats in the close() call. Provided
 * they are closed in the worker thread, all stats collected in
 * helper threads will be included.
 */
public class IntegratedS3AStatisticsContext implements S3AStatisticsContext {

  private final S3AFSStatisticsSource statisticsSource;

  /**
   * Instantiate.
   * @param statisticsSource integration binding
   */
  public IntegratedS3AStatisticsContext(
      final S3AFSStatisticsSource statisticsSource) {
    this.statisticsSource = statisticsSource;
  }


  /**
   * Get the instrumentation from the FS integraation.
   * @return instrumentation instance.
   */
  private S3AInstrumentation getInstrumentation() {
    return statisticsSource.getInstrumentation();
  }

  /**
   * The filesystem statistics: know this is thread-local.
   * @return FS statistics.
   */
  private FileSystem.Statistics getInstanceStatistics() {
    return statisticsSource.getInstanceStatistics();
  }

  /**
   * Get a MetastoreInstrumentation getInstrumentation() instance for this
   * context.
   * @return the S3Guard getInstrumentation() point.
   */
  @Override
  public MetastoreInstrumentation getMetastoreInstrumentation() {
    return getInstrumentation().getS3GuardInstrumentation();
  }

  /**
   * Create a stream input statistics instance.
   * @return the new instance
   */
  @Override
  public S3AInputStreamStatistics newInputStreamStatistics() {
    return getInstrumentation().newInputStreamStatistics(
        statisticsSource.getInstanceStatistics());
  }

  /**
   * Create a new instance of the committer statistics.
   * @return a new committer statistics instance
   */
  @Override
  public CommitterStatistics newCommitterStatistics() {
    return getInstrumentation().newCommitterStatistics();
  }

  /**
   * Create a stream output statistics instance.
   * @return the new instance
   */
  @Override
  public BlockOutputStreamStatistics newOutputStreamStatistics() {
    return getInstrumentation()
        .newOutputStreamStatistics(getInstanceStatistics());
  }

  /**
   * Increment a specific counter.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   */
  @Override
  public void incrementCounter(Statistic op, long count) {
    getInstrumentation().incrementCounter(op, count);
  }

  /**
   * Increment a specific gauge.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   * @throws ClassCastException if the metric is of the wrong type
   */
  @Override
  public void incrementGauge(Statistic op, long count) {
    getInstrumentation().incrementGauge(op, count);
  }

  /**
   * Decrement a specific gauge.
   * No-op if not defined.
   * @param op operation
   * @param count increment value
   * @throws ClassCastException if the metric is of the wrong type
   */
  @Override
  public void decrementGauge(Statistic op, long count) {
    getInstrumentation().decrementGauge(op, count);
  }

  /**
   * Add a value to a quantiles statistic. No-op if the quantile
   * isn't found.
   * @param op operation to look up.
   * @param value value to add.
   * @throws ClassCastException if the metric is not a Quantiles.
   */
  @Override
  public void addValueToQuantiles(Statistic op, long value) {
    getInstrumentation().addValueToQuantiles(op, value);
  }

  /**
   * Create a delegation token statistics instance.
   * @return an instance of delegation token statistics
   */
  @Override
  public DelegationTokenStatistics newDelegationTokenStatistics() {
    return getInstrumentation().newDelegationTokenStatistics();
  }

  /**
   * This is the interface which an integration source must implement
   * for the integration.
   * Note that the FileSystem.statistics field may be null for a class;
   */
  public interface S3AFSStatisticsSource {

    /**
     * Get the S3A Instrumentation.
     * @return a non-null instrumentation instance
     */
    S3AInstrumentation getInstrumentation();

    /**
     * Get the statistics of the FS instance, shared across all threads.
     * @return filesystem statistics
     */
    @Nullable
    FileSystem.Statistics getInstanceStatistics();

  }
}
