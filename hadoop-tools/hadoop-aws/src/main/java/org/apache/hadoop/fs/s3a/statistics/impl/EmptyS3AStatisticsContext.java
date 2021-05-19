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

package org.apache.hadoop.fs.s3a.statistics.impl;

import java.io.IOException;
import java.time.Duration;

import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.s3guard.MetastoreInstrumentation;
import org.apache.hadoop.fs.s3a.s3guard.MetastoreInstrumentationImpl;
import org.apache.hadoop.fs.s3a.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.s3a.statistics.ChangeTrackerStatistics;
import org.apache.hadoop.fs.s3a.statistics.CommitterStatistics;
import org.apache.hadoop.fs.s3a.statistics.DelegationTokenStatistics;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.s3a.statistics.S3AMultipartUploaderStatistics;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticInterface;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.s3a.statistics.StatisticsFromAwsSdk;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.DurationTracker;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.emptyStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.stubDurationTracker;

/**
 * Special statistics context, all of whose context operations are no-ops.
 * All statistics instances it returns are also empty.
 * <p>
 * This class is here primarily to aid in testing, but it also allows for
 * classes to require a non-empty statistics context in their constructor -yet
 * still be instantiated without one bound to any filesystem.
 */
public final class EmptyS3AStatisticsContext implements S3AStatisticsContext {

  public static final MetastoreInstrumentation
      METASTORE_INSTRUMENTATION = new MetastoreInstrumentationImpl();

  public static final S3AInputStreamStatistics
      EMPTY_INPUT_STREAM_STATISTICS = new EmptyInputStreamStatistics();

  public static final CommitterStatistics
      EMPTY_COMMITTER_STATISTICS = new EmptyCommitterStatistics();

  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  public static final BlockOutputStreamStatistics
      EMPTY_BLOCK_OUTPUT_STREAM_STATISTICS
      = new EmptyBlockOutputStreamStatistics();

  public static final DelegationTokenStatistics
      EMPTY_DELEGATION_TOKEN_STATISTICS = new EmptyDelegationTokenStatistics();

  public static final StatisticsFromAwsSdk
      EMPTY_STATISTICS_FROM_AWS_SDK = new EmptyStatisticsFromAwsSdk();

  @Override
  public MetastoreInstrumentation getS3GuardInstrumentation() {
    return METASTORE_INSTRUMENTATION;
  }

  @Override
  public S3AInputStreamStatistics newInputStreamStatistics() {
    return EMPTY_INPUT_STREAM_STATISTICS;
  }

  @Override
  public CommitterStatistics newCommitterStatistics() {
    return EMPTY_COMMITTER_STATISTICS;
  }

  @Override
  public BlockOutputStreamStatistics newOutputStreamStatistics() {
    return EMPTY_BLOCK_OUTPUT_STREAM_STATISTICS;
  }

  @Override
  public DelegationTokenStatistics newDelegationTokenStatistics() {
    return EMPTY_DELEGATION_TOKEN_STATISTICS;
  }

  @Override
  public StatisticsFromAwsSdk newStatisticsFromAwsSdk() {
    return EMPTY_STATISTICS_FROM_AWS_SDK;
  }

  @Override
  public S3AMultipartUploaderStatistics createMultipartUploaderStatistics() {
    return new EmptyMultipartUploaderStatistics();
  }

  @Override
  public void incrementCounter(final Statistic op, final long count) {

  }

  @Override
  public void incrementGauge(final Statistic op, final long count) {

  }

  @Override
  public void decrementGauge(final Statistic op, final long count) {

  }

  @Override
  public void addValueToQuantiles(final Statistic op, final long value) {

  }

  @Override
  public void recordDuration(final Statistic op,
      final boolean success,
      final Duration duration) {

  }

  /**
   * Base class for all the empty implementations.
   */
  private static class EmptyS3AStatisticImpl implements
      S3AStatisticInterface {

    /**
     * Always return the stub duration tracker.
     * @param key statistic key prefix
     * @param count  #of times to increment the matching counter in this
     * operation.
     * @return stub tracker.
     */
    public DurationTracker trackDuration(String key, long count) {
      return stubDurationTracker();
    }
  }

  /**
   * Input Stream statistics callbacks.
   */
  private static final class EmptyInputStreamStatistics
      extends EmptyS3AStatisticImpl
      implements S3AInputStreamStatistics {

    @Override
    public void seekBackwards(final long negativeOffset) {

    }

    @Override
    public void seekForwards(final long skipped,
        final long bytesReadInSeek) {

    }

    @Override
    public long streamOpened() {
      return 0;
    }

    @Override
    public void streamClose(final boolean abortedConnection,
        final long remainingInCurrentRequest) {

    }

    @Override
    public void readException() {

    }

    @Override
    public void bytesRead(final long bytes) {

    }

    @Override
    public void readOperationStarted(final long pos, final long len) {

    }

    @Override
    public void readFullyOperationStarted(final long pos, final long len) {

    }

    @Override
    public void readOperationCompleted(final int requested, final int actual) {

    }

    @Override
    public void close() {

    }

    @Override
    public void inputPolicySet(final int updatedPolicy) {

    }

    @Override
    public void unbuffered() {

    }

    /**
     * Return an IO statistics instance.
     * @return an empty IO statistics instance.
     */
    @Override
    public IOStatistics getIOStatistics() {
      return emptyStatistics();
    }

    @Override
    public long getCloseOperations() {
      return 0;
    }

    @Override
    public long getClosed() {
      return 0;
    }

    @Override
    public long getAborted() {
      return 0;
    }

    @Override
    public long getForwardSeekOperations() {
      return 0;
    }

    @Override
    public long getBackwardSeekOperations() {
      return 0;
    }

    @Override
    public long getBytesRead() {
      return 0;
    }

    @Override
    public long getTotalBytesRead() {
      return 0;
    }

    @Override
    public long getBytesSkippedOnSeek() {
      return 0;
    }

    @Override
    public long getBytesBackwardsOnSeek() {
      return 0;
    }

    @Override
    public long getBytesReadInClose() {
      return 0;
    }

    @Override
    public long getBytesDiscardedInAbort() {
      return 0;
    }

    @Override
    public long getOpenOperations() {
      return 0;
    }

    @Override
    public long getSeekOperations() {
      return 0;
    }

    @Override
    public long getReadExceptions() {
      return 0;
    }

    @Override
    public long getReadOperations() {
      return 0;
    }

    @Override
    public long getReadFullyOperations() {
      return 0;
    }

    @Override
    public long getReadsIncomplete() {
      return 0;
    }

    @Override
    public long getPolicySetCount() {
      return 0;
    }

    @Override
    public long getVersionMismatches() {
      return 0;
    }

    @Override
    public long getInputPolicy() {
      return 0;
    }

    @Override
    public Long lookupCounterValue(final String name) {
      return 0L;
    }

    @Override
    public Long lookupGaugeValue(final String name) {
      return 0L;
    }

    @Override
    public ChangeTrackerStatistics getChangeTrackerStatistics() {
      return new CountingChangeTracker();
    }

    @Override
    public DurationTracker initiateGetRequest() {
      return stubDurationTracker();
    }

  }

  /**
   * Committer statistics.
   */
  private static final class EmptyCommitterStatistics
      extends EmptyS3AStatisticImpl
      implements CommitterStatistics {

    @Override
    public void commitCreated() {
    }

    @Override
    public void commitUploaded(final long size) {
    }

    @Override
    public void commitCompleted(final long size) {
    }

    @Override
    public void commitAborted() {
    }

    @Override
    public void commitReverted() {
    }

    @Override
    public void commitFailed() {
    }

    @Override
    public void taskCompleted(final boolean success) {
    }

    @Override
    public void jobCompleted(final boolean success) {
    }
  }

  private static final class EmptyBlockOutputStreamStatistics
      extends EmptyS3AStatisticImpl
      implements BlockOutputStreamStatistics {

    @Override
    public void blockUploadQueued(final int blockSize) {
    }

    @Override
    public void blockUploadStarted(final Duration timeInQueue,
        final int blockSize) {
    }

    @Override
    public void blockUploadCompleted(final Duration timeSinceUploadStarted,
        final int blockSize) {
    }

    @Override
    public void blockUploadFailed(final Duration timeSinceUploadStarted,
        final int blockSize) {
    }

    @Override
    public void bytesTransferred(final long byteCount) {
    }

    @Override
    public void exceptionInMultipartComplete(final int count) {

    }

    @Override
    public void exceptionInMultipartAbort() {
    }

    @Override
    public long getBytesPendingUpload() {
      return 0;
    }

    @Override
    public void commitUploaded(final long size) {

    }

    @Override
    public int getBlocksAllocated() {
      return 0;
    }

    @Override
    public int getBlocksReleased() {
      return 0;
    }

    @Override
    public int getBlocksActivelyAllocated() {
      return 0;
    }

    @Override
    public IOStatistics getIOStatistics() {
      return emptyStatistics();
    }

    @Override
    public void blockAllocated() {
    }

    @Override
    public void blockReleased() {
    }

    @Override
    public void writeBytes(final long count) {
    }

    @Override
    public long getBytesWritten() {
      return 0;
    }

    @Override
    public Long lookupCounterValue(final String name) {
      return 0L;
    }

    @Override
    public Long lookupGaugeValue(final String name) {
      return 0L;
    }

    @Override
    public void hflushInvoked() {
    }

    @Override
    public void hsyncInvoked() {
    }

    @Override
    public void close() throws IOException {
    }

  }

  /**
   * Delegation Token Statistics.
   */
  private static final class EmptyDelegationTokenStatistics
      extends EmptyS3AStatisticImpl
      implements DelegationTokenStatistics {

    @Override
    public void tokenIssued() {

    }
  }

  /**
   * AWS SDK Callbacks.
   */
  private static final class EmptyStatisticsFromAwsSdk
      implements StatisticsFromAwsSdk {

    @Override
    public void updateAwsRequestCount(final long longValue) {

    }

    @Override
    public void updateAwsRetryCount(final long longValue) {

    }

    @Override
    public void updateAwsThrottleExceptionsCount(final long longValue) {

    }

    @Override
    public void noteAwsRequestTime(final Duration ofMillis) {

    }

    @Override
    public void noteAwsClientExecuteTime(final Duration ofMillis) {

    }

    @Override
    public void noteRequestMarshallTime(final Duration duration) {

    }

    @Override
    public void noteRequestSigningTime(final Duration duration) {

    }

    @Override
    public void noteResponseProcessingTime(final Duration duration) {

    }
  }

  /**
   * Multipart Uploader.
   */
  public static final class EmptyMultipartUploaderStatistics
      implements S3AMultipartUploaderStatistics {

    @Override
    public void instantiated() {

    }

    @Override
    public void uploadStarted() {

    }

    @Override
    public void partPut(final long lengthInBytes) {

    }

    @Override
    public void uploadCompleted() {

    }

    @Override
    public void uploadAborted() {

    }

    @Override
    public void abortUploadsUnderPathInvoked() {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public DurationTracker trackDuration(final String key, final long count) {
      return stubDurationTracker();
    }
  }
}
