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

import java.io.IOException;

import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.s3guard.MetastoreInstrumentation;
import org.apache.hadoop.fs.s3a.s3guard.MetastoreInstrumentationImpl;
import org.apache.hadoop.fs.statistics.IOStatistics;

/**
 * Special statistics context, all of whose context operations are no-ops.
 * All statistics instances it returns are also empty.
 *
 * This class is here primarily to aid in testing, but it also allows for
 * classes to require a non-empty statistics context in their constructor -yet
 * still be instantiated without one bound to any filesystem.
 */
public final class EmptyS3AStatisticsContext implements S3AStatisticsContext {

  @Override
  public MetastoreInstrumentation getMetastoreInstrumentation() {
    return new MetastoreInstrumentationImpl();
  }

  @Override
  public S3AInputStreamStatistics newInputStreamStatistics() {
    return new EmptyInputStreamStatistics();
  }

  @Override
  public CommitterStatistics newCommitterStatistics() {
    return new EmptyCommitterStatistics();
  }

  @Override
  public BlockOutputStreamStatistics newOutputStreamStatistics() {
    return new EmptyBlockOutputStreamStatistics();
  }

  @Override
  public DelegationTokenStatistics newDelegationTokenStatistics() {
    return new EmptyDelegationTokenStatistics();
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

  private static final class EmptyInputStreamStatistics
      implements S3AInputStreamStatistics {

    @Override
    public void seekBackwards(final long negativeOffset) {

    }

    @Override
    public void seekForwards(final long skipped) {

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
    public void merge(final boolean isClosed) {

    }

    @Override
    public IOStatistics createIOStatistics() {
      return null;
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
    public ChangeTrackerStatistics getChangeTrackerStatistics() {
      return new CountingChangeTracker();
    }
  }

  private static final class EmptyCommitterStatistics
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
      implements BlockOutputStreamStatistics {

    @Override
    public void blockUploadQueued(final int blockSize) {

    }

    @Override
    public void blockUploadStarted(final long duration, final int blockSize) {

    }

    @Override
    public void blockUploadCompleted(final long duration, final int blockSize) {

    }

    @Override
    public void blockUploadFailed(final long duration, final int blockSize) {

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
    public IOStatistics createIOStatistics() {
      return null;
    }

    @Override
    public void blockAllocated() {

    }

    @Override
    public void blockReleased() {

    }

    @Override
    public void close() throws IOException {

    }
  }

  private static final class EmptyDelegationTokenStatistics
      implements DelegationTokenStatistics {

    @Override
    public void tokenIssued() {

    }
  }
}
