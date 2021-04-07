/**
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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

/**
 * OutputStream statistics implementation for Abfs.
 */
public class AbfsOutputStreamStatisticsImpl
    implements AbfsOutputStreamStatistics {

  private final IOStatisticsStore ioStatisticsStore = iostatisticsStore()
      .withCounters(
          StreamStatisticNames.BYTES_TO_UPLOAD,
          StreamStatisticNames.BYTES_UPLOAD_SUCCESSFUL,
          StreamStatisticNames.BYTES_UPLOAD_FAILED,
          StreamStatisticNames.QUEUE_SHRUNK_OPS,
          StreamStatisticNames.WRITE_CURRENT_BUFFER_OPERATIONS
      )
      .withDurationTracking(
          StreamStatisticNames.TIME_SPENT_ON_PUT_REQUEST,
          StreamStatisticNames.TIME_SPENT_ON_TASK_WAIT
      )
      .build();

  /* Reference to the atomic counter for frequently updated counters to avoid
   * cost of the map lookup on every increment.
   */
  private final AtomicLong bytesUpload =
      ioStatisticsStore.getCounterReference(StreamStatisticNames.BYTES_TO_UPLOAD);
  private final AtomicLong bytesUploadedSuccessfully =
      ioStatisticsStore.getCounterReference(StreamStatisticNames.BYTES_UPLOAD_SUCCESSFUL);
  private final AtomicLong writeCurrentBufferOps =
      ioStatisticsStore.getCounterReference(StreamStatisticNames.WRITE_CURRENT_BUFFER_OPERATIONS);

  /**
   * Records the need to upload bytes and increments the total bytes that
   * needs to be uploaded.
   *
   * @param bytes total bytes to upload. Negative bytes are ignored.
   */
  @Override
  public void bytesToUpload(long bytes) {
    bytesUpload.addAndGet(bytes);
  }

  /**
   * Records the total bytes successfully uploaded through AbfsOutputStream.
   *
   * @param bytes number of bytes that were successfully uploaded. Negative
   *              bytes are ignored.
   */
  @Override
  public void uploadSuccessful(long bytes) {
    bytesUploadedSuccessfully.addAndGet(bytes);
  }

  /**
   * Records the total bytes failed to upload through AbfsOutputStream.
   *
   * @param bytes number of bytes failed to upload. Negative bytes are ignored.
   */
  @Override
  public void uploadFailed(long bytes) {
    ioStatisticsStore.incrementCounter(StreamStatisticNames.BYTES_UPLOAD_FAILED, bytes);
  }

  /**
   * {@inheritDoc}
   *
   * Records the total time spent waiting for a task to complete.
   *
   * When the thread executor has a task queue
   * {@link java.util.concurrent.BlockingQueue} of size greater than or
   * equal to 2 times the maxConcurrentRequestCounts then, it waits for a
   * task in that queue to finish, then do the next task in the queue.
   *
   * This time spent while waiting for the task to be completed is being
   * recorded in this counter.
   *
   */
  @Override
  public DurationTracker timeSpentTaskWait() {
    return ioStatisticsStore.trackDuration(StreamStatisticNames.TIME_SPENT_ON_TASK_WAIT);
  }

  /**
   * {@inheritDoc}
   *
   * Records the number of times AbfsOutputStream try to remove the completed
   * write operations from the beginning of write operation task queue.
   */
  @Override
  public void queueShrunk() {
    ioStatisticsStore.incrementCounter(StreamStatisticNames.QUEUE_SHRUNK_OPS);
  }

  /**
   * {@inheritDoc}
   *
   * Records the number of times AbfsOutputStream writes the buffer to the
   * service via the AbfsClient and appends the buffer to the service.
   */
  @Override
  public void writeCurrentBuffer() {
    writeCurrentBufferOps.incrementAndGet();
  }

  /**
   * {@inheritDoc}
   *
   * A getter for IOStatisticsStore instance which extends IOStatistics.
   *
   * @return IOStatisticsStore instance.
   */
  @Override
  public IOStatistics getIOStatistics() {
    return ioStatisticsStore;
  }

  @VisibleForTesting
  public long getBytesToUpload() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.BYTES_TO_UPLOAD);
  }

  @VisibleForTesting
  public long getBytesUploadSuccessful() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.BYTES_UPLOAD_SUCCESSFUL);
  }

  @VisibleForTesting
  public long getBytesUploadFailed() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.BYTES_UPLOAD_FAILED);
  }

  @VisibleForTesting
  public long getTimeSpentOnTaskWait() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.TIME_SPENT_ON_TASK_WAIT);
  }

  @VisibleForTesting
  public long getQueueShrunkOps() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.QUEUE_SHRUNK_OPS);
  }

  @VisibleForTesting
  public long getWriteCurrentBufferOperations() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.WRITE_CURRENT_BUFFER_OPERATIONS);
  }

  /**
   * Getter for mean value of time taken to complete a PUT request by
   * AbfsOutputStream.
   * @return mean value.
   */
  @VisibleForTesting
  public double getTimeSpentOnPutRequest() {
    return ioStatisticsStore.meanStatistics().get(StreamStatisticNames.TIME_SPENT_ON_PUT_REQUEST + StoreStatisticNames.SUFFIX_MEAN).mean();
  }

  /**
   * String to show AbfsOutputStream statistics values in AbfsOutputStream.
   *
   * @return String with AbfsOutputStream statistics.
   */
  @Override public String toString() {
    final StringBuilder outputStreamStats = new StringBuilder(
        "OutputStream Statistics{");
    outputStreamStats.append(ioStatisticsStore.toString());
    outputStreamStats.append("}");
    return outputStreamStats.toString();
  }
}
