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

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.ACTION_HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_MEAN;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;

/**
 * Stats for the AbfsInputStream.
 */
public class AbfsInputStreamStatisticsImpl
    implements AbfsInputStreamStatistics {

  private final IOStatisticsStore ioStatisticsStore = iostatisticsStore()
      .withCounters(
          StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS,
          StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS,
          StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS,
          StreamStatisticNames.STREAM_READ_BYTES,
          StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED,
          StreamStatisticNames.STREAM_READ_OPERATIONS,
          StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS,
          StreamStatisticNames.SEEK_IN_BUFFER,
          StreamStatisticNames.BYTES_READ_BUFFER,
          StreamStatisticNames.REMOTE_READ_OP,
          StreamStatisticNames.READ_AHEAD_BYTES_READ,
          StreamStatisticNames.REMOTE_BYTES_READ
          )
      .withDurationTracking(ACTION_HTTP_GET_REQUEST)
      .build();

  /* Reference to the atomic counter for frequently updated counters to avoid
   * cost of the map lookup on every increment.
   */
  private final AtomicLong bytesRead =
      ioStatisticsStore.getCounterReference(StreamStatisticNames.STREAM_READ_BYTES);
  private final AtomicLong readOps =
      ioStatisticsStore.getCounterReference(StreamStatisticNames.STREAM_READ_OPERATIONS);
  private final AtomicLong seekOps =
      ioStatisticsStore.getCounterReference(StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS);

  /**
   * Seek backwards, incrementing the seek and backward seek counters.
   *
   * @param negativeOffset how far was the seek?
   *                       This is expected to be negative.
   */
  @Override
  public void seekBackwards(long negativeOffset) {
    seekOps.incrementAndGet();
    ioStatisticsStore.incrementCounter(StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS);
    ioStatisticsStore.incrementCounter(StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS, negativeOffset);
  }

  /**
   * Record a forward seek, adding a seek operation, a forward
   * seek operation, and any bytes skipped.
   *
   * @param skipped number of bytes skipped by reading from the stream.
   *                If the seek was implemented by a close + reopen, set this to zero.
   */
  @Override
  public void seekForwards(long skipped) {
    seekOps.incrementAndGet();
    ioStatisticsStore.incrementCounter(StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS);
    ioStatisticsStore.incrementCounter(StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED, skipped);
  }

  /**
   * Record a forward or backward seek, adding a seek operation, a forward or
   * a backward seek operation, and number of bytes skipped.
   * The seek direction will be calculated based on the parameters.
   *
   * @param seekTo     seek to the position.
   * @param currentPos current position.
   */
  @Override
  public void seek(long seekTo, long currentPos) {
    if (seekTo >= currentPos) {
      this.seekForwards(seekTo - currentPos);
    } else {
      this.seekBackwards(currentPos - seekTo);
    }
  }

  /**
   * Increment the bytes read counter by the number of bytes;
   * no-op if the argument is negative.
   *
   * @param bytes number of bytes read.
   */
  @Override
  public void bytesRead(long bytes) {
    bytesRead.addAndGet(bytes);
  }

  /**
   * {@inheritDoc}
   *
   * Total bytes read from the buffer.
   *
   * @param bytes number of bytes that are read from buffer.
   */
  @Override
  public void bytesReadFromBuffer(long bytes) {
    ioStatisticsStore.incrementCounter(StreamStatisticNames.BYTES_READ_BUFFER, bytes);
  }

  /**
   * {@inheritDoc}
   *
   * Increment the number of seeks in the buffer.
   */
  @Override
  public void seekInBuffer() {
    ioStatisticsStore.incrementCounter(StreamStatisticNames.SEEK_IN_BUFFER);
  }

  /**
   * A {@code read(byte[] buf, int off, int len)} operation has started.
   */
  @Override
  public void readOperationStarted() {
    readOps.incrementAndGet();
  }

  /**
   * Total bytes read from readAhead buffer during a read operation.
   *
   * @param bytes the bytes to be incremented.
   */
  @Override
  public void readAheadBytesRead(long bytes) {
    ioStatisticsStore.incrementCounter(StreamStatisticNames.READ_AHEAD_BYTES_READ, bytes);
  }

  /**
   * Total bytes read remotely after nothing was read from readAhead buffer.
   *
   * @param bytes the bytes to be incremented.
   */
  @Override
  public void remoteBytesRead(long bytes) {
    ioStatisticsStore.incrementCounter(StreamStatisticNames.REMOTE_BYTES_READ, bytes);
  }

  /**
   * {@inheritDoc}
   *
   * Increment the counter when a remote read operation occurs.
   */
  @Override
  public void remoteReadOperation() {
    ioStatisticsStore.incrementCounter(StreamStatisticNames.REMOTE_READ_OP);
  }

  /**
   * Getter for IOStatistics instance used.
   * @return IOStatisticsStore instance which extends IOStatistics.
   */
  @Override
  public IOStatistics getIOStatistics() {
    return ioStatisticsStore;
  }

  @VisibleForTesting
  public long getSeekOperations() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS);
  }

  @VisibleForTesting
  public long getForwardSeekOperations() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS);
  }

  @VisibleForTesting
  public long getBackwardSeekOperations() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS);
  }

  @VisibleForTesting
  public long getBytesRead() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.STREAM_READ_BYTES);
  }

  @VisibleForTesting
  public long getBytesSkippedOnSeek() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED);
  }

  @VisibleForTesting
  public long getBytesBackwardsOnSeek() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS);
  }

  @VisibleForTesting
  public long getSeekInBuffer() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.SEEK_IN_BUFFER);

  }

  @VisibleForTesting
  public long getReadOperations() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.STREAM_READ_OPERATIONS);
  }

  @VisibleForTesting
  public long getBytesReadFromBuffer() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.BYTES_READ_BUFFER);
  }

  @VisibleForTesting
  public long getRemoteReadOperations() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.REMOTE_READ_OP);
  }

  @VisibleForTesting
  public long getReadAheadBytesRead() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.READ_AHEAD_BYTES_READ);
  }

  @VisibleForTesting
  public long getRemoteBytesRead() {
    return ioStatisticsStore.counters().get(StreamStatisticNames.REMOTE_BYTES_READ);
  }

  /**
   * Getter for the mean value of the time taken to complete a HTTP GET
   * request by AbfsInputStream.
   * @return mean value.
   */
  @VisibleForTesting
  public double getActionHttpGetRequest() {
    return ioStatisticsStore.meanStatistics().
        get(ACTION_HTTP_GET_REQUEST + SUFFIX_MEAN).mean();
  }

  /**
   * String operator describes all the current statistics.
   * <b>Important: there are no guarantees as to the stability
   * of this value.</b>
   *
   * @return the current values of the stream statistics.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "StreamStatistics{");
    sb.append(ioStatisticsStore.toString());
    sb.append('}');
    return sb.toString();
  }
}
