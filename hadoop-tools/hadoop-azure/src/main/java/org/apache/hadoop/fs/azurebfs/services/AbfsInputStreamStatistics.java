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

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * Interface for statistics for the AbfsInputStream.
 */
@InterfaceStability.Unstable
public interface AbfsInputStreamStatistics extends IOStatisticsSource {
  /**
   * Seek backwards, incrementing the seek and backward seek counters.
   *
   * @param negativeOffset how far was the seek?
   *                       This is expected to be negative.
   */
  void seekBackwards(long negativeOffset);

  /**
   * Record a forward seek, adding a seek operation, a forward
   * seek operation, and any bytes skipped.
   *
   * @param skipped number of bytes skipped by reading from the stream.
   *                If the seek was implemented by a close + reopen, set this to zero.
   */
  void seekForwards(long skipped);

  /**
   * Record a forward or backward seek, adding a seek operation, a forward or
   * a backward seek operation, and number of bytes skipped.
   *
   * @param seekTo     seek to the position.
   * @param currentPos current position.
   */
  void seek(long seekTo, long currentPos);

  /**
   * Increment the bytes read counter by the number of bytes;
   * no-op if the argument is negative.
   *
   * @param bytes number of bytes read.
   */
  void bytesRead(long bytes);

  /**
   * Record the total bytes read from buffer.
   *
   * @param bytes number of bytes that are read from buffer.
   */
  void bytesReadFromBuffer(long bytes);

  /**
   * Records the total number of seeks done in the buffer.
   */
  void seekInBuffer();

  /**
   * A {@code read(byte[] buf, int off, int len)} operation has started.
   */
  void readOperationStarted();

  /**
   * Records a successful remote read operation.
   */
  void remoteReadOperation();

  /**
   * Records the bytes read from readAhead buffer.
   * @param bytes the bytes to be incremented.
   */
  void readAheadBytesRead(long bytes);

  /**
   * Records bytes read remotely after nothing from readAheadBuffer was read.
   * @param bytes the bytes to be incremented.
   */
  void remoteBytesRead(long bytes);

  /**
   * Get the IOStatisticsStore instance from AbfsInputStreamStatistics.
   * @return instance of IOStatisticsStore which extends IOStatistics.
   */
  IOStatistics getIOStatistics();

  /**
   * Makes the string of all the AbfsInputStream statistics.
   * @return the string with all the statistics.
   */
  @Override
  String toString();
}
