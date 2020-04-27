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

import org.apache.hadoop.fs.statistics.IOStatistics;

/**
 * Statistics updated by an input stream during its actual operation.
 * It also contains getters for tests.
 */
public interface S3AInputStreamStatistics extends AutoCloseable {

  /**
   * Seek backwards, incrementing the seek and backward seek counters.
   * @param negativeOffset how far was the seek?
   * This is expected to be negative.
   */
  void seekBackwards(long negativeOffset);

  /**
   * Record a forward seek, adding a seek operation, a forward
   * seek operation, and any bytes skipped.
   * @param skipped number of bytes skipped by reading from the stream.
   * If the seek was implemented by a close + reopen, set this to zero.
   */
  void seekForwards(long skipped);

  /**
   * The inner stream was opened.
   * @return the previous count
   */
  long streamOpened();

  /**
   * The inner stream was closed.
   * @param abortedConnection flag to indicate the stream was aborted,
   * rather than closed cleanly
   * @param remainingInCurrentRequest the number of bytes remaining in
   * the current request.
   */
  void streamClose(boolean abortedConnection,
      long remainingInCurrentRequest);

  /**
   * An ignored stream read exception was received.
   */
  void readException();

  /**
   * Increment the bytes read counter by the number of bytes;
   * no-op if the argument is negative.
   * @param bytes number of bytes read
   */
  void bytesRead(long bytes);

  /**
   * A {@code read(byte[] buf, int off, int len)} operation has started.
   * @param pos starting position of the read
   * @param len length of bytes to read
   */
  void readOperationStarted(long pos, long len);

  /**
   * A {@code PositionedRead.read(position, buffer, offset, length)}
   * operation has just started.
   * @param pos starting position of the read
   * @param len length of bytes to read
   */
  void readFullyOperationStarted(long pos, long len);

  /**
   * A read operation has completed.
   * @param requested number of requested bytes
   * @param actual the actual number of bytes
   */
  void readOperationCompleted(int requested, int actual);

  @Override
  void close();

  /**
   * The input policy has been switched.
   * @param updatedPolicy enum value of new policy.
   */
  void inputPolicySet(int updatedPolicy);

  /**
   * Get a reference to the change tracker statistics for this
   * stream.
   * @return a reference to the change tracker statistics
   */
  ChangeTrackerStatistics getChangeTrackerStatistics();

  /**
   * Merge the statistics into the filesystem's instrumentation instance.
   * Takes a diff between the current version of the stats and the
   * version of the stats when merge was last called, and merges the diff
   * into the instrumentation instance. Used to periodically merge the
   * stats into the fs-wide stats. <b>Behavior is undefined if called on a
   * closed instance.</b>
   */
  void merge(boolean isClosed);

  /**
   * Convert to an IOStatistics source which is
   * dynamically updated.
   * @return statistics
   */
  IOStatistics createIOStatistics();

  long getCloseOperations();

  long getClosed();

  long getAborted();

  long getForwardSeekOperations();

  long getBackwardSeekOperations();

  long getBytesRead();

  long getBytesSkippedOnSeek();

  long getBytesBackwardsOnSeek();

  long getBytesReadInClose();

  long getBytesDiscardedInAbort();

  long getOpenOperations();

  long getSeekOperations();

  long getReadExceptions();

  long getReadOperations();

  long getReadFullyOperations();

  long getReadsIncomplete();

  long getPolicySetCount();

  long getVersionMismatches();

  long getInputPolicy();
}
