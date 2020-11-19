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

/**
 * Stats for the AbfsInputStream.
 */
public class AbfsInputStreamStatisticsImpl
    implements AbfsInputStreamStatistics {
  private long seekOperations;
  private long forwardSeekOperations;
  private long backwardSeekOperations;
  private long bytesRead;
  private long bytesSkippedOnSeek;
  private long bytesBackwardsOnSeek;
  private long seekInBuffer;
  private long readOperations;
  private long bytesReadFromBuffer;
  private long remoteReadOperations;
  private long readAheadBytesRead;
  private long remoteBytesRead;

  /**
   * Seek backwards, incrementing the seek and backward seek counters.
   *
   * @param negativeOffset how far was the seek?
   *                       This is expected to be negative.
   */
  @Override
  public void seekBackwards(long negativeOffset) {
    seekOperations++;
    backwardSeekOperations++;
    bytesBackwardsOnSeek -= negativeOffset;
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
    seekOperations++;
    forwardSeekOperations++;
    if (skipped > 0) {
      bytesSkippedOnSeek += skipped;
    }
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
    if (bytes > 0) {
      bytesRead += bytes;
    }
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
    if (bytes > 0) {
      bytesReadFromBuffer += bytes;
    }
  }

  /**
   * {@inheritDoc}
   *
   * Increment the number of seeks in the buffer.
   */
  @Override
  public void seekInBuffer() {
    seekInBuffer++;
  }

  /**
   * A {@code read(byte[] buf, int off, int len)} operation has started.
   *
   * @param pos starting position of the read.
   * @param len length of bytes to read.
   */
  @Override
  public void readOperationStarted(long pos, long len) {
    readOperations++;
  }

  /**
   * Total bytes read from readAhead buffer during a read operation.
   *
   * @param bytes the bytes to be incremented.
   */
  @Override
  public void readAheadBytesRead(long bytes) {
    if (bytes > 0) {
      readAheadBytesRead += bytes;
    }
  }

  /**
   * Total bytes read remotely after nothing was read from readAhead buffer.
   *
   * @param bytes the bytes to be incremented.
   */
  @Override
  public void remoteBytesRead(long bytes) {
    if (bytes > 0) {
      remoteBytesRead += bytes;
    }
  }

  /**
   * {@inheritDoc}
   *
   * Increment the counter when a remote read operation occurs.
   */
  @Override
  public void remoteReadOperation() {
    remoteReadOperations++;
  }

  public long getSeekOperations() {
    return seekOperations;
  }

  public long getForwardSeekOperations() {
    return forwardSeekOperations;
  }

  public long getBackwardSeekOperations() {
    return backwardSeekOperations;
  }

  public long getBytesRead() {
    return bytesRead;
  }

  public long getBytesSkippedOnSeek() {
    return bytesSkippedOnSeek;
  }

  public long getBytesBackwardsOnSeek() {
    return bytesBackwardsOnSeek;
  }

  public long getSeekInBuffer() {
    return seekInBuffer;
  }

  public long getReadOperations() {
    return readOperations;
  }

  public long getBytesReadFromBuffer() {
    return bytesReadFromBuffer;
  }

  public long getRemoteReadOperations() {
    return remoteReadOperations;
  }

  public long getReadAheadBytesRead() {
    return readAheadBytesRead;
  }

  public long getRemoteBytesRead() {
    return remoteBytesRead;
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
    sb.append(", SeekOperations=").append(seekOperations);
    sb.append(", ForwardSeekOperations=").append(forwardSeekOperations);
    sb.append(", BackwardSeekOperations=").append(backwardSeekOperations);
    sb.append(", BytesSkippedOnSeek=").append(bytesSkippedOnSeek);
    sb.append(", BytesBackwardsOnSeek=").append(bytesBackwardsOnSeek);
    sb.append(", seekInBuffer=").append(seekInBuffer);
    sb.append(", BytesRead=").append(bytesRead);
    sb.append(", ReadOperations=").append(readOperations);
    sb.append(", bytesReadFromBuffer=").append(bytesReadFromBuffer);
    sb.append(", remoteReadOperations=").append(remoteReadOperations);
    sb.append(", readAheadBytesRead=").append(readAheadBytesRead);
    sb.append(", remoteBytesRead=").append(remoteBytesRead);
    sb.append('}');
    return sb.toString();
  }
}
