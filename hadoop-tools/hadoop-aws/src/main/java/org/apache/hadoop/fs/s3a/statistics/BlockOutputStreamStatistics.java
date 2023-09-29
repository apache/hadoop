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

package org.apache.hadoop.fs.s3a.statistics;

import java.io.Closeable;
import java.time.Duration;

/**
 * Block output stream statistics.
 */
public interface BlockOutputStreamStatistics extends Closeable,
    S3AStatisticInterface,
    PutTrackerStatistics {

  /**
   * Block is queued for upload.
   * @param blockSize block size.
   */
  void blockUploadQueued(long blockSize);

  /**
   * Queued block has been scheduled for upload.
   * @param timeInQueue time in the queue.
   * @param blockSize block size.
   */
  void blockUploadStarted(Duration timeInQueue, long blockSize);

  /**
   * A block upload has completed. Duration excludes time in the queue.
   * @param timeSinceUploadStarted time in since the transfer began.
   * @param blockSize block size
   */
  void blockUploadCompleted(Duration timeSinceUploadStarted, long blockSize);

  /**
   *  A block upload has failed. Duration excludes time in the queue.
   * <p>
   *  A final transfer completed event is still expected, so this
   *  does not decrement the active block counter.
   * </p>
   * @param timeSinceUploadStarted time in since the transfer began.
   * @param blockSize block size
   */
  void blockUploadFailed(Duration timeSinceUploadStarted, long blockSize);

  /**
   * Intermediate report of bytes uploaded.
   * @param byteCount bytes uploaded
   */
  void bytesTransferred(long byteCount);

  /**
   * Note exception in a multipart complete.
   * @param count count of exceptions
   */
  void exceptionInMultipartComplete(int count);

  /**
   * Note an exception in a multipart abort.
   */
  void exceptionInMultipartAbort();

  /**
   * Get the number of bytes pending upload.
   * @return the number of bytes in the pending upload state.
   */
  long getBytesPendingUpload();

  /**
   * Data has been uploaded to be committed in a subsequent operation;
   * to be called at the end of the write.
   * @param size size in bytes
   */
  void commitUploaded(long size);

  int getBlocksAllocated();

  int getBlocksReleased();

  /**
   * Get counters of blocks actively allocated; may be inaccurate
   * if the numbers change during the (non-synchronized) calculation.
   * @return the number of actively allocated blocks.
   */
  int getBlocksActivelyAllocated();

  /**
   * Record bytes written.
   * @param count number of bytes
   */
  void writeBytes(long count);

  /**
   * Get the current count of bytes written.
   * @return the counter value.
   */
  long getBytesWritten();

  /**
   * A block has been allocated.
   */
  void blockAllocated();

  /**
   * A block has been released.
   */
  void blockReleased();

  /**
   * Get the value of a counter.
   * @param name counter name
   * @return the value or null if no matching counter was found.
   */
  Long lookupCounterValue(String name);

  /**
   * Get the value of a gauge.
   * @param name gauge name
   * @return the value or null if no matching gauge was found.
   */
  Long lookupGaugeValue(String name);

  /**
   * Syncable.hflush() has been invoked.
   */
  void hflushInvoked();

  /**
   * Syncable.hsync() has been invoked.
   */
  void hsyncInvoked();
}
