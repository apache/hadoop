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

import java.io.Closeable;

import org.apache.hadoop.fs.statistics.IOStatistics;

/**
 * Block output stream statistics.
 */
public interface BlockOutputStreamStatistics extends Closeable {

  /**
   * Block is queued for upload.
   */
  void blockUploadQueued(int blockSize);

  /** Queued block has been scheduled for upload. */
  void blockUploadStarted(long duration, int blockSize);

  /** A block upload has completed. */
  void blockUploadCompleted(long duration, int blockSize);

  /**
   *  A block upload has failed.
   *  A final transfer completed event is still expected, so this
   *  does not decrement the active block counter.
   */
  void blockUploadFailed(long duration, int blockSize);

  /** Intermediate report of bytes uploaded. */
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
   * Get counters of blocks actively allocated; my be inaccurate
   * if the numbers change during the (non-synchronized) calculation.
   * @return the number of actively allocated blocks.
   */
  int getBlocksActivelyAllocated();

  /**
   * Convert to an IOStatistics source which is
   * dynamically updated.
   * @return statistics
   */
  IOStatistics createIOStatistics();

  /**
   * A block has been allocated.
   */
  void blockAllocated();

  /**
   * A block has been released.
   */
  void blockReleased();
}
