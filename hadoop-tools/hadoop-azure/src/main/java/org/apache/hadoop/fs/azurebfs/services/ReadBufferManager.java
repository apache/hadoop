/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

/**
 * Interface for managing read buffers for Azure Blob File System input streams.
 */
public interface ReadBufferManager {

  /**
   * Queues a read-ahead request from {@link AbfsInputStream}
   * for a given offset in file and given length.
   * @param stream the input stream requesting the read-ahead
   * @param requestedOffset the offset in the remote file to start reading
   * @param requestedLength the number of bytes to read from file
   * @param tracingContext the tracing context for diagnostics
   */
  void queueReadAhead(AbfsInputStream stream,
      long requestedOffset,
      int requestedLength,
      TracingContext tracingContext);

  /**
   * Gets a block of data from the prefetched data by ReadBufferManager.
   * {@link AbfsInputStream} calls this method read any bytes already available in a buffer (thereby saving a
   * remote read). This returns the bytes if the data already exists in buffer. If there is a buffer that is reading
   * the requested offset, then this method blocks until that read completes. If the data is queued in a read-ahead
   * but not picked up by a worker thread yet, then it cancels that read-ahead and reports cache miss. This is because
   * depending on worker thread availability, the read-ahead may take a while - the calling thread can do its own
   * read to get the data faster (compared to the read waiting in queue for an indeterminate amount of time).
   *
   * @param stream the input stream requesting the block
   * @param position the position in the file to read from
   * @param length the number of bytes to read
   * @param buffer the buffer to store the read data
   * @return the number of bytes actually read
   * @throws IOException if an I/O error occurs
   */
  int getBlock(AbfsInputStream stream,
      long position,
      int length,
      byte[] buffer) throws IOException;

  /**
   * {@link ReadBufferWorker} calls this to get the next buffer to read from read-ahead queue.
   * Requested read will be performed by background thread.
   *
   * @return the next {@link ReadBuffer} to read
   * @throws InterruptedException if interrupted while waiting
   */
  ReadBuffer getNextBlockToRead() throws InterruptedException;

  /**
   * Marks the specified buffer as done reading and updates its status.
   * Called by {@link ReadBufferWorker} after reading is complete.
   *
   * @param buffer the buffer that was read by worker thread
   * @param result the status of the read operation
   * @param bytesActuallyRead the number of bytes actually read by worker thread.
   */
  void doneReading(ReadBuffer buffer,
      ReadBufferStatus result,
      int bytesActuallyRead);

  /**
   * Purging the buffers associated with an {@link AbfsInputStream}
   * from {@link ReadBufferManager} when stream is closed.
   *
   * @param stream the input stream whose buffers should be purged.
   */
  void purgeBuffersForStream(AbfsInputStream stream);


  // Following Methods are for testing purposes only and should not be used in production code.
  /**
   * Gets the threshold age in milliseconds for buffer eviction.
   *
   * @return the threshold age in milliseconds
   */
  @VisibleForTesting
  int getThresholdAgeMilliseconds();

  /**
   * Gets the block size used for read-ahead operations.
   *
   * @return the read-ahead block size in bytes
   */
  @VisibleForTesting
  int getReadAheadBlockSize();

  /**
   * Gets the number of buffers currently managed by the read buffer manager.
   *
   * @return the number of buffers
   */
  @VisibleForTesting
  int getNumBuffers();

  /**
   * Gets a copy of the list of free buffer indices.
   *
   * @return a list of free buffer indices
   */
  @VisibleForTesting
  List<Integer> getFreeListCopy();

  /**
   * Gets a copy of the read-ahead queue.
   *
   * @return a list of {@link ReadBuffer} objects in the read-ahead queue
   */
  @VisibleForTesting
  List<ReadBuffer> getReadAheadQueueCopy();

  /**
   * Gets a copy of the list of in-progress read buffers.
   *
   * @return a list of in-progress {@link ReadBuffer} objects
   */
  @VisibleForTesting
  List<ReadBuffer> getInProgressCopiedList();

  /**
   * Gets a copy of the list of completed read buffers.
   *
   * @return a list of completed {@link ReadBuffer} objects
   */
  @VisibleForTesting
  List<ReadBuffer> getCompletedReadListCopy();

  /**
   * Gets the size of the completed read list.
   *
   * @return the number of completed read buffers
   */
  @VisibleForTesting
  int getCompletedReadListSize();

  /**
   * Attempts to evict buffers based on the eviction policy.
   */
  @VisibleForTesting
  void callTryEvict();

  /**
   * Resets the read buffer manager for testing purposes. Clean up the current
   * state of readAhead buffers and the lists. Will also trigger a fresh init.
   */
  @VisibleForTesting
  void testResetReadBufferManager();

  /**
   * Resets the read buffer manager for testing with the specified block size and threshold age.
   *
   * @param readAheadBlockSize the block size for read-ahead
   * @param thresholdAgeMilliseconds the threshold age in milliseconds
   */
  @VisibleForTesting
  void testResetReadBufferManager(int readAheadBlockSize, int thresholdAgeMilliseconds);

  /**
   * Simulates full buffer usage and adds a failed buffer for testing.
   *
   * @param buf the buffer to add as failed
   */
  @VisibleForTesting
  void testMimicFullUseAndAddFailedBuffer(ReadBuffer buf);
}
