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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_READ_AHEAD_BLOCK_SIZE;

/**
 * Abstract class for managing read buffers for Azure Blob File System input streams.
 */
public abstract class ReadBufferManager {
  protected static final Logger LOGGER = LoggerFactory.getLogger(
      ReadBufferManager.class);
  protected static final ReentrantLock LOCK = new ReentrantLock();
  private static int thresholdAgeMilliseconds;
  private static int blockSize = DEFAULT_READ_AHEAD_BLOCK_SIZE; // default block size for read-ahead in bytes

  private Stack<Integer> freeList = new Stack<>();   // indices in buffers[] array that are available
  private Queue<ReadBuffer> readAheadQueue = new LinkedList<>(); // queue of requests that are not picked up by any worker thread yet
  private LinkedList<ReadBuffer> inProgressList = new LinkedList<>(); // requests being processed by worker threads
  private LinkedList<ReadBuffer> completedReadList = new LinkedList<>(); // buffers available for reading

  /**
   * Initializes the ReadBufferManager singleton instance. Creates the read buffers and threads.
   * This method should be called once to set up the read buffer manager.
   */
  abstract void init();

  /**
   * Queues a read-ahead request from {@link AbfsInputStream}
   * for a given offset in file and given length.
   * @param stream the input stream requesting the read-ahead
   * @param requestedOffset the offset in the remote file to start reading
   * @param requestedLength the number of bytes to read from file
   * @param tracingContext the tracing context for diagnostics
   */
  abstract void queueReadAhead(AbfsInputStream stream,
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
  abstract int getBlock(AbfsInputStream stream,
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
  abstract ReadBuffer getNextBlockToRead() throws InterruptedException;

  /**
   * Marks the specified buffer as done reading and updates its status.
   * Called by {@link ReadBufferWorker} after reading is complete.
   *
   * @param buffer the buffer that was read by worker thread
   * @param result the status of the read operation
   * @param bytesActuallyRead the number of bytes actually read by worker thread.
   */
  abstract void doneReading(ReadBuffer buffer,
      ReadBufferStatus result,
      int bytesActuallyRead);

  /**
   * Purging the buffers associated with an {@link AbfsInputStream}
   * from {@link ReadBufferManager} when stream is closed.
   *
   * @param stream the input stream whose buffers should be purged.
   */
  abstract void purgeBuffersForStream(AbfsInputStream stream);


  // Following Methods are for testing purposes only and should not be used in production code.

  /**
   * Gets the number of buffers currently managed by the read buffer manager.
   *
   * @return the number of buffers
   */
  @VisibleForTesting
  abstract int getNumBuffers();

  /**
   * Attempts to evict buffers based on the eviction policy.
   */
  @VisibleForTesting
  abstract void callTryEvict();

  /**
   * Resets the read buffer manager for testing purposes. Clean up the current
   * state of readAhead buffers and the lists. Will also trigger a fresh init.
   */
  @VisibleForTesting
  abstract void testResetReadBufferManager();

  /**
   * Resets the read buffer manager for testing with the specified block size and threshold age.
   *
   * @param readAheadBlockSize the block size for read-ahead
   * @param thresholdAgeMilliseconds the threshold age in milliseconds
   */
  @VisibleForTesting
  abstract void testResetReadBufferManager(int readAheadBlockSize, int thresholdAgeMilliseconds);

  /**
   * Resets the buffer manager instance to null for testing purposes.
   * This allows for reinitialization in tests.
   */
  abstract void resetBufferManager();

  /**
   * Gets the threshold age in milliseconds for buffer eviction.
   *
   * @return the threshold age in milliseconds
   */
  @VisibleForTesting
  protected static int getThresholdAgeMilliseconds() {
    return thresholdAgeMilliseconds;
  }

  /**
   * Sets the threshold age in milliseconds for buffer eviction.
   *
   * @param thresholdAgeMs the threshold age in milliseconds
   */
  @VisibleForTesting
  protected static void setThresholdAgeMilliseconds(int thresholdAgeMs) {
    thresholdAgeMilliseconds = thresholdAgeMs;
  }

  /**
   * Gets the block size used for read-ahead operations.
   *
   * @return the read-ahead block size in bytes
   */
  @VisibleForTesting
  protected static int getReadAheadBlockSize() {
    return blockSize;
  }

  /**
   * Sets the block size used for read-ahead operations.
   *
   * @param readAheadBlockSize the read-ahead block size in bytes
   */
  @VisibleForTesting
  protected static void setReadAheadBlockSize(int readAheadBlockSize) {
    if (readAheadBlockSize <= 0) {
      throw new IllegalArgumentException("Read-ahead block size must be positive");
    }
    blockSize = readAheadBlockSize;
  }

  /**
   * Gets the stack of free buffer indices.
   *
   * @return the stack of free buffer indices
   */
  public Stack<Integer> getFreeList() {
    return freeList;
  }

  /**
   * Gets the queue of read-ahead requests.
   *
   * @return the queue of {@link ReadBuffer} objects in the read-ahead queue
   */
  public Queue<ReadBuffer> getReadAheadQueue() {
    return readAheadQueue;
  }

  /**
   * Gets the list of in-progress read buffers.
   *
   * @return the list of {@link ReadBuffer} objects that are currently being processed
   */
  public LinkedList<ReadBuffer> getInProgressList() {
    return inProgressList;
  }

  /**
   * Gets the list of completed read buffers.
   *
   * @return the list of {@link ReadBuffer} objects that have been read and are available for use
   */
  public LinkedList<ReadBuffer> getCompletedReadList() {
    return completedReadList;
  }


  /**
   * Gets a copy of the list of free buffer indices.
   *
   * @return a list of free buffer indices
   */
  @VisibleForTesting
  protected synchronized List<Integer> getFreeListCopy() {
    return new ArrayList<>(freeList);
  }

  /**
   * Gets a copy of the read-ahead queue.
   *
   * @return a list of {@link ReadBuffer} objects in the read-ahead queue
   */
  @VisibleForTesting
  protected synchronized List<ReadBuffer> getReadAheadQueueCopy() {
    return new ArrayList<>(readAheadQueue);
  }

  /**
   * Gets a copy of the list of in-progress read buffers.
   *
   * @return a list of in-progress {@link ReadBuffer} objects
   */
  @VisibleForTesting
  protected synchronized List<ReadBuffer> getInProgressCopiedList() {
    return new ArrayList<>(inProgressList);
  }

  /**
   * Gets a copy of the list of completed read buffers.
   *
   * @return a list of completed {@link ReadBuffer} objects
   */
  @VisibleForTesting
  protected synchronized List<ReadBuffer> getCompletedReadListCopy() {
    return new ArrayList<>(completedReadList);
  }

  /**
   * Gets the size of the completed read list.
   *
   * @return the number of completed read buffers
   */
  @VisibleForTesting
  protected int getCompletedReadListSize() {
    return completedReadList.size();
  }

  /**
   * Simulates full buffer usage and adds a failed buffer for testing.
   *
   * @param buf the buffer to add as failed
   */
  @VisibleForTesting
  protected void testMimicFullUseAndAddFailedBuffer(ReadBuffer buf) {
    freeList.clear();
    completedReadList.add(buf);
  }
}
