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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.statistics.DurationTracker;

import static org.apache.hadoop.util.Preconditions.checkState;

class ReadBufferWorker implements Runnable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ReadBufferWorker.class);

  protected static final CountDownLatch UNLEASH_WORKERS = new CountDownLatch(1);
  private int id;

  private boolean doneReadingInvoked;

  ReadBufferWorker(final int id) {
    this.id = id;
  }

  /**
   * return the ID of ReadBufferWorker.
   */
  public int getId() {
    return this.id;
  }

  /**
   * Waits until a buffer becomes available in ReadAheadQueue.
   * Once a buffer becomes available, reads the file specified in it and then posts results back to buffer manager.
   * Rinse and repeat. Forever.
   */
  public void run() {
    try {
      UNLEASH_WORKERS.await();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    ReadBufferManager bufferManager = ReadBufferManager.getBufferManager();
    ReadBuffer buffer;
    while (true) {
      try {
        buffer = bufferManager.getNextBlockToRead();   // blocks, until a buffer is available for this thread
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        return;
      }
      if (buffer != null) {
        fetchBuffer(bufferManager, buffer);
      }
    }
  }

  /**
   * Fetch the data for the buffer.
   * @param bufferManager buffer manager.
   * @param buffer the buffer to read in.
   */
  @VisibleForTesting
  void fetchBuffer(final ReadBufferManager bufferManager, final ReadBuffer buffer) {
    LOGGER.trace("Reading {}", buffer);
    doneReadingInvoked = false;
    // Stop network call if stream is closed
    if (postFailureWhenStreamClosed(bufferManager, buffer)) {
      // stream closed before the HTTP request was initiated.
      // Immediately move to the next request in the queue.
      return;
    }
    checkState(buffer.hasIndexedBuffer(),
        "ReadBuffer buffer has no allocated buffer to read into: %s",
        buffer);
    // input stream is updated with count/duration of prefetching
    DurationTracker tracker = buffer.trackPrefetchOperation();
    try {
      // do the actual read, from the file.
      int bytesRead = buffer.getStream().readRemote(
          buffer.getOffset(),
          buffer.getBuffer(),
          0,
          // If AbfsInputStream was created with bigger buffer size than
          // read-ahead buffer size, make sure a valid length is passed
          // for remote read
          Math.min(buffer.getRequestedLength(), buffer.getBuffer().length),
          buffer.getTracingContext());
      LOGGER.trace("Read {} bytes", bytesRead);

      // Update failure to completed list if stream is closed
      if (!postFailureWhenStreamClosed(bufferManager, buffer)) {
        // post result back to ReadBufferManager
        doneReading(bufferManager, buffer, ReadBufferStatus.AVAILABLE, bytesRead);
      }
      tracker.close();
    } catch (Exception ex) {
      tracker.failed();
      IOException ioe = ex instanceof IOException
          ? (IOException) ex
          : new PathIOException(buffer.getStream().getPath(), ex);
      buffer.setErrException(ioe);
      LOGGER.debug("prefetch failure for {}", buffer, ex);

      // if doneReading hasn't already been called for this iteration, call it.
      // the check ensures that any exception raised in doneReading() doesn't trigger
      // a second attempt.
      if (!doneReadingInvoked) {
        doneReading(bufferManager, buffer, ReadBufferStatus.READ_FAILED, 0);
      }
    }
  }

  /**
   * Check for the owning stream being closed; if so it
   * reports it to the buffer manager and then returns "true".
   * @param bufferManager buffer manager
   * @param buffer buffer to review
   * @return true if the stream is closed
   */
  private boolean postFailureWhenStreamClosed(
      ReadBufferManager bufferManager,
      ReadBuffer buffer) {

    // When stream is closed report failure to be picked by eviction
    if (buffer.isStreamClosed()) {
      LOGGER.debug("Stream closed; failing read");
      // Fail read
      doneReading(bufferManager, buffer, ReadBufferStatus.READ_FAILED, 0);
      return true;
    }
    return false;
  }

  private void doneReading(final ReadBufferManager bufferManager,
      final ReadBuffer buffer,
      final ReadBufferStatus result,
      final int bytesActuallyRead) {
    doneReadingInvoked = true;
    bufferManager.doneReading(buffer, result, bytesActuallyRead);
  }
}
