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

import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;

class ReadBufferWorker implements Runnable {

  protected static final CountDownLatch UNLEASH_WORKERS = new CountDownLatch(1);
  private int id;

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
        try {
          // Stop network call if stream is closed
          if (postFailureWhenStreamClosed(bufferManager, buffer)) {
            continue;
          }

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

          // Update failure to completed list if stream is closed
          if (!postFailureWhenStreamClosed(bufferManager, buffer)) {
            bufferManager.doneReading(buffer, ReadBufferStatus.AVAILABLE,
                bytesRead);  // post result back to ReadBufferManager
          }
        } catch (IOException ex) {
          buffer.setErrException(ex);
          bufferManager.doneReading(buffer, ReadBufferStatus.READ_FAILED, 0);
        } catch (Exception ex) {
          buffer.setErrException(new PathIOException(buffer.getStream().getPath(), ex));
          bufferManager.doneReading(buffer, ReadBufferStatus.READ_FAILED, 0);
        }
      }
    }
  }

  private boolean postFailureWhenStreamClosed(ReadBufferManager bufferManager,
      ReadBuffer buffer) {

    // When stream is closed report failure to be picked by eviction
    if (buffer.getStream().isClosed()) {
      // Fail read
      bufferManager.doneReading(buffer, ReadBufferStatus.READ_FAILED, 0);
      return true;
    }
    return false;
  }
}
