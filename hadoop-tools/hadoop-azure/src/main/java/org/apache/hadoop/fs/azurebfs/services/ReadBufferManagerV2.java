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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class ReadBufferManagerV2 extends ReadBufferManager {

  // hide instance constructor
  private ReadBufferManagerV2() {
    LOGGER.trace("Creating readbuffer manager with HADOOP-18546 patch");
  }

  /**
   * Sets the read buffer manager configurations.
   * @param readAheadBlockSize the size of the read-ahead block in bytes
   * @param configuration the AbfsConfiguration instance for other configurations
   */
  static void setReadBufferManagerConfigs(int readAheadBlockSize, AbfsConfiguration configuration) {

  }

  /**
   * Returns the singleton instance of ReadBufferManagerV2.
   * @return the singleton instance of ReadBufferManagerV2
   */
  static ReadBufferManagerV2 getBufferManager() {
    if (bufferManager == null) {
      LOCK.lock();
      try {
        if (bufferManager == null) {
          bufferManager = new ReadBufferManagerV2();
          bufferManager.init();
        }
      } finally {
        LOCK.unlock();
      }
    }
    return (ReadBufferManagerV2) bufferManager;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  void init() {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void queueReadAhead(final AbfsInputStream stream,
      final long requestedOffset,
      final int requestedLength,
      final TracingContext tracingContext) {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getBlock(final AbfsInputStream stream,
      final long position,
      final int length,
      final byte[] buffer) throws IOException {
    return 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ReadBuffer getNextBlockToRead() throws InterruptedException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void doneReading(final ReadBuffer buffer,
      final ReadBufferStatus result,
      final int bytesActuallyRead) {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void purgeBuffersForStream(final AbfsInputStream stream) {

  }

  /**
   * {@inheritDoc}
   */
  @VisibleForTesting
  @Override
  public int getNumBuffers() {
    return 0;
  }
  /**
   * {@inheritDoc}
   */
  @VisibleForTesting
  @Override
  public void callTryEvict() {

  }

  /**
   * {@inheritDoc}
   */
  @VisibleForTesting
  @Override
  public void testResetReadBufferManager() {

  }

  /**
   * {@inheritDoc}
   */
  @VisibleForTesting
  @Override
  public void testResetReadBufferManager(final int readAheadBlockSize,
      final int thresholdAgeMilliseconds) {

  }

  /**
   * {@inheritDoc}
   */
  @VisibleForTesting


  /**
   * {@inheritDoc}
   */
  @Override
  public void testMimicFullUseAndAddFailedBuffer(final ReadBuffer buf) {

  }
}
