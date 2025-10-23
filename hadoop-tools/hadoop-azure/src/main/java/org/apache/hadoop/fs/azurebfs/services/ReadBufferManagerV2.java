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
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

final class ReadBufferManagerV2 extends ReadBufferManager {

  // Thread Pool Configurations
  private static int minThreadPoolSize;
  private static int maxThreadPoolSize;
  private static int executorServiceKeepAliveTimeInMilliSec;
  private ThreadPoolExecutor workerPool;

  // Buffer Pool Configurations
  private static int minBufferPoolSize;
  private static int maxBufferPoolSize;
  private int numberOfActiveBuffers = 0;
  private byte[][] bufferPool;

  private static ReadBufferManagerV2 bufferManager;

  // hide instance constructor
  private ReadBufferManagerV2() {
    LOGGER.trace("Creating readbuffer manager with HADOOP-18546 patch");
  }

  /**
   * Sets the read buffer manager configurations.
   * @param readAheadBlockSize the size of the read-ahead block in bytes
   * @param abfsConfiguration the AbfsConfiguration instance for other configurations
   */
  static void setReadBufferManagerConfigs(int readAheadBlockSize, AbfsConfiguration abfsConfiguration) {
    if (bufferManager == null) {
      minThreadPoolSize = abfsConfiguration.getMinReadAheadV2ThreadPoolSize();
      maxThreadPoolSize = abfsConfiguration.getMaxReadAheadV2ThreadPoolSize();
      executorServiceKeepAliveTimeInMilliSec = abfsConfiguration.getReadAheadExecutorServiceTTLInMillis();

      minBufferPoolSize = abfsConfiguration.getMinReadAheadV2BufferPoolSize();
      maxBufferPoolSize = abfsConfiguration.getMaxReadAheadV2BufferPoolSize();
      setThresholdAgeMilliseconds(abfsConfiguration.getReadAheadV2CachedBufferTTLMillis());
      setReadAheadBlockSize(readAheadBlockSize);
    }
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
    return bufferManager;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  void init() {
    // Initialize Buffer Pool
    bufferPool = new byte[maxBufferPoolSize][];
    for (int i = 0; i < minBufferPoolSize; i++) {
      bufferPool[i] = new byte[getReadAheadBlockSize()];  // same buffers are reused. These byte arrays are never garbage collected
      getFreeList().add(i);
      numberOfActiveBuffers++;
    }

    // Initialize a Fixed Size Thread Pool with minThreadPoolSize threads
    workerPool = new ThreadPoolExecutor(
        minThreadPoolSize,
        maxThreadPoolSize,
        executorServiceKeepAliveTimeInMilliSec,
        TimeUnit.MILLISECONDS,
        new SynchronousQueue<>(),
        namedThreadFactory);
    workerPool.allowCoreThreadTimeOut(true);
    for (int i = 0; i < minThreadPoolSize; i++) {
      ReadBufferWorker worker = new ReadBufferWorker(i, this);
      workerPool.submit(worker);
    }
    ReadBufferWorker.UNLEASH_WORKERS.countDown();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void queueReadAhead(final AbfsInputStream stream,
      final long requestedOffset,
      final int requestedLength,
      final TracingContext tracingContext) {
    // TODO: To be implemented
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getBlock(final AbfsInputStream stream,
      final long position,
      final int length,
      final byte[] buffer) throws IOException {
    // TODO: To be implemented
    return 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ReadBuffer getNextBlockToRead() throws InterruptedException {
    // TODO: To be implemented
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void doneReading(final ReadBuffer buffer,
      final ReadBufferStatus result,
      final int bytesActuallyRead) {
    // TODO: To be implemented
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void purgeBuffersForStream(final AbfsInputStream stream) {
    // TODO: To be implemented
  }

  /**
   * {@inheritDoc}
   */
  @VisibleForTesting
  @Override
  public int getNumBuffers() {
    return numberOfActiveBuffers;
  }
  /**
   * {@inheritDoc}
   */
  @VisibleForTesting
  @Override
  public void callTryEvict() {
    // TODO: To be implemented
  }

  /**
   * {@inheritDoc}
   */
  @VisibleForTesting
  @Override
  public void testResetReadBufferManager() {
    // TODO: To be implemented
  }

  /**
   * {@inheritDoc}
   */
  @VisibleForTesting
  @Override
  public void testResetReadBufferManager(final int readAheadBlockSize,
      final int thresholdAgeMilliseconds) {
    // TODO: To be implemented
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void testMimicFullUseAndAddFailedBuffer(final ReadBuffer buf) {
    // TODO: To be implemented
  }

  private final ThreadFactory namedThreadFactory = new ThreadFactory() {
    private int count = 0;
    @Override
    public Thread newThread(Runnable r) {
      return new Thread(r, "ReadAheadV2-Thread-" + count++);
    }
  };

  @Override
  void resetBufferManager() {
    setBufferManager(null); // reset the singleton instance
  }

  private static void setBufferManager(ReadBufferManagerV2 manager) {
    bufferManager = manager;
  }
}
