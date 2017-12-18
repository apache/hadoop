/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.cblock.jscsiHelper.cache.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_BLOCK_BUFFER_FLUSH_INTERVAL;
import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_BLOCK_BUFFER_FLUSH_INTERVAL_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE;
import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_CACHE_KEEP_ALIVE;
import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_CACHE_KEEP_ALIVE_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_CACHE_THREAD_PRIORITY;
import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_CACHE_THREAD_PRIORITY_DEFAULT;

/**
 * This class manages the block ID buffer.
 * Block ID Buffer keeps a list of blocks which are in leveldb cache
 * This buffer is used later when the blocks are flushed to container
 *
 * Two blockIDBuffers are maintained so that write are not blocked when
 * DirtyLog is being written. Once a blockIDBuffer is full, it will be
 * enqueued for DirtyLog write while the other buffer accepts new write.
 * Once the DirtyLog write is done, the buffer is returned back to the pool.
 *
 * There are three triggers for blockIDBuffer flush
 * 1) BlockIDBuffer is full,
 * 2) Time period defined for blockIDBuffer flush has elapsed.
 * 3) Shutdown
 */
public class BlockBufferManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(BlockBufferManager.class);

  private enum FlushReason {
    BUFFER_FULL,
    SHUTDOWN,
    TIMER
  };

  private final int blockBufferSize;
  private final CBlockLocalCache parentCache;
  private final ScheduledThreadPoolExecutor scheduledExecutor;
  private final ThreadPoolExecutor threadPoolExecutor;
  private final long intervalSeconds;
  private final ArrayBlockingQueue<ByteBuffer> acquireQueue;
  private final ArrayBlockingQueue<Runnable> workQueue;
  private ByteBuffer currentBuffer;

  BlockBufferManager(Configuration config, CBlockLocalCache parentCache) {
    this.parentCache = parentCache;
    this.scheduledExecutor = new ScheduledThreadPoolExecutor(1);

    this.intervalSeconds =
        config.getTimeDuration(DFS_CBLOCK_BLOCK_BUFFER_FLUSH_INTERVAL,
            DFS_CBLOCK_BLOCK_BUFFER_FLUSH_INTERVAL_DEFAULT,
            TimeUnit.SECONDS);

    long keepAlive = config.getTimeDuration(DFS_CBLOCK_CACHE_KEEP_ALIVE,
        DFS_CBLOCK_CACHE_KEEP_ALIVE_DEFAULT,
        TimeUnit.SECONDS);
    this.workQueue = new ArrayBlockingQueue<>(2, true);
    int threadPri = config.getInt(DFS_CBLOCK_CACHE_THREAD_PRIORITY,
        DFS_CBLOCK_CACHE_THREAD_PRIORITY_DEFAULT);
    ThreadFactory workerThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("Cache Block Buffer Manager Thread #%d")
        .setDaemon(true)
        .setPriority(threadPri)
        .build();
    /*
     * starting a thread pool with core pool size of 1 and maximum of 2 threads
     * as there are maximum of 2 buffers which can be flushed at the same time.
     */
    this.threadPoolExecutor = new ThreadPoolExecutor(1, 2,
        keepAlive, TimeUnit.SECONDS, workQueue, workerThreadFactory,
        new ThreadPoolExecutor.AbortPolicy());

    this.blockBufferSize = config.getInt(DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE,
        DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE_DEFAULT) * (Long.SIZE / Byte.SIZE);
    this.acquireQueue = new ArrayBlockingQueue<>(2, true);

    for (int i = 0; i < 2; i++) {
      acquireQueue.add(ByteBuffer.allocate(blockBufferSize));
    }
    // get the first buffer to be used
    this.currentBuffer = acquireQueue.remove();

    LOG.info("BufferManager: Buffer Size:{} FlushIntervalSeconds:{}",
        blockBufferSize, intervalSeconds);
  }

  // triggerBlockBufferFlush enqueues current ByteBuffer for flush and returns.
  // This enqueue is asynchronous and hence triggerBlockBufferFlush will
  // only block when there are no available buffers in acquireQueue
  // Once the DirtyLog write is done, buffer is returned back to
  // BlockBufferManager using releaseBuffer
  private synchronized void triggerBlockBufferFlush(FlushReason reason) {
    LOG.debug("Flush triggered because: " + reason.toString() +
        " Num entries in buffer: " +
        currentBuffer.position() / (Long.SIZE / Byte.SIZE) +
        " Acquire Queue Size: " + acquireQueue.size());

    parentCache.getTargetMetrics().incNumBlockBufferFlushTriggered();
    BlockBufferFlushTask flushTask =
        new BlockBufferFlushTask(currentBuffer, parentCache, this);
    threadPoolExecutor.submit(flushTask);
    try {
      currentBuffer = acquireQueue.take();
    } catch (InterruptedException ex) {
      currentBuffer = null;
      parentCache.getTargetMetrics().incNumInterruptedBufferWaits();
      LOG.error("wait on take operation on acquire queue interrupted", ex);
      Thread.currentThread().interrupt();
    }
  }

  public synchronized void addToBlockBuffer(long blockId)  {
    parentCache.getTargetMetrics().incNumBlockBufferUpdates();
    currentBuffer.putLong(blockId);
    // if no space left, flush this buffer
    if (currentBuffer.remaining() == 0) {
      triggerBlockBufferFlush(FlushReason.BUFFER_FULL);
    }
  }

  public void releaseBuffer(ByteBuffer buffer) {
    if (buffer.position() != 0) {
      LOG.error("requeuing a non empty buffer with:{}",
          "elements enqueued in the acquire queue",
          buffer.position() / (Long.SIZE / Byte.SIZE));
      buffer.reset();
    }
    // There should always be space in the queue to add an element
    acquireQueue.add(buffer);
  }

  // Start a scheduled task to flush blockIDBuffer
  public void start() {
    Runnable scheduledTask = () -> triggerBlockBufferFlush(FlushReason.TIMER);
    scheduledExecutor.scheduleWithFixedDelay(scheduledTask, intervalSeconds,
                                        intervalSeconds, TimeUnit.SECONDS);
    threadPoolExecutor.prestartAllCoreThreads();
  }

  public void shutdown() {
    triggerBlockBufferFlush(FlushReason.SHUTDOWN);
    scheduledExecutor.shutdown();
    threadPoolExecutor.shutdown();
  }
}