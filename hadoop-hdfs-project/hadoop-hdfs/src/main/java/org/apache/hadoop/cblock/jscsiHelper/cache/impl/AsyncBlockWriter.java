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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.cblock.jscsiHelper.cache.LogicalBlock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.LevelDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Queue that is used to write blocks asynchronously to the container.
 */
public class AsyncBlockWriter {
  private static final Logger LOG =
      LoggerFactory.getLogger(AsyncBlockWriter.class);

  /**
   * XceiverClientManager is used to get client connections to a set of
   * machines.
   */
  private final XceiverClientManager xceiverClientManager;

  /**
   * This lock is used as a signal to re-queuing thread. The requeue thread
   * wakes up as soon as it is signaled some blocks are in the retry queue.
   * We try really aggressively since this new block will automatically move
   * to the end of the queue.
   * <p>
   * In the event a container is unavailable for a long time, we can either
   * fail all writes or remap and let the writes succeed. The easier
   * semantics is to fail the volume until the container is recovered by SCM.
   */
  private final Lock lock;
  private final Condition notEmpty;
  /**
   * The cache this writer is operating against.
   */
  private final CBlockLocalCache parentCache;
  private final BlockBufferManager blockBufferManager;
  public final static String DIRTY_LOG_PREFIX = "DirtyLog";
  public static final String RETRY_LOG_PREFIX = "RetryLog";
  private AtomicLong localIoCount;

  /**
   * Constructs an Async Block Writer.
   *
   * @param config - Config
   * @param cache - Parent Cache for this writer
   */
  public AsyncBlockWriter(Configuration config, CBlockLocalCache cache) {

    Preconditions.checkNotNull(cache, "Cache cannot be null.");
    Preconditions.checkNotNull(cache.getCacheDB(), "DB cannot be null.");
    localIoCount = new AtomicLong();
    lock = new ReentrantLock();
    notEmpty = lock.newCondition();
    parentCache = cache;
    xceiverClientManager = cache.getClientManager();
    blockBufferManager = new BlockBufferManager(config, parentCache);
  }

  public void start() throws IOException {
    File logDir = new File(parentCache.getDbPath().toString());
    if (!logDir.exists() && !logDir.mkdirs()) {
      LOG.error("Unable to create the log directory, Critical error cannot " +
          "continue. Log Dir : {}", logDir);
      throw new IllegalStateException("Cache Directory create failed, Cannot " +
          "continue. Log Dir: {}" + logDir);
    }
    blockBufferManager.start();
  }

  /**
   * Return the log to write to.
   *
   * @return Logger.
   */
  public static Logger getLOG() {
    return LOG;
  }

  /**
   * Get the CacheDB.
   *
   * @return LevelDB Handle
   */
  LevelDBStore getCacheDB() {
    return parentCache.getCacheDB();
  }

  /**
   * Returns the client manager.
   *
   * @return XceiverClientManager
   */
  XceiverClientManager getXceiverClientManager() {
    return xceiverClientManager;
  }

  /**
   * Incs the localIoPacket Count that has gone into this device.
   */
  public long incrementLocalIO() {
    return localIoCount.incrementAndGet();
  }

  /**
   * Return the local io counts to this device.
   * @return the count of io
   */
  public long getLocalIOCount() {
    return localIoCount.get();
  }

  /**
   * Writes a block to LevelDB store and queues a work item for the system to
   * sync the block to containers.
   *
   * @param block - Logical Block
   */
  public void writeBlock(LogicalBlock block) throws IOException {
    byte[] keybuf = Longs.toByteArray(block.getBlockID());
    String traceID = parentCache.getTraceID(block.getBlockID());
    if (parentCache.isShortCircuitIOEnabled()) {
      long startTime = Time.monotonicNow();
      getCacheDB().put(keybuf, block.getData().array());
      incrementLocalIO();
      long endTime = Time.monotonicNow();
      parentCache.getTargetMetrics().updateDBWriteLatency(
                                                    endTime - startTime);
      if (parentCache.isTraceEnabled()) {
        String datahash = DigestUtils.sha256Hex(block.getData().array());
        parentCache.getTracer().info(
            "Task=WriterTaskDBPut,BlockID={},Time={},SHA={}",
            block.getBlockID(), endTime - startTime, datahash);
      }
      block.clearData();
      blockBufferManager.addToBlockBuffer(block.getBlockID());
    } else {
      Pipeline pipeline = parentCache.getPipeline(block.getBlockID());
      String containerName = pipeline.getContainerName();
      XceiverClientSpi client = null;
      try {
        long startTime = Time.monotonicNow();
        client = parentCache.getClientManager()
            .acquireClient(parentCache.getPipeline(block.getBlockID()));
        ContainerProtocolCalls.writeSmallFile(client, containerName,
            Long.toString(block.getBlockID()), block.getData().array(),
            traceID);
        long endTime = Time.monotonicNow();
        if (parentCache.isTraceEnabled()) {
          String datahash = DigestUtils.sha256Hex(block.getData().array());
          parentCache.getTracer().info(
              "Task=DirectWriterPut,BlockID={},Time={},SHA={}",
              block.getBlockID(), endTime - startTime, datahash);
        }
        parentCache.getTargetMetrics().
            updateDirectBlockWriteLatency(endTime - startTime);
        parentCache.getTargetMetrics().incNumDirectBlockWrites();
      } catch (Exception ex) {
        parentCache.getTargetMetrics().incNumFailedDirectBlockWrites();
        LOG.error("Direct I/O writing of block:{} traceID:{} to "
            + "container {} failed", block.getBlockID(), traceID,
            containerName, ex);
        throw ex;
      } finally {
        if (client != null) {
          parentCache.getClientManager().releaseClient(client);
        }
        block.clearData();
      }
    }
  }

  /**
   * Shutdown by writing any pending I/O to dirtylog buffer.
   */
  public void shutdown() {
    blockBufferManager.shutdown();
  }
  /**
   * Returns tracer.
   *
   * @return Tracer
   */
  Logger getTracer() {
    return parentCache.getTracer();
  }

}
