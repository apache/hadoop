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

import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

/**
 * This task is responsible for flushing the BlockIDBuffer
 * to Dirty Log File. This Dirty Log file is used later by
 * ContainerCacheFlusher when the data is written to container
 */
public class BlockBufferFlushTask implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(BlockBufferFlushTask.class);
  private final CBlockLocalCache parentCache;
  private final BlockBufferManager bufferManager;
  private final ByteBuffer blockIDBuffer;

  BlockBufferFlushTask(ByteBuffer blockIDBuffer, CBlockLocalCache parentCache,
                       BlockBufferManager manager) {
    this.parentCache = parentCache;
    this.bufferManager = manager;
    this.blockIDBuffer = blockIDBuffer;
  }

  /**
   * When an object implementing interface <code>Runnable</code> is used
   * to create a thread, starting the thread causes the object's
   * <code>run</code> method to be called in that separately executing
   * thread.
   * <p>
   * The general contract of the method <code>run</code> is that it may
   * take any action whatsoever.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    try {
      writeBlockBufferToFile(blockIDBuffer);
    } catch (Exception e) {
      parentCache.getTargetMetrics().incNumFailedBlockBufferFlushes();
      LOG.error("Unable to sync the Block map to disk with "
          + (blockIDBuffer.position() / Long.SIZE) + "entries "
          + "-- NOTE: This might cause a data loss or corruption", e);
    } finally {
      bufferManager.releaseBuffer(blockIDBuffer);
    }
  }

  /**
   * Write Block Buffer to file.
   *
   * @param buffer - ByteBuffer
   * @throws IOException
   */
  private void writeBlockBufferToFile(ByteBuffer buffer)
      throws IOException {
    long startTime = Time.monotonicNow();
    boolean append = false;

    // If there is nothing written to blockId buffer,
    // then skip flushing of blockId buffer
    if (buffer.position() == 0) {
      return;
    }

    buffer.flip();
    String fileName =
        String.format("%s.%s", AsyncBlockWriter.DIRTY_LOG_PREFIX,
                      Time.monotonicNow());
    String log = Paths.get(parentCache.getDbPath().toString(), fileName)
        .toString();

    FileChannel channel = new FileOutputStream(log, append).getChannel();
    int bytesWritten = channel.write(buffer);
    channel.close();
    buffer.clear();
    parentCache.processDirtyMessage(fileName);
    long endTime = Time.monotonicNow();
    if (parentCache.isTraceEnabled()) {
      parentCache.getTracer().info(
          "Task=DirtyBlockLogWrite,Time={} bytesWritten={}",
          endTime - startTime, bytesWritten);
    }

    parentCache.getTargetMetrics().incNumBlockBufferFlushCompleted();
    parentCache.getTargetMetrics().incNumBytesDirtyLogWritten(bytesWritten);
    parentCache.getTargetMetrics().
        updateBlockBufferFlushLatency(endTime - startTime);
    LOG.debug("Block buffer writer bytesWritten:{} Time:{}",
        bytesWritten, endTime - startTime);
  }
}