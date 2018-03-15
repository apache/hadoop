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
package org.apache.hadoop.cblock.jscsiHelper;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.hadoop.cblock.jscsiHelper.cache.LogicalBlock;
import org.apache.hadoop.cblock.jscsiHelper.cache.impl.AsyncBlockWriter;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.LevelDBStore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

/**
 * The blockWriter task.
 */
public class BlockWriterTask implements Runnable {
  private final LogicalBlock block;
  private int tryCount;
  private final ContainerCacheFlusher flusher;
  private final String dbPath;
  private final String fileName;
  private final int maxRetryCount;

  /**
   * Constructs a BlockWriterTask.
   *
   * @param block - Block Information.
   * @param flusher - ContainerCacheFlusher.
   */
  public BlockWriterTask(LogicalBlock block, ContainerCacheFlusher flusher,
      String dbPath, int tryCount, String fileName, int maxRetryCount) {
    this.block = block;
    this.flusher = flusher;
    this.dbPath = dbPath;
    this.tryCount = tryCount;
    this.fileName = fileName;
    this.maxRetryCount = maxRetryCount;
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
    String containerName = null;
    XceiverClientSpi client = null;
    LevelDBStore levelDBStore = null;
    String traceID = flusher.getTraceID(new File(dbPath), block.getBlockID());
    flusher.getLOG().debug(
        "Writing block to remote. block ID: {}", block.getBlockID());
    try {
      incTryCount();
      Pipeline pipeline = flusher.getPipeline(this.dbPath, block.getBlockID());
      client = flusher.getXceiverClientManager().acquireClient(pipeline);
      containerName = pipeline.getContainerName();
      byte[] keybuf = Longs.toByteArray(block.getBlockID());
      byte[] data;
      long startTime = Time.monotonicNow();
      levelDBStore = flusher.getCacheDB(this.dbPath);
      data = levelDBStore.get(keybuf);
      Preconditions.checkNotNull(data);
      long endTime = Time.monotonicNow();
      Preconditions.checkState(data.length > 0, "Block data is zero length");
      startTime = Time.monotonicNow();
      ContainerProtocolCalls.writeSmallFile(client, containerName,
          Long.toString(block.getBlockID()), data, traceID);
      endTime = Time.monotonicNow();
      flusher.getTargetMetrics().updateContainerWriteLatency(
          endTime - startTime);
      flusher.getLOG().debug("Time taken for Write Small File : {} ms",
          endTime - startTime);

      flusher.incrementRemoteIO();

    } catch (Exception ex) {
      flusher.getLOG().error("Writing of block:{} failed, We have attempted " +
              "to write this block {} times to the container {}.Trace ID:{}",
          block.getBlockID(), this.getTryCount(), containerName, traceID, ex);
      writeRetryBlock(block);
      if (ex instanceof IOException) {
        flusher.getTargetMetrics().incNumWriteIOExceptionRetryBlocks();
      } else {
        flusher.getTargetMetrics().incNumWriteGenericExceptionRetryBlocks();
      }
      if (this.getTryCount() >= maxRetryCount) {
        flusher.getTargetMetrics().incNumWriteMaxRetryBlocks();
      }
    } finally {
      flusher.incFinishCount(fileName);
      if (levelDBStore != null) {
        flusher.releaseCacheDB(dbPath);
      }
      if(client != null) {
        flusher.getXceiverClientManager().releaseClient(client);
      }
    }
  }


  private void writeRetryBlock(LogicalBlock currentBlock) {
    boolean append = false;
    String retryFileName =
        String.format("%s.%d.%s.%s", AsyncBlockWriter.RETRY_LOG_PREFIX,
            currentBlock.getBlockID(), Time.monotonicNow(), tryCount);
    File logDir = new File(this.dbPath);
    if (!logDir.exists() && !logDir.mkdirs()) {
      flusher.getLOG().error(
          "Unable to create the log directory, Critical error cannot continue");
      return;
    }
    String log = Paths.get(this.dbPath, retryFileName).toString();
    ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE / Byte.SIZE);
    buffer.putLong(currentBlock.getBlockID());
    buffer.flip();
    try {
      FileChannel channel = new FileOutputStream(log, append).getChannel();
      channel.write(buffer);
      channel.close();
      flusher.processDirtyBlocks(this.dbPath, retryFileName);
    } catch (IOException e) {
      flusher.getTargetMetrics().incNumFailedRetryLogFileWrites();
      flusher.getLOG().error("Unable to write the retry block. Block ID: {}",
          currentBlock.getBlockID(), e);
    }
  }

  /**
   * Increments the try count. This is done each time we try this block
   * write to the container.
   */
  private void incTryCount() {
    tryCount++;
  }

  /**
   * Get the retry count.
   *
   * @return int
   */
  public int getTryCount() {
    return tryCount;
  }
}
