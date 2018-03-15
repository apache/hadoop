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

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.cblock.jscsiHelper.cache.LogicalBlock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopThreadPoolExecutor;
import org.apache.hadoop.utils.LevelDBStore;
import org.iq80.leveldb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Reads blocks from the container via the local cache.
 */
public class SyncBlockReader {
  private static final Logger LOG =
      LoggerFactory.getLogger(SyncBlockReader.class);

  /**
   * Update Queue - The reason why we have the queue is that we want to
   * return the block as soon as we read it from the containers. This queue
   * is work queue which will take the read block and update the cache.
   * During testing we found levelDB is slow during writes, hence we wanted
   * to return as block as soon as possible and update levelDB asynchronously.
   */
  private final static int QUEUE_SIZE = 1024;
  /**
   * Config.
   */
  private final Configuration conf;
  /**
   * The parent cache this reader is operating against.
   */
  private final CBlockLocalCache parentCache;
  private final BlockingQueue<Runnable> updateQueue;

  /**
   * executor is used for running LevelDB updates. In future, we might do
   * read-aheads and this pool is useful for that too. The reason why we
   * don't share an executor for reads and writes is because the write task
   * is couple of magnitude slower than read task. So we don't want the
   * update DB to queue up behind the writes.
   */
  private final ThreadPoolExecutor executor;

  /**
   * Number of threads that pool starts with.
   */
  private final int corePoolSize = 1;
  /**
   * Maximum number of threads our pool will ever create.
   */
  private final int maxPoolSize = 10;
  /**
   * The idle time a thread hangs around waiting for work. if we don't find
   * new work in 60 seconds the worker thread is killed.
   */
  private final long keepAlive = 60L;

  /**
   * Constructs a SyncBlock reader.
   *
   * @param conf - Configuration
   * @param cache - Cache
   */
  public SyncBlockReader(Configuration conf, CBlockLocalCache cache) {
    this.conf = conf;
    this.parentCache = cache;
    updateQueue = new ArrayBlockingQueue<>(QUEUE_SIZE, true);
    ThreadFactory workerThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("SyncBlockReader Thread #%d").setDaemon(true).build();
    executor = new HadoopThreadPoolExecutor(
        corePoolSize, maxPoolSize, keepAlive, TimeUnit.SECONDS,
        updateQueue, workerThreadFactory,
        new ThreadPoolExecutor.CallerRunsPolicy());
  }

  /**
   * Returns the cache DB.
   *
   * @return LevelDB
   */
  private LevelDBStore getCacheDB() {
    return parentCache.getCacheDB();
  }

  /**
   * Returns data from the local cache if found, else reads from the remote
   * container.
   *
   * @param blockID - blockID
   * @return LogicalBlock
   */
  LogicalBlock readBlock(long blockID) throws IOException {
    XceiverClientSpi client = null;
    byte[] data = getblockFromDB(blockID);
    if (data != null) {
      parentCache.getTargetMetrics().incNumReadCacheHits();
      return new DiskBlock(blockID, data, false);
    }

    parentCache.getTargetMetrics().incNumReadCacheMiss();
    try {
      client = parentCache.getClientManager()
          .acquireClient(parentCache.getPipeline(blockID));
      LogicalBlock block = getBlockFromContainer(blockID, client);
      return block;
    } catch (Exception ex) {
      parentCache.getTargetMetrics().incNumFailedReadBlocks();
      LOG.error("read failed for BlockId: {}", blockID, ex);
      throw ex;
    } finally {
      if (client != null) {
        parentCache.getClientManager().releaseClient(client);
      }
    }
  }

  /**
   * Gets data from the DB if it exists.
   *
   * @param blockID - block id
   * @return data
   */
  private byte[] getblockFromDB(long blockID) {
    try {
      if(parentCache.isShortCircuitIOEnabled()) {
        long startTime = Time.monotonicNow();
        byte[] data = getCacheDB().get(Longs.toByteArray(blockID));
        long endTime = Time.monotonicNow();

        if (parentCache.isTraceEnabled()) {
          parentCache.getTracer().info(
              "Task=ReadTaskDBRead,BlockID={},SHA={},Time={}",
              blockID, (data != null && data.length > 0)
                  ? DigestUtils.sha256Hex(data) : null,
              endTime - startTime);
        }
        parentCache.getTargetMetrics().updateDBReadLatency(
            endTime - startTime);
        return data;
      }


    } catch (DBException dbe) {
      LOG.error("Error while reading from cacheDB.", dbe);
      throw dbe;
    }
    return null;
  }


  /**
   * Returns a block from a Remote Container. if the key is not found on a
   * remote container we just return a block initialzied with zeros.
   *
   * @param blockID - blockID
   * @param client - client
   * @return LogicalBlock
   * @throws IOException
   */
  private LogicalBlock getBlockFromContainer(long blockID,
      XceiverClientSpi client) throws IOException {
    String containerName = parentCache.getPipeline(blockID).getContainerName();
    try {
      long startTime = Time.monotonicNow();
      ContainerProtos.GetSmallFileResponseProto response =
          ContainerProtocolCalls.readSmallFile(client, containerName,
              Long.toString(blockID), parentCache.getTraceID(blockID));
      long endTime = Time.monotonicNow();
      if (parentCache.isTraceEnabled()) {
        parentCache.getTracer().info(
            "Task=ReadTaskContainerRead,BlockID={},SHA={},Time={}",
            blockID, response.getData().getData().toByteArray().length > 0 ?
                DigestUtils.sha256Hex(response.getData()
                    .getData().toByteArray()) : null, endTime - startTime);
      }

      parentCache.getTargetMetrics().updateContainerReadLatency(
          endTime - startTime);
      DiskBlock block = new DiskBlock(blockID,
          response.getData().getData().toByteArray(), false);

      if(parentCache.isShortCircuitIOEnabled()) {
        queueUpdateTask(block);
      }

      return block;
    } catch (IOException ex) {
      if (ex instanceof StorageContainerException) {
        parentCache.getTargetMetrics().incNumReadLostBlocks();
        StorageContainerException sce = (StorageContainerException) ex;
        if (sce.getResult() == ContainerProtos.Result.NO_SUCH_KEY ||
            sce.getResult() == ContainerProtos.Result.IO_EXCEPTION) {
          return new DiskBlock(blockID, new byte[parentCache.getBlockSize()],
              false);
        }
      }
      throw ex;
    }
  }

  /**
   * Updates the cache with the block that we just read.
   *
   * @param block
   */
  private void queueUpdateTask(final DiskBlock block) {
    Runnable updateTask = () -> {
      if(block.getData().array().length > 0) {
        getCacheDB().put(Longs.toByteArray(block.getBlockID()),
            block.getData().array());
        block.setPersisted(true);
      } else {
        LOG.error("Refusing to update the a null block in the local cache.");
      }
    };
    if (this.executor.isShutdown() || this.executor.isTerminated()) {
      LOG.error("Thread executor is not running.");
    } else {
      this.executor.submit(updateTask);
    }
  }

  /**
   * This is a read operation, we don't care if we updated the cache with the
   * last block e read.
   */
  void shutdown() {
    this.executor.shutdownNow();
  }
}
