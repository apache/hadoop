/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.cblock;

import com.google.common.primitives.Longs;
import static java.util.concurrent.TimeUnit.SECONDS;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.cblock.jscsiHelper.CBlockTargetMetrics;
import org.apache.hadoop.cblock.jscsiHelper.ContainerCacheFlusher;
import org.apache.hadoop.cblock.jscsiHelper.cache.impl.CBlockLocalCache;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeoutException;


import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_DISK_CACHE_PATH_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_TRACE_IO;
import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO;
import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_BLOCK_BUFFER_FLUSH_INTERVAL;

/**
 * Tests for Local Cache Buffer Manager.
 */
public class TestBufferManager {
  private final static long GB = 1024 * 1024 * 1024;
  private final static int KB = 1024;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration config;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static XceiverClientManager xceiverClientManager;

  @BeforeClass
  public static void init() throws IOException {
    config = new OzoneConfiguration();
    String path = GenericTestUtils.getTempPath(
        TestBufferManager.class.getSimpleName());
    config.set(DFS_CBLOCK_DISK_CACHE_PATH_KEY, path);
    config.setBoolean(DFS_CBLOCK_TRACE_IO, true);
    config.setBoolean(DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO, true);
    cluster = new MiniOzoneClassicCluster.Builder(config)
        .numDataNodes(1).setHandlerType("distributed").build();
    storageContainerLocationClient = cluster
        .createStorageContainerLocationClient();
    xceiverClientManager = new XceiverClientManager(config);
  }

  @AfterClass
  public static void shutdown() throws InterruptedException {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient, cluster);
  }

  /**
   * createContainerAndGetPipeline creates a set of containers and returns the
   * Pipelines that define those containers.
   *
   * @param count - Number of containers to create.
   * @return - List of Pipelines.
   * @throws IOException
   */
  private List<Pipeline> createContainerAndGetPipeline(int count)
      throws IOException {
    List<Pipeline> containerPipelines = new LinkedList<>();
    for (int x = 0; x < count; x++) {
      String traceID = "trace" + RandomStringUtils.randomNumeric(4);
      String containerName = "container" + RandomStringUtils.randomNumeric(10);
      Pipeline pipeline =
          storageContainerLocationClient.allocateContainer(
              xceiverClientManager.getType(),
              xceiverClientManager.getFactor(), containerName, "CBLOCK");
      XceiverClientSpi client = xceiverClientManager.acquireClient(pipeline);
      ContainerProtocolCalls.createContainer(client, traceID);
      // This step is needed since we set private data on pipelines, when we
      // read the list from CBlockServer. So we mimic that action here.
      pipeline.setData(Longs.toByteArray(x));
      containerPipelines.add(pipeline);
      xceiverClientManager.releaseClient(client);
    }
    return containerPipelines;
  }

  /**
   * This test writes some block to the cache and then shuts down the cache.
   * The cache is then restarted to check that the
   * correct number of blocks are read from Dirty Log
   *
   * @throws IOException
   */
  @Test
  public void testEmptyBlockBufferHandling() throws IOException,
      InterruptedException, TimeoutException {
    // Create a new config so that this tests write metafile to new location
    OzoneConfiguration flushTestConfig = new OzoneConfiguration();
    String path = GenericTestUtils
        .getTempPath(TestBufferManager.class.getSimpleName()
            + RandomStringUtils.randomNumeric(4));
    flushTestConfig.set(DFS_CBLOCK_DISK_CACHE_PATH_KEY, path);
    flushTestConfig.setBoolean(DFS_CBLOCK_TRACE_IO, true);
    flushTestConfig.setBoolean(DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO, true);

    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);
    String data = RandomStringUtils.random(4 * KB);
    List<Pipeline> pipelines = createContainerAndGetPipeline(10);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(flushTestConfig,
        xceiverClientManager, metrics);
    CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(flushTestConfig)
        .setVolumeName(volumeName)
        .setUserName(userName)
        .setPipelines(pipelines)
        .setClientManager(xceiverClientManager)
        .setBlockSize(4 * KB)
        .setVolumeSize(50 * GB)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    cache.start();
    // Write data to the cache
    cache.put(1, data.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(0, metrics.getNumDirectBlockWrites());
    Assert.assertEquals(1, metrics.getNumWriteOps());
    cache.put(2, data.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(0, metrics.getNumDirectBlockWrites());
    Assert.assertEquals(2, metrics.getNumWriteOps());

    // Store the previous block buffer position
    Assert.assertEquals(2, metrics.getNumBlockBufferUpdates());
    // Simulate a shutdown by closing the cache
    cache.close();
    Thread.sleep(1000);
    Assert.assertEquals(1, metrics.getNumBlockBufferFlushTriggered());
    Assert.assertEquals(1, metrics.getNumBlockBufferFlushCompleted());
    Assert.assertEquals(2 * (Long.SIZE/ Byte.SIZE),
                                metrics.getNumBytesDirtyLogWritten());
    Assert.assertEquals(0, metrics.getNumFailedBlockBufferFlushes());
    Assert.assertEquals(0, metrics.getNumInterruptedBufferWaits());

    // Restart cache and check that right number of entries are read
    CBlockTargetMetrics newMetrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher newFlusher =
        new ContainerCacheFlusher(flushTestConfig,
            xceiverClientManager, newMetrics);
    CBlockLocalCache newCache = CBlockLocalCache.newBuilder()
        .setConfiguration(flushTestConfig)
        .setVolumeName(volumeName)
        .setUserName(userName)
        .setPipelines(pipelines)
        .setClientManager(xceiverClientManager)
        .setBlockSize(4 * KB)
        .setVolumeSize(50 * GB)
        .setFlusher(newFlusher)
        .setCBlockTargetMetrics(newMetrics)
        .build();
    newCache.start();
    Thread fllushListenerThread = new Thread(newFlusher);
    fllushListenerThread.setDaemon(true);
    fllushListenerThread.start();

    Thread.sleep(5000);
    Assert.assertEquals(metrics.getNumBlockBufferUpdates(),
                                      newMetrics.getNumDirtyLogBlockRead());
    Assert.assertEquals(newMetrics.getNumDirtyLogBlockRead()
            * (Long.SIZE/ Byte.SIZE), newMetrics.getNumBytesDirtyLogReads());
    // Now shutdown again, nothing should be flushed
    newFlusher.shutdown();
    Assert.assertEquals(0, newMetrics.getNumBlockBufferUpdates());
    Assert.assertEquals(0, newMetrics.getNumBytesDirtyLogWritten());
  }

  @Test
  public void testPeriodicFlush() throws IOException,
      InterruptedException, TimeoutException{
    // Create a new config so that this tests write metafile to new location
    OzoneConfiguration flushTestConfig = new OzoneConfiguration();
    String path = GenericTestUtils
        .getTempPath(TestBufferManager.class.getSimpleName()
            + RandomStringUtils.randomNumeric(4));
    flushTestConfig.set(DFS_CBLOCK_DISK_CACHE_PATH_KEY, path);
    flushTestConfig.setBoolean(DFS_CBLOCK_TRACE_IO, true);
    flushTestConfig.setBoolean(DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO, true);
    flushTestConfig
        .setTimeDuration(DFS_CBLOCK_BLOCK_BUFFER_FLUSH_INTERVAL, 5, SECONDS);

    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(flushTestConfig,
        xceiverClientManager, metrics);
    CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(flushTestConfig)
        .setVolumeName(volumeName)
        .setUserName(userName)
        .setPipelines(createContainerAndGetPipeline(10))
        .setClientManager(xceiverClientManager)
        .setBlockSize(4 * KB)
        .setVolumeSize(50 * GB)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    cache.start();
    Thread.sleep(8000);
    // Ticks will be at 5s, 10s and so on, so this count should be 1
    Assert.assertEquals(1, metrics.getNumBlockBufferFlushTriggered());
    // Nothing pushed to cache, so nothing should be written
    Assert.assertEquals(0, metrics.getNumBytesDirtyLogWritten());
    Assert.assertEquals(0, metrics.getNumBlockBufferFlushCompleted());
    cache.close();
    // After close, another trigger should happen but still no data written
    Assert.assertEquals(2, metrics.getNumBlockBufferFlushTriggered());
    Assert.assertEquals(0, metrics.getNumBytesDirtyLogWritten());
    Assert.assertEquals(0, metrics.getNumBlockBufferFlushCompleted());
    Assert.assertEquals(0, metrics.getNumFailedBlockBufferFlushes());
  }

  @Test
  public void testSingleBufferFlush() throws IOException,
      InterruptedException, TimeoutException {
    // Create a new config so that this tests write metafile to new location
    OzoneConfiguration flushTestConfig = new OzoneConfiguration();
    String path = GenericTestUtils
        .getTempPath(TestBufferManager.class.getSimpleName()
            + RandomStringUtils.randomNumeric(4));
    flushTestConfig.set(DFS_CBLOCK_DISK_CACHE_PATH_KEY, path);
    flushTestConfig.setBoolean(DFS_CBLOCK_TRACE_IO, true);
    flushTestConfig.setBoolean(DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO, true);

    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);
    String data = RandomStringUtils.random(4 * KB);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(flushTestConfig,
        xceiverClientManager, metrics);
    CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(flushTestConfig)
        .setVolumeName(volumeName)
        .setUserName(userName)
        .setPipelines(createContainerAndGetPipeline(10))
        .setClientManager(xceiverClientManager)
        .setBlockSize(4 * KB)
        .setVolumeSize(50 * GB)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    cache.start();

    for (int i = 0; i < 511; i++) {
      cache.put(i, data.getBytes(StandardCharsets.UTF_8));
    }
    // After writing 511 block no flush should happen
    Assert.assertEquals(0, metrics.getNumBlockBufferFlushTriggered());
    Assert.assertEquals(0, metrics.getNumBlockBufferFlushCompleted());


    // After one more block it should
    cache.put(512, data.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(1, metrics.getNumBlockBufferFlushTriggered());
    Thread.sleep(1000);
    Assert.assertEquals(1, metrics.getNumBlockBufferFlushCompleted());
    cache.close();
    Assert.assertEquals(512 * (Long.SIZE / Byte.SIZE),
                                metrics.getNumBytesDirtyLogWritten());
  }

  @Test
  public void testMultipleBuffersFlush() throws IOException,
      InterruptedException, TimeoutException {
    // Create a new config so that this tests write metafile to new location
    OzoneConfiguration flushTestConfig = new OzoneConfiguration();
    String path = GenericTestUtils
        .getTempPath(TestBufferManager.class.getSimpleName()
            + RandomStringUtils.randomNumeric(4));
    flushTestConfig.set(DFS_CBLOCK_DISK_CACHE_PATH_KEY, path);
    flushTestConfig.setBoolean(DFS_CBLOCK_TRACE_IO, true);
    flushTestConfig.setBoolean(DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO, true);
    flushTestConfig
        .setTimeDuration(DFS_CBLOCK_BLOCK_BUFFER_FLUSH_INTERVAL, 120, SECONDS);

    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);
    String data = RandomStringUtils.random(4 * KB);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(flushTestConfig,
        xceiverClientManager, metrics);
    CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(flushTestConfig)
        .setVolumeName(volumeName)
        .setUserName(userName)
        .setPipelines(createContainerAndGetPipeline(10))
        .setClientManager(xceiverClientManager)
        .setBlockSize(4 * KB)
        .setVolumeSize(50 * GB)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    cache.start();

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 512; j++) {
        cache.put(i * 512 + j, data.getBytes(StandardCharsets.UTF_8));
      }
      // Flush should be triggered after every 512 block write
      Assert.assertEquals(i + 1, metrics.getNumBlockBufferFlushTriggered());
    }
    Assert.assertEquals(0, metrics.getNumIllegalDirtyLogFiles());
    Assert.assertEquals(0, metrics.getNumFailedDirtyLogFileDeletes());
    cache.close();
    Assert.assertEquals(4 * 512 * (Long.SIZE / Byte.SIZE),
        metrics.getNumBytesDirtyLogWritten());
    Assert.assertEquals(5, metrics.getNumBlockBufferFlushTriggered());
    Assert.assertEquals(4, metrics.getNumBlockBufferFlushCompleted());
  }

  @Test
  public void testSingleBlockFlush() throws IOException,
      InterruptedException, TimeoutException{
    // Create a new config so that this tests write metafile to new location
    OzoneConfiguration flushTestConfig = new OzoneConfiguration();
    String path = GenericTestUtils
        .getTempPath(TestBufferManager.class.getSimpleName()
            + RandomStringUtils.randomNumeric(4));
    flushTestConfig.set(DFS_CBLOCK_DISK_CACHE_PATH_KEY, path);
    flushTestConfig.setBoolean(DFS_CBLOCK_TRACE_IO, true);
    flushTestConfig.setBoolean(DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO, true);
    flushTestConfig
        .setTimeDuration(DFS_CBLOCK_BLOCK_BUFFER_FLUSH_INTERVAL,
            5, SECONDS);

    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);
    String data = RandomStringUtils.random(4 * KB);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(flushTestConfig,
        xceiverClientManager, metrics);
    CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(flushTestConfig)
        .setVolumeName(volumeName)
        .setUserName(userName)
        .setPipelines(createContainerAndGetPipeline(10))
        .setClientManager(xceiverClientManager)
        .setBlockSize(4 * KB)
        .setVolumeSize(50 * GB)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    cache.start();
    cache.put(0, data.getBytes(StandardCharsets.UTF_8));
    Thread.sleep(8000);
    // Ticks will be at 5s, 10s and so on, so this count should be 1
    Assert.assertEquals(1, metrics.getNumBlockBufferFlushTriggered());
    // 1 block written to cache, which should be flushed
    Assert.assertEquals(8, metrics.getNumBytesDirtyLogWritten());
    Assert.assertEquals(1, metrics.getNumBlockBufferFlushCompleted());
    cache.close();
    // After close, another trigger should happen but no data should be written
    Assert.assertEquals(2, metrics.getNumBlockBufferFlushTriggered());
    Assert.assertEquals(8, metrics.getNumBytesDirtyLogWritten());
    Assert.assertEquals(1, metrics.getNumBlockBufferFlushCompleted());
    Assert.assertEquals(0, metrics.getNumFailedBlockBufferFlushes());
  }

  @Test
  public void testRepeatedBlockWrites() throws IOException,
      InterruptedException, TimeoutException{
    // Create a new config so that this tests write metafile to new location
    OzoneConfiguration flushTestConfig = new OzoneConfiguration();
    String path = GenericTestUtils
        .getTempPath(TestBufferManager.class.getSimpleName()
            + RandomStringUtils.randomNumeric(4));
    flushTestConfig.set(DFS_CBLOCK_DISK_CACHE_PATH_KEY, path);
    flushTestConfig.setBoolean(DFS_CBLOCK_TRACE_IO, true);
    flushTestConfig.setBoolean(DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO, true);

    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);
    String data = RandomStringUtils.random(4 * KB);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(flushTestConfig,
        xceiverClientManager, metrics);
    CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(flushTestConfig)
        .setVolumeName(volumeName)
        .setUserName(userName)
        .setPipelines(createContainerAndGetPipeline(10))
        .setClientManager(xceiverClientManager)
        .setBlockSize(4 * KB)
        .setVolumeSize(50 * GB)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    Thread fllushListenerThread = new Thread(flusher);
    fllushListenerThread.setDaemon(true);
    fllushListenerThread.start();
    cache.start();
    for (int i = 0; i < 512; i++) {
      cache.put(i, data.getBytes(StandardCharsets.UTF_8));
    }
    Assert.assertEquals(512, metrics.getNumWriteOps());
    Assert.assertEquals(512, metrics.getNumBlockBufferUpdates());
    Assert.assertEquals(1, metrics.getNumBlockBufferFlushTriggered());
    Thread.sleep(5000);
    Assert.assertEquals(1, metrics.getNumBlockBufferFlushCompleted());


    for (int i = 0; i < 512; i++) {
      cache.put(i, data.getBytes(StandardCharsets.UTF_8));
    }
    Assert.assertEquals(1024, metrics.getNumWriteOps());
    Assert.assertEquals(1024, metrics.getNumBlockBufferUpdates());
    Assert.assertEquals(2, metrics.getNumBlockBufferFlushTriggered());

    Thread.sleep(5000);
    Assert.assertEquals(0, metrics.getNumWriteIOExceptionRetryBlocks());
    Assert.assertEquals(0, metrics.getNumWriteGenericExceptionRetryBlocks());
    Assert.assertEquals(2, metrics.getNumBlockBufferFlushCompleted());
  }
}