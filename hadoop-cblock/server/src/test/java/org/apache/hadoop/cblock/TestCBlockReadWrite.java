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
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.cblock.jscsiHelper.CBlockTargetMetrics;
import org.apache.hadoop.cblock.jscsiHelper.ContainerCacheFlusher;
import org.apache.hadoop.cblock.jscsiHelper.cache.LogicalBlock;
import org.apache.hadoop.cblock.jscsiHelper.cache.impl.CBlockLocalCache;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos.LifeCycleState;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos.ReplicationFactor;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos.ReplicationType;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.container.common.helpers.PipelineChannel;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_DISK_CACHE_PATH_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_TRACE_IO;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_BLOCK_BUFFER_FLUSH_INTERVAL;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE;

/**
 * Tests for Cblock read write functionality.
 */
public class TestCBlockReadWrite {
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
    String path = GenericTestUtils
        .getTempPath(TestCBlockReadWrite.class.getSimpleName());
    config.set(DFS_CBLOCK_DISK_CACHE_PATH_KEY, path);
    config.setBoolean(DFS_CBLOCK_TRACE_IO, true);
    config.setBoolean(DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO, true);
    cluster = new MiniOzoneClassicCluster.Builder(config)
        .numDataNodes(1)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
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
   * getContainerPipelines creates a set of containers and returns the
   * Pipelines that define those containers.
   *
   * @param count - Number of containers to create.
   * @return - List of Pipelines.
   * @throws IOException throws Exception
   */
  private List<Pipeline> getContainerPipeline(int count) throws IOException {
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
    }
    return containerPipelines;
  }

  /**
   * This test creates a cache and performs a simple write / read.
   * The operations are done by bypassing the cache.
   *
   * @throws IOException
   */
  @Test
  public void testDirectIO() throws IOException,
      InterruptedException, TimeoutException {
    OzoneConfiguration cConfig = new OzoneConfiguration();
    cConfig.setBoolean(DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO, false);
    cConfig.setBoolean(DFS_CBLOCK_TRACE_IO, true);
    final long blockID = 0;
    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);
    String data = RandomStringUtils.random(4 * KB);
    String dataHash = DigestUtils.sha256Hex(data);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(cConfig,
        xceiverClientManager, metrics);
    CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(cConfig)
        .setVolumeName(volumeName)
        .setUserName(userName)
        .setPipelines(getContainerPipeline(10))
        .setClientManager(xceiverClientManager)
        .setBlockSize(4 * KB)
        .setVolumeSize(50 * GB)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    cache.start();
    Assert.assertFalse(cache.isShortCircuitIOEnabled());
    cache.put(blockID, data.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(1, metrics.getNumDirectBlockWrites());
    Assert.assertEquals(1, metrics.getNumWriteOps());
    // Please note that this read is directly from remote container
    LogicalBlock block = cache.get(blockID);
    Assert.assertEquals(1, metrics.getNumReadOps());
    Assert.assertEquals(0, metrics.getNumReadCacheHits());
    Assert.assertEquals(1, metrics.getNumReadCacheMiss());
    Assert.assertEquals(0, metrics.getNumReadLostBlocks());
    Assert.assertEquals(0, metrics.getNumFailedDirectBlockWrites());

    cache.put(blockID + 1, data.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(2, metrics.getNumDirectBlockWrites());
    Assert.assertEquals(2, metrics.getNumWriteOps());
    Assert.assertEquals(0, metrics.getNumFailedDirectBlockWrites());
    // Please note that this read is directly from remote container
    block = cache.get(blockID + 1);
    Assert.assertEquals(2, metrics.getNumReadOps());
    Assert.assertEquals(0, metrics.getNumReadCacheHits());
    Assert.assertEquals(2, metrics.getNumReadCacheMiss());
    Assert.assertEquals(0, metrics.getNumReadLostBlocks());
    String readHash = DigestUtils.sha256Hex(block.getData().array());
    Assert.assertEquals("File content does not match.", dataHash, readHash);
    GenericTestUtils.waitFor(() -> !cache.isDirtyCache(), 100, 20 * 1000);
    cache.close();
  }

  /**
   * This test writes some block to the cache and then shuts down the cache
   * The cache is then restarted with "short.circuit.io" disable to check
   * that the blocks are read correctly from the container.
   *
   * @throws IOException
   */
  @Test
  public void testContainerWrites() throws IOException,
      InterruptedException, TimeoutException {
    // Create a new config so that this tests write metafile to new location
    OzoneConfiguration flushTestConfig = new OzoneConfiguration();
    String path = GenericTestUtils
        .getTempPath(TestCBlockReadWrite.class.getSimpleName());
    flushTestConfig.set(DFS_CBLOCK_DISK_CACHE_PATH_KEY, path);
    flushTestConfig.setBoolean(DFS_CBLOCK_TRACE_IO, true);
    flushTestConfig.setBoolean(DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO, true);
    flushTestConfig.setTimeDuration(DFS_CBLOCK_BLOCK_BUFFER_FLUSH_INTERVAL, 3,
        TimeUnit.SECONDS);
    XceiverClientManager xcm = new XceiverClientManager(flushTestConfig);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);

    int numUniqueBlocks = 4;
    String[] data = new String[numUniqueBlocks];
    String[] dataHash = new String[numUniqueBlocks];
    for (int i = 0; i < numUniqueBlocks; i++) {
      data[i] = RandomStringUtils.random(4 * KB);
      dataHash[i] = DigestUtils.sha256Hex(data[i]);
    }

    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(flushTestConfig,
        xcm, metrics);
    List<Pipeline> pipelines = getContainerPipeline(10);
    CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(flushTestConfig)
        .setVolumeName(volumeName)
        .setUserName(userName)
        .setPipelines(pipelines)
        .setClientManager(xcm)
        .setBlockSize(4 * KB)
        .setVolumeSize(50 * GB)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    cache.start();
    Thread flushListenerThread = new Thread(flusher);
    flushListenerThread.setDaemon(true);
    flushListenerThread.start();
    Assert.assertTrue(cache.isShortCircuitIOEnabled());
    // Write data to the cache
    for (int i = 0; i < 512; i++) {
      cache.put(i, data[i % numUniqueBlocks].getBytes(StandardCharsets.UTF_8));
    }
    // Close the cache and flush the data to the containers
    cache.close();
    Assert.assertEquals(0, metrics.getNumDirectBlockWrites());
    Assert.assertEquals(512, metrics.getNumWriteOps());
    Thread.sleep(3000);
    flusher.shutdown();
    Assert.assertTrue(metrics.getNumBlockBufferFlushTriggered() > 1);
    Assert.assertEquals(1, metrics.getNumBlockBufferFlushCompleted());
    Assert.assertEquals(0, metrics.getNumWriteIOExceptionRetryBlocks());
    Assert.assertEquals(0, metrics.getNumWriteGenericExceptionRetryBlocks());
    Assert.assertEquals(0, metrics.getNumFailedReleaseLevelDB());
    // Now disable DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO and restart cache
    flushTestConfig.setBoolean(DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO, false);
    CBlockTargetMetrics newMetrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher newFlusher =
        new ContainerCacheFlusher(flushTestConfig, xcm, newMetrics);
    CBlockLocalCache newCache = CBlockLocalCache.newBuilder()
        .setConfiguration(flushTestConfig)
        .setVolumeName(volumeName)
        .setUserName(userName)
        .setPipelines(pipelines)
        .setClientManager(xcm)
        .setBlockSize(4 * KB)
        .setVolumeSize(50 * GB)
        .setFlusher(newFlusher)
        .setCBlockTargetMetrics(newMetrics)
        .build();
    newCache.start();
    Assert.assertFalse(newCache.isShortCircuitIOEnabled());
    // this read will be from the container, also match the hash
    for (int i = 0; i < 512; i++) {
      LogicalBlock block = newCache.get(i);
      String readHash = DigestUtils.sha256Hex(block.getData().array());
      Assert.assertEquals("File content does not match, for index:"
          + i, dataHash[i % numUniqueBlocks], readHash);
    }
    Assert.assertEquals(0, newMetrics.getNumReadLostBlocks());
    Assert.assertEquals(0, newMetrics.getNumFailedReadBlocks());
    newCache.close();
    newFlusher.shutdown();
  }

  @Test
  public void testRetryLog() throws IOException,
      InterruptedException, TimeoutException {
    // Create a new config so that this tests write metafile to new location
    OzoneConfiguration flushTestConfig = new OzoneConfiguration();
    String path = GenericTestUtils
        .getTempPath(TestCBlockReadWrite.class.getSimpleName());
    flushTestConfig.set(DFS_CBLOCK_DISK_CACHE_PATH_KEY, path);
    flushTestConfig.setBoolean(DFS_CBLOCK_TRACE_IO, true);
    flushTestConfig.setBoolean(DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO, true);
    flushTestConfig.setTimeDuration(DFS_CBLOCK_BLOCK_BUFFER_FLUSH_INTERVAL,
        3,
        TimeUnit.SECONDS);

    int numblocks = 10;
    flushTestConfig.setInt(DFS_CBLOCK_CACHE_BLOCK_BUFFER_SIZE, numblocks);

    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);
    String data = RandomStringUtils.random(4 * KB);

    List<Pipeline> fakeContainerPipelines = new LinkedList<>();
    PipelineChannel pipelineChannel = new PipelineChannel("fake",
        LifeCycleState.OPEN, ReplicationType.STAND_ALONE, ReplicationFactor.ONE,
        "fake");
    Pipeline fakePipeline = new Pipeline("fake", pipelineChannel);
    fakePipeline.setData(Longs.toByteArray(1));
    fakeContainerPipelines.add(fakePipeline);

    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(flushTestConfig,
        xceiverClientManager, metrics);
    CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(flushTestConfig)
        .setVolumeName(volumeName)
        .setUserName(userName)
        .setPipelines(fakeContainerPipelines)
        .setClientManager(xceiverClientManager)
        .setBlockSize(4 * KB)
        .setVolumeSize(50 * GB)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    cache.start();
    Thread flushListenerThread = new Thread(flusher);
    flushListenerThread.setDaemon(true);
    flushListenerThread.start();

    for (int i = 0; i < numblocks; i++) {
      cache.put(i, data.getBytes(StandardCharsets.UTF_8));
    }
    Assert.assertEquals(numblocks, metrics.getNumWriteOps());
    Thread.sleep(3000);

    // all the writes to the container will fail because of fake pipelines
    Assert.assertEquals(numblocks, metrics.getNumDirtyLogBlockRead());
    Assert.assertTrue(
        metrics.getNumWriteGenericExceptionRetryBlocks() >= numblocks);
    Assert.assertEquals(0, metrics.getNumWriteIOExceptionRetryBlocks());
    Assert.assertEquals(0, metrics.getNumFailedRetryLogFileWrites());
    Assert.assertEquals(0, metrics.getNumFailedReleaseLevelDB());
    cache.close();
    flusher.shutdown();

    // restart cache with correct pipelines, now blocks should be uploaded
    // correctly
    CBlockTargetMetrics newMetrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher newFlusher =
        new ContainerCacheFlusher(flushTestConfig,
            xceiverClientManager, newMetrics);
    CBlockLocalCache newCache = CBlockLocalCache.newBuilder()
        .setConfiguration(flushTestConfig)
        .setVolumeName(volumeName)
        .setUserName(userName)
        .setPipelines(getContainerPipeline(10))
        .setClientManager(xceiverClientManager)
        .setBlockSize(4 * KB)
        .setVolumeSize(50 * GB)
        .setFlusher(newFlusher)
        .setCBlockTargetMetrics(newMetrics)
        .build();
    newCache.start();
    Thread newFlushListenerThread = new Thread(newFlusher);
    newFlushListenerThread.setDaemon(true);
    newFlushListenerThread.start();
    Thread.sleep(3000);
    Assert.assertTrue(newMetrics.getNumRetryLogBlockRead() >= numblocks);
    Assert.assertEquals(0, newMetrics.getNumWriteGenericExceptionRetryBlocks());
    Assert.assertEquals(0, newMetrics.getNumWriteIOExceptionRetryBlocks());
    Assert.assertEquals(0, newMetrics.getNumFailedReleaseLevelDB());
  }
}
