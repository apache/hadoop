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
import org.apache.hadoop.cblock.jscsiHelper.CBlockIStorageImpl;
import org.apache.hadoop.cblock.jscsiHelper.CBlockTargetMetrics;
import org.apache.hadoop.cblock.jscsiHelper.ContainerCacheFlusher;
import org.apache.hadoop.cblock.jscsiHelper.cache.LogicalBlock;
import org.apache.hadoop.cblock.jscsiHelper.cache.impl.CBlockLocalCache;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.Math.abs;
import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_DISK_CACHE_PATH_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO;
import static org.apache.hadoop.cblock.CBlockConfigKeys.
    DFS_CBLOCK_TRACE_IO;

/**
 * Tests for local cache.
 */
public class TestLocalBlockCache {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestLocalBlockCache.class);
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
        .getTempPath(TestLocalBlockCache.class.getSimpleName());
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
      xceiverClientManager.releaseClient(client);
    }
    return containerPipelines;
  }

  /**
   * This test creates a cache and performs a simple write / read.
   * Due to the cache - we have Read-after-write consistency for cBlocks.
   *
   * @throws IOException throws Exception
   */
  @Test
  public void testCacheWriteRead() throws IOException,
      InterruptedException, TimeoutException {
    final long blockID = 0;
    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);
    String data = RandomStringUtils.random(4 * KB);
    String dataHash = DigestUtils.sha256Hex(data);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(this.config,
        xceiverClientManager, metrics);
    CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(this.config)
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
    cache.put(blockID, data.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(1, metrics.getNumWriteOps());
    // Please note that this read is from the local cache.
    LogicalBlock block = cache.get(blockID);
    Assert.assertEquals(1, metrics.getNumReadOps());
    Assert.assertEquals(1, metrics.getNumReadCacheHits());
    Assert.assertEquals(0, metrics.getNumReadCacheMiss());
    Assert.assertEquals(0, metrics.getNumReadLostBlocks());

    cache.put(blockID + 1, data.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(2, metrics.getNumWriteOps());
    // Please note that this read is from the local cache.
    block = cache.get(blockID + 1);
    Assert.assertEquals(2, metrics.getNumReadOps());
    Assert.assertEquals(2, metrics.getNumReadCacheHits());
    Assert.assertEquals(0, metrics.getNumReadCacheMiss());
    Assert.assertEquals(0, metrics.getNumReadLostBlocks());
    String readHash = DigestUtils.sha256Hex(block.getData().array());
    Assert.assertEquals("File content does not match.", dataHash, readHash);
    GenericTestUtils.waitFor(() -> !cache.isDirtyCache(), 100, 20 * 1000);
    cache.close();

  }

  @Test
  public void testCacheWriteToRemoteContainer() throws IOException,
      InterruptedException, TimeoutException {
    final long blockID = 0;
    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);
    String data = RandomStringUtils.random(4 * KB);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(this.config,
        xceiverClientManager, metrics);
    CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(this.config)
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
    cache.put(blockID, data.getBytes(StandardCharsets.UTF_8));
    GenericTestUtils.waitFor(() -> !cache.isDirtyCache(), 100, 20 * 1000);
    cache.close();
  }

  @Test
  public void testCacheWriteToRemote50KBlocks() throws IOException,
      InterruptedException, TimeoutException {
    final long totalBlocks = 50 * 1000;
    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);
    String data = RandomStringUtils.random(4 * KB);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(this.config,
        xceiverClientManager, metrics);
    CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(this.config)
        .setVolumeName(volumeName)
        .setUserName(userName)
        .setPipelines(getContainerPipeline(10))
        .setClientManager(xceiverClientManager)
        .setBlockSize(4 * 1024)
        .setVolumeSize(50 * GB)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    cache.start();
    long startTime = Time.monotonicNow();
    for (long blockid = 0; blockid < totalBlocks; blockid++) {
      cache.put(blockid, data.getBytes(StandardCharsets.UTF_8));
    }
    Assert.assertEquals(totalBlocks, metrics.getNumWriteOps());
    Assert.assertEquals(totalBlocks,  metrics.getNumBlockBufferUpdates());
    LOG.info("Wrote 50K blocks, waiting for replication to finish.");
    GenericTestUtils.waitFor(() -> !cache.isDirtyCache(), 100, 20 * 1000);
    long endTime = Time.monotonicNow();
    LOG.info("Time taken for writing {} blocks is {} seconds", totalBlocks,
        TimeUnit.MILLISECONDS.toSeconds(endTime - startTime));
    // TODO: Read this data back.
    cache.close();
  }

  @Test
  public void testCacheInvalidBlock() throws IOException {
    final int blockID = 1024;
    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(this.config,
        xceiverClientManager, metrics);
    CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(this.config)
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
    // Read a non-existent block ID.
    LogicalBlock block = cache.get(blockID);
    Assert.assertNotNull(block);
    Assert.assertEquals(4 * 1024, block.getData().array().length);
    Assert.assertEquals(1, metrics.getNumReadOps());
    Assert.assertEquals(1, metrics.getNumReadLostBlocks());
    Assert.assertEquals(1, metrics.getNumReadCacheMiss());
    cache.close();
  }

  @Test
  public void testReadWriteCorrectness() throws IOException,
      InterruptedException, TimeoutException {
    Random r = new Random();
    final int maxBlock = 12500000;
    final int blockCount = 10 * 1000;
    Map<Long, String> blockShaMap = new HashMap<>();
    List<Pipeline> pipelines = getContainerPipeline(10);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(4);
    String userName = "user" + RandomStringUtils.randomNumeric(4);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(this.config,
        xceiverClientManager, metrics);
    final CBlockLocalCache cache = CBlockLocalCache.newBuilder()
        .setConfiguration(this.config)
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
    for (int x = 0; x < blockCount; x++) {
      String data = RandomStringUtils.random(4 * 1024);
      String dataHash = DigestUtils.sha256Hex(data);
      long blockId = abs(r.nextInt(maxBlock));
      blockShaMap.put(blockId, dataHash);
      cache.put(blockId, data.getBytes(StandardCharsets.UTF_8));
    }
    Assert.assertEquals(blockCount, metrics.getNumWriteOps());
    GenericTestUtils.waitFor(() -> !cache.isDirtyCache(), 100, 20 * 1000);
    LOG.info("Finished with putting blocks ..starting reading blocks back. " +
        "unique blocks : {}", blockShaMap.size());
    // Test reading from local cache.
    for (Map.Entry<Long, String> entry : blockShaMap.entrySet()) {
      LogicalBlock block = cache.get(entry.getKey());
      String blockSha = DigestUtils.sha256Hex(block.getData().array());
      Assert.assertEquals("Block data is not equal", entry.getValue(),
          blockSha);
    }
    Assert.assertEquals(blockShaMap.size(), metrics.getNumReadOps());
    Assert.assertEquals(blockShaMap.size(), metrics.getNumReadCacheHits());
    Assert.assertEquals(0, metrics.getNumReadCacheMiss());
    Assert.assertEquals(0, metrics.getNumReadLostBlocks());

    LOG.info("Finished with reading blocks, SUCCESS.");
    // Close and discard local cache.
    cache.close();
    LOG.info("Closing the and destroying local cache");
    CBlockTargetMetrics newMetrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher newflusher = new ContainerCacheFlusher(this.config,
        xceiverClientManager, newMetrics);
    Assert.assertEquals(0, newMetrics.getNumReadCacheHits());
    CBlockLocalCache newCache = null;
    try {
      newCache = CBlockLocalCache.newBuilder()
          .setConfiguration(this.config)
          .setVolumeName(volumeName)
          .setUserName(userName)
          .setPipelines(pipelines)
          .setClientManager(xceiverClientManager)
          .setBlockSize(4 * KB)
          .setVolumeSize(50 * GB)
          .setFlusher(newflusher)
          .setCBlockTargetMetrics(newMetrics)
          .build();
      newCache.start();
      for (Map.Entry<Long, String> entry : blockShaMap.entrySet()) {
        LogicalBlock block = newCache.get(entry.getKey());
        String blockSha = DigestUtils.sha256Hex(block.getData().array());
        Assert.assertEquals("Block data is not equal", entry.getValue(),
            blockSha);
      }

      Assert.assertEquals(blockShaMap.size(), newMetrics.getNumReadOps());
      Assert.assertEquals(blockShaMap.size(), newMetrics.getNumReadCacheHits());
      Assert.assertEquals(0, newMetrics.getNumReadCacheMiss());
      Assert.assertEquals(0, newMetrics.getNumReadLostBlocks());

      LOG.info("Finished with reading blocks from remote cache, SUCCESS.");
    } finally {
      if (newCache != null) {
        newCache.close();
      }
    }
  }

  @Test
  public void testStorageImplReadWrite() throws IOException,
      InterruptedException, TimeoutException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    long volumeSize = 50L * (1024L * 1024L * 1024L);
    int blockSize = 4096;
    byte[] data =
        RandomStringUtils.randomAlphanumeric(10 * (1024 * 1024))
            .getBytes(StandardCharsets.UTF_8);
    String hash = DigestUtils.sha256Hex(data);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(this.config,
        xceiverClientManager, metrics);
    CBlockIStorageImpl ozoneStore = CBlockIStorageImpl.newBuilder()
        .setUserName(userName)
        .setVolumeName(volumeName)
        .setVolumeSize(volumeSize)
        .setBlockSize(blockSize)
        .setContainerList(getContainerPipeline(10))
        .setClientManager(xceiverClientManager)
        .setConf(this.config)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    ozoneStore.write(data, 0);

    byte[] newData = new byte[10 * 1024 * 1024];
    ozoneStore.read(newData, 0);
    String newHash = DigestUtils.sha256Hex(newData);
    Assert.assertEquals("hashes don't match.", hash, newHash);
    GenericTestUtils.waitFor(() -> !ozoneStore.getCache().isDirtyCache(),
        100, 20 * 1000);
    ozoneStore.close();
  }

  //@Test
  // Disabling this test for time being since the bug in JSCSI
  // forces us always to have a local cache.
  public void testStorageImplNoLocalCache() throws IOException,
      InterruptedException, TimeoutException {
    OzoneConfiguration oConfig = new OzoneConfiguration();
    oConfig.setBoolean(DFS_CBLOCK_ENABLE_SHORT_CIRCUIT_IO, false);
    oConfig.setBoolean(DFS_CBLOCK_TRACE_IO, true);
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    long volumeSize = 50L * (1024L * 1024L * 1024L);
    int blockSize = 4096;
    byte[] data =
        RandomStringUtils.randomAlphanumeric(10 * (1024 * 1024))
            .getBytes(StandardCharsets.UTF_8);
    String hash = DigestUtils.sha256Hex(data);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(oConfig,
        xceiverClientManager, metrics);
    CBlockIStorageImpl ozoneStore = CBlockIStorageImpl.newBuilder()
        .setUserName(userName)
        .setVolumeName(volumeName)
        .setVolumeSize(volumeSize)
        .setBlockSize(blockSize)
        .setContainerList(getContainerPipeline(10))
        .setClientManager(xceiverClientManager)
        .setConf(oConfig)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    ozoneStore.write(data, 0);

    byte[] newData = new byte[10 * 1024 * 1024];
    ozoneStore.read(newData, 0);
    String newHash = DigestUtils.sha256Hex(newData);
    Assert.assertEquals("hashes don't match.", hash, newHash);
    GenericTestUtils.waitFor(() -> !ozoneStore.getCache().isDirtyCache(),
        100, 20 * 1000);
    ozoneStore.close();
  }
}
