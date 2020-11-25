/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.impl.BlockReaderTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.NoMlockCacheManipulator;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MetricsAsserts;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.event.Level;

import java.util.function.Supplier;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;

/**
 * Tests HDFS persistent memory cache by PmemMappableBlockLoader.
 *
 * Bogus persistent memory volume is used to cache blocks.
 */
public class TestPmemCacheRecovery {
  protected static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(TestCacheByPmemMappableBlockLoader.class);

  protected static final long CACHE_AMOUNT = 64 * 1024;
  protected static final long BLOCK_SIZE = 4 * 1024;

  private static Configuration conf;
  private static MiniDFSCluster cluster = null;
  private static DistributedFileSystem fs;
  private static DataNode dn;
  private static FsDatasetCache cacheManager;
  private static String blockPoolId = "";
  /**
   * Used to pause DN BPServiceActor threads. BPSA threads acquire the
   * shared read lock. The test acquires the write lock for exclusive access.
   */
  private static ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private static CacheManipulator prevCacheManipulator;
  private static DataNodeFaultInjector oldInjector;

  private static final String PMEM_DIR_0 =
      MiniDFSCluster.getBaseDirectory() + "pmem0";
  private static final String PMEM_DIR_1 =
      MiniDFSCluster.getBaseDirectory() + "pmem1";

  static {
    GenericTestUtils.setLogLevel(
        LoggerFactory.getLogger(FsDatasetCache.class), Level.DEBUG);
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    oldInjector = DataNodeFaultInjector.get();
    DataNodeFaultInjector.set(new DataNodeFaultInjector() {
      @Override
      public void startOfferService() throws Exception {
        lock.readLock().lock();
      }

      @Override
      public void endOfferService() throws Exception {
        lock.readLock().unlock();
      }
    });
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    DataNodeFaultInjector.set(oldInjector);
  }

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFS_DATANODE_PMEM_CACHE_RECOVERY_KEY, true);
    conf.setLong(DFSConfigKeys.
        DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS, 100);
    conf.setLong(DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(
        DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_KEY, 10);

    // Configuration for pmem cache
    new File(PMEM_DIR_0).getAbsoluteFile().mkdir();
    new File(PMEM_DIR_1).getAbsoluteFile().mkdir();
    // Configure two bogus pmem volumes
    conf.set(DFS_DATANODE_PMEM_CACHE_DIRS_KEY, PMEM_DIR_0 + "," + PMEM_DIR_1);

    prevCacheManipulator = NativeIO.POSIX.getCacheManipulator();
    NativeIO.POSIX.setCacheManipulator(new NoMlockCacheManipulator());

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    dn = cluster.getDataNodes().get(0);
    cacheManager = ((FsDatasetImpl) dn.getFSDataset()).cacheManager;
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    NativeIO.POSIX.setCacheManipulator(prevCacheManipulator);
  }

  protected static void restartCluster() throws Exception {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFS_DATANODE_PMEM_CACHE_RECOVERY_KEY, true);
    conf.setLong(DFSConfigKeys.
        DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS, 100);
    conf.setLong(DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(
        DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_KEY, 10);
    // Configure two bogus pmem volumes
    conf.set(DFS_DATANODE_PMEM_CACHE_DIRS_KEY, PMEM_DIR_0 + "," + PMEM_DIR_1);

    prevCacheManipulator = NativeIO.POSIX.getCacheManipulator();
    NativeIO.POSIX.setCacheManipulator(new NoMlockCacheManipulator());

    FsDatasetImpl.setBlockPoolId(blockPoolId);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    dn = cluster.getDataNodes().get(0);
    cacheManager = ((FsDatasetImpl) dn.getFSDataset()).cacheManager;
  }

  protected static void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    PmemVolumeManager.reset();
  }

  public List<ExtendedBlockId> getExtendedBlockId(Path filePath, long fileLen)
      throws IOException {
    List<ExtendedBlockId> keys = new ArrayList<>();
    HdfsBlockLocation[] locs = (HdfsBlockLocation[]) fs.getFileBlockLocations(
        filePath, 0, fileLen);
    for (HdfsBlockLocation loc : locs) {
      long bkid = loc.getLocatedBlock().getBlock().getBlockId();
      String bpid = loc.getLocatedBlock().getBlock().getBlockPoolId();
      keys.add(new ExtendedBlockId(bkid, bpid));
    }
    return keys;
  }

  @Test(timeout = 60000)
  public void testCacheRecovery() throws Exception {
    final int cacheBlocksNum =
        Ints.checkedCast(CACHE_AMOUNT / BLOCK_SIZE);
    BlockReaderTestUtil.enableHdfsCachingTracing();
    Assert.assertEquals(0, CACHE_AMOUNT % BLOCK_SIZE);

    final Path testFile = new Path("/testFile");
    final long testFileLen = cacheBlocksNum * BLOCK_SIZE;
    DFSTestUtil.createFile(fs, testFile,
        testFileLen, (short) 1, 0xbeef);
    List<ExtendedBlockId> blockKeys =
        getExtendedBlockId(testFile, testFileLen);
    fs.addCachePool(new CachePoolInfo("testPool"));
    final long cacheDirectiveId = fs.addCacheDirective(
        new CacheDirectiveInfo.Builder().setPool("testPool").
            setPath(testFile).setReplication((short) 1).build());
    // wait for caching
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        MetricsRecordBuilder dnMetrics = getMetrics(dn.getMetrics().name());
        long blocksCached =
            MetricsAsserts.getLongCounter("BlocksCached", dnMetrics);
        if (blocksCached != cacheBlocksNum) {
          LOG.info("waiting for " + cacheBlocksNum + " blocks to " +
              "be cached. Right now " + blocksCached + " blocks are cached.");
          return false;
        }
        LOG.info(cacheBlocksNum + " blocks are now cached.");
        return true;
      }
    }, 1000, 30000);

    assertEquals(CACHE_AMOUNT, cacheManager.getCacheUsed());
    Map<ExtendedBlockId, Byte> blockKeyToVolume =
        PmemVolumeManager.getInstance().getBlockKeyToVolume();
    // All block keys should be kept in blockKeyToVolume
    assertEquals(blockKeyToVolume.size(), cacheBlocksNum);
    assertTrue(blockKeyToVolume.keySet().containsAll(blockKeys));
    // Test each replica's cache file path
    for (ExtendedBlockId key : blockKeys) {
      if (blockPoolId.isEmpty()) {
        blockPoolId = key.getBlockPoolId();
      }
      String cachePath = cacheManager.
          getReplicaCachePath(key.getBlockPoolId(), key.getBlockId());
      // The cachePath shouldn't be null if the replica has been cached
      // to pmem.
      assertNotNull(cachePath);
      Path path = new Path(cachePath);
      String fileName = path.getName();
      if (cachePath.startsWith(PMEM_DIR_0)) {
        String expectPath = PmemVolumeManager.
            getRealPmemDir(PMEM_DIR_0) + "/" + key.getBlockPoolId();
        assertTrue(path.toString().startsWith(expectPath));
        assertTrue(key.getBlockId() == Long.parseLong(fileName));
      } else if (cachePath.startsWith(PMEM_DIR_1)) {
        String expectPath = PmemVolumeManager.
            getRealPmemDir(PMEM_DIR_1) + "/" + key.getBlockPoolId();
        assertTrue(path.toString().startsWith(expectPath));
        assertTrue(key.getBlockId() == Long.parseLong(fileName));
      } else {
        fail("The cache path is not the expected one: " + cachePath);
      }
    }

    // Trigger cache recovery
    shutdownCluster();
    restartCluster();

    assertEquals(CACHE_AMOUNT, cacheManager.getCacheUsed());
    blockKeyToVolume = PmemVolumeManager.getInstance().getBlockKeyToVolume();
    // All block keys should be kept in blockKeyToVolume
    assertEquals(blockKeyToVolume.size(), cacheBlocksNum);
    assertTrue(blockKeyToVolume.keySet().containsAll(blockKeys));
    // Test each replica's cache file path
    for (ExtendedBlockId key : blockKeys) {
      String cachePath = cacheManager.
          getReplicaCachePath(key.getBlockPoolId(), key.getBlockId());
      // The cachePath shouldn't be null if the replica has been cached
      // to pmem.
      assertNotNull(cachePath);
      Path path = new Path(cachePath);
      String fileName = path.getName();
      if (cachePath.startsWith(PMEM_DIR_0)) {
        String expectPath = PmemVolumeManager.
            getRealPmemDir(PMEM_DIR_0) + "/" + key.getBlockPoolId();
        assertTrue(path.toString().startsWith(expectPath));
        assertTrue(key.getBlockId() == Long.parseLong(fileName));
      } else if (cachePath.startsWith(PMEM_DIR_1)) {
        String expectPath = PmemVolumeManager.
            getRealPmemDir(PMEM_DIR_1) + "/" + key.getBlockPoolId();
        assertTrue(path.toString().startsWith(expectPath));
        assertTrue(key.getBlockId() == Long.parseLong(fileName));
      } else {
        fail("The cache path is not the expected one: " + cachePath);
      }
    }

    // Uncache the test file
    for (ExtendedBlockId key : blockKeys) {
      cacheManager.uncacheBlock(blockPoolId, key.getBlockId());
    }
    // Wait for uncaching
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        MetricsRecordBuilder dnMetrics = getMetrics(dn.getMetrics().name());
        long blocksUncached =
            MetricsAsserts.getLongCounter("BlocksUncached", dnMetrics);
        if (blocksUncached != cacheBlocksNum) {
          LOG.info("waiting for " + cacheBlocksNum + " blocks to be " +
              "uncached. Right now " + blocksUncached +
              " blocks are uncached.");
          return false;
        }
        LOG.info(cacheBlocksNum + " blocks have been uncached.");
        return true;
      }
    }, 1000, 30000);

    // It is expected that no pmem cache space is used.
    assertEquals(0, cacheManager.getCacheUsed());
    // No record should be kept by blockKeyToVolume after testFile is uncached.
    assertEquals(blockKeyToVolume.size(), 0);
  }
}
