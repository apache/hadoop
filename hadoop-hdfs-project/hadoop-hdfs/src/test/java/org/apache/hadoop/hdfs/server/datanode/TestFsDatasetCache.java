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
package org.apache.hadoop.hdfs.server.datanode;

import net.jcip.annotations.NotThreadSafe;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.client.impl.BlockReaderTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetCache;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetCache.PageRounder;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlockIdCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.NoMlockCacheManipulator;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.google.common.base.Supplier;
import com.google.common.primitives.Ints;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_KEY;

@NotThreadSafe
public class TestFsDatasetCache {
  private static final Log LOG = LogFactory.getLog(TestFsDatasetCache.class);

  // Most Linux installs allow a default of 64KB locked memory
  static final long CACHE_CAPACITY = 64 * 1024;
  // mlock always locks the entire page. So we don't need to deal with this
  // rounding, use the OS page size for the block size.
  private static final long PAGE_SIZE =
      NativeIO.POSIX.getCacheManipulator().getOperatingSystemPageSize();
  private static final long BLOCK_SIZE = PAGE_SIZE;

  private static Configuration conf;
  private static MiniDFSCluster cluster = null;
  private static FileSystem fs;
  private static NameNode nn;
  private static FSImage fsImage;
  private static DataNode dn;
  private static FsDatasetSpi<?> fsd;
  private static DatanodeProtocolClientSideTranslatorPB spyNN;
  /**
   * Used to pause DN BPServiceActor threads. BPSA threads acquire the
   * shared read lock. The test acquires the write lock for exclusive access.
   */
  private static ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private static final PageRounder rounder = new PageRounder();
  private static CacheManipulator prevCacheManipulator;
  private static DataNodeFaultInjector oldInjector;

  static {
    LogManager.getLogger(FsDatasetCache.class).setLevel(Level.DEBUG);
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
    conf.setLong(
        DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS, 100);
    conf.setLong(DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
        CACHE_CAPACITY);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_KEY, 10);

    prevCacheManipulator = NativeIO.POSIX.getCacheManipulator();
    NativeIO.POSIX.setCacheManipulator(new NoMlockCacheManipulator());

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    nn = cluster.getNameNode();
    fsImage = nn.getFSImage();
    dn = cluster.getDataNodes().get(0);
    fsd = dn.getFSDataset();

    spyNN = InternalDataNodeTestUtils.spyOnBposToNN(dn, nn);
  }

  @After
  public void tearDown() throws Exception {
    // Verify that each test uncached whatever it cached.  This cleanup is
    // required so that file descriptors are not leaked across tests.
    DFSTestUtil.verifyExpectedCacheUsage(0, 0, fsd);
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    // Restore the original CacheManipulator
    NativeIO.POSIX.setCacheManipulator(prevCacheManipulator);
  }

  private static void setHeartbeatResponse(DatanodeCommand[] cmds)
      throws Exception {
    lock.writeLock().lock();
    try {
      NNHAStatusHeartbeat ha = new NNHAStatusHeartbeat(HAServiceState.ACTIVE,
          fsImage.getLastAppliedOrWrittenTxId());
      HeartbeatResponse response =
          new HeartbeatResponse(cmds, ha, null,
              ThreadLocalRandom.current().nextLong() | 1L);
      doReturn(response).when(spyNN).sendHeartbeat(
          (DatanodeRegistration) any(),
          (StorageReport[]) any(), anyLong(), anyLong(),
          anyInt(), anyInt(), anyInt(), (VolumeFailureSummary) any(),
          anyBoolean(), any(SlowPeerReports.class),
          any(SlowDiskReports.class));
    } finally {
      lock.writeLock().unlock();
    }
  }

  private static DatanodeCommand[] cacheBlock(HdfsBlockLocation loc) {
    return cacheBlocks(new HdfsBlockLocation[] {loc});
  }

  private static DatanodeCommand[] cacheBlocks(HdfsBlockLocation[] locs) {
    return new DatanodeCommand[] {
        getResponse(locs, DatanodeProtocol.DNA_CACHE)
    };
  }

  private static DatanodeCommand[] uncacheBlock(HdfsBlockLocation loc) {
    return uncacheBlocks(new HdfsBlockLocation[] {loc});
  }

  private static DatanodeCommand[] uncacheBlocks(HdfsBlockLocation[] locs) {
    return new DatanodeCommand[] {
        getResponse(locs, DatanodeProtocol.DNA_UNCACHE)
    };
  }

  /**
   * Creates a cache or uncache DatanodeCommand from an array of locations
   */
  private static DatanodeCommand getResponse(HdfsBlockLocation[] locs,
      int action) {
    String bpid = locs[0].getLocatedBlock().getBlock().getBlockPoolId();
    long[] blocks = new long[locs.length];
    for (int i=0; i<locs.length; i++) {
      blocks[i] = locs[i].getLocatedBlock().getBlock().getBlockId();
    }
    return new BlockIdCommand(action, bpid, blocks);
  }

  private static long[] getBlockSizes(HdfsBlockLocation[] locs)
      throws Exception {
    long[] sizes = new long[locs.length];
    for (int i=0; i<locs.length; i++) {
      HdfsBlockLocation loc = locs[i];
      String bpid = loc.getLocatedBlock().getBlock().getBlockPoolId();
      Block block = loc.getLocatedBlock().getBlock().getLocalBlock();
      ExtendedBlock extBlock = new ExtendedBlock(bpid, block);
      FileInputStream blockInputStream = null;
      FileChannel blockChannel = null;
      try {
        blockInputStream =
          (FileInputStream)fsd.getBlockInputStream(extBlock, 0);
        blockChannel = blockInputStream.getChannel();
        sizes[i] = blockChannel.size();
      } finally {
        IOUtils.cleanup(LOG, blockChannel, blockInputStream);
      }
    }
    return sizes;
  }

  private void testCacheAndUncacheBlock() throws Exception {
    LOG.info("beginning testCacheAndUncacheBlock");
    final int NUM_BLOCKS = 5;

    DFSTestUtil.verifyExpectedCacheUsage(0, 0, fsd);
    assertEquals(0, fsd.getNumBlocksCached());

    // Write a test file
    final Path testFile = new Path("/testCacheBlock");
    final long testFileLen = BLOCK_SIZE*NUM_BLOCKS;
    DFSTestUtil.createFile(fs, testFile, testFileLen, (short)1, 0xABBAl);

    // Get the details of the written file
    HdfsBlockLocation[] locs =
        (HdfsBlockLocation[])fs.getFileBlockLocations(testFile, 0, testFileLen);
    assertEquals("Unexpected number of blocks", NUM_BLOCKS, locs.length);
    final long[] blockSizes = getBlockSizes(locs);

    // Check initial state
    final long cacheCapacity = fsd.getCacheCapacity();
    long cacheUsed = fsd.getCacheUsed();
    long current = 0;
    assertEquals("Unexpected cache capacity", CACHE_CAPACITY, cacheCapacity);
    assertEquals("Unexpected amount of cache used", current, cacheUsed);

    MetricsRecordBuilder dnMetrics;
    long numCacheCommands = 0;
    long numUncacheCommands = 0;

    // Cache each block in succession, checking each time
    for (int i=0; i<NUM_BLOCKS; i++) {
      setHeartbeatResponse(cacheBlock(locs[i]));
      current = DFSTestUtil.verifyExpectedCacheUsage(
          current + blockSizes[i], i + 1, fsd);
      dnMetrics = getMetrics(dn.getMetrics().name());
      long cmds = MetricsAsserts.getLongCounter("BlocksCached", dnMetrics);
      assertTrue("Expected more cache requests from the NN ("
          + cmds + " <= " + numCacheCommands + ")",
           cmds > numCacheCommands);
      numCacheCommands = cmds;
    }

    // Uncache each block in succession, again checking each time
    for (int i=0; i<NUM_BLOCKS; i++) {
      setHeartbeatResponse(uncacheBlock(locs[i]));
      current = DFSTestUtil.
          verifyExpectedCacheUsage(current - blockSizes[i],
              NUM_BLOCKS - 1 - i, fsd);
      dnMetrics = getMetrics(dn.getMetrics().name());
      long cmds = MetricsAsserts.getLongCounter("BlocksUncached", dnMetrics);
      assertTrue("Expected more uncache requests from the NN",
           cmds > numUncacheCommands);
      numUncacheCommands = cmds;
    }
    LOG.info("finishing testCacheAndUncacheBlock");
  }

  @Test(timeout=600000)
  public void testCacheAndUncacheBlockSimple() throws Exception {
    testCacheAndUncacheBlock();
  }

  /**
   * Run testCacheAndUncacheBlock with some failures injected into the mlock
   * call.  This tests the ability of the NameNode to resend commands.
   */
  @Test(timeout=600000)
  public void testCacheAndUncacheBlockWithRetries() throws Exception {
    // We don't have to save the previous cacheManipulator
    // because it will be reinstalled by the @After function.
    NativeIO.POSIX.setCacheManipulator(new NoMlockCacheManipulator() {
      private final Set<String> seenIdentifiers = new HashSet<String>();
      
      @Override
      public void mlock(String identifier,
          ByteBuffer mmap, long length) throws IOException {
        if (seenIdentifiers.contains(identifier)) {
          // mlock succeeds the second time.
          LOG.info("mlocking " + identifier);
          return;
        }
        seenIdentifiers.add(identifier);
        throw new IOException("injecting IOException during mlock of " +
            identifier);
      }
    });
    testCacheAndUncacheBlock();
  }

  @Test(timeout=600000)
  public void testFilesExceedMaxLockedMemory() throws Exception {
    LOG.info("beginning testFilesExceedMaxLockedMemory");

    // Create some test files that will exceed total cache capacity
    final int numFiles = 5;
    final long fileSize = CACHE_CAPACITY / (numFiles-1);

    final Path[] testFiles = new Path[numFiles];
    final HdfsBlockLocation[][] fileLocs = new HdfsBlockLocation[numFiles][];
    final long[] fileSizes = new long[numFiles];
    for (int i=0; i<numFiles; i++) {
      testFiles[i] = new Path("/testFilesExceedMaxLockedMemory-" + i);
      DFSTestUtil.createFile(fs, testFiles[i], fileSize, (short)1, 0xDFAl);
      fileLocs[i] = (HdfsBlockLocation[])fs.getFileBlockLocations(
          testFiles[i], 0, fileSize);
      // Get the file size (sum of blocks)
      long[] sizes = getBlockSizes(fileLocs[i]);
      for (int j=0; j<sizes.length; j++) {
        fileSizes[i] += sizes[j];
      }
    }

    // Cache the first n-1 files
    long total = 0;
    DFSTestUtil.verifyExpectedCacheUsage(0, 0, fsd);
    for (int i=0; i<numFiles-1; i++) {
      setHeartbeatResponse(cacheBlocks(fileLocs[i]));
      total = DFSTestUtil.verifyExpectedCacheUsage(
          rounder.roundUp(total + fileSizes[i]), 4 * (i + 1), fsd);
    }

    // nth file should hit a capacity exception
    final LogVerificationAppender appender = new LogVerificationAppender();
    final Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);
    setHeartbeatResponse(cacheBlocks(fileLocs[numFiles-1]));

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        int lines = appender.countLinesWithMessage(
            "more bytes in the cache: " +
            DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY);
        return lines > 0;
      }
    }, 500, 30000);
    // Also check the metrics for the failure
    assertTrue("Expected more than 0 failed cache attempts",
        fsd.getNumBlocksFailedToCache() > 0);

    // Uncache the n-1 files
    int curCachedBlocks = 16;
    for (int i=0; i<numFiles-1; i++) {
      setHeartbeatResponse(uncacheBlocks(fileLocs[i]));
      long uncachedBytes = rounder.roundUp(fileSizes[i]);
      total -= uncachedBytes;
      curCachedBlocks -= uncachedBytes / BLOCK_SIZE;
      DFSTestUtil.verifyExpectedCacheUsage(total, curCachedBlocks, fsd);
    }
    LOG.info("finishing testFilesExceedMaxLockedMemory");
  }

  @Test(timeout=600000)
  public void testUncachingBlocksBeforeCachingFinishes() throws Exception {
    LOG.info("beginning testUncachingBlocksBeforeCachingFinishes");
    final int NUM_BLOCKS = 5;

    DFSTestUtil.verifyExpectedCacheUsage(0, 0, fsd);

    // Write a test file
    final Path testFile = new Path("/testCacheBlock");
    final long testFileLen = BLOCK_SIZE*NUM_BLOCKS;
    DFSTestUtil.createFile(fs, testFile, testFileLen, (short)1, 0xABBAl);

    // Get the details of the written file
    HdfsBlockLocation[] locs =
        (HdfsBlockLocation[])fs.getFileBlockLocations(testFile, 0, testFileLen);
    assertEquals("Unexpected number of blocks", NUM_BLOCKS, locs.length);
    final long[] blockSizes = getBlockSizes(locs);

    // Check initial state
    final long cacheCapacity = fsd.getCacheCapacity();
    long cacheUsed = fsd.getCacheUsed();
    long current = 0;
    assertEquals("Unexpected cache capacity", CACHE_CAPACITY, cacheCapacity);
    assertEquals("Unexpected amount of cache used", current, cacheUsed);

    NativeIO.POSIX.setCacheManipulator(new NoMlockCacheManipulator() {
      @Override
      public void mlock(String identifier,
          ByteBuffer mmap, long length) throws IOException {
        LOG.info("An mlock operation is starting on " + identifier);
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          Assert.fail();
        }
      }
    });
    // Starting caching each block in succession.  The usedBytes amount
    // should increase, even though caching doesn't complete on any of them.
    for (int i=0; i<NUM_BLOCKS; i++) {
      setHeartbeatResponse(cacheBlock(locs[i]));
      current = DFSTestUtil.verifyExpectedCacheUsage(
          current + blockSizes[i], i + 1, fsd);
    }
    
    setHeartbeatResponse(new DatanodeCommand[] {
      getResponse(locs, DatanodeProtocol.DNA_UNCACHE)
    });

    // wait until all caching jobs are finished cancelling.
    current = DFSTestUtil.verifyExpectedCacheUsage(0, 0, fsd);
    LOG.info("finishing testUncachingBlocksBeforeCachingFinishes");
  }

  @Test(timeout=60000)
  public void testUncacheUnknownBlock() throws Exception {
    // Create a file
    Path fileName = new Path("/testUncacheUnknownBlock");
    int fileLen = 4096;
    DFSTestUtil.createFile(fs, fileName, fileLen, (short)1, 0xFDFD);
    HdfsBlockLocation[] locs = (HdfsBlockLocation[])fs.getFileBlockLocations(
        fileName, 0, fileLen);

    // Try to uncache it without caching it first
    setHeartbeatResponse(uncacheBlocks(locs));

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return fsd.getNumBlocksFailedToUncache() > 0;
      }
    }, 100, 10000);
  }

  @Test(timeout=600000)
  public void testPageRounder() throws Exception {
    // Write a small file
    Path fileName = new Path("/testPageRounder");
    final int smallBlocks = 512; // This should be smaller than the page size
    assertTrue("Page size should be greater than smallBlocks!",
        PAGE_SIZE > smallBlocks);
    final int numBlocks = 5;
    final int fileLen = smallBlocks * numBlocks;
    FSDataOutputStream out =
        fs.create(fileName, false, 4096, (short)1, smallBlocks);
    out.write(new byte[fileLen]);
    out.close();
    HdfsBlockLocation[] locs = (HdfsBlockLocation[])fs.getFileBlockLocations(
        fileName, 0, fileLen);
    // Cache the file and check the sizes match the page size
    setHeartbeatResponse(cacheBlocks(locs));
    DFSTestUtil.verifyExpectedCacheUsage(PAGE_SIZE * numBlocks, numBlocks, fsd);
    // Uncache and check that it decrements by the page size too
    setHeartbeatResponse(uncacheBlocks(locs));
    DFSTestUtil.verifyExpectedCacheUsage(0, 0, fsd);
  }

  @Test(timeout=60000)
  public void testUncacheQuiesces() throws Exception {
    // Create a file
    Path fileName = new Path("/testUncacheQuiesces");
    int fileLen = 4096;
    DFSTestUtil.createFile(fs, fileName, fileLen, (short)1, 0xFDFD);
    // Cache it
    DistributedFileSystem dfs = cluster.getFileSystem();
    dfs.addCachePool(new CachePoolInfo("pool"));
    dfs.addCacheDirective(new CacheDirectiveInfo.Builder()
        .setPool("pool").setPath(fileName).setReplication((short)3).build());
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        MetricsRecordBuilder dnMetrics = getMetrics(dn.getMetrics().name());
        long blocksCached =
            MetricsAsserts.getLongCounter("BlocksCached", dnMetrics);
        return blocksCached > 0;
      }
    }, 1000, 30000);
    // Uncache it
    dfs.removeCacheDirective(1);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        MetricsRecordBuilder dnMetrics = getMetrics(dn.getMetrics().name());
        long blocksUncached =
            MetricsAsserts.getLongCounter("BlocksUncached", dnMetrics);
        return blocksUncached > 0;
      }
    }, 1000, 30000);
    // Make sure that no additional messages were sent
    Thread.sleep(10000);
    MetricsRecordBuilder dnMetrics = getMetrics(dn.getMetrics().name());
    MetricsAsserts.assertCounter("BlocksCached", 1l, dnMetrics);
    MetricsAsserts.assertCounter("BlocksUncached", 1l, dnMetrics);
  }

  @Test(timeout=60000)
  public void testReCacheAfterUncache() throws Exception {
    final int TOTAL_BLOCKS_PER_CACHE =
        Ints.checkedCast(CACHE_CAPACITY / BLOCK_SIZE);
    BlockReaderTestUtil.enableHdfsCachingTracing();
    Assert.assertEquals(0, CACHE_CAPACITY % BLOCK_SIZE);
    
    // Create a small file
    final Path SMALL_FILE = new Path("/smallFile");
    DFSTestUtil.createFile(fs, SMALL_FILE,
        BLOCK_SIZE, (short)1, 0xcafe);

    // Create a file that will take up the whole cache
    final Path BIG_FILE = new Path("/bigFile");
    DFSTestUtil.createFile(fs, BIG_FILE,
        TOTAL_BLOCKS_PER_CACHE * BLOCK_SIZE, (short)1, 0xbeef);
    final DistributedFileSystem dfs = cluster.getFileSystem();
    dfs.addCachePool(new CachePoolInfo("pool"));
    final long bigCacheDirectiveId = 
        dfs.addCacheDirective(new CacheDirectiveInfo.Builder()
        .setPool("pool").setPath(BIG_FILE).setReplication((short)1).build());
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        MetricsRecordBuilder dnMetrics = getMetrics(dn.getMetrics().name());
        long blocksCached =
            MetricsAsserts.getLongCounter("BlocksCached", dnMetrics);
        if (blocksCached != TOTAL_BLOCKS_PER_CACHE) {
          LOG.info("waiting for " + TOTAL_BLOCKS_PER_CACHE + " to " +
              "be cached.   Right now only " + blocksCached + " blocks are cached.");
          return false;
        }
        LOG.info(TOTAL_BLOCKS_PER_CACHE + " blocks are now cached.");
        return true;
      }
    }, 1000, 30000);
    
    // Try to cache a smaller file.  It should fail.
    final long shortCacheDirectiveId =
      dfs.addCacheDirective(new CacheDirectiveInfo.Builder()
        .setPool("pool").setPath(SMALL_FILE).setReplication((short)1).build());
    Thread.sleep(10000);
    MetricsRecordBuilder dnMetrics = getMetrics(dn.getMetrics().name());
    Assert.assertEquals(TOTAL_BLOCKS_PER_CACHE,
        MetricsAsserts.getLongCounter("BlocksCached", dnMetrics));
    
    // Uncache the big file and verify that the small file can now be
    // cached (regression test for HDFS-6107)
    dfs.removeCacheDirective(bigCacheDirectiveId);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        RemoteIterator<CacheDirectiveEntry> iter;
        try {
          iter = dfs.listCacheDirectives(
              new CacheDirectiveInfo.Builder().build());
          CacheDirectiveEntry entry;
          do {
            entry = iter.next();
          } while (entry.getInfo().getId() != shortCacheDirectiveId);
          if (entry.getStats().getFilesCached() != 1) {
            LOG.info("waiting for directive " + shortCacheDirectiveId +
                " to be cached.  stats = " + entry.getStats());
            return false;
          }
          LOG.info("directive " + shortCacheDirectiveId + " has been cached.");
        } catch (IOException e) {
          Assert.fail("unexpected exception" + e.toString());
        }
        return true;
      }
    }, 1000, 30000);

    dfs.removeCacheDirective(shortCacheDirectiveId);
  }
}
