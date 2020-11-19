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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SCANNER_SKIP_RECENT_ACCESSED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SCANNER_VOLUME_JOIN_TIMEOUT_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND;
import static org.apache.hadoop.hdfs.server.datanode.BlockScanner.Conf.INTERNAL_DFS_DATANODE_SCAN_PERIOD_MS;
import static org.apache.hadoop.hdfs.server.datanode.BlockScanner.Conf.INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER;
import static org.apache.hadoop.hdfs.server.datanode.BlockScanner.Conf.INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import java.util.function.Supplier;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.FsDatasetTestUtils.MaterializedReplica;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.VolumeScanner.ScanResultHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.BlockIterator;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.datanode.VolumeScanner.Statistics;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBlockScanner {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestBlockScanner.class);

  @Before
  public void before() {
    BlockScanner.Conf.allowUnitTestSettings = true;
    GenericTestUtils.setLogLevel(BlockScanner.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(VolumeScanner.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(FsVolumeImpl.LOG, Level.ALL);
  }

  private static void disableBlockScanner(Configuration conf) {
    conf.setLong(DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND, 0L);
  }

  private static class TestContext implements Closeable {
    final int numNameServices;
    final MiniDFSCluster cluster;
    final DistributedFileSystem[] dfs;
    final String[] bpids;
    final DataNode datanode;
    final BlockScanner blockScanner;
    final FsDatasetSpi<? extends FsVolumeSpi> data;
    final FsDatasetSpi.FsVolumeReferences volumes;

    TestContext(Configuration conf, int numNameServices) throws Exception {
      this.numNameServices = numNameServices;
      File basedir = new File(GenericTestUtils.getRandomizedTempPath());
      long volumeScannerTimeOutFromConf =
          conf.getLong(DFS_BLOCK_SCANNER_VOLUME_JOIN_TIMEOUT_MSEC_KEY, -1);
      long expectedVScannerTimeOut =
          volumeScannerTimeOutFromConf == -1
              ? MiniDFSCluster.DEFAULT_SCANNER_VOLUME_JOIN_TIMEOUT_MSEC
              : volumeScannerTimeOutFromConf;
      MiniDFSCluster.Builder bld = new MiniDFSCluster.Builder(conf, basedir).
          numDataNodes(1).
          storagesPerDatanode(1);
      // verify that the builder was initialized to get the default
      // configuration designated for Junit tests.
      assertEquals(expectedVScannerTimeOut,
            conf.getLong(DFS_BLOCK_SCANNER_VOLUME_JOIN_TIMEOUT_MSEC_KEY, -1));
      if (numNameServices > 1) {
        bld.nnTopology(MiniDFSNNTopology.
              simpleFederatedTopology(numNameServices));
      }
      cluster = bld.build();
      cluster.waitActive();
      dfs = new DistributedFileSystem[numNameServices];
      for (int i = 0; i < numNameServices; i++) {
        dfs[i] = cluster.getFileSystem(i);
      }
      bpids = new String[numNameServices];
      for (int i = 0; i < numNameServices; i++) {
        bpids[i] = cluster.getNamesystem(i).getBlockPoolId();
      }
      datanode = cluster.getDataNodes().get(0);
      blockScanner = datanode.getBlockScanner();
      for (int i = 0; i < numNameServices; i++) {
        dfs[i].mkdirs(new Path("/test"));
      }
      data = datanode.getFSDataset();
      volumes = data.getFsVolumeReferences();
    }

    @Override
    public void close() throws IOException {
      volumes.close();
      if (cluster != null) {
        for (int i = 0; i < numNameServices; i++) {
          dfs[i].delete(new Path("/test"), true);
          dfs[i].close();
        }
        cluster.shutdown();
      }
    }

    public void createFiles(int nsIdx, int numFiles, int length)
          throws Exception {
      for (int blockIdx = 0; blockIdx < numFiles; blockIdx++) {
        DFSTestUtil.createFile(dfs[nsIdx], getPath(blockIdx), length,
            (short)1, 123L);
      }
    }

    public Path getPath(int fileIdx) {
      return new Path("/test/" + fileIdx);
    }

    public ExtendedBlock getFileBlock(int nsIdx, int fileIdx)
          throws Exception {
      return DFSTestUtil.getFirstBlock(dfs[nsIdx], getPath(fileIdx));
    }

    public MaterializedReplica getMaterializedReplica(int nsIdx, int fileIdx)
        throws Exception {
      return cluster.getMaterializedReplica(0, getFileBlock(nsIdx, fileIdx));
    }
  }

  /**
   * Test iterating through a bunch of blocks in a volume using a volume
   * iterator.<p/>
   *
   * We will rewind the iterator when about halfway through the blocks.
   *
   * @param numFiles        The number of files to create.
   * @param maxStaleness    The maximum staleness to allow with the iterator.
   * @throws Exception
   */
  private void testVolumeIteratorImpl(int numFiles,
              long maxStaleness) throws Exception {
    Configuration conf = new Configuration();
    disableBlockScanner(conf);
    TestContext ctx = new TestContext(conf, 1);
    ctx.createFiles(0, numFiles, 1);
    assertEquals(1, ctx.volumes.size());
    FsVolumeSpi volume = ctx.volumes.get(0);
    ExtendedBlock savedBlock = null, loadedBlock = null;
    boolean testedRewind = false, testedSave = false, testedLoad = false;
    int blocksProcessed = 0, savedBlocksProcessed = 0;
    try {
      List<BPOfferService> bpos = ctx.datanode.getAllBpOs();
      assertEquals(1, bpos.size());
      BlockIterator iter = volume.newBlockIterator(ctx.bpids[0], "test");
      assertEquals(ctx.bpids[0], iter.getBlockPoolId());
      iter.setMaxStalenessMs(maxStaleness);
      while (true) {
        HashSet<ExtendedBlock> blocks = new HashSet<ExtendedBlock>();
        for (int blockIdx = 0; blockIdx < numFiles; blockIdx++) {
          blocks.add(ctx.getFileBlock(0, blockIdx));
        }
        while (true) {
          ExtendedBlock block = iter.nextBlock();
          if (block == null) {
            break;
          }
          blocksProcessed++;
          LOG.info("BlockIterator for {} found block {}, blocksProcessed = {}",
              volume, block, blocksProcessed);
          if (testedSave && (savedBlock == null)) {
            savedBlock = block;
          }
          if (testedLoad && (loadedBlock == null)) {
            loadedBlock = block;
            // The block that we get back right after loading the iterator
            // should be the same block we got back right after saving
            // the iterator.
            assertEquals(savedBlock, loadedBlock);
          }
          boolean blockRemoved = blocks.remove(block);
          assertTrue("Found unknown block " + block, blockRemoved);
          if (blocksProcessed > (numFiles / 3)) {
            if (!testedSave) {
              LOG.info("Processed {} blocks out of {}.  Saving iterator.",
                  blocksProcessed, numFiles);
              iter.save();
              testedSave = true;
              savedBlocksProcessed = blocksProcessed;
            }
          }
          if (blocksProcessed > (numFiles / 2)) {
            if (!testedRewind) {
              LOG.info("Processed {} blocks out of {}.  Rewinding iterator.",
                  blocksProcessed, numFiles);
              iter.rewind();
              break;
            }
          }
          if (blocksProcessed > ((2 * numFiles) / 3)) {
            if (!testedLoad) {
              LOG.info("Processed {} blocks out of {}.  Loading iterator.",
                  blocksProcessed, numFiles);
              iter = volume.loadBlockIterator(ctx.bpids[0], "test");
              iter.setMaxStalenessMs(maxStaleness);
              break;
            }
          }
        }
        if (!testedRewind) {
          testedRewind = true;
          blocksProcessed = 0;
          LOG.info("Starting again at the beginning...");
          continue;
        }
        if (!testedLoad) {
          testedLoad = true;
          blocksProcessed = savedBlocksProcessed;
          LOG.info("Starting again at the load point...");
          continue;
        }
        assertEquals(numFiles, blocksProcessed);
        break;
      }
    } finally {
      ctx.close();
    }
  }

  @Test(timeout=60000)
  public void testVolumeIteratorWithoutCaching() throws Exception {
    testVolumeIteratorImpl(5, 0);
  }

  @Test(timeout=300000)
  public void testVolumeIteratorWithCaching() throws Exception {
    testVolumeIteratorImpl(600, 100);
  }

  @Test(timeout=60000)
  public void testDisableVolumeScanner() throws Exception {
    Configuration conf = new Configuration();
    disableBlockScanner(conf);
    TestContext ctx = new TestContext(conf, 1);
    try {
      Assert.assertFalse(ctx.datanode.getBlockScanner().isEnabled());
    } finally {
      ctx.close();
    }
  }

  public static class TestScanResultHandler extends ScanResultHandler {
    static class Info {
      boolean shouldRun = false;
      final Set<ExtendedBlock> badBlocks = new HashSet<ExtendedBlock>();
      final Set<ExtendedBlock> goodBlocks = new HashSet<ExtendedBlock>();
      long blocksScanned = 0;
      Semaphore sem = null;

      @Override
      public String toString() {
        final StringBuilder bld = new StringBuilder();
        bld.append("ScanResultHandler.Info{");
        bld.append("shouldRun=").append(shouldRun).append(", ");
        bld.append("blocksScanned=").append(blocksScanned).append(", ");
        bld.append("sem#availablePermits=").append(sem.availablePermits()).
            append(", ");
        bld.append("badBlocks=").append(badBlocks).append(", ");
        bld.append("goodBlocks=").append(goodBlocks);
        bld.append("}");
        return bld.toString();
      }
    }

    private VolumeScanner scanner;

    final static ConcurrentHashMap<String, Info> infos =
        new ConcurrentHashMap<String, Info>();

    static Info getInfo(FsVolumeSpi volume) {
      Info newInfo = new Info();
      Info prevInfo = infos.
          putIfAbsent(volume.getStorageID(), newInfo);
      return prevInfo == null ? newInfo : prevInfo;
    }

    @Override
    public void setup(VolumeScanner scanner) {
      this.scanner = scanner;
      Info info = getInfo(scanner.volume);
      LOG.info("about to start scanning.");
      synchronized (info) {
        while (!info.shouldRun) {
          try {
            info.wait();
          } catch (InterruptedException e) {
          }
        }
      }
      LOG.info("starting scanning.");
    }

    @Override
    public void handle(ExtendedBlock block, IOException e) {
      LOG.info("handling block {} (exception {})", block, e);
      Info info = getInfo(scanner.volume);
      Semaphore sem;
      synchronized (info) {
        sem = info.sem;
      }
      if (sem != null) {
        try {
          sem.acquire();
        } catch (InterruptedException ie) {
          throw new RuntimeException("interrupted");
        }
      }
      synchronized (info) {
        if (!info.shouldRun) {
          throw new RuntimeException("stopping volumescanner thread.");
        }
        if (e == null) {
          info.goodBlocks.add(block);
        } else {
          info.badBlocks.add(block);
        }
        info.blocksScanned++;
      }
    }
  }

  private void testScanAllBlocksImpl(final boolean rescan) throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND, 1048576L);
    if (rescan) {
      conf.setLong(INTERNAL_DFS_DATANODE_SCAN_PERIOD_MS, 100L);
    } else {
      conf.setLong(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 100L);
    }
    conf.set(INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER,
        TestScanResultHandler.class.getName());
    final TestContext ctx = new TestContext(conf, 1);
    final int NUM_EXPECTED_BLOCKS = 10;
    ctx.createFiles(0, NUM_EXPECTED_BLOCKS, 1);
    final Set<ExtendedBlock> expectedBlocks = new HashSet<ExtendedBlock>();
    for (int i = 0; i < NUM_EXPECTED_BLOCKS; i++) {
      expectedBlocks.add(ctx.getFileBlock(0, i));
    }
    TestScanResultHandler.Info info =
        TestScanResultHandler.getInfo(ctx.volumes.get(0));
    synchronized (info) {
      info.shouldRun = true;
      info.notify();
    }
    GenericTestUtils.waitFor(new Supplier<Boolean>(){
      @Override
      public Boolean get() {
        TestScanResultHandler.Info info =
            TestScanResultHandler.getInfo(ctx.volumes.get(0));
        int numFoundBlocks = 0;
        StringBuilder foundBlocksBld = new StringBuilder();
        String prefix = "";
        synchronized (info) {
          for (ExtendedBlock block : info.goodBlocks) {
            assertTrue(expectedBlocks.contains(block));
            numFoundBlocks++;
            foundBlocksBld.append(prefix).append(block);
            prefix = ", ";
          }
          LOG.info("numFoundBlocks = {}.  blocksScanned = {}. Found blocks {}",
              numFoundBlocks, info.blocksScanned, foundBlocksBld.toString());
          if (rescan) {
            return (numFoundBlocks == NUM_EXPECTED_BLOCKS) &&
                     (info.blocksScanned >= 2 * NUM_EXPECTED_BLOCKS);
          } else {
            return numFoundBlocks == NUM_EXPECTED_BLOCKS;
          }
        }
      }
    }, 10, 60000);
    if (!rescan) {
      synchronized (info) {
        assertEquals(NUM_EXPECTED_BLOCKS, info.blocksScanned);
      }
      Statistics stats = ctx.blockScanner.getVolumeStats(
          ctx.volumes.get(0).getStorageID());
      assertEquals(5 * NUM_EXPECTED_BLOCKS, stats.bytesScannedInPastHour);
      assertEquals(NUM_EXPECTED_BLOCKS, stats.blocksScannedSinceRestart);
      assertEquals(NUM_EXPECTED_BLOCKS, stats.blocksScannedInCurrentPeriod);
      assertEquals(0, stats.scanErrorsSinceRestart);
      assertEquals(1, stats.scansSinceRestart);
    }
    ctx.close();
  }

  /**
   * Test scanning all blocks.  Set the scan period high enough that
   * we shouldn't rescan any block during this test.
   */
  @Test(timeout=60000)
  public void testScanAllBlocksNoRescan() throws Exception {
    testScanAllBlocksImpl(false);
  }

  /**
   * Test scanning all blocks.  Set the scan period high enough that
   * we should rescan all blocks at least twice during this test.
   */
  @Test(timeout=60000)
  public void testScanAllBlocksWithRescan() throws Exception {
    testScanAllBlocksImpl(true);
  }

  /**
   * Test that we don't scan too many blocks per second.
   */
  @Test(timeout=120000)
  public void testScanRateLimit() throws Exception {
    Configuration conf = new Configuration();
    // Limit scan bytes per second dramatically
    conf.setLong(DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND, 4096L);
    // Scan continuously
    conf.setLong(INTERNAL_DFS_DATANODE_SCAN_PERIOD_MS, 1L);
    conf.set(INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER,
        TestScanResultHandler.class.getName());
    final TestContext ctx = new TestContext(conf, 1);
    final int NUM_EXPECTED_BLOCKS = 5;
    ctx.createFiles(0, NUM_EXPECTED_BLOCKS, 4096);
    final TestScanResultHandler.Info info =
        TestScanResultHandler.getInfo(ctx.volumes.get(0));
    long startMs = Time.monotonicNow();
    synchronized (info) {
      info.shouldRun = true;
      info.notify();
    }
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        synchronized (info) {
          return info.blocksScanned > 0;
        }
      }
    }, 1, 30000);
    Thread.sleep(2000);
    synchronized (info) {
      long endMs = Time.monotonicNow();
      // Should scan no more than one block a second.
      long seconds = ((endMs + 999 - startMs) / 1000);
      long maxBlocksScanned = seconds * 1;
      assertTrue("The number of blocks scanned is too large.  Scanned " +
          info.blocksScanned + " blocks; only expected to scan at most " +
          maxBlocksScanned + " in " + seconds + " seconds.",
          info.blocksScanned <= maxBlocksScanned);
    }
    ctx.close();
  }

  @Test(timeout=120000)
  public void testCorruptBlockHandling() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 100L);
    conf.set(INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER,
        TestScanResultHandler.class.getName());
    final TestContext ctx = new TestContext(conf, 1);
    final int NUM_EXPECTED_BLOCKS = 5;
    final int CORRUPT_INDEX = 3;
    ctx.createFiles(0, NUM_EXPECTED_BLOCKS, 4);
    ExtendedBlock badBlock = ctx.getFileBlock(0, CORRUPT_INDEX);
    ctx.cluster.corruptBlockOnDataNodes(badBlock);
    final TestScanResultHandler.Info info =
        TestScanResultHandler.getInfo(ctx.volumes.get(0));
    synchronized (info) {
      info.shouldRun = true;
      info.notify();
    }
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        synchronized (info) {
          return info.blocksScanned == NUM_EXPECTED_BLOCKS;
        }
      }
    }, 3, 30000);
    synchronized (info) {
      assertTrue(info.badBlocks.contains(badBlock));
      for (int i = 0; i < NUM_EXPECTED_BLOCKS; i++) {
        if (i != CORRUPT_INDEX) {
          ExtendedBlock block = ctx.getFileBlock(0, i);
          assertTrue(info.goodBlocks.contains(block));
        }
      }
    }
    ctx.close();
  }

  /**
   * Test that we save the scan cursor when shutting down the datanode, and
   * restart scanning from there when the datanode is restarted.
   */
  @Test(timeout=120000)
  public void testDatanodeCursor() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 100L);
    conf.set(INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER,
        TestScanResultHandler.class.getName());
    conf.setLong(INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS, 0L);
    final TestContext ctx = new TestContext(conf, 1);
    final int NUM_EXPECTED_BLOCKS = 10;
    ctx.createFiles(0, NUM_EXPECTED_BLOCKS, 1);
    final TestScanResultHandler.Info info =
        TestScanResultHandler.getInfo(ctx.volumes.get(0));
    synchronized (info) {
      info.sem = new Semaphore(5);
      info.shouldRun = true;
      info.notify();
    }
    // Scan the first 5 blocks
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        synchronized (info) {
          return info.blocksScanned == 5;
        }
      }
    }, 3, 30000);
    synchronized (info) {
      assertEquals(5, info.goodBlocks.size());
      assertEquals(5, info.blocksScanned);
      info.shouldRun = false;
    }
    ctx.datanode.shutdown();
    URI vURI = ctx.volumes.get(0).getStorageLocation().getUri();
    File cursorPath = new File(new File(new File(new File(vURI), "current"),
          ctx.bpids[0]), "scanner.cursor");
    assertTrue("Failed to find cursor save file in " +
        cursorPath.getAbsolutePath(), cursorPath.exists());
    Set<ExtendedBlock> prevGoodBlocks = new HashSet<ExtendedBlock>();
    synchronized (info) {
      info.sem = new Semaphore(4);
      prevGoodBlocks.addAll(info.goodBlocks);
      info.goodBlocks.clear();
    }

    // The block that we were scanning when we shut down the DN won't get
    // recorded.
    // After restarting the datanode, we should scan the next 4 blocks.
    ctx.cluster.restartDataNode(0);
    synchronized (info) {
      info.shouldRun = true;
      info.notify();
    }
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        synchronized (info) {
          if (info.blocksScanned != 9) {
            LOG.info("Waiting for blocksScanned to reach 9.  It is at {}",
                info.blocksScanned);
          }
          return info.blocksScanned == 9;
        }
      }
    }, 3, 30000);
    synchronized (info) {
      assertEquals(4, info.goodBlocks.size());
      info.goodBlocks.addAll(prevGoodBlocks);
      assertEquals(9, info.goodBlocks.size());
      assertEquals(9, info.blocksScanned);
    }
    ctx.datanode.shutdown();

    // After restarting the datanode, we should not scan any more blocks.
    // This is because we reached the end of the block pool earlier, and
    // the scan period is much, much longer than the test time.
    synchronized (info) {
      info.sem = null;
      info.shouldRun = false;
      info.goodBlocks.clear();
    }
    ctx.cluster.restartDataNode(0);
    synchronized (info) {
      info.shouldRun = true;
      info.notify();
    }
    Thread.sleep(3000);
    synchronized (info) {
      assertTrue(info.goodBlocks.isEmpty());
    }
    ctx.close();
  }

  @Test(timeout=120000)
  public void testMultipleBlockPoolScanning() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 100L);
    conf.set(INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER,
        TestScanResultHandler.class.getName());
    final TestContext ctx = new TestContext(conf, 3);

    // We scan 5 bytes per file (1 byte in file, 4 bytes of checksum)
    final int BYTES_SCANNED_PER_FILE = 5;
    int TOTAL_FILES = 16;
    ctx.createFiles(0, TOTAL_FILES, 1);

    // start scanning
    final TestScanResultHandler.Info info =
        TestScanResultHandler.getInfo(ctx.volumes.get(0));
    synchronized (info) {
      info.shouldRun = true;
      info.notify();
    }

    // Wait for all the block pools to be scanned.
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        synchronized (info) {
          Statistics stats = ctx.blockScanner.getVolumeStats(
              ctx.volumes.get(0).getStorageID());
          if (stats.scansSinceRestart < 3) {
            LOG.info("Waiting for scansSinceRestart to reach 3 (it is {})",
                stats.scansSinceRestart);
            return false;
          }
          if (!stats.eof) {
            LOG.info("Waiting for eof.");
            return false;
          }
          return true;
        }
      }
    }, 3, 30000);

    Statistics stats = ctx.blockScanner.getVolumeStats(
        ctx.volumes.get(0).getStorageID());
    assertEquals(TOTAL_FILES, stats.blocksScannedSinceRestart);
    assertEquals(BYTES_SCANNED_PER_FILE * TOTAL_FILES,
        stats.bytesScannedInPastHour);
    ctx.close();
  }

  @Test(timeout=120000)
  public void testNextSorted() throws Exception {
    List<String> arr = new LinkedList<String>();
    arr.add("1");
    arr.add("3");
    arr.add("5");
    arr.add("7");
    Assert.assertEquals("3", FsVolumeImpl.nextSorted(arr, "2"));
    Assert.assertEquals("3", FsVolumeImpl.nextSorted(arr, "1"));
    Assert.assertEquals("1", FsVolumeImpl.nextSorted(arr, ""));
    Assert.assertEquals("1", FsVolumeImpl.nextSorted(arr, null));
    Assert.assertEquals(null, FsVolumeImpl.nextSorted(arr, "9"));
  }

  @Test(timeout=120000)
  public void testCalculateNeededBytesPerSec() throws Exception {
    // If we didn't check anything the last hour, we should scan now.
    Assert.assertTrue(
        VolumeScanner.calculateShouldScan("test", 100, 0, 0, 60));

    // If, on average, we checked 101 bytes/s checked during the last hour,
    // stop checking now.
    Assert.assertFalse(VolumeScanner.
        calculateShouldScan("test", 100, 101 * 3600, 1000, 5000));

    // Target is 1 byte / s, but we didn't scan anything in the last minute.
    // Should scan now.
    Assert.assertTrue(VolumeScanner.
        calculateShouldScan("test", 1, 3540, 0, 60));

    // Target is 1000000 byte / s, but we didn't scan anything in the last
    // minute.  Should scan now.
    Assert.assertTrue(VolumeScanner.
        calculateShouldScan("test", 100000L, 354000000L, 0, 60));

    Assert.assertFalse(VolumeScanner.
        calculateShouldScan("test", 100000L, 365000000L, 0, 60));
  }

  /**
   * Test that we can mark certain blocks as suspect, and get them quickly
   * rescanned that way.  See HDFS-7686 and HDFS-7548.
   */
  @Test(timeout=120000)
  public void testMarkSuspectBlock() throws Exception {
    Configuration conf = new Configuration();
    // Set a really long scan period.
    conf.setLong(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 100L);
    conf.set(INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER,
        TestScanResultHandler.class.getName());
    conf.setLong(INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS, 0L);
    final TestContext ctx = new TestContext(conf, 1);
    final int NUM_EXPECTED_BLOCKS = 10;
    ctx.createFiles(0, NUM_EXPECTED_BLOCKS, 1);
    final TestScanResultHandler.Info info =
        TestScanResultHandler.getInfo(ctx.volumes.get(0));
    String storageID = ctx.volumes.get(0).getStorageID();
    synchronized (info) {
      info.sem = new Semaphore(4);
      info.shouldRun = true;
      info.notify();
    }
    // Scan the first 4 blocks
    LOG.info("Waiting for the first 4 blocks to be scanned.");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        synchronized (info) {
          if (info.blocksScanned >= 4) {
            LOG.info("info = {}.  blockScanned has now reached 4.", info);
            return true;
          } else {
            LOG.info("info = {}.  Waiting for blockScanned to reach 4.", info);
            return false;
          }
        }
      }
    }, 50, 30000);
    // We should have scanned 4 blocks
    synchronized (info) {
      assertEquals("Expected 4 good blocks.", 4, info.goodBlocks.size());
      info.goodBlocks.clear();
      assertEquals("Expected 4 blocksScanned", 4, info.blocksScanned);
      assertEquals("Did not expect bad blocks.", 0, info.badBlocks.size());
      info.blocksScanned = 0;
    }
    ExtendedBlock first = ctx.getFileBlock(0, 0);
    ctx.datanode.getBlockScanner().markSuspectBlock(storageID, first);

    // When we increment the semaphore, the TestScanResultHandler will finish
    // adding the block that it was scanning previously (the 5th block).
    // We increment the semaphore twice so that the handler will also
    // get a chance to see the suspect block which we just requested the
    // VolumeScanner to process.
    info.sem.release(2);

    LOG.info("Waiting for 2 more blocks to be scanned.");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        synchronized (info) {
          if (info.blocksScanned >= 2) {
            LOG.info("info = {}.  blockScanned has now reached 2.", info);
            return true;
          } else {
            LOG.info("info = {}.  Waiting for blockScanned to reach 2.", info);
            return false;
          }
        }
      }
    }, 50, 30000);

    synchronized (info) {
      assertTrue("Expected block " + first + " to have been scanned.",
          info.goodBlocks.contains(first));
      assertEquals(2, info.goodBlocks.size());
      info.goodBlocks.clear();
      assertEquals("Did not expect bad blocks.", 0, info.badBlocks.size());
      assertEquals(2, info.blocksScanned);
      info.blocksScanned = 0;
    }

    // Re-mark the same block as suspect.
    ctx.datanode.getBlockScanner().markSuspectBlock(storageID, first);
    info.sem.release(10);

    LOG.info("Waiting for 5 more blocks to be scanned.");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        synchronized (info) {
          if (info.blocksScanned >= 5) {
            LOG.info("info = {}.  blockScanned has now reached 5.", info);
            return true;
          } else {
            LOG.info("info = {}.  Waiting for blockScanned to reach 5.", info);
            return false;
          }
        }
      }
    }, 50, 30000);
    synchronized (info) {
      assertEquals(5, info.goodBlocks.size());
      assertEquals(0, info.badBlocks.size());
      assertEquals(5, info.blocksScanned);
      // We should not have rescanned the "suspect block",
      // because it was recently rescanned by the suspect block system.
      // This is a test of the "suspect block" rate limiting.
      Assert.assertFalse("We should not " +
          "have rescanned block " + first + ", because it should have been " +
          "in recentSuspectBlocks.", info.goodBlocks.contains(first));
      info.blocksScanned = 0;
    }
    ctx.close();
  }

  /**
   * Test that blocks which are in the wrong location are ignored.
   */
  @Test(timeout=120000)
  public void testIgnoreMisplacedBlock() throws Exception {
    Configuration conf = new Configuration();
    // Set a really long scan period.
    conf.setLong(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 100L);
    conf.set(INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER,
        TestScanResultHandler.class.getName());
    conf.setLong(INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS, 0L);
    final TestContext ctx = new TestContext(conf, 1);
    final int NUM_FILES = 4;
    ctx.createFiles(0, NUM_FILES, 5);
    MaterializedReplica unreachableReplica = ctx.getMaterializedReplica(0, 1);
    ExtendedBlock unreachableBlock = ctx.getFileBlock(0, 1);
    unreachableReplica.makeUnreachable();
    final TestScanResultHandler.Info info =
        TestScanResultHandler.getInfo(ctx.volumes.get(0));
    String storageID = ctx.volumes.get(0).getStorageID();
    synchronized (info) {
      info.sem = new Semaphore(NUM_FILES);
      info.shouldRun = true;
      info.notify();
    }
    // Scan the first 4 blocks
    LOG.info("Waiting for the blocks to be scanned.");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        synchronized (info) {
          if (info.blocksScanned >= NUM_FILES - 1) {
            LOG.info("info = {}.  blockScanned has now reached " +
                info.blocksScanned, info);
            return true;
          } else {
            LOG.info("info = {}.  Waiting for blockScanned to reach " +
                (NUM_FILES - 1), info);
            return false;
          }
        }
      }
    }, 50, 30000);
    // We should have scanned 4 blocks
    synchronized (info) {
      assertFalse(info.goodBlocks.contains(unreachableBlock));
      assertFalse(info.badBlocks.contains(unreachableBlock));
      assertEquals("Expected 3 good blocks.", 3, info.goodBlocks.size());
      info.goodBlocks.clear();
      assertEquals("Expected 3 blocksScanned", 3, info.blocksScanned);
      assertEquals("Did not expect bad blocks.", 0, info.badBlocks.size());
      info.blocksScanned = 0;
    }
    info.sem.release(1);
    ctx.close();
  }

  /**
   * Test concurrent append and scan.
   * @throws Exception
   */
  @Test(timeout=120000)
  public void testAppendWhileScanning() throws Exception {
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.ALL);
    Configuration conf = new Configuration();
    // throttle the block scanner: 1MB per second
    conf.setLong(DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND, 1048576);
    // Set a really long scan period.
    conf.setLong(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 100L);
    conf.set(INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER,
        TestScanResultHandler.class.getName());
    conf.setLong(INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS, 0L);
    final int numExpectedFiles = 1;
    final int numExpectedBlocks = 1;
    final int numNameServices = 1;
    // the initial file length can not be too small.
    // Otherwise checksum file stream buffer will be pre-filled and
    // BlockSender will not see the updated checksum.
    final int initialFileLength = 2*1024*1024+100;
    final TestContext ctx = new TestContext(conf, numNameServices);
    // create one file, with one block.
    ctx.createFiles(0, numExpectedFiles, initialFileLength);
    final TestScanResultHandler.Info info =
        TestScanResultHandler.getInfo(ctx.volumes.get(0));
    String storageID = ctx.volumes.get(0).getStorageID();
    synchronized (info) {
      info.sem = new Semaphore(numExpectedBlocks*2);
      info.shouldRun = true;
      info.notify();
    }
    // VolumeScanner scans the first block when DN starts.
    // Due to throttler, this should take approximately 2 seconds.
    waitForRescan(info, numExpectedBlocks);

    // update throttler to schedule rescan immediately.
    // this number must be larger than initial file length, otherwise
    // throttler prevents immediate rescan.
    conf.setLong(DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND,
        initialFileLength+32*1024);
    BlockScanner.Conf newConf = new BlockScanner.Conf(conf);
    ctx.datanode.getBlockScanner().setConf(newConf);
    // schedule the first block for scanning
    ExtendedBlock first = ctx.getFileBlock(0, 0);
    ctx.datanode.getBlockScanner().markSuspectBlock(storageID, first);

    // append the file before VolumeScanner completes scanning the block,
    // which takes approximately 2 seconds to complete.
    FileSystem fs = ctx.cluster.getFileSystem();
    FSDataOutputStream os = fs.append(ctx.getPath(0));
    long seed = -1;
    int size = 200;
    final byte[] bytes = AppendTestUtil.randomBytes(seed, size);
    os.write(bytes);
    os.hflush();
    os.close();

    // verify that volume scanner does not find bad blocks after append.
    waitForRescan(info, numExpectedBlocks);

    GenericTestUtils.setLogLevel(DataNode.LOG, Level.INFO);
    ctx.close();
  }

  private void waitForRescan(final TestScanResultHandler.Info info,
      final int numExpectedBlocks)
      throws TimeoutException, InterruptedException {
    LOG.info("Waiting for the first 1 blocks to be scanned.");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        synchronized (info) {
          if (info.blocksScanned >= numExpectedBlocks) {
            LOG.info("info = {}.  blockScanned has now reached 1.", info);
            return true;
          } else {
            LOG.info("info = {}.  Waiting for blockScanned to reach 1.", info);
            return false;
          }
        }
      }
    }, 1000, 30000);

    synchronized (info) {
      assertEquals("Expected 1 good block.",
          numExpectedBlocks, info.goodBlocks.size());
      info.goodBlocks.clear();
      assertEquals("Expected 1 blocksScanned",
          numExpectedBlocks, info.blocksScanned);
      assertEquals("Did not expect bad blocks.", 0, info.badBlocks.size());
      info.blocksScanned = 0;
    }
  }

  @Test
  public void testSkipRecentAccessFile() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFS_BLOCK_SCANNER_SKIP_RECENT_ACCESSED, true);
    conf.setLong(INTERNAL_DFS_DATANODE_SCAN_PERIOD_MS, 2000L);
    conf.set(INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER,
        TestScanResultHandler.class.getName());
    final TestContext ctx = new TestContext(conf, 1);
    final int totalBlocks =  5;
    ctx.createFiles(0, totalBlocks, 4096);

    final TestScanResultHandler.Info info =
        TestScanResultHandler.getInfo(ctx.volumes.get(0));
    synchronized (info) {
      info.shouldRun = true;
      info.notify();
    }
    try {
      GenericTestUtils.waitFor(() -> {
        synchronized (info) {
          return info.blocksScanned > 0;
        }
      }, 10, 500);
      fail("Scan nothing for all files are accessed in last period.");
    } catch (TimeoutException e) {
      LOG.debug("Timeout for all files are accessed in last period.");
    }
    synchronized (info) {
      info.shouldRun = false;
      info.notify();
    }
    assertEquals("Should not scan block accessed in last period",
        0, info.blocksScanned);
    ctx.close();
  }

  /**
   * Test a DN does not wait for the VolumeScanners to finish before shutting
   * down.
   *
   * @throws Exception
   */
  @Test(timeout=120000)
  public void testFastDatanodeShutdown() throws Exception {
    // set the joinTimeOut to a value smaller than the completion time of the
    // VolumeScanner.
    testDatanodeShutDown(50L, 1000L, true);
  }

  /**
   * Test a DN waits for the VolumeScanners to finish before shutting down.
   *
   * @throws Exception
   */
  @Test(timeout=120000)
  public void testSlowDatanodeShutdown() throws Exception {
    // Set the joinTimeOut to a value larger than the completion time of the
    // volume scanner
    testDatanodeShutDown(TimeUnit.MINUTES.toMillis(5), 1000L,
        false);
  }

  private void testDatanodeShutDown(final long joinTimeOutMS,
      final long delayMS, boolean isFastShutdown) throws Exception {
    VolumeScannerCBInjector prevVolumeScannerCBInject =
        VolumeScannerCBInjector.get();
    try {
      DelayVolumeScannerResponseToInterrupt injectDelay =
          new DelayVolumeScannerResponseToInterrupt(delayMS);
      VolumeScannerCBInjector.set(injectDelay);
      Configuration conf = new Configuration();
      conf.setLong(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 100L);
      conf.set(INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER,
          TestScanResultHandler.class.getName());
      conf.setLong(INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS, 0L);
      conf.setLong(DFS_BLOCK_SCANNER_VOLUME_JOIN_TIMEOUT_MSEC_KEY,
          joinTimeOutMS);
      final TestContext ctx = new TestContext(conf, 1);
      final int numExpectedBlocks = 10;
      ctx.createFiles(0, numExpectedBlocks, 1);
      final TestScanResultHandler.Info info =
          TestScanResultHandler.getInfo(ctx.volumes.get(0));
      synchronized (info) {
        info.sem = new Semaphore(5);
        info.shouldRun = true;
        info.notify();
      }
      // make sure that the scanners are doing progress
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          synchronized (info) {
            return info.blocksScanned >= 1;
          }
        }
      }, 3, 30000);
      // mark the time where the
      long startShutdownTime = Time.monotonicNow();
      ctx.datanode.shutdown();
      long endShutdownTime = Time.monotonicNow();
      long totalTimeShutdown = endShutdownTime - startShutdownTime;

      if (isFastShutdown) {
        assertTrue("total shutdown time of DN must be smaller than "
                + "VolumeScanner Response time: " + totalTimeShutdown,
            totalTimeShutdown < delayMS
                && totalTimeShutdown >= joinTimeOutMS);
        // wait for scanners to terminate before we move to the next test.
        injectDelay.waitForScanners();
        return;
      }
      assertTrue("total shutdown time of DN must be larger than " +
              "VolumeScanner Response time: " + totalTimeShutdown,
          totalTimeShutdown >= delayMS
              && totalTimeShutdown < joinTimeOutMS);
    } finally {
      // restore the VolumeScanner callback injector.
      VolumeScannerCBInjector.set(prevVolumeScannerCBInject);
    }
  }

  private static class DelayVolumeScannerResponseToInterrupt extends
      VolumeScannerCBInjector {
    final private long delayAmountNS;
    final private Set<VolumeScanner> scannersToShutDown;

    DelayVolumeScannerResponseToInterrupt(long delayMS) {
      delayAmountNS =
          TimeUnit.NANOSECONDS.convert(delayMS, TimeUnit.MILLISECONDS);
      scannersToShutDown = ConcurrentHashMap.newKeySet();
    }

    @Override
    public void preSavingBlockIteratorTask(VolumeScanner volumeScanner) {
      long remainingTimeNS = delayAmountNS;
      // busy delay without sleep().
      long startTime = Time.monotonicNowNanos();
      long endTime = startTime + remainingTimeNS;
      long currTime, waitTime = 0;
      while ((currTime = Time.monotonicNowNanos()) < endTime) {
        // empty loop. No need to sleep because the thread could be in an
        // interrupt mode.
        waitTime = currTime - startTime;
      }
      LOG.info("VolumeScanner {} finished delayed Task after {}",
          volumeScanner.toString(),
          TimeUnit.NANOSECONDS.convert(waitTime, TimeUnit.MILLISECONDS));
    }

    @Override
    public void shutdownCallBack(VolumeScanner volumeScanner) {
      scannersToShutDown.add(volumeScanner);
    }

    @Override
    public void terminationCallBack(VolumeScanner volumeScanner) {
      scannersToShutDown.remove(volumeScanner);
    }

    public void waitForScanners() throws TimeoutException,
        InterruptedException {
      GenericTestUtils.waitFor(
          () -> scannersToShutDown.isEmpty(), 10, 120000);
    }
  }
}
