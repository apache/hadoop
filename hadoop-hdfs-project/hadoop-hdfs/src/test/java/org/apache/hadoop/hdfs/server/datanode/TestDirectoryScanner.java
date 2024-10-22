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

import static org.apache.hadoop.hdfs.protocol.Block.BLOCK_FILE_PREFIX;
import static org.apache.hadoop.util.Shell.getMemlockLimit;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager.LockLevel;
import org.apache.hadoop.hdfs.server.datanode.DirectoryScanner.ReportCompiler;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DataNodeVolumeMetrics;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi.FsVolumeReferences;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetTestUtil;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.LazyPersistTestCase;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Tests {@link DirectoryScanner} handling of differences between blocks on the
 * disk and block in memory.
 */
public class TestDirectoryScanner {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDirectoryScanner.class);
  private static final int DEFAULT_GEN_STAMP = 9999;

  private MiniDFSCluster cluster;
  private String bpid;
  private DFSClient client;
  private FsDatasetSpi<? extends FsVolumeSpi> fds = null;
  private DirectoryScanner scanner = null;
  private final Random rand = new Random();
  private final Random r = new Random();
  private static final int BLOCK_LENGTH = 100;

  public Configuration getConfiguration() {
    Configuration configuration = new HdfsConfiguration();
    configuration.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_LENGTH);
    configuration.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);
    configuration.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    configuration.setLong(DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
        getMemlockLimit(Long.MAX_VALUE));
    return configuration;
  }

  @Before
  public void setup() {
    LazyPersistTestCase.initCacheManipulator();
  }

  /** create a file with a length of <code>fileLen</code>. */
  private List<LocatedBlock> createFile(String fileNamePrefix, long fileLen,
      boolean isLazyPersist) throws IOException {
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/" + fileNamePrefix + ".dat");
    DFSTestUtil.createFile(fs, filePath, isLazyPersist, 1024, fileLen,
        BLOCK_LENGTH, (short) 1, r.nextLong(), false);
    return client.getLocatedBlocks(filePath.toString(), 0, fileLen)
        .getLocatedBlocks();
  }

  /** Truncate a block file. */
  private long truncateBlockFile() throws IOException {
    try (AutoCloseableLock lock = fds.acquireDatasetLockManager().writeLock(
        LockLevel.BLOCK_POOl, bpid)) {
      for (ReplicaInfo b : FsDatasetTestUtil.getReplicas(fds, bpid)) {
        File f = new File(b.getBlockURI());
        File mf = new File(b.getMetadataURI());
        // Truncate a block file that has a corresponding metadata file
        if (f.exists() && f.length() != 0 && mf.exists()) {
          FileOutputStream s = null;
          FileChannel channel = null;
          try {
            s = new FileOutputStream(f);
            channel = s.getChannel();
            channel.truncate(0);
            LOG.info("Truncated block file " + f.getAbsolutePath());
            return b.getBlockId();
          } finally {
            IOUtils.cleanupWithLogger(LOG, channel, s);
          }
        }
      }
    }
    return 0;
  }

  /** Delete a block file */
  private long deleteBlockFile() {
    try (AutoCloseableLock lock = fds.acquireDatasetLockManager().
        writeLock(LockLevel.BLOCK_POOl, bpid)) {
      for (ReplicaInfo b : FsDatasetTestUtil.getReplicas(fds, bpid)) {
        File f = new File(b.getBlockURI());
        File mf = new File(b.getMetadataURI());
        // Delete a block file that has corresponding metadata file
        if (f.exists() && mf.exists() && f.delete()) {
          LOG.info("Deleting block file " + f.getAbsolutePath());
          return b.getBlockId();
        }
      }
    }
    return 0;
  }

  /** Delete block meta file */
  private long deleteMetaFile() {
    try (AutoCloseableLock lock = fds.acquireDatasetLockManager().
        writeLock(LockLevel.BLOCK_POOl, bpid)) {
      for (ReplicaInfo b : FsDatasetTestUtil.getReplicas(fds, bpid)) {
        // Delete a metadata file
        if (b.metadataExists() && b.deleteMetadata()) {
          LOG.info("Deleting metadata " + b.getMetadataURI());
          return b.getBlockId();
        }
      }
    }
    return 0;
  }

  /**
   * Duplicate the given block on all volumes.
   *
   * @param blockId
   * @throws IOException
   */
  private void duplicateBlock(long blockId) throws IOException {
    try (AutoCloseableLock lock = fds.acquireDatasetLockManager().
        writeLock(LockLevel.BLOCK_POOl, bpid)) {
      ReplicaInfo b = FsDatasetTestUtil.fetchReplicaInfo(fds, bpid, blockId);
      try (FsDatasetSpi.FsVolumeReferences volumes =
          fds.getFsVolumeReferences()) {
        for (FsVolumeSpi v : volumes) {
          if (v.getStorageID().equals(b.getVolume().getStorageID())) {
            continue;
          }

          // Volume without a copy of the block. Make a copy now.
          File sourceBlock = new File(b.getBlockURI());
          File sourceMeta = new File(b.getMetadataURI());
          URI sourceRoot = b.getVolume().getStorageLocation().getUri();
          URI destRoot = v.getStorageLocation().getUri();

          String relativeBlockPath =
              sourceRoot.relativize(sourceBlock.toURI()).getPath();
          String relativeMetaPath =
              sourceRoot.relativize(sourceMeta.toURI()).getPath();

          File destBlock =
              new File(new File(destRoot).toString(), relativeBlockPath);
          File destMeta =
              new File(new File(destRoot).toString(), relativeMetaPath);

          destBlock.getParentFile().mkdirs();
          FileUtils.copyFile(sourceBlock, destBlock);
          FileUtils.copyFile(sourceMeta, destMeta);

          if (destBlock.exists() && destMeta.exists()) {
            LOG.info("Copied " + sourceBlock + " ==> " + destBlock);
            LOG.info("Copied " + sourceMeta + " ==> " + destMeta);
          }
        }
      }
    }
  }

  /** Get a random blockId that is not used already. */
  private long getFreeBlockId() {
    long id = rand.nextLong();
    while (true) {
      id = rand.nextLong();
      if (FsDatasetTestUtil.fetchReplicaInfo(fds, bpid, id) == null) {
        break;
      }
    }
    return id;
  }

  private String getBlockFile(long id) {
    return BLOCK_FILE_PREFIX + id;
  }

  private String getMetaFile(long id) {
    return BLOCK_FILE_PREFIX + id + "_" + DEFAULT_GEN_STAMP
        + Block.METADATA_EXTENSION;
  }

  /** Create a block file in a random volume. */
  private long createBlockFile(long id) throws IOException {
    try (
        FsDatasetSpi.FsVolumeReferences volumes = fds.getFsVolumeReferences()) {
      int numVolumes = volumes.size();
      int index = rand.nextInt(numVolumes - 1);
      File finalizedDir =
          ((FsVolumeImpl) volumes.get(index)).getFinalizedDir(bpid);
      File file = new File(finalizedDir, getBlockFile(id));
      if (file.createNewFile()) {
        LOG.info("Created block file " + file.getName());
      }
    }
    return id;
  }

  /** Create a metafile in a random volume */
  private long createMetaFile() throws IOException {
    long id = getFreeBlockId();
    try (FsDatasetSpi.FsVolumeReferences refs = fds.getFsVolumeReferences()) {
      int numVolumes = refs.size();
      int index = rand.nextInt(numVolumes - 1);
      File finalizedDir =
          ((FsVolumeImpl) refs.get(index)).getFinalizedDir(bpid);
      File file = new File(finalizedDir, getMetaFile(id));
      if (file.createNewFile()) {
        LOG.info("Created metafile " + file.getName());
      }
    }
    return id;
  }

  /** Create block file and corresponding metafile in a rondom volume. */
  private long createBlockMetaFile(long id) throws IOException {

    try (FsDatasetSpi.FsVolumeReferences refs = fds.getFsVolumeReferences()) {
      int numVolumes = refs.size();
      int index = rand.nextInt(numVolumes - 1);

      File finalizedDir =
          ((FsVolumeImpl) refs.get(index)).getFinalizedDir(bpid);
      File file = new File(finalizedDir, getBlockFile(id));
      if (file.createNewFile()) {
        LOG.info("Created block file " + file.getName());

        // Create files with same prefix as block file but extension names
        // such that during sorting, these files appear around meta file
        // to test how DirectoryScanner handles extraneous files
        String name1 = file.getAbsolutePath() + ".l";
        String name2 = file.getAbsolutePath() + ".n";
        file = new File(name1);
        if (file.createNewFile()) {
          LOG.info("Created extraneous file " + name1);
        }

        file = new File(name2);
        if (file.createNewFile()) {
          LOG.info("Created extraneous file " + name2);
        }

        file = new File(finalizedDir, getMetaFile(id));
        if (file.createNewFile()) {
          LOG.info("Created metafile " + file.getName());
        }
      }
    }
    return id;
  }

  private void scan(long totalBlocks, int diffsize, long missingMetaFile,
      long missingBlockFile, long missingMemoryBlocks, long mismatchBlocks)
      throws IOException, InterruptedException, TimeoutException {
    scan(totalBlocks, diffsize, missingMetaFile, missingBlockFile,
        missingMemoryBlocks, mismatchBlocks, 0);
  }

  private void scan(long totalBlocks, int diffsize, long missingMetaFile,
      long missingBlockFile, long missingMemoryBlocks, long mismatchBlocks,
      long duplicateBlocks)
      throws IOException, InterruptedException, TimeoutException {
    scanner.reconcile();

    GenericTestUtils.waitFor(() -> {
      try {
        verifyStats(totalBlocks, diffsize, missingMetaFile, missingBlockFile,
            missingMemoryBlocks, mismatchBlocks, duplicateBlocks);
      } catch (AssertionError ex) {
        LOG.warn("Assertion Error", ex);
        return false;
      }

      return true;
    }, 100, 2000);
  }

  private void verifyStats(long totalBlocks, int diffsize, long missingMetaFile,
      long missingBlockFile, long missingMemoryBlocks, long mismatchBlocks,
      long duplicateBlocks) {
    Collection<FsVolumeSpi.ScanInfo> diff = scanner.diffs.getScanInfo(bpid);
    assertEquals(diffsize, diff.size());

    DirectoryScanner.Stats stats = scanner.stats.get(bpid);
    assertNotNull(stats);
    assertEquals(totalBlocks, stats.totalBlocks);
    assertEquals(missingMetaFile, stats.missingMetaFile);
    assertEquals(missingBlockFile, stats.missingBlockFile);
    assertEquals(missingMemoryBlocks, stats.missingMemoryBlocks);
    assertEquals(mismatchBlocks, stats.mismatchBlocks);
    assertEquals(duplicateBlocks, stats.duplicateBlocks);
  }

  @Test(timeout = 300000)
  public void testRetainBlockOnPersistentStorage() throws Exception {
    Configuration conf = getConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
        .storageTypes(
            new StorageType[] { StorageType.RAM_DISK, StorageType.DEFAULT })
        .numDataNodes(1).build();
    try {
      cluster.waitActive();
      bpid = cluster.getNamesystem().getBlockPoolId();
      fds = DataNodeTestUtils.getFSDataset(cluster.getDataNodes().get(0));
      client = cluster.getFileSystem().getClient();
      scanner = new DirectoryScanner(fds, conf);
      scanner.setRetainDiffs(true);
      FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

      // Add a file with 1 block
      List<LocatedBlock> blocks =
          createFile(GenericTestUtils.getMethodName(), BLOCK_LENGTH, false);

      // Ensure no difference between volumeMap and disk.
      scan(1, 0, 0, 0, 0, 0);

      // Make a copy of the block on RAM_DISK and ensure that it is
      // picked up by the scanner.
      duplicateBlock(blocks.get(0).getBlock().getBlockId());
      scan(2, 1, 0, 0, 0, 0, 1);
      verifyStorageType(blocks.get(0).getBlock().getBlockId(), false);
      scan(1, 0, 0, 0, 0, 0);

    } finally {
      if (scanner != null) {
        scanner.shutdown();
        scanner = null;
      }
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * test scan only meta file NOT generate wrong folder structure warn log.
   */
  @Test(timeout=600000)
  public void testScanDirectoryStructureWarn() throws Exception {

    //add a logger stream to check what has printed to log
    ByteArrayOutputStream loggerStream = new ByteArrayOutputStream();
    org.apache.log4j.Logger rootLogger =
        org.apache.log4j.Logger.getRootLogger();
    GenericTestUtils.setRootLogLevel(Level.INFO);
    WriterAppender writerAppender =
        new WriterAppender(new SimpleLayout(), loggerStream);
    rootLogger.addAppender(writerAppender);

    Configuration conf = getConfiguration();
    cluster = new MiniDFSCluster
        .Builder(conf)
        .storageTypes(new StorageType[] {
            StorageType.RAM_DISK, StorageType.DEFAULT })
        .numDataNodes(1)
        .build();
    try {
      cluster.waitActive();
      bpid = cluster.getNamesystem().getBlockPoolId();
      fds = DataNodeTestUtils.getFSDataset(cluster.getDataNodes().get(0));
      client = cluster.getFileSystem().getClient();
      scanner = new DirectoryScanner(fds, conf);
      scanner.setRetainDiffs(true);
      FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

      // Create a file file on RAM_DISK
      createFile(GenericTestUtils.getMethodName(), BLOCK_LENGTH, true);

      // Ensure no difference between volumeMap and disk.
      scan(1, 0, 0, 0, 0, 0);

      //delete thre block file , left the meta file alone
      deleteBlockFile();

      //scan to ensure log warn not printed
      scan(1, 1, 0, 1, 0, 0, 0);

      //ensure the warn log not appear and missing block log do appear
      String logContent = new String(loggerStream.toByteArray());
      String missingBlockWarn = "Deleted a metadata file" +
          " for the deleted block";
      String dirStructureWarnLog = " found in invalid directory." +
          "  Expected directory: ";
      assertFalse("directory check print meaningless warning message",
          logContent.contains(dirStructureWarnLog));
      assertTrue("missing block warn log not appear",
          logContent.contains(missingBlockWarn));
      LOG.info("check pass");

    } finally {
      if (scanner != null) {
        scanner.shutdown();
        scanner = null;
      }
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 300000)
  public void testDeleteBlockOnTransientStorage() throws Exception {
    Configuration conf = getConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
        .storageTypes(
            new StorageType[] { StorageType.RAM_DISK, StorageType.DEFAULT })
        .numDataNodes(1).build();
    try {
      cluster.waitActive();
      bpid = cluster.getNamesystem().getBlockPoolId();
      fds = DataNodeTestUtils.getFSDataset(cluster.getDataNodes().get(0));
      client = cluster.getFileSystem().getClient();
      scanner = new DirectoryScanner(fds, conf);
      scanner.setRetainDiffs(true);
      FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

      // Create a file on RAM_DISK
      List<LocatedBlock> blocks =
          createFile(GenericTestUtils.getMethodName(), BLOCK_LENGTH, true);

      // Ensure no difference between volumeMap and disk.
      scan(1, 0, 0, 0, 0, 0);

      // Make a copy of the block on DEFAULT storage and ensure that it is
      // picked up by the scanner.
      duplicateBlock(blocks.get(0).getBlock().getBlockId());
      scan(2, 1, 0, 0, 0, 0, 1);

      // Ensure that the copy on RAM_DISK was deleted.
      verifyStorageType(blocks.get(0).getBlock().getBlockId(), false);
      scan(1, 0, 0, 0, 0, 0);

    } finally {
      if (scanner != null) {
        scanner.shutdown();
        scanner = null;
      }
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 600000)
  public void testRegularBlock() throws Exception {
    Configuration conf = getConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      bpid = cluster.getNamesystem().getBlockPoolId();
      fds = DataNodeTestUtils.getFSDataset(cluster.getDataNodes().get(0));
      client = cluster.getFileSystem().getClient();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY, 1);
      // log trace
      GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer.
          captureLogs(NameNode.stateChangeLog);
      // Add files with 5 blocks
      createFile(GenericTestUtils.getMethodName(), BLOCK_LENGTH * 5, false);

      List<ReplicaInfo> infos = new ArrayList<>(FsDatasetTestUtil.getReplicas(fds, bpid));
      ReplicaInfo lastReplica = infos.get(infos.size() - 1);
      ReplicaInfo penultimateReplica = infos.get(infos.size() - 2);

      String blockParent = new File(lastReplica.getBlockURI().getPath()).getParent();
      File lastBlockFile = new File(blockParent, getBlockFile(lastReplica.getBlockId()));
      File penultimateBlockFile = new File(blockParent,
          getBlockFile(penultimateReplica.getBlockId()));
      FileUtil.symLink(lastBlockFile.toString(), penultimateBlockFile.toString());
      ExtendedBlock block = new ExtendedBlock(bpid, penultimateReplica.getBlockId());

      scanner = new DirectoryScanner(fds, conf);
      scanner.setRetainDiffs(true);
      scanner.reconcile();
      DirectoryScanner.Stats stats = scanner.stats.get(bpid);
      assertNotNull(stats);
      assertEquals(1, stats.mismatchBlocks);

      // check nn log
      String msg = "*DIR* reportBadBlocks for block: " + bpid + ":" +
          getBlockFile(block.getBlockId());
      assertTrue(logCapturer.getOutput().contains(msg));
    } finally {
      if (scanner != null) {
        scanner.shutdown();
        scanner = null;
      }
      cluster.shutdown();
    }
  }

  @Test(timeout = 600000)
  public void testDirectoryScannerDuringUpdateBlockMeta() throws Exception {
    Configuration conf = getConfiguration();
    DataNodeFaultInjector oldDnInjector = DataNodeFaultInjector.get();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      cluster.waitActive();
      bpid = cluster.getNamesystem().getBlockPoolId();
      fds = DataNodeTestUtils.getFSDataset(cluster.getDataNodes().get(0));
      client = cluster.getFileSystem().getClient();
      DistributedFileSystem fs = cluster.getFileSystem();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY, 1);
      GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer.
          captureLogs(NameNode.stateChangeLog);

      // Add files with 1 blocks.
      Path path = new Path("/testFile");
      DFSTestUtil.createFile(fs, path, 50, (short) 1, 0);
      DFSTestUtil.waitReplication(fs, path, (short) 1);
      LocatedBlock lb = DFSTestUtil.getAllBlocks(fs, path).get(0);
      DatanodeInfo[] loc = lb.getLocations();
      assertEquals(1, loc.length);
      DataNodeFaultInjector dnFaultInjector = new DataNodeFaultInjector() {
        @Override
        public void delayDiffRecord() {
          try {
            Thread.sleep(8000);
          } catch (InterruptedException e) {
            // Ignore exception.
          }
        }
      };

      DataNodeFaultInjector.set(dnFaultInjector);
      ExecutorService executorService = Executors.newFixedThreadPool(2);
      try {
        Future<?> directoryScannerFuture = executorService.submit(() -> {
          try {
            // Submit tasks run directory scanner.
            scanner = new DirectoryScanner(fds, conf);
            scanner.setRetainDiffs(true);
            scanner.reconcile();
          } catch (IOException e) {
            // Ignore exception.
          }
        });

        Future<?> appendBlockFuture = executorService.submit(() -> {
          try {
            // Submit tasks run append file.
            DFSTestUtil.appendFile(fs, path, 50);
          } catch (Exception e) {
            // Ignore exception.
          }
        });

        // Wait for both tasks to complete.
        directoryScannerFuture.get();
        appendBlockFuture.get();
      } finally {
        executorService.shutdown();
      }

      DirectoryScanner.Stats stats = scanner.stats.get(bpid);
      assertNotNull(stats);
      assertEquals(1, stats.mismatchBlocks);

      // Check nn log will not reportBadBlocks message.
      String msg = "*DIR* reportBadBlocks for block: " + bpid + ":" +
          getBlockFile(lb.getBlock().getBlockId());
      assertFalse(logCapturer.getOutput().contains(msg));
    } finally {
      if (scanner != null) {
        scanner.shutdown();
        scanner = null;
      }
      DataNodeFaultInjector.set(oldDnInjector);
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 600000)
  public void testDirectoryScanner() throws Exception {
    // Run the test with and without parallel scanning
    for (int parallelism = 1; parallelism < 3; parallelism++) {
      runTest(parallelism);
    }
  }

  public void runTest(int parallelism) throws Exception {
    Configuration conf = getConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      bpid = cluster.getNamesystem().getBlockPoolId();
      fds = DataNodeTestUtils.getFSDataset(cluster.getDataNodes().get(0));
      client = cluster.getFileSystem().getClient();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
          parallelism);

      scanner = new DirectoryScanner(fds, conf);
      scanner.setRetainDiffs(true);

      // Add files with 100 blocks
      createFile(GenericTestUtils.getMethodName(), BLOCK_LENGTH * 100, false);
      long totalBlocks = 100;

      // Test1: No difference between volumeMap and disk
      scan(100, 0, 0, 0, 0, 0);

      // Test2: block metafile is missing
      long blockId = deleteMetaFile();
      scan(totalBlocks, 1, 1, 0, 0, 1);
      verifyGenStamp(blockId, HdfsConstants.GRANDFATHER_GENERATION_STAMP);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test3: block file is missing
      blockId = deleteBlockFile();
      scan(totalBlocks, 1, 0, 1, 0, 0);
      totalBlocks--;
      verifyDeletion(blockId);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test4: A block file exists for which there is no metafile and
      // a block in memory
      blockId = createBlockFile(blockId);
      totalBlocks++;
      scan(totalBlocks, 1, 1, 0, 1, 0);
      verifyAddition(blockId, HdfsConstants.GRANDFATHER_GENERATION_STAMP, 0);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test5: A metafile exists for which there is no block file and
      // a block in memory
      blockId = createMetaFile();
      scan(totalBlocks + 1, 1, 0, 1, 1, 0);
      File metafile = new File(getMetaFile(blockId));
      assertTrue(!metafile.exists());
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test6: A block file and metafile exists for which there is no block in
      blockId = deleteBlockFile();
      scan(totalBlocks, 1, 0, 1, 0, 0);
      totalBlocks--;
      verifyDeletion(blockId);

      blockId = createBlockMetaFile(blockId);
      totalBlocks++;
      scan(totalBlocks, 1, 0, 0, 1, 0);
      verifyAddition(blockId, DEFAULT_GEN_STAMP, 0);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test7: Delete bunch of metafiles
      for (int i = 0; i < 10; i++) {
        blockId = deleteMetaFile();
      }
      scan(totalBlocks, 10, 10, 0, 0, 10);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test8: Delete bunch of block files and record the ids.
      List<Long> ids = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        ids.add(deleteBlockFile());
      }
      scan(totalBlocks, 10, 0, 10, 0, 0);
      totalBlocks -= 10;
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test9: create a bunch of blocks files
      for (int i = 0; i < 10; i++) {
        blockId = createBlockFile(ids.get(i));
      }
      totalBlocks += 10;
      scan(totalBlocks, 10, 10, 0, 10, 0);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test10: create a bunch of metafiles
      for (int i = 0; i < 10; i++) {
        blockId = createMetaFile();
      }
      scan(totalBlocks + 10, 10, 0, 10, 10, 0);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test11: create a bunch block files and meta files
      ids.clear();
      for (int i = 0; i < 10; i++) {
        ids.add(deleteBlockFile());
      }
      scan(totalBlocks, 10, 0, 10, 0, 0);
      totalBlocks -= 10;

      for (int i = 0; i < 10; i++) {
        blockId = createBlockMetaFile(ids.get(i));
      }
      totalBlocks += 10;
      scan(totalBlocks, 10, 0, 0, 10, 0);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test12: truncate block files to test block length mismatch
      for (int i = 0; i < 10; i++) {
        truncateBlockFile();
      }
      scan(totalBlocks, 10, 0, 0, 0, 10);
      scan(totalBlocks, 0, 0, 0, 0, 0);

      // Test13: all the conditions combined
      long blockId1 = deleteBlockFile();
      long blockId2 = deleteBlockFile();
      scan(totalBlocks, 2, 0, 2, 0, 0);
      totalBlocks -= 2;
      verifyDeletion(blockId1);
      verifyDeletion(blockId2);

      createMetaFile();
      createBlockFile(blockId1);
      createBlockMetaFile(blockId2);
      deleteMetaFile();
      deleteBlockFile();
      truncateBlockFile();
      scan(totalBlocks + 3, 6, 2, 2, 3, 2);
      scan(totalBlocks + 1, 0, 0, 0, 0, 0);

      // Test14: make sure no throttling is happening
      assertTrue("Throttle appears to be engaged",
          scanner.timeWaitingMs.get() < 10L);
      assertTrue("Report complier threads logged no execution time",
          scanner.timeRunningMs.get() > 0L);

      scanner.shutdown();
      assertFalse(scanner.getRunStatus());

    } finally {
      if (scanner != null) {
        scanner.shutdown();
        scanner = null;
      }
      cluster.shutdown();
    }
  }

  @Test
  public void testCalculateSleepTime() {
    // disabled
    runCalculateSleepTimeTest(0, 100, 0);
    runCalculateSleepTimeTest(-2, 100, 0);
    runCalculateSleepTimeTest(1001, 10000, 0);
    // below the limit
    runCalculateSleepTimeTest(100, 99, 899);
    // equals to the limit
    runCalculateSleepTimeTest(100, 100, 900);
    // above the limit
    runCalculateSleepTimeTest(100, 101, 901);
    // above 1s
    runCalculateSleepTimeTest(100, 1001, 1000);
  }

  public void runCalculateSleepTimeTest(int throttleLimitMsPerSec, int runningTime, long expected) {
    FsDatasetSpi<?> fds = mock(FsDatasetSpi.class);
    FsVolumeSpi volumeSpi = mock(FsVolumeImpl.class);
    Configuration conf = new Configuration(getConfiguration());
    conf.setInt(
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
            throttleLimitMsPerSec);
    scanner = new DirectoryScanner(fds, conf);
    ReportCompiler compiler = scanner.new ReportCompiler(volumeSpi);
    assertTrue(compiler.calculateSleepTime(runningTime) == expected);
  }

  /**
   * Test that the timeslice throttle limits the report compiler thread's
   * execution time correctly. We test by mocking a FsVolumeSpi throttling
   * every {@link DFSConfigKeys#DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY}
   * and comparing the time spent waiting to the time spent running.
   *
   * @throws Exception thrown on unexpected failure
   */
  @Test
  public void testThrottling() throws Exception {
    FsDatasetSpi<?> fds = mock(FsDatasetSpi.class);
    Configuration conf = new Configuration(getConfiguration());
    bpid = "bp01";

    FsVolumeSpi volumeSpi = mock(FsVolumeImpl.class);
    FsVolumeReference reference = mock(FsVolumeReference.class);
    when(reference.getVolume()).thenReturn(volumeSpi);
    when(volumeSpi.obtainReference()).thenReturn(reference);
    when(fds.getFsVolumeReferences()).thenAnswer((Answer<FsVolumeReferences>)
            invocationOnMock ->
            new FsVolumeReferences(Lists.newArrayList(volumeSpi, volumeSpi)));
    when(volumeSpi.getBlockPoolList()).thenReturn(new String[]{bpid});
    doAnswer(invocationOnMock -> {
      ReportCompiler compiler = invocationOnMock.getArgument(2, ReportCompiler.class);
      int throttleLimitMs = conf.getInt(
              DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
              0);
      for (int i = 0; i < 3; i++) {
        compiler.throttle();
        // Make sure to throttle next time.
        Thread.sleep(Math.max(1, throttleLimitMs));
      }
      return null;
    }).when(volumeSpi).compileReport(any(), any(), any());

    float ratio;
    int blocks = 0;

    conf.setInt(
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
            100);
    scanner = new DirectoryScanner(fds, conf);
    ratio = runThrottleTest(blocks);

    // Waiting should be about 9x running.
    LOG.info("RATIO: " + ratio);
    assertTrue("Throttle is too restrictive", ratio <= 10f);
    assertTrue("Throttle is too permissive" + ratio, ratio >= 7f);

    // Test with a different limit
    conf.setInt(
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
            200);
    scanner = new DirectoryScanner(fds, conf);
    ratio = runThrottleTest(blocks);

    // Waiting should be about 4x running.
    LOG.info("RATIO: " + ratio);
    assertTrue("Throttle is too restrictive", ratio <= 4.5f);
    assertTrue("Throttle is too permissive", ratio >= 2.75f);

    // Test with more than 1 thread
    conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY, 3);
    conf.setInt(
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
            100);
    scanner = new DirectoryScanner(fds, conf);
    ratio = runThrottleTest(blocks);

    // Waiting should be about 9x running.
    LOG.info("RATIO: " + ratio);
    assertTrue("Throttle is too restrictive", ratio <= 10f);
    assertTrue("Throttle is too permissive", ratio >= 7f);

    // Test with no limit
    scanner = new DirectoryScanner(fds, getConfiguration());
    scanner.setRetainDiffs(true);
    scan(blocks, 0, 0, 0, 0, 0);
    scanner.shutdown();
    assertFalse(scanner.getRunStatus());

    assertTrue("Throttle appears to be engaged",
            scanner.timeWaitingMs.get() < 10L);
    assertTrue("Report complier threads logged no execution time",
            scanner.timeRunningMs.get() > 0L);

    // Test with a 1ms limit. This also tests whether the scanner can be
    // shutdown cleanly in mid stride.
    conf.setInt(
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
            1);
    ratio = 0.0f;
    ScheduledExecutorService interruptor =
            Executors.newScheduledThreadPool(1);

    try {
      scanner = new DirectoryScanner(fds, conf);
      scanner.setRetainDiffs(true);

      final AtomicLong nowMs = new AtomicLong();

      // Stop the scanner after 2 seconds because otherwise it will take an
      // eternity to complete it's run
      interruptor.schedule(new Runnable() {
        @Override
        public void run() {
          nowMs.set(Time.monotonicNow());
          scanner.shutdown();
        }
      }, 2L, TimeUnit.SECONDS);

      scanner.reconcile();
      assertFalse(scanner.getRunStatus());

      long finalMs = nowMs.get();

      // If the scan didn't complete before the shutdown was run, check
      // that the shutdown was timely
      if (finalMs > 0) {
        LOG.info("Scanner took " + (Time.monotonicNow() - finalMs)
                + "ms to shutdown");
        assertTrue("Scanner took too long to shutdown",
                Time.monotonicNow() - finalMs < 1000L);
      }

      ratio =
              (float) scanner.timeWaitingMs.get() / scanner.timeRunningMs.get();
    } finally {
      interruptor.shutdown();
    }

    // We just want to test that it waits a lot, but it also runs some
    LOG.info("RATIO: " + ratio);
    assertTrue("Throttle is too permissive", ratio > 8);
    assertTrue("Report complier threads logged no execution time",
            scanner.timeRunningMs.get() > 0L);

    // Test with a 0 limit, i.e. disabled
    conf.setInt(
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
            0);
    scanner = new DirectoryScanner(fds, conf);
    scanner.setRetainDiffs(true);
    scan(blocks, 0, 0, 0, 0, 0);
    scanner.shutdown();
    assertFalse(scanner.getRunStatus());

    assertTrue("Throttle appears to be engaged",
            scanner.timeWaitingMs.get() < 10L);
    assertTrue("Report complier threads logged no execution time",
            scanner.timeRunningMs.get() > 0L);

    // Test with a 1000 limit, i.e. disabled
    conf.setInt(
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
            1000);
    scanner = new DirectoryScanner(fds, conf);
    scanner.setRetainDiffs(true);
    scan(blocks, 0, 0, 0, 0, 0);
    scanner.shutdown();
    assertFalse(scanner.getRunStatus());

    assertTrue("Throttle appears to be engaged",
            scanner.timeWaitingMs.get() < 10L);
    assertTrue("Report complier threads logged no execution time",
            scanner.timeRunningMs.get() > 0L);

    // Test that throttle works from regular start
    conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY, 1);
    conf.setInt(
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
            10);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1);
    scanner = new DirectoryScanner(fds, conf);
    scanner.setRetainDiffs(true);
    scanner.start();

    int count = 50;

    while ((count > 0) && (scanner.timeWaitingMs.get() < 500L)) {
      Thread.sleep(100L);
      count -= 1;
    }

    scanner.shutdown();
    assertFalse(scanner.getRunStatus());
    assertTrue("Throttle does not appear to be engaged", count > 0);
  }

  private float runThrottleTest(int blocks)
      throws IOException, InterruptedException, TimeoutException {
    scanner.setRetainDiffs(true);
    scan(blocks, 0, 0, 0, 0, 0);
    scanner.shutdown();
    assertFalse(scanner.getRunStatus());

    return (float) scanner.timeWaitingMs.get() / scanner.timeRunningMs.get();
  }

  private void verifyAddition(long blockId, long genStamp, long size) {
    final ReplicaInfo replicainfo;
    replicainfo = FsDatasetTestUtil.fetchReplicaInfo(fds, bpid, blockId);
    assertNotNull(replicainfo);

    // Added block has the same file as the one created by the test
    File file = new File(getBlockFile(blockId));
    assertEquals(file.getName(),
        FsDatasetTestUtil.getFile(fds, bpid, blockId).getName());

    // Generation stamp is same as that of created file
    assertEquals(genStamp, replicainfo.getGenerationStamp());

    // File size matches
    assertEquals(size, replicainfo.getNumBytes());
  }

  private void verifyDeletion(long blockId) {
    // Ensure block does not exist in memory
    assertNull(FsDatasetTestUtil.fetchReplicaInfo(fds, bpid, blockId));
  }

  private void verifyGenStamp(long blockId, long genStamp) {
    final ReplicaInfo memBlock;
    memBlock = FsDatasetTestUtil.fetchReplicaInfo(fds, bpid, blockId);
    assertNotNull(memBlock);
    assertEquals(genStamp, memBlock.getGenerationStamp());
  }

  private void verifyStorageType(long blockId, boolean expectTransient) {
    final ReplicaInfo memBlock;
    memBlock = FsDatasetTestUtil.fetchReplicaInfo(fds, bpid, blockId);
    assertNotNull(memBlock);
    assertThat(memBlock.getVolume().isTransientStorage(), is(expectTransient));
  }

  private static class TestFsVolumeSpi implements FsVolumeSpi {
    @Override
    public String[] getBlockPoolList() {
      return new String[0];
    }

    @Override
    public FsVolumeReference obtainReference() throws ClosedChannelException {
      return null;
    }

    @Override
    public long getAvailable() throws IOException {
      return 0;
    }

    public File getFinalizedDir(String bpid) throws IOException {
      return new File("/base/current/" + bpid + "/finalized");
    }

    @Override
    public StorageType getStorageType() {
      return StorageType.DEFAULT;
    }

    @Override
    public String getStorageID() {
      return "";
    }

    @Override
    public boolean isTransientStorage() {
      return false;
    }

    @Override
    public boolean isRAMStorage() {
      return false;
    }

    @Override
    public void reserveSpaceForReplica(long bytesToReserve) {
    }

    @Override
    public void releaseReservedSpace(long bytesToRelease) {
    }

    @Override
    public void releaseLockedMemory(long bytesToRelease) {
    }

    @Override
    public BlockIterator newBlockIterator(String bpid, String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BlockIterator loadBlockIterator(String bpid, String name)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public FsDatasetSpi getDataset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public StorageLocation getStorageLocation() {
      return null;
    }

    @Override
    public URI getBaseURI() {
      return (new File("/base")).toURI();
    }

    @Override
    public DF getUsageStats(Configuration conf) {
      return null;
    }

    @Override
    public byte[] loadLastPartialChunkChecksum(File blockFile, File metaFile)
        throws IOException {
      return null;
    }

    @Override
    public void compileReport(String bpid,
        Collection<ScanInfo> report, ReportCompiler reportCompiler)
        throws InterruptedException, IOException {
    }

    @Override
    public FileIoProvider getFileIoProvider() {
      return null;
    }

    @Override
    public DataNodeVolumeMetrics getMetrics() {
      return null;
    }

    @Override
    public VolumeCheckResult check(VolumeCheckContext context)
        throws Exception {
      return VolumeCheckResult.HEALTHY;
    }
  }

  private final static TestFsVolumeSpi TEST_VOLUME = new TestFsVolumeSpi();

  private final static String BPID_1 = "BP-783049782-127.0.0.1-1370971773491";

  private final static String BPID_2 = "BP-367845636-127.0.0.1-5895645674231";

  void testScanInfoObject(long blockId, File baseDir, String blockFile,
                          String metaFile)
      throws Exception {
    FsVolumeSpi.ScanInfo scanInfo =
        new FsVolumeSpi.ScanInfo(blockId, baseDir, blockFile, metaFile,
            TEST_VOLUME);
    assertEquals(blockId, scanInfo.getBlockId());
    if (blockFile != null) {
      assertEquals(new File(baseDir, blockFile).getAbsolutePath(),
          scanInfo.getBlockFile().getAbsolutePath());
    } else {
      assertNull(scanInfo.getBlockFile());
    }
    if (metaFile != null) {
      assertEquals(new File(baseDir, metaFile).getAbsolutePath(),
          scanInfo.getMetaFile().getAbsolutePath());
    } else {
      assertNull(scanInfo.getMetaFile());
    }
    assertEquals(TEST_VOLUME, scanInfo.getVolume());
  }

  void testScanInfoObject(long blockId) throws Exception {
    FsVolumeSpi.ScanInfo scanInfo =
        new FsVolumeSpi.ScanInfo(blockId, null, null, null, null);
    assertEquals(blockId, scanInfo.getBlockId());
    assertNull(scanInfo.getBlockFile());
    assertNull(scanInfo.getMetaFile());
  }

  @Test(timeout = 120000)
  public void TestScanInfo() throws Exception {
    testScanInfoObject(123,
        new File(TEST_VOLUME.getFinalizedDir(BPID_1).getAbsolutePath()),
            "blk_123", "blk_123__1001.meta");
    testScanInfoObject(464,
        new File(TEST_VOLUME.getFinalizedDir(BPID_1).getAbsolutePath()),
            "blk_123", null);
    testScanInfoObject(523,
        new File(TEST_VOLUME.getFinalizedDir(BPID_1).getAbsolutePath()),
            null, "blk_123__1009.meta");
    testScanInfoObject(789, null, null, null);
    testScanInfoObject(456);
    testScanInfoObject(123,
        new File(TEST_VOLUME.getFinalizedDir(BPID_2).getAbsolutePath()),
            "blk_567", "blk_567__1004.meta");
  }

  /**
   * Test the behavior of exception handling during directory scan operation.
   * Directory scanner shouldn't abort the scan on every directory just because
   * one had an error.
   */
  @Test(timeout = 60000)
  public void testExceptionHandlingWhileDirectoryScan() throws Exception {
    Configuration conf = getConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      bpid = cluster.getNamesystem().getBlockPoolId();
      fds = DataNodeTestUtils.getFSDataset(cluster.getDataNodes().get(0));
      client = cluster.getFileSystem().getClient();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY, 1);

      // Add files with 2 blocks
      createFile(GenericTestUtils.getMethodName(), BLOCK_LENGTH * 2, false);

      // Inject error on #getFinalizedDir() so that ReportCompiler#call() will
      // hit exception while preparing the block info report list.
      List<FsVolumeSpi> volumes = new ArrayList<>();
      Iterator<FsVolumeSpi> iterator = fds.getFsVolumeReferences().iterator();
      while (iterator.hasNext()) {
        FsVolumeImpl volume = (FsVolumeImpl) iterator.next();
        FsVolumeImpl spy = Mockito.spy(volume);
        Mockito.doThrow(new IOException("Error while getFinalizedDir"))
            .when(spy).getFinalizedDir(volume.getBlockPoolList()[0]);
        volumes.add(spy);
      }
      FsVolumeReferences volReferences = new FsVolumeReferences(volumes);
      FsDatasetSpi<? extends FsVolumeSpi> spyFds = Mockito.spy(fds);
      Mockito.doReturn(volReferences).when(spyFds).getFsVolumeReferences();

      scanner = new DirectoryScanner(spyFds, conf);
      scanner.setRetainDiffs(true);
      scanner.reconcile();
    } finally {
      if (scanner != null) {
        scanner.shutdown();
        scanner = null;
      }
      cluster.shutdown();
    }
  }

  @Test
  public void testDirectoryScannerInFederatedCluster() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration(getConfiguration());
    // Create Federated cluster with two nameservices and one DN
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHAFederatedTopology(2))
        .numDataNodes(1).build()) {
      cluster.waitActive();
      cluster.transitionToActive(1);
      cluster.transitionToActive(3);
      fds = DataNodeTestUtils.getFSDataset(cluster.getDataNodes().get(0));
      // Create one block in first nameservice
      FileSystem fs = cluster.getFileSystem(1);
      int bp1Files = 1;
      writeFile(fs, bp1Files);
      // Create two blocks in second nameservice
      FileSystem fs2 = cluster.getFileSystem(3);
      int bp2Files = 2;
      writeFile(fs2, bp2Files);
      // Call the Directory scanner
      scanner = new DirectoryScanner(fds, conf);
      scanner.setRetainDiffs(true);
      scanner.reconcile();
      // Check blocks in corresponding BP

      GenericTestUtils.waitFor(() -> {
        try {
          bpid = cluster.getNamesystem(1).getBlockPoolId();
          verifyStats(bp1Files, 0, 0, 0, 0, 0, 0);
          bpid = cluster.getNamesystem(3).getBlockPoolId();
          verifyStats(bp2Files, 0, 0, 0, 0, 0, 0);
        } catch (AssertionError ex) {
          return false;
        }

        return true;
      }, 50, 2000);
    } finally {
      if (scanner != null) {
        scanner.shutdown();
        scanner = null;
      }
    }
  }

  private static final String SEP = System.getProperty("file.separator");

  /**
   * Test parsing LocalReplica. We should be able to find the replica's path
   * even if the replica's dir doesn't match the idToBlockDir.
   */
  @Test(timeout = 3000)
  public void testLocalReplicaParsing() {
    String baseDir = GenericTestUtils.getRandomizedTempPath();
    long blkId = getRandomBlockId();
    File blockDir = DatanodeUtil.idToBlockDir(new File(baseDir), blkId);
    String subdir1 = new File(blockDir.getParent()).getName();

    // test parsing dir without ./subdir/subdir
    LocalReplica.ReplicaDirInfo info =
        LocalReplica.parseBaseDir(new File(baseDir), blkId);
    assertEquals(baseDir, info.baseDirPath);
    assertEquals(false, info.hasSubidrs);

    // test when path doesn't match the idToBLockDir.
    String pathWithOneSubdir = baseDir + SEP + subdir1;
    info = LocalReplica.parseBaseDir(new File(pathWithOneSubdir), blkId);
    assertEquals(pathWithOneSubdir, info.baseDirPath);
    assertEquals(false, info.hasSubidrs);

    // test when path doesn't match the idToBlockDir.
    String badPath = baseDir + SEP + subdir1 + SEP + "subdir-not-exist";
    info = LocalReplica.parseBaseDir(new File(badPath), blkId);
    assertEquals(badPath, info.baseDirPath);
    assertEquals(false, info.hasSubidrs);

    // test when path matches the idToBlockDir.
    info = LocalReplica.parseBaseDir(blockDir, blkId);
    assertEquals(baseDir, info.baseDirPath);
    assertEquals(true, info.hasSubidrs);
  }

  /**
   * Test whether can LocalReplica.updateWithReplica() correct the wrongly
   * recorded replica location.
   */
  @Test(timeout = 3000)
  public void testLocalReplicaUpdateWithReplica() throws Exception {
    String baseDir = GenericTestUtils.getRandomizedTempPath();
    long blkId = getRandomBlockId();
    File blockDir = DatanodeUtil.idToBlockDir(new File(baseDir), blkId);
    String subdir2 = blockDir.getName();
    String subdir1 = new File(blockDir.getParent()).getName();
    String diskSub = subdir2.equals("subdir0") ? "subdir1" : "subdir0";

    // the block file on disk
    File diskBlockDir = new File(baseDir + SEP + subdir1 + SEP + diskSub);
    File realBlkFile = new File(diskBlockDir, BLOCK_FILE_PREFIX + blkId);
    // the block file in mem
    File memBlockDir = blockDir;
    LocalReplica localReplica = (LocalReplica) new ReplicaBuilder(
        HdfsServerConstants.ReplicaState.FINALIZED)
        .setDirectoryToUse(memBlockDir).setBlockId(blkId).build();

    // DirectoryScanner find the inconsistent file and try to make it right
    StorageLocation sl = StorageLocation.parse(realBlkFile.toString());
    localReplica.updateWithReplica(sl);
    assertEquals(realBlkFile, localReplica.getBlockFile());
  }

  @Test(timeout = 60000)
  public void testLastDirScannerFinishTimeIsUpdated() throws Exception {
    Configuration conf = getConfiguration();
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 3L);
    cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      bpid = cluster.getNamesystem().getBlockPoolId();
      final DataNode dn = cluster.getDataNodes().get(0);
      fds = DataNodeTestUtils.getFSDataset(cluster.getDataNodes().get(0));
      long lastDirScannerFinishTime = fds.getLastDirScannerFinishTime();
      dn.getDirectoryScanner().run();
      assertNotEquals(lastDirScannerFinishTime, fds.getLastDirScannerFinishTime());
    } finally {
      cluster.shutdown();
    }
  }

  public long getRandomBlockId() {
    return Math.abs(new Random().nextLong());
  }

  private void writeFile(FileSystem fs, int numFiles) throws IOException {
    final String fileName = "/" + GenericTestUtils.getMethodName();
    for (int i = 0; i < numFiles; i++) {
      final Path filePath = new Path(fileName + i);
      DFSTestUtil.createFile(fs, filePath, 1, (short) 1, 0);
    }
  }

  @Test(timeout = 30000)
  public void testNullStorage() throws Exception {
    DataNodeFaultInjector oldInjector = DataNodeFaultInjector.get();

    Configuration conf = getConfiguration();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      bpid = cluster.getNamesystem().getBlockPoolId();
      fds = DataNodeTestUtils.getFSDataset(cluster.getDataNodes().get(0));
      client = cluster.getFileSystem().getClient();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY, 1);
      createFile(GenericTestUtils.getMethodName(), BLOCK_LENGTH, false);
      // Make sure checkAndUpdate will run
      truncateBlockFile();

      // Mock a volume corruption after DirectoryScanner.scan() but before checkAndUpdate()
      FsVolumeImpl volumeToRemove = fds.getVolumeList().get(0);
      DataNodeFaultInjector injector = new DataNodeFaultInjector() {
        @Override
        public void waitUntilStorageRemoved() {
          Set<FsVolumeSpi> volumesToRemove = new HashSet<>();
          volumesToRemove.add(volumeToRemove);
          cluster.getDataNodes().get(0).handleVolumeFailures(volumesToRemove);
        }
      };
      DataNodeFaultInjector.set(injector);

      GenericTestUtils.LogCapturer logCapturer =
          GenericTestUtils.LogCapturer.captureLogs(DataNode.LOG);
      scanner = new DirectoryScanner(fds, conf);
      scanner.setRetainDiffs(true);
      scanner.reconcile();
      assertFalse(logCapturer.getOutput()
          .contains("Trying to add RDBI for null storage UUID " + volumeToRemove.getStorageID()));
    } finally {
      if (scanner != null) {
        scanner.shutdown();
        scanner = null;
      }
      cluster.shutdown();
      DataNodeFaultInjector.set(oldInjector);
    }
  }
}
