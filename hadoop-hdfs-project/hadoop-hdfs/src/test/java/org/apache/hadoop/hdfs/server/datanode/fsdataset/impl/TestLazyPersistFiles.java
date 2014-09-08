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
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.StorageType.DEFAULT;
import static org.apache.hadoop.hdfs.StorageType.RAM_DISK;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TestLazyPersistFiles {
  public static final Log LOG = LogFactory.getLog(TestLazyPersistFiles.class);

  static {
    ((Log4JLogger) NameNode.blockStateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) FsDatasetImpl.LOG).getLogger().setLevel(Level.ALL);
  }

  private static final int THREADPOOL_SIZE = 10;

  private static final short REPL_FACTOR = 1;
  private static final int BLOCK_SIZE = 10485760;   // 10 MB
  private static final int LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC = 3;
  private static final long HEARTBEAT_INTERVAL_SEC = 1;
  private static final int HEARTBEAT_RECHECK_INTERVAL_MSEC = 500;
  private static final int LAZY_WRITER_INTERVAL_SEC = 1;
  private static final int BUFFER_LENGTH = 4096;
  private static final int EVICTION_LOW_WATERMARK = 1;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private DFSClient client;
  private Configuration conf;

  @After
  public void shutDownCluster() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
      client = null;
    }

    if (cluster != null) {
      cluster.shutdownDataNodes();
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test (timeout=300000)
  public void testFlagNotSetByDefault() throws IOException {
    startUpCluster(false, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, false);
    // Stat the file and check that the lazyPersist flag is returned back.
    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.isLazyPersist(), is(false));
  }

  @Test (timeout=300000)
  public void testFlagPropagation() throws IOException {
    startUpCluster(false, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, true);
    // Stat the file and check that the lazyPersist flag is returned back.
    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.isLazyPersist(), is(true));
  }

  @Test (timeout=300000)
  public void testFlagPersistenceInEditLog() throws IOException {
    startUpCluster(false, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, true);
    cluster.restartNameNode(true);

    // Stat the file and check that the lazyPersist flag is returned back.
    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.isLazyPersist(), is(true));
  }

  @Test (timeout=300000)
  public void testFlagPersistenceInFsImage() throws IOException {
    startUpCluster(false, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, true);
    // checkpoint
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    fs.saveNamespace();
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    cluster.restartNameNode(true);

    // Stat the file and check that the lazyPersist flag is returned back.
    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.isLazyPersist(), is(true));
  }

  @Test (timeout=300000)
  public void testPlacementOnRamDisk() throws IOException {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, RAM_DISK);
  }

  @Test (timeout=300000)
  public void testPlacementOnSizeLimitedRamDisk() throws IOException {
    startUpCluster(true, 3);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");

    makeTestFile(path1, BLOCK_SIZE, true);
    makeTestFile(path2, BLOCK_SIZE, true);

    ensureFileReplicasOnStorageType(path1, RAM_DISK);
    ensureFileReplicasOnStorageType(path2, RAM_DISK);
  }

  /**
   * Client tries to write LAZY_PERSIST to same DN with no RamDisk configured
   * Write should default to disk. No error.
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testFallbackToDisk() throws IOException {
    startUpCluster(false, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, DEFAULT);
  }

  /**
   * File can not fit in RamDisk even with eviction
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testFallbackToDiskFull() throws IOException {
    startUpCluster(false, 0);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, DEFAULT);
  }

  /**
   * File partially fit in RamDisk after eviction.
   * RamDisk can fit 2 blocks. Write a file with 5 blocks.
   * Expect 2 or less blocks are on RamDisk and 3 or more on disk.
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testFallbackToDiskPartial()
    throws IOException, InterruptedException {
    startUpCluster(true, 2);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE * 5, true);

    // Sleep for a short time to allow the lazy writer thread to do its job
    Thread.sleep(6 * LAZY_WRITER_INTERVAL_SEC * 1000);

    triggerBlockReport();

    int numBlocksOnRamDisk = 0;
    int numBlocksOnDisk = 0;

    long fileLength = client.getFileInfo(path.toString()).getLen();
    LocatedBlocks locatedBlocks =
      client.getLocatedBlocks(path.toString(), 0, fileLength);
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      if (locatedBlock.getStorageTypes()[0] == RAM_DISK) {
        numBlocksOnRamDisk++;
      } else if (locatedBlock.getStorageTypes()[0] == DEFAULT) {
        numBlocksOnDisk++;
      }
    }

    // Since eviction is asynchronous, depending on the timing of eviction
    // wrt writes, we may get 2 or less blocks on RAM disk.
    assert(numBlocksOnRamDisk <= 2);
    assert(numBlocksOnDisk >= 3);
  }

  /**
   * If the only available storage is RAM_DISK and the LAZY_PERSIST flag is not
   * specified, then block placement should fail.
   *
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testRamDiskNotChosenByDefault() throws IOException {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    try {
      makeTestFile(path, BLOCK_SIZE, false);
      fail("Block placement to RAM_DISK should have failed without lazyPersist flag");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }
  }

  /**
   * Append to lazy persist file is denied.
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testAppendIsDenied() throws IOException {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);

    try {
      client.append(path.toString(), BUFFER_LENGTH, null, null).close();
      fail("Append to LazyPersist file did not fail as expected");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }
  }

  /**
   * If one or more replicas of a lazyPersist file are lost, then the file
   * must be discarded by the NN, instead of being kept around as a
   * 'corrupt' file.
   */
  @Test (timeout=300000)
  public void testLazyPersistFilesAreDiscarded()
      throws IOException, InterruptedException {
    startUpCluster(true, 2);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    // Stop the DataNode and sleep for the time it takes the NN to
    // detect the DN as being dead.
    cluster.shutdownDataNodes();
    Thread.sleep(30000L);
    assertThat(cluster.getNamesystem().getNumDeadDataNodes(), is(1));

    // Next, wait for the replication monitor to mark the file as corrupt
    Thread.sleep(2 * DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT * 1000);

    // Wait for the LazyPersistFileScrubber to run
    Thread.sleep(2 * LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC * 1000);

    // Ensure that path1 does not exist anymore, whereas path2 does.
    assert(!fs.exists(path1));

    // We should have zero blocks that needs replication i.e. the one
    // belonging to path2.
    assertThat(cluster.getNameNode()
                      .getNamesystem()
                      .getBlockManager()
                      .getUnderReplicatedBlocksCount(),
               is(0L));
  }

  @Test (timeout=300000)
  public void testLazyPersistBlocksAreSaved()
      throws IOException, InterruptedException {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    // Create a test file
    makeTestFile(path, BLOCK_SIZE * 10, true);
    LocatedBlocks locatedBlocks = ensureFileReplicasOnStorageType(path, RAM_DISK);

    // Sleep for a short time to allow the lazy writer thread to do its job
    Thread.sleep(6 * LAZY_WRITER_INTERVAL_SEC * 1000);
    
    LOG.info("Verifying copy was saved to lazyPersist/");

    // Make sure that there is a saved copy of the replica on persistent
    // storage.
    final String bpid = cluster.getNamesystem().getBlockPoolId();
    List<? extends FsVolumeSpi> volumes =
        cluster.getDataNodes().get(0).getFSDataset().getVolumes();

    final Set<Long> persistedBlockIds = new HashSet<Long>();

    // Make sure at least one non-transient volume has a saved copy of
    // the replica.
    for (FsVolumeSpi v : volumes) {
      if (v.isTransientStorage()) {
        continue;
      }

      FsVolumeImpl volume = (FsVolumeImpl) v;
      File lazyPersistDir = volume.getBlockPoolSlice(bpid).getLazypersistDir();

      for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
        File targetDir = DatanodeUtil.idToBlockDir(lazyPersistDir, lb.getBlock().getBlockId());
        File blockFile = new File(targetDir, lb.getBlock().getBlockName());
        if (blockFile.exists()) {
          // Found a persisted copy for this block!
          boolean added = persistedBlockIds.add(lb.getBlock().getBlockId());
          assertThat(added, is(true));
        }
      }
    }

    // We should have found a persisted copy for each located block.
    assertThat(persistedBlockIds.size(), is(locatedBlocks.getLocatedBlocks().size()));
  }

  /**
   * RamDisk eviction after lazy persist to disk.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000)
  public void testRamDiskEviction() throws IOException, InterruptedException {
    startUpCluster(true, 1 + EVICTION_LOW_WATERMARK);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");

    final int SEED = 0xFADED;
    makeRandomTestFile(path1, BLOCK_SIZE, true, SEED);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    // Sleep for a short time to allow the lazy writer thread to do its job.
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    // Create another file with a replica on RAM_DISK.
    makeTestFile(path2, BLOCK_SIZE, true);
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);
    triggerBlockReport();

    // Ensure the first file was evicted to disk, the second is still on
    // RAM_DISK.
    ensureFileReplicasOnStorageType(path2, RAM_DISK);
    ensureFileReplicasOnStorageType(path1, DEFAULT);
  }

  /**
   * RamDisk eviction should not happen on blocks that are not yet
   * persisted on disk.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000)
  public void testRamDiskEvictionBeforePersist()
    throws IOException, InterruptedException {
    startUpCluster(true, 1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");
    final int SEED = 0XFADED;

    // Stop lazy writer to ensure block for path1 is not persisted to disk.
    stopLazyWriter(cluster.getDataNodes().get(0));

    makeRandomTestFile(path1, BLOCK_SIZE, true, SEED);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    // Create second file with a replica on RAM_DISK.
    makeTestFile(path2, BLOCK_SIZE, true);

    // Eviction should not happen for block of the first file that is not
    // persisted yet.
    ensureFileReplicasOnStorageType(path1, RAM_DISK);
    ensureFileReplicasOnStorageType(path2, DEFAULT);

    assert(fs.exists(path1));
    assert(fs.exists(path2));
    verifyReadRandomFile(path1, BLOCK_SIZE, SEED);
  }

  /**
   * Validates lazy persisted blocks are evicted from RAM_DISK based on LRU.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000)
  public void testRamDiskEvictionLRU()
    throws IOException, InterruptedException {
    startUpCluster(true, 3);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final int NUM_PATHS = 6;
    Path paths[] = new Path[NUM_PATHS];

    for (int i = 0; i < NUM_PATHS; i++) {
      paths[i] = new Path("/" + METHOD_NAME + "." + i +".dat");
    }

    // No eviction for the first half of files
    for (int i = 0; i < NUM_PATHS/2; i++) {
      makeTestFile(paths[i], BLOCK_SIZE, true);
      ensureFileReplicasOnStorageType(paths[i], RAM_DISK);
    }

    // Lazy persist writer persists the first half of files
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);

    // Create the second half of files with eviction upon each create.
    for (int i = NUM_PATHS/2; i < NUM_PATHS; i++) {
      makeTestFile(paths[i], BLOCK_SIZE, true);
      ensureFileReplicasOnStorageType(paths[i], RAM_DISK);

      // path[i-NUM_PATHS/2] is expected to be evicted by LRU
      triggerBlockReport();
      ensureFileReplicasOnStorageType(paths[i - NUM_PATHS / 2], DEFAULT);
    }
  }

  /**
   * Delete lazy-persist file that has not been persisted to disk.
   * Memory is freed up and file is gone.
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testDeleteBeforePersist()
    throws IOException, InterruptedException {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    stopLazyWriter(cluster.getDataNodes().get(0));

    Path path = new Path("/" + METHOD_NAME + ".dat");
    makeTestFile(path, BLOCK_SIZE, true);
    LocatedBlocks locatedBlocks =
      ensureFileReplicasOnStorageType(path, RAM_DISK);

    // Delete before persist
    client.delete(path.toString(), false);
    Assert.assertFalse(fs.exists(path));

    assertThat(verifyDeletedBlocks(locatedBlocks), is(true));
  }

  /**
   * Delete lazy-persist file that has been persisted to disk
   * Both memory blocks and disk blocks are deleted.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000)
  public void testDeleteAfterPersist()
    throws IOException, InterruptedException {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    LocatedBlocks locatedBlocks = ensureFileReplicasOnStorageType(path, RAM_DISK);

    // Sleep for a short time to allow the lazy writer thread to do its job
    Thread.sleep(6 * LAZY_WRITER_INTERVAL_SEC * 1000);

    // Delete after persist
    client.delete(path.toString(), false);
    Assert.assertFalse(fs.exists(path));

    triggerBlockReport();

    assertThat(verifyDeletedBlocks(locatedBlocks), is(true));
  }

  /**
   * RAM_DISK used/free space
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000)
  public void testDfsUsageCreateDelete()
    throws IOException, InterruptedException {
    startUpCluster(true, 4);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    // Get the usage before write BLOCK_SIZE
    long usedBeforeCreate = fs.getUsed();

    makeTestFile(path, BLOCK_SIZE, true);
    long usedAfterCreate = fs.getUsed();

    assertThat(usedAfterCreate, is((long) BLOCK_SIZE));

    // Sleep for a short time to allow the lazy writer thread to do its job
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);

    long usedAfterPersist = fs.getUsed();
    assertThat(usedAfterPersist, is((long) BLOCK_SIZE));

    // Delete after persist
    client.delete(path.toString(), false);
    long usedAfterDelete = fs.getUsed();

    assertThat(usedBeforeCreate, is(usedAfterDelete));
  }

  /**
   * Concurrent read from the same node and verify the contents.
   */
  @Test (timeout=300000)
  public void testConcurrentRead()
    throws Exception {
    startUpCluster(true, 2);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final Path path1 = new Path("/" + METHOD_NAME + ".dat");

    final int SEED = 0xFADED;
    final int NUM_TASKS = 5;
    makeRandomTestFile(path1, BLOCK_SIZE, true, SEED);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    //Read from multiple clients
    final CountDownLatch latch = new CountDownLatch(NUM_TASKS);
    final AtomicBoolean testFailed = new AtomicBoolean(false);

    Runnable readerRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          Assert.assertTrue(verifyReadRandomFile(path1, BLOCK_SIZE, SEED));
        } catch (Throwable e) {
          LOG.error("readerRunnable error", e);
          testFailed.set(true);
        } finally {
          latch.countDown();
        }
      }
    };

    Thread threads[] = new Thread[NUM_TASKS];
    for (int i = 0; i < NUM_TASKS; i++) {
      threads[i] = new Thread(readerRunnable);
      threads[i].start();
    }

    Thread.sleep(500);

    for (int i = 0; i < NUM_TASKS; i++) {
      Uninterruptibles.joinUninterruptibly(threads[i]);
    }
    Assert.assertFalse(testFailed.get());
  }

  /**
   * Concurrent write with eviction
   * RAM_DISK can hold 9 replicas
   * 4 threads each write 5 replicas
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000)
  public void testConcurrentWrites()
    throws IOException, InterruptedException {
    startUpCluster(true, 9);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final int SEED = 0xFADED;
    final int NUM_WRITERS = 4;
    final int NUM_WRITER_PATHS = 5;

    Path paths[][] = new Path[NUM_WRITERS][NUM_WRITER_PATHS];
    for (int i = 0; i < NUM_WRITERS; i++) {
      paths[i] = new Path[NUM_WRITER_PATHS];
      for (int j = 0; j < NUM_WRITER_PATHS; j++) {
        paths[i][j] =
          new Path("/" + METHOD_NAME + ".Writer" + i + ".File." + j + ".dat");
      }
    }

    final CountDownLatch latch = new CountDownLatch(NUM_WRITERS);
    final AtomicBoolean testFailed = new AtomicBoolean(false);

    ExecutorService executor = Executors.newFixedThreadPool(THREADPOOL_SIZE);
    for (int i = 0; i < NUM_WRITERS; i++) {
      Runnable writer = new WriterRunnable(i, paths[i], SEED, latch, testFailed);
      executor.execute(writer);
    }

    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);
    triggerBlockReport();

    // Stop executor from adding new tasks to finish existing threads in queue
    latch.await();

    assertThat(testFailed.get(), is(false));
  }

  @Test (timeout=300000)
  public void testDnRestartWithSavedReplicas()
      throws IOException, InterruptedException {

    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    // Sleep for a short time to allow the lazy writer thread to do its job.
    // However the block replica should not be evicted from RAM_DISK yet.
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    LOG.info("Restarting the DataNode");
    cluster.restartDataNode(0, true);
    cluster.waitActive();

    // Ensure that the replica is now on persistent storage.
    ensureFileReplicasOnStorageType(path1, DEFAULT);
  }

  @Test (timeout=300000)
  public void testDnRestartWithUnsavedReplicas()
      throws IOException, InterruptedException {

    startUpCluster(true, 1);
    stopLazyWriter(cluster.getDataNodes().get(0));

    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    LOG.info("Restarting the DataNode");
    cluster.restartDataNode(0, true);
    cluster.waitActive();

    // Ensure that the replica is still on transient storage.
    ensureFileReplicasOnStorageType(path1, RAM_DISK);
  }

  // ---- Utility functions for all test cases -------------------------------

  /**
   * If ramDiskStorageLimit is >=0, then RAM_DISK capacity is artificially
   * capped. If ramDiskStorageLimit < 0 then it is ignored.
   */
  private void startUpCluster(boolean hasTransientStorage,
                              final int ramDiskReplicaCapacity,
                              final boolean useSCR)
      throws IOException {

    conf = new Configuration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC,
                LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC);
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL_SEC);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                HEARTBEAT_RECHECK_INTERVAL_MSEC);
    conf.setInt(DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC,
                LAZY_WRITER_INTERVAL_SEC);
    conf.setInt(DFS_DATANODE_RAM_DISK_LOW_WATERMARK_REPLICAS,
                EVICTION_LOW_WATERMARK);

    conf.setBoolean(DFS_CLIENT_READ_SHORTCIRCUIT_KEY, useSCR);

    long[] capacities = null;
    if (hasTransientStorage && ramDiskReplicaCapacity >= 0) {
      // Convert replica count to byte count, add some delta for .meta and VERSION files.
      long ramDiskStorageLimit = ((long) ramDiskReplicaCapacity * BLOCK_SIZE) + (BLOCK_SIZE - 1);
      capacities = new long[] { ramDiskStorageLimit, -1 };
    }

    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(REPL_FACTOR)
        .storageCapacities(capacities)
        .storageTypes(hasTransientStorage ? new StorageType[]{ RAM_DISK, DEFAULT } : null)
        .build();
    fs = cluster.getFileSystem();
    client = fs.getClient();
    LOG.info("Cluster startup complete");
  }

  private void startUpCluster(boolean hasTransientStorage,
                              final int ramDiskReplicaCapacity)
    throws IOException {
    startUpCluster(hasTransientStorage, ramDiskReplicaCapacity, false);
  }

  private void makeTestFile(Path path, long length, final boolean isLazyPersist)
      throws IOException {

    EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE);

    if (isLazyPersist) {
      createFlags.add(LAZY_PERSIST);
    }

    FSDataOutputStream fos = null;
    try {
      fos =
          fs.create(path,
              FsPermission.getFileDefault(),
              createFlags,
              BUFFER_LENGTH,
              REPL_FACTOR,
              BLOCK_SIZE,
              null);

      // Allocate a block.
      byte[] buffer = new byte[BUFFER_LENGTH];
      for (int bytesWritten = 0; bytesWritten < length; ) {
        fos.write(buffer, 0, buffer.length);
        bytesWritten += buffer.length;
      }
      if (length > 0) {
        fos.hsync();
      }
    } finally {
      IOUtils.closeQuietly(fos);
    }
  }

  private LocatedBlocks ensureFileReplicasOnStorageType(
      Path path, StorageType storageType) throws IOException {
    // Ensure that returned block locations returned are correct!
    LOG.info("Ensure path: " + path + " is on StorageType: " + storageType);
    assertThat(fs.exists(path), is(true));
    long fileLength = client.getFileInfo(path.toString()).getLen();
    LocatedBlocks locatedBlocks =
        client.getLocatedBlocks(path.toString(), 0, fileLength);
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      assertThat(locatedBlock.getStorageTypes()[0], is(storageType));
    }
    return locatedBlocks;
  }

  private void stopLazyWriter(DataNode dn) {
    // Stop the lazyWriter daemon.
    FsDatasetImpl fsDataset = ((FsDatasetImpl) dn.getFSDataset());
    ((FsDatasetImpl.LazyWriter) fsDataset.lazyWriter.getRunnable()).stop();
  }

  private void makeRandomTestFile(Path path, long length, final boolean isLazyPersist,
                                  long seed) throws IOException {
    DFSTestUtil.createFile(fs, path, isLazyPersist, BUFFER_LENGTH, length,
      BLOCK_SIZE, REPL_FACTOR, seed, true);
  }

  private boolean verifyReadRandomFile(
    Path path, int fileLength, int seed) throws IOException {
    byte contents[] = DFSTestUtil.readFileBuffer(fs, path);
    byte expected[] = DFSTestUtil.
      calculateFileContentsFromSeed(seed, fileLength);
    return Arrays.equals(contents, expected);
  }

  private boolean verifyDeletedBlocks(LocatedBlocks locatedBlocks)
    throws IOException, InterruptedException {

    LOG.info("Verifying replica has no saved copy after deletion.");
    triggerBlockReport();

    while(
      DataNodeTestUtils.getPendingAsyncDeletions(cluster.getDataNodes().get(0))
        > 0L){
      Thread.sleep(1000);
    }

    final String bpid = cluster.getNamesystem().getBlockPoolId();
    List<? extends FsVolumeSpi> volumes =
      cluster.getDataNodes().get(0).getFSDataset().getVolumes();

    // Make sure deleted replica does not have a copy on either finalized dir of
    // transient volume or finalized dir of non-transient volume
    for (FsVolumeSpi v : volumes) {
      FsVolumeImpl volume = (FsVolumeImpl) v;
      File targetDir = (v.isTransientStorage()) ?
          volume.getBlockPoolSlice(bpid).getFinalizedDir() :
          volume.getBlockPoolSlice(bpid).getLazypersistDir();
      if (verifyBlockDeletedFromDir(targetDir, locatedBlocks) == false) {
        return false;
      }
    }
    return true;
  }

  private boolean verifyBlockDeletedFromDir(File dir, LocatedBlocks locatedBlocks) {

    for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
      File targetDir =
        DatanodeUtil.idToBlockDir(dir, lb.getBlock().getBlockId());

      File blockFile = new File(targetDir, lb.getBlock().getBlockName());
      if (blockFile.exists()) {
        LOG.warn("blockFile: " + blockFile.getAbsolutePath() +
          " exists after deletion.");
        return false;
      }
      File metaFile = new File(targetDir,
        DatanodeUtil.getMetaName(lb.getBlock().getBlockName(),
          lb.getBlock().getGenerationStamp()));
      if (metaFile.exists()) {
        LOG.warn("metaFile: " + metaFile.getAbsolutePath() +
          " exists after deletion.");
        return false;
      }
    }
    return true;
  }

  private void triggerBlockReport()
    throws IOException, InterruptedException {
    // Trigger block report to NN
    DataNodeTestUtils.triggerBlockReport(cluster.getDataNodes().get(0));
    Thread.sleep(10 * 1000);
  }

  class WriterRunnable implements Runnable {
    private final int id;
    private final Path paths[];
    private final int seed;
    private CountDownLatch latch;
    private AtomicBoolean bFail;

    public WriterRunnable(int threadIndex, Path[] paths,
                          int seed, CountDownLatch latch,
                          AtomicBoolean bFail) {
      id = threadIndex;
      this.paths = paths;
      this.seed = seed;
      this.latch = latch;
      this.bFail = bFail;
      System.out.println("Creating Writer: " + id);
    }

    public void run() {
      System.out.println("Writer " + id + " starting... ");
      int i = 0;
      try {
        for (i = 0; i < paths.length; i++) {
          makeRandomTestFile(paths[i], BLOCK_SIZE, true, seed);
          // eviction may faiL when all blocks are not persisted yet.
          // ensureFileReplicasOnStorageType(paths[i], RAM_DISK);
        }
      } catch (IOException e) {
        bFail.set(true);
        LOG.error("Writer exception: writer id:" + id +
          " testfile: " + paths[i].toString() +
          " " + e);
      } finally {
        latch.countDown();
      }
    }
  }
}
