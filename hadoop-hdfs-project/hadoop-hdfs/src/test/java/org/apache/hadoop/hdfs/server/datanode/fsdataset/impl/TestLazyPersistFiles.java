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
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.ArrayList;
import java.util.Collections;

import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestLazyPersistFiles extends LazyPersistTestCase {
  private static final byte LAZY_PERSIST_POLICY_ID = (byte) 15;

  private static final int THREADPOOL_SIZE = 10;

  @Test
  public void testPolicyNotSetByDefault() throws IOException {
    startUpCluster(false, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, false);
    // Stat the file and check that the LAZY_PERSIST policy is not
    // returned back.
    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.getStoragePolicy(), not(LAZY_PERSIST_POLICY_ID));
  }

  @Test
  public void testPolicyPropagation() throws IOException {
    startUpCluster(false, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, true);
    // Stat the file and check that the lazyPersist flag is returned back.
    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.getStoragePolicy(), is(LAZY_PERSIST_POLICY_ID));
  }

  @Test
  public void testPolicyPersistenceInEditLog() throws IOException {
    startUpCluster(false, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, true);
    cluster.restartNameNode(true);

    // Stat the file and check that the lazyPersist flag is returned back.
    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.getStoragePolicy(), is(LAZY_PERSIST_POLICY_ID));
  }

  @Test
  public void testPolicyPersistenceInFsImage() throws IOException {
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
    assertThat(status.getStoragePolicy(), is(LAZY_PERSIST_POLICY_ID));
  }

  @Test
  public void testPlacementOnRamDisk() throws IOException {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, RAM_DISK);
  }

  @Test
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
  @Test
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
  @Test
  public void testFallbackToDiskFull() throws Exception {
    startUpCluster(false, 0);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, DEFAULT);

    verifyRamDiskJMXMetric("RamDiskBlocksWriteFallback", 1);
  }

  /**
   * File partially fit in RamDisk after eviction.
   * RamDisk can fit 2 blocks. Write a file with 5 blocks.
   * Expect 2 or less blocks are on RamDisk and 3 or more on disk.
   * @throws IOException
   */
  @Test
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
  @Test
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
  @Test
  public void testAppendIsDenied() throws IOException {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);

    try {
      client.append(path.toString(), BUFFER_LENGTH,
          EnumSet.of(CreateFlag.APPEND), null, null).close();
      fail("Append to LazyPersist file did not fail as expected");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }
  }

  /**
   * Truncate to lazy persist file is denied.
   * @throws IOException
   */
  @Test
  public void testTruncateIsDenied() throws IOException {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);

    try {
      client.truncate(path.toString(), BLOCK_SIZE/2);
      fail("Truncate to LazyPersist file did not fail as expected");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }
  }

  /**
   * If one or more replicas of a lazyPersist file are lost, then the file
   * must be discarded by the NN, instead of being kept around as a
   * 'corrupt' file.
   */
  @Test
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

  @Test
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
    ensureLazyPersistBlocksAreSaved(locatedBlocks);
  }

  /**
   * RamDisk eviction after lazy persist to disk.
   * @throws Exception
   */
  @Test
  public void testRamDiskEviction() throws Exception {
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

    verifyRamDiskJMXMetric("RamDiskBlocksEvicted", 1);
    verifyRamDiskJMXMetric("RamDiskBlocksEvictedWithoutRead", 1);
  }

  /**
   * RamDisk eviction should not happen on blocks that are not yet
   * persisted on disk.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRamDiskEvictionBeforePersist()
    throws IOException, InterruptedException {
    startUpCluster(true, 1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");
    final int SEED = 0XFADED;

    // Stop lazy writer to ensure block for path1 is not persisted to disk.
    FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

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
    assertTrue(verifyReadRandomFile(path1, BLOCK_SIZE, SEED));
  }

  /**
   * Validates lazy persisted blocks are evicted from RAM_DISK based on LRU.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRamDiskEvictionIsLru()
    throws Exception {
    final int NUM_PATHS = 5;
    startUpCluster(true, NUM_PATHS + EVICTION_LOW_WATERMARK);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path paths[] = new Path[NUM_PATHS * 2];

    for (int i = 0; i < paths.length; i++) {
      paths[i] = new Path("/" + METHOD_NAME + "." + i +".dat");
    }

    for (int i = 0; i < NUM_PATHS; i++) {
      makeTestFile(paths[i], BLOCK_SIZE, true);
    }

    // Sleep for a short time to allow the lazy writer thread to do its job.
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);

    for (int i = 0; i < NUM_PATHS; ++i) {
      ensureFileReplicasOnStorageType(paths[i], RAM_DISK);
    }

    // Open the files for read in a random order.
    ArrayList<Integer> indexes = new ArrayList<Integer>(NUM_PATHS);
    for (int i = 0; i < NUM_PATHS; ++i) {
      indexes.add(i);
    }
    Collections.shuffle(indexes);

    for (int i = 0; i < NUM_PATHS; ++i) {
      LOG.info("Touching file " + paths[indexes.get(i)]);
      DFSTestUtil.readFile(fs, paths[indexes.get(i)]);
    }

    // Create an equal number of new files ensuring that the previous
    // files are evicted in the same order they were read.
    for (int i = 0; i < NUM_PATHS; ++i) {
      makeTestFile(paths[i + NUM_PATHS], BLOCK_SIZE, true);
      triggerBlockReport();
      Thread.sleep(3000);
      ensureFileReplicasOnStorageType(paths[i + NUM_PATHS], RAM_DISK);
      ensureFileReplicasOnStorageType(paths[indexes.get(i)], DEFAULT);
      for (int j = i + 1; j < NUM_PATHS; ++j) {
        ensureFileReplicasOnStorageType(paths[indexes.get(j)], RAM_DISK);
      }
    }

    verifyRamDiskJMXMetric("RamDiskBlocksWrite", NUM_PATHS * 2);
    verifyRamDiskJMXMetric("RamDiskBlocksWriteFallback", 0);
    verifyRamDiskJMXMetric("RamDiskBytesWrite", BLOCK_SIZE * NUM_PATHS * 2);
    verifyRamDiskJMXMetric("RamDiskBlocksReadHits", NUM_PATHS);
    verifyRamDiskJMXMetric("RamDiskBlocksEvicted", NUM_PATHS);
    verifyRamDiskJMXMetric("RamDiskBlocksEvictedWithoutRead", 0);
    verifyRamDiskJMXMetric("RamDiskBlocksDeletedBeforeLazyPersisted", 0);
  }

  /**
   * Delete lazy-persist file that has not been persisted to disk.
   * Memory is freed up and file is gone.
   * @throws IOException
   */
  @Test
  public void testDeleteBeforePersist()
    throws Exception {
    startUpCluster(true, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

    Path path = new Path("/" + METHOD_NAME + ".dat");
    makeTestFile(path, BLOCK_SIZE, true);
    LocatedBlocks locatedBlocks =
      ensureFileReplicasOnStorageType(path, RAM_DISK);

    // Delete before persist
    client.delete(path.toString(), false);
    Assert.assertFalse(fs.exists(path));

    assertThat(verifyDeletedBlocks(locatedBlocks), is(true));

    verifyRamDiskJMXMetric("RamDiskBlocksDeletedBeforeLazyPersisted", 1);
  }

  /**
   * Delete lazy-persist file that has been persisted to disk
   * Both memory blocks and disk blocks are deleted.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testDeleteAfterPersist()
    throws Exception {
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

    assertThat(verifyDeletedBlocks(locatedBlocks), is(true));

    verifyRamDiskJMXMetric("RamDiskBlocksLazyPersisted", 1);
    verifyRamDiskJMXMetric("RamDiskBytesLazyPersisted", BLOCK_SIZE);
  }

  /**
   * RAM_DISK used/free space
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
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
  @Test
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
  @Test
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

  @Test
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
    triggerBlockReport();

    // Ensure that the replica is now on persistent storage.
    ensureFileReplicasOnStorageType(path1, DEFAULT);
  }

  @Test
  public void testDnRestartWithUnsavedReplicas()
      throws IOException, InterruptedException {

    startUpCluster(true, 1);
    FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

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
