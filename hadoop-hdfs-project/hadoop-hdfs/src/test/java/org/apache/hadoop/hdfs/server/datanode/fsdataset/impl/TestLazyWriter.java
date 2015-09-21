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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestLazyWriter extends LazyPersistTestCase {
  @Test
  public void testLazyPersistBlocksAreSaved()
      throws IOException, InterruptedException, TimeoutException {
    getClusterBuilder().build();
    final int NUM_BLOCKS = 10;
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    // Create a test file
    makeTestFile(path, BLOCK_SIZE * NUM_BLOCKS, true);
    LocatedBlocks locatedBlocks = ensureFileReplicasOnStorageType(path, RAM_DISK);
    waitForMetric("RamDiskBlocksLazyPersisted", NUM_BLOCKS);
    LOG.info("Verifying copy was saved to lazyPersist/");

    // Make sure that there is a saved copy of the replica on persistent
    // storage.
    ensureLazyPersistBlocksAreSaved(locatedBlocks);
  }

  @Test
  public void testSynchronousEviction() throws Exception {
    getClusterBuilder().setMaxLockedMemory(BLOCK_SIZE).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();

    final Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    // Wait until the replica is written to persistent storage.
    waitForMetric("RamDiskBlocksLazyPersisted", 1);

    // Ensure that writing a new file to RAM DISK evicts the block
    // for the previous one.
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");
    makeTestFile(path2, BLOCK_SIZE, true);
    waitForMetric("RamDiskBlocksEvicted", 1);
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
      throws Exception {
    getClusterBuilder().setMaxLockedMemory(BLOCK_SIZE).build();
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
    verifyRamDiskJMXMetric("RamDiskBlocksEvicted", 0);
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
    getClusterBuilder().setMaxLockedMemory(NUM_PATHS * BLOCK_SIZE).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path paths[] = new Path[NUM_PATHS * 2];

    for (int i = 0; i < paths.length; i++) {
      paths[i] = new Path("/" + METHOD_NAME + "." + i +".dat");
    }

    for (int i = 0; i < NUM_PATHS; i++) {
      makeTestFile(paths[i], BLOCK_SIZE, true);
    }

    waitForMetric("RamDiskBlocksLazyPersisted", NUM_PATHS);

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
    getClusterBuilder().build();
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
    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    LocatedBlocks locatedBlocks = ensureFileReplicasOnStorageType(path, RAM_DISK);
    waitForMetric("RamDiskBlocksLazyPersisted", 1);

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
      throws IOException, InterruptedException, TimeoutException {
    getClusterBuilder().setRamDiskReplicaCapacity(4).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    // Get the usage before write BLOCK_SIZE
    long usedBeforeCreate = fs.getUsed();

    makeTestFile(path, BLOCK_SIZE, true);
    long usedAfterCreate = fs.getUsed();

    assertThat(usedAfterCreate, is((long) BLOCK_SIZE));

    waitForMetric("RamDiskBlocksLazyPersisted", 1);

    long usedAfterPersist = fs.getUsed();
    assertThat(usedAfterPersist, is((long) BLOCK_SIZE));

    // Delete after persist
    client.delete(path.toString(), false);
    long usedAfterDelete = fs.getUsed();

    assertThat(usedBeforeCreate, is(usedAfterDelete));
  }
}
