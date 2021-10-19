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
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class TestLazyPersistReplicaPlacement extends LazyPersistTestCase {
  @Test
  public void testPlacementOnRamDisk()
      throws IOException, TimeoutException, InterruptedException {
    getClusterBuilder().build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, RAM_DISK);
  }

  @Test
  public void testPlacementOnSizeLimitedRamDisk()
      throws IOException, TimeoutException, InterruptedException {
    getClusterBuilder().setRamDiskReplicaCapacity(3).build();
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
  public void testFallbackToDisk()
      throws IOException, TimeoutException, InterruptedException {
    getClusterBuilder().setHasTransientStorage(false).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, DEFAULT);
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
    verifyRamDiskJMXMetric("RamDiskBlocksEvictedWithoutRead", 1);
  }

  /**
   * File can not fit in RamDisk even with eviction
   * @throws IOException
   */
  @Test
  public void testFallbackToDiskFull() throws Exception {
    getClusterBuilder().setMaxLockedMemory(BLOCK_SIZE / 2).build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, DEFAULT);
    verifyRamDiskJMXMetric("RamDiskBlocksWriteFallback", 1);
  }

  /**
   * File partially fit in RamDisk after eviction.
   * RamDisk can fit 2 blocks. Write a file with 5 blocks.
   * Expect 2 blocks are on RamDisk and rest on disk.
   * @throws IOException
   */
  @Test
  public void testFallbackToDiskPartial()
      throws IOException, InterruptedException, TimeoutException {
    getClusterBuilder().setMaxLockedMemory(2 * BLOCK_SIZE).build();
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
    assertThat(numBlocksOnRamDisk, is(2));
    assertThat(numBlocksOnDisk, is(3));
  }

  /**
   * If the only available storage is RAM_DISK and the LAZY_PERSIST flag is not
   * specified, then block placement should fail.
   *
   * @throws IOException
   */
  @Test
  public void testRamDiskNotChosenByDefault() throws IOException {
    getClusterBuilder().setStorageTypes(new StorageType[] {RAM_DISK, RAM_DISK})
                       .build();
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    try {
      makeTestFile(path, BLOCK_SIZE, false);
      fail("Block placement to RAM_DISK should have failed without lazyPersist flag");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }
  }
}
