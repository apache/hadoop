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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.SimpleReadCacheManager;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_CAPACITY_BYTES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_CAPACITY_THRESHOLD;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_SCAN_INTERVAL_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the {@link SimpleReadCacheManager}.
 */
public class TestSimpleReadCacheManager {

  private static final double CACHE_FRACTION = 0.5;
  private static final double CACHE_THRESHOLD = 1;
  private static final long SCAN_INTERVAL_MS = 100;
  private static final long TOTAL_CAPACITY = 1000;
  private static final long MAX_CACHE_ALLOWED =
      (long) (TOTAL_CAPACITY * CACHE_FRACTION * CACHE_THRESHOLD);
  private static SimpleReadCacheManager cacheManager;

  @BeforeClass
  public static void setup() {
    Configuration conf = new Configuration();
    conf.setDouble(DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION, CACHE_FRACTION);
    conf.setDouble(DFS_PROVIDED_READ_CACHE_CAPACITY_THRESHOLD, CACHE_THRESHOLD);
    conf.set(DFS_PROVIDED_READ_CACHE_SCAN_INTERVAL_MS, SCAN_INTERVAL_MS +"ms");

    // setup mocks for FSN and BlockManager.
    FSNamesystem mockFSN = mock(FSNamesystem.class);
    BlockManager mockBlockManager = mock(BlockManager.class);
    doNothing().when(mockFSN).writeLock();
    doNothing().when(mockFSN).writeUnlock();
    when(mockFSN.hasWriteLock()).thenReturn(true);
    when(mockFSN.getCapacityTotal()).thenReturn(TOTAL_CAPACITY);

    cacheManager = new SimpleReadCacheManager(conf, mockFSN, mockBlockManager);
  }

  @Test
  public void testEviction() throws Exception {
    // throws exception without starting monitor thread.
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> cacheManager.addCachedBlock(
            mock(BlockInfo.class), mock(DatanodeStorageInfo.class)));

    cacheManager.startService();

    int numBlocks = 4;
    BlockInfo[] blocks = new BlockInfo[numBlocks];
    for (int i = 0; i < numBlocks; i++) {
      blocks[i] = new BlockInfoContiguous(
          new Block(i, MAX_CACHE_ALLOWED / numBlocks, 100), (short) 3);
      cacheManager.addCachedBlock(blocks[i], mock(DatanodeStorageInfo.class));
    }

    // all blocks should fit in the cache.
    assertTrue(cacheManager.getCacheUsedForProvided() <= MAX_CACHE_ALLOWED);
    assertEquals(0, cacheManager.getNumBlocksEvicted());

    // add a block that occupies all of the cache.
    cacheManager.addCachedBlock(
        new BlockInfoContiguous(
            new Block(numBlocks + 1, MAX_CACHE_ALLOWED, 100), (short) 3),
        mock(DatanodeStorageInfo.class));
    // allow scan to kick in.
    Thread.sleep(SCAN_INTERVAL_MS * 2);
    // cache used should still be less than max.
    assertTrue(cacheManager.getCacheUsedForProvided() <= MAX_CACHE_ALLOWED);
    // at least one block needs to be evicted.
    assertTrue(cacheManager.getNumBlocksEvicted() >= 1);
    assertFalse(cacheManager.getBlocksCached().isEmpty());
    // add the older blocks again.
    for (int i = 0; i < 4; i++) {
      cacheManager.addCachedBlock(blocks[i], mock(DatanodeStorageInfo.class));
    }
    // allow scan to kick in.
    Thread.sleep(SCAN_INTERVAL_MS * 2);
    // cache used should still be less than max.
    assertTrue(cacheManager.getCacheUsedForProvided() <= MAX_CACHE_ALLOWED);
    // one more block should be evicted.
    assertTrue(cacheManager.getNumBlocksEvicted() >= 2);
    assertFalse(cacheManager.getBlocksCached().isEmpty());
    cacheManager.stopService();
  }

  @Test
  public void testBlockAddRemove() throws Exception {
    cacheManager.startService();

    int numBlocks = 5;
    // size blocks so that only (numBlocks - 1) can fit.
    long blockSize = MAX_CACHE_ALLOWED / (numBlocks - 1);
    BlockInfo[] blocks = new BlockInfo[numBlocks];
    for (int i = 0; i < numBlocks; i++) {
      blocks[i] =
          new BlockInfoContiguous(new Block(i, blockSize, 100), (short) 3);

    }

    DatanodeStorageInfo dnInfo = mock(DatanodeStorageInfo.class);
    DatanodeID dnID =
        new DatanodeID("0.0.0.0", "nonexistantHost", "1", 0, 0, 0, 0);
    DatanodeDescriptor dnDesc = new DatanodeDescriptor(dnID);
    when(dnInfo.getDatanodeDescriptor()).thenReturn(dnDesc);

    // add num -1 blocks.
    for (int i = 0; i < numBlocks-1; i++) {
      cacheManager.addCachedBlock(blocks[i], dnInfo);
    }
    assertTrue(cacheManager.getCacheUsedForProvided() <= MAX_CACHE_ALLOWED);
    assertEquals(0, cacheManager.getNumBlocksEvicted());
    assertFalse(cacheManager.getBlocksCached().isEmpty());

    // remove and add one block.
    cacheManager.removeCachedBlocks(Arrays.asList(blocks[0]));
    cacheManager.addCachedBlock(blocks[numBlocks - 1],
        mock(DatanodeStorageInfo.class));
    // no evictions.
    assertTrue(cacheManager.getCacheUsedForProvided() <= MAX_CACHE_ALLOWED);
    assertEquals(0, cacheManager.getNumBlocksEvicted());

    // add back the block, results in 1 eviction
    cacheManager.addCachedBlock(blocks[0], dnInfo);
    // allow scan to kick in.
    Thread.sleep(SCAN_INTERVAL_MS * 2);
    long cacheUsed = cacheManager.getCacheUsedForProvided();
    assertTrue(cacheUsed <= MAX_CACHE_ALLOWED);
    assertEquals(1, cacheManager.getNumBlocksEvicted());

    // remove one of the remaining blocks
    List<BlockInfo> blocksCached = cacheManager.getBlocksCached();
    cacheManager.removeCachedBlock(blocksCached.get(0), dnDesc);
    // block size should be freed from before.
    assertEquals(cacheUsed - blockSize, cacheManager.getCacheUsedForProvided());
    cacheManager.stopService();
  }

  @Test
  public void testCacheCapacity() {
    long capacity = 1000;
    FSNamesystem mockFSN = mock(FSNamesystem.class);
    BlockManager mockBlockManager = mock(BlockManager.class);
    when(mockFSN.getCapacityTotal()).thenReturn(capacity);

    double cacheFractionToUse = -1;
    long cacheCapacityBytes = -1;
    double cacheThreshold = 0.5;

    Configuration conf = new Configuration();
    conf.setDouble(DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION,
        cacheFractionToUse);
    conf.setDouble(DFS_PROVIDED_READ_CACHE_CAPACITY_THRESHOLD, cacheThreshold);
    conf.setLong(DFS_PROVIDED_READ_CACHE_CAPACITY_BYTES, cacheCapacityBytes);
    conf.set(DFS_PROVIDED_READ_CACHE_SCAN_INTERVAL_MS, SCAN_INTERVAL_MS +"ms");

    SimpleReadCacheManager manager =
        new SimpleReadCacheManager(conf, mockFSN, mockBlockManager);
    // as both are negative, we have to get 0.
    assertEquals(0, manager.getCacheCapacityForProvided());

    // ignores the cacheCapacityBytes now as it is negative
    cacheFractionToUse = 0.1;
    conf.setDouble(DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION,
        cacheFractionToUse);
    manager = new SimpleReadCacheManager(conf, mockFSN, mockBlockManager);
    long expectedCapacity = (long) (cacheFractionToUse * capacity);
    assertEquals(expectedCapacity, manager.getCacheCapacityForProvided());
    assertEquals(manager.getCacheSpaceAllowed(),
        (long) (cacheThreshold * manager.getCacheCapacityForProvided()));

    // now as both are positive, the minimum is used.
    cacheCapacityBytes = 10;
    conf.setLong(DFS_PROVIDED_READ_CACHE_CAPACITY_BYTES, cacheCapacityBytes);
    manager = new SimpleReadCacheManager(conf, mockFSN, mockBlockManager);
    expectedCapacity =
        (long) (Math.min(cacheFractionToUse * capacity, cacheCapacityBytes));
    assertEquals(expectedCapacity, manager.getCacheCapacityForProvided());
    assertEquals(manager.getCacheSpaceAllowed(),
        (long) (cacheThreshold * manager.getCacheCapacityForProvided()));

    cacheCapacityBytes = capacity;
    conf.setLong(DFS_PROVIDED_READ_CACHE_CAPACITY_BYTES, cacheCapacityBytes);
    manager = new SimpleReadCacheManager(conf, mockFSN, mockBlockManager);
    expectedCapacity =
        (long) (Math.min(cacheFractionToUse * capacity, cacheCapacityBytes));
    assertEquals(expectedCapacity, manager.getCacheCapacityForProvided());
    assertEquals(manager.getCacheSpaceAllowed(),
        (long) (cacheThreshold * manager.getCacheCapacityForProvided()));

    // if fraction is negative, the cacheCapacityBytes should be used.
    cacheFractionToUse = -1;
    conf.setDouble(DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION,
        cacheFractionToUse);
    manager = new SimpleReadCacheManager(conf, mockFSN, mockBlockManager);
    assertEquals(cacheCapacityBytes, manager.getCacheCapacityForProvided());
    assertEquals(manager.getCacheSpaceAllowed(),
        (long) (cacheThreshold * manager.getCacheCapacityForProvided()));

    // configuring large value should still be limited to total capacity.
    cacheCapacityBytes = capacity * 100;
    conf.setLong(DFS_PROVIDED_READ_CACHE_CAPACITY_BYTES, cacheCapacityBytes);
    manager = new SimpleReadCacheManager(conf, mockFSN, mockBlockManager);
    assertEquals(capacity, manager.getCacheCapacityForProvided());
    assertEquals(manager.getCacheSpaceAllowed(),
        (long) (cacheThreshold * manager.getCacheCapacityForProvided()));
  }
}
