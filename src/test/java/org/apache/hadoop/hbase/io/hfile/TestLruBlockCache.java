/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io.hfile;

import java.util.Random;

import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.ClassSize;

import junit.framework.TestCase;

/**
 * Tests the concurrent LruBlockCache.<p>
 *
 * Tests will ensure it grows and shrinks in size properly,
 * evictions run when they're supposed to and do what they should,
 * and that cached blocks are accessible when expected to be.
 */
public class TestLruBlockCache extends TestCase {

  public void testBackgroundEvictionThread() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSizeDefault(maxSize, 9); // room for 9, will evict

    LruBlockCache cache = new LruBlockCache(maxSize,blockSize);

    CachedItem [] blocks = generateFixedBlocks(10, blockSize, "block");

    // Add all the blocks
    for (CachedItem block : blocks) {
      cache.cacheBlock(block.blockName, block);
    }

    // Let the eviction run
    int n = 0;
    while(cache.getEvictionCount() == 0) {
      System.out.println("sleep");
      Thread.sleep(1000);
      assertTrue(n++ < 2);
    }
    System.out.println("Background Evictions run: " + cache.getEvictionCount());

    // A single eviction run should have occurred
    assertEquals(cache.getEvictionCount(), 1);
  }

  public void testCacheSimple() throws Exception {

    long maxSize = 1000000;
    long blockSize = calculateBlockSizeDefault(maxSize, 101);

    LruBlockCache cache = new LruBlockCache(maxSize, blockSize);

    CachedItem [] blocks = generateRandomBlocks(100, blockSize);

    long expectedCacheSize = cache.heapSize();

    // Confirm empty
    for (CachedItem block : blocks) {
      assertTrue(cache.getBlock(block.blockName, true) == null);
    }

    // Add blocks
    for (CachedItem block : blocks) {
      cache.cacheBlock(block.blockName, block);
      expectedCacheSize += block.cacheBlockHeapSize();
    }

    // Verify correctly calculated cache heap size
    assertEquals(expectedCacheSize, cache.heapSize());

    // Check if all blocks are properly cached and retrieved
    for (CachedItem block : blocks) {
      HeapSize buf = cache.getBlock(block.blockName, true);
      assertTrue(buf != null);
      assertEquals(buf.heapSize(), block.heapSize());
    }

    // Re-add same blocks and ensure nothing has changed
    for (CachedItem block : blocks) {
      try {
        cache.cacheBlock(block.blockName, block);
        assertTrue("Cache should not allow re-caching a block", false);
      } catch(RuntimeException re) {
        // expected
      }
    }

    // Verify correctly calculated cache heap size
    assertEquals(expectedCacheSize, cache.heapSize());

    // Check if all blocks are properly cached and retrieved
    for (CachedItem block : blocks) {
      HeapSize buf = cache.getBlock(block.blockName, true);
      assertTrue(buf != null);
      assertEquals(buf.heapSize(), block.heapSize());
    }

    // Expect no evictions
    assertEquals(0, cache.getEvictionCount());
    Thread t = new LruBlockCache.StatisticsThread(cache);
    t.start();
    t.join();
  }

  public void testCacheEvictionSimple() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSizeDefault(maxSize, 10);

    LruBlockCache cache = new LruBlockCache(maxSize,blockSize,false);

    CachedItem [] blocks = generateFixedBlocks(10, blockSize, "block");

    long expectedCacheSize = cache.heapSize();

    // Add all the blocks
    for (CachedItem block : blocks) {
      cache.cacheBlock(block.blockName, block);
      expectedCacheSize += block.cacheBlockHeapSize();
    }

    // A single eviction run should have occurred
    assertEquals(1, cache.getEvictionCount());

    // Our expected size overruns acceptable limit
    assertTrue(expectedCacheSize >
      (maxSize * LruBlockCache.DEFAULT_ACCEPTABLE_FACTOR));

    // But the cache did not grow beyond max
    assertTrue(cache.heapSize() < maxSize);

    // And is still below the acceptable limit
    assertTrue(cache.heapSize() <
        (maxSize * LruBlockCache.DEFAULT_ACCEPTABLE_FACTOR));

    // All blocks except block 0 and 1 should be in the cache
    assertTrue(cache.getBlock(blocks[0].blockName, true) == null);
    assertTrue(cache.getBlock(blocks[1].blockName, true) == null);
    for(int i=2;i<blocks.length;i++) {
      assertEquals(cache.getBlock(blocks[i].blockName, true),
          blocks[i]);
    }
  }

  public void testCacheEvictionTwoPriorities() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSizeDefault(maxSize, 10);

    LruBlockCache cache = new LruBlockCache(maxSize,blockSize,false);

    CachedItem [] singleBlocks = generateFixedBlocks(5, 10000, "single");
    CachedItem [] multiBlocks = generateFixedBlocks(5, 10000, "multi");

    long expectedCacheSize = cache.heapSize();

    // Add and get the multi blocks
    for (CachedItem block : multiBlocks) {
      cache.cacheBlock(block.blockName, block);
      expectedCacheSize += block.cacheBlockHeapSize();
      assertEquals(cache.getBlock(block.blockName, true), block);
    }

    // Add the single blocks (no get)
    for (CachedItem block : singleBlocks) {
      cache.cacheBlock(block.blockName, block);
      expectedCacheSize += block.heapSize();
    }

    // A single eviction run should have occurred
    assertEquals(cache.getEvictionCount(), 1);

    // We expect two entries evicted
    assertEquals(cache.getEvictedCount(), 2);

    // Our expected size overruns acceptable limit
    assertTrue(expectedCacheSize >
      (maxSize * LruBlockCache.DEFAULT_ACCEPTABLE_FACTOR));

    // But the cache did not grow beyond max
    assertTrue(cache.heapSize() <= maxSize);

    // And is now below the acceptable limit
    assertTrue(cache.heapSize() <=
        (maxSize * LruBlockCache.DEFAULT_ACCEPTABLE_FACTOR));

    // We expect fairness across the two priorities.
    // This test makes multi go barely over its limit, in-memory
    // empty, and the rest in single.  Two single evictions and
    // one multi eviction expected.
    assertTrue(cache.getBlock(singleBlocks[0].blockName, true) == null);
    assertTrue(cache.getBlock(multiBlocks[0].blockName, true) == null);

    // And all others to be cached
    for(int i=1;i<4;i++) {
      assertEquals(cache.getBlock(singleBlocks[i].blockName, true),
          singleBlocks[i]);
      assertEquals(cache.getBlock(multiBlocks[i].blockName, true),
          multiBlocks[i]);
    }
  }

  public void testCacheEvictionThreePriorities() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSize(maxSize, 10);

    LruBlockCache cache = new LruBlockCache(maxSize, blockSize, false,
        (int)Math.ceil(1.2*maxSize/blockSize),
        LruBlockCache.DEFAULT_LOAD_FACTOR,
        LruBlockCache.DEFAULT_CONCURRENCY_LEVEL,
        0.98f, // min
        0.99f, // acceptable
        0.33f, // single
        0.33f, // multi
        0.34f);// memory


    CachedItem [] singleBlocks = generateFixedBlocks(5, blockSize, "single");
    CachedItem [] multiBlocks = generateFixedBlocks(5, blockSize, "multi");
    CachedItem [] memoryBlocks = generateFixedBlocks(5, blockSize, "memory");

    long expectedCacheSize = cache.heapSize();

    // Add 3 blocks from each priority
    for(int i=0;i<3;i++) {

      // Just add single blocks
      cache.cacheBlock(singleBlocks[i].blockName, singleBlocks[i]);
      expectedCacheSize += singleBlocks[i].cacheBlockHeapSize();

      // Add and get multi blocks
      cache.cacheBlock(multiBlocks[i].blockName, multiBlocks[i]);
      expectedCacheSize += multiBlocks[i].cacheBlockHeapSize();
      cache.getBlock(multiBlocks[i].blockName, true);

      // Add memory blocks as such
      cache.cacheBlock(memoryBlocks[i].blockName, memoryBlocks[i], true);
      expectedCacheSize += memoryBlocks[i].cacheBlockHeapSize();

    }

    // Do not expect any evictions yet
    assertEquals(0, cache.getEvictionCount());

    // Verify cache size
    assertEquals(expectedCacheSize, cache.heapSize());

    // Insert a single block, oldest single should be evicted
    cache.cacheBlock(singleBlocks[3].blockName, singleBlocks[3]);

    // Single eviction, one thing evicted
    assertEquals(1, cache.getEvictionCount());
    assertEquals(1, cache.getEvictedCount());

    // Verify oldest single block is the one evicted
    assertEquals(null, cache.getBlock(singleBlocks[0].blockName, true));

    // Change the oldest remaining single block to a multi
    cache.getBlock(singleBlocks[1].blockName, true);

    // Insert another single block
    cache.cacheBlock(singleBlocks[4].blockName, singleBlocks[4]);

    // Two evictions, two evicted.
    assertEquals(2, cache.getEvictionCount());
    assertEquals(2, cache.getEvictedCount());

    // Oldest multi block should be evicted now
    assertEquals(null, cache.getBlock(multiBlocks[0].blockName, true));

    // Insert another memory block
    cache.cacheBlock(memoryBlocks[3].blockName, memoryBlocks[3], true);

    // Three evictions, three evicted.
    assertEquals(3, cache.getEvictionCount());
    assertEquals(3, cache.getEvictedCount());

    // Oldest memory block should be evicted now
    assertEquals(null, cache.getBlock(memoryBlocks[0].blockName, true));

    // Add a block that is twice as big (should force two evictions)
    CachedItem [] bigBlocks = generateFixedBlocks(3, blockSize*3, "big");
    cache.cacheBlock(bigBlocks[0].blockName, bigBlocks[0]);

    // Four evictions, six evicted (inserted block 3X size, expect +3 evicted)
    assertEquals(4, cache.getEvictionCount());
    assertEquals(6, cache.getEvictedCount());

    // Expect three remaining singles to be evicted
    assertEquals(null, cache.getBlock(singleBlocks[2].blockName, true));
    assertEquals(null, cache.getBlock(singleBlocks[3].blockName, true));
    assertEquals(null, cache.getBlock(singleBlocks[4].blockName, true));

    // Make the big block a multi block
    cache.getBlock(bigBlocks[0].blockName, true);

    // Cache another single big block
    cache.cacheBlock(bigBlocks[1].blockName, bigBlocks[1]);

    // Five evictions, nine evicted (3 new)
    assertEquals(5, cache.getEvictionCount());
    assertEquals(9, cache.getEvictedCount());

    // Expect three remaining multis to be evicted
    assertEquals(null, cache.getBlock(singleBlocks[1].blockName, true));
    assertEquals(null, cache.getBlock(multiBlocks[1].blockName, true));
    assertEquals(null, cache.getBlock(multiBlocks[2].blockName, true));

    // Cache a big memory block
    cache.cacheBlock(bigBlocks[2].blockName, bigBlocks[2], true);

    // Six evictions, twelve evicted (3 new)
    assertEquals(6, cache.getEvictionCount());
    assertEquals(12, cache.getEvictedCount());

    // Expect three remaining in-memory to be evicted
    assertEquals(null, cache.getBlock(memoryBlocks[1].blockName, true));
    assertEquals(null, cache.getBlock(memoryBlocks[2].blockName, true));
    assertEquals(null, cache.getBlock(memoryBlocks[3].blockName, true));


  }

  // test scan resistance
  public void testScanResistance() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSize(maxSize, 10);

    LruBlockCache cache = new LruBlockCache(maxSize, blockSize, false,
        (int)Math.ceil(1.2*maxSize/blockSize),
        LruBlockCache.DEFAULT_LOAD_FACTOR,
        LruBlockCache.DEFAULT_CONCURRENCY_LEVEL,
        0.66f, // min
        0.99f, // acceptable
        0.33f, // single
        0.33f, // multi
        0.34f);// memory

    CachedItem [] singleBlocks = generateFixedBlocks(20, blockSize, "single");
    CachedItem [] multiBlocks = generateFixedBlocks(5, blockSize, "multi");

    // Add 5 multi blocks
    for (CachedItem block : multiBlocks) {
      cache.cacheBlock(block.blockName, block);
      cache.getBlock(block.blockName, true);
    }

    // Add 5 single blocks
    for(int i=0;i<5;i++) {
      cache.cacheBlock(singleBlocks[i].blockName, singleBlocks[i]);
    }

    // An eviction ran
    assertEquals(1, cache.getEvictionCount());

    // To drop down to 2/3 capacity, we'll need to evict 4 blocks
    assertEquals(4, cache.getEvictedCount());

    // Should have been taken off equally from single and multi
    assertEquals(null, cache.getBlock(singleBlocks[0].blockName, true));
    assertEquals(null, cache.getBlock(singleBlocks[1].blockName, true));
    assertEquals(null, cache.getBlock(multiBlocks[0].blockName, true));
    assertEquals(null, cache.getBlock(multiBlocks[1].blockName, true));

    // Let's keep "scanning" by adding single blocks.  From here on we only
    // expect evictions from the single bucket.

    // Every time we reach 10 total blocks (every 4 inserts) we get 4 single
    // blocks evicted.  Inserting 13 blocks should yield 3 more evictions and
    // 12 more evicted.

    for(int i=5;i<18;i++) {
      cache.cacheBlock(singleBlocks[i].blockName, singleBlocks[i]);
    }

    // 4 total evictions, 16 total evicted
    assertEquals(4, cache.getEvictionCount());
    assertEquals(16, cache.getEvictedCount());

    // Should now have 7 total blocks
    assertEquals(7, cache.size());

  }

  // test setMaxSize
  public void testResizeBlockCache() throws Exception {

    long maxSize = 300000;
    long blockSize = calculateBlockSize(maxSize, 31);

    LruBlockCache cache = new LruBlockCache(maxSize, blockSize, false,
        (int)Math.ceil(1.2*maxSize/blockSize),
        LruBlockCache.DEFAULT_LOAD_FACTOR,
        LruBlockCache.DEFAULT_CONCURRENCY_LEVEL,
        0.98f, // min
        0.99f, // acceptable
        0.33f, // single
        0.33f, // multi
        0.34f);// memory

    CachedItem [] singleBlocks = generateFixedBlocks(10, blockSize, "single");
    CachedItem [] multiBlocks = generateFixedBlocks(10, blockSize, "multi");
    CachedItem [] memoryBlocks = generateFixedBlocks(10, blockSize, "memory");

    // Add all blocks from all priorities
    for(int i=0;i<10;i++) {

      // Just add single blocks
      cache.cacheBlock(singleBlocks[i].blockName, singleBlocks[i]);

      // Add and get multi blocks
      cache.cacheBlock(multiBlocks[i].blockName, multiBlocks[i]);
      cache.getBlock(multiBlocks[i].blockName, true);

      // Add memory blocks as such
      cache.cacheBlock(memoryBlocks[i].blockName, memoryBlocks[i], true);
    }

    // Do not expect any evictions yet
    assertEquals(0, cache.getEvictionCount());

    // Resize to half capacity plus an extra block (otherwise we evict an extra)
    cache.setMaxSize((long)(maxSize * 0.5f));

    // Should have run a single eviction
    assertEquals(1, cache.getEvictionCount());

    // And we expect 1/2 of the blocks to be evicted
    assertEquals(15, cache.getEvictedCount());

    // And the oldest 5 blocks from each category should be gone
    for(int i=0;i<5;i++) {
      assertEquals(null, cache.getBlock(singleBlocks[i].blockName, true));
      assertEquals(null, cache.getBlock(multiBlocks[i].blockName, true));
      assertEquals(null, cache.getBlock(memoryBlocks[i].blockName, true));
    }

    // And the newest 5 blocks should still be accessible
    for(int i=5;i<10;i++) {
      assertEquals(singleBlocks[i], cache.getBlock(singleBlocks[i].blockName, true));
      assertEquals(multiBlocks[i], cache.getBlock(multiBlocks[i].blockName, true));
      assertEquals(memoryBlocks[i], cache.getBlock(memoryBlocks[i].blockName, true));
    }
  }

  private CachedItem [] generateFixedBlocks(int numBlocks, int size, String pfx) {
    CachedItem [] blocks = new CachedItem[numBlocks];
    for(int i=0;i<numBlocks;i++) {
      blocks[i] = new CachedItem(pfx + i, size);
    }
    return blocks;
  }

  private CachedItem [] generateFixedBlocks(int numBlocks, long size, String pfx) {
    return generateFixedBlocks(numBlocks, (int)size, pfx);
  }

  private CachedItem [] generateRandomBlocks(int numBlocks, long maxSize) {
    CachedItem [] blocks = new CachedItem[numBlocks];
    Random r = new Random();
    for(int i=0;i<numBlocks;i++) {
      blocks[i] = new CachedItem("block" + i, r.nextInt((int)maxSize)+1);
    }
    return blocks;
  }

  private long calculateBlockSize(long maxSize, int numBlocks) {
    long roughBlockSize = maxSize / numBlocks;
    int numEntries = (int)Math.ceil((1.2)*maxSize/roughBlockSize);
    long totalOverhead = LruBlockCache.CACHE_FIXED_OVERHEAD +
        ClassSize.CONCURRENT_HASHMAP +
        (numEntries * ClassSize.CONCURRENT_HASHMAP_ENTRY) +
        (LruBlockCache.DEFAULT_CONCURRENCY_LEVEL * ClassSize.CONCURRENT_HASHMAP_SEGMENT);
    long negateBlockSize = (long)(totalOverhead/numEntries);
    negateBlockSize += CachedBlock.PER_BLOCK_OVERHEAD;
    return ClassSize.align((long)Math.floor((roughBlockSize - negateBlockSize)*0.99f));
  }

  private long calculateBlockSizeDefault(long maxSize, int numBlocks) {
    long roughBlockSize = maxSize / numBlocks;
    int numEntries = (int)Math.ceil((1.2)*maxSize/roughBlockSize);
    long totalOverhead = LruBlockCache.CACHE_FIXED_OVERHEAD +
        ClassSize.CONCURRENT_HASHMAP +
        (numEntries * ClassSize.CONCURRENT_HASHMAP_ENTRY) +
        (LruBlockCache.DEFAULT_CONCURRENCY_LEVEL * ClassSize.CONCURRENT_HASHMAP_SEGMENT);
    long negateBlockSize = totalOverhead / numEntries;
    negateBlockSize += CachedBlock.PER_BLOCK_OVERHEAD;
    return ClassSize.align((long)Math.floor((roughBlockSize - negateBlockSize)*
        LruBlockCache.DEFAULT_ACCEPTABLE_FACTOR));
  }

  private static class CachedItem implements HeapSize {
    String blockName;
    int size;

    CachedItem(String blockName, int size) {
      this.blockName = blockName;
      this.size = size;
    }

    /** The size of this item reported to the block cache layer */
    @Override
    public long heapSize() {
      return ClassSize.align(size);
    }

    /** Size of the cache block holding this item. Used for verification. */
    public long cacheBlockHeapSize() {
      return CachedBlock.PER_BLOCK_OVERHEAD
          + ClassSize.align(blockName.length())
          + ClassSize.align(size);
    }
  }
}
