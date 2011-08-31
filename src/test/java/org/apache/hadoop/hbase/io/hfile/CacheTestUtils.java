/*
 * Copyright 2011 The Apache Software Foundation
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MultithreadedTestUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestThread;

public class CacheTestUtils {

  public static void testCacheMultiThreaded(final BlockCache toBeTested,
      final int blockSize, final int numThreads, final int numQueries,
      final double passingScore) throws Exception {

    Configuration conf = new Configuration();
    MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext(
        conf);

    final AtomicInteger totalQueries = new AtomicInteger();
    final ConcurrentLinkedQueue<HFileBlockPair> blocksToTest = new ConcurrentLinkedQueue<HFileBlockPair>();
    final AtomicInteger hits = new AtomicInteger();
    final AtomicInteger miss = new AtomicInteger();

    HFileBlockPair[] blocks = generateHFileBlocks(numQueries, blockSize);
    blocksToTest.addAll(Arrays.asList(blocks));

    for (int i = 0; i < numThreads; i++) {
      TestThread t = new MultithreadedTestUtil.RepeatingTestThread(ctx) {
        @Override
        public void doAnAction() throws Exception {
          if (!blocksToTest.isEmpty()) {
            HFileBlockPair ourBlock = blocksToTest.poll();
            //if we run out of blocks to test, then we should stop the tests.
            if(ourBlock == null){
              ctx.stop();
              return;
            }
            toBeTested.cacheBlock(ourBlock.blockName, ourBlock.block);
            Cacheable retrievedBlock = toBeTested.getBlock(ourBlock.blockName,
                false);
            if (retrievedBlock != null) {
              assertEquals(ourBlock.block, retrievedBlock);
              hits.incrementAndGet();
            } else {
              miss.incrementAndGet();
            }
            totalQueries.incrementAndGet();
          }
        }
      };
      ctx.addThread(t);
    }
    ctx.startThreads();
    while (!blocksToTest.isEmpty() && ctx.shouldRun()) {
      Thread.sleep(10);
    }
    ctx.stop();
    if((double) hits.get() / ((double) hits.get() +  (double) miss.get()) < passingScore){
      fail("Too many nulls returned. Hits: " + hits.get() + " Misses: " + miss.get());
    }
  }

  public static void testCacheSimple(BlockCache toBeTested, int blockSize,
      int numBlocks) throws Exception {

    HFileBlockPair[] blocks = generateHFileBlocks(numBlocks, blockSize);
    // Confirm empty
    for (HFileBlockPair block : blocks) {
      assertNull(toBeTested.getBlock(block.blockName, true));
    }

    // Add blocks
    for (HFileBlockPair block : blocks) {
      toBeTested.cacheBlock(block.blockName, block.block);
    }

    // Check if all blocks are properly cached and contain the right
    // information, or the blocks are null.
    // MapMaker makes no guarantees when it will evict, so neither can we.

    for (HFileBlockPair block : blocks) {
      HFileBlock buf = (HFileBlock) toBeTested.getBlock(block.blockName, true);
      if (buf != null) {
        assertEquals(block.block, buf);
      }

    }

    // Re-add some duplicate blocks. Hope nothing breaks.

    for (HFileBlockPair block : blocks) {
      try {
        if (toBeTested.getBlock(block.blockName, true) != null) {
          toBeTested.cacheBlock(block.blockName, block.block);
          fail("Cache should not allow re-caching a block");
        }
      } catch (RuntimeException re) {
        // expected
      }
    }

  }

  public static void hammerSingleKey(final BlockCache toBeTested,
      int BlockSize, int numThreads, int numQueries) throws Exception {
    final HFileBlockPair kv = generateHFileBlocks(BlockSize, 1)[0];
    Configuration conf = new Configuration();
    MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext(
        conf);

    final AtomicInteger totalQueries = new AtomicInteger();
    toBeTested.cacheBlock(kv.blockName, kv.block);

    for (int i = 0; i < numThreads; i++) {
      TestThread t = new MultithreadedTestUtil.RepeatingTestThread(ctx) {
        @Override
        public void doAnAction() throws Exception {
          assertEquals(kv.block, toBeTested.getBlock(kv.blockName, false));
          totalQueries.incrementAndGet();
        }
      };

      ctx.addThread(t);
    }

    ctx.startThreads();
    while (totalQueries.get() < numQueries && ctx.shouldRun()) {
      Thread.sleep(10);
    }
    ctx.stop();
  }

  private static HFileBlockPair[] generateHFileBlocks(int blockSize,
      int numBlocks) {
    HFileBlockPair[] returnedBlocks = new HFileBlockPair[numBlocks];
    Random rand = new Random();
    HashSet<String> usedStrings = new HashSet<String>();
    for (int i = 0; i < numBlocks; i++) {

      // The buffer serialized size needs to match the size of BlockSize. So we
      // declare our data size to be smaller than it by the serialization space
      // required.

      ByteBuffer cachedBuffer = ByteBuffer.allocate(blockSize
          - HFileBlock.EXTRA_SERIALIZATION_SPACE);
      rand.nextBytes(cachedBuffer.array());
      cachedBuffer.rewind();
      int onDiskSizeWithoutHeader = blockSize
          - HFileBlock.EXTRA_SERIALIZATION_SPACE;
      int uncompressedSizeWithoutHeader = blockSize
          - HFileBlock.EXTRA_SERIALIZATION_SPACE;
      long prevBlockOffset = rand.nextLong();
      BlockType.DATA.write(cachedBuffer);
      cachedBuffer.putInt(onDiskSizeWithoutHeader);
      cachedBuffer.putInt(uncompressedSizeWithoutHeader);
      cachedBuffer.putLong(prevBlockOffset);
      cachedBuffer.rewind();

      HFileBlock generated = new HFileBlock(BlockType.DATA,
          onDiskSizeWithoutHeader, uncompressedSizeWithoutHeader,
          prevBlockOffset, cachedBuffer, false, blockSize);

      String strKey;
      /* No conflicting keys */
      for (strKey = new Long(rand.nextLong()).toString(); !usedStrings
          .add(strKey); strKey = new Long(rand.nextLong()).toString())
        ;

      returnedBlocks[i] = new HFileBlockPair();
      returnedBlocks[i].blockName = strKey;
      returnedBlocks[i].block = generated;
    }
    return returnedBlocks;
  }

  private static class HFileBlockPair {
    String blockName;
    HFileBlock block;
  }
}
