/**
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
package org.apache.hadoop.hbase.io.hfile.slab;

import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.slab.SingleSizeCache;
import org.junit.*;

/**
 * Tests SingleSlabCache.
 * <p>
 *
 * Tests will ensure that evictions operate when they're supposed to and do what
 * they should, and that cached blocks are accessible when expected to be.
 */
public class TestSingleSizeCache {
  SingleSizeCache cache;
  final int CACHE_SIZE = 1000000;
  final int NUM_BLOCKS = 100;
  final int BLOCK_SIZE = CACHE_SIZE / NUM_BLOCKS;
  final int NUM_THREADS = 100;
  final int NUM_QUERIES = 10000;

  @Before
  public void setup() {
    cache = new SingleSizeCache(BLOCK_SIZE, NUM_BLOCKS, null);
  }

  @After
  public void tearDown() {
    cache.shutdown();
  }

  @Ignore @Test
  public void testCacheSimple() throws Exception {
    CacheTestUtils.testCacheSimple(cache, BLOCK_SIZE, NUM_QUERIES);
  }

  @Ignore @Test
  public void testCacheMultiThreaded() throws Exception {
    CacheTestUtils.testCacheMultiThreaded(cache, BLOCK_SIZE,
        NUM_THREADS, NUM_QUERIES, 0.80);
  }

  @Ignore @Test
  public void testCacheMultiThreadedSingleKey() throws Exception {
    CacheTestUtils.hammerSingleKey(cache, BLOCK_SIZE, NUM_THREADS, NUM_QUERIES);
  }

  @Ignore @Test
  public void testCacheMultiThreadedEviction() throws Exception {
    CacheTestUtils.hammerEviction(cache, BLOCK_SIZE, NUM_THREADS, NUM_QUERIES);
  }

  @Ignore @Test
  public void testHeapSizeChanges(){
    CacheTestUtils.testHeapSizeChanges(cache, BLOCK_SIZE);
  }

}
