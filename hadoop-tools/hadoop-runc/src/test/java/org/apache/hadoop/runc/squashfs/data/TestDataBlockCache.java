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

package org.apache.hadoop.runc.squashfs.data;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class TestDataBlockCache {

  private DataBlockCache cache;
  private DataBlockCache.Key[] keys;
  private DataBlock[] blocks;
  private DataBlockCache.Key extraKey;
  private DataBlock extraBlock;

  @Before
  public void setUp() throws Exception {
    int size = 64;
    keys = new DataBlockCache.Key[size];
    blocks = new DataBlock[size];

    cache = new DataBlockCache(size);
    for (int i = 0; i < size; i++) {
      keys[i] = new DataBlockCache.Key(1, false, i, 32, 32);
      blocks[i] = new DataBlock(new byte[32], 32, 32);
      cache.put(keys[i], blocks[i]);
    }
    extraKey = new DataBlockCache.Key(1, false, size, 32, 32);
    extraBlock = new DataBlock(new byte[32], 32, 32);
  }

  @After
  public void tearDown() {
    cache = null;
    keys = null;
    blocks = null;
    extraKey = null;
    extraBlock = null;
  }

  @Test
  public void nullCacheShouldAlwaysMiss() throws Exception {
    cache = DataBlockCache.NO_CACHE;
    cache.clearCache();

    for (int i = 0; i < keys.length; i++) {
      assertNull("not null block", cache.get(keys[i]));
      cache.put(keys[i], blocks[i]);
      assertNull("not null block", cache.get(keys[i]));
    }
    assertEquals("wrong hit count", 0L, cache.getCacheHits());
    assertEquals("wrong miss count", (long) keys.length * 2L,
        cache.getCacheMisses());
    assertEquals("wrong cache load", 0, cache.getCacheLoad());
  }

  @Test
  public void readingAllBlocksShouldResultInAllCacheHits() throws Exception {
    for (int i = 0; i < keys.length; i++) {
      assertSame("wrong block", blocks[i], cache.get(keys[i]));
    }
    assertEquals("wrong hit count", (long) keys.length, cache.getCacheHits());
    assertEquals("wrong miss count", 0L, cache.getCacheMisses());
    assertEquals("wrong cache load", keys.length, cache.getCacheLoad());
  }

  @Test
  public void readingMoreThanCapacityShouldResultInOneCacheMiss()
      throws Exception {
    cache.put(extraKey, extraBlock);
    for (int i = 0; i < keys.length; i++) {
      if (i == 0) {
        assertNull("not null block", cache.get(keys[i]));
      } else {
        assertSame("wrong block", blocks[i], cache.get(keys[i]));

      }
    }
    assertEquals("wrong hit count", (long) (keys.length - 1),
        cache.getCacheHits());
    assertEquals("wrong miss count", 1L, cache.getCacheMisses());
    assertEquals("wrong cache load", keys.length, cache.getCacheLoad());
  }

  @Test
  public void readingAndWritingAllBlocksShouldResultInHalfCacheMisses()
      throws Exception {
    cache.clearCache();

    for (int i = 0; i < keys.length; i++) {
      assertNull("not null block", cache.get(keys[i]));
      cache.put(keys[i], blocks[i]);
      assertSame("wrong block", blocks[i], cache.get(keys[i]));
    }
    assertEquals("wrong hit count", (long) keys.length, cache.getCacheHits());
    assertEquals("wrong miss count", (long) keys.length,
        cache.getCacheMisses());
    assertEquals("wrong cache load", keys.length, cache.getCacheLoad());
  }

}
