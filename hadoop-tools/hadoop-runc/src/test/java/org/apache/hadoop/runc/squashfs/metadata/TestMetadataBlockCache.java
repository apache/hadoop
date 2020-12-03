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

package org.apache.hadoop.runc.squashfs.metadata;

import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.test.MetadataBlockReaderMock;
import org.apache.hadoop.runc.squashfs.test.MetadataTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestMetadataBlockCache {

  private SuperBlock sb;
  private MetadataBlockReaderMock mbr;
  private TaggedMetadataBlockReader tmbr;
  private MetadataBlockCache cache;
  private SortedMap<Long, MetadataBlock> blockMap;

  @Before
  public void setUp() {
    sb = new SuperBlock();
    blockMap = new TreeMap<>();
    for (int i = 0; i < 10000; i++) {
      blockMap.put(
          Long.valueOf(i * 1000L),
          MetadataTestUtils.block(new byte[] {(byte) (i % 100)}));
    }
    mbr = new MetadataBlockReaderMock(10101, sb, blockMap);
    tmbr = new TaggedMetadataBlockReader(true);
    cache = new MetadataBlockCache(tmbr);
    cache.add(10101, mbr);
  }

  @After
  public void tearDown() {
    cache = null;
    mbr = null;
    blockMap = null;
    sb = null;
  }

  @Test
  public void superBlockShouldReturnUnderlyingValue() {
    assertSame(sb, cache.getSuperBlock(10101));
  }

  @Test
  public void readingAllBlocksTwiceShouldResultInNoCacheHits()
      throws Exception {
    for (Map.Entry<Long, MetadataBlock> entry : blockMap.entrySet()) {
      assertSame(String
              .format("wrong block for offset %d", entry.getKey().longValue()),
          entry.getValue(),
          cache.read(10101, entry.getKey().longValue()));
    }
    for (Map.Entry<Long, MetadataBlock> entry : blockMap.entrySet()) {
      assertSame(String
              .format("wrong block for offset %d", entry.getKey().longValue()),
          entry.getValue(),
          cache.read(10101, entry.getKey().longValue()));
    }
    assertEquals("wrong hit count", 0L, cache.getCacheHits());
    assertEquals("wrong miss count", 20000L, cache.getCacheMisses());
  }

  @Test
  public void readingSameTenBlocksTwiceShouldResultInEqualCacheHits()
      throws Exception {
    int count = 0;
    for (Map.Entry<Long, MetadataBlock> entry : blockMap.entrySet()) {
      count++;
      if (count > 10) {
        break;
      }
      assertSame(String
              .format("wrong block for offset %d", entry.getKey().longValue()),
          entry.getValue(),
          cache.read(10101, entry.getKey().longValue()));
    }
    count = 0;
    for (Map.Entry<Long, MetadataBlock> entry : blockMap.entrySet()) {
      count++;
      if (count > 10) {
        break;
      }
      assertSame(String
              .format("wrong block for offset %d", entry.getKey().longValue()),
          entry.getValue(),
          cache.read(10101, entry.getKey().longValue()));
    }
    assertEquals("wrong hit count", 10L, cache.getCacheHits());
    assertEquals("wrong miss count", 10L, cache.getCacheMisses());
  }

  @Test
  public void resetStatisticsShouldResetHitsAndMisses() throws Exception {
    MetadataBlock block = blockMap.get(Long.valueOf(0L));
    assertSame(block, cache.read(10101, Long.valueOf(0L)));
    assertSame(block, cache.read(10101, Long.valueOf(0L)));

    assertEquals("wrong hit count", 1L, cache.getCacheHits());
    assertEquals("wrong miss count", 1L, cache.getCacheMisses());

    cache.resetStatistics();

    assertEquals("wrong hit count", 0L, cache.getCacheHits());
    assertEquals("wrong miss count", 0L, cache.getCacheMisses());
  }

  @Test
  public void clearCacheShouldResetHitsAndMisses() throws Exception {
    MetadataBlock block = blockMap.get(Long.valueOf(0L));
    assertSame(block, cache.read(10101, Long.valueOf(0L)));
    assertSame(block, cache.read(10101, Long.valueOf(0L)));

    assertEquals("wrong hit count", 1L, cache.getCacheHits());
    assertEquals("wrong miss count", 1L, cache.getCacheMisses());

    cache.clearCache();

    assertEquals("wrong hit count", 0L, cache.getCacheHits());
    assertEquals("wrong miss count", 0L, cache.getCacheMisses());
  }

  @Test
  public void cacheSizeOfZeroShouldBeInterpretedAsOne() throws Exception {
    cache = new MetadataBlockCache(tmbr, 0);
    MetadataBlock block = blockMap.get(Long.valueOf(0L));
    assertSame(block, cache.read(10101, Long.valueOf(0L)));
    assertSame(block, cache.read(10101, Long.valueOf(0L)));

    assertEquals("wrong hit count", 1L, cache.getCacheHits());
    assertEquals("wrong miss count", 1L, cache.getCacheMisses());
  }

  @Test
  public void explicitCacheSizeShoudldHitIfQueryingLessThanCacheSize()
      throws Exception {
    cache = new MetadataBlockCache(tmbr, 10);

    int count = 0;
    for (Map.Entry<Long, MetadataBlock> entry : blockMap.entrySet()) {
      count++;
      if (count > 10) {
        break;
      }
      assertSame(String
              .format("wrong block for offset %d", entry.getKey().longValue()),
          entry.getValue(),
          cache.read(10101, entry.getKey().longValue()));
    }
    count = 0;
    for (Map.Entry<Long, MetadataBlock> entry : blockMap.entrySet()) {
      count++;
      if (count > 10) {
        break;
      }
      assertSame(String
              .format("wrong block for offset %d", entry.getKey().longValue()),
          entry.getValue(),
          cache.read(10101, entry.getKey().longValue()));
    }
    assertEquals("wrong hit count", 10L, cache.getCacheHits());
    assertEquals("wrong miss count", 10L, cache.getCacheMisses());
  }

  @Test
  public void explicitCacheSizeShoudldMissIfQueryingMoreThanCacheSize()
      throws Exception {
    cache = new MetadataBlockCache(tmbr, 10);

    int count = 0;
    for (Map.Entry<Long, MetadataBlock> entry : blockMap.entrySet()) {
      count++;
      if (count > 11) {
        break;
      }
      assertSame(String
              .format("wrong block for offset %d", entry.getKey().longValue()),
          entry.getValue(),
          cache.read(10101, entry.getKey().longValue()));
    }
    count = 0;
    for (Map.Entry<Long, MetadataBlock> entry : blockMap.entrySet()) {
      count++;
      if (count > 11) {
        break;
      }
      assertSame(String
              .format("wrong block for offset %d", entry.getKey().longValue()),
          entry.getValue(),
          cache.read(10101, entry.getKey().longValue()));
    }
    assertEquals("wrong hit count", 0L, cache.getCacheHits());
    assertEquals("wrong miss count", 22L, cache.getCacheMisses());
  }

  @Test
  public void closeShouldCloseBlockReaderByDefault()
      throws Exception {
    cache.close();
    assertTrue("not closed", mbr.isClosed());
  }

  @Test
  public void closeShouldCloseBlockReaderIfExplicitlySet()
      throws Exception {
    cache = new MetadataBlockCache(tmbr, true);
    cache.close();
    assertTrue("not closed", mbr.isClosed());
  }

  @Test
  public void closeShouldNotCloseBlockReaderIfExplicitlyUnset()
      throws Exception {
    cache = new MetadataBlockCache(tmbr, false);
    cache.close();
    assertFalse("closed", mbr.isClosed());
  }

  @Test
  public void closeShouldCloseBlockReaderIfExplicitlySetAndSizeSpecified()
      throws Exception {
    cache = new MetadataBlockCache(tmbr, 1, true);
    cache.close();
    assertTrue("not closed", mbr.isClosed());
  }

  @Test
  public void closeShouldNotCloseBlockReaderIfExplicitlyUnsetAndSizeSpecified()
      throws Exception {
    cache = new MetadataBlockCache(tmbr, 1, false);
    cache.close();
    assertFalse("closed", mbr.isClosed());
  }
}
