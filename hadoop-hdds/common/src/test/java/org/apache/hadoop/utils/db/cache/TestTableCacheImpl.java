/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.utils.db.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import com.google.common.base.Optional;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.fail;

/**
 * Class tests partial table cache.
 */
@RunWith(value = Parameterized.class)
public class TestTableCacheImpl {
  private TableCache<CacheKey<String>, CacheValue<String>> tableCache;

  private final TableCacheImpl.CacheCleanupPolicy cacheCleanupPolicy;


  @Parameterized.Parameters
  public static Collection<Object[]> policy() {
    Object[][] params = new Object[][] {
        {TableCacheImpl.CacheCleanupPolicy.NEVER},
        {TableCacheImpl.CacheCleanupPolicy.MANUAL}
    };
    return Arrays.asList(params);
  }

  public TestTableCacheImpl(
      TableCacheImpl.CacheCleanupPolicy cacheCleanupPolicy) {
    this.cacheCleanupPolicy = cacheCleanupPolicy;
  }


  @Before
  public void create() {
    tableCache =
        new TableCacheImpl<>(cacheCleanupPolicy);
  }
  @Test
  public void testPartialTableCache() {


    for (int i = 0; i< 10; i++) {
      tableCache.put(new CacheKey<>(Integer.toString(i)),
          new CacheValue<>(Optional.of(Integer.toString(i)), i));
    }


    for (int i=0; i < 10; i++) {
      Assert.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i))).getCacheValue());
    }

    // On a full table cache if some one calls cleanup it is a no-op.
    tableCache.cleanup(4);

    for (int i=5; i < 10; i++) {
      Assert.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i))).getCacheValue());
    }
  }


  @Test
  public void testPartialTableCacheParallel() throws Exception {

    int totalCount = 0;
    CompletableFuture<Integer> future =
        CompletableFuture.supplyAsync(() -> {
          try {
            return writeToCache(10, 1, 0);
          } catch (InterruptedException ex) {
            fail("writeToCache got interrupt exception");
          }
          return 0;
        });
    int value = future.get();
    Assert.assertEquals(10, value);

    totalCount += value;

    future =
        CompletableFuture.supplyAsync(() -> {
          try {
            return writeToCache(10, 11, 100);
          } catch (InterruptedException ex) {
            fail("writeToCache got interrupt exception");
          }
          return 0;
        });

    // Check we have first 10 entries in cache.
    for (int i=1; i <= 10; i++) {
      Assert.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i))).getCacheValue());
    }


    value = future.get();
    Assert.assertEquals(10, value);

    totalCount += value;

    if (cacheCleanupPolicy == TableCacheImpl.CacheCleanupPolicy.MANUAL) {
      int deleted = 5;

      // cleanup first 5 entires
      tableCache.cleanup(deleted);

      // We should totalCount - deleted entries in cache.
      final int tc = totalCount;
      GenericTestUtils.waitFor(() -> (tc - deleted == tableCache.size()), 100,
          5000);
      // Check if we have remaining entries.
      for (int i=6; i <= totalCount; i++) {
        Assert.assertEquals(Integer.toString(i), tableCache.get(
            new CacheKey<>(Integer.toString(i))).getCacheValue());
      }
      tableCache.cleanup(10);

      tableCache.cleanup(totalCount);

      // Cleaned up all entries, so cache size should be zero.
      GenericTestUtils.waitFor(() -> (0 == tableCache.size()), 100,
          5000);
    } else {
      tableCache.cleanup(totalCount);
      Assert.assertEquals(totalCount, tableCache.size());
    }


  }

  private int writeToCache(int count, int startVal, long sleep)
      throws InterruptedException {
    int counter = 1;
    while (counter <= count){
      tableCache.put(new CacheKey<>(Integer.toString(startVal)),
          new CacheValue<>(Optional.of(Integer.toString(startVal)), startVal));
      startVal++;
      counter++;
      Thread.sleep(sleep);
    }
    return count;
  }
}
