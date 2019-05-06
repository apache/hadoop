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

import java.util.concurrent.CompletableFuture;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Class tests FullTable cache.
 */
public class TestFullTableCache {

  private TableCache<CacheKey<String>, CacheValue<String>> tableCache;

  @Before
  public void create() {
    tableCache = new FullTableCache<>();
  }


  @Test
  public void testFullTableCache() {
    tableCache = new FullTableCache<>();


    for (int i = 0; i< 10; i++) {
      tableCache.put(new CacheKey<>(Integer.toString(i)),
          new CacheValue<>(Integer.toString(i),
              CacheValue.OperationType.CREATED, i));
    }


    for (int i=0; i < 10; i++) {
      Assert.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i))).getValue());
    }

    // On a full table cache if some one calls cleanup it is a no-op.
    tableCache.cleanup(10);

    for (int i=0; i < 10; i++) {
      Assert.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i))).getValue());
    }
  }

  @Test
  public void testFullTableCacheParallel() throws Exception {

    int totalCount = 0;
    CompletableFuture<Integer> future =
        CompletableFuture.supplyAsync(() -> {
          try {
            return writeToCache(10, 0, 0);
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
            return writeToCache(10, 10, 100);
          } catch (InterruptedException ex) {
            fail("writeToCache got interrupt exception");
          }
          return 0;
        });

    for (int i=0; i < 10; i++) {
      Assert.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i))).getValue());
    }

    value = future.get();
    Assert.assertEquals(10, value);

    totalCount += value;
    Assert.assertEquals(totalCount, tableCache.size());

    for (int i=0; i < 20; i++) {
      Assert.assertEquals(Integer.toString(i),
          tableCache.get(new CacheKey<>(Integer.toString(i))).getValue());
    }
  }

  private int writeToCache(int count, int startVal, long sleep)
      throws InterruptedException {
    int counter = 0;
    while (counter < count){
      tableCache.put(new CacheKey<>(Integer.toString(startVal)),
          new CacheValue<>(Integer.toString(startVal),
              CacheValue.OperationType.CREATED, startVal));
      startVal++;
      counter++;
      Thread.sleep(sleep);
    }
    return count;
  }
}
