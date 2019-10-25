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

package org.apache.hadoop.hdds.utils.db.cache;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Cache implementation for the table. Depending on the cache clean up policy
 * this cache will be full cache or partial cache.
 *
 * If cache cleanup policy is set as {@link CacheCleanupPolicy#MANUAL},
 * this will be a partial cache.
 *
 * If cache cleanup policy is set as {@link CacheCleanupPolicy#NEVER},
 * this will be a full cache.
 */
@Private
@Evolving
public class TableCacheImpl<CACHEKEY extends CacheKey,
    CACHEVALUE extends CacheValue> implements TableCache<CACHEKEY, CACHEVALUE> {

  private final Map<CACHEKEY, CACHEVALUE> cache;
  private final NavigableSet<EpochEntry<CACHEKEY>> epochEntries;
  private ExecutorService executorService;
  private CacheCleanupPolicy cleanupPolicy;



  public TableCacheImpl(CacheCleanupPolicy cleanupPolicy) {

    // As for full table cache only we need elements to be inserted in sorted
    // manner, so that list will be easy. For other we can go with Hash map.
    if (cleanupPolicy == CacheCleanupPolicy.NEVER) {
      cache = new ConcurrentSkipListMap<>();
    } else {
      cache = new ConcurrentHashMap<>();
    }
    epochEntries = new ConcurrentSkipListSet<>();
    // Created a singleThreadExecutor, so one cleanup will be running at a
    // time.
    ThreadFactory build = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("PartialTableCache Cleanup Thread - %d").build();
    executorService = Executors.newSingleThreadExecutor(build);
    this.cleanupPolicy = cleanupPolicy;
  }

  @Override
  public CACHEVALUE get(CACHEKEY cachekey) {
    return cache.get(cachekey);
  }

  @Override
  public void loadInitial(CACHEKEY cacheKey, CACHEVALUE cacheValue) {
    // No need to add entry to epochEntries. Adding to cache is required during
    // normal put operation.
    cache.put(cacheKey, cacheValue);
  }

  @Override
  public void put(CACHEKEY cacheKey, CACHEVALUE value) {
    cache.put(cacheKey, value);
    epochEntries.add(new EpochEntry<>(value.getEpoch(), cacheKey));
  }

  @Override
  public void cleanup(long epoch) {
    executorService.submit(() -> evictCache(epoch, cleanupPolicy));
  }

  @Override
  public int size() {
    return cache.size();
  }

  @Override
  public Iterator<Map.Entry<CACHEKEY, CACHEVALUE>> iterator() {
    return cache.entrySet().iterator();
  }

  private void evictCache(long epoch, CacheCleanupPolicy cacheCleanupPolicy) {
    EpochEntry<CACHEKEY> currentEntry = null;
    for (Iterator<EpochEntry<CACHEKEY>> iterator = epochEntries.iterator();
         iterator.hasNext();) {
      currentEntry = iterator.next();
      CACHEKEY cachekey = currentEntry.getCachekey();
      CacheValue cacheValue = cache.computeIfPresent(cachekey, ((k, v) -> {
        if (cleanupPolicy == CacheCleanupPolicy.MANUAL) {
          if (v.getEpoch() <= epoch) {
            iterator.remove();
            return null;
          }
        } else if (cleanupPolicy == CacheCleanupPolicy.NEVER) {
          // Remove only entries which are marked for delete.
          if (v.getEpoch() <= epoch && v.getCacheValue() == null) {
            iterator.remove();
            return null;
          }
        }
        return v;
      }));
      // If currentEntry epoch is greater than epoch, we have deleted all
      // entries less than specified epoch. So, we can break.
      if (cacheValue != null && cacheValue.getEpoch() >= epoch) {
        break;
      }
    }
  }

  public CacheResult<CACHEVALUE> lookup(CACHEKEY cachekey) {

    CACHEVALUE cachevalue = cache.get(cachekey);
    if (cachevalue == null) {
      if (cleanupPolicy == CacheCleanupPolicy.NEVER) {
        return new CacheResult<>(CacheResult.CacheStatus.NOT_EXIST, null);
      } else {
        return new CacheResult<>(CacheResult.CacheStatus.MAY_EXIST,
            null);
      }
    } else {
      if (cachevalue.getCacheValue() != null) {
        return new CacheResult<>(CacheResult.CacheStatus.EXISTS, cachevalue);
      } else {
        // When entity is marked for delete, cacheValue will be set to null.
        // In that case we can return NOT_EXIST irrespective of cache cleanup
        // policy.
        return new CacheResult<>(CacheResult.CacheStatus.NOT_EXIST, null);
      }
    }
  }

  /**
   * Cleanup policies for table cache.
   */
  public enum CacheCleanupPolicy {
    NEVER, // Cache will not be cleaned up. This mean's the table maintains
    // full cache.
    MANUAL // Cache will be cleaned up, once after flushing to DB. It is
    // caller's responsibility to flush to DB, before calling cleanup cache.
  }
}
