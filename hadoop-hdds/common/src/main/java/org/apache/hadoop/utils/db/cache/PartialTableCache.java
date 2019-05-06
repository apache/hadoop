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

import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;



/**
 * This is used for the tables where we don't want to cache entire table in
 * in-memory.
 */
@Private
@Evolving
public class PartialTableCache<CACHEKEY, CACHEVALUE>
    implements TableCache<CACHEKEY, CACHEVALUE>{

  private final ConcurrentHashMap<CACHEKEY, CACHEVALUE> cache;
  private final TreeSet<EpochEntry<CACHEKEY>> epochEntries;
  private ExecutorService executorService;



  public PartialTableCache() {
    cache = new ConcurrentHashMap<>();
    epochEntries = new TreeSet<EpochEntry<CACHEKEY>>();
    // Created a singleThreadExecutor, so one cleanup will be running at a
    // time.
    executorService = Executors.newSingleThreadExecutor();
  }

  @Override
  public CACHEVALUE get(CACHEKEY cachekey) {
    return cache.get(cachekey);
  }

  @Override
  public void put(CACHEKEY cacheKey, CACHEVALUE value) {
    cache.put(cacheKey, value);
    CacheValue cacheValue = (CacheValue) cache.get(cacheKey);
    epochEntries.add(new EpochEntry<>(cacheValue.getEpoch(), cacheKey));
  }

  @Override
  public void cleanup(long epoch) {
    executorService.submit(() -> evictCache(epoch));
  }

  @Override
  public int size() {
    return cache.size();
  }

  private void evictCache(long epoch) {
    EpochEntry<CACHEKEY> currentEntry = null;
    for (Iterator iterator = epochEntries.iterator(); iterator.hasNext();) {
      currentEntry = (EpochEntry<CACHEKEY>) iterator.next();
      CACHEKEY cachekey = currentEntry.getCachekey();
      CacheValue cacheValue = (CacheValue) cache.get(cachekey);
      if (cacheValue.getEpoch() <= epoch) {
        cache.remove(cachekey);
        iterator.remove();
      }

      // If currentEntry epoch is greater than epoch, we have deleted all
      // entries less than specified epoch. So, we can break.
      if (currentEntry.getEpoch() > epoch) {
        break;
      }
    }
  }
}
