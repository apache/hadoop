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

package org.apache.hadoop.utils.db.cache;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import java.util.concurrent.ConcurrentHashMap;

/**
 * This is the full table cache, where it uses concurrentHashMap internally,
 * and does not do any evict or cleanup. This full table cache need to be
 * used by tables where we want to cache the entire table with out any
 * cleanup to the cache
 * @param <CACHEKEY>
 * @param <CACHEVALUE>
 */

@Private
@Evolving
public class FullTableCache<CACHEKEY, CACHEVALUE>
    implements TableCache<CACHEKEY, CACHEVALUE> {

  private final ConcurrentHashMap<CACHEKEY, CACHEVALUE> cache;

  public FullTableCache() {
    cache = new ConcurrentHashMap<>();
  }

  @Override
  public CACHEVALUE get(CACHEKEY cacheKey) {
    return cache.get(cacheKey);
  }


  @Override
  public void put(CACHEKEY cacheKey, CACHEVALUE value) {
    cache.put(cacheKey, value);
  }

  @Override
  public void cleanup(long epoch) {
    // Do nothing
  }

  @Override
  public int size() {
    return cache.size();
  }
}
