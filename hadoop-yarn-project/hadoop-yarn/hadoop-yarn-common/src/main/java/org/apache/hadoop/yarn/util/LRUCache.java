/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.util;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Time;

import java.util.Map;

public class LRUCache<K, V> {

  private final long expireTimeMs;
  private final Map<K, CacheNode<V>> cache;

  public LRUCache(int capacity) {
    this(capacity, -1);
  }

  public LRUCache(int capacity, long expireTimeMs) {
    cache = new LRUCacheHashMap<>(capacity, true);
    this.expireTimeMs = expireTimeMs;
  }

  public synchronized V get(K key) {
    CacheNode<V> cacheNode = cache.get(key);
    if (cacheNode != null) {
      if (expireTimeMs > 0 && Time.now() > cacheNode.getCacheTime() + expireTimeMs) {
        cache.remove(key);
        return null;
      }
    }
    return cacheNode == null ? null : cacheNode.get();
  }

  public synchronized V put(K key, V value) {
    cache.put(key, new CacheNode<>(value));
    return value;
  }

  @VisibleForTesting
  public void clear(){
    cache.clear();
  }

  public int size() {
    return cache.size();
  }
}