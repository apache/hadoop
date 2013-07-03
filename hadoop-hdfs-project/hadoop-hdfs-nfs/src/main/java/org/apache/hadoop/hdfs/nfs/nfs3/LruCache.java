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
package org.apache.hadoop.hdfs.nfs.nfs3;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A thread-safe LRU table.
 */
public class LruCache<K, V> {
  private final int maxSize;
  private final LinkedHashMap<K, V> map;
  private static final float hashTableLoadFactor = 0.75f;

  public LruCache(int maxSize) {
    this.maxSize = maxSize;
    int hashTableCapacity = (int) Math.ceil(maxSize / hashTableLoadFactor) + 1;
    map = new LinkedHashMap<K, V>(hashTableCapacity, hashTableLoadFactor, true) {
      private static final long serialVersionUID = 1L;

      @Override
      protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > LruCache.this.maxSize;
      }
    };
  }

  // The found entry becomes the most recently used.
  public synchronized V get(K key) {
    return map.get(key);
  }

  public synchronized void put(K key, V value) {
    map.put(key, value);
  }

  public synchronized int usedSize() {
    return map.size();
  }

  public synchronized boolean containsKey(K key) {
    return map.containsKey(key);
  }
}
