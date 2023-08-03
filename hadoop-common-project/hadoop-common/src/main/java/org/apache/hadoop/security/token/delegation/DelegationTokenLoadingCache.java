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

package org.apache.hadoop.security.token.delegation;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;


/**
 * Cache for delegation tokens that can handle high volume of tokens. A
 * loading cache will prevent all active tokens from being in memory at the
 * same time. It will also trigger more requests from the persistent token storage.
 */
public class DelegationTokenLoadingCache<K, V> implements Map<K, V> {
  private LoadingCache<K, V> internalLoadingCache;

  public DelegationTokenLoadingCache(long cacheExpirationMs, long maximumCacheSize,
      Function<K, V> singleEntryFunction) {
    this.internalLoadingCache = CacheBuilder.newBuilder()
        .expireAfterWrite(cacheExpirationMs, TimeUnit.MILLISECONDS)
        .maximumSize(maximumCacheSize)
        .build(new CacheLoader<K, V>() {
          @Override
          public V load(K k) throws Exception {
            return singleEntryFunction.apply(k);
          }
        });
  }

  @Override
  public int size() {
    return (int) this.internalLoadingCache.size();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    return this.internalLoadingCache.getIfPresent(key) != null;
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V get(Object key) {
    try {
      return this.internalLoadingCache.get((K) key);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public V put(K key, V value) {
    this.internalLoadingCache.put(key, value);
    return this.internalLoadingCache.getIfPresent(key);
  }

  @Override
  public V remove(Object key) {
    V value = this.internalLoadingCache.getIfPresent(key);
    this.internalLoadingCache.invalidate(key);
    return value;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    this.internalLoadingCache.putAll(m);
  }

  @Override
  public void clear() {
    this.internalLoadingCache.invalidateAll();
  }

  @Override
  public Set<K> keySet() {
    return this.internalLoadingCache.asMap().keySet();
  }

  @Override
  public Collection<V> values() {
    return this.internalLoadingCache.asMap().values();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return this.internalLoadingCache.asMap().entrySet();
  }
}
