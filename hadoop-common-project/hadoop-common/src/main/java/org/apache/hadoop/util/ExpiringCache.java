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

package org.apache.hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A thread-safe cache with automatic expiration and cleanup.
 */
public class ExpiringCache <K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(ExpiringCache.class);
  public static class CacheEntry<V> {
    V value;
    long timestamp;

    CacheEntry(V value, long timestamp) {
      this.value = value;
      this.timestamp = timestamp;
    }

    public V getValue() {
      return value;
    }

    @Override
    public String toString() {
      return value == null ? "null" : value.toString();
    }
  }

  public interface Loader<K, V> {
    V load(K key) throws Exception;
  }

  private final Map<K, CacheEntry<V>> cache = new ConcurrentHashMap<>();
  private final long expiryIntervalMs;
  private final Timer cleanupTimer;
  private final Clock clock;
  private final Loader<K, V> loader;
  public ExpiringCache(String name, Clock clock, long expiryIntervalSecs, Loader<K, V> loader) {
    this.expiryIntervalMs = expiryIntervalSecs * 1000;
    this.clock = clock;
    this.cleanupTimer = new Timer(name + "-ExpiringCache-Cleanup-Timer", true);
    this.loader = loader;
    startCleanupTask();
  }


  public V get(K key) throws Exception {
    CacheEntry<V> entry = cache.get(key);
    if (entry != null) {
      return entry.value;
    }
    V value = loader.load(key);
    cache.put(key, new CacheEntry<>(value, clock.getTime()));
    return value;
  }

  public void remove(K key) {
    cache.remove(key);
  }

  private boolean isExpired(CacheEntry<V> entry) {
    return clock.getTime() > entry.timestamp + expiryIntervalMs;
  }

  private void startCleanupTask() {
    cleanupTimer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        cache.entrySet().removeIf(entry -> {
          boolean expired = isExpired(entry.getValue());
          if (expired && LOG.isDebugEnabled()) {
            LOG.debug("[{}:{}] Expired after {} secs", entry.getKey(),
                entry.getValue(), expiryIntervalMs / 1000);
          }
          return expired;
        });
      }
    }, expiryIntervalMs / 3, expiryIntervalMs / 3);
  }

  public void stopCleanup() {
    cleanupTimer.cancel();
  }
}
