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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches frequently used names to facilitate reuse.
 * (example: byte[] representation of the file name in {@link INode}).
 * 
 * This class is used by initially adding all the file names. Cache
 * tracks the number of times a name is used in a transient map. It promotes 
 * a name used more than {@code useThreshold} to the cache.
 * 
 * One all the names are added, {@link #initialized()} should be called to
 * finish initialization. The transient map where use count is tracked is
 * discarded and cache is ready for use.
 * 
 * <p>
 * This class must be synchronized externally after initialized .
 * 
 * @param <K> name to be added to the cache
 */
class NameCache<K> {
  /**
   * Class for tracking use count of a name
   */
  private class UseCount {
    private final AtomicInteger count = new AtomicInteger();
    final K value;  // Internal value for the name

    UseCount(final K value) {
      this.value = value;
    }

    int incrementAndGet() {
      return count.incrementAndGet();
    }

  }

  static final Logger LOG = LoggerFactory.getLogger(NameCache.class.getName());

  /** indicates initialization is in progress */
  private boolean initialized = false;

  /** names used more than {@code useThreshold} is added to the cache */
  private final int useThreshold;

  /** of times a cache look up was successful */
  private int lookups = 0;

  private final LongAdder lookupsBeforeInitialized = new LongAdder();

  /** Cached names */
  final HashMap<K, K> cache = new HashMap<K, K>();

  /** Names and with number of occurrences tracked during initialization */
  Map<K, UseCount> transientMap = new ConcurrentHashMap<>();

  /**
   * Constructor
   * @param useThreshold names occurring more than this is promoted to the
   *          cache
   */
  NameCache(int useThreshold) {
    Preconditions.checkArgument(useThreshold > 0);
    this.useThreshold = useThreshold;
  }
  
  /**
   * Add a given name to the cache or track use count.
   * exist. If the name already exists, then the internal value is returned.
   * If not initialized, this method is thread safe.
   * 
   * @param name name to be looked up
   * @return internal value for the name if found; otherwise null
   */
  K put(final K name) {
    if (initialized) {
      K internal = cache.get(name);
      if (internal != null) {
        lookups++;
      }
      return internal;
    } else {
      UseCount useCount = transientMap.computeIfAbsent(name, UseCount::new);
      int count = useCount.incrementAndGet();
      if (count == useThreshold) {
        promote(useCount);
      } else if (count > useThreshold) {
        lookupsBeforeInitialized.increment();
      }
      return useCount.value;
    }
  }

  /**
   * Lookup count when a lookup for a name returned cached object
   * @return number of successful lookups
   */
  int getLookupCount() {
    return lookups;
  }

  /**
   * Size of the cache
   * @return Number of names stored in the cache
   */
  int size() {
    return cache.size();
  }

  /**
   * Mark the name cache as initialized. The use count is no longer tracked
   * and the transient map used for initializing the cache is discarded to
   * save heap space.
   */
  void initialized() {
    this.lookups = lookups + lookupsBeforeInitialized.intValue();
    LOG.info("initialized with " + size() + " entries " + lookups + " lookups");
    this.initialized = true;
    this.transientMap = null;
  }
  
  /** Promote a frequently used name to the cache */
  private synchronized void promote(final UseCount useCount) {
    cache.put(useCount.value, useCount.value);
    lookups += useThreshold;
  }

  public void reset() {
    initialized = false;
    cache.clear();
    if (transientMap == null) {
      transientMap = new ConcurrentHashMap<>();
    } else {
      transientMap.clear();
    }
  }
}
