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
package org.apache.hadoop.hdfs.server.namenode.fgl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor;

/**
 * A lock manager that supports various lock operations for a key class {@link K}. These operations
 * include (but are not limited to) acquisition, release, leak check, emptying and closure.
 *
 * @param <K> key for the lock manager
 */
public class LockPoolManager<K> {
  private final Logger LOG = LoggerFactory.getLogger(LockPoolManager.class);

  // All locks, both cached and uncached, are stored here in this universal lock pool
  private final ConcurrentHashMap<K, LockResource<K>> locks;
  // Cached locks are stored here
  private volatile ConcurrentHashMap<K, LockResource<K>> cachedLocks = new ConcurrentHashMap<>();
  // Keeps track of lock usage for periodic promotion into cache
  private PromotionMonitor promotionMonitor = null;
  private PromotionService promotionService = null;
  private final LockTraceManager lockTraceManager;
  private final boolean openLockTrace;

  public LockPoolManager(int initialCapacity, int cacheCapacity, boolean openLockTrace) {
    this.locks = new ConcurrentHashMap<>(initialCapacity);
    this.openLockTrace = openLockTrace;
    this.lockTraceManager = new LockTraceManager();
    if (cacheCapacity > 0) {
      LOG.info("Automatic cache promotion enabled, capacity={}", cacheCapacity);
      this.promotionMonitor = new PromotionMonitor();
      this.promotionService = new PromotionService(cacheCapacity);
      ScheduledExecutorService service = new HadoopScheduledThreadPoolExecutor(1);
      service.scheduleAtFixedRate(promotionService, 1, 1, TimeUnit.HOURS);
    }
    LOG.info(
        "Initializing LockPoolManager with initialCapacity={}, cacheCapacity={}, openLockTrace={}.",
        initialCapacity, cacheCapacity, openLockTrace);
  }

  /**
   * Attempts to acquire a lock for the given lock key then cache it in
   * {@link LockPoolManager#cachedLocks} if the lock is not cached.
   * To be used for smaller pools that cache all locks and don't rely on automatic promotion,
   * like the DN lock pool.
   */
  public AutoCloseableLockInPool<K> acquireAndCacheLock(K key, LockMode mode) {
    return acquireLockInternal(key, mode, true);
  }

  /**
   * Attempts to acquire a lock for the given lock key and stores in {@link LockPoolManager#locks}.
   */
  public AutoCloseableLockInPool<K> acquireLock(K key, LockMode mode) {
    return acquireLockInternal(key, mode, false);
  }

  /**
   * Acquires an {@link AutoCloseableLockInPool} for the given lock key.
   */
  private AutoCloseableLockInPool<K> acquireLockInternal(K key, LockMode mode, boolean doesCache) {
    if (key == null) {
      throw new IllegalArgumentException("The key shouldn't be null");
    }
    LockResource<K> lock = acquireLockResource(key, doesCache);

    // AutoCloseableLockInPool objects are not directly stored
    // because there are some cases that the lock mode will be changed.
    AutoCloseableLockInPool<K> autoCloseableLockInPool =
        new AutoCloseableLockInPool<>(this, lock, mode);

    if (openLockTrace) {
      lockTraceManager.putThreadName();
    }
    return autoCloseableLockInPool;
  }

  /**
   * Attempts to acquire a {@link LockResource}. Tries from {@link LockPoolManager#cachedLocks}
   * first, then falls through to {@link LockPoolManager#locks} if not found in cache.
   * Just increments ref count if found in cache. If no lock found, creates a new lock with
   * reference count 1 and stores it in {@link LockPoolManager#locks}. If cache is required,
   * increments the reference count again to prevent the cached lock from being evicted, then stores
   * in {@link LockPoolManager#cachedLocks}. A cached lock is stored in both maps.
   *
   * @param key       lock key to acquire a lock for
   * @param doesCache stores the lock in cache if true
   * @return the lock
   */
  private LockResource<K> acquireLockResource(K key, boolean doesCache) {
    LockResource<K> lock = getFromCache(key);
    if (lock != null) {
      lock.ref.incrementAndGet();
    } else {
      // Ref count is already incremented by getFromPool
      lock = getFromPool(key);
      if (doesCache) {
        // Bump the ref count for a cached lock
        lock.getRef().incrementAndGet();
        cachedLocks.put(key, lock);
      }
    }
    // Sample operations instead of counting everything, can bump up the rate for higher accuracy
    if (promotionMonitor != null && ThreadLocalRandom.current().nextDouble() < 0.1) {
      promotionMonitor.queueKey(key);
    }
    return lock;
  }

  /**
   * Attempts to get a lock for the given lock key from the cache.
   *
   * @param key lock key to get from the cache pool
   */
  private LockResource<K> getFromCache(K key) {
    ConcurrentHashMap<K, LockResource<K>> cachedLocksRef = cachedLocks;
    LockResource<K> lockResource = cachedLocksRef == null ? null : cachedLocksRef.get(key);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cached lock={},key={},hash={}.", lockResource, key, key.hashCode());
    }
    return lockResource;
  }

  /**
   * Attempts to get a lock for the given lock key from the universal pool.
   * If hit, increments the ref count. If missed, creates a new lock with ref count 1.
   *
   * @param key lock key to get from the universal pool
   */
  private LockResource<K> getFromPool(K key) {
    return locks.compute(key,
        (k, v) -> v != null && v.getRef().incrementAndGet() > 1 ? v : new LockResource<>(key));
  }

  /**
   * Releases the lock for the given lock key.
   * If cached, just decrements the ref count. Else, attempts to release the lock from
   * the universal lock pool.
   *
   * @param key lock key to release
   */
  public void releaseLockResource(K key) {
    LockResource<K> persistentLock = getFromCache(key);
    if (persistentLock != null) {
      persistentLock.ref.decrementAndGet();
    } else {
      releaseFromPool(key);
    }
  }

  /**
   * Attempts to release a lock for the given lock key from the universal lock pool.
   * Decrements the ref count if lock found, removes from the lock pool if ref count reaches 0.
   */
  private void releaseFromPool(K key) {
    locks.compute(key, (k, v) -> {
      if (v == null) {
        LOG.warn("Lock not found, key={}.", key);
        return null;
      } else {
        int refValue = v.getRef().decrementAndGet();
        if (refValue < 0) {
          LOG.warn("Negative ref count lock key={}, hash={}.", key, key.hashCode(),
              new Throwable());
          return null;
        } else if (refValue == 0) {
          return null;
        } else {
          return v;
        }
      }
    });
  }

  public void releaseLockResourceAndRemoveFromLockTrace(LockResource<K> lockResource) {
    releaseLockResource(lockResource.getLockKey());
    if (openLockTrace) {
      lockTraceManager.removeThreadName();
    }
  }

  public void close() {
    locks.clear();
    cachedLocks.clear();
    checkForLockLeak();
  }

  /**
   * Checks for leak.
   */
  private void checkForLockLeak() {
    if (!openLockTrace) {
      LOG.warn("Lock trace disabled.");
      return;
    }
    lockTraceManager.checkForLeak();
  }

  public enum LockMode {
    READ, WRITE
  }

  public void emptyCache() {
    cachedLocks.forEach((k, v) -> releaseFromPool(k));
    cachedLocks.clear();
  }

  /**
   * Caches a list of locks for the given keys.
   * Locks that are not already in cache are fetched from (or created anew in) the universal pool.
   * Existing locks in cache are reused.
   * {@link LockPoolManager#releaseFromPool} is called for locks that were in the old cache set
   * but no longer in the new cache set due to the extra ref stored exclusively by cached locks
   * (see {@link LockPoolManager#acquireLockResource}).
   * @param keys lock keys to store in cache
   */
  public synchronized void cacheLocks(List<K> keys) {
    ConcurrentHashMap<K, LockResource<K>> cachedLocksRef = cachedLocks;
    ConcurrentHashMap<K, LockResource<K>> newCachedLocks = new ConcurrentHashMap<>();
    for (K k : keys) {
      LockResource<K> lock = cachedLocksRef == null ? null : cachedLocksRef.get(k);
      if (lock == null) {
        lock = getFromPool(k);
      }
      newCachedLocks.put(k, lock);
    }
    this.cachedLocks = newCachedLocks;

    // Decrement ref count or remove locks that were cached but no longer are.
    if (cachedLocksRef != null) {
      cachedLocksRef.forEach((k, v) -> {
        if (!newCachedLocks.containsKey(k)) {
          releaseFromPool(k);
        }
      });
    }
  }

  /**
   * A class that keeps track of lock usage. Used by {@link PromotionService} to promote
   * the most active locks to cache.
   */
  private class PromotionMonitor {
    volatile int mapIdx;
    ConcurrentMap<K, Integer>[] maps;
    ConcurrentMap<K, Integer> mapRef;

    @SuppressWarnings("unchecked")
    PromotionMonitor() {
      maps = new ConcurrentMap[2];
      maps[0] = new ConcurrentHashMap<>();
      maps[1] = new ConcurrentHashMap<>();

      mapIdx = 0;
      mapRef = maps[mapIdx];
    }

    void queueKey(K key) {
      maps[mapIdx].compute(key, (k, v) -> v == null ? 1 : v + 1);
    }

    synchronized Map<K, Integer> fullyConsumeAndSwapQueue() throws InterruptedException {
      mapIdx = 1 - mapIdx;
      ConcurrentMap<K, Integer> oldRef = maps[1 - mapIdx];
      Map<K, Integer> clone = new HashMap<>(oldRef);
      oldRef.clear();
      return clone;
    }
  }

  /**
   * A class that periodically scans the lock pool and replaces the cache every hour
   * with the most active locks.
   */
  class PromotionService implements Runnable {
    final int cacheCapacity;

    public PromotionService(int cacheCapacity) {
      this.cacheCapacity = cacheCapacity;
    }

    @Override
    public void run() {
      assert promotionMonitor != null;
      try {
        LOG.info("Promoting locks...");
        Map<K, Integer> counts = promotionMonitor.fullyConsumeAndSwapQueue();
        List<Map.Entry<K, Integer>> entries = new LinkedList<>(counts.entrySet());
        entries.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        entries = entries.subList(0, cacheCapacity);
        cacheLocks(entries.stream().map(Map.Entry::getKey).collect(Collectors.toList()));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @VisibleForTesting
  public void promoteLocks() {
    if (promotionService != null) {
      promotionService.run();
    }
  }

  @VisibleForTesting
  public int getPoolSize() {
    return locks.size();
  }

  @VisibleForTesting
  public List<K> getCachedKeys() {
    return new ArrayList<>(cachedLocks.keySet());
  }
}
