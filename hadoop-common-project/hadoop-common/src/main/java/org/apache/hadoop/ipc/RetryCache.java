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
package org.apache.hadoop.ipc;


import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.metrics.RetryCacheMetrics;
import org.apache.hadoop.util.LightWeightCache;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.LightWeightGSet.LinkedElement;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains a cache of non-idempotent requests that have been successfully
 * processed by the RPC server implementation, to handle the retries. A request
 * is uniquely identified by the unique client ID + call ID of the RPC request.
 * On receiving retried request, an entry will be found in the
 * {@link RetryCache} and the previous response is sent back to the request.
 * <p>
 * To look an implementation using this cache, see HDFS FSNamesystem class.
 */
@InterfaceAudience.Private
public class RetryCache {
  public static final Logger LOG = LoggerFactory.getLogger(RetryCache.class);
  private final RetryCacheMetrics retryCacheMetrics;
  private static final int MAX_CAPACITY = 16;

  /**
   * CacheEntry is tracked using unique client ID and callId of the RPC request
   */
  public static class CacheEntry implements LightWeightCache.Entry {
    /**
     * Processing state of the requests
     */
    private static byte INPROGRESS = 0;
    private static byte SUCCESS = 1;
    private static byte FAILED = 2;

    private byte state = INPROGRESS;
    
    // Store uuid as two long for better memory utilization
    private final long clientIdMsb; // Most signficant bytes
    private final long clientIdLsb; // Least significant bytes
    
    private final int callId;
    private final long expirationTime;
    private LightWeightGSet.LinkedElement next;

    CacheEntry(byte[] clientId, int callId, long expirationTime) {
      // ClientId must be a UUID - that is 16 octets.
      Preconditions.checkArgument(clientId.length == ClientId.BYTE_LENGTH,
          "Invalid clientId - length is " + clientId.length
              + " expected length " + ClientId.BYTE_LENGTH);
      // Convert UUID bytes to two longs
      clientIdMsb = ClientId.getMsb(clientId);
      clientIdLsb = ClientId.getLsb(clientId);
      this.callId = callId;
      this.expirationTime = expirationTime;
    }

    CacheEntry(byte[] clientId, int callId, long expirationTime,
        boolean success) {
      this(clientId, callId, expirationTime);
      this.state = success ? SUCCESS : FAILED;
    }

    private static int hashCode(long value) {
      return (int)(value ^ (value >>> 32));
    }
    
    @Override
    public int hashCode() {
      return (hashCode(clientIdMsb) * 31 + hashCode(clientIdLsb)) * 31 + callId;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof CacheEntry)) {
        return false;
      }
      CacheEntry other = (CacheEntry) obj;
      return callId == other.callId && clientIdMsb == other.clientIdMsb
          && clientIdLsb == other.clientIdLsb;
    }

    @Override
    public void setNext(LinkedElement next) {
      this.next = next;
    }

    @Override
    public LinkedElement getNext() {
      return next;
    }

    synchronized void completed(boolean success) {
      state = success ? SUCCESS : FAILED;
      this.notifyAll();
    }

    public synchronized boolean isSuccess() {
      return state == SUCCESS;
    }

    @Override
    public void setExpirationTime(long timeNano) {
      // expiration time does not change
    }

    @Override
    public long getExpirationTime() {
      return expirationTime;
    }
    
    @Override
    public String toString() {
      return (new UUID(this.clientIdMsb, this.clientIdLsb)).toString() + ":"
          + this.callId + ":" + this.state;
    }
  }

  /**
   * CacheEntry with payload that tracks the previous response or parts of
   * previous response to be used for generating response for retried requests.
   */
  public static class CacheEntryWithPayload extends CacheEntry {
    private Object payload;

    CacheEntryWithPayload(byte[] clientId, int callId, Object payload,
        long expirationTime) {
      super(clientId, callId, expirationTime);
      this.payload = payload;
    }

    CacheEntryWithPayload(byte[] clientId, int callId, Object payload,
        long expirationTime, boolean success) {
     super(clientId, callId, expirationTime, success);
     this.payload = payload;
   }

    /** Override equals to avoid findbugs warnings */
    @Override
    public boolean equals(Object obj) {
      return super.equals(obj);
    }

    /** Override hashcode to avoid findbugs warnings */
    @Override
    public int hashCode() {
      return super.hashCode();
    }

    public Object getPayload() {
      return payload;
    }
  }

  private final LightWeightGSet<CacheEntry, CacheEntry> set;
  private final long expirationTime;
  private String cacheName;

  private final ReentrantLock lock = new ReentrantLock();

  /**
   * Constructor
   * @param cacheName name to identify the cache by
   * @param percentage percentage of total java heap space used by this cache
   * @param expirationTime time for an entry to expire in nanoseconds
   */
  public RetryCache(String cacheName, double percentage, long expirationTime) {
    int capacity = LightWeightGSet.computeCapacity(percentage, cacheName);
    capacity = capacity > MAX_CAPACITY ? capacity : MAX_CAPACITY;
    this.set = new LightWeightCache<CacheEntry, CacheEntry>(capacity, capacity,
        expirationTime, 0);
    this.expirationTime = expirationTime;
    this.cacheName = cacheName;
    this.retryCacheMetrics =  RetryCacheMetrics.create(this);
  }

  private static boolean skipRetryCache() {
    // Do not track non RPC invocation or RPC requests with
    // invalid callId or clientId in retry cache
    return !Server.isRpcInvocation() || Server.getCallId() < 0
        || Arrays.equals(Server.getClientId(), RpcConstants.DUMMY_CLIENT_ID);
  }

  public void lock() {
    this.lock.lock();
  }

  public void unlock() {
    this.lock.unlock();
  }

  private void incrCacheClearedCounter() {
    retryCacheMetrics.incrCacheCleared();
  }

  @VisibleForTesting
  public LightWeightGSet<CacheEntry, CacheEntry> getCacheSet() {
    return set;
  }

  @VisibleForTesting
  public RetryCacheMetrics getMetricsForTests() {
    return retryCacheMetrics;
  }

  /**
   * This method returns cache name for metrics.
   */
  public String getCacheName() {
    return cacheName;
  }

  /**
   * This method handles the following conditions:
   * <ul>
   * <li>If retry is not to be processed, return null</li>
   * <li>If there is no cache entry, add a new entry {@code newEntry} and return
   * it.</li>
   * <li>If there is an existing entry, wait for its completion. If the
   * completion state is {@link CacheEntry#FAILED}, the expectation is that the
   * thread that waited for completion, retries the request. the
   * {@link CacheEntry} state is set to {@link CacheEntry#INPROGRESS} again.
   * <li>If the completion state is {@link CacheEntry#SUCCESS}, the entry is
   * returned so that the thread that waits for it can can return previous
   * response.</li>
   * <ul>
   * 
   * @return {@link CacheEntry}.
   */
  private CacheEntry waitForCompletion(CacheEntry newEntry) {
    CacheEntry mapEntry = null;
    lock.lock();
    try {
      mapEntry = set.get(newEntry);
      // If an entry in the cache does not exist, add a new one
      if (mapEntry == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Adding Rpc request clientId "
              + newEntry.clientIdMsb + newEntry.clientIdLsb + " callId "
              + newEntry.callId + " to retryCache");
        }
        set.put(newEntry);
        retryCacheMetrics.incrCacheUpdated();
        return newEntry;
      } else {
        retryCacheMetrics.incrCacheHit();
      }
    } finally {
      lock.unlock();
    }
    // Entry already exists in cache. Wait for completion and return its state
    Preconditions.checkNotNull(mapEntry,
        "Entry from the cache should not be null");
    // Wait for in progress request to complete
    synchronized (mapEntry) {
      while (mapEntry.state == CacheEntry.INPROGRESS) {
        try {
          mapEntry.wait();
        } catch (InterruptedException ie) {
          // Restore the interrupted status
          Thread.currentThread().interrupt();
        }
      }
      // Previous request has failed, the expectation is that it will be
      // retried again.
      if (mapEntry.state != CacheEntry.SUCCESS) {
        mapEntry.state = CacheEntry.INPROGRESS;
      }
    }
    return mapEntry;
  }
  
  /** 
   * Add a new cache entry into the retry cache. The cache entry consists of 
   * clientId and callId extracted from editlog.
   */
  public void addCacheEntry(byte[] clientId, int callId) {
    CacheEntry newEntry = new CacheEntry(clientId, callId, System.nanoTime()
        + expirationTime, true);
    lock.lock();
    try {
      set.put(newEntry);
    } finally {
      lock.unlock();
    }
    retryCacheMetrics.incrCacheUpdated();
  }
  
  public void addCacheEntryWithPayload(byte[] clientId, int callId,
      Object payload) {
    // since the entry is loaded from editlog, we can assume it succeeded.    
    CacheEntry newEntry = new CacheEntryWithPayload(clientId, callId, payload,
        System.nanoTime() + expirationTime, true);
    lock.lock();
    try {
      set.put(newEntry);
    } finally {
      lock.unlock();
    }
    retryCacheMetrics.incrCacheUpdated();
  }

  private static CacheEntry newEntry(long expirationTime) {
    return new CacheEntry(Server.getClientId(), Server.getCallId(),
        System.nanoTime() + expirationTime);
  }

  private static CacheEntryWithPayload newEntry(Object payload,
      long expirationTime) {
    return new CacheEntryWithPayload(Server.getClientId(), Server.getCallId(),
        payload, System.nanoTime() + expirationTime);
  }

  /** Static method that provides null check for retryCache */
  public static CacheEntry waitForCompletion(RetryCache cache) {
    if (skipRetryCache()) {
      return null;
    }
    return cache != null ? cache
        .waitForCompletion(newEntry(cache.expirationTime)) : null;
  }

  /** Static method that provides null check for retryCache */
  public static CacheEntryWithPayload waitForCompletion(RetryCache cache,
      Object payload) {
    if (skipRetryCache()) {
      return null;
    }
    return (CacheEntryWithPayload) (cache != null ? cache
        .waitForCompletion(newEntry(payload, cache.expirationTime)) : null);
  }

  public static void setState(CacheEntry e, boolean success) {
    if (e == null) {
      return;
    }
    e.completed(success);
  }

  public static void setState(CacheEntryWithPayload e, boolean success,
      Object payload) {
    if (e == null) {
      return;
    }
    e.payload = payload;
    e.completed(success);
  }

  public static void clear(RetryCache cache) {
    if (cache != null) {
      cache.set.clear();
      cache.incrCacheClearedCounter();
    }
  }
}
