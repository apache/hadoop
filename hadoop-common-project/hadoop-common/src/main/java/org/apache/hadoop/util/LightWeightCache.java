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

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A low memory footprint Cache which extends {@link LightWeightGSet}.
 * An entry in the cache is expired if
 * (1) it is added to the cache longer than the creation-expiration period, and
 * (2) it is not accessed for the access-expiration period.
 * When an entry is expired, it may be evicted from the cache.
 * When the size limit of the cache is set, the cache will evict the entries
 * with earliest expiration time, even if they are not expired.
 * 
 * It is guaranteed that number of entries in the cache is less than or equal
 * to the size limit.  However, It is not guaranteed that expired entries are
 * evicted from the cache. An expired entry may possibly be accessed after its
 * expiration time. In such case, the expiration time may be updated.
 *
 * This class does not support null entry.
 *
 * This class is not thread safe.
 *
 * @param <K> Key type for looking up the entries
 * @param <E> Entry type, which must be
 *       (1) a subclass of K, and
 *       (2) implementing {@link Entry} interface, and
 */
@InterfaceAudience.Private
public class LightWeightCache<K, E extends K> extends LightWeightGSet<K, E> {
  /** Limit the number of entries in each eviction. */
  private static final int EVICTION_LIMIT = 1 << 16;

  /**
   * Entries of {@link LightWeightCache}.
   */
  public static interface Entry extends LinkedElement {
    /** Set the expiration time. */
    public void setExpirationTime(long timeNano);

    /** Get the expiration time. */
    public long getExpirationTime();
  }

  /** Comparator for sorting entries by expiration time in ascending order. */
  private static final Comparator<Entry> expirationTimeComparator
      = new Comparator<Entry>() {
    @Override
    public int compare(Entry left, Entry right) {
      final long l = left.getExpirationTime();
      final long r = right.getExpirationTime();
      return l > r? 1: l < r? -1: 0;
    }
  };
  
  private static int updateRecommendedLength(int recommendedLength,
      int sizeLimit) {
    return sizeLimit > 0 && sizeLimit < recommendedLength?
        (sizeLimit/4*3) // 0.75 load factor
        : recommendedLength;
  }

  /*
   * The memory footprint for java.util.PriorityQueue is low but the
   * remove(Object) method runs in linear time. We may improve it by using a
   * balanced tree. However, we do not yet have a low memory footprint balanced
   * tree implementation.
   */
  private final PriorityQueue<Entry> queue;
  private final long creationExpirationPeriod;
  private final long accessExpirationPeriod;
  private final int sizeLimit;
  private final Timer timer;

  /**
   * @param recommendedLength Recommended size of the internal array.
   * @param sizeLimit the limit of the size of the cache.
   *            The limit is disabled if it is <= 0.
   * @param creationExpirationPeriod the time period C > 0 in nanoseconds that
   *            the creation of an entry is expired if it is added to the cache
   *            longer than C.
   * @param accessExpirationPeriod the time period A >= 0 in nanoseconds that
   *            the access of an entry is expired if it is not accessed
   *            longer than A. 
   */
  public LightWeightCache(final int recommendedLength,
      final int sizeLimit,
      final long creationExpirationPeriod,
      final long accessExpirationPeriod) {
    this(recommendedLength, sizeLimit,
        creationExpirationPeriod, accessExpirationPeriod, new Timer());
  }

  @VisibleForTesting
  LightWeightCache(final int recommendedLength,
      final int sizeLimit,
      final long creationExpirationPeriod,
      final long accessExpirationPeriod,
      final Timer timer) {
    super(updateRecommendedLength(recommendedLength, sizeLimit));

    this.sizeLimit = sizeLimit;

    if (creationExpirationPeriod <= 0) {
      throw new IllegalArgumentException("creationExpirationPeriod = "
          + creationExpirationPeriod + " <= 0");
    }
    this.creationExpirationPeriod = creationExpirationPeriod;

    if (accessExpirationPeriod < 0) {
      throw new IllegalArgumentException("accessExpirationPeriod = "
          + accessExpirationPeriod + " < 0");
    }
    this.accessExpirationPeriod = accessExpirationPeriod;

    this.queue = new PriorityQueue<Entry>(
        sizeLimit > 0? sizeLimit + 1: 1 << 10, expirationTimeComparator);
    this.timer = timer;
  }

  void setExpirationTime(final Entry e, final long expirationPeriod) {
    e.setExpirationTime(timer.monotonicNowNanos() + expirationPeriod);
  }

  boolean isExpired(final Entry e, final long now) {
    return now > e.getExpirationTime();
  }

  private E evict() {
    @SuppressWarnings("unchecked")
    final E polled = (E)queue.poll();
    final E removed = super.remove(polled);
    Preconditions.checkState(removed == polled);
    return polled;
  }

  /** Evict expired entries. */
  private void evictExpiredEntries() {
    final long now = timer.monotonicNowNanos();
    for(int i = 0; i < EVICTION_LIMIT; i++) {
      final Entry peeked = queue.peek();
      if (peeked == null || !isExpired(peeked, now)) {
        return;
      }

      final E evicted = evict();
      Preconditions.checkState(evicted == peeked);
    }
  }

  /** Evict entries in order to enforce the size limit of the cache. */
  private void evictEntries() {
    if (sizeLimit > 0) {
      for(int i = size(); i > sizeLimit; i--) {
        evict();
      }
    }
  }
  
  @Override
  public E get(K key) {
    final E entry = super.get(key);
    if (entry != null) {
      if (accessExpirationPeriod > 0) {
        // update expiration time
        final Entry existing = (Entry)entry;
        Preconditions.checkState(queue.remove(existing));
        setExpirationTime(existing, accessExpirationPeriod);
        queue.offer(existing);
      }
    }
    return entry;
  }

  @Override
  public E put(final E entry) {
    if (!(entry instanceof Entry)) {
      throw new HadoopIllegalArgumentException(
          "!(entry instanceof Entry), entry.getClass()=" + entry.getClass());
    }

    evictExpiredEntries();

    final E existing = super.put(entry);
    if (existing != null) {
      queue.remove(existing);
    }

    final Entry e = (Entry)entry;
    setExpirationTime(e, creationExpirationPeriod);
    queue.offer(e);
    
    evictEntries();
    return existing;
  }

  @Override
  public E remove(K key) {
    evictExpiredEntries();

    final E removed = super.remove(key);
    if (removed != null) {
      Preconditions.checkState(queue.remove(removed));
    }
    return removed;
  }

  @Override
  public Iterator<E> iterator() {
    final Iterator<E> iter = super.iterator();
    return new Iterator<E>() {
      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public E next() {
        return iter.next();
      }

      @Override
      public void remove() {
        // It would be tricky to support this because LightWeightCache#remove
        // may evict multiple elements via evictExpiredEntries.
        throw new UnsupportedOperationException("Remove via iterator is " +
            "not supported for LightWeightCache");
      }
    };
  }
}
