/*
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

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience;

import static java.util.Objects.requireNonNull;

/**
 * A map of keys type K to objects of type V which uses weak references,
 * so does lot leak memory through long-lived references
 * <i>at the expense of losing references when GC takes place.</i>.
 *
 * This class is intended be used instead of ThreadLocal storage when
 * references are to be cleaned up when the instance holding.
 * In this use case, the key is the Long key.
 *
 * Concurrency.
 * The class assumes that map entries are rarely contended for when writing,
 * and that not blocking other threads is more important than atomicity.
 * - a ConcurrentHashMap is used to map keys to weak references, with
 *   all its guarantees.
 * - there is no automatic pruning.
 * - see {@link #create(Object)} for the concurrency semantics on entry creation.
 */
@InterfaceAudience.Private
public class WeakReferenceMap<K, V> {

  /**
   * The reference map.
   */
  private final Map<K, WeakReference<V>> map = new ConcurrentHashMap<>();

  /**
   * Supplier of new instances.
   */
  private final Function<? super K, ? extends V> factory;

  /**
   * Nullable callback when a get on a key got a weak reference back.
   * The assumption is that this is for logging/stats, which is why
   * no attempt is made to use the call as a supplier of a new value.
   */
  private final Consumer<? super K> referenceLost;

  /**
   * Counter of references lost.
   */
  private final AtomicLong referenceLostCount = new AtomicLong();

  /**
   * Counter of entries created.
   */
  private final AtomicLong entriesCreatedCount = new AtomicLong();

  /**
   * instantiate.
   * @param factory supplier of new instances
   * @param referenceLost optional callback on lost references.
   */
  public WeakReferenceMap(
      Function<? super K, ? extends V> factory,
      @Nullable final Consumer<? super K> referenceLost) {

    this.factory = requireNonNull(factory);
    this.referenceLost = referenceLost;
  }

  @Override
  public String toString() {
    return "WeakReferenceMap{" +
        "size=" + size() +
        ", referenceLostCount=" + referenceLostCount +
        ", entriesCreatedCount=" + entriesCreatedCount +
        '}';
  }

  /**
   * Map size.
   * @return the current map size.
   */
  public int size() {
    return map.size();
  }

  /**
   * Clear all entries.
   */
  public void clear() {
    map.clear();
  }

  /**
   * look up the value, returning the possibly empty weak reference
   * to a value, or null if no value was found.
   * @param key key to look up
   * @return null if there is no entry, a weak reference if found
   */
  public WeakReference<V> lookup(K key) {
    return map.get(key);
  }

  /**
   * Get the value, creating if needed.
   * @param key key.
   * @return an instance.
   */
  public V get(K key) {
    final WeakReference<V> current = lookup(key);
    V val = resolve(current);
    if (val != null) {
      // all good.
      return  val;
    }

    // here, either no ref, or the value is null
    if (current != null) {
      noteLost(key);
    }
    return create(key);
  }

  /**
   * Create a new instance under a key.
   * The instance is created, added to the map and then the
   * map value retrieved.
   * This ensures that the reference returned is that in the map,
   * even if there is more than one entry being created at the same time.
   * @param key key
   * @return the value
   */
  public V create(K key) {
    entriesCreatedCount.incrementAndGet();
    WeakReference<V> newRef = new WeakReference<>(
        requireNonNull(factory.apply(key)));
    map.put(key, newRef);
    return map.get(key).get();
  }

  /**
   * Put a value under the key.
   * A null value can be put, though on a get() call
   * a new entry is generated
   *
   * @param key key
   * @param value value
   * @return any old non-null reference.
   */
  public V put(K key, V value) {
    return resolve(map.put(key, new WeakReference<>(value)));
  }

  /**
   * Remove any value under the key.
   * @param key key
   * @return any old non-null reference.
   */
  public V remove(K key) {
    return resolve(map.remove(key));
  }

  /**
   * Does the map have a valid reference for this object?
   * no-side effects: there's no attempt to notify or cleanup
   * if the reference is null.
   * @param key key to look up
   * @return true if there is a valid reference.
   */
  public boolean containsKey(K key) {
    final WeakReference<V> current = lookup(key);
    return resolve(current) != null;
  }

  /**
   * Given a possibly null weak reference, resolve
   * its value.
   * @param r reference to resolve
   * @return the value or null
   */
  private V resolve(WeakReference<V> r) {
    return r == null ? null : r.get();
  }

  /**
   * Prune all null weak references, calling the referenceLost
   * callback for each one.
   *
   * non-atomic and non-blocking.
   * @return the number of entries pruned.
   */
  public int prune() {
    int count = 0;
    final Iterator<Map.Entry<K, WeakReference<V>>> it = map.entrySet().iterator();
    while (it.hasNext()) {
      final Map.Entry<K, WeakReference<V>> next = it.next();
      if (next.getValue().get() == null) {
        it.remove();
        count++;
        noteLost(next.getKey());
      }
    }
    return count;
  }

  /**
   * Notify the reference lost callback.
   * @param key key of lost reference
   */
  private void noteLost(final K key) {
    // incrment local counter
    referenceLostCount.incrementAndGet();

    // and call any notification function supplied in the constructor
    if (referenceLost != null) {
      referenceLost.accept(key);
    }
  }

  /**
   * Get count of references lost as detected
   * during prune() or get() calls.
   * @return count of references lost
   */
  public final long getReferenceLostCount() {
    return referenceLostCount.get();
  }

  /**
   * Get count of entries created on demand.
   * @return count of entries created
   */
  public final long getEntriesCreatedCount() {
    return entriesCreatedCount.get();
  }
}
