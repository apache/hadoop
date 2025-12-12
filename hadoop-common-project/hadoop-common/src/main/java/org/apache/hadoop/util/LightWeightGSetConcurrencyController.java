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

import java.util.concurrent.atomic.LongAdder;

/**
 * A concurrency controller implementation for {@link LightWeightGSet}.
 *
 * <p>This class provides fine-grained locking mechanism for concurrent access to
 * {@link LightWeightGSet} by using a fixed array of locks. Each key is mapped to
 * a specific lock based on its hash value, allowing multiple threads to operate
 * on different parts of the set simultaneously while maintaining thread safety.
 * 
 * <p>The controller uses a lock striping approach with a fixed number of locks
 * (4096 by default) to balance between memory usage and concurrency level. Keys are
 * distributed across these locks using modulo operation on the key's index.
 * 
 * <p><strong>Thread Safety Scope:</strong>
 * <ul>
 *   <li><strong>Safe:</strong> Single-key operations like {@code get()}, {@code put()}, 
 *       {@code remove()}, {@code contains()} when properly synchronized using the lock
 *       returned by {@link #getLock(Object)}</li>
 *   <li><strong>NOT Safe:</strong> Bulk operations like {@code values()}, {@code iterator()}, 
 *       or any operations that traverse the entire set. These operations 
 *       require additional synchronization mechanisms beyond this controller.
 *       The {@code size()} will also be inconsistent if multiple threads are modifying the
 *       set concurrently, call {@code correctSize(int)} to correct it. </li>
 * </ul>
 *
 * @param <K> Key type for looking up the elements
 * @param <E> Element type, which must be
 *       (1) a subclass of K, and
 *       (2) implementing {@link LightWeightGSet.LinkedElement} interface.
 * 
 * @see LightWeightGSet
 * @see GSetConcurrencyController
 */
public class LightWeightGSetConcurrencyController<K, E extends K>
    implements GSetConcurrencyController<K> {

  private static final int CONCURRENCY = 16 * 16 * 16;

  /**
   * Array of lock objects used for synchronization.
   * Each lock protects a subset of keys based on their hash values.
   */
  private final Object[] locks;
  
  /**
   * Reference to the underlying LightWeightGSet that this controller manages.
   */
  private final LightWeightGSet<K, E> lightWeightGSet;

  /**
   * 
   */
  private final LongAdder size = new LongAdder();

  /**
   * Constructs a new concurrency controller for the given LightWeightGSet.
   * 
   * @param lightWeightGSet the LightWeightGSet instance to control concurrent access for
   */
  public LightWeightGSetConcurrencyController(LightWeightGSet<K, E> lightWeightGSet) {
    this.locks = new Object[CONCURRENCY];
    this.lightWeightGSet = lightWeightGSet;
    this.size.add(lightWeightGSet.size());
    initLocks();
  }

  private void initLocks() {
    for (int i = 0; i < this.locks.length; i++) {
      locks[i] = new Object();
    }
  }

  /**
   * Corrects the size of the underlying LightWeightGSet.
   *
   * <p>This method is used when concurrent modifications may have left
   * the size of the LightWeightGSet in an inconsistent state. It directly updates the
   * size field of the underlying set to the correct value.
   *
   * @param size the correct size to set
   */
  @Override
  public void correctSize(int size) {
    lightWeightGSet.size = size;
  }

  @Override
  public void addSize(int deltaSize) {
    size.add(deltaSize);
  }

  @Override
  public void correctSize() {
    correctSize(size.intValue());
  }

  /**
   * Returns the lock object associated with the given key.
   * 
   * <p>The lock is determined by:
   * <ol>
   *   <li>Computing the key's index using the underlying set's getIndex method</li>
   *   <li>Mapping the index to a lock using modulo operation</li>
   * </ol>
   * 
   * <p>This ensures that the same key always maps to the same lock, while
   * distributing different keys across multiple locks for better concurrency.
   * 
   * @param key the key for which to get the associated lock
   * @return the lock object that should be used for synchronizing operations on this key
   * @throws NullPointerException if key is null (depending on the underlying set's behavior)
   */
  public Object getLock(K key) {
    int index = lightWeightGSet.getIndex(key);
    return locks[index % locks.length];
  }

}
