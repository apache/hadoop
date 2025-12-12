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

/**
 * A concurrency controller for {@link GSet}.
 * This interface provides thread-safety mechanisms for GSet operations,
 * including lock management and size correction for concurrent modifications.
 *
 * @param <K> The type of the key.
 */
public interface GSetConcurrencyController<K> {

  /**
   * Get the lock object for the given key.
   *
   * @param key the key.
   * @return a lock object to synchronize on, or null if no synchronization is needed.
   */
  Object getLock(K key);

  /**
   * Some implementations may allow modifying GSet concurrently, but leave the GSet size inaccurate.
   * GSetConcurrencyController provides an independent size counter(initial value is the size of
   * the GSet when constructed), which needs caller to keep track of the size change and call
   * this method to correct the size.
   * The size change will not be applied to the underlying GSet util {@link #correctSize()}
   * is called.
   *
   * @param deltaSize deltaSize, a negative value means size decrease.
   */
  void addSize(int deltaSize);

  /**
   * Apply the size correction to the underlying GSet.
   */
  void correctSize();

  /**
   * In some scenarios, we already know the final size of the GSet. We can use this method
   * to correct the size directly without calling {@link #addSize(int)} repeatedly.
   * 
   * <p>NOTE: Caller is responsible for ensuring the correctness of the given size.</p>
   *
   * @param size the corrected size.
   */
  void correctSize(int size);

  /**
   * A convenience method to execute a runnable under the lock of the given key.
   *
   * @param key      the key.
   * @param runnable the runnable to be executed.
   */
  default void doUnderLock(K key, Runnable runnable) {
    Object lock = getLock(key);
    if (lock != null) {
      synchronized (lock) {
        runnable.run();
      }
    } else {
      runnable.run();
    }
  }

}
