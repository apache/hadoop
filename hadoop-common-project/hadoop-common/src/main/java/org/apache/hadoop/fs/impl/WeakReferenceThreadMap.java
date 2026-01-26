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

package org.apache.hadoop.fs.impl;

import java.lang.ref.WeakReference;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

import org.apache.hadoop.util.WeakReferenceMap;

import static java.util.Objects.requireNonNull;

/**
 * A WeakReferenceMap for threads.
 * @param <V> value type of the map
 */
public class WeakReferenceThreadMap<V> extends WeakReferenceMap<Long, V> {

  public WeakReferenceThreadMap(final Function<? super Long, ? extends V> factory,
      @Nullable final Consumer<? super Long> referenceLost) {
    super(factory, referenceLost);
  }

  /**
   * Get the value for the current thread, creating if needed.
   * @return an instance.
   */
  public V getForCurrentThread() {
    return get(currentThreadId());
  }

  /**
   * Remove the reference for the current thread.
   * @return any reference value which existed.
   */
  public V removeForCurrentThread() {
    return remove(currentThreadId());
  }

  /**
   * Get the current thread ID.
   * @return thread ID.
   */
  public long currentThreadId() {
    return Thread.currentThread().getId();
  }

  /**
   * Set the new value for the current thread.
   * @param newVal new reference to set for the active thread.
   * @return the previously set value, possibly null
   */
  public V setForCurrentThread(V newVal) {
    requireNonNull(newVal);
    long id = currentThreadId();

    // if the same object is already in the map, just return it.
    WeakReference<V> existingWeakRef = lookup(id);

    // The looked up reference could be one of
    // 1. null: nothing there
    // 2. valid but get() == null : reference lost by GC.
    // 3. different from the new value
    // 4. the same as the old value
    if (resolve(existingWeakRef) == newVal) {
      // case 4: do nothing, return the new value
      return newVal;
    } else {
      // cases 1, 2, 3: update the map and return the old value
      return put(id, newVal);
    }

  }

}
