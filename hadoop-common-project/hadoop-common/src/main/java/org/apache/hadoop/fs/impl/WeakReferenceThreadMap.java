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

/**
 * A WeakReferenceMap for threads.
 * @param <V> value type of the map
 */
public class WeakReferenceThreadMap<V> extends WeakReferenceMap<Long, V> {

  public WeakReferenceThreadMap(final Function<? super Long, ? extends V> factory,
      @Nullable final Consumer<? super Long> referenceLost) {
    super(factory, referenceLost);
  }

  public V getForCurrentThread() {
    return get(currentThreadId());
  }

  public V removeForCurrentThread() {
    return remove(currentThreadId());
  }

  public long currentThreadId() {
    return Thread.currentThread().getId();
  }

  public V setForCurrentThread(V newVal) {
    long id = currentThreadId();

    // if the same object is already in the map, just return it.
    WeakReference<V> ref = lookup(id);
    // Reference value could be set to null. Thus, ref.get() could return
    // null. Should be handled accordingly while using the returned value.
    if (ref != null && ref.get() == newVal) {
      return ref.get();
    }

    return put(id, newVal);
  }

}
