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

package org.apache.hadoop.yarn.server.timeline;

import java.util.Iterator;

/**
 * An adapter for map timeline store implementations
 * @param <K> the type of the key set
 * @param <V> the type of the value set
 */
interface TimelineStoreMapAdapter<K, V> {
  /**
   * @param key
   * @return map(key)
   */
  V get(K key);

  /**
   * Add mapping key->value in the map
   * @param key
   * @param value
   */
  void put(K key, V value);

  /**
   * Remove mapping with key keyToRemove
   * @param keyToRemove
   */
  void remove(K keyToRemove);

  /**
   * @return the iterator of the value set of the map
   */
  Iterator<V> valueSetIterator();

  /**
   * Return the iterator of the value set of the map, starting from minV if type
   * V is comparable.
   * @param minV
   * @return
   */
  Iterator<V> valueSetIterator(V minV);
}
