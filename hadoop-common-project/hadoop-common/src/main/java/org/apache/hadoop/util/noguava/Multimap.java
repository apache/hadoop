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

package org.apache.hadoop.util.noguava;

import java.util.Collection;
import java.util.Set;


/**
 * A collection that maps keys to values, similar to {@link java.util.Map},
 * but in which each key may be associated with <i>multiple</i> values.
 */
public interface Multimap <K, V> {

  int size();
  boolean isEmpty();
  boolean containsKey(Object key);
  boolean containsValue(Object value);

  /**
   * Stores a key-value pair in this multimap.
   *
   * <p>Some multimap implementations allow duplicate key-value pairs, in
   * which case {@code put}
   * always adds a new key-value pair and increases the multimap size by 1.
   * Other implementations
   * prohibit duplicates, and storing a key-value pair that's already in the
   * multimap has no effect.
   *
   * @return {@code true} if the method increased the size of the multimap,
   * or {@code false} if the
   * multimap already contained the key-value pair and doesn't allow duplicates
   */
  boolean put(K key, V value);
  void remove(K key, V value);
  void clear();
  Collection<V> get(K key);
  Set<K> keySet();
  Collection<V> values();
}
