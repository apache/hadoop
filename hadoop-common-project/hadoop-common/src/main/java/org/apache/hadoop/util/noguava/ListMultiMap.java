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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * The type List multi map.
 *
 * @param <K> the type parameter
 * @param <V> the type parameter
 */
public class ListMultiMap<K, V> implements Multimap<K, V> {

  private final Map<K, List<V>> entries;

  /**
   * Instantiates a new List multi map.
   *
   * @param map the map
   */
  public ListMultiMap(final Map<K, List<V>> map) {
    this.entries = map;
  }

  /**
   * Instantiates a new List multi map.
   */
  public ListMultiMap() {
    this(new HashMap<>());
  }

  @Override
  public boolean put(final K key, final V value) {
    this.entries.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    return false;
  }

  @Override
  public void remove(final K key, final V value) {
    this.entries.computeIfPresent(key,
        (k, set) -> set.remove(value) && set.isEmpty() ? null : set);
  }

  @Override
  public void clear() {
    entries.clear();
  }

  public List<V> get(K key) {
    return this.entries.getOrDefault(key, Collections.emptyList());
  }

  public int size() {
    return this.entries.values().stream().mapToInt(List::size).sum();
  }

  @Override
  public boolean isEmpty() {
    return keySet().isEmpty();
  }

  @Override
  public boolean containsKey(final Object key) {
    return keySet().contains(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    return values().contains(value);
  }

  @Override
  public List<V> values() {
    return (List<V>) entries.values().stream().flatMap(List::stream);
  }

  @Override
  public Set<K> keySet() {
    return entries.keySet();
  }

  /**
   * Creates an index {@code ImmutableListMultimap} that contains the results
   * of applying a specified function to each item in an {@code Iterable}
   * of values.
   *
   * @param <K>         the type parameter
   * @param <V>         the type parameter
   * @param values      the values to use when constructing the {@code
   * ListMultimap}
   * @param keyFunction the function used to produce the key for each value
   * @return {@code ListMultimap} mapping the result of evaluating the
   * function {@code keyFunction} on each value in the input collection
   * to that value
   * @throws NullPointerException if any element of {@code values} is
   *                              {@code null}, or if {@code keyFunction}
   *                              produces {@code null} for        any key
   */
  public static <K, V> ListMultiMap<K, V> index(
      Iterable<V> values, Function<? super V, K> keyFunction) {
    Map<K, List<V>> immutableMap =
        StreamSupport.stream(values.spliterator(), false)
            .collect(Collectors.toMap(keyFunction,
                val -> Stream.of(val).collect(Collectors.toList()),
                (a, b) -> Stream.of(a, b).flatMap(Collection::stream).collect(
                    Collectors.toList())));
    return new ListMultiMap<>(immutableMap);
  }


}

