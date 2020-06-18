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

package org.apache.hadoop.fs.statistics.impl;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;


/**
 * Take a snapshot of a single source map.
 * Important. Entries are copied using the copy function.
 * @param <E> map element type.
 */
@SuppressWarnings("NewExceptionWithoutArguments")
public final class StatisticsMapSnapshot<E extends Serializable> implements
    Map<String, E>, Serializable {

  private static final long serialVersionUID = 1360985530415418415L;

  /**
   * Treemaps sort their insertions so the iterator is ordered.
   * They are also serializable.
   */
  private final TreeMap<String, E> inner = new TreeMap<>();

  /**
   * Empty constructor for ser/deser
   */
  public StatisticsMapSnapshot() {
  }

  /**
   * Snapshot a type whose copy operation is
   * a simple passthrough
   * @param source source map
   */
  public StatisticsMapSnapshot(Map<String, E> source) {
    this(source, StatisticsMapSnapshot::passthrough);
  }

  /**
   * Snapshot arbitrary type.
   * @param source source map
   * @param copy function to copy a source entry.
   */
  public StatisticsMapSnapshot(
      Map<String, E> source,
      Function<E, E> copy) {
    // we have to clone the values so that they aren't
    // bound to the original values
    source.entrySet()
        .forEach(entry ->
            inner.put(entry.getKey(), copy.apply(entry.getValue())));
  }

  /**
   * A passthrough copy operation suitable for immutable
   * types, including numbers
   * @param src source object
   * @return the source object
   */
  static <E extends Serializable> E passthrough(E src) {
    return src;
  }

  @Override
  public int size() {
    return inner.size();
  }

  @Override
  public boolean isEmpty() {
    return inner.isEmpty();
  }

  @Override
  public boolean containsKey(final Object key) {
    return inner.containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    return inner.containsValue(value);
  }

  @Override
  public E get(final Object key) {
    return inner.get(key);
  }

  @Override
  public E put(final String key, final E value) {
    throw new UnsupportedOperationException();

  }

  @Override
  public E remove(final Object key) {
    throw new UnsupportedOperationException();

  }

  @Override
  public void putAll(final Map<? extends String, ? extends E> m) {
    throw new UnsupportedOperationException();

  }

  @Override
  public void clear() {
    inner.clear();
  }

  @Override
  public Set<String> keySet() {
    return inner.keySet();
  }

  @Override
  public Collection<E> values() {
    return inner.values();
  }

  @Override
  public Set<Entry<String, E>> entrySet() {
    return inner.entrySet();
  }

  @Override
  public E putIfAbsent(final String key, final E value) {
    throw new UnsupportedOperationException();

  }

  @Override
  public boolean remove(final Object key, final Object value) {
    throw new UnsupportedOperationException();

  }

  @Override
  public boolean replace(final String key, final E oldValue, final E newValue) {
    throw new UnsupportedOperationException();

  }

  @Override
  public E replace(final String key, final E value) {
    throw new UnsupportedOperationException();

  }

  @Override
  public E merge(final String key,
      final E value,
      final BiFunction<? super E, ? super E, ? extends E> remappingFunction) {
    throw new UnsupportedOperationException();

  }

  @Override
  public int hashCode() {
    return inner.hashCode();
  }

}
