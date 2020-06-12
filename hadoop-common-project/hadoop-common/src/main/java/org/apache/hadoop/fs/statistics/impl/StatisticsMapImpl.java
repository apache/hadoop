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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import org.apache.hadoop.fs.statistics.StatisticsMap;

/**
 * Complete implementation of {@link StatisticsMap}.
 * <p></p>
 * An inner map is used; this is volatile.
 * All serialization is done explicitly.
 * @param <E> type of elements
 */
public final class StatisticsMapImpl<E extends Serializable>
    implements StatisticsMap<E> {

  private final Map<String, E> inner = new ConcurrentHashMap<>();

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
    return inner.put(key, value);
  }

  @Override
  public E remove(final Object key) {
    return inner.remove(key);
  }

  @Override
  public void putAll(final Map<? extends String, ? extends E> m) {
    inner.putAll(m);
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
    return inner.putIfAbsent(key, value);
  }

  @Override
  public boolean remove(final Object key, final Object value) {
    return inner.remove(key, value);
  }

  @Override
  public boolean replace(final String key, final E oldValue, final E newValue) {
    return inner.replace(key, oldValue, newValue);
  }

  @Override
  public E replace(final String key, final E value) {
    return inner.replace(key, value);
  }

  @Override
  public E merge(final String key,
      final E value,
      final BiFunction<? super E, ? super E, ? extends E> remappingFunction) {
    return inner.merge(key, value, remappingFunction);
  }

  @Override
  public int hashCode() {
    return inner.hashCode();
  }


}
