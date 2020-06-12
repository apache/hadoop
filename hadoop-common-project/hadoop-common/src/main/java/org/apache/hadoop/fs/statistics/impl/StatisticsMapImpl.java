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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import org.apache.hadoop.fs.statistics.StatisticsMap;

import static com.google.common.base.Preconditions.checkState;

/**
 * Complete implementation of {@link StatisticsMap}.
 * <p></p>
 * An inner map is used; this is volatile.
 * All serialization is done explicitly.
 * @param <E> type of elements
 */
public final class StatisticsMapImpl<E extends Serializable>
    implements StatisticsMap<E> {

  private volatile Map<String, E> inner = new ConcurrentHashMap<>();

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
  public int hashCode() {
    return inner.hashCode();
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

  /**
   * Wire format is size followed by each entry in order.
   * @param out destination
   * @throws IOException write failure
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeInt(inner.size());
    for (Entry<String, E> entry : inner.entrySet()) {
      out.writeObject(entry.getKey());
      out.writeObject(entry.getValue());
    }
  }

  /**
   * Read in the object.
   * For safety, the size must be > 0.
   * There's still the risk that something significantly malicious will
   * create a mismatch between map size and the value list...that should
   * fail either in this read (list too short) or in the next object.
   * @param in
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private void readObject(ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    inner.clear();
    int size = in.readInt();
    checkState(size > 0, "invalid map size: %s", size);
    for (int i = 0; i < size; i++) {
      inner.put((String) in.readObject(),
          (E) in.readObject());
    }
  }

  /**
   * Read without data. Just clear the inner map.
   * @throws ObjectStreamException never thrown.
   */
  private void readObjectNoData() throws ObjectStreamException {
    inner.clear();
  }

}
