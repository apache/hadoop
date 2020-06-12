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

import com.google.common.collect.ImmutableSet;

import org.apache.hadoop.fs.statistics.StatisticsMap;

/**
 *
 * An immutable and empty stats map.
 *
 * @param <E>
 */
class EmptyStatisticsMap<E extends Serializable> implements StatisticsMap<E> {

  @SuppressWarnings("rawtypes")
  private static final EmptyStatisticsMap INSTANCE = new EmptyStatisticsMap();

  private static final long serialVersionUID = -4377604814347870386L;

  @SuppressWarnings("unchecked")
  public static <V extends Serializable> StatisticsMap<V> of() {
    return (StatisticsMap<V>) INSTANCE;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return true;
  }

  @Override
  public boolean containsKey(final Object key) {
    return false;
  }

  @Override
  public boolean containsValue(final Object value) {
    return false;
  }

  @Override
  public E get(final Object key) {
    return null;
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

  }

  @Override
  public Set<String> keySet() {
    return ImmutableSet.of();
  }

  @Override
  public Collection<E> values() {
    return ImmutableSet.of();
  }

  @Override
  public Set<Entry<String, E>> entrySet() {
    return ImmutableSet.of();
  }
}
