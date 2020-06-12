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
import java.util.function.Function;

import org.apache.hadoop.fs.statistics.StatisticsMap;


/**
 * This map evaluates on demand; it is the primary
 * mechanism by which statistics are collected.
 * @param <E> statistics type
 */
final class EvaluatingStatisticsMap<E extends Serializable> implements
    StatisticsMap<E> {


  /**
   * Functions to invoke when evaluating keys
   */
  private final Map<String, Function<String, E>> evaluators
      = new TreeMap<>();

  @Override
  public E get(final Object key) {
    Function<String, E> fn = evaluators.get(key);
    return fn != null
        ? fn.apply((String) key)
        : null;
  }

  @Override
  public Set<Entry<String, E>> entrySet() {
    return null;
  }

  /**
   * add a mapping of a key to a long function.
   * @param key the key
   * @param eval the evaluator
   */
  void addFunction(String key, Function<String, E> eval) {
    evaluators.put(key, eval);
  }

  @Override
  public boolean containsKey(final Object key) {
    return evaluators.containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return evaluators.size();
  }

  @Override
  public boolean isEmpty() {
    return evaluators.isEmpty();
  }

  @Override
  public E put(final String key, final E value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(final Map<? extends String, ? extends E> m) {
    throw new UnsupportedOperationException();

  }

  @Override
  public E remove(final Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    evaluators.clear();
  }

  @Override
  public Set<String> keySet() {
    return evaluators.keySet();
  }

  /**
   * Takes a snapshot and then provide an iterator around this.
   * @return the iterator.
   */
  @Override
  public Collection<E> values() {
    return new StatisticsMapSnapshot<>(this).values();

  }
}
