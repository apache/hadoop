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
import java.util.stream.Collectors;

final class EvaluatingStatisticsMap<E extends Serializable> implements
    Map<String, E> {

  /**
   * Functions to invoke when evaluating keys.
   */
  private final Map<String, Function<String, E>> evaluators
      = new TreeMap<>();

  private final Function<E, E> copyFn;

  /**
   * Name for use in getter/error messages.
   */
  private final String name;

  EvaluatingStatisticsMap(final String name) {
    this(name, StatisticsMapSnapshot::passthrough);
  }

  EvaluatingStatisticsMap(final String name,
      final Function<E, E> copyFn) {
    this.name = name;
    this.copyFn = copyFn;
  }

  /**
   * add a mapping of a key to a function.
   * @param key the key
   * @param eval the evaluator
   */
  void addFunction(String key, Function<String, E> eval) {
    evaluators.put(key, eval);
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
  public boolean containsKey(final Object key) {
    return evaluators.containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public E get(final Object key) {
    Function<String, E> fn = evaluators.get(key);
    return fn != null
        ? fn.apply((String) key)
        : null;
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
    return snapshot().values();
  }

  public StatisticsMapSnapshot<E> snapshot() {
    return new StatisticsMapSnapshot<>(this, copyFn);
  }

  /**
   * Creating the entry set forces an evaluation of the functions.
   * The evaluation may be parallelized.
   * @return an evaluated set of values
   */
  @Override
  public Set<Entry<String, E>> entrySet() {
    Set<Entry<String, Function<String, E>>> evalEntries =
        evaluators.entrySet();
    Set<Entry<String, E>> r = evalEntries.parallelStream().map((e) ->
        new EntryImpl<>(e.getKey(), e.getValue().apply(e.getKey())))
        .collect(Collectors.toSet());
    return r;
  }

  String getName() {
    return name;
  }

  /**
   * Simple entry.
   * @param <E> entry type
   */
  private static final class EntryImpl<E> implements Entry<String, E> {

    private String key;

    private E value;

    private EntryImpl(final String key, final E value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public String getKey() {
      return key;
    }

    @Override
    public E getValue() {
      return value;
    }

    @Override
    public E setValue(final E value) {
      this.value = value;
      return value;
    }
  }


}
