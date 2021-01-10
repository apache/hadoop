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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A map of functions which can be invoked to dynamically
 * create the value of an entry.
 * @param <E> type of entry value.
 */
final class EvaluatingStatisticsMap<E extends Serializable> implements
    Map<String, E> {

  /**
   * Functions to invoke when evaluating keys.
   */
  private final Map<String, Function<String, E>> evaluators
      = new ConcurrentHashMap<>();

  /**
   * Function to use when copying map values.
   */
  private final Function<E, E> copyFn;

  /**
   * Construct with the copy function being simple passthrough.
   */
  EvaluatingStatisticsMap() {
    this(IOStatisticsBinding::passthroughFn);
  }

  /**
   * Construct with the copy function being that supplied in.
   * @param copyFn copy function.
   */
  EvaluatingStatisticsMap(final Function<E, E> copyFn) {
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
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> keySet() {
    return evaluators.keySet();
  }

  /**
   * Evaluate all the entries and provide a list of the results.
   *
   * This is not a snapshot, so if the evaluators actually return
   * references to mutable objects (e.g. a MeanStatistic instance)
   * then that value may still change.
   * @return the current list of evaluated results.
   */
  @Override
  public Collection<E> values() {
    Set<Entry<String, Function<String, E>>> evalEntries =
        evaluators.entrySet();
    return evalEntries.parallelStream().map((e) ->
        e.getValue().apply(e.getKey()))
        .collect(Collectors.toList());
  }

  /**
   * Take a snapshot.
   * @return a map snapshot.
   */
  public Map<String, E> snapshot() {
    return IOStatisticsBinding.snapshotMap(this, copyFn);
  }

  /**
   * Creating the entry set forces an evaluation of the functions.
   *
   * This is not a snapshot, so if the evaluators actually return
   * references to mutable objects (e.g. a MeanStatistic instance)
   * then that value may still change.
   *
   * The evaluation may be parallelized.
   * @return an evaluated set of values
   */
  @Override
  public synchronized Set<Entry<String, E>> entrySet() {
    Set<Entry<String, Function<String, E>>> evalEntries =
        evaluators.entrySet();
    Set<Entry<String, E>> r = evalEntries.parallelStream().map((e) ->
        new EntryImpl<>(e.getKey(), e.getValue().apply(e.getKey())))
        .collect(Collectors.toSet());
    return r;
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
    public E setValue(final E val) {
      this.value = val;
      return val;
    }
  }

}
