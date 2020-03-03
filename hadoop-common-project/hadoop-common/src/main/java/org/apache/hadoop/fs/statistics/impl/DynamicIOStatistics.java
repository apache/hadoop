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

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.ToLongFunction;

import org.apache.hadoop.fs.statistics.IOStatistics;

/**
 * These statistics are dynamically evaluated by the supplied
 * String -&gt; Long functions.
 *
 * This allows statistic sources to supply a list of callbacks used to
 * generate the statistics on demand; similar to some of the Coda Hale metrics.
 *
 * The evaluation actually takes place during the iteration's {@code next()}
 * call; the returned a value is fixed.
 */
final class DynamicIOStatistics implements IOStatistics {

  /**
   * Treemaps sort their insertions so the iterator is ordered.
   */
  private final Map<String, ToLongFunction<String>> evaluators
      = new TreeMap<>();

  DynamicIOStatistics() {
  }

  /**
   * add a mapping of a key to an evaluator.
   * @param key the key
   * @param eval the evaluator
   */
  void add(String key, ToLongFunction<String> eval) {
    evaluators.put(key, eval);
  }

  /**
   * Get the value of a key.
   * If the key is present, this will (re)evaluate it
   * @param key key to look for.
   * @return the latest value of that statistic, if found, else null.
   */
  @Override
  public Long getLong(final String key) {
    ToLongFunction<String> fn = evaluators.get(key);
    return fn != null
        ? fn.applyAsLong(key)
        : null;
  }

  @Override
  public boolean isTracked(final String key) {
    return evaluators.containsKey(key);
  }

  @Override
  public Iterator<Map.Entry<String, Long>> iterator() {
    return new DynamicIterator();
  }

  @Override
  public boolean hasAttribute(final Attributes attr) {
    return Attributes.Dynamic == attr;
  }

  /**
   * Iterator over the entries, evaluating each one in the next() call.
   */
  private final class DynamicIterator
      implements Iterator<Map.Entry<String, Long>> {

    private final Iterator<Map.Entry<String, ToLongFunction<String>>>
        iterator = evaluators.entrySet().iterator();

    private DynamicIterator() {
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Map.Entry<String, Long> next() {
      final Map.Entry<String, ToLongFunction<String>> entry = iterator.next();
      return new IOStatisticsImplementationSupport.StatsMapEntry(
          entry.getKey(),
          entry.getValue().applyAsLong(entry.getKey()));
    }

  }

}
