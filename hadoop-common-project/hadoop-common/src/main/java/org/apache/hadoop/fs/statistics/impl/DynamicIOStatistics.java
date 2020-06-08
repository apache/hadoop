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
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.apache.hadoop.fs.statistics.IOStatisticEntry;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;

import static org.apache.hadoop.fs.statistics.IOStatisticEntry.entry;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.snapshotStatistics;

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
   * Use a concurrent hash map for the ability to add across
   * threads.
   */
  private final Map<String, Function<String, IOStatisticEntry>> evaluators
      = new TreeMap<>();

  DynamicIOStatistics() {
  }

  /**
   * add a mapping of a key to a long function.
   * @param key the key
   * @param eval the evaluator
   */
  void addLongFunction(String key, ToLongFunction<String> eval) {
    addFunction(key, k ->
        entry(IOStatisticEntry.IOSTATISTIC_COUNTER,
            eval.applyAsLong(k)));
  }

  /**
   * add a mapping of a key to a long function.
   * @param key the key
   * @param eval the evaluator
   */
  void addFunction(String key, Function<String, IOStatisticEntry> eval) {
    evaluators.put(key, eval);
  }

  /**
   * Get the value of a key.
   * If the key is present, this will (re)evaluate it
   * @param key key to look for.
   * @return the latest value of that statistic, if found, else null.
   */
  @Override
  public IOStatisticEntry getStatistic(final String key) {
    Function<String, IOStatisticEntry> fn = evaluators.get(key);
    return fn != null
        ? fn.apply(key)
        : null;
  }

  @Override
  public boolean isTracked(final String key) {
    return evaluators.containsKey(key);
  }

  /**
   * Takes a snapshot and then provide an iterator around this.
   * @return the iterator.
   */
  @Override
  public Iterator<Map.Entry<String, IOStatisticEntry>> iterator() {

    return snapshotStatistics(this).iterator();
  }

  @Override
  public Set<String> keys() {
    return evaluators.keySet();
  }

  @Override
  public String toString() {
    return IOStatisticsLogging.iostatisticsToString(this);
  }
}
