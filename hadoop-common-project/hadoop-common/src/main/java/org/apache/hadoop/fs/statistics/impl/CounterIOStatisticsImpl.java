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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.dynamicIOStatistics;

/**
 * Implement statistics as a map of atomic longs.
 */
final class CounterIOStatisticsImpl extends WrappedIOStatistics
    implements CounterIOStatistics {


  private final Map<String, AtomicLong> counters = new HashMap<>();

  /**
   * Constructor.
   * @param keys keys to use for the counter statistics.
   */
  CounterIOStatisticsImpl(String[] keys) {
    super(null);
    DynamicIOStatisticsBuilder builder = dynamicIOStatistics();
    for (int i = 0; i < keys.length; i++) {
      AtomicLong counter = new AtomicLong();
      String key = keys[i];
      counters.put(key, counter);
      builder.add(key, counter);
    }
    setSource(builder.build());
  }

  @Override
  public void increment(final String key, final long value) {
    AtomicLong counter = counters.get(key);
    if (counter != null) {
      counter.addAndGet(value);
    }
  }

  @Override
  public void set(final String key, final long value) {
    AtomicLong counter = counters.get(key);
    if (counter != null) {
      counter.set(value);
    }
  }

}
