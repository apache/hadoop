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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.apache.hadoop.fs.statistics.IOStatisticEntry;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import static com.google.common.base.Preconditions.checkState;

/**
 * Builder of Dynamic IO Statistics which serve up up longs.
 * Instantiate through
 * {@link IOStatisticsBinding#dynamicIOStatistics()}.
 */
public class DynamicIOStatisticsBuilder {

  /**
   * the instance being built up. Will be null after the (single)
   * call to {@link #build()}.
   */
  private DynamicIOStatistics instance = new DynamicIOStatistics();

  /**
   * Add a new evaluator to the statistics being built up.
   * @param key key of this statistic
   * @param eval evaluator for the statistic
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withFunction(String key,
      Function<String, IOStatisticEntry> eval) {
    activeInstance().addFunction(key, eval);
    return this;
  }

  /**
   * Add a new evaluator to the statistics being built up.
   * @param key key of this statistic
   * @param eval evaluator for the statistic
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withLongFunction(String key,
      ToLongFunction<String> eval) {
    activeInstance().addLongFunction(key, eval);
    return this;
  }

  /**
   * Add a statistic to dynamically return the
   * latest value of the source.
   * @param key key of this statistic
   * @param source atomic long counter
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withAtomicLong(String key,
      AtomicLong source) {
    withLongFunction(key, s -> source.get());
    return this;
  }

  /**
   * Add a statistic to dynamically return the
   * latest value of the source.
   * @param key key of this statistic
   * @param source atomic int counter
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withAtomicInteger(String key,
      AtomicInteger source) {
    withLongFunction(key, s -> source.get());
    return this;
  }

  /**
   * Build a dynamic statistic from a
   * {@link MutableCounterLong}.
   * @param key key of this statistic
   * @param source mutable long counter
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withMutableCounter(String key,
      MutableCounterLong source) {
    withLongFunction(key, s -> source.value());
    return this;
  }

  /**
   * Build the IOStatistics instance.
   * @return an instance.
   * @throws IllegalStateException if the builder has already been built.
   */
  public IOStatistics build() {
    final DynamicIOStatistics stats = activeInstance();
    // stop the builder from working any more.
    instance = null;
    return stats;
  }

  /**
   * Get the statistics instance.
   * @return the instance to build/return
   * @throws IllegalStateException if the builder has already been built.
   */
  private DynamicIOStatistics activeInstance() {
    checkState(instance != null, "Already built");
    return instance;
  }
}
