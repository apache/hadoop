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

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.MeanStatistic;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkState;

/**
 * Builder of {@link DynamicIOStatistics}.
 *
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

  /**
   * Add a new evaluator to the counter statistics.
   * @param key key of this statistic
   * @param eval evaluator for the statistic
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withLongFunctionCounter(String key,
      ToLongFunction<String> eval) {
    activeInstance().addCounterFunction(key, eval::applyAsLong);
    return this;
  }

  /**
   * Add a counter statistic to dynamically return the
   * latest value of the source.
   * @param key key of this statistic
   * @param source atomic long counter
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withAtomicLongCounter(String key,
      AtomicLong source) {
    withLongFunctionCounter(key, s -> source.get());
    return this;
  }

  /**
   * Add a counter statistic to dynamically return the
   * latest value of the source.
   * @param key key of this statistic
   * @param source atomic int counter
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withAtomicIntegerCounter(String key,
      AtomicInteger source) {
    withLongFunctionCounter(key, s -> source.get());
    return this;
  }

  /**
   * Build a dynamic counter statistic from a
   * {@link MutableCounterLong}.
   * @param key key of this statistic
   * @param source mutable long counter
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withMutableCounter(String key,
      MutableCounterLong source) {
    withLongFunctionCounter(key, s -> source.value());
    return this;
  }

  /**
   * Add a new evaluator to the gauge statistics.
   * @param key key of this statistic
   * @param eval evaluator for the statistic
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withLongFunctionGauge(String key,
      ToLongFunction<String> eval) {
    activeInstance().addGaugeFunction(key, eval::applyAsLong);
    return this;
  }

  /**
   * Add a gauge statistic to dynamically return the
   * latest value of the source.
   * @param key key of this statistic
   * @param source atomic long gauge
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withAtomicLongGauge(String key,
      AtomicLong source) {
    withLongFunctionGauge(key, s -> source.get());
    return this;
  }

  /**
   * Add a gauge statistic to dynamically return the
   * latest value of the source.
   * @param key key of this statistic
   * @param source atomic int gauge
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withAtomicIntegerGauge(String key,
      AtomicInteger source) {
    withLongFunctionGauge(key, s -> source.get());
    return this;
  }

  /**
   * Add a new evaluator to the minimum statistics.
   * @param key key of this statistic
   * @param eval evaluator for the statistic
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withLongFunctionMinimum(String key,
      ToLongFunction<String> eval) {
    activeInstance().addMinimumFunction(key, eval::applyAsLong);
    return this;
  }

  /**
   * Add a minimum statistic to dynamically return the
   * latest value of the source.
   * @param key key of this statistic
   * @param source atomic long minimum
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withAtomicLongMinimum(String key,
      AtomicLong source) {
    withLongFunctionMinimum(key, s -> source.get());
    return this;
  }

  /**
   * Add a minimum statistic to dynamically return the
   * latest value of the source.
   * @param key key of this statistic
   * @param source atomic int minimum
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withAtomicIntegerMinimum(String key,
      AtomicInteger source) {
    withLongFunctionMinimum(key, s -> source.get());
    return this;
  }


  /**
   * Add a new evaluator to the maximum statistics.
   * @param key key of this statistic
   * @param eval evaluator for the statistic
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withLongFunctionMaximum(String key,
      ToLongFunction<String> eval) {
    activeInstance().addMaximumFunction(key, eval::applyAsLong);
    return this;
  }

  /**
   * Add a maximum statistic to dynamically return the
   * latest value of the source.
   * @param key key of this statistic
   * @param source atomic long maximum
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withAtomicLongMaximum(String key,
      AtomicLong source) {
    withLongFunctionMaximum(key, s -> source.get());
    return this;
  }

  /**
   * Add a maximum statistic to dynamically return the
   * latest value of the source.
   * @param key key of this statistic
   * @param source atomic int maximum
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withAtomicIntegerMaximum(String key,
      AtomicInteger source) {
    withLongFunctionMaximum(key, s -> source.get());
    return this;
  }

  /**
   * Add a new evaluator to the mean statistics.
   *
   * This is a function which must return the mean and the sample count.
   * @param key key of this statistic
   * @param eval evaluator for the statistic
   * @return the builder.
   */
  public DynamicIOStatisticsBuilder withMeanStatisticFunction(String key,
      Function<String, MeanStatistic> eval) {
    activeInstance().addMeanStatisticFunction(key, eval);
    return this;
  }

}
