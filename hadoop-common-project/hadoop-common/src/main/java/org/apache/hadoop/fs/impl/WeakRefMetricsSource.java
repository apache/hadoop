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

package org.apache.hadoop.fs.impl;

import java.lang.ref.WeakReference;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;

import static java.util.Objects.requireNonNull;

/**
 * A weak referenced metrics source which avoids hanging on to large objects
 * if somehow they don't get fully closed/cleaned up.
 * The JVM may clean up all objects which are only weakly referenced whenever
 * it does a GC, <i>even if there is no memory pressure</i>.
 * To avoid these refs being removed, always keep a strong reference around
 * somewhere.
 */
@InterfaceAudience.Private
public class WeakRefMetricsSource implements MetricsSource {

  /**
   * Name to know when unregistering.
   */
  private final String name;

  /**
   * Underlying metrics source.
   */
  private final WeakReference<MetricsSource> sourceWeakReference;

  /**
   * Constructor.
   * @param name Name to know when unregistering.
   * @param source metrics source
   */
  public WeakRefMetricsSource(final String name, final MetricsSource source) {
    this.name = name;
    this.sourceWeakReference = new WeakReference<>(requireNonNull(source));
  }

  /**
   * If the weak reference is non null, update the metrics.
   * @param collector to contain the resulting metrics snapshot
   * @param all if true, return all metrics even if unchanged.
   */
  @Override
  public void getMetrics(final MetricsCollector collector, final boolean all) {
    MetricsSource metricsSource = sourceWeakReference.get();
    if (metricsSource != null) {
      metricsSource.getMetrics(collector, all);
    }
  }

  /**
   * Name to know when unregistering.
   * @return the name passed in during construction.
   */
  public String getName() {
    return name;
  }

  /**
   * Get the source, will be null if the reference has been GC'd
   * @return the source reference
   */
  public MetricsSource getSource() {
    return sourceWeakReference.get();
  }

  @Override
  public String toString() {
    return "WeakRefMetricsSource{" +
        "name='" + name + '\'' +
        ", sourceWeakReference is " +
        (sourceWeakReference.get() == null ? "unset" : "set") +
        '}';
  }
}
