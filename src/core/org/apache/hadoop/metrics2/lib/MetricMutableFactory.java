/**
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

package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.metrics2.Metric;

/**
 * Factory class for mutable metrics
 */
public class MetricMutableFactory {

  static final String DEFAULT_SAMPLE_NAME = "ops";
  static final String DEFAULT_VALUE_NAME  = "time";

  /**
   * Create a new mutable metric by name
   * Usually overridden by app specific factory
   * @param name  of the metric
   * @return  a new metric object
   */
  public MetricMutable newMetric(String name) {
    return null;
  }

  /**
   * Create a mutable integer counter
   * @param name  of the metric
   * @param description of the metric
   * @param initValue of the metric
   * @return  a new metric object
   */
  public MetricMutableCounterInt newCounter(String name, String description,
                                            int initValue) {
    return new MetricMutableCounterInt(name, description, initValue);
  }

  /**
   * Create a mutable integer counter with name only.
   * Usually gets overridden.
   * @param name  of the metric
   * @return  a new metric object
   */
  public MetricMutableCounterInt newCounterInt(String name) {
    return new MetricMutableCounterInt(name, Metric.NO_DESCRIPTION, 0);
  }

  /**
   * Create a mutable long integer counter
   * @param name  of the metric
   * @param description of the metric
   * @param initValue of the metric
   * @return  a new metric object
   */
  public MetricMutableCounterLong newCounter(String name, String description,
                                             long initValue) {
    return new MetricMutableCounterLong(name, description, initValue);
  }

  /**
   * Create a mutable long integer counter with a name
   * Usually gets overridden.
   * @param name  of the metric
   * @return  a new metric object
   */
  public MetricMutableCounterLong newCounterLong(String name) {
    return new MetricMutableCounterLong(name, Metric.NO_DESCRIPTION, 0L);
  }

  /**
   * Create a mutable integer gauge
   * @param name  of the metric
   * @param description of the metric
   * @param initValue of the metric
   * @return  a new metric object
   */
  public MetricMutableGaugeInt newGauge(String name, String description,
                                        int initValue) {
    return new MetricMutableGaugeInt(name, description, initValue);
  }

  /**
   * Create a mutable integer gauge with name only.
   * Usually gets overridden.
   * @param name  of the metric
   * @return  a new metric object
   */
  public MetricMutableGaugeInt newGaugeInt(String name) {
    return new MetricMutableGaugeInt(name, Metric.NO_DESCRIPTION, 0);
  }

  /**
   * Create a mutable long integer gauge
   * @param name  of the metric
   * @param description of the metric
   * @param initValue of the metric
   * @return  a new metric object
   */
  public MetricMutableGaugeLong newGauge(String name, String description,
                                         long initValue) {
    return new MetricMutableGaugeLong(name, description, initValue);
  }

  /**
   * Create a mutable long integer gauge with name only.
   * Usually gets overridden.
   * @param name  of the metric
   * @return  a new metric object
   */
  public MetricMutableGaugeLong newGaugeLong(String name) {
    return new MetricMutableGaugeLong(name, Metric.NO_DESCRIPTION, 0L);
  }

  /**
   * Create a mutable stat metric
   * @param name  of the metric
   * @param description of the metric
   * @param sampleName  of the metric (e.g., ops)
   * @param valueName   of the metric (e.g., time or latency)
   * @param extended    if true, produces extended stat (stdev, min/max etc.)
   * @return  a new metric object
   */
  public MetricMutableStat newStat(String name, String description,
                                   String sampleName, String valueName,
                                   boolean extended) {
    return new MetricMutableStat(name, description, sampleName, valueName,
                                 extended);
  }

  /**
   * Create a mutable stat metric with name only.
   * Usually gets overridden.
   * @param name  of the metric
   * @return  a new metric object
   */
  public MetricMutableStat newStat(String name) {
    return new MetricMutableStat(name, name, DEFAULT_SAMPLE_NAME,
                                 DEFAULT_VALUE_NAME);
  }

}
