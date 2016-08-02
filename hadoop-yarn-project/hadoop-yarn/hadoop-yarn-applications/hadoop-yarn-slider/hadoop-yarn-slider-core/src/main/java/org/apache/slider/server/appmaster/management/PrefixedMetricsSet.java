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

package org.apache.slider.server.appmaster.management;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import java.util.HashMap;
import java.util.Map;

/**
 * From an existing metrics set, generate a new metrics set with the
 * prefix in front of every key.
 *
 * The prefix is added directly: if you want a '.' between prefix and metric
 * keys, include it in the prefix.
 */
public class PrefixedMetricsSet implements MetricSet {

  private final String prefix;
  private final MetricSet source;

  public PrefixedMetricsSet(String prefix, MetricSet source) {
    this.prefix = prefix;
    this.source = source;
  }

  @Override
  public Map<String, Metric> getMetrics() {
    Map<String, Metric> sourceMetrics = source.getMetrics();
    Map<String, Metric> metrics = new HashMap<>(sourceMetrics.size());
    for (Map.Entry<String, Metric> entry : sourceMetrics.entrySet()) {
      metrics.put(prefix + "." + entry.getKey(), entry.getValue());
    }
    return metrics;
  }
}
