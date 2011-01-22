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

package org.apache.hadoop.metrics2.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.metrics2.Metric;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import static org.apache.hadoop.metrics2.lib.MetricsRegistry.*;

class MetricsRecordBuilderImpl extends MetricsRecordBuilder {
  private final long timestamp;
  private final String name;
  private final List<Metric> metrics;
  private final List<MetricsTag> tags;
  private final MetricsFilter recordFilter, metricFilter;
  private final boolean acceptable;

  MetricsRecordBuilderImpl(String name, MetricsFilter rf, MetricsFilter mf,
                           boolean acceptable) {
    timestamp = System.currentTimeMillis();
    this.name = name;
    metrics = new ArrayList<Metric>();
    tags = new ArrayList<MetricsTag>();
    recordFilter = rf;
    metricFilter = mf;
    this.acceptable = acceptable;
  }

  @Override
  public MetricsRecordBuilder tag(String name, String description,
                                  String value) {
    if (acceptable) {
      tags.add(new MetricsTag(name, description, value));
    }
    return this;
  }

  @Override
  public MetricsRecordBuilder addCounter(String name, String description,
                                         int value) {
    if (acceptable && (metricFilter == null || metricFilter.accepts(name))) {
      metrics.add(new MetricCounterInt(name, description, value));
    }
    return this;
  }

  @Override
  public MetricsRecordBuilder addCounter(String name, String description,
                                         long value) {
    if (acceptable && (metricFilter == null || metricFilter.accepts(name))) {
      metrics.add(new MetricCounterLong(name, description, value));
    }
    return this;
  }

  @Override
  public MetricsRecordBuilder addGauge(String name, String description,
                                       int value) {
    if (acceptable && (metricFilter == null || metricFilter.accepts(name))) {
      metrics.add(new MetricGaugeInt(name, description, value));
    }
    return this;
  }

  @Override
  public MetricsRecordBuilder addGauge(String name, String description,
                                       long value) {
    if (acceptable && (metricFilter == null || metricFilter.accepts(name))) {
      metrics.add(new MetricGaugeLong(name, description, value));
    }
    return this;
  }

  @Override
  public MetricsRecordBuilder addGauge(String name, String description,
                                       float value) {
    if (acceptable && (metricFilter == null || metricFilter.accepts(name))) {
      metrics.add(new MetricGaugeFloat(name, description, value));
    }
    return this;
  }

  @Override
  public MetricsRecordBuilder addGauge(String name, String description,
                                       double value) {
    if (acceptable && (metricFilter == null || metricFilter.accepts(name))) {
      metrics.add(new MetricGaugeDouble(name, description, value));
    }
    return this;
  }

  @Override
  public MetricsRecordBuilder add(MetricsTag tag) {
    tags.add(tag);
    return this;
  }

  @Override
  public MetricsRecordBuilder add(Metric metric) {
    metrics.add(metric);
    return this;
  }

  @Override
  public MetricsRecordBuilder setContext(String value) {
    return tag(CONTEXT_KEY, CONTEXT_DESC, value);
  }

  public MetricsRecordImpl getRecord() {
    if (acceptable && (recordFilter == null || recordFilter.accepts(tags))) {
      return new MetricsRecordImpl(name, timestamp, tags(), metrics());
    }
    return null;
  }

  List<MetricsTag> tags() {
    return Collections.unmodifiableList(tags);
  }

  List<Metric> metrics() {
    return Collections.unmodifiableList(metrics);
  }

}
