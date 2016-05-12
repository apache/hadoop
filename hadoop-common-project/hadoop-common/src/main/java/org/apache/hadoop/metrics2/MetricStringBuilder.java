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

package org.apache.hadoop.metrics2;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Build a string dump of the metrics.
 *
 * The {@link #toString()} operator dumps out all values collected.
 *
 * Every entry is formatted as
 * {@code prefix + name + separator + value + suffix}
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricStringBuilder extends MetricsRecordBuilder {

  private final StringBuilder builder = new StringBuilder(256);

  private final String prefix;
  private final String suffix;
  private final String separator;
  private final MetricsCollector parent;

  /**
   * Build an instance.
   * @param parent parent collector. Unused in this instance; only used for
   * the {@link #parent()} method
   * @param prefix string before each entry
   * @param separator separator between name and value
   * @param suffix suffix after each entry
   */
  public MetricStringBuilder(MetricsCollector parent,
      String prefix,
      String separator,
      String suffix) {
    this.parent = parent;
    this.prefix = prefix;
    this.suffix = suffix;
    this.separator = separator;
  }

  public MetricStringBuilder add(MetricsInfo info, Object value) {
    return tuple(info.name(), value.toString());
  }

  /**
   * Add any key,val pair to the string, between the prefix and suffix,
   * separated by the separator.
   * @param key key
   * @param value value
   * @return this instance
   */
  public MetricStringBuilder tuple(String key, String value) {
    builder.append(prefix)
        .append(key)
        .append(separator)
        .append(value)
        .append(suffix);
    return this;
  }

  @Override
  public MetricsRecordBuilder tag(MetricsInfo info, String value) {
    return add(info, value);
  }

  @Override
  public MetricsRecordBuilder add(MetricsTag tag) {
    return tuple(tag.name(), tag.value());
  }

  @Override
  public MetricsRecordBuilder add(AbstractMetric metric) {
    add(metric.info(), metric.toString());
    return this;
  }

  @Override
  public MetricsRecordBuilder setContext(String value) {
    return tuple("context", value);
  }

  @Override
  public MetricsRecordBuilder addCounter(MetricsInfo info, int value) {
    return add(info, value);
  }

  @Override
  public MetricsRecordBuilder addCounter(MetricsInfo info, long value) {
    return add(info, value);
  }

  @Override
  public MetricsRecordBuilder addGauge(MetricsInfo info, int value) {
    return add(info, value);
  }

  @Override
  public MetricsRecordBuilder addGauge(MetricsInfo info, long value) {
    return add(info, value);
  }

  @Override
  public MetricsRecordBuilder addGauge(MetricsInfo info, float value) {
    return add(info, value);
  }

  @Override
  public MetricsRecordBuilder addGauge(MetricsInfo info, double value) {
    return add(info, value);
  }

  @Override
  public MetricsCollector parent() {
    return parent;
  }

  @Override
  public String toString() {
    return builder.toString();
  }
}
