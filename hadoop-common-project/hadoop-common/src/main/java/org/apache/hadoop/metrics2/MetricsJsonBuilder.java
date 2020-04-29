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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Build a JSON dump of the metrics.
 *
 * The {@link #toString()} operator dumps out all values collected.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsJsonBuilder extends MetricsRecordBuilder {

  public static final Logger LOG = LoggerFactory
      .getLogger(MetricsRecordBuilder.class);
  private final MetricsCollector parent;
  private Map<String, Object> innerMetrics = new LinkedHashMap<>();

  private static final ObjectWriter WRITER =
      new ObjectMapper().writer();

  /**
   * Build an instance.
   * @param parent parent collector. Unused in this instance; only used for
   * the {@link #parent()} method
   */
  public MetricsJsonBuilder(MetricsCollector parent) {
    this.parent = parent;
  }

  private MetricsRecordBuilder tuple(String key, Object value) {
    innerMetrics.put(key, value);
    return this;
  }

  @Override
  public MetricsRecordBuilder tag(MetricsInfo info, String value) {
    return tuple(info.name(), value);
  }

  @Override
  public MetricsRecordBuilder add(MetricsTag tag) {
    return tuple(tag.name(), tag.value());
  }

  @Override
  public MetricsRecordBuilder add(AbstractMetric metric) {
    return tuple(metric.info().name(), metric.toString());
  }

  @Override
  public MetricsRecordBuilder setContext(String value) {
    return tuple("context", value);
  }

  @Override
  public MetricsRecordBuilder addCounter(MetricsInfo info, int value) {
    return tuple(info.name(), value);
  }

  @Override
  public MetricsRecordBuilder addCounter(MetricsInfo info, long value) {
    return tuple(info.name(), value);
  }

  @Override
  public MetricsRecordBuilder addGauge(MetricsInfo info, int value) {
    return tuple(info.name(), value);
  }

  @Override
  public MetricsRecordBuilder addGauge(MetricsInfo info, long value) {
    return tuple(info.name(), value);
  }

  @Override
  public MetricsRecordBuilder addGauge(MetricsInfo info, float value) {
    return tuple(info.name(), value);
  }

  @Override
  public MetricsRecordBuilder addGauge(MetricsInfo info, double value) {
    return tuple(info.name(), value);
  }

  @Override
  public MetricsCollector parent() {
    return parent;
  }

  @Override
  public String toString() {
    try {
      return WRITER.writeValueAsString(innerMetrics);
    } catch (IOException e) {
      LOG.warn("Failed to dump to Json.", e);
      return ExceptionUtils.getStackTrace(e);
    }
  }
}
