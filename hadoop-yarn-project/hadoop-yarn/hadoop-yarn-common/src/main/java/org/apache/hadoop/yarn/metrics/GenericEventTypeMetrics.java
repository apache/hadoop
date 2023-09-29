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
package org.apache.hadoop.yarn.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;

@InterfaceAudience.Private
@Metrics(context="yarn")
public class GenericEventTypeMetrics<T extends Enum<T>>
    implements EventTypeMetrics<T> {

  static final Logger LOG =
      LoggerFactory.getLogger(GenericEventTypeMetrics.class);

  private final EnumMap<T, MutableGaugeLong> eventCountMetrics;
  private final EnumMap<T, MutableGaugeLong> processingTimeMetrics;
  private final MetricsRegistry registry;
  private final MetricsSystem ms;
  private final MetricsInfo info;
  private final Class<T> enumClass;

  private boolean isInitialized = false;

  public GenericEventTypeMetrics(MetricsInfo info, MetricsSystem ms,
                                 final T[] enums, Class<T> enumClass) {
    this.enumClass = enumClass;
    this.eventCountMetrics = new EnumMap<>(this.enumClass);
    this.processingTimeMetrics = new EnumMap<>(this.enumClass);
    this.ms = ms;
    this.info = info;
    this.registry = new MetricsRegistry(this.info);

    //Initialize enum
    for (final T type : enums) {
      String eventCountMetricsName = type + "_" + "event_count";
      String processingTimeMetricsName = type + "_" + "processing_time";
      eventCountMetrics.put(type, this.registry.
          newGauge(eventCountMetricsName, eventCountMetricsName, 0L));
      processingTimeMetrics.put(type, this.registry.
          newGauge(processingTimeMetricsName, processingTimeMetricsName, 0L));
    }
  }

  public synchronized GenericEventTypeMetrics registerMetrics() {
    if (!isInitialized) {
      // Register with the MetricsSystems
      if (this.ms != null) {
        LOG.info("Registering GenericEventTypeMetrics");
        ms.register(info.name(),
            info.description(), this);
        isInitialized = true;
      }
    }
    return this;
  }

  @Override
  public void increment(T type, long processingTimeUs) {
    if (eventCountMetrics.get(type) != null) {
      eventCountMetrics.get(type).incr();
      processingTimeMetrics.get(type).incr(processingTimeUs);
    }
  }

  @Override
  public long get(T type) {
    return eventCountMetrics.get(type).value();
  }

  public long getTotalProcessingTime(T type) {
    return processingTimeMetrics.get(type).value();
  }

  public EnumMap<T, MutableGaugeLong> getEventCountMetrics() {
    return eventCountMetrics;
  }

  public EnumMap<T, MutableGaugeLong> getProcessingTimeMetrics() {
    return processingTimeMetrics;
  }

  public MetricsRegistry getRegistry() {
    return registry;
  }

  public MetricsInfo getInfo() {
    return info;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(registry.info()), all);
  }

  public Class<T> getEnumClass() {
    return enumClass;
  }

  /** Builder class for GenericEventTypeMetrics. */
  public static class EventTypeMetricsBuilder<T extends Enum<T>>{
    public EventTypeMetricsBuilder() {
    }

    public EventTypeMetricsBuilder setEnumClass(Class<T> enumClassValue) {
      this.enumClass = enumClassValue;
      return this;
    }

    public EventTypeMetricsBuilder setEnums(T[] enumsValue) {
      this.enums = enumsValue.clone();
      return this;
    }

    public EventTypeMetricsBuilder setInfo(MetricsInfo infoValue) {
      this.info = infoValue;
      return this;
    }

    public EventTypeMetricsBuilder setMs(MetricsSystem msValue) {
      this.ms = msValue;
      return this;
    }

    public GenericEventTypeMetrics build() {
      return new GenericEventTypeMetrics(info, ms, enums, enumClass);
    }

    private MetricsSystem ms;
    private MetricsInfo info;
    private Class<T> enumClass;
    private T[] enums;
  }
}