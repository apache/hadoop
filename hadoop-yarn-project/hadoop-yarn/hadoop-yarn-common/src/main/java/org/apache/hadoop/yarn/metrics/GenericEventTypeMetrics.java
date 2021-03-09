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

import java.util.EnumMap;

@InterfaceAudience.Private
@Metrics(context="yarn")
public class GenericEventTypeMetrics<T extends Enum<T>>
    implements EventTypeMetrics<T> {

  private final EnumMap<T, MutableGaugeLong> eventCountMetrics;
  private final EnumMap<T, MutableGaugeLong> processingTimeMetrics;
  private final MetricsRegistry registry;
  private final MetricsSystem ms;
  private final MetricsInfo info;
  private final Class<T> enumClass;
  private final T[] enums;

  public GenericEventTypeMetrics(MetricsInfo info, MetricsSystem ms,
                                 T[] enums, Class<T> enumClass) {

    this.enumClass = enumClass;
    this.eventCountMetrics = new EnumMap<>(this.enumClass);
    this.processingTimeMetrics = new EnumMap<>(this.enumClass);
    this.ms = ms;
    this.info = info;
    this.registry = new MetricsRegistry(this.info);
    this.enums = enums;

    //Initialze enum
    for (T type : this.enums) {
      String eventCountMetricsName =
          type.toString().toLowerCase() + "_" + "event_count";
      String processingTimeMetricsName =
          type.toString().toLowerCase() + "_" + "processing_time";
      eventCountMetrics.put(type, this.registry.
          newGauge(eventCountMetricsName, eventCountMetricsName, 0L));
      processingTimeMetrics.put(type, this.registry.
          newGauge(processingTimeMetricsName, processingTimeMetricsName, 0L));
    }
  }

  @Override
  public void incr(T type, long processingTimeUs) {
    if (eventCountMetrics.get(type) != null) {
      eventCountMetrics.get(type).incr();
      processingTimeMetrics.get(type).incr(processingTimeUs);
    }
  }

  @Override
  public void get(T type) {
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
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