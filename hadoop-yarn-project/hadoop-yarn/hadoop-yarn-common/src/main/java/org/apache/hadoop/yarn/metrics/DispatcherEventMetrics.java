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

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

@InterfaceAudience.Private
@Metrics(context="yarn")
public class DispatcherEventMetrics implements MetricsSource {

  private final Map<String, MutableGaugeLong> currentEventCountMetrics;
  private final Map<String, MutableRate> processingTimeMetrics;
  private final MetricsRegistry registry;

  public DispatcherEventMetrics(String name) {
    this.currentEventCountMetrics = new HashMap<>();
    this.processingTimeMetrics = new HashMap<>();
    this.registry = new MetricsRegistry(Interns.info(
        "DispatcherEventMetrics for " + name,
        "DispatcherEventMetrics for " + name
    ));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(registry.info()), all);
  }

  public void init(Class<? extends Enum> typeClass) {
    for(Object c : typeClass.getEnumConstants()) {
      String key = createKey(c);
      currentEventCountMetrics.put(key, this.registry.newGauge(
          key + "_Current", key + "_Current", 0L));
      processingTimeMetrics.put(key, this.registry.newRate(key + "_", key+ "_"));
    }
  }

  public void addEvent(Object type) {
    currentEventCountMetrics.get(createKey(type)).incr();
  }

  public void removeEvent(Object type) {
    currentEventCountMetrics.get(createKey(type)).decr();
  }

  public void updateRate(Object type, long millisecond) {
    processingTimeMetrics.get(createKey(type)).add(millisecond);
  }

  private String createKey(Object constant) {
    return constant.getClass().getSimpleName() + "#" + constant;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ")
        .add("currentEventCountMetrics=" + currentEventCountMetrics)
        .add("processingTimeMetrics=" + processingTimeMetrics)
        .toString();
  }

}
