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
package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.metrics.GenericEventTypeMetrics;

import static org.apache.hadoop.metrics2.lib.Interns.info;

public final class GenericEventTypeMetricsManager {

  private GenericEventTypeMetricsManager() {
    // nothing to do
  }

  // Construct a GenericEventTypeMetrics for dispatcher
  public static <T extends Enum<T>> GenericEventTypeMetrics
      create(String dispatcherName, Class<T> eventTypeClass) {
    MetricsInfo metricsInfo = info("GenericEventTypeMetrics for " + eventTypeClass.getName(),
        "Metrics for " + dispatcherName);
    return new GenericEventTypeMetrics.EventTypeMetricsBuilder<T>()
        .setMs(DefaultMetricsSystem.instance())
        .setInfo(metricsInfo)
        .setEnumClass(eventTypeClass)
        .setEnums(eventTypeClass.getEnumConstants())
        .build().registerMetrics();
  }
}
