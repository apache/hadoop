/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.insight;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.ozone.insight.Component.Type;

/**
 * Definition of a group of metrics which can be displayed.
 */
public class MetricGroupDisplay {

  /**
   * List fhe included metrics.
   */
  private List<MetricDisplay> metrics = new ArrayList<>();

  /**
   * Name of the component which includes the metrics (scm, om,...).
   */
  private Component component;

  /**
   * Human readable description.
   */
  private String description;

  public MetricGroupDisplay(Component component, String description) {
    this.component = component;
    this.description = description;
  }

  public MetricGroupDisplay(Type componentType, String metricName) {
    this(new Component(componentType), metricName);
  }

  public List<MetricDisplay> getMetrics() {
    return metrics;
  }

  public void addMetrics(MetricDisplay item) {
    this.metrics.add(item);
  }

  public String getDescription() {
    return description;
  }

  public Component getComponent() {
    return component;
  }
}
