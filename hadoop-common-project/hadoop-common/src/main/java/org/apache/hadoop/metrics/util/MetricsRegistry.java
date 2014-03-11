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
package org.apache.hadoop.metrics.util;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 
 * This is the registry for metrics.
 * Related set of metrics should be declared in a holding class and registered
 * in a registry for those metrics which is also stored in the the holding class.
 *
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
public class MetricsRegistry {
  private ConcurrentHashMap<String, MetricsBase> metricsList =
      new ConcurrentHashMap<String, MetricsBase>();

  public MetricsRegistry() {
  }
  
  /**
   * 
   * @return number of metrics in the registry
   */
  public int size() {
    return metricsList.size();
  }
  
  /**
   * Add a new metrics to the registry
   * @param metricsName - the name
   * @param theMetricsObj - the metrics
   * @throws IllegalArgumentException if a name is already registered
   */
  public void add(final String metricsName, final MetricsBase theMetricsObj) {
    if (metricsList.putIfAbsent(metricsName, theMetricsObj) != null) {
      throw new IllegalArgumentException("Duplicate metricsName:" +
          metricsName);
    }
  }

  
  /**
   * 
   * @param metricsName
   * @return the metrics if there is one registered by the supplied name.
   *         Returns null if none is registered
   */
  public MetricsBase get(final String metricsName) {
    return metricsList.get(metricsName);
  }
  
  
  /**
   * 
   * @return the list of metrics names
   */
  public Collection<String> getKeyList() {
    return metricsList.keySet();
  }
  
  /**
   * 
   * @return the list of metrics
   */
  public Collection<MetricsBase> getMetricsList() {
    return metricsList.values();
  }
}
