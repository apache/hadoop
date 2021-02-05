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

import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * This is base class for allocated and available metrics for
 * custom resources.
 */
public class CustomResourceMetrics {
  private static final String ALLOCATED_RESOURCE_METRIC_PREFIX =
      "AllocatedResource.";
  private static final String ALLOCATED_RESOURCE_METRIC_DESC = "Allocated NAME";

  private static final String AVAILABLE_RESOURCE_METRIC_PREFIX =
      "AvailableResource.";
  private static final String AVAILABLE_RESOURCE_METRIC_DESC = "Available NAME";

  private final CustomResourceMetricValue allocated =
      new CustomResourceMetricValue();
  private final CustomResourceMetricValue available =
      new CustomResourceMetricValue();

  /**
   * Register all custom resources metrics as part of initialization.
   * @param customResources Map containing all custom resource types
   * @param registry of the metric type
   */
  public void registerCustomResources(Map<String, Long> customResources,
      MetricsRegistry registry) {
    registerCustomResources(customResources, registry,
        ALLOCATED_RESOURCE_METRIC_PREFIX, ALLOCATED_RESOURCE_METRIC_DESC);
    registerCustomResources(customResources, registry,
        AVAILABLE_RESOURCE_METRIC_PREFIX, AVAILABLE_RESOURCE_METRIC_DESC);
  }

  /**
   * Get a map of all custom resource metric.
   * @return map of custom resource
   */
  public Map<String, Long> initAndGetCustomResources() {
    Map<String, Long> customResources = new HashMap<String, Long>();
    ResourceInformation[] resources = ResourceUtils.getResourceTypesArray();

    for (int i = 2; i < resources.length; i++) {
      ResourceInformation resource = resources[i];
      customResources.put(resource.getName(), Long.valueOf(0));
    }
    return customResources;
  }

  /**
   * As and when this metric object construction happens for any queue, all
   * custom resource metrics value would be initialized with '0' like any other
   * mandatory resources metrics.
   * @param customResources Map containing all custom resource types
   * @param registry of the metric type
   * @param metricPrefix prefix in metric name
   * @param metricDesc suffix for metric name
   */
  public void registerCustomResources(Map<String, Long> customResources,
      MetricsRegistry registry, String metricPrefix, String metricDesc) {
    for (Map.Entry<String, Long> entry : customResources.entrySet()) {
      String resourceName = entry.getKey();
      Long resourceValue = entry.getValue();

      MutableGaugeLong resourceMetric =
          (MutableGaugeLong) registry.get(metricPrefix + resourceName);

      if (resourceMetric == null) {
        resourceMetric = registry.newGauge(metricPrefix + resourceName,
            metricDesc.replace("NAME", resourceName), 0L);
      }
      resourceMetric.set(resourceValue);
    }
  }

  public void setAvailable(Resource res) {
    available.set(res);
  }

  public void increaseAllocated(Resource res) {
    allocated.increase(res);
  }

  public void increaseAllocated(Resource res, int containers) {
    allocated.increaseWithMultiplier(res, containers);
  }

  public void decreaseAllocated(Resource res) {
    allocated.decrease(res);
  }

  public void decreaseAllocated(Resource res, int containers) {
    allocated.decreaseWithMultiplier(res, containers);
  }

  public Map<String, Long> getAllocatedValues() {
    return allocated.getValues();
  }

  public Map<String, Long> getAvailableValues() {
    return available.getValues();
  }

  public CustomResourceMetricValue getAvailable() {
    return available;
  }
}
