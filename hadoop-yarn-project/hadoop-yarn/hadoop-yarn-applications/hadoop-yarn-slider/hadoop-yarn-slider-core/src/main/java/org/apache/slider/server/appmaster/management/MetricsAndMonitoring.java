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

package org.apache.slider.server.appmaster.management;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.health.HealthCheckRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class for all metrics and monitoring
 */
public class MetricsAndMonitoring extends CompositeService {
  protected static final Logger log =
    LoggerFactory.getLogger(MetricsAndMonitoring.class);
  public MetricsAndMonitoring(String name) {
    super(name);
  }
  
  public MetricsAndMonitoring() {
    super("MetricsAndMonitoring");
  }
  
  /**
   * Singleton of metrics registry
   */
  final MetricRegistry metrics = new MetricRegistry();

  final HealthCheckRegistry health = new HealthCheckRegistry();

  private final Map<String, MeterAndCounter> meterAndCounterMap
      = new ConcurrentHashMap<>();

  private final List<MetricSet> metricSets = new ArrayList<>();

  public static final int EVENT_LIMIT = 1000;

  public MetricRegistry getMetrics() {
    return metrics;
  }

  public HealthCheckRegistry getHealth() {
    return health;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    addService(new MetricsBindingService("MetricsBindingService",
        metrics));
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    for (MetricSet set : metricSets) {
      unregister(set);
    }
  }

  public MeterAndCounter getMeterAndCounter(String name) {
    return meterAndCounterMap.get(name);
  }

  /**
   * Get or create the meter/counter pair
   * @param name name of instance
   * @return an instance
   */
  public MeterAndCounter getOrCreateMeterAndCounter(String name) {
    MeterAndCounter instance = meterAndCounterMap.get(name);
    if (instance == null) {
      synchronized (this) {
        // check in a sync block
        instance = meterAndCounterMap.get(name);
        if (instance == null) {
          instance = new MeterAndCounter(metrics, name);
          meterAndCounterMap.put(name, instance);
        }
      }
    }
    return instance;
  }

  /**
   * Get a specific meter and mark it. This will create and register it on demand.
   * @param name name of meter/counter
   */
  public void markMeterAndCounter(String name) {
    MeterAndCounter meter = getOrCreateMeterAndCounter(name);
    meter.mark();
  }

  /**
   * Given a {@link Metric}, registers it under the given name.
   *
   * @param name   the name of the metric
   * @param metric the metric
   * @param <T>    the type of the metric
   * @return {@code metric}
   * @throws IllegalArgumentException if the name is already registered
   */
  public <T extends Metric> T register(String name, T metric) throws IllegalArgumentException {
    return metrics.register(name, metric);
  }

  public <T extends Metric> T register(Class<?> klass, T metric, String... names)
      throws IllegalArgumentException {
    return register(MetricRegistry.name(klass, names), metric);
  }

  /**
   * Add a metric set for registering and deregistration on service stop
   * @param metricSet metric set
   */
  public void addMetricSet(MetricSet metricSet) {
    metricSets.add(metricSet);
    metrics.registerAll(metricSet);
  }

  /**
   * add a metric set, giving each entry a prefix
   * @param prefix prefix (a trailing "." is automatically added)
   * @param metricSet the metric set to register
   */
  public void addMetricSet(String prefix, MetricSet metricSet) {
    addMetricSet(new PrefixedMetricsSet(prefix, metricSet));
  }

  /**
   * Unregister a metric set; robust
   * @param metricSet metric set to unregister
   */
  public void unregister(MetricSet metricSet) {
    for (String s : metricSet.getMetrics().keySet()) {
      try {
        metrics.remove(s);
      } catch (IllegalArgumentException e) {
        // log but continue
        log.info("Exception when trying to unregister {}", s, e);
      }
    }
  }
}

