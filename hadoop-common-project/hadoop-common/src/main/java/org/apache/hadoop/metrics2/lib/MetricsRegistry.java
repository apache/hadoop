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

package org.apache.hadoop.metrics2.lib;

import com.google.common.collect.Maps;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MsInfo;

import java.util.Collection;
import java.util.Map;
import java.util.StringJoiner;

/**
 * An optional metrics registry class for creating and maintaining a
 * collection of MetricsMutables, making writing metrics source easier.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsRegistry {
  private final Map<String, MutableMetric> metricsMap = Maps.newLinkedHashMap();
  private final Map<String, MetricsTag> tagsMap = Maps.newLinkedHashMap();
  private final MetricsInfo metricsInfo;

  /**
   * Construct the registry with a record name
   * @param name  of the record of the metrics
   */
  public MetricsRegistry(String name) {
    metricsInfo = Interns.info(name, name);
  }

  /**
   * Construct the registry with a metadata object
   * @param info  the info object for the metrics record/group
   */
  public MetricsRegistry(MetricsInfo info) {
    metricsInfo = info;
  }

  /**
   * @return the info object of the metrics registry
   */
  public MetricsInfo info() {
    return metricsInfo;
  }

  /**
   * Get a metric by name
   * @param name  of the metric
   * @return the metric object
   */
  public synchronized MutableMetric get(String name) {
    return metricsMap.get(name);
  }

  /**
   * Get a tag by name
   * @param name  of the tag
   * @return the tag object
   */
  public synchronized MetricsTag getTag(String name) {
    return tagsMap.get(name);
  }

  /**
   * Create a mutable integer counter
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new counter object
   */
  public MutableCounterInt newCounter(String name, String desc, int iVal) {
    return newCounter(Interns.info(name, desc), iVal);
  }

  /**
   * Create a mutable integer counter
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new counter object
   */
  public synchronized MutableCounterInt newCounter(MetricsInfo info, int iVal) {
    checkMetricName(info.name());
    MutableCounterInt ret = new MutableCounterInt(info, iVal);
    metricsMap.put(info.name(), ret);
    return ret;
  }

  /**
   * Create a mutable long integer counter
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new counter object
   */
  public MutableCounterLong newCounter(String name, String desc, long iVal) {
    return newCounter(Interns.info(name, desc), iVal);
  }

  /**
   * Create a mutable long integer counter
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new counter object
   */
  public synchronized
  MutableCounterLong newCounter(MetricsInfo info, long iVal) {
    checkMetricName(info.name());
    MutableCounterLong ret = new MutableCounterLong(info, iVal);
    metricsMap.put(info.name(), ret);
    return ret;
  }

  /**
   * Create a mutable integer gauge
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new gauge object
   */
  public MutableGaugeInt newGauge(String name, String desc, int iVal) {
    return newGauge(Interns.info(name, desc), iVal);
  }

  /**
   * Create a mutable integer gauge
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new gauge object
   */
  public synchronized MutableGaugeInt newGauge(MetricsInfo info, int iVal) {
    checkMetricName(info.name());
    MutableGaugeInt ret = new MutableGaugeInt(info, iVal);
    metricsMap.put(info.name(), ret);
    return ret;
  }

  /**
   * Create a mutable long integer gauge
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new gauge object
   */
  public MutableGaugeLong newGauge(String name, String desc, long iVal) {
    return newGauge(Interns.info(name, desc), iVal);
  }

  /**
   * Create a mutable long integer gauge
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new gauge object
   */
  public synchronized MutableGaugeLong newGauge(MetricsInfo info, long iVal) {
    checkMetricName(info.name());
    MutableGaugeLong ret = new MutableGaugeLong(info, iVal);
    metricsMap.put(info.name(), ret);
    return ret;
  }

  /**
   * Create a mutable float gauge
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new gauge object
   */
  public MutableGaugeFloat newGauge(String name, String desc, float iVal) {
    return newGauge(Interns.info(name, desc), iVal);
  }

  /**
   * Create a mutable float gauge
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new gauge object
   */
  public synchronized MutableGaugeFloat newGauge(MetricsInfo info, float iVal) {
    checkMetricName(info.name());
    MutableGaugeFloat ret = new MutableGaugeFloat(info, iVal);
    metricsMap.put(info.name(), ret);
    return ret;
  }

  /**
   * Create a mutable metric that estimates quantiles of a stream of values
   * @param name of the metric
   * @param desc metric description
   * @param sampleName of the metric (e.g., "Ops")
   * @param valueName of the metric (e.g., "Time" or "Latency")
   * @param interval rollover interval of estimator in seconds
   * @return a new quantile estimator object
   * @throws MetricsException if interval is not a positive integer
   */
  public synchronized MutableQuantiles newQuantiles(String name, String desc,
      String sampleName, String valueName, int interval) {
    checkMetricName(name);
    if (interval <= 0) {
      throw new MetricsException("Interval should be positive.  Value passed" +
          " is: " + interval);
    }
    MutableQuantiles ret =
        new MutableQuantiles(name, desc, sampleName, valueName, interval);
    metricsMap.put(name, ret);
    return ret;
  }

  /**
   * Create a mutable metric with stats
   * @param name  of the metric
   * @param desc  metric description
   * @param sampleName  of the metric (e.g., "Ops")
   * @param valueName   of the metric (e.g., "Time" or "Latency")
   * @param extended    produce extended stat (stdev, min/max etc.) if true.
   * @return a new mutable stat metric object
   */
  public synchronized MutableStat newStat(String name, String desc,
      String sampleName, String valueName, boolean extended) {
    checkMetricName(name);
    MutableStat ret =
        new MutableStat(name, desc, sampleName, valueName, extended);
    metricsMap.put(name, ret);
    return ret;
  }

  /**
   * Create a mutable metric with stats
   * @param name  of the metric
   * @param desc  metric description
   * @param sampleName  of the metric (e.g., "Ops")
   * @param valueName   of the metric (e.g., "Time" or "Latency")
   * @return a new mutable metric object
   */
  public MutableStat newStat(String name, String desc,
                             String sampleName, String valueName) {
    return newStat(name, desc, sampleName, valueName, false);
  }

  /**
   * Create a mutable rate metric
   * @param name  of the metric
   * @return a new mutable metric object
   */
  public MutableRate newRate(String name) {
    return newRate(name, name, false);
  }

  /**
   * Create a mutable rate metric
   * @param name  of the metric
   * @param description of the metric
   * @return a new mutable rate metric object
   */
  public MutableRate newRate(String name, String description) {
    return newRate(name, description, false);
  }

  /**
   * Create a mutable rate metric (for throughput measurement)
   * @param name  of the metric
   * @param desc  description
   * @param extended  produce extended stat (stdev/min/max etc.) if true
   * @return a new mutable rate metric object
   */
  public MutableRate newRate(String name, String desc, boolean extended) {
    return newRate(name, desc, extended, true);
  }

  @InterfaceAudience.Private
  public synchronized MutableRate newRate(String name, String desc,
      boolean extended, boolean returnExisting) {
    if (returnExisting) {
      MutableMetric rate = metricsMap.get(name);
      if (rate != null) {
        if (rate instanceof MutableRate) return (MutableRate) rate;
        throw new MetricsException("Unexpected metrics type "+ rate.getClass()
                                   +" for "+ name);
      }
    }
    checkMetricName(name);
    MutableRate ret = new MutableRate(name, desc, extended);
    metricsMap.put(name, ret);
    return ret;
  }

  public synchronized MutableRatesWithAggregation newRatesWithAggregation(
      String name) {
    checkMetricName(name);
    MutableRatesWithAggregation rates = new MutableRatesWithAggregation();
    metricsMap.put(name, rates);
    return rates;
  }

  public synchronized MutableRollingAverages newMutableRollingAverages(
      String name, String valueName) {
    checkMetricName(name);
    MutableRollingAverages rollingAverages =
        new MutableRollingAverages(valueName);
    metricsMap.put(name, rollingAverages);
    return rollingAverages;
  }

  synchronized void add(String name, MutableMetric metric) {
    checkMetricName(name);
    metricsMap.put(name, metric);
  }

  /**
   * Add sample to a stat metric by name.
   * @param name  of the metric
   * @param value of the snapshot to add
   */
  public synchronized void add(String name, long value) {
    MutableMetric m = metricsMap.get(name);

    if (m != null) {
      if (m instanceof MutableStat) {
        ((MutableStat) m).add(value);
      }
      else {
        throw new MetricsException("Unsupported add(value) for metric "+ name);
      }
    }
    else {
      metricsMap.put(name, newRate(name)); // default is a rate metric
      add(name, value);
    }
  }

  /**
   * Set the metrics context tag
   * @param name of the context
   * @return the registry itself as a convenience
   */
  public MetricsRegistry setContext(String name) {
    return tag(MsInfo.Context, name, true);
  }

  /**
   * Add a tag to the metrics
   * @param name  of the tag
   * @param description of the tag
   * @param value of the tag
   * @return the registry (for keep adding tags)
   */
  public MetricsRegistry tag(String name, String description, String value) {
    return tag(name, description, value, false);
  }

  /**
   * Add a tag to the metrics
   * @param name  of the tag
   * @param description of the tag
   * @param value of the tag
   * @param override  existing tag if true
   * @return the registry (for keep adding tags)
   */
  public MetricsRegistry tag(String name, String description, String value,
                             boolean override) {
    return tag(Interns.info(name, description), value, override);
  }

  /**
   * Add a tag to the metrics
   * @param info  metadata of the tag
   * @param value of the tag
   * @param override existing tag if true
   * @return the registry (for keep adding tags etc.)
   */
  public synchronized
  MetricsRegistry tag(MetricsInfo info, String value, boolean override) {
    if (!override) checkTagName(info.name());
    tagsMap.put(info.name(), Interns.tag(info, value));
    return this;
  }

  public MetricsRegistry tag(MetricsInfo info, String value) {
    return tag(info, value, false);
  }

  Collection<MetricsTag> tags() {
    return tagsMap.values();
  }

  Collection<MutableMetric> metrics() {
    return metricsMap.values();
  }

  private void checkMetricName(String name) {
    // Check for invalid characters in metric name
    boolean foundWhitespace = false;
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (Character.isWhitespace(c)) {
        foundWhitespace = true;
        break;
      }
    }
    if (foundWhitespace) {
      throw new MetricsException("Metric name '"+ name +
          "' contains illegal whitespace character");
    }
    // Check if name has already been registered
    if (metricsMap.containsKey(name)) {
      throw new MetricsException("Metric name "+ name +" already exists!");
    }
  }

  private void checkTagName(String name) {
    if (tagsMap.containsKey(name)) {
      throw new MetricsException("Tag "+ name +" already exists!");
    }
  }

  /**
   * Sample all the mutable metrics and put the snapshot in the builder
   * @param builder to contain the metrics snapshot
   * @param all get all the metrics even if the values are not changed.
   */
  public synchronized void snapshot(MetricsRecordBuilder builder, boolean all) {
    for (MetricsTag tag : tags()) {
      builder.add(tag);
    }
    for (MutableMetric metric : metrics()) {
      metric.snapshot(builder, all);
    }
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", this.getClass().getSimpleName() + "{", "}")
        .add("info=" + metricsInfo.toString())
        .add("tags=" + tags())
        .add("metrics=" + metrics())
        .toString();
  }

}
