/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.metrics2.sink.prometheus;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.metrics2.MetricType.COUNTER;
import static org.apache.hadoop.metrics2.MetricType.GAUGE;

/**
 * A metrics sink that writes to a Prometheus PushGateWay.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PushGatewaySink implements MetricsSink, Closeable {
  private static final Logger LOG =
      LoggerFactory.getLogger(PushGatewaySink.class);

  private static final String JOB_NAME = "job";
  private static final String HOST_KEY = "host";
  private static final String PORT_KEY = "port";
  private static final String GROUP_KEY = "groupingKey";

  private static final Pattern SPLIT_PATTERN =
      Pattern.compile("(?<!(^|[A-Z_]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])");

  private static final String NUM_OPEN_CONNECTION_SPERUSER = "numopenconnectionsperuser";

  private static final String NULL = "null";

  private Map<String, String> groupingKey;
  private PushGateway pg = null;
  private String jobName;

  @Override
  public void init(SubsetConfiguration conf) {
    // Get PushGateWay host configurations.
    jobName = conf.getString(JOB_NAME, "hadoop-job");
    final String serverHost = conf.getString(HOST_KEY);
    final int serverPort = Integer.parseInt(conf.getString(PORT_KEY));

    if (serverHost == null || serverHost.isEmpty() || serverPort < 1) {
      throw new MetricsException(
          "Invalid host/port configuration. Host: " + serverHost + " Port: " + serverPort);
    }

    groupingKey = parseGroupingKey(conf.getString(GROUP_KEY, ""));
    pg = new PushGateway(serverHost + ':' + serverPort);
  }

  @Override
  public void putMetrics(MetricsRecord metricsRecord) {
    try {
      CollectorRegistry registry = new CollectorRegistry();
      for (AbstractMetric metrics : metricsRecord.metrics()) {
        if (metrics.type() == COUNTER
            || metrics.type() == GAUGE) {

          String key = getMetricsName(
              metricsRecord.name(), metrics.name()).replace(" ", "");

          int tagSize = metricsRecord.tags().size();
          String[] labelNames = new String[tagSize];
          String[] labelValues = new String[tagSize];
          int index = 0;
          for (MetricsTag tag : metricsRecord.tags()) {
            String tagName = tag.name().toLowerCase();

            //ignore specific tag which includes sub-hierarchy
            if (NUM_OPEN_CONNECTION_SPERUSER.equals(tagName)) {
              continue;
            }
            labelNames[index] = tagName;
            labelValues[index] =
                tag.value() == null ? NULL : tag.value();
            index++;
          }

          switch (metrics.type()) {
          case GAUGE:
            Gauge.build(key, key)
                .labelNames(labelNames)
                .register(registry)
                .labels(labelValues)
                .set(metrics.value().doubleValue());
            break;
          case COUNTER:
            Counter.build(key, key)
                .labelNames(labelNames)
                .register(registry)
                .labels(labelValues)
                .inc(metrics.value().doubleValue());
            break;
          default:
            break;
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("register succeed, metrics name is: {} Type is :{} tag is: {} tagValue is {} Value is : {}"
                , key, metrics.type(), Arrays.toString(labelNames), Arrays.toString(labelValues), metrics.value());
          }
        }
      }
      pg.push(registry, jobName, groupingKey);
    } catch (Exception e) {
      LOG.error("pushing job's metrics to gateway is failed ", e);
    }
  }

  /**
   * Convert CamelCase based names to lower-case names where the separator is the underscore, to
   * follow prometheus naming conventions.
   */
  public String getMetricsName(String recordName,
      String metricName) {
    String baseName = StringUtils.capitalize(recordName)
        + StringUtils.capitalize(metricName);
    baseName = baseName.replace('-', '_');
    String[] parts = SPLIT_PATTERN.split(baseName);
    return String.join("_", parts).toLowerCase();
  }

  Map<String, String> parseGroupingKey(final String groupingKeyConfig) {
    if (!groupingKeyConfig.isEmpty()) {
      Map<String, String> groupingKey = new HashMap<>();
      String[] kvs = groupingKeyConfig.split(";");
      for (String kv : kvs) {
        int idx = kv.indexOf("=");
        if (idx < 0) {
          LOG.warn("Invalid prometheusPushGateway groupingKey:{}, will be ignored", kv);
          continue;
        }

        String labelKey = kv.substring(0, idx);
        String labelValue = kv.substring(idx + 1);
        if (StringUtils.isEmpty(labelKey) || StringUtils.isEmpty(labelValue)) {
          LOG.warn("Invalid groupingKey labelKey:{}, labelValue:{} must not be empty", labelKey, labelValue);
          continue;
        }
        groupingKey.put(labelKey, labelValue);
      }
      return groupingKey;
    }
    return Collections.emptyMap();
  }

  @Override
  public void flush() {

  }

  @Override
  public void close() throws IOException {
  }
}
