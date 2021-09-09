/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.metrics2.sink;

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * Metrics sink for prometheus exporter.
 * <p>
 * Stores the metric data in-memory and return with it on request.
 */
public class PrometheusMetricsSink implements MetricsSink {

  /**
   * Cached output lines for each metrics.
   */
  private Map<String, Map<Collection<MetricsTag>, AbstractMetric>> promMetrics =
      new ConcurrentHashMap<>();
  private Map<String, Map<Collection<MetricsTag>, AbstractMetric>> nextPromMetrics =
      new ConcurrentHashMap<>();

  private static final Pattern SPLIT_PATTERN =
      Pattern.compile("(?<!(^|[A-Z_]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])");
  private static final Pattern DELIMITERS = Pattern.compile("[^a-zA-Z0-9]+");

  public PrometheusMetricsSink() {
  }

  @Override
  public void putMetrics(MetricsRecord metricsRecord) {
    for (AbstractMetric metric : metricsRecord.metrics()) {
      if (metric.type() == MetricType.COUNTER
          || metric.type() == MetricType.GAUGE) {

        String key = prometheusName(
            metricsRecord.name(), metric.name());

        nextPromMetrics.computeIfAbsent(key,
            any -> new ConcurrentHashMap<>())
            .put(metricsRecord.tags(), metric);
      }
    }
  }

  /**
   * Convert CamelCase based names to lower-case names where the separator
   * is the underscore, to follow prometheus naming conventions.
   */
  public String prometheusName(String recordName,
                               String metricName) {
    String baseName = StringUtils.capitalize(recordName)
        + StringUtils.capitalize(metricName);
    String[] parts = SPLIT_PATTERN.split(baseName);
    String joined =  String.join("_", parts).toLowerCase();
    return DELIMITERS.matcher(joined).replaceAll("_");
  }

  @Override
  public void flush() {
    promMetrics = nextPromMetrics;
    nextPromMetrics = new ConcurrentHashMap<>();
  }

  @Override
  public void init(SubsetConfiguration conf) {
  }

  public void writeMetrics(Writer writer) throws IOException {
    for (Map.Entry<String, Map<Collection<MetricsTag>, AbstractMetric>> promMetric :
        promMetrics.entrySet()) {
      AbstractMetric firstMetric = promMetric.getValue().values().iterator().next();

      StringBuilder builder = new StringBuilder();
      builder.append("# HELP ")
          .append(promMetric.getKey())
          .append(" ")
          .append(firstMetric.description())
          .append("\n")
          .append("# TYPE ")
          .append(promMetric.getKey())
          .append(" ")
          .append(firstMetric.type().toString().toLowerCase())
          .append("\n");

      for (Map.Entry<Collection<MetricsTag>, AbstractMetric> metric :
          promMetric.getValue().entrySet()) {
        builder.append(promMetric.getKey())
            .append("{");

        String sep = "";
        for (MetricsTag tag : metric.getKey()) {
          String tagName = tag.name().toLowerCase();

          if (!tagName.equals("numopenconnectionsperuser")) {
            builder.append(sep)
                .append(tagName)
                .append("=\"")
                .append(tag.value())
                .append("\"");
            sep = ",";
          }
        }
        builder.append("} ");
        builder.append(metric.getValue().value());
        builder.append("\n");
      }

      writer.write(builder.toString());
    }
  }
}
