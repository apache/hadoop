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
import java.util.HashMap;
import java.util.Map;
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
  private Map<String, String> metricLines = new HashMap<>();

  private static final Pattern SPLIT_PATTERN =
      Pattern.compile("(?<!(^|[A-Z_]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])");

  public PrometheusMetricsSink() {
  }

  @Override
  public void putMetrics(MetricsRecord metricsRecord) {
    for (AbstractMetric metrics : metricsRecord.metrics()) {
      if (metrics.type() == MetricType.COUNTER
          || metrics.type() == MetricType.GAUGE) {

        String key = prometheusName(
            metricsRecord.name(), metrics.name());

        StringBuilder builder = new StringBuilder();
        builder.append("# TYPE " + key + " " +
            metrics.type().toString().toLowerCase() + "\n");
        builder.append(key + "{");
        String sep = "";

        //add tags
        for (MetricsTag tag : metricsRecord.tags()) {
          String tagName = tag.name().toLowerCase();

          //ignore specific tag which includes sub-hierarchy
          if (!tagName.equals("numopenconnectionsperuser")) {
            builder.append(
                sep + tagName + "=\"" + tag.value() + "\"");
            sep = ",";
          }
        }
        builder.append("} ");
        builder.append(metrics.value());
        metricLines.put(key, builder.toString());

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
    baseName = baseName.replace('-', '_');
    String[] parts = SPLIT_PATTERN.split(baseName);
    return String.join("_", parts).toLowerCase();
  }

  @Override
  public void flush() {

  }

  @Override
  public void init(SubsetConfiguration subsetConfiguration) {

  }

  public void writeMetrics(Writer writer) throws IOException {
    for (String line : metricLines.values()) {
      writer.write(line + "\n");
    }
  }
}
