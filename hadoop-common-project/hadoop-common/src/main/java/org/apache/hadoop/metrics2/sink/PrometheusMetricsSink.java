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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
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
  private final Map<String, String> metricLines = new ConcurrentHashMap<>();

  private static final Pattern SPLIT_PATTERN =
      Pattern.compile("(?<!(^|[A-Z_]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])");
  private static final Pattern DELIMITERS = Pattern.compile("[^a-zA-Z0-9]+");
  private static final Pattern OP = Pattern.compile("op=([a-zA-Z*]+)\\.");
  private static final Pattern USER = Pattern.compile("user=(.+)\\.count");

  public PrometheusMetricsSink() {
  }

  @Override
  public void putMetrics(MetricsRecord metricsRecord) {
    for (AbstractMetric metrics : metricsRecord.metrics()) {
      if (metrics.type() == MetricType.COUNTER
          || metrics.type() == MetricType.GAUGE) {
        final StringBuilder builder = new StringBuilder();
        String key = buildMetrics(builder, metricsRecord, metrics);
        metricLines.put(key, builder.toString());
      }
    }
  }

  private String buildMetrics(StringBuilder builder,
                              MetricsRecord metricsRecord,
                              AbstractMetric metrics) {
    boolean isNNTopMetrics = false;
    final String recordName = metricsRecord.name();

    String key;
    // Move window_ms, op, user from metrics name to metrics tag
    if (recordName.startsWith("NNTopUserOpCounts")) {
      isNNTopMetrics = true;
      key = "nn_top_user_op_counts";
    } else {
      key = prometheusName(recordName, metrics.name());
    }

    builder.append("# TYPE ")
        .append(key)
        .append(" ")
        .append(metrics.type().toString().toLowerCase())
        .append("\n")
        .append(key)
        .append("{");
    String sep = "";

    //add tags
    for (MetricsTag tag : metricsRecord.tags()) {
      String tagName = tag.name().toLowerCase();
      //ignore specific tag which includes sub-hierarchy
      if (tagName.equals("numopenconnectionsperuser")) {
        continue;
      }
      builder.append(sep)
          .append(tagName)
          .append("=\"")
          .append(tag.value())
          .append("\"");
      sep = ",";
    }

    if (isNNTopMetrics) {
      moveNNTopMetricsNameToLabels(builder, recordName, metrics.name());
    }

    builder.append("} ");
    builder.append(metrics.value());
    builder.append("\n");

    if (isNNTopMetrics) {
      return prometheusName(recordName, metrics.name());
    } else {
      return key;
    }
  }

  /**
   *  Publish window_ms, op, and user from metrics name as metrics labels
   *  for better support of Prometheus.
   */
  @VisibleForTesting
  void moveNNTopMetricsNameToLabels(
      StringBuilder builder, String recordName, String metricsName) {
    final char sep = ',';
    // Get window_ms
    final int pos = recordName.indexOf("=");
    final String window = recordName.substring(pos+1);
    builder.append(sep)
        .append("window_ms=\"")
        .append(window)
        .append('"');

    // Get op
    final Matcher opMatcher = OP.matcher(metricsName);
    if (opMatcher.find()) {
      final String op = opMatcher.group(1);
      builder.append(sep)
          .append("op=\"")
          .append(op)
          .append('"');
    }

    if (metricsName.endsWith(".count")) {
      // Get user
      final Matcher userMatcher = USER.matcher(metricsName);
      if (userMatcher.find()) {
        final String user = userMatcher.group(1);
        builder.append(sep)
            .append("user=\"")
            .append(user)
            .append('"');
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

  }

  @Override
  public void init(SubsetConfiguration subsetConfiguration) {

  }

  public void writeMetrics(Writer writer) throws IOException {
    for (String line : metricLines.values()) {
      writer.write(line);
    }
  }
}
