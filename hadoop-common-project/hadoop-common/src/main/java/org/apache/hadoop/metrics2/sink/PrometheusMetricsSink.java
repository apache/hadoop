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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
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

  private static final Pattern NN_TOPMETRICS_PATTERN =
      Pattern.compile(
          "^(nn_top_user_op_counts_window_ms_\\d+)_op_.*?(total_count|count)$");
  private static final Pattern NN_TOPMETRICS_TAGS_PATTERN =
      Pattern
          .compile("^op=(?<op>\\w+)(.user=(?<user>.*)|)\\.(TotalCount|count)$");

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
    List<String> extendMetricsTags = new ArrayList<>();
    for (Map.Entry<String, Map<Collection<MetricsTag>, AbstractMetric>> promMetric :
        promMetrics.entrySet()) {
      AbstractMetric firstMetric = promMetric.getValue().values().iterator().next();
      String metricKey = getMetricKey(promMetric.getKey(), firstMetric,
          extendMetricsTags);

      StringBuilder builder = new StringBuilder();
      builder.append("# HELP ")
          .append(metricKey)
          .append(" ")
          .append(firstMetric.description())
          .append("\n")
          .append("# TYPE ")
          .append(metricKey)
          .append(" ")
          .append(firstMetric.type().toString().toLowerCase())
          .append("\n");

      for (Map.Entry<Collection<MetricsTag>, AbstractMetric> metric :
          promMetric.getValue().entrySet()) {
        builder.append(metricKey)
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
        if (!extendMetricsTags.isEmpty()) {
          //add extend tags
          for (String tagStr : extendMetricsTags) {
            builder.append(sep).append(tagStr);
          }
          extendMetricsTags.clear();
        }
        builder.append("} ");
        builder.append(metric.getValue().value());
        builder.append("\n");
      }

      writer.write(builder.toString());
    }
  }

  private String getMetricKey(String promMetricKey, AbstractMetric metric,
      List<String> extendTags) {
    Matcher matcher = NN_TOPMETRICS_PATTERN.matcher(promMetricKey);
    if (matcher.find() && matcher.groupCount() == 2) {
      extendTags.addAll(parseTopMetricsTags(metric.name()));
      return String.format("%s_%s",
          matcher.group(1), matcher.group(2));
    }
    return promMetricKey;
  }

  /**
   * Parse Custom tags for TopMetrics.
   *
   * @param metricName metricName
   * @return Tags for TopMetrics
   */
  private List<String> parseTopMetricsTags(String metricName) {
    List<String> topMetricsTags = new ArrayList<>();
    Matcher matcher = NN_TOPMETRICS_TAGS_PATTERN.matcher(metricName);
    if (matcher.find()) {
      String op = matcher.group("op");
      String user = matcher.group("user");
      // add tag op = "$op"
      topMetricsTags.add(String
          .format("op=\"%s\"", op));
      if (StringUtils.isNoneEmpty(user)) {
        // add tag user = "$user"
        topMetricsTags.add(String
            .format("user=\"%s\"", user));
      }
    }
    return topMetricsTags;
  }
}
