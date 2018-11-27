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
package org.apache.hadoop.hdds.server;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

import org.apache.commons.configuration2.SubsetConfiguration;

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

  private static final Pattern UPPER_CASE_SEQ =
      Pattern.compile("([A-Z]*)([A-Z])");

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
   * Convert CamelCase based namess to lower-case names where the separator
   * is the underscore, to follow prometheus naming conventions.
   */
  public String prometheusName(String recordName,
      String metricName) {
    String baseName = upperFirst(recordName) + upperFirst(metricName);
    Matcher m = UPPER_CASE_SEQ.matcher(baseName);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      String replacement = "_" + m.group(2).toLowerCase();
      if (m.group(1).length() > 0) {
        replacement = "_" + m.group(1).toLowerCase() + replacement;
      }
      m.appendReplacement(sb, replacement);
    }
    m.appendTail(sb);

    //always prefixed with "_"
    return sb.toString().substring(1);
  }

  private String upperFirst(String name) {
    if (Character.isLowerCase(name.charAt(0))) {
      return Character.toUpperCase(name.charAt(0)) + name.substring(1);
    } else {
      return name;
    }

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
