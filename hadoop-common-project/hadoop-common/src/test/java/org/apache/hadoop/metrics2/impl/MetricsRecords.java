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

package org.apache.hadoop.metrics2.impl;

import java.util.function.Predicate;
import java.util.stream.StreamSupport;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Utility class mainly for tests
 */
public class MetricsRecords {

  public static void assertTag(MetricsRecord record, String tagName,
      String expectedValue) {
    MetricsTag processIdTag = getFirstTagByName(record,
        tagName);
    assertNotNull(processIdTag);
    assertEquals(expectedValue, processIdTag.value());
  }

  public static void assertMetric(MetricsRecord record,
      String metricName,
      Number expectedValue) {
    AbstractMetric resourceLimitMetric = getFirstMetricByName(
        record, metricName);
    assertNotNull(resourceLimitMetric);
    assertEquals(expectedValue, resourceLimitMetric.value());
  }

  public static Number getMetricValueByName(MetricsRecord record,
      String metricName) {
    AbstractMetric resourceLimitMetric = getFirstMetricByName(
        record, metricName);
    assertNotNull(resourceLimitMetric);
    return resourceLimitMetric.value();
  }

  public static void assertMetricNotNull(MetricsRecord record,
      String metricName) {
    AbstractMetric resourceLimitMetric = getFirstMetricByName(
        record, metricName);
    assertNotNull("Metric " + metricName + " doesn't exist",
        resourceLimitMetric);
  }

  private static MetricsTag getFirstTagByName(MetricsRecord record,
      String name) {
    if (record.tags() == null) {
      return null;
    }
    return record.tags().stream().filter(
        new MetricsTagPredicate(name)).findFirst().orElse(null);
  }

  private static AbstractMetric getFirstMetricByName(
      MetricsRecord record, String name) {
    if (record.metrics() == null) {
      return null;
    }
    return StreamSupport.stream(record.metrics().spliterator(), false)
        .filter(new AbstractMetricPredicate(name)).findFirst().orElse(null);
  }

  private static class MetricsTagPredicate implements Predicate<MetricsTag> {
    private String tagName;

    public MetricsTagPredicate(String tagName) {

      this.tagName = tagName;
    }

    @Override
    public boolean test(MetricsTag input) {
      return input.name().equals(tagName);
    }
  }

  private static class AbstractMetricPredicate
      implements Predicate<AbstractMetric> {
    private String metricName;

    public AbstractMetricPredicate(
        String metricName) {
      this.metricName = metricName;
    }

    @Override
    public boolean test(AbstractMetric input) {
      return input.name().equals(metricName);
    }
  }
}
