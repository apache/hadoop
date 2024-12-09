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

package org.apache.hadoop.fs.azurebfs.services;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_SUCCEEDED;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.TOTAL_NUMBER_OF_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.enums.RetryValue.ONE;
import static org.apache.hadoop.fs.azurebfs.enums.RetryValue.THREE;
import static org.apache.hadoop.fs.azurebfs.enums.RetryValue.TWO;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_COUNTER;
import static org.apache.hadoop.fs.azurebfs.enums.StatisticTypeEnum.TYPE_GAUGE;

public class TestAbfsBackoffMetrics {
    private AbfsBackoffMetrics metrics;
    private static final int TOTAL_COUNTERS = 22;
    private static final int TOTAL_GAUGES = 21;

    /**
     * Sets up the test environment by initializing the AbfsBackoffMetrics instance.
     */
    @Before
    public void setUp() {
        metrics = new AbfsBackoffMetrics();
    }

    /**
     * Tests the retrieval of metric names based on the statistic type.
     */
    @Test
    public void retrievesMetricNamesBasedOnStatisticType() {
        String[] counterMetrics = metrics.getMetricNamesByType(TYPE_COUNTER);
        String[] gaugeMetrics = metrics.getMetricNamesByType(TYPE_GAUGE);
        Assertions.assertThat(counterMetrics.length)
                .describedAs("Counter metrics should have 22 elements")
                .isEqualTo(TOTAL_COUNTERS);
        Assertions.assertThat(gaugeMetrics.length)
                .describedAs("Gauge metrics should have 21 elements")
                .isEqualTo(TOTAL_GAUGES);
    }

    /**
     * Tests the retrieval of the value of a specific metric.
     */
    @Test
    public void retrievesValueOfSpecificMetric() {
        metrics.setMetricValue(NUMBER_OF_REQUESTS_SUCCEEDED, 5, ONE);
        Assertions.assertThat(metrics.getMetricValue(NUMBER_OF_REQUESTS_SUCCEEDED, ONE))
                .describedAs("Number of request succeeded for retry 1 should be 5")
                .isEqualTo(5);
        Assertions.assertThat(metrics.getMetricValue(NUMBER_OF_REQUESTS_SUCCEEDED, TWO))
                .describedAs("Number of request succeeded for other retries except 1 should be 0")
                .isEqualTo(0);
    }

    /**
     * Tests the increment of the value of a specific metric.
     */
    @Test
    public void incrementsValueOfSpecificMetric() {
        metrics.incrementMetricValue(NUMBER_OF_REQUESTS_SUCCEEDED, ONE);
        Assertions.assertThat(metrics.getMetricValue(NUMBER_OF_REQUESTS_SUCCEEDED, ONE))
                .describedAs("Number of request succeeded for retry 1 should be 1")
                .isEqualTo(1);
        Assertions.assertThat(metrics.getMetricValue(NUMBER_OF_REQUESTS_SUCCEEDED, THREE))
                .describedAs("Number of request succeeded for other retries except 1 should be 0")
                .isEqualTo(0);
    }

    /**
     * Tests the string representation of empty backoff metrics.
     */
    @Test
    public void returnsStringRepresentationOfEmptyBackoffMetrics() {
        Assertions.assertThat(metrics.getMetricValue(TOTAL_NUMBER_OF_REQUESTS))
                .describedAs("String representation of backoff metrics should be empty")
                .isEqualTo(0);
        Assertions.assertThat(metrics.toString())
                .describedAs("String representation of backoff metrics should be empty")
                .isEmpty();
    }

    /**
     * Tests the string representation of backoff metrics.
     */
    @Test
    public void returnsStringRepresentationOfBackoffMetrics() {
        metrics.incrementMetricValue(TOTAL_NUMBER_OF_REQUESTS);
        Assertions.assertThat(metrics.getMetricValue(TOTAL_NUMBER_OF_REQUESTS))
                .describedAs("String representation of backoff metrics should not be empty")
                .isEqualTo(1);
        Assertions.assertThat(metrics.toString())
                .describedAs("String representation of backoff metrics should not be empty")
                .contains("$TR=1");
    }
}
