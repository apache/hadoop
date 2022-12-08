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

package org.apache.hadoop.fs.s3a;

import java.net.URI;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.impl.WeakRefMetricsSource;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.S3AInstrumentation.getMetricsSystem;
import static org.apache.hadoop.fs.s3a.Statistic.DIRECTORIES_CREATED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the {@link S3AInstrumentation} lifecycle,  in particular how
 * it binds to hadoop metrics through a {@link WeakRefMetricsSource}
 * and that it will deregister itself in {@link S3AInstrumentation#close()}.
 */
public class TestInstrumentationLifecycle extends AbstractHadoopTestBase {

  @Test
  public void testDoubleClose() throws Throwable {
    S3AInstrumentation instrumentation = new S3AInstrumentation(new URI("s3a://example/"));

    // the metric system is created in the constructor
    assertThat(S3AInstrumentation.hasMetricSystem())
        .describedAs("S3AInstrumentation.hasMetricSystem()")
        .isTrue();
    // ask for a metric
    String metricName = DIRECTORIES_CREATED.getSymbol();
    assertThat(instrumentation.lookupMetric(metricName))
        .describedAs("lookupMetric(%s) while open", metricName)
        .isNotNull();

    MetricsSystem activeMetrics = getMetricsSystem();
    final String metricSourceName = instrumentation.getMetricSourceName();
    final MetricsSource source = activeMetrics.getSource(metricSourceName);
    // verify the source is registered through a weak ref, and that the
    // reference maps to the instance.
    Assertions.assertThat(source)
        .describedAs("metric source %s", metricSourceName)
        .isNotNull()
        .isInstanceOf(WeakRefMetricsSource.class)
        .extracting(m -> ((WeakRefMetricsSource) m).getSource())
        .isSameAs(instrumentation);

    // this will close the metrics system
    instrumentation.close();

    // iostats is still valid
    assertThat(instrumentation.getIOStatistics())
        .describedAs("iostats of %s", instrumentation)
        .isNotNull();

    // no metrics
    assertThat(S3AInstrumentation.hasMetricSystem())
        .describedAs("S3AInstrumentation.hasMetricSystem()")
        .isFalse();

    // metric lookup still works, so any invocation of an s3a
    // method which still updates a metric also works
    assertThat(instrumentation.lookupMetric(metricName))
        .describedAs("lookupMetric(%s) when closed", metricName)
        .isNotNull();

    // which we can implicitly verify by asking for it and
    // verifying that we get given a different one back
    // from the demand-created instance
    MetricsSystem metrics2 = getMetricsSystem();
    assertThat(metrics2)
        .describedAs("metric system 2")
        .isNotSameAs(activeMetrics);

    // this is going to be a no-op
    instrumentation.close();

    // which we can verify because the metrics system doesn't
    // get closed this time
    assertThat(getMetricsSystem())
        .describedAs("metric system 3")
        .isSameAs(metrics2);
  }
}
