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
package org.apache.hadoop.hdds.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.commons.codec.CharEncoding.UTF_8;

/**
 * Test prometheus Sink.
 */
public class TestPrometheusMetricsSink {

  @Test
  public void testPublish() throws IOException {
    //GIVEN
    MetricsSystem metrics = DefaultMetricsSystem.instance();

    metrics.init("test");
    PrometheusMetricsSink sink = new PrometheusMetricsSink();
    metrics.register("Prometheus", "Prometheus", sink);
    TestMetrics testMetrics = metrics
        .register("TestMetrics", "Testing metrics", new TestMetrics());

    metrics.start();
    testMetrics.numBucketCreateFails.incr();
    metrics.publishMetricsNow();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(stream, UTF_8);

    //WHEN
    sink.writeMetrics(writer);
    writer.flush();

    //THEN
    System.out.println(stream.toString(UTF_8));
    Assert.assertTrue(
        "The expected metric line is missing from prometheus metrics output",
        stream.toString(UTF_8).contains(
            "test_metrics_num_bucket_create_fails{context=\"dfs\"")
    );

    metrics.stop();
    metrics.shutdown();
  }

  @Test
  public void testNaming() throws IOException {
    PrometheusMetricsSink sink = new PrometheusMetricsSink();

    Assert.assertEquals("rpc_time_some_metrics",
        sink.prometheusName("RpcTime", "SomeMetrics"));

    Assert.assertEquals("om_rpc_time_om_info_keys",
        sink.prometheusName("OMRpcTime", "OMInfoKeys"));

    Assert.assertEquals("rpc_time_small",
        sink.prometheusName("RpcTime", "small"));
  }

  /**
   * Example metric pojo.
   */
  @Metrics(about = "Test Metrics", context = "dfs")
  public static class TestMetrics {

    @Metric
    private MutableCounterLong numBucketCreateFails;
  }
}