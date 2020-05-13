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
package org.apache.hadoop.metrics2.sink;

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

import static java.nio.charset.StandardCharsets.UTF_8;

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
    String writtenMetrics = stream.toString(UTF_8.name());
    System.out.println(writtenMetrics);
    Assert.assertTrue(
        "The expected metric line is missing from prometheus metrics output",
        writtenMetrics.contains(
            "test_metrics_num_bucket_create_fails{context=\"dfs\"")
    );

    metrics.stop();
    metrics.shutdown();
  }

  @Test
  public void testNamingCamelCase() {
    PrometheusMetricsSink sink = new PrometheusMetricsSink();

    Assert.assertEquals("rpc_time_some_metrics",
        sink.prometheusName("RpcTime", "SomeMetrics"));

    Assert.assertEquals("om_rpc_time_om_info_keys",
        sink.prometheusName("OMRpcTime", "OMInfoKeys"));

    Assert.assertEquals("rpc_time_small",
        sink.prometheusName("RpcTime", "small"));
  }

  @Test
  public void testNamingPipeline() {
    PrometheusMetricsSink sink = new PrometheusMetricsSink();

    String recordName = "SCMPipelineMetrics";
    String metricName = "NumBlocksAllocated-"
        + "RATIS-THREE-47659e3d-40c9-43b3-9792-4982fc279aba";
    Assert.assertEquals(
        "scm_pipeline_metrics_"
            + "num_blocks_allocated_"
            + "ratis_three_47659e3d_40c9_43b3_9792_4982fc279aba",
        sink.prometheusName(recordName, metricName));
  }

  @Test
  public void testNamingPeriods() {
    PrometheusMetricsSink sink = new PrometheusMetricsSink();

    String recordName = "org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl";
    String metricName = "DfsUsed";
    Assert.assertEquals(
        "org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_dfs_used",
        sink.prometheusName(recordName, metricName));
  }

  @Test
  public void testNamingWhitespaces() {
    PrometheusMetricsSink sink = new PrometheusMetricsSink();

    String recordName = "JvmMetrics";
    String metricName = "GcCount" + "G1 Old Generation";
    Assert.assertEquals(
        "jvm_metrics_gc_count_g1_old_generation",
        sink.prometheusName(recordName, metricName));
  }

  /**
   * Example metric pojo.
   */
  @Metrics(about = "Test Metrics", context = "dfs")
  private static class TestMetrics {

    @Metric
    private MutableCounterLong numBucketCreateFails;
  }
}