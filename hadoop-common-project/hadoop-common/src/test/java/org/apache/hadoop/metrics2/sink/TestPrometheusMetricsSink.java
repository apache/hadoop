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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.annotation.Metric.Type;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test prometheus Sink.
 */
public class TestPrometheusMetricsSink {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestPrometheusMetricsSink.class);

  /**
   * Shared {@code DefaultMetricsSystem} singleton, set up fresh per test.
   * Each test does exactly one balanced {@code init}/{@code shutdown} so the
   * system (and the JVM-global source-name registry it owns) is fully reset
   * between tests. This keeps tests isolated even when an assertion fails
   * before the test body finishes, avoiding flaky failures such as
   * "Metrics source TestMetrics already exists!" on later tests or reruns.
   */
  private MetricsSystem metrics;

  @BeforeEach
  public void setUp() {
    metrics = DefaultMetricsSystem.instance();
    metrics.init("test");
  }

  @AfterEach
  public void tearDown() {
    // DefaultMetricsSystem.shutdown() shuts down the underlying metrics system
    // and, once its refCount reaches 0, clears the JVM-global source-name and
    // MBean-name registries. Running it in @AfterEach guarantees cleanup even
    // when a test assertion fails before reaching the end of the test body.
    DefaultMetricsSystem.shutdown();
  }

  @Test
  public void testPublish() throws IOException {
    //GIVEN
    PrometheusMetricsSink sink = new PrometheusMetricsSink();
    metrics.register("Prometheus", "Prometheus", sink);
    TestMetrics testMetrics = metrics
        .register("TestMetrics", "Testing metrics", new TestMetrics());

    testMetrics.numBucketCreateFails.incr();
    metrics.publishMetricsNow();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(stream, UTF_8);

    //WHEN
    sink.writeMetrics(writer);
    writer.flush();

    //THEN
    String writtenMetrics = stream.toString(UTF_8);
    LOG.debug(writtenMetrics);
    assertTrue(writtenMetrics.contains("test_metrics_num_bucket_create_fails{context=\"dfs\""),
        "The expected metric line is missing from prometheus metrics output");
  }

  /**
   * Fix for HADOOP-17804, make sure Prometheus metrics get deduped based on metric
   * and tags, not just the metric.
   */
  @Test
  public void testPublishMultiple() throws IOException {
    //GIVEN
    PrometheusMetricsSink sink = new PrometheusMetricsSink();
    metrics.register("Prometheus", "Prometheus", sink);
    TestMetrics testMetrics1 = metrics
        .register("TestMetrics1", "Testing metrics", new TestMetrics("1"));
    TestMetrics testMetrics2 = metrics
        .register("TestMetrics2", "Testing metrics", new TestMetrics("2"));

    testMetrics1.numBucketCreateFails.incr();
    testMetrics2.numBucketCreateFails.incr();
    metrics.publishMetricsNow();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(stream, UTF_8);

    //WHEN
    sink.writeMetrics(writer);
    writer.flush();

    //THEN
    String writtenMetrics = stream.toString(UTF_8);
    LOG.debug(writtenMetrics);
    assertTrue(writtenMetrics.contains(
        "test_metrics_num_bucket_create_fails{context=\"dfs\",testtag=\"testTagValue1\""),
        "The expected first metric line is missing from prometheus metrics output");
    assertTrue(writtenMetrics.contains(
        "test_metrics_num_bucket_create_fails{context=\"dfs\",testtag=\"testTagValue2\""),
        "The expected second metric line is missing from prometheus metrics output");
  }

  /**
   * Fix for HADOOP-17804, make sure Prometheus metrics start fresh after each flush.
   */
  @Test
  public void testPublishFlush() throws IOException {
    //GIVEN
    PrometheusMetricsSink sink = new PrometheusMetricsSink();
    metrics.register("Prometheus", "Prometheus", sink);
    TestMetrics testMetrics = metrics
        .register("TestMetrics", "Testing metrics", new TestMetrics("1"));

    testMetrics.numBucketCreateFails.incr();
    metrics.publishMetricsNow();

    metrics.unregisterSource("TestMetrics");
    testMetrics = metrics
        .register("TestMetrics", "Testing metrics", new TestMetrics("2"));

    testMetrics.numBucketCreateFails.incr();
    metrics.publishMetricsNow();

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(stream, UTF_8);

    //WHEN
    sink.writeMetrics(writer);
    writer.flush();

    //THEN
    String writtenMetrics = stream.toString(UTF_8);
    LOG.debug(writtenMetrics);
    assertFalse(writtenMetrics.contains(
        "test_metrics_num_bucket_create_fails{context=\"dfs\",testtag=\"testTagValue1\""),
        "The first metric should not exist after flushing");
    assertTrue(writtenMetrics.contains(
        "test_metrics_num_bucket_create_fails{context=\"dfs\",testtag=\"testTagValue2\""),
        "The expected metric line is missing from prometheus metrics output");
  }

  @Test
  public void testNamingCamelCase() {
    PrometheusMetricsSink sink = new PrometheusMetricsSink();

    assertEquals("rpc_time_some_metrics",
        sink.prometheusName("RpcTime", "SomeMetrics"));

    assertEquals("om_rpc_time_om_info_keys",
        sink.prometheusName("OMRpcTime", "OMInfoKeys"));

    assertEquals("rpc_time_small",
        sink.prometheusName("RpcTime", "small"));
  }

  @Test
  public void testNamingPipeline() {
    PrometheusMetricsSink sink = new PrometheusMetricsSink();

    String recordName = "SCMPipelineMetrics";
    String metricName = "NumBlocksAllocated-"
        + "RATIS-THREE-47659e3d-40c9-43b3-9792-4982fc279aba";
    assertEquals(
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
    assertEquals(
        "org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_dfs_used",
        sink.prometheusName(recordName, metricName));
  }

  @Test
  public void testNamingWhitespaces() {
    PrometheusMetricsSink sink = new PrometheusMetricsSink();

    String recordName = "JvmMetrics";
    String metricName = "GcCount" + "G1 Old Generation";
    assertEquals(
        "jvm_metrics_gc_count_g1_old_generation",
        sink.prometheusName(recordName, metricName));
  }

  /**
   * testTopMetricsPublish.
   */
  @Test
  public void testTopMetricsPublish() throws IOException {
    //GIVEN
    PrometheusMetricsSink sink = new PrometheusMetricsSink();

    metrics.register("prometheus", "prometheus", sink);
    TestTopMetrics topMetrics = new TestTopMetrics();
    topMetrics.add("60000");
    topMetrics.add("1500000");
    metrics.register(TestTopMetrics.TOPMETRICS_METRICS_SOURCE_NAME,
        "Top N operations by user", topMetrics);

    metrics.start();

    metrics.publishMetricsNow();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(stream, UTF_8);

    //WHEN
    sink.writeMetrics(writer);
    writer.flush();

    //THEN
    String writtenMetrics = stream.toString(UTF_8);
    LOG.debug(writtenMetrics);

    assertThat(writtenMetrics)
        .contains(
            "nn_top_user_op_counts_window_ms_60000_total_count{context=\"dfs\"")
        .contains(
            "nn_top_user_op_counts_window_ms_60000_count{")
        .contains(
            "nn_top_user_op_counts_window_ms_1500000_count{")
        .contains(
            "op=\"rename\",user=\"hadoop/TEST_HOSTNAME.com@HOSTNAME.COM\"");
  }

  /**
   * Example metric pojo.
   */
  @Metrics(about = "Test Metrics", context = "dfs")
  private static class TestMetrics {
    private String id;

    TestMetrics() {
      this("1");
    }

    TestMetrics(String id) {
      this.id = id;
    }

    @Metric(value={"testTag", ""}, type=Type.TAG)
    String testTag1() {
      return "testTagValue" + id;
    }

    @Metric
    private MutableCounterLong numBucketCreateFails;
  }

  /**
   * Example metric TopMetrics.
   */
  private class TestTopMetrics implements MetricsSource {

    public static final String TOPMETRICS_METRICS_SOURCE_NAME =
        "NNTopUserOpCounts";
    private final List<String> windowMsNames = new ArrayList<>();

    public void add(String windowMs) {
      windowMsNames.add(String.format(".windowMs=%s", windowMs));
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
      for (String windowMs : windowMsNames) {
        MetricsRecordBuilder rb = collector
            .addRecord(TOPMETRICS_METRICS_SOURCE_NAME + windowMs)
            .setContext("dfs");
        rb.addCounter(
            Interns.info("op=" + StringUtils.deleteWhitespace("rename")
                + ".TotalCount", "Total operation count"), 2);
        rb.addCounter(
            Interns.info("op=" + StringUtils.deleteWhitespace("rename")
                + ".user=" + "hadoop/TEST_HOSTNAME.com@HOSTNAME.COM"
                + ".count", "Total operations performed by user"), 3);
        rb.addCounter(
            Interns.info("op=" + StringUtils.deleteWhitespace("delete")
                + ".user=" + "test_user2"
                + ".count", "Total operations performed by user"), 4);
      }
    }
  }
}