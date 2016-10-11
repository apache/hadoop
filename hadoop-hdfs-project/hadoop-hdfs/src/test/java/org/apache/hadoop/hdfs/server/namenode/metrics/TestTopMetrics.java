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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.hdfs.server.namenode.top.metrics.TopMetrics;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.junit.Test;

import static org.apache.hadoop.hdfs.server.namenode.top.metrics.TopMetrics.TOPMETRICS_METRICS_SOURCE_NAME;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test for MetricsSource part of the {@link TopMetrics} impl.
 */
public class TestTopMetrics {
  @Test
  public void testPresence() {
    Configuration conf = new Configuration();
    TopConf topConf = new TopConf(conf);
    TopMetrics topMetrics = new TopMetrics(conf,
        topConf.nntopReportingPeriodsMs);
    // Dummy command
    topMetrics.report("test", "listStatus");
    topMetrics.report("test", "listStatus");
    topMetrics.report("test", "listStatus");

    MetricsRecordBuilder rb = getMetrics(topMetrics);
    MetricsCollector mc = rb.parent();

    verify(mc).addRecord(TOPMETRICS_METRICS_SOURCE_NAME + ".windowMs=60000");
    verify(mc).addRecord(TOPMETRICS_METRICS_SOURCE_NAME + ".windowMs=300000");
    verify(mc).addRecord(TOPMETRICS_METRICS_SOURCE_NAME + ".windowMs=1500000");

    verify(rb, times(3)).addCounter(Interns.info("op=listStatus.TotalCount",
        "Total operation count"), 3L);
    verify(rb, times(3)).addCounter(Interns.info("op=*.TotalCount",
        "Total operation count"), 3L);

    verify(rb, times(3)).addCounter(Interns.info("op=listStatus." +
        "user=test.count", "Total operations performed by user"), 3L);
  }
}
