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

package org.apache.hadoop.ozone.scm.pipeline;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineMetrics;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

/**
 * Test cases to verify the metrics exposed by SCMPipelineManager.
 */
public class TestSCMPipelineMetrics {

  private MiniOzoneCluster cluster;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Verifies pipeline creation metric.
   */
  @Test
  public void testPipelineCreation() {
    MetricsRecordBuilder metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    long numPipelineCreated = getLongCounter("NumPipelineCreated", metrics);
    // Pipelines are created in background when the cluster starts.
    Assert.assertTrue(numPipelineCreated > 0);
  }

  /**
   * Verifies pipeline destroy metric.
   */
  @Test
  public void testPipelineDestroy() {
    PipelineManager pipelineManager = cluster
        .getStorageContainerManager().getPipelineManager();
    Optional<Pipeline> pipeline = pipelineManager
        .getPipelines().stream().findFirst();
    Assert.assertTrue(pipeline.isPresent());
    pipeline.ifPresent(pipeline1 -> {
      try {
        cluster.getStorageContainerManager()
            .getClientProtocolServer().closePipeline(
                pipeline.get().getId().getProtobuf());
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail();
      }
    });
    MetricsRecordBuilder metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    assertCounter("NumPipelineDestroyed", 1L, metrics);
  }


  @After
  public void teardown() {
    cluster.shutdown();
  }
}
