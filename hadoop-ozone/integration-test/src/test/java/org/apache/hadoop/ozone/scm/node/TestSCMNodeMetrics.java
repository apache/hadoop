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

package org.apache.hadoop.ozone.scm.node;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.node.SCMNodeMetrics;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

/**
 * Test cases to verify the metrics exposed by SCMNodeManager.
 */
public class TestSCMNodeMetrics {

  private MiniOzoneCluster cluster;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Verifies heartbeat processing count.
   *
   * @throws InterruptedException
   */
  @Test
  public void testHBProcessing() throws InterruptedException {
    MetricsRecordBuilder metrics = getMetrics(
        SCMNodeMetrics.class.getSimpleName());
    long hbProcessed = getLongCounter("NumHBProcessed", metrics);
    cluster.getHddsDatanodes().get(0)
        .getDatanodeStateMachine().triggerHeartbeat();
    // Give some time so that SCM receives and processes the heartbeat.
    Thread.sleep(100L);
    assertCounter("NumHBProcessed", hbProcessed + 1,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
  }

  /**
   * Verifies heartbeat processing failure count.
   */
  @Test
  public void testHBProcessingFailure() {
    MetricsRecordBuilder metrics = getMetrics(
        SCMNodeMetrics.class.getSimpleName());
    long hbProcessedFailed = getLongCounter("NumHBProcessingFailed", metrics);
    cluster.getStorageContainerManager().getScmNodeManager()
        .processHeartbeat(TestUtils.randomDatanodeDetails());
    assertCounter("NumHBProcessingFailed", hbProcessedFailed + 1,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
  }

  /**
   * Verifies node report processing count.
   *
   * @throws InterruptedException
   */
  @Test
  public void testNodeReportProcessing() throws InterruptedException {
    MetricsRecordBuilder metrics = getMetrics(
        SCMNodeMetrics.class.getSimpleName());
    long nrProcessed = getLongCounter("NumNodeReportProcessed", metrics);
    HddsDatanodeService datanode = cluster.getHddsDatanodes().get(0);
    StorageReportProto storageReport = TestUtils.createStorageReport(
        datanode.getDatanodeDetails().getUuid(), "/tmp", 100, 10, 90, null);
    NodeReportProto nodeReport = NodeReportProto.newBuilder()
        .addStorageReport(storageReport).build();
    datanode.getDatanodeStateMachine().getContext().addReport(nodeReport);
    cluster.getStorageContainerManager().getScmNodeManager()
        .processNodeReport(datanode.getDatanodeDetails(), nodeReport);

    assertCounter("NumNodeReportProcessed", nrProcessed + 1,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
  }

  /**
   * Verifies node report processing failure count.
   */
  @Test
  public void testNodeReportProcessingFailure() {
    MetricsRecordBuilder metrics = getMetrics(
        SCMNodeMetrics.class.getSimpleName());
    long nrProcessed = getLongCounter("NumNodeReportProcessingFailed",
        metrics);
    DatanodeDetails datanode = TestUtils.randomDatanodeDetails();
    StorageReportProto storageReport = TestUtils.createStorageReport(
        datanode.getUuid(), "/tmp", 100, 10, 90, null);
    NodeReportProto nodeReport = NodeReportProto.newBuilder()
        .addStorageReport(storageReport).build();

    cluster.getStorageContainerManager().getScmNodeManager()
        .processNodeReport(datanode, nodeReport);
    assertCounter("NumNodeReportProcessingFailed", nrProcessed + 1,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
  }

  /**
   * Verify that datanode aggregated state and capacity metrics are reported.
   */
  @Test
  public void testNodeCountAndInfoMetricsReported() throws Exception {
    HddsDatanodeService datanode = cluster.getHddsDatanodes().get(0);
    StorageReportProto storageReport = TestUtils.createStorageReport(
        datanode.getDatanodeDetails().getUuid(), "/tmp", 100, 10, 90, null);
    NodeReportProto nodeReport = NodeReportProto.newBuilder()
        .addStorageReport(storageReport).build();
    datanode.getDatanodeStateMachine().getContext().addReport(nodeReport);
    cluster.getStorageContainerManager().getScmNodeManager()
        .processNodeReport(datanode.getDatanodeDetails(), nodeReport);

    assertGauge("HealthyNodes", 1,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("StaleNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DeadNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissioningNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissionedNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DiskCapacity", 100L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DiskUsed", 10L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DiskRemaining", 90L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("SSDCapacity", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("SSDUsed", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("SSDRemaining", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
  }

  @After
  public void teardown() {
    cluster.shutdown();
  }
}
