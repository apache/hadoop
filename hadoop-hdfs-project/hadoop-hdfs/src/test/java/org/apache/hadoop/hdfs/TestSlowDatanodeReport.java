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

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.OutlierMetrics;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys
    .DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY;

/**
 * Tests to report slow running datanodes.
 */
public class TestSlowDatanodeReport {

  private static final Logger LOG = LoggerFactory.getLogger(TestSlowDatanodeReport.class);

  private MiniDFSCluster cluster;

  @Before
  public void testSetup() throws Exception {
    Configuration conf = new Configuration();

    conf.set(DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY, "1000");
    conf.set(DFS_DATANODE_PEER_STATS_ENABLED_KEY, "true");
    conf.set(DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY, "1");
    conf.set(DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY, "1");

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testSingleNodeReport() throws Exception {
    List<DataNode> dataNodes = cluster.getDataNodes();
    DataNode slowNode = dataNodes.get(1);
    OutlierMetrics outlierMetrics = new OutlierMetrics(1.245, 2.69375, 4.5667, 15.5);
    dataNodes.get(0).getPeerMetrics().setTestOutliers(
        ImmutableMap.of(slowNode.getDatanodeHostname() + ":" + slowNode.getIpcPort(),
            outlierMetrics));
    DistributedFileSystem distributedFileSystem = cluster.getFileSystem();
    Assert.assertEquals(3, distributedFileSystem.getDataNodeStats().length);
    GenericTestUtils.waitFor(() -> {
      try {
        DatanodeInfo[] slowNodeInfo = distributedFileSystem.getSlowDatanodeStats();
        LOG.info("Slow Datanode report: {}", Arrays.asList(slowNodeInfo));
        return slowNodeInfo.length == 1;
      } catch (IOException e) {
        LOG.error("Failed to retrieve slownode report", e);
        return false;
      }
    }, 2000, 180000, "Slow nodes could not be detected");
    LOG.info("Slow peer report: {}", cluster.getNameNode().getSlowPeersReport());
    Assert.assertTrue(cluster.getNameNode().getSlowPeersReport().length() > 0);
    Assert.assertTrue(
        cluster.getNameNode().getSlowPeersReport().contains(slowNode.getDatanodeHostname()));
    Assert.assertTrue(cluster.getNameNode().getSlowPeersReport().contains("15.5"));
    Assert.assertTrue(cluster.getNameNode().getSlowPeersReport().contains("1.245"));
    Assert.assertTrue(cluster.getNameNode().getSlowPeersReport().contains("2.69375"));
    Assert.assertTrue(cluster.getNameNode().getSlowPeersReport().contains("4.5667"));
  }

  @Test
  public void testMultiNodesReport() throws Exception {
    List<DataNode> dataNodes = cluster.getDataNodes();
    OutlierMetrics outlierMetrics1 = new OutlierMetrics(2.498237, 19.2495, 23.568204, 14.5);
    OutlierMetrics outlierMetrics2 = new OutlierMetrics(3.2535, 22.4945, 44.5667, 18.7);
    dataNodes.get(0).getPeerMetrics().setTestOutliers(ImmutableMap.of(
        dataNodes.get(1).getDatanodeHostname() + ":" + dataNodes.get(1).getIpcPort(),
        outlierMetrics1));
    dataNodes.get(1).getPeerMetrics().setTestOutliers(ImmutableMap.of(
        dataNodes.get(2).getDatanodeHostname() + ":" + dataNodes.get(2).getIpcPort(),
        outlierMetrics2));
    DistributedFileSystem distributedFileSystem = cluster.getFileSystem();
    Assert.assertEquals(3, distributedFileSystem.getDataNodeStats().length);
    GenericTestUtils.waitFor(() -> {
      try {
        DatanodeInfo[] slowNodeInfo = distributedFileSystem.getSlowDatanodeStats();
        LOG.info("Slow Datanode report: {}", Arrays.asList(slowNodeInfo));
        return slowNodeInfo.length == 2;
      } catch (IOException e) {
        LOG.error("Failed to retrieve slownode report", e);
        return false;
      }
    }, 2000, 200000, "Slow nodes could not be detected");
    LOG.info("Slow peer report: {}", cluster.getNameNode().getSlowPeersReport());
    Assert.assertTrue(cluster.getNameNode().getSlowPeersReport().length() > 0);
    Assert.assertTrue(cluster.getNameNode().getSlowPeersReport()
        .contains(dataNodes.get(1).getDatanodeHostname()));
    Assert.assertTrue(cluster.getNameNode().getSlowPeersReport()
        .contains(dataNodes.get(2).getDatanodeHostname()));
    Assert.assertTrue(cluster.getNameNode().getSlowPeersReport().contains("14.5"));
    Assert.assertTrue(cluster.getNameNode().getSlowPeersReport().contains("18.7"));
    Assert.assertTrue(cluster.getNameNode().getSlowPeersReport().contains("23.568204"));
    Assert.assertTrue(cluster.getNameNode().getSlowPeersReport().contains("22.4945"));
  }

}
