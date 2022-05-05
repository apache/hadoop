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
    dataNodes.get(0).getPeerMetrics().setTestOutliers(
        ImmutableMap.of(slowNode.getDatanodeHostname() + ":" + slowNode.getIpcPort(), 15.5));
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
  }

  @Test
  public void testMultiNodesReport() throws Exception {
    List<DataNode> dataNodes = cluster.getDataNodes();
    dataNodes.get(0).getPeerMetrics().setTestOutliers(ImmutableMap.of(
        dataNodes.get(1).getDatanodeHostname() + ":" + dataNodes.get(1).getIpcPort(), 15.5));
    dataNodes.get(1).getPeerMetrics().setTestOutliers(ImmutableMap.of(
        dataNodes.get(2).getDatanodeHostname() + ":" + dataNodes.get(2).getIpcPort(), 18.7));
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
  }

}
