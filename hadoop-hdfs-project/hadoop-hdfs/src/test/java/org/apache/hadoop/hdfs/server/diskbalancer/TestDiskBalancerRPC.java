/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ClusterConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ConnectorFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.GreedyPlanner;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;

public class TestDiskBalancerRPC {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private MiniDFSCluster cluster;

  @Before
  public void setUp() throws Exception {
    Configuration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void TestSubmitTestRpc() throws Exception {
    URI clusterJson = getClass()
        .getResource("/diskBalancer/data-cluster-3node-3disk.json").toURI();
    ClusterConnector jsonConnector = ConnectorFactory.getCluster(clusterJson,
        null);
    DiskBalancerCluster diskBalancerCluster = new DiskBalancerCluster(jsonConnector);
    diskBalancerCluster.readClusterInfo();
    Assert.assertEquals(3, diskBalancerCluster.getNodes().size());
    diskBalancerCluster.setNodesToProcess(diskBalancerCluster.getNodes());
    DiskBalancerDataNode node = diskBalancerCluster.getNodes().get(0);
    GreedyPlanner planner = new GreedyPlanner(10.0f, node);
    NodePlan plan = new NodePlan(node.getDataNodeName(), node.getDataNodePort
        ());
    planner.balanceVolumeSet(node, node.getVolumeSets().get("SSD"), plan);

    final int dnIndex = 0;
    final int planVersion = 0; // So far we support only one version.
    DataNode dataNode = cluster.getDataNodes().get(dnIndex);
    String planHash = DigestUtils.sha512Hex(plan.toJson());

    // Since submitDiskBalancerPlan is not implemented yet, it throws an
    // Exception, this will be modified with the actual implementation.
    thrown.expect(DiskbalancerException.class);
    dataNode.submitDiskBalancerPlan(planHash, planVersion, 10, plan.toJson());

  }
}
