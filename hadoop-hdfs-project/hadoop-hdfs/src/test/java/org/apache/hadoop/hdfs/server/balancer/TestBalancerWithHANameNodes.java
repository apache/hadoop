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
package org.apache.hadoop.hdfs.server.balancer;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_ALLOW_STALE_READ_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_ALLOW_STALE_READ_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NNConf;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Test balancer with HA NameNodes
 */
public class TestBalancerWithHANameNodes {
  private MiniDFSCluster cluster;
  ClientProtocol client;

  // array of racks for original nodes in cluster
  private static final String[] TEST_RACKS =
      {TestBalancer.RACK0, TestBalancer.RACK1};
  // array of capacities for original nodes in cluster
  private static final long[] TEST_CAPACITIES =
      {TestBalancer.CAPACITY, TestBalancer.CAPACITY};

  static {
    TestBalancer.initTestSetup();
  }

  /**
   * Test a cluster with even distribution, then a new empty node is added to
   * the cluster. Test start a cluster with specified number of nodes, and fills
   * it to be 30% full (with a single file replicated identically to all
   * datanodes); It then adds one new empty node and starts balancing.
   */
  @Test(timeout = 60000)
  public void testBalancerWithHANameNodes() throws Exception {
    Configuration conf = new HdfsConfiguration();
    TestBalancer.initConf(conf);
    assertEquals(TEST_CAPACITIES.length, TEST_RACKS.length);
    NNConf nn1Conf = new MiniDFSNNTopology.NNConf("nn1");
    nn1Conf.setIpcPort(HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT);
    Configuration copiedConf = new Configuration(conf);
    cluster = new MiniDFSCluster.Builder(copiedConf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(TEST_CAPACITIES.length)
        .racks(TEST_RACKS)
        .simulatedCapacities(TEST_CAPACITIES)
        .build();
    HATestUtil.setFailoverConfigurations(cluster, conf);
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      Thread.sleep(500);
      client = NameNodeProxies.createProxy(conf, FileSystem.getDefaultUri(conf),
          ClientProtocol.class).getProxy();

      doTest(conf);
    } finally {
      cluster.shutdown();
    }
  }

  void doTest(Configuration conf) throws Exception {
    int numOfDatanodes = TEST_CAPACITIES.length;
    long totalCapacity = TestBalancer.sum(TEST_CAPACITIES);
    // fill up the cluster to be 30% full
    long totalUsedSpace = totalCapacity * 3 / 10;
    TestBalancer.createFile(cluster, TestBalancer.filePath, totalUsedSpace
        / numOfDatanodes, (short) numOfDatanodes, 0);

    boolean isRequestStandby = conf.getBoolean(
        DFS_HA_ALLOW_STALE_READ_KEY, DFS_HA_ALLOW_STALE_READ_DEFAULT);
    if (isRequestStandby) {
      HATestUtil.waitForStandbyToCatchUp(cluster.getNameNode(0),
          cluster.getNameNode(1));
    }
    // start up an empty node with the same capacity and on the same rack
    long newNodeCapacity = TestBalancer.CAPACITY; // new node's capacity
    String newNodeRack = TestBalancer.RACK2; // new node's rack
    cluster.startDataNodes(conf, 1, true, null, new String[] {newNodeRack},
        new long[] {newNodeCapacity});
    totalCapacity += newNodeCapacity;
    TestBalancer.waitForHeartBeat(totalUsedSpace, totalCapacity, client,
        cluster);
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    Collection<String> nsIds = DFSUtilClient.getNameServiceIds(conf);
    assertEquals(1, namenodes.size());
    final int r = Balancer.run(namenodes, nsIds, BalancerParameters.DEFAULT,
        conf);
    assertEquals(ExitStatus.SUCCESS.getExitCode(), r);
    TestBalancer.waitForBalancer(totalUsedSpace, totalCapacity, client,
        cluster, BalancerParameters.DEFAULT);
  }

  /**
   * Test Balancer request Standby NameNode when enable this feature.
   */
  @Test(timeout = 60000)
  public void testBalancerRequestSBNWithHA() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFS_HA_ALLOW_STALE_READ_KEY, true);
    conf.setLong(DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    //conf.setBoolean(DFS_HA_BALANCER_REQUEST_STANDBY_KEY, true);
    TestBalancer.initConf(conf);
    assertEquals(TEST_CAPACITIES.length, TEST_RACKS.length);
    NNConf nn1Conf = new MiniDFSNNTopology.NNConf("nn1");
    nn1Conf.setIpcPort(HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT);
    Configuration copiedConf = new Configuration(conf);
    cluster = new MiniDFSCluster.Builder(copiedConf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(TEST_CAPACITIES.length)
        .racks(TEST_RACKS)
        .simulatedCapacities(TEST_CAPACITIES)
        .build();
    // Try capture NameNodeConnector log.
    LogCapturer log =LogCapturer.captureLogs(
        LoggerFactory.getLogger(NameNodeConnector.class));
    HATestUtil.setFailoverConfigurations(cluster, conf);
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      Thread.sleep(500);
      client = NameNodeProxies.createProxy(conf, FileSystem.getDefaultUri(conf),
          ClientProtocol.class).getProxy();
      doTest(conf);
      // Check getBlocks request to Standby NameNode.
      assertTrue(log.getOutput().contains(
          "Request #getBlocks to Standby NameNode success."));
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test Balancer with ObserverNodes.
   */
  @Test(timeout = 120000)
  public void testBalancerWithObserver() throws Exception {
    testBalancerWithObserver(false);
  }

  /**
   * Test Balancer with ObserverNodes when one has failed.
   */
  @Test(timeout = 180000)
  public void testBalancerWithObserverWithFailedNode() throws Exception {
    testBalancerWithObserver(true);
  }

  private void testBalancerWithObserver(boolean withObserverFailure)
      throws Exception {
    final Configuration conf = new HdfsConfiguration();
    TestBalancer.initConf(conf);
    // Avoid the same FS being reused between tests
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);

    MiniQJMHACluster qjmhaCluster = null;
    try {
      qjmhaCluster = HATestUtil.setUpObserverCluster(conf, 2,
          TEST_CAPACITIES.length, true, TEST_CAPACITIES, TEST_RACKS);
      cluster = qjmhaCluster.getDfsCluster();
      cluster.waitClusterUp();
      cluster.waitActive();
      List<FSNamesystem> namesystemSpies = new ArrayList<>();
      for (int i = 0; i < cluster.getNumNameNodes(); i++) {
        namesystemSpies.add(
            NameNodeAdapter.spyOnNamesystem(cluster.getNameNode(i)));
      }
      if (withObserverFailure) {
        // First observer NN is at index 2
        cluster.shutdownNameNode(2);
      }

      DistributedFileSystem dfs = HATestUtil.configureObserverReadFs(
          cluster, conf, ObserverReadProxyProvider.class, true);
      client = dfs.getClient().getNamenode();

      doTest(conf);
      for (int i = 0; i < cluster.getNumNameNodes(); i++) {
        // First observer node is at idx 2, or 3 if 2 has been shut down
        // It should get both getBlocks calls, all other NNs should see 0 calls
        int expectedObserverIdx = withObserverFailure ? 3 : 2;
        int expectedCount = (i == expectedObserverIdx) ? 2 : 0;
        verify(namesystemSpies.get(i), times(expectedCount))
            .getBlocks(any(), anyLong(), anyLong());
      }
    } finally {
      if (qjmhaCluster != null) {
        qjmhaCluster.shutdown();
      }
    }
  }
}
