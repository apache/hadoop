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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.hadoop.util.Tool;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test balancer run as a service.
 */
public class TestBalancerService {
  private MiniDFSCluster cluster;
  private ClientProtocol client;
  private long totalUsedSpace;

  // array of racks for original nodes in cluster
  private static final String[] TEST_RACKS =
      {TestBalancer.RACK0, TestBalancer.RACK1};
  // array of capacities for original nodes in cluster
  private static final long[] TEST_CAPACITIES =
      {TestBalancer.CAPACITY, TestBalancer.CAPACITY};
  private static final double USED = 0.3;

  static {
    TestBalancer.initTestSetup();
  }

  private void setupCluster(Configuration conf) throws Exception {
    MiniDFSNNTopology.NNConf nn1Conf = new MiniDFSNNTopology.NNConf("nn1");
    nn1Conf.setIpcPort(HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT);
    Configuration copiedConf = new Configuration(conf);
    // Limit the number of failover retries to avoid the test taking too long
    conf.setInt(HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_KEY, 2);
    conf.setInt(HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_KEY, 0);
    cluster = new MiniDFSCluster.Builder(copiedConf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(TEST_CAPACITIES.length).racks(TEST_RACKS)
        .simulatedCapacities(TEST_CAPACITIES).build();
    HATestUtil.setFailoverConfigurations(cluster, conf);
    cluster.waitActive();
    cluster.transitionToActive(0);
    client = NameNodeProxies
        .createProxy(conf, FileSystem.getDefaultUri(conf), ClientProtocol.class)
        .getProxy();

    int numOfDatanodes = TEST_CAPACITIES.length;
    long totalCapacity = TestBalancer.sum(TEST_CAPACITIES);
    // fill up the cluster to be 30% full
    totalUsedSpace = (long) (totalCapacity * USED);
    TestBalancer.createFile(cluster, TestBalancer.filePath,
        totalUsedSpace / numOfDatanodes, (short) numOfDatanodes, 0);
  }

  private long addOneDataNode(Configuration conf) throws Exception {
    // start up an empty node with the same capacity and on the same rack
    cluster.startDataNodes(conf, 1, true, null,
        new String[] {TestBalancer.RACK2},
        new long[] {TestBalancer.CAPACITY});
    long totalCapacity = cluster.getDataNodes().size() * TestBalancer.CAPACITY;
    TestBalancer.waitForHeartBeat(totalUsedSpace, totalCapacity, client,
        cluster);
    return totalCapacity;
  }

  private Thread newBalancerService(Configuration conf, String[] args) {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        Tool cli = new Balancer.Cli();
        cli.setConf(conf);
        try {
          cli.run(args);
        } catch (Exception e) {
          fail("balancer failed for " + e);
        }
      }
    });
  }

  /**
   * The normal test case. Start with an imbalanced cluster, then balancer
   * should balance succeed but not exit, then make the cluster imbalanced and
   * wait for balancer to balance it again
   */
  @Test(timeout = 60000)
  public void testBalancerServiceBalanceTwice() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setTimeDuration(DFSConfigKeys.DFS_BALANCER_SERVICE_INTERVAL_KEY, 5,
        TimeUnit.SECONDS);
    TestBalancer.initConf(conf);
    try {
      setupCluster(conf);
      TestBalancerWithHANameNodes.waitStoragesNoStale(cluster, client, 0);
      long totalCapacity = addOneDataNode(conf); // make cluster imbalanced

      Thread balancerThread =
          newBalancerService(conf, new String[] {"-asService"});
      balancerThread.start();

      // Check metrics
      final String balancerMetricsName = "Balancer-"
          + cluster.getNameNode(0).getNamesystem().getBlockPoolId();
      GenericTestUtils.waitFor(() -> {
        // Validate metrics after metrics system initiated.
        if (DefaultMetricsSystem.instance().getSource(balancerMetricsName) == null) {
          return false;
        }
        MetricsRecordBuilder rb = MetricsAsserts.getMetrics(balancerMetricsName);
        return rb != null && MetricsAsserts.getLongGauge("BytesLeftToMove", rb) > 0;
      }, 100, 2000);

      TestBalancer.waitForBalancer(totalUsedSpace, totalCapacity, client,
          cluster, BalancerParameters.DEFAULT);

      MetricsRecordBuilder rb = MetricsAsserts.getMetrics(balancerMetricsName);
      assertTrue(MetricsAsserts.getLongGauge("BytesMovedInCurrentRun", rb) > 0);

      cluster.triggerHeartbeats();
      cluster.triggerBlockReports();

      // add another empty datanode, wait for cluster become balance again
      totalCapacity = addOneDataNode(conf);
      TestBalancer.waitForBalancer(totalUsedSpace, totalCapacity, client,
          cluster, BalancerParameters.DEFAULT);

      Balancer.stop();
      balancerThread.join();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout = 120000)
  public void testBalancerServiceOnError() throws Exception {
    Configuration conf = new HdfsConfiguration();
    // retry for every 5 seconds
    conf.setTimeDuration(DFSConfigKeys.DFS_BALANCER_SERVICE_INTERVAL_KEY, 5,
        TimeUnit.SECONDS);
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    TestBalancer.initConf(conf);
    try {
      setupCluster(conf);

      Thread balancerThread =
          newBalancerService(conf, new String[] {"-asService"});
      balancerThread.start();

      // cluster is out of service for 10+ secs, the balancer service will retry
      // for 2+ times
      cluster.shutdownNameNode(0);
      GenericTestUtils.waitFor(
          () -> Balancer.getExceptionsSinceLastBalance() > 0, 1000, 10000);
      assertTrue(Balancer.getExceptionsSinceLastBalance() > 0);
      cluster.restartNameNode(0);
      cluster.transitionToActive(0);
      cluster.waitActive();

      TestBalancerWithHANameNodes.waitStoragesNoStale(cluster, client, 0);
      long totalCapacity = addOneDataNode(conf);
      TestBalancer.waitForBalancer(totalUsedSpace, totalCapacity, client,
          cluster, BalancerParameters.DEFAULT);

      Balancer.stop();
      balancerThread.join();

      // reset to 0 once the balancer finished without exception
      assertEquals(Balancer.getExceptionsSinceLastBalance(), 0);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
