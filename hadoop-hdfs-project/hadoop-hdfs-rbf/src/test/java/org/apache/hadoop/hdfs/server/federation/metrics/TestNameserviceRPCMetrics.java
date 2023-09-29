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
package org.apache.hadoop.hdfs.server.federation.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCPerformanceMonitor.CONCURRENT;
import static org.apache.hadoop.hdfs.server.federation.metrics.NameserviceRPCMetrics.NAMESERVICE_RPC_METRICS_PREFIX;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

/**
 * Test case for RouterClientMetrics.
 */
public class TestNameserviceRPCMetrics {
  private static final Configuration CONF = new HdfsConfiguration();
  static {
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 100);
    CONF.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);
    CONF.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    CONF.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
  }

  private static final int NUM_SUBCLUSTERS = 2;
  private static final int NUM_DNS = 3;

  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;

  /** The first Router Context for this federated cluster. */
  private MiniRouterDFSCluster.RouterContext routerContext;

  /** The first Router for this federated cluster. */
  private Router router;

  /** Filesystem interface to the Router. */
  private FileSystem routerFS;
  /** Filesystem interface to the Namenode. */
  private FileSystem nnFS;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new MiniRouterDFSCluster(false, NUM_SUBCLUSTERS);
    cluster.setNumDatanodesPerNameservice(NUM_DNS);
    cluster.startCluster();

    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .quota()
        .build();
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();

  }

  @Before
  public void testSetup() throws Exception {
    // Create mock locations
    cluster.installMockLocations();

    // Delete all files via the NNs and verify
    cluster.deleteAllFiles();

    // Create test fixtures on NN
    cluster.createTestDirectoriesNamenode();

    // Wait to ensure NN has fully created its test directories
    Thread.sleep(100);

    routerContext = cluster.getRouters().get(0);
    this.routerFS = routerContext.getFileSystem();

    // Add extra location to the root mount / such that the root mount points:
    // /
    //   ns0 -> /target-ns0
    //   ns1 -> /target-ns1
    router = routerContext.getRouter();
    MockResolver resolver = (MockResolver) router.getSubclusterResolver();
    resolver.addLocation("/target-ns0", cluster.getNameservices().get(0), "/target-ns0");
    resolver.addLocation("/target-ns1", cluster.getNameservices().get(1), "/target-ns1");

  }

  @AfterClass
  public static void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testProxyOp() throws IOException {
    routerFS.listStatus(new Path("/target-ns0"));
    assertCounter("ProxyOp", 1L,
        getMetrics(NAMESERVICE_RPC_METRICS_PREFIX + "ns0"));
    assertCounter("ProxyOp", 0L,
        getMetrics(NAMESERVICE_RPC_METRICS_PREFIX + "ns1"));

    routerFS.listStatus(new Path("/target-ns1"));
    assertCounter("ProxyOp", 1L,
        getMetrics(NAMESERVICE_RPC_METRICS_PREFIX + "ns0"));
    assertCounter("ProxyOp", 1L,
        getMetrics(NAMESERVICE_RPC_METRICS_PREFIX + "ns1"));
  }

  @Test
  public void testProxyOpCompleteConcurrent() throws IOException {

    long ns0ProxyOpBefore = getLongCounter("ProxyOp",
        getMetrics(NAMESERVICE_RPC_METRICS_PREFIX + "ns0"));
    long ns1ProxyOpBefore = getLongCounter("ProxyOp",
        getMetrics(NAMESERVICE_RPC_METRICS_PREFIX + "ns1"));
    long concurrentProxyOpBefore = getLongCounter("ProxyOp",
        getMetrics(NAMESERVICE_RPC_METRICS_PREFIX + "concurrent"));
    // RPC which uses invokeConcurrent.
    router.getRpcServer().setBalancerBandwidth(1024 * 1024L);

    assertCounter("ProxyOp", ns0ProxyOpBefore + 1,
        getMetrics(NAMESERVICE_RPC_METRICS_PREFIX + "ns0"));
    assertCounter("ProxyOp", ns1ProxyOpBefore + 1,
        getMetrics(NAMESERVICE_RPC_METRICS_PREFIX + "ns1"));
    assertCounter("ProxyOp", concurrentProxyOpBefore + 1,
        getMetrics(NAMESERVICE_RPC_METRICS_PREFIX + CONCURRENT));

  }

}

