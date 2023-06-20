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
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

/**
 * Test case for RouterClientMetrics.
 */
public class TestRouterClientMetrics {
  private static final Configuration CONF = new HdfsConfiguration();
  private static final String ROUTER_METRICS = "RouterClientActivity";
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
    //   ns0 -> /
    //   ns1 -> /
    router = routerContext.getRouter();
    MockResolver resolver = (MockResolver) router.getSubclusterResolver();
    resolver.addLocation("/", cluster.getNameservices().get(1), "/");

  }

  @AfterClass
  public static void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testGetListing() throws IOException {
    routerFS.listStatus(new Path("/"));
    assertCounter("GetListingOps", 2L, getMetrics(ROUTER_METRICS));
    assertCounter("ConcurrentGetListingOps", 1L, getMetrics(ROUTER_METRICS));
  }

  @Test
  public void testCreate() throws IOException {
    Path testFile = new Path("/testCreate");
    routerFS.create(testFile);
    assertCounter("CreateOps", 1L, getMetrics(ROUTER_METRICS));
  }

  @Test
  public void testGetServerDefaults() throws IOException {
    router.getRpcServer().getServerDefaults();
    assertCounter("GetServerDefaultsOps", 1L, getMetrics(ROUTER_METRICS));
  }

  @Test
  public void testSetQuota() throws Exception {
    router.getRpcServer().setQuota("/", 1L, 1L, null);
    assertCounter("SetQuotaOps", 2L, getMetrics(ROUTER_METRICS));
    assertCounter("ConcurrentSetQuotaOps", 1L, getMetrics(ROUTER_METRICS));
  }

  @Test
  public void testGetQuota() throws Exception {
    router.getRpcServer().getQuotaUsage("/");
    assertCounter("GetQuotaUsageOps", 2L, getMetrics(ROUTER_METRICS));
    assertCounter("ConcurrentGetQuotaUsageOps", 1L, getMetrics(ROUTER_METRICS));
  }

  @Test
  public void testRenewLease() throws Exception {
    router.getRpcServer().renewLease("test", null);
    assertCounter("RenewLeaseOps", 2L, getMetrics(ROUTER_METRICS));
    assertCounter("ConcurrentRenewLeaseOps", 1L, getMetrics(ROUTER_METRICS));
  }

  @Test
  public void testGetDatanodeReport() throws Exception {
    router.getRpcServer().
        getDatanodeReport(HdfsConstants.DatanodeReportType.LIVE);
    assertCounter("GetDatanodeReportOps", 2L, getMetrics(ROUTER_METRICS));
    assertCounter("ConcurrentGetDatanodeReportOps", 1L,
        getMetrics(ROUTER_METRICS));
  }

  @Test
  public void testGetSlowDatanodeReport() throws Exception {
    router.getRpcServer().getSlowDatanodeReport();
    assertCounter("GetSlowDatanodeReportOps", 2L, getMetrics(ROUTER_METRICS));
    assertCounter("ConcurrentGetSlowDatanodeReportOps", 1L, getMetrics(ROUTER_METRICS));
  }

}

