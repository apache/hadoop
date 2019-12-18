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
package org.apache.hadoop.hdfs.server.federation.router;

import static java.util.Arrays.asList;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.getFileSystem;
import static org.apache.hadoop.hdfs.server.federation.MockNamenode.registerSubclusters;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.getStateStoreConfiguration;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.federation.MockNamenode;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.NamenodeStatusReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test namenodes monitor behavior in the Router.
 */
public class TestRouterNamenodeMonitoring {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterNamenodeMonitoring.class);


  /** Router for the test. */
  private Router router;
  /** Namenodes in the cluster. */
  private Map<String, Map<String, MockNamenode>> nns = new HashMap<>();
  /** Nameservices in the federated cluster. */
  private List<String> nsIds = asList("ns0", "ns1");
  /** Namenodes in the cluster. */
  private List<String> nnIds = asList("nn0", "nn1");

  /** Time the test starts. */
  private long initializedTime;


  @Before
  public void setup() throws Exception {
    LOG.info("Initialize the Mock Namenodes to monitor");
    for (String nsId : nsIds) {
      nns.put(nsId, new HashMap<>());
      for (String nnId : nnIds) {
        nns.get(nsId).put(nnId, new MockNamenode(nsId));
      }
    }

    LOG.info("Set nn0 to active for all nameservices");
    for (Map<String, MockNamenode> nnNS : nns.values()) {
      nnNS.get("nn0").transitionToActive();
      nnNS.get("nn1").transitionToStandby();
    }

    initializedTime = Time.now();
  }

  @After
  public void cleanup() throws Exception {
    for (Map<String, MockNamenode> nnNS : nns.values()) {
      for (MockNamenode nn : nnNS.values()) {
        nn.stop();
      }
    }
    nns.clear();

    if (router != null) {
      router.stop();
    }
  }

  /**
   * Get the configuration of the cluster which contains all the Namenodes and
   * their addresses.
   * @return Configuration containing all the Namenodes.
   */
  private Configuration getNamenodesConfig() {
    final Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES,
        StringUtils.join(",", nns.keySet()));
    for (String nsId : nns.keySet()) {
      Set<String> nsNnIds = nns.get(nsId).keySet();

      StringBuilder sb = new StringBuilder();
      sb.append(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX);
      sb.append(".").append(nsId);
      conf.set(sb.toString(), StringUtils.join(",", nsNnIds));

      for (String nnId : nsNnIds) {
        final MockNamenode nn = nns.get(nsId).get(nnId);

        sb = new StringBuilder();
        sb.append(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
        sb.append(".").append(nsId);
        sb.append(".").append(nnId);
        conf.set(sb.toString(), "localhost:" + nn.getRPCPort());

        sb = new StringBuilder();
        sb.append(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
        sb.append(".").append(nsId);
        sb.append(".").append(nnId);
        conf.set(sb.toString(), "localhost:" + nn.getHTTPPort());
      }
    }
    return conf;
  }

  @Test
  public void testNamenodeMonitoring() throws Exception {
    Configuration nsConf = getNamenodesConfig();

    // Setup the State Store for the Router to use
    Configuration stateStoreConfig = getStateStoreConfiguration();
    stateStoreConfig.setClass(
        RBFConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        MembershipNamenodeResolver.class, ActiveNamenodeResolver.class);
    stateStoreConfig.setClass(
        RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        MountTableResolver.class, FileSubclusterResolver.class);

    Configuration routerConf = new RouterConfigBuilder(nsConf)
        .enableLocalHeartbeat(true)
        .heartbeat()
        .stateStore()
        .rpc()
        .build();

    // Specify namenodes (ns1.nn0,ns1.nn1) to monitor
    routerConf.set(RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY, "0.0.0.0:0");
    routerConf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE,
        "ns1.nn0,ns1.nn1");
    routerConf.addResource(stateStoreConfig);

    // Specify local node (ns0.nn1) to monitor
    routerConf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, "ns0");
    routerConf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");

    // Start the Router with the namenodes to monitor
    router = new Router();
    router.init(routerConf);
    router.start();

    // Manually trigger the heartbeat and update the values
    Collection<NamenodeHeartbeatService> heartbeatServices =
        router.getNamenodeHeartbeatServices();
    for (NamenodeHeartbeatService service : heartbeatServices) {
      service.periodicInvoke();
    }
    MembershipNamenodeResolver resolver =
        (MembershipNamenodeResolver) router.getNamenodeResolver();
    resolver.loadCache(true);

    // Check that the monitored values are expected
    final List<FederationNamenodeContext> namespaceInfo = new ArrayList<>();
    for (String nsId : nns.keySet()) {
      List<? extends FederationNamenodeContext> nnReports =
          resolver.getNamenodesForNameserviceId(nsId);
      namespaceInfo.addAll(nnReports);
    }
    for (FederationNamenodeContext nnInfo : namespaceInfo) {
      long modTime = nnInfo.getDateModified();
      long diff = modTime - initializedTime;
      if ("ns0".equals(nnInfo.getNameserviceId()) &&
          "nn0".equals(nnInfo.getNamenodeId())) {
        // The modified date won't be updated in ns0.nn0
        // since it isn't monitored by the Router.
        assertTrue(nnInfo + " shouldn't be updated: " + diff,
            modTime < initializedTime);
      } else {
        // other namnodes should be updated as expected
        assertTrue(nnInfo + " should be updated: " + diff,
            modTime > initializedTime);
      }
    }
  }

  @Test
  public void testNamenodeMonitoringConfig() throws Exception {
    testConfig(asList(), "");
    testConfig(asList("ns1.nn0"), "ns1.nn0");
    testConfig(asList("ns1.nn0", "ns1.nn1"), "ns1.nn0,ns1.nn1");
    testConfig(asList("ns1.nn0", "ns1.nn1"), "ns1.nn0, ns1.nn1");
    testConfig(asList("ns1.nn0", "ns1.nn1"), " ns1.nn0,ns1.nn1");
    testConfig(asList("ns1.nn0", "ns1.nn1"), "ns1.nn0,ns1.nn1,");
  }

  /**
   * Test if configuring a Router to monitor particular Namenodes actually
   * takes effect.
   * @param expectedNNs Namenodes that should be monitored.
   * @param confNsIds Router configuration setting for Namenodes to monitor.
   */
  private void testConfig(
      Collection<String> expectedNNs, String confNsIds) {

    // Setup and start the Router
    Configuration conf = getNamenodesConfig();
    Configuration routerConf = new RouterConfigBuilder(conf)
        .heartbeat(true)
        .build();
    routerConf.set(RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY, "0.0.0.0:0");
    routerConf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE, confNsIds);
    router = new Router();
    router.init(routerConf);

    // Test the heartbeat services of the Router
    Collection<NamenodeHeartbeatService> heartbeatServices =
        router.getNamenodeHeartbeatServices();
    assertNamenodeHeartbeatService(expectedNNs, heartbeatServices);
  }

  /**
   * Assert that the namenodes monitored by the Router are the expected.
   * @param expected Expected namenodes.
   * @param actual Actual heartbeat services for the Router
   */
  private static void assertNamenodeHeartbeatService(
      Collection<String> expected,
      Collection<NamenodeHeartbeatService> actual) {

    final Set<String> actualSet = new TreeSet<>();
    for (NamenodeHeartbeatService heartbeatService : actual) {
      NamenodeStatusReport report = heartbeatService.getNamenodeStatusReport();
      StringBuilder sb = new StringBuilder();
      sb.append(report.getNameserviceId());
      sb.append(".");
      sb.append(report.getNamenodeId());
      actualSet.add(sb.toString());
    }
    assertTrue(expected + " does not contain all " + actualSet,
        expected.containsAll(actualSet));
    assertTrue(actualSet + " does not contain all " + expected,
        actualSet.containsAll(expected));
  }

  @Test
  public void testJmxUrlHTTP() {
    verifyUrlSchemes(HttpConfig.Policy.HTTP_ONLY.name());
  }

  @Test
  public void testJmxUrlHTTPs() {
    verifyUrlSchemes(HttpConfig.Policy.HTTPS_ONLY.name());
  }

  private void verifyUrlSchemes(String scheme) {

    // Attach our own log appender so we can verify output
    final LogVerificationAppender appender =
        new LogVerificationAppender();
    final org.apache.log4j.Logger logger =
        org.apache.log4j.Logger.getRootLogger();
    logger.addAppender(appender);
    logger.setLevel(Level.DEBUG);

    // Setup and start the Router
    Configuration conf = getNamenodesConfig();
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, scheme);
    Configuration routerConf = new RouterConfigBuilder(conf)
        .heartbeat(true)
        .build();
    routerConf.set(RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY, "0.0.0.0:0");
    routerConf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE, "ns1.nn0");
    router = new Router();
    router.init(routerConf);

    // Test the heartbeat services of the Router
    Collection<NamenodeHeartbeatService> heartbeatServices =
        router.getNamenodeHeartbeatServices();
    for (NamenodeHeartbeatService heartbeatService : heartbeatServices) {
      heartbeatService.getNamenodeStatusReport();
    }
    if (HttpConfig.Policy.HTTPS_ONLY.name().equals(scheme)) {
      assertEquals(1, appender.countLinesWithMessage("JMX URL: https://"));
      assertEquals(0, appender.countLinesWithMessage("JMX URL: http://"));
    } else {
      assertEquals(1, appender.countLinesWithMessage("JMX URL: http://"));
      assertEquals(0, appender.countLinesWithMessage("JMX URL: https://"));
    }
  }

  /**
   * Test the view of the Datanodes that the Router sees. If a Datanode is
   * registered in two subclusters, it should return the most up to date
   * information.
   * @throws IOException If the test cannot run.
   */
  @Test
  public void testDatanodesView() throws IOException {

    // Setup the router
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .rpc()
        .build();
    router = new Router();
    router.init(routerConf);
    router.start();

    // Setup the namenodes
    for (String nsId : nsIds) {
      registerSubclusters(router, nns.get(nsId).values());
      for (String nnId : nnIds) {
        MockNamenode nn = nns.get(nsId).get(nnId);
        if ("nn0".equals(nnId)) {
          nn.transitionToActive();
        }
        nn.addDatanodeMock();
      }
    }

    // Set different states for the DNs in each namespace
    long time = Time.now();
    for (String nsId : nsIds) {
      for (String nnId : nnIds) {
        // dn0 is DECOMMISSIONED in the most recent (ns1)
        DatanodeInfoBuilder dn0Builder = new DatanodeInfoBuilder()
            .setDatanodeUuid("dn0")
            .setHostName("dn0")
            .setIpAddr("dn0")
            .setXferPort(10000);
        if ("ns0".equals(nsId)) {
          dn0Builder.setLastUpdate(time - 1000);
          dn0Builder.setAdminState(AdminStates.NORMAL);
        } else if ("ns1".equals(nsId)) {
          dn0Builder.setLastUpdate(time - 500);
          dn0Builder.setAdminState(AdminStates.DECOMMISSIONED);
        }

        // dn1 is NORMAL in the most recent (ns0)
        DatanodeInfoBuilder dn1Builder = new DatanodeInfoBuilder()
            .setDatanodeUuid("dn1")
            .setHostName("dn1")
            .setIpAddr("dn1")
            .setXferPort(10000);
        if ("ns0".equals(nsId)) {
          dn1Builder.setLastUpdate(time - 1000);
          dn1Builder.setAdminState(AdminStates.NORMAL);
        } else if ("ns1".equals(nsId)) {
          dn1Builder.setLastUpdate(time - 5 * 1000);
          dn1Builder.setAdminState(AdminStates.DECOMMISSION_INPROGRESS);
        }

        // Update the mock NameNode with the DN views
        MockNamenode nn = nns.get(nsId).get(nnId);
        List<DatanodeInfo> dns = nn.getDatanodes();
        dns.add(dn0Builder.build());
        dns.add(dn1Builder.build());
      }
    }

    // Get the datanodes from the Router and check we get the right view
    DistributedFileSystem dfs = (DistributedFileSystem)getFileSystem(router);
    DFSClient dfsClient = dfs.getClient();
    DatanodeStorageReport[] dns = dfsClient.getDatanodeStorageReport(
        DatanodeReportType.ALL);
    assertEquals(2, dns.length);
    for (DatanodeStorageReport dn : dns) {
      DatanodeInfo dnInfo = dn.getDatanodeInfo();
      if ("dn0".equals(dnInfo.getHostName())) {
        assertEquals(AdminStates.DECOMMISSIONED, dnInfo.getAdminState());
      } else if ("dn1".equals(dnInfo.getHostName())) {
        assertEquals(AdminStates.NORMAL, dnInfo.getAdminState());
      } else {
        fail("Unexpected DN: " + dnInfo.getHostName());
      }
    }
  }
}
