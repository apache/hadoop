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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICES;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMESERVICES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.router.SecurityConfUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.net.MockDomainNameResolver;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test the service that heartbeats the state of the namenodes to the State
 * Store.
 */
public class TestRouterNamenodeHeartbeat {

  private static MiniRouterDFSCluster cluster;
  private static ActiveNamenodeResolver namenodeResolver;
  private static List<NamenodeHeartbeatService> services;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void globalSetUp() throws Exception {

    cluster = new MiniRouterDFSCluster(true, 2);

    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Mock locator that records the heartbeats
    List<String> nss = cluster.getNameservices();
    String ns = nss.get(0);
    Configuration conf = cluster.generateNamenodeConfiguration(ns);
    namenodeResolver = new MockResolver(conf);
    namenodeResolver.setRouterId("testrouter");

    // Create one heartbeat service per NN
    services = new ArrayList<>();
    for (NamenodeContext nn : cluster.getNamenodes()) {
      String nsId = nn.getNameserviceId();
      String nnId = nn.getNamenodeId();
      NamenodeHeartbeatService service = new NamenodeHeartbeatService(
          namenodeResolver, nsId, nnId);
      service.init(conf);
      service.start();
      services.add(service);
    }
  }

  @AfterClass
  public static void tearDown() throws IOException {
    cluster.shutdown();
    for (NamenodeHeartbeatService service: services) {
      service.stop();
      service.close();
    }
  }

  @Test
  public void testNamenodeHeartbeatService() throws IOException {

    MiniRouterDFSCluster testCluster = new MiniRouterDFSCluster(true, 1);
    Configuration heartbeatConfig = testCluster.generateNamenodeConfiguration(
        NAMESERVICES[0]);
    NamenodeHeartbeatService server = new NamenodeHeartbeatService(
        namenodeResolver, NAMESERVICES[0], NAMENODES[0]);
    server.init(heartbeatConfig);
    assertEquals(STATE.INITED, server.getServiceState());
    server.start();
    assertEquals(STATE.STARTED, server.getServiceState());
    server.stop();
    assertEquals(STATE.STOPPED, server.getServiceState());
    server.close();
  }

  @Test
  public void testLocalNamenodeHeartbeatService() throws IOException {
    Router router = new Router();
    Configuration conf = new Configuration();
    assertEquals(null, DFSUtil.getNamenodeNameServiceId(conf));

    // case 1: no local nn is configured
    router.setConf(conf);
    assertNull(router.createLocalNamenodeHeartbeatService());

    // case 2: local nn is configured
    conf.set(DFS_NAMESERVICES, "ns1");
    assertEquals("ns1", DFSUtil.getNamenodeNameServiceId(conf));
    conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, "ns1"),
        "nn1,nn2");
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, "ns1", "nn1"),
        "localhost:8020");
    conf.set(DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, "ns1", "nn2"),
        "ns1-nn2.example.com:8020");
    router.setConf(conf);
    NamenodeHeartbeatService heartbeatService =
        router.createLocalNamenodeHeartbeatService();
    assertNotNull(heartbeatService);
    // we have to start the service to get the serviceAddress assigned
    heartbeatService.init(conf);
    assertEquals("ns1-nn1:localhost:8020",
        heartbeatService.getNamenodeDesc());
    heartbeatService.stop();
  }

  @Test
  public void testHearbeat() throws InterruptedException, IOException {

    // Set NAMENODE1 to active for all nameservices
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
      }
    }

    // Wait for heartbeats to record
    Thread.sleep(5000);

    // Verify the locator has matching NN entries for each NS
    for (String ns : cluster.getNameservices()) {
      List<? extends FederationNamenodeContext> nns =
          namenodeResolver.getNamenodesForNameserviceId(ns, false);

      // Active
      FederationNamenodeContext active = nns.get(0);
      assertEquals(NAMENODES[0], active.getNamenodeId());

      // Standby
      FederationNamenodeContext standby = nns.get(1);
      assertEquals(NAMENODES[1], standby.getNamenodeId());
    }

    // Switch active NNs in 1/2 nameservices
    List<String> nss = cluster.getNameservices();
    String failoverNS = nss.get(0);
    String normalNs = nss.get(1);

    cluster.switchToStandby(failoverNS, NAMENODES[0]);
    cluster.switchToActive(failoverNS, NAMENODES[1]);

    // Wait for heartbeats to record
    Thread.sleep(5000);

    // Verify the locator has recorded the failover for the failover NS
    List<? extends FederationNamenodeContext> failoverNSs =
        namenodeResolver.getNamenodesForNameserviceId(failoverNS, false);
    // Active
    FederationNamenodeContext active = failoverNSs.get(0);
    assertEquals(NAMENODES[1], active.getNamenodeId());

    // Standby
    FederationNamenodeContext standby = failoverNSs.get(1);
    assertEquals(NAMENODES[0], standby.getNamenodeId());

    // Verify the locator has the same records for the other ns
    List<? extends FederationNamenodeContext> normalNss =
        namenodeResolver.getNamenodesForNameserviceId(normalNs, false);
    // Active
    active = normalNss.get(0);
    assertEquals(NAMENODES[0], active.getNamenodeId());
    // Standby
    standby = normalNss.get(1);
    assertEquals(NAMENODES[1], standby.getNamenodeId());
  }

  @Test
  public void testNamenodeHeartbeatServiceHAServiceProtocolProxy(){
    testNamenodeHeartbeatServiceHAServiceProtocol(
        "test-ns", "nn", 1000, -1, -1, 1003,
        "host01.test:1000", "host02.test:1000");
    testNamenodeHeartbeatServiceHAServiceProtocol(
        "test-ns", "nn", 1000, 1001, -1, 1003,
        "host01.test:1001", "host02.test:1001");
    testNamenodeHeartbeatServiceHAServiceProtocol(
        "test-ns", "nn", 1000, -1, 1002, 1003,
        "host01.test:1002", "host02.test:1002");
    testNamenodeHeartbeatServiceHAServiceProtocol(
        "test-ns", "nn", 1000, 1001, 1002, 1003,
        "host01.test:1002", "host02.test:1002");
  }

  private void testNamenodeHeartbeatServiceHAServiceProtocol(
      String nsId, String nnId,
      int rpcPort, int servicePort,
      int lifelinePort, int webAddressPort,
      String expected0, String expected1) {
    Configuration conf = generateNamenodeConfiguration(nsId, nnId,
        rpcPort, servicePort, lifelinePort, webAddressPort);

    Router testRouter = new Router();
    testRouter.setConf(conf);

    Collection<NamenodeHeartbeatService> heartbeatServices =
        testRouter.createNamenodeHeartbeatServices();

    assertEquals(2, heartbeatServices.size());

    Iterator<NamenodeHeartbeatService> iterator = heartbeatServices.iterator();
    NamenodeHeartbeatService service0 = iterator.next();
    service0.init(conf);
    assertNotNull(service0.getLocalTarget());
    assertEquals(expected0, service0.getLocalTarget().getHealthMonitorAddress().toString());

    NamenodeHeartbeatService service1 = iterator.next();
    service1.init(conf);
    assertNotNull(service1.getLocalTarget());
    assertEquals(expected1, service1.getLocalTarget().getHealthMonitorAddress().toString());
  }

  @Test
  public void testNamenodeHeartbeatServiceNNResolution() {
    String nsId = "test-ns";
    String nnId = "nn";
    int rpcPort = 1000;
    int servicePort = 1001;
    int lifelinePort = 1002;
    int webAddressPort = 1003;
    Configuration conf = generateNamenodeConfiguration(nsId, nnId,
        rpcPort, servicePort, lifelinePort, webAddressPort);

    Router testRouter = new Router();
    testRouter.setConf(conf);

    Collection<NamenodeHeartbeatService> heartbeatServices =
        testRouter.createNamenodeHeartbeatServices();

    assertEquals(2, heartbeatServices.size());

    Iterator<NamenodeHeartbeatService> iterator = heartbeatServices.iterator();
    NamenodeHeartbeatService service = iterator.next();
    service.init(conf);
    assertEquals("test-ns-nn-host01.test:host01.test:1001",
        service.getNamenodeDesc());

    service = iterator.next();
    service.init(conf);
    assertEquals("test-ns-nn-host02.test:host02.test:1001",
        service.getNamenodeDesc());

  }

  private Configuration generateNamenodeConfiguration(
      String nsId, String nnId,
      int rpcPort, int servicePort,
      int lifelinePort, int webAddressPort) {
    Configuration conf = new HdfsConfiguration();
    String suffix = nsId + "." + nnId;

    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_MONITOR_LOCAL_NAMENODE, false);
    conf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE, nsId + "." + nnId);

    conf.setBoolean(
        RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE_RESOLUTION_ENABLED + "." + nsId, true);
    conf.set(
        RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE_RESOLVER_IMPL + "." + nsId,
        MockDomainNameResolver.class.getName());

    conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY + "." + suffix,
        MockDomainNameResolver.DOMAIN + ":" + rpcPort);
    if (servicePort >= 0){
      conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + "." + suffix,
          MockDomainNameResolver.DOMAIN + ":" + servicePort);
    }
    if (lifelinePort >= 0){
      conf.set(DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + "." + suffix,
          MockDomainNameResolver.DOMAIN + ":" + lifelinePort);
    }
    conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY + "." + suffix,
        MockDomainNameResolver.DOMAIN + ":" + webAddressPort);

    return conf;
  }

  @Test
  public void testNamenodeHeartbeatWithSecurity() throws Exception {
    Configuration conf = SecurityConfUtil.initSecurity();
    MiniRouterDFSCluster testCluster = null;
    try {
      testCluster = new MiniRouterDFSCluster(true, 1, conf);
      // Start Namenodes and routers
      testCluster.startCluster(conf);
      testCluster.startRouters();

      // Register Namenodes to generate a NamenodeStatusReport
      testCluster.registerNamenodes();
      testCluster.waitNamenodeRegistration();

      for (MiniRouterDFSCluster.RouterContext routerContext : testCluster.getRouters()) {
        ActiveNamenodeResolver resolver = routerContext.getRouter().getNamenodeResolver();
        // Validate that NamenodeStatusReport has been registered
        assertNotNull(resolver.getNamespaces());
        assertFalse(resolver.getNamespaces().isEmpty());
      }
    } finally {
      if (testCluster != null) {
        testCluster.shutdown();
      }
      UserGroupInformation.reset();
      SecurityConfUtil.destroy();
    }
  }
}
