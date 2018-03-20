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

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMESERVICES;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
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
public class TestNamenodeHeartbeat {

  private static RouterDFSCluster cluster;
  private static ActiveNamenodeResolver namenodeResolver;
  private static List<NamenodeHeartbeatService> services;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void globalSetUp() throws Exception {

    cluster = new RouterDFSCluster(true, 2);

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

    RouterDFSCluster testCluster = new RouterDFSCluster(true, 1);
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
          namenodeResolver.getNamenodesForNameserviceId(ns);

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
        namenodeResolver.getNamenodesForNameserviceId(failoverNS);
    // Active
    FederationNamenodeContext active = failoverNSs.get(0);
    assertEquals(NAMENODES[1], active.getNamenodeId());

    // Standby
    FederationNamenodeContext standby = failoverNSs.get(1);
    assertEquals(NAMENODES[0], standby.getNamenodeId());

    // Verify the locator has the same records for the other ns
    List<? extends FederationNamenodeContext> normalNss =
        namenodeResolver.getNamenodesForNameserviceId(normalNs);
    // Active
    active = normalNss.get(0);
    assertEquals(NAMENODES[0], active.getNamenodeId());
    // Standby
    standby = normalNss.get(1);
    assertEquals(NAMENODES[1], standby.getNamenodeId());
  }
}
