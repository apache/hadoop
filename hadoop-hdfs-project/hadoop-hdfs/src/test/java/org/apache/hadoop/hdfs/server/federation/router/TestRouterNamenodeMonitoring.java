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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test namenodes monitor behavior in the Router.
 */
public class TestRouterNamenodeMonitoring {

  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  private static MembershipNamenodeResolver resolver;

  private String ns0;
  private String ns1;
  private long initializedTime;

  @Before
  public void setUp() throws Exception {
    // Build and start a federated cluster with HA enabled
    cluster = new StateStoreDFSCluster(true, 2);
    // Enable heartbeat service and local heartbeat
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .enableLocalHeartbeat(true)
        .heartbeat()
        .build();

    // Specify local node (ns0.nn1) to monitor
    StringBuilder sb = new StringBuilder();
    ns0 = cluster.getNameservices().get(0);
    NamenodeContext context = cluster.getNamenodes(ns0).get(1);
    routerConf.set(DFS_NAMESERVICE_ID, ns0);
    routerConf.set(DFS_HA_NAMENODE_ID_KEY, context.getNamenodeId());

    // Specify namenodes (ns1.nn0,ns1.nn1) to monitor
    sb = new StringBuilder();
    ns1 = cluster.getNameservices().get(1);
    for (NamenodeContext ctx : cluster.getNamenodes(ns1)) {
      String suffix = ctx.getConfSuffix();
      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(suffix);
    }
    // override with the namenodes: ns1.nn0,ns1.nn1
    routerConf.set(DFS_ROUTER_MONITOR_NAMENODE, sb.toString());

    cluster.addRouterOverrides(routerConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    routerContext = cluster.getRandomRouter();
    resolver = (MembershipNamenodeResolver) routerContext.getRouter()
        .getNamenodeResolver();
    initializedTime = Time.now();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testNamenodeMonitoring() throws Exception {
    // Set nn0 to active for all nameservices
    for (String ns : cluster.getNameservices()) {
      cluster.switchToActive(ns, "nn0");
      cluster.switchToStandby(ns, "nn1");
    }

    Collection<NamenodeHeartbeatService> heartbeatServices = routerContext
        .getRouter().getNamenodeHearbeatServices();
    // manually trigger the heartbeat
    for (NamenodeHeartbeatService service : heartbeatServices) {
      service.periodicInvoke();
    }

    resolver.loadCache(true);
    List<? extends FederationNamenodeContext> namespaceInfo0 =
        resolver.getNamenodesForNameserviceId(ns0);
    List<? extends FederationNamenodeContext> namespaceInfo1 =
        resolver.getNamenodesForNameserviceId(ns1);

    // The modified date won't be updated in ns0.nn0 since it isn't
    // monitored by the Router.
    assertEquals("nn0", namespaceInfo0.get(1).getNamenodeId());
    assertTrue(namespaceInfo0.get(1).getDateModified() < initializedTime);

    // other namnodes should be updated as expected
    assertEquals("nn1", namespaceInfo0.get(0).getNamenodeId());
    assertTrue(namespaceInfo0.get(0).getDateModified() > initializedTime);

    assertEquals("nn0", namespaceInfo1.get(0).getNamenodeId());
    assertTrue(namespaceInfo1.get(0).getDateModified() > initializedTime);

    assertEquals("nn1", namespaceInfo1.get(1).getNamenodeId());
    assertTrue(namespaceInfo1.get(1).getDateModified() > initializedTime);
  }
}
